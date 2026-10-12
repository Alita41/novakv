#include "novakv/store.hpp"

#include <charconv>
#include <limits>
#include <mutex>

namespace novakv {

Store::Store(std::size_t shard_count, std::size_t cache_capacity)
    : cache_(cache_capacity) {
    if (shard_count == 0) shard_count = 1;
    shards_.reserve(shard_count);
    for (std::size_t i = 0; i < shard_count; ++i) shards_.push_back(std::make_unique<Shard>());
}

std::int64_t Store::now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

std::size_t Store::shard_index(const std::string& key) const {
    return std::hash<std::string>{}(key) % shards_.size();
}

bool Store::expired(const Entry& entry, std::int64_t now) const {
    return entry.expires_at_ms > 0 && entry.expires_at_ms <= now;
}

void Store::set(const std::string& key, std::string value, std::int64_t ttl_ms_value) {
    std::int64_t expires = 0;
    if (ttl_ms_value > 0) {
        const auto now = now_ms();
        expires = ttl_ms_value > (std::numeric_limits<std::int64_t>::max() - now)
                      ? std::numeric_limits<std::int64_t>::max()
                      : now + ttl_ms_value;
    }
    set_absolute(key, std::move(value), expires);
}

void Store::set_absolute(const std::string& key, std::string value, std::int64_t expires_at_ms) {
    auto& shard = *shards_[shard_index(key)];
    {
        std::unique_lock<std::shared_mutex> lock(shard.mutex);
        shard.data[key] = Entry{value, expires_at_ms};
    }
    cache_.put(key, std::move(value));
    sets_.fetch_add(1, std::memory_order_relaxed);
}

std::optional<std::string> Store::get(const std::string& key) {
    gets_.fetch_add(1, std::memory_order_relaxed);
    if (auto cached = cache_.get(key)) {
        auto& shard = *shards_[shard_index(key)];
        std::shared_lock<std::shared_mutex> lock(shard.mutex);
        auto it = shard.data.find(key);
        if (it != shard.data.end() && !expired(it->second, now_ms())) {
            cache_hits_.fetch_add(1, std::memory_order_relaxed);
            hits_.fetch_add(1, std::memory_order_relaxed);
            return cached;
        }
    }
    cache_misses_.fetch_add(1, std::memory_order_relaxed);

    auto& shard = *shards_[shard_index(key)];
    const auto now = now_ms();
    {
        std::shared_lock<std::shared_mutex> lock(shard.mutex);
        auto it = shard.data.find(key);
        if (it == shard.data.end()) {
            misses_.fetch_add(1, std::memory_order_relaxed);
            return std::nullopt;
        }
        if (!expired(it->second, now)) {
            auto value = it->second.value;
            lock.unlock();
            cache_.put(key, value);
            hits_.fetch_add(1, std::memory_order_relaxed);
            return value;
        }
    }
    {
        std::unique_lock<std::shared_mutex> lock(shard.mutex);
        auto it = shard.data.find(key);
        if (it != shard.data.end() && expired(it->second, now)) shard.data.erase(it);
    }
    cache_.erase(key);
    misses_.fetch_add(1, std::memory_order_relaxed);
    return std::nullopt;
}

bool Store::erase(const std::string& key) {
    auto& shard = *shards_[shard_index(key)];
    bool removed = false;
    {
        std::unique_lock<std::shared_mutex> lock(shard.mutex);
        removed = shard.data.erase(key) > 0;
    }
    cache_.erase(key);
    deletes_.fetch_add(1, std::memory_order_relaxed);
    return removed;
}

bool Store::exists(const std::string& key) {
    return get(key).has_value();
}

std::int64_t Store::ttl_ms(const std::string& key) {
    auto& shard = *shards_[shard_index(key)];
    const auto now = now_ms();
    {
        std::shared_lock<std::shared_mutex> lock(shard.mutex);
        auto it = shard.data.find(key);
        if (it == shard.data.end()) return -2;
        if (expired(it->second, now)) {
            lock.unlock();
            erase(key);
            return -2;
        }
        if (it->second.expires_at_ms == 0) return -1;
        return std::max<std::int64_t>(0, it->second.expires_at_ms - now);
    }
}

std::optional<std::int64_t> Store::incr(const std::string& key, std::int64_t delta) {
    auto& shard = *shards_[shard_index(key)];
    std::string updated;
    std::int64_t result = 0;
    {
        std::unique_lock<std::shared_mutex> lock(shard.mutex);
        auto it = shard.data.find(key);
        if (it != shard.data.end() && expired(it->second, now_ms())) {
            shard.data.erase(it);
            it = shard.data.end();
        }
        if (it == shard.data.end()) {
            result = delta;
            updated = std::to_string(result);
            shard.data[key] = Entry{updated, 0};
        } else {
            const auto& raw = it->second.value;
            std::int64_t current = 0;
            const auto parsed = std::from_chars(raw.data(), raw.data() + raw.size(), current);
            if (parsed.ec != std::errc{} || parsed.ptr != raw.data() + raw.size()) return std::nullopt;
            if ((delta > 0 && current > std::numeric_limits<std::int64_t>::max() - delta) ||
                (delta < 0 && current < std::numeric_limits<std::int64_t>::min() - delta)) return std::nullopt;
            result = current + delta;
            updated = std::to_string(result);
            it->second.value = updated;
        }
    }
    cache_.put(key, updated);
    sets_.fetch_add(1, std::memory_order_relaxed);
    return result;
}

std::vector<std::pair<std::string, Entry>> Store::snapshot() const {
    std::vector<std::pair<std::string, Entry>> out;
    const auto now = now_ms();
    for (const auto& shard_ptr : shards_) {
        std::shared_lock<std::shared_mutex> lock(shard_ptr->mutex);
        for (const auto& item : shard_ptr->data) {
            if (!expired(item.second, now)) out.push_back(item);
        }
    }
    return out;
}

void Store::clear() {
    for (auto& shard_ptr : shards_) {
        std::unique_lock<std::shared_mutex> lock(shard_ptr->mutex);
        shard_ptr->data.clear();
    }
    cache_.clear();
}

StoreStats Store::stats() const {
    StoreStats s;
    s.gets = gets_.load(std::memory_order_relaxed);
    s.sets = sets_.load(std::memory_order_relaxed);
    s.deletes = deletes_.load(std::memory_order_relaxed);
    s.hits = hits_.load(std::memory_order_relaxed);
    s.misses = misses_.load(std::memory_order_relaxed);
    s.cache_hits = cache_hits_.load(std::memory_order_relaxed);
    s.cache_misses = cache_misses_.load(std::memory_order_relaxed);
    s.live_keys = snapshot().size();
    return s;
}

}  // namespace novakv
