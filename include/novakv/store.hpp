#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "novakv/lru_cache.hpp"

namespace novakv {

struct Entry {
    std::string value;
    std::int64_t expires_at_ms = 0;  // 0 => no expiry
};

struct StoreStats {
    std::uint64_t gets = 0;
    std::uint64_t sets = 0;
    std::uint64_t deletes = 0;
    std::uint64_t hits = 0;
    std::uint64_t misses = 0;
    std::uint64_t cache_hits = 0;
    std::uint64_t cache_misses = 0;
    std::size_t live_keys = 0;
};

class Store {
public:
    explicit Store(std::size_t shard_count = 64, std::size_t cache_capacity = 4096);

    void set(const std::string& key, std::string value, std::int64_t ttl_ms = 0);
    void set_absolute(const std::string& key, std::string value, std::int64_t expires_at_ms);
    std::optional<std::string> get(const std::string& key);
    bool erase(const std::string& key);
    bool exists(const std::string& key);
    std::int64_t ttl_ms(const std::string& key);
    std::optional<std::int64_t> incr(const std::string& key, std::int64_t delta);

    std::vector<std::pair<std::string, Entry>> snapshot() const;
    void clear();
    StoreStats stats() const;

    static std::int64_t now_ms();

private:
    struct Shard {
        mutable std::shared_mutex mutex;
        std::unordered_map<std::string, Entry> data;
    };

    std::size_t shard_index(const std::string& key) const;
    bool expired(const Entry& entry, std::int64_t now) const;

    std::vector<std::unique_ptr<Shard>> shards_;
    LRUCache cache_;

    std::atomic<std::uint64_t> gets_{0};
    std::atomic<std::uint64_t> sets_{0};
    std::atomic<std::uint64_t> deletes_{0};
    std::atomic<std::uint64_t> hits_{0};
    std::atomic<std::uint64_t> misses_{0};
    std::atomic<std::uint64_t> cache_hits_{0};
    std::atomic<std::uint64_t> cache_misses_{0};
};

}  // namespace novakv
