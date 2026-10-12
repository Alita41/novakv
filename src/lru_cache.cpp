#include "novakv/lru_cache.hpp"

namespace novakv {

LRUCache::LRUCache(std::size_t capacity) : capacity_(capacity) {}

std::optional<std::string> LRUCache::get(const std::string& key) {
    if (capacity_ == 0) return std::nullopt;
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = lookup_.find(key);
    if (it == lookup_.end()) return std::nullopt;
    items_.splice(items_.begin(), items_, it->second);
    return it->second->second;
}

void LRUCache::put(const std::string& key, std::string value) {
    if (capacity_ == 0) return;
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = lookup_.find(key);
    if (it != lookup_.end()) {
        it->second->second = std::move(value);
        items_.splice(items_.begin(), items_, it->second);
        return;
    }
    items_.emplace_front(key, std::move(value));
    lookup_[key] = items_.begin();
    while (items_.size() > capacity_) {
        auto last = std::prev(items_.end());
        lookup_.erase(last->first);
        items_.pop_back();
    }
}

void LRUCache::erase(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = lookup_.find(key);
    if (it == lookup_.end()) return;
    items_.erase(it->second);
    lookup_.erase(it);
}

void LRUCache::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    items_.clear();
    lookup_.clear();
}

std::size_t LRUCache::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return items_.size();
}

}  // namespace novakv
