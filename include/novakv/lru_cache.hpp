#pragma once

#include <cstddef>
#include <list>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

namespace novakv {

class LRUCache {
public:
    explicit LRUCache(std::size_t capacity);

    std::optional<std::string> get(const std::string& key);
    void put(const std::string& key, std::string value);
    void erase(const std::string& key);
    void clear();

    std::size_t size() const;
    std::size_t capacity() const { return capacity_; }

private:
    using Item = std::pair<std::string, std::string>;
    using List = std::list<Item>;

    const std::size_t capacity_;
    mutable std::mutex mutex_;
    List items_;
    std::unordered_map<std::string, List::iterator> lookup_;
};

}  // namespace novakv
