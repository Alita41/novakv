#pragma once

#include <cstdint>
#include <fstream>
#include <mutex>
#include <string>
#include <vector>

#include "novakv/store.hpp"

namespace novakv {

enum class WalOp : std::uint8_t { Set = 1, Delete = 2 };

class WAL {
public:
    explicit WAL(std::string path);
    ~WAL();

    void append_set(const std::string& key, const std::string& value, std::int64_t expires_at_ms);
    void append_delete(const std::string& key);
    std::size_t replay(Store& store);
    void checkpoint(const Store& store);

    const std::string& path() const { return path_; }

private:
    void append_record(WalOp op, const std::string& key, const std::string& value, std::int64_t expires_at_ms);
    void reopen_append();

    std::string path_;
    std::ofstream out_;
    std::mutex mutex_;
};

}  // namespace novakv
