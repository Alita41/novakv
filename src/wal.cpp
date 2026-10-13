#include "novakv/wal.hpp"

#include <array>
#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <system_error>
#include <unistd.h>

namespace novakv {
namespace {
constexpr std::uint32_t MAGIC = 0x4E4B5631; // NKV1

std::uint32_t crc32(const unsigned char* data, std::size_t len) {
    std::uint32_t crc = 0xFFFFFFFFu;
    for (std::size_t i = 0; i < len; ++i) {
        crc ^= data[i];
        for (int j = 0; j < 8; ++j) crc = (crc >> 1) ^ (0xEDB88320u & (0u - (crc & 1u)));
    }
    return ~crc;
}

template <typename T>
void append_bytes(std::vector<unsigned char>& b, T value) {
    for (std::size_t i = 0; i < sizeof(T); ++i) b.push_back(static_cast<unsigned char>((static_cast<std::uint64_t>(value) >> (i * 8)) & 0xFF));
}

template <typename T>
bool read_num(std::istream& in, T& value) {
    std::array<unsigned char, sizeof(T)> b{};
    if (!in.read(reinterpret_cast<char*>(b.data()), b.size())) return false;
    std::uint64_t x = 0;
    for (std::size_t i = 0; i < b.size(); ++i) x |= static_cast<std::uint64_t>(b[i]) << (i * 8);
    value = static_cast<T>(x);
    return true;
}
}

WAL::WAL(std::string path)
    : path_(std::move(path)) {
    std::filesystem::path p(path_);
    if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    reopen_append();
}

WAL::~WAL() { if (out_.is_open()) out_.close(); }

void WAL::reopen_append() {
    if (out_.is_open()) out_.close();
    out_.open(path_, std::ios::binary | std::ios::app);
    if (!out_) throw std::runtime_error("unable to open WAL: " + path_);
}

void WAL::append_record(WalOp op, const std::string& key, const std::string& value, std::int64_t expires_at_ms) {
    if (key.size() > 1024 * 1024 || value.size() > 64 * 1024 * 1024) throw std::runtime_error("WAL record too large");
    std::vector<unsigned char> payload;
    payload.reserve(1 + 4 + 4 + 8 + key.size() + value.size());
    payload.push_back(static_cast<unsigned char>(op));
    append_bytes<std::uint32_t>(payload, static_cast<std::uint32_t>(key.size()));
    append_bytes<std::uint32_t>(payload, static_cast<std::uint32_t>(value.size()));
    append_bytes<std::uint64_t>(payload, static_cast<std::uint64_t>(expires_at_ms));
    payload.insert(payload.end(), key.begin(), key.end());
    payload.insert(payload.end(), value.begin(), value.end());
    const auto checksum = crc32(payload.data(), payload.size());

    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<unsigned char> header;
    append_bytes<std::uint32_t>(header, MAGIC);
    append_bytes<std::uint32_t>(header, static_cast<std::uint32_t>(payload.size()));
    append_bytes<std::uint32_t>(header, checksum);
    out_.write(reinterpret_cast<const char*>(header.data()), header.size());
    out_.write(reinterpret_cast<const char*>(payload.data()), payload.size());
    out_.flush();
    if (!out_) throw std::runtime_error("failed writing WAL");

}

void WAL::append_set(const std::string& key, const std::string& value, std::int64_t expires_at_ms) {
    append_record(WalOp::Set, key, value, expires_at_ms);
}

void WAL::append_delete(const std::string& key) {
    append_record(WalOp::Delete, key, "", 0);
}

std::size_t WAL::replay(Store& store) {
    std::lock_guard<std::mutex> lock(mutex_);
    out_.flush();
    std::ifstream in(path_, std::ios::binary);
    if (!in) return 0;
    std::size_t applied = 0;
    while (true) {
        std::uint32_t magic = 0, payload_len = 0, expected_crc = 0;
        if (!read_num(in, magic)) break;
        if (!read_num(in, payload_len) || !read_num(in, expected_crc)) break;
        if (magic != MAGIC || payload_len > 70 * 1024 * 1024) break;
        std::vector<unsigned char> payload(payload_len);
        if (!in.read(reinterpret_cast<char*>(payload.data()), payload.size())) break; // tolerate partial tail
        if (crc32(payload.data(), payload.size()) != expected_crc) break;
        if (payload.size() < 17) break;
        std::size_t pos = 0;
        const auto op = static_cast<WalOp>(payload[pos++]);
        auto take_u32 = [&]() {
            std::uint32_t x = 0; for (int i=0;i<4;++i) x |= static_cast<std::uint32_t>(payload[pos++]) << (8*i); return x;
        };
        auto take_i64 = [&]() {
            std::uint64_t x = 0; for (int i=0;i<8;++i) x |= static_cast<std::uint64_t>(payload[pos++]) << (8*i); return static_cast<std::int64_t>(x);
        };
        const auto key_len = take_u32();
        const auto value_len = take_u32();
        const auto expires = take_i64();
        if (pos + key_len + value_len != payload.size()) break;
        const std::string key(reinterpret_cast<const char*>(payload.data()+pos), key_len); pos += key_len;
        const std::string value(reinterpret_cast<const char*>(payload.data()+pos), value_len);
        if (op == WalOp::Set) {
            if (expires == 0 || expires > Store::now_ms()) store.set_absolute(key, value, expires);
            else store.erase(key);
        } else if (op == WalOp::Delete) {
            store.erase(key);
        } else break;
        ++applied;
    }
    return applied;
}

void WAL::checkpoint(const Store& store) {
    const auto entries = store.snapshot();
    const std::string tmp = path_ + ".checkpoint";
    {
        std::ofstream fresh(tmp, std::ios::binary | std::ios::trunc);
        if (!fresh) throw std::runtime_error("unable to create WAL checkpoint");
        for (const auto& [key, entry] : entries) {
            std::vector<unsigned char> payload;
            payload.push_back(static_cast<unsigned char>(WalOp::Set));
            append_bytes<std::uint32_t>(payload, static_cast<std::uint32_t>(key.size()));
            append_bytes<std::uint32_t>(payload, static_cast<std::uint32_t>(entry.value.size()));
            append_bytes<std::uint64_t>(payload, static_cast<std::uint64_t>(entry.expires_at_ms));
            payload.insert(payload.end(), key.begin(), key.end());
            payload.insert(payload.end(), entry.value.begin(), entry.value.end());
            const auto checksum = crc32(payload.data(), payload.size());
            std::vector<unsigned char> header;
            append_bytes<std::uint32_t>(header, MAGIC);
            append_bytes<std::uint32_t>(header, static_cast<std::uint32_t>(payload.size()));
            append_bytes<std::uint32_t>(header, checksum);
            fresh.write(reinterpret_cast<const char*>(header.data()), header.size());
            fresh.write(reinterpret_cast<const char*>(payload.data()), payload.size());
        }
        fresh.flush();
        if (!fresh) throw std::runtime_error("failed writing WAL checkpoint");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (out_.is_open()) out_.close();
    std::filesystem::rename(tmp, path_);
    reopen_append();
}

}  // namespace novakv
