#include "novakv/store.hpp"
#include "novakv/wal.hpp"

#include <arpa/inet.h>
#include <atomic>
#include <condition_variable>
#include <csignal>
#include <cstring>
#include <deque>
#include <iostream>
#include <mutex>
#include <netinet/in.h>
#include <optional>
#include <sstream>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {
std::atomic<bool> g_running{true};
volatile std::sig_atomic_t g_stop = 0;
void on_signal(int) { g_stop = 1; }

bool send_all(int fd, const std::string& data) {
    std::size_t sent = 0;
    while (sent < data.size()) {
        const auto n = ::send(fd, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
        if (n <= 0) return false;
        sent += static_cast<std::size_t>(n);
    }
    return true;
}

std::string bulk(const std::optional<std::string>& value) {
    if (!value) return "$-1\n";
    return "$" + std::to_string(value->size()) + "\n" + *value + "\n";
}

std::string json_stats(const novakv::StoreStats& s) {
    std::ostringstream out;
    out << "{\"gets\":" << s.gets << ",\"sets\":" << s.sets
        << ",\"deletes\":" << s.deletes << ",\"hits\":" << s.hits
        << ",\"misses\":" << s.misses << ",\"cache_hits\":" << s.cache_hits
        << ",\"cache_misses\":" << s.cache_misses << ",\"live_keys\":" << s.live_keys << "}";
    return out.str();
}

std::string trim_left(std::string s) {
    const auto p = s.find_first_not_of(' ');
    return p == std::string::npos ? "" : s.substr(p);
}

std::string handle(const std::string& line, novakv::Store& store, novakv::WAL& wal, bool& quit) {
    std::istringstream in(line);
    std::string cmd;
    in >> cmd;
    for (auto& c : cmd) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));

    if (cmd == "PING") return "+PONG\n";
    if (cmd == "QUIT") { quit = true; return "+BYE\n"; }
    if (cmd == "GET") {
        std::string key; if (!(in >> key)) return "-ERR GET requires key\n";
        return bulk(store.get(key));
    }
    if (cmd == "SET") {
        std::string key; if (!(in >> key)) return "-ERR SET requires key and value\n";
        std::string value; std::getline(in, value); value = trim_left(value);
        if (value.empty()) return "-ERR SET requires value\n";
        store.set(key, value); wal.append_set(key, value, 0); return "+OK\n";
    }
    if (cmd == "SETEX") {
        std::string key; long long ttl = 0;
        if (!(in >> key >> ttl) || ttl <= 0) return "-ERR SETEX requires key positive_ttl_ms value\n";
        std::string value; std::getline(in, value); value = trim_left(value);
        if (value.empty()) return "-ERR SETEX requires value\n";
        const auto expires = novakv::Store::now_ms() + ttl;
        store.set_absolute(key, value, expires); wal.append_set(key, value, expires); return "+OK\n";
    }
    if (cmd == "DEL") {
        std::string key; if (!(in >> key)) return "-ERR DEL requires key\n";
        const bool removed = store.erase(key); wal.append_delete(key); return ":" + std::to_string(removed ? 1 : 0) + "\n";
    }
    if (cmd == "EXISTS") {
        std::string key; if (!(in >> key)) return "-ERR EXISTS requires key\n";
        return ":" + std::to_string(store.exists(key) ? 1 : 0) + "\n";
    }
    if (cmd == "TTL") {
        std::string key; if (!(in >> key)) return "-ERR TTL requires key\n";
        return ":" + std::to_string(store.ttl_ms(key)) + "\n";
    }
    if (cmd == "INCR") {
        std::string key; long long delta = 1;
        if (!(in >> key)) return "-ERR INCR requires key [delta]\n";
        if (!(in >> delta)) delta = 1;
        auto value = store.incr(key, delta);
        if (!value) return "-ERR value is not an integer or overflow\n";
        auto stored = store.get(key);
        wal.append_set(key, stored.value_or("0"), 0);
        return ":" + std::to_string(*value) + "\n";
    }
    if (cmd == "STATS") {
        auto body = json_stats(store.stats()); return "$" + std::to_string(body.size()) + "\n" + body + "\n";
    }
    if (cmd == "CHECKPOINT") {
        wal.checkpoint(store); return "+OK\n";
    }
    return "-ERR unknown command\n";
}

class ConnectionQueue {
public:
    void push(int fd) { { std::lock_guard<std::mutex> lock(m_); q_.push_back(fd); } cv_.notify_one(); }
    bool pop(int& fd) {
        std::unique_lock<std::mutex> lock(m_);
        cv_.wait(lock, [&]{ return !q_.empty() || !g_running.load(); });
        if (q_.empty()) return false;
        fd = q_.front(); q_.pop_front(); return true;
    }
    void wake_all() { cv_.notify_all(); }
private:
    std::mutex m_; std::condition_variable cv_; std::deque<int> q_;
};

void serve_client(int fd, novakv::Store& store, novakv::WAL& wal) {
    timeval timeout{}; timeout.tv_sec = 0; timeout.tv_usec = 500000;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    std::string buffer;
    char chunk[4096];
    bool quit = false;
    while (!quit && g_running.load()) {
        auto n = ::recv(fd, chunk, sizeof(chunk), 0);
        if (n <= 0) break;
        buffer.append(chunk, static_cast<std::size_t>(n));
        while (true) {
            auto pos = buffer.find('\n');
            if (pos == std::string::npos) break;
            std::string line = buffer.substr(0, pos);
            if (!line.empty() && line.back() == '\r') line.pop_back();
            buffer.erase(0, pos + 1);
            if (!send_all(fd, handle(line, store, wal, quit))) { quit = true; break; }
        }
    }
    ::close(fd);
}
}

int main(int argc, char** argv) {
    int port = 7379;
    std::size_t workers = std::max(2u, std::thread::hardware_concurrency());
    std::string wal_path = "data/novakv.wal";
    std::size_t cache_capacity = 4096;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--port" && i+1 < argc) port = std::stoi(argv[++i]);
        else if (a == "--workers" && i+1 < argc) workers = static_cast<std::size_t>(std::stoul(argv[++i]));
        else if (a == "--wal" && i+1 < argc) wal_path = argv[++i];
        else if (a == "--cache-capacity" && i+1 < argc) cache_capacity = static_cast<std::size_t>(std::stoul(argv[++i]));
        else {
            std::cerr << "usage: novakv-server [--port N] [--workers N] [--wal path] [--cache-capacity N]\n";
            return 2;
        }
    }

    struct sigaction sa{};
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
    novakv::Store store(64, cache_capacity);
    novakv::WAL wal(wal_path);
    const auto replayed = wal.replay(store);

    int server_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { std::perror("socket"); return 1; }
    int yes = 1; ::setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK); addr.sin_port = htons(static_cast<uint16_t>(port));
    if (::bind(server_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) { std::perror("bind"); return 1; }
    if (::listen(server_fd, 256) < 0) { std::perror("listen"); return 1; }

    std::cerr << "NovaKV listening on 127.0.0.1:" << port << " workers=" << workers << " replayed=" << replayed << "\n";
    ConnectionQueue queue;
    std::vector<std::thread> pool;
    pool.reserve(workers);
    for (std::size_t i = 0; i < workers; ++i) {
        pool.emplace_back([&]{ int fd; while (queue.pop(fd)) serve_client(fd, store, wal); });
    }

    while (g_running.load() && !g_stop) {
        sockaddr_in client{}; socklen_t len = sizeof(client);
        int fd = ::accept(server_fd, reinterpret_cast<sockaddr*>(&client), &len);
        if (fd < 0) {
            if (g_stop || !g_running.load()) break;
            if (errno == EINTR) continue;
            std::perror("accept"); break;
        }
        queue.push(fd);
    }
    ::close(server_fd);
    g_running.store(false); queue.wake_all();
    for (auto& t : pool) if (t.joinable()) t.join();
    return 0;
}
