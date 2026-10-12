#include "novakv/lru_cache.hpp"
#include "novakv/store.hpp"
#include "novakv/wal.hpp"

#include <cassert>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <thread>
#include <vector>

int main() {
    {
        novakv::LRUCache c(2);
        c.put("a", "1"); c.put("b", "2");
        assert(c.get("a").value() == "1");
        c.put("c", "3");
        assert(!c.get("b"));
        assert(c.get("a").value() == "1");
        assert(c.get("c").value() == "3");
    }
    {
        novakv::Store s(16, 8);
        s.set("hello", "world");
        assert(s.get("hello").value() == "world");
        s.set("temp", "x", 30);
        std::this_thread::sleep_for(std::chrono::milliseconds(45));
        assert(!s.get("temp"));
        s.set("n", "41");
        assert(s.incr("n", 1).value() == 42);
        assert(s.get("n").value() == "42");
        assert(!s.incr("hello", 1));
    }
    {
        novakv::Store s(64, 64);
        constexpr int threads = 8;
        constexpr int per = 1000;
        std::vector<std::thread> pool;
        for (int t=0;t<threads;++t) {
            pool.emplace_back([&,t]{
                for (int i=0;i<per;++i) {
                    const auto key = "k" + std::to_string(t) + ":" + std::to_string(i);
                    s.set(key, std::to_string(i));
                    assert(s.get(key).has_value());
                }
            });
        }
        for (auto& th : pool) th.join();
        assert(s.stats().live_keys == static_cast<std::size_t>(threads*per));
    }
    {
        auto path = std::filesystem::temp_directory_path() / "novakv_test.wal";
        std::filesystem::remove(path);
        {
            novakv::Store s;
            novakv::WAL wal(path.string());
            s.set("persist", "yes"); wal.append_set("persist", "yes", 0);
            s.set("gone", "x"); wal.append_set("gone", "x", 0);
            s.erase("gone"); wal.append_delete("gone");
            const auto expiry = novakv::Store::now_ms() + 5000;
            s.set_absolute("ttl", "live", expiry); wal.append_set("ttl", "live", expiry);
        }
        {
            novakv::Store recovered;
            novakv::WAL wal(path.string());
            auto n = wal.replay(recovered);
            assert(n == 4);
            assert(recovered.get("persist").value() == "yes");
            assert(!recovered.get("gone"));
            assert(recovered.get("ttl").value() == "live");
            assert(recovered.ttl_ms("ttl") > 0);
            wal.checkpoint(recovered);
        }
        {
            novakv::Store recovered;
            novakv::WAL wal(path.string());
            auto n = wal.replay(recovered);
            assert(n == 2);
            assert(recovered.get("persist").value() == "yes");
            assert(recovered.get("ttl").value() == "live");
        }
        std::filesystem::remove(path);
    }
    std::cout << "all core tests passed\n";
}
