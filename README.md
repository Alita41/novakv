# NovaKV

[![CI](https://github.com/Alita41/novakv/actions/workflows/ci.yml/badge.svg)](https://github.com/Alita41/novakv/actions/workflows/ci.yml)

**A concurrent C++ key-value service with TCP networking, sharded synchronization, TTL, LRU caching, and write-ahead-log recovery.**

NovaKV is a systems project built to make the storage-engine mechanics visible instead of hiding them behind an existing database. It implements a multi-client TCP service, a sharded in-memory store, expiration semantics, a bounded hot-read cache, checksummed persistence, restart recovery, checkpoint compaction, tests, and reproducible performance tooling.

## What it demonstrates

- **C++17 systems programming** with RAII and standard concurrency primitives
- **TCP networking** over a small RESP-inspired text protocol
- **Multithreaded request handling** with a fixed worker pool
- **Sharded synchronization** using `std::shared_mutex`
- **TTL expiration** persisted as absolute timestamps across restarts
- **Bounded LRU read cache** with hit/miss instrumentation
- **Binary WAL** with CRC32 checksums and partial-tail recovery
- **Checkpoint compaction** of current live state
- **Performance benchmarking** with throughput and p50/p95/p99 latency
- **Unit + TCP integration tests** and GitHub Actions CI

## Architecture

```text
clients --> TCP accept loop --> worker pool --> command dispatcher
                                              |       |       |
                                              v       v       v
                                          sharded    LRU     WAL
                                           store    cache   replay/
                                            + TTL          checkpoint
```

See [`docs/architecture.md`](docs/architecture.md) for the design and scope.

## Build

Requirements: Linux/macOS with a C++17 compiler, CMake 3.16+, Python 3.10+ for integration tests/benchmarks.

```bash
make build
```

## Run

```bash
./build/novakv-server --port 7379 --workers 8 --wal data/novakv.wal
```

The server binds to `127.0.0.1` by default.

## Protocol examples

```text
PING
+PONG

SET user:42 Talat
+OK

GET user:42
$5
Talat

SETEX session:1 5000 active
+OK

TTL session:1
:4987

INCR counter 5
:5
```

Supported commands: `PING`, `SET`, `SETEX`, `GET`, `DEL`, `EXISTS`, `TTL`, `INCR`, `STATS`, `CHECKPOINT`, and `QUIT`.

See [`docs/protocol.md`](docs/protocol.md).

## Python client

```python
from novakv_client import NovaKVClient

client = NovaKVClient()
client.set("user:42", "Talat")
print(client.get("user:42"))
client.close()
```

## Persistence behavior

Every mutating command appends a checksummed binary record to the WAL before returning. On restart, NovaKV replays valid records and stops cleanly at an incomplete/corrupt tail. `CHECKPOINT` rewrites the current live keyspace to a compact WAL.

This project intentionally does **not** claim fsync-level power-loss durability, distributed consensus, replication, or automatic failover. Those are different engineering problems and are outside this repository's current scope.

## Tests

```bash
make test
```

The tests cover:

- LRU promotion and eviction
- SET/GET/DELETE behavior
- TTL expiry
- integer increments
- concurrent multi-threaded reads/writes
- WAL replay
- checkpoint compaction
- TCP command protocol
- restart recovery through the real server process

## Benchmark

```bash
make benchmark
```

The benchmark drives mixed traffic over persistent TCP connections and reports throughput plus mean/p50/p95/p99 latency. Raw measured output is stored in `artifacts/benchmark.json`.

A reference run in the project build environment (5 reported CPUs, x86_64) used 30,000 requests, 16 concurrent clients, 64-byte values, and an 80/20 GET/SET mix:

| Metric | Measured result |
|---|---:|
| Throughput | 12,625.78 ops/s |
| Mean latency | 0.9241 ms |
| p50 latency | 0.5778 ms |
| p95 latency | 1.2444 ms |
| p99 latency | 1.6484 ms |

These are **local reference measurements, not production claims**. Rerun the benchmark on target hardware before quoting performance externally.

See [`docs/benchmarking.md`](docs/benchmarking.md) for methodology and interpretation.

## Resume-safe description

> Built NovaKV, a concurrent C++17 key-value service with TCP networking, a fixed worker pool, sharded `shared_mutex` synchronization, TTL expiry, a bounded LRU read cache, and checksummed write-ahead-log recovery; added unit/integration tests and a reproducible latency/throughput benchmark harness.

## License

MIT
