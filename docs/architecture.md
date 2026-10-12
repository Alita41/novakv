# Architecture

```text
TCP clients
    |
    v
accept loop ---> bounded-by-worker connection queue ---> worker threads
                                                    |
                                                    v
                                           command dispatcher
                                           /       |       \
                                          /        |        \
                                      sharded    LRU hot    WAL
                                      store       cache      |
                                        |                   |
                              shared_mutex/shard       restart replay
                                        |
                                      TTL expiry
```

## Concurrency

The canonical in-memory store is split across 64 shards by default. Each shard owns an `unordered_map` guarded by `std::shared_mutex`: concurrent readers can proceed together while writes take an exclusive lock only on the relevant shard. The TCP layer uses a fixed worker pool so the process does not create an unbounded thread per accepted connection.

## TTL

Each entry optionally stores an absolute Unix expiry timestamp in milliseconds. Expired keys are removed lazily on access. Absolute timestamps are persisted so TTL semantics survive a restart rather than resetting from the original duration.

## LRU cache

A bounded, mutex-protected LRU sits in front of the canonical store for hot reads. Every cache hit is revalidated against the canonical entry so an expired key cannot be served from cache. Writes and deletes update/invalidate the cache.

## Persistence

The write-ahead log stores binary, checksummed records for `SET` and `DELETE`. Replay tolerates a partially written tail by stopping at the first incomplete/corrupt record. `CHECKPOINT` compacts current live state into a replacement WAL, removing overwritten/deleted history.

The current implementation flushes the C++ output stream after every record. It does **not** claim crash-proof fsync durability against sudden power loss; that would require a platform-specific `fsync` policy and is deliberately not overstated.

## Scope

NovaKV is a single-node concurrent storage service. It does not claim consensus, automatic failover, distributed replication, or linearizable multi-node transactions.
