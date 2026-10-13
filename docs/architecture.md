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

Each WAL record is flushed through the C++ output stream. Because the implementation does not call `fsync`, it cannot guarantee that recent writes survive sudden power loss.

## Scope

NovaKV is a single-node concurrent service. Replication, consensus, failover, and multi-node transactions are outside its scope.

## Mutation ordering

A server-wide mutex serializes write commands and checkpoint creation. This maintains a coherent order between in-memory updates and WAL records; read commands still run concurrently through shared shard locks. The WAL is flushed, not fsynced, so it does not guarantee durability after sudden power loss.
