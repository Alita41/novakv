# Benchmarking

Run a local server and benchmark mixed traffic:

```bash
make build
./build/novakv-server --port 7379 --workers 8 --wal data/benchmark.wal
python benchmarks/benchmark.py --requests 20000 --clients 16 --value-size 64
```

The benchmark uses persistent TCP connections with an 80/20 GET/SET mix over a 10,000-key working set and records throughput plus mean/p50/p95/p99 client-observed latency.

Numbers are hardware- and configuration-specific. Commit raw JSON output under `artifacts/` only when it was actually measured; do not convert a local result into a production claim.
