from __future__ import annotations
import argparse, json, socket, statistics, time, os, platform
from concurrent.futures import ThreadPoolExecutor


def percentile(xs, p):
    xs = sorted(xs)
    if not xs: return 0.0
    i = round((len(xs)-1)*p/100)
    return xs[max(0,min(i,len(xs)-1))]


def worker(host, port, start, count, value_size):
    lat=[]; value="x"*value_size
    with socket.create_connection((host,port),timeout=5) as s, s.makefile("rwb",buffering=0) as f:
        for i in range(start,start+count):
            key=f"bench:{i%10000}"
            command = f"SET {key} {value}" if i % 5 == 0 else f"GET {key}"
            t=time.perf_counter_ns(); f.write((command+"\n").encode()); f.flush()
            first=f.readline()
            if first.startswith(b"$") and first != b"$-1\n":
                n=int(first[1:]); f.read(n+1)
            lat.append((time.perf_counter_ns()-t)/1e6)
    return lat


def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--host",default="127.0.0.1"); ap.add_argument("--port",type=int,default=7379); ap.add_argument("--clients",type=int,default=16); ap.add_argument("--requests",type=int,default=20000); ap.add_argument("--value-size",type=int,default=64); ap.add_argument("--out"); a=ap.parse_args()
    each=a.requests//a.clients; extra=a.requests%a.clients
    jobs=[]; offset=0
    t0=time.perf_counter()
    with ThreadPoolExecutor(max_workers=a.clients) as ex:
        for c in range(a.clients):
            n=each+(1 if c<extra else 0); jobs.append(ex.submit(worker,a.host,a.port,offset,n,a.value_size)); offset+=n
        lat=[x for fut in jobs for x in fut.result()]
    elapsed=time.perf_counter()-t0
    result={"requests":len(lat),"clients":a.clients,"value_size_bytes":a.value_size,"elapsed_s":round(elapsed,4),"throughput_ops_s":round(len(lat)/elapsed,2),"latency_ms":{"mean":round(statistics.mean(lat),4),"p50":round(percentile(lat,50),4),"p95":round(percentile(lat,95),4),"p99":round(percentile(lat,99),4)},"environment":{"cpu_count":os.cpu_count(),"machine":platform.machine(),"note":"Local reference measurement; rerun on target hardware before making external performance claims."}}
    text=json.dumps(result,indent=2); print(text)
    if a.out: open(a.out,"w").write(text+"\n")
if __name__=="__main__": main()
