import os, socket, subprocess, tempfile, time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BIN = ROOT / "build" / "novakv-server"


def read_response(f):
    line = f.readline().decode().rstrip("\n")
    if line.startswith("$") and line != "$-1":
        n = int(line[1:])
        data = f.read(n).decode()
        assert f.read(1) == b"\n"
        return (line, data)
    return line


def cmd(f, text):
    f.write((text + "\n").encode()); f.flush(); return read_response(f)


def test_tcp_protocol_and_recovery():
    port = 17379
    with tempfile.TemporaryDirectory() as td:
        wal = str(Path(td) / "test.wal")
        p = subprocess.Popen([str(BIN), "--port", str(port), "--workers", "4", "--wal", wal], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            for _ in range(50):
                try:
                    s = socket.create_connection(("127.0.0.1", port), timeout=0.2); break
                except OSError: time.sleep(0.05)
            else: raise RuntimeError("server did not start")
            with s, s.makefile("rwb", buffering=0) as f:
                assert cmd(f,"PING") == "+PONG"
                assert cmd(f,"SET name nova kv") == "+OK"
                assert cmd(f,"GET name")[1] == "nova kv"
                assert cmd(f,"SETEX temp 60 short") == "+OK"
                ttl = int(cmd(f,"TTL temp")[1:]); assert 0 <= ttl <= 60
                assert cmd(f,"SET counter 10") == "+OK"
                assert cmd(f,"INCR counter 5") == ":15"
                assert cmd(f,"DEL name") == ":1"
                assert cmd(f,"GET name") == "$-1"
                stats = cmd(f,"STATS")[1]; assert '"gets"' in stats and '"cache_hits"' in stats
                assert cmd(f,"SET persist yes") == "+OK"
                assert cmd(f,"CHECKPOINT") == "+OK"
        finally:
            p.terminate(); p.wait(timeout=3)

        p2 = subprocess.Popen([str(BIN), "--port", str(port), "--workers", "2", "--wal", wal], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            for _ in range(50):
                try:
                    s = socket.create_connection(("127.0.0.1", port), timeout=0.2); break
                except OSError: time.sleep(0.05)
            else: raise RuntimeError("restarted server did not start")
            with s, s.makefile("rwb", buffering=0) as f:
                assert cmd(f,"GET persist")[1] == "yes"
                assert cmd(f,"GET counter")[1] == "15"
        finally:
            p2.terminate(); p2.wait(timeout=3)
