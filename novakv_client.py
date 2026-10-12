from __future__ import annotations
import socket

class NovaKVClient:
    def __init__(self, host="127.0.0.1", port=7379, timeout=5):
        self.sock=socket.create_connection((host,port),timeout=timeout)
        self.file=self.sock.makefile("rwb",buffering=0)
    def close(self): self.file.close(); self.sock.close()
    def command(self, text):
        self.file.write((text+"\n").encode()); self.file.flush(); line=self.file.readline().decode().rstrip("\n")
        if line.startswith("$") and line != "$-1":
            n=int(line[1:]); data=self.file.read(n).decode(); self.file.read(1); return data
        if line == "$-1": return None
        if line.startswith(":"): return int(line[1:])
        if line.startswith("+"): return line[1:]
        if line.startswith("-ERR"): raise RuntimeError(line[5:])
        return line
    def get(self,key): return self.command(f"GET {key}")
    def set(self,key,value): return self.command(f"SET {key} {value}")
    def setex(self,key,ttl_ms,value): return self.command(f"SETEX {key} {ttl_ms} {value}")
    def delete(self,key): return self.command(f"DEL {key}")
