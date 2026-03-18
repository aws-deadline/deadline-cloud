# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Minimal HTTP CONNECT proxy listening on a Unix domain socket."""

import json
import os
import select
import socket
import sys
import threading
from typing import Dict, Optional


class ConnectProxy:
    """HTTP CONNECT proxy that listens on a Unix domain socket and tunnels TCP."""

    def __init__(self, sock_path):
        self.sock_path = sock_path
        self.connection_count = 0
        self.bytes_relayed = 0
        self.hosts: Dict[str, int] = {}
        self._lock = threading.Lock()
        self._server: Optional[socket.socket] = None
        self._stats_server: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None
        self._stop = False

    def start(self):
        if os.path.exists(self.sock_path):
            os.unlink(self.sock_path)
        self._server = socket.socket(getattr(socket, "AF_UNIX"), socket.SOCK_STREAM)
        self._server.bind(self.sock_path)
        self._server.listen(64)
        self._server.settimeout(1.0)
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()

        # Stats socket for reporting
        stats_path = self.sock_path.replace(".sock", "_stats.sock")
        if os.path.exists(stats_path):
            os.unlink(stats_path)
        self._stats_server = socket.socket(getattr(socket, "AF_UNIX"), socket.SOCK_STREAM)
        self._stats_server.bind(stats_path)
        self._stats_server.listen(4)
        self._stats_server.settimeout(1.0)
        threading.Thread(target=self._stats_loop, daemon=True).start()

    def stop(self):
        self._stop = True
        if self._thread:
            self._thread.join(timeout=5)
        if self._server:
            self._server.close()
        if self._stats_server:
            self._stats_server.close()

    def _stats_loop(self):
        assert self._stats_server is not None
        while not self._stop:
            try:
                client, _ = self._stats_server.accept()
            except socket.timeout:
                continue
            with self._lock:
                stats = {
                    "connection_count": self.connection_count,
                    "bytes_relayed": self.bytes_relayed,
                    "hosts": dict(self.hosts),
                }
            client.sendall(json.dumps(stats).encode())
            client.close()

    def _accept_loop(self):
        assert self._server is not None
        while not self._stop:
            try:
                client, _ = self._server.accept()
            except socket.timeout:
                continue
            threading.Thread(target=self._handle, args=(client,), daemon=True).start()

    def _handle(self, client):
        remote = None
        try:
            data = client.recv(4096)
            if not data:
                return
            line = data.split(b"\r\n")[0].decode()
            parts = line.split()
            if len(parts) < 2 or parts[0] != "CONNECT":
                client.sendall(b"HTTP/1.1 400 Bad Request\r\n\r\n")
                return
            target = parts[1]
            host, port = target.rsplit(":", 1)
            port = int(port)

            remote = socket.create_connection((host, port), timeout=30)
            client.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            with self._lock:
                self.connection_count += 1
                self.hosts[target] = self.hosts.get(target, 0) + 1
            relayed = self._relay(client, remote)
            with self._lock:
                self.bytes_relayed += relayed
        except Exception:
            pass
        finally:
            try:
                client.close()
            except Exception:
                pass
            if remote:
                try:
                    remote.close()
                except Exception:
                    pass

    @staticmethod
    def _relay(a, b) -> int:
        """Bidirectional byte relay. Returns total bytes relayed."""
        total = 0
        a.setblocking(False)
        b.setblocking(False)
        while True:
            r, _, _ = select.select([a, b], [], [], 60)
            if not r:
                break
            for s in r:
                try:
                    data = s.recv(65536)
                except (OSError, ConnectionError):
                    return total
                if not data:
                    return total
                dest = b if s is a else a
                try:
                    dest.setblocking(True)
                    dest.sendall(data)
                    dest.setblocking(False)
                    total += len(data)
                except (OSError, ConnectionError):
                    return total
        return total


if __name__ == "__main__":
    sock_path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/deadline_proxy.sock"
    proxy = ConnectProxy(sock_path)
    proxy.start()
    print(f"CONNECT proxy listening on {sock_path}", flush=True)
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        proxy.stop()
