# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Helpers for the proxy / CA-bundle end-to-end test (``test_proxy_config.py``).

Provides:
  * ``generate_ca_and_server_cert`` - mint a throwaway CA + server cert with
    ``cryptography`` (already available transitively via ``moto[server]``), so
    no test-only dependency is added.
  * ``RedirectingConnectProxy`` - an in-process HTTP CONNECT proxy that records
    every ``CONNECT`` it receives and tunnels the bytes to a *fixed* backend
    address, ignoring the host the client asked for. That redirect is what lets
    a CLI configured with a realistic ``https://deadline.<region>.amazonaws.com``
    endpoint actually reach our localhost mock - the traffic only gets there by
    going through the proxy, which is exactly the behavior under test.
"""

from __future__ import annotations

import socket
import ssl
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional, Tuple

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID


def generate_ca_and_server_cert(out_dir: Path, hostnames: List[str]) -> Tuple[Path, Path, Path]:
    """Mint a self-signed CA and a server cert/key signed by it.

    The server cert carries ``hostnames`` as Subject Alternative Names so a TLS
    client validating against the CA accepts a connection to any of them.

    Returns ``(ca_pem, server_cert_pem, server_key_pem)`` file paths.
    """
    now = datetime.now(timezone.utc)

    # --- Certificate authority ---
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "deadline-cloud test CA")])
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(ca_subject)
        .issuer_name(ca_subject)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=1))
        .not_valid_after(now + timedelta(days=2))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(ca_key, hashes.SHA256())
    )

    # --- Server cert signed by the CA ---
    server_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    server_cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, hostnames[0])]))
        .issuer_name(ca_subject)
        .public_key(server_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=1))
        .not_valid_after(now + timedelta(days=2))
        .add_extension(
            x509.SubjectAlternativeName([x509.DNSName(h) for h in hostnames]), critical=False
        )
        .sign(ca_key, hashes.SHA256())
    )

    ca_pem = out_dir / "ca.pem"
    server_cert_pem = out_dir / "server_cert.pem"
    server_key_pem = out_dir / "server_key.pem"

    ca_pem.write_bytes(ca_cert.public_bytes(serialization.Encoding.PEM))
    server_cert_pem.write_bytes(server_cert.public_bytes(serialization.Encoding.PEM))
    server_key_pem.write_bytes(
        server_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    return ca_pem, server_cert_pem, server_key_pem


def server_ssl_context(server_cert_pem: Path, server_key_pem: Path) -> ssl.SSLContext:
    """Build a server-side SSLContext from a cert/key pair."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    # Pin a modern floor; the default context would otherwise still permit the
    # long-deprecated TLSv1/TLSv1.1 handshakes.
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.load_cert_chain(certfile=str(server_cert_pem), keyfile=str(server_key_pem))
    return ctx


class RedirectingConnectProxy:
    """HTTP CONNECT proxy that tunnels every connection to a fixed backend.

    Listens on ``127.0.0.1:<ephemeral>``. For each ``CONNECT host:port`` request
    it records the requested target (proof the client routed through the proxy),
    answers ``200 Connection Established``, then blind-relays bytes to
    ``(target_host, target_port)`` regardless of what the client asked for.
    """

    def __init__(self, target_host: str, target_port: int):
        self._target = (target_host, target_port)
        self.connect_targets: List[str] = []
        self._lock = threading.Lock()
        self._server: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None
        self._stop = False

    @property
    def port(self) -> int:
        assert self._server is not None
        return self._server.getsockname()[1]

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self) -> "RedirectingConnectProxy":
        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server.bind(("127.0.0.1", 0))
        self._server.listen(16)
        self._server.settimeout(0.5)
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop = True
        if self._thread:
            self._thread.join(timeout=5)
        if self._server:
            self._server.close()

    def _accept_loop(self) -> None:
        assert self._server is not None
        while not self._stop:
            try:
                client, _ = self._server.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self._handle, args=(client,), daemon=True).start()

    def _handle(self, client: socket.socket) -> None:
        remote = None
        try:
            data = client.recv(8192)
            if not data:
                return
            line = data.split(b"\r\n", 1)[0].decode("latin-1")
            parts = line.split()
            if len(parts) < 2 or parts[0].upper() != "CONNECT":
                client.sendall(b"HTTP/1.1 405 Method Not Allowed\r\n\r\n")
                return
            with self._lock:
                self.connect_targets.append(parts[1])

            # Redirect: ignore the requested host, tunnel to our fixed backend.
            remote = socket.create_connection(self._target, timeout=10)
            client.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            self._relay(client, remote, lambda: self._stop)
        except Exception:
            # Best-effort test proxy: a malformed request or a peer that hangs
            # up mid-tunnel should just drop this connection, never crash the
            # accept loop. The test asserts on recorded CONNECTs / CLI output,
            # not on per-connection errors here.
            pass
        finally:
            for s in (client, remote):
                if s is not None:
                    try:
                        s.close()
                    except Exception:
                        # Socket already closed / errored during teardown -
                        # nothing left to do.
                        pass

    @staticmethod
    def _relay(a: socket.socket, b: socket.socket, stopped) -> None:
        import select

        a.setblocking(False)
        b.setblocking(False)
        # Poll on a short interval so the relay re-checks ``stopped()`` and exits
        # promptly at teardown instead of sitting in a long ``select`` after the
        # accept loop has stopped (which would leak this thread + its sockets).
        while not stopped():
            r, _, _ = select.select([a, b], [], [], 1)
            if not r:
                continue
            for s in r:
                try:
                    chunk = s.recv(65536)
                except (OSError, ConnectionError):
                    return
                if not chunk:
                    return
                dest = b if s is a else a
                try:
                    dest.setblocking(True)
                    dest.sendall(chunk)
                    dest.setblocking(False)
                except (OSError, ConnectionError):
                    return


class TLSInterceptConnectProxy:
    """HTTP CONNECT proxy that *terminates* TLS and forwards plaintext to a fixed backend.

    Models a TLS-intercepting corporate proxy -- the exact scenario ``settings.ca_bundle``
    exists for. For each ``CONNECT host:port`` it records the requested target (proof the
    client routed through the proxy), answers ``200 Connection Established``, performs the
    server-side TLS handshake *itself* using ``ssl_context`` (serving a cert the client
    trusts only because ``settings.ca_bundle`` points at the signing CA), then relays the
    decrypted HTTP to a fixed plaintext backend (e.g. a moto S3 server), ignoring the host
    the client asked for.

    This lets a client configured with a realistic ``https://s3.<region>.amazonaws.com``
    endpoint actually reach a localhost moto server -- the traffic only gets there by
    going through the proxy and trusting its CA, which is the behavior under test.
    """

    def __init__(self, target_host: str, target_port: int, ssl_context: ssl.SSLContext):
        self._target = (target_host, target_port)
        self._ssl_context = ssl_context
        self.connect_targets: List[str] = []
        self._lock = threading.Lock()
        self._server: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None
        self._stop = False

    @property
    def port(self) -> int:
        assert self._server is not None
        return self._server.getsockname()[1]

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self) -> "TLSInterceptConnectProxy":
        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server.bind(("127.0.0.1", 0))
        self._server.listen(16)
        self._server.settimeout(0.5)
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop = True
        if self._thread:
            self._thread.join(timeout=5)
        if self._server:
            self._server.close()

    def _accept_loop(self) -> None:
        assert self._server is not None
        while not self._stop:
            try:
                client, _ = self._server.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self._handle, args=(client,), daemon=True).start()

    def _handle(self, client: socket.socket) -> None:
        tls_client = None
        remote = None
        try:
            # Read the CONNECT request line + headers (terminated by a blank line). The
            # client waits for our 200 before starting TLS, so nothing past the headers
            # is on the wire yet -- we won't accidentally swallow the TLS ClientHello.
            buf = b""
            while b"\r\n\r\n" not in buf:
                chunk = client.recv(4096)
                if not chunk:
                    return
                buf += chunk
            line = buf.split(b"\r\n", 1)[0].decode("latin-1")
            parts = line.split()
            if len(parts) < 2 or parts[0].upper() != "CONNECT":
                client.sendall(b"HTTP/1.1 405 Method Not Allowed\r\n\r\n")
                return
            with self._lock:
                self.connect_targets.append(parts[1])

            client.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")

            # Terminate TLS here (the intercept), then forward decrypted bytes to the
            # fixed plaintext backend, ignoring the host the client asked for.
            tls_client = self._ssl_context.wrap_socket(client, server_side=True)
            remote = socket.create_connection(self._target, timeout=10)
            # Bound every blocking ``recv`` in the pump threads. Without this a
            # half-open peer (common when the CLI subprocess leaves a pooled
            # connection open at teardown) leaves a pump blocked on ``recv``
            # forever; the pump never observes ``_stop`` and the connection's
            # sockets stay open, leaking threads/fds across the run. A read
            # timeout lets each pump wake, re-check ``_stop``, and exit.
            tls_client.settimeout(5)
            remote.settimeout(5)
            self._relay(tls_client, remote, lambda: self._stop)
        except Exception:
            # Best-effort test proxy: a malformed request, a TLS handshake failure, or a
            # peer that hangs up mid-tunnel should drop this connection, never crash the
            # accept loop. The test asserts on recorded CONNECTs / CLI output.
            pass
        finally:
            for s in (tls_client, client, remote):
                if s is not None:
                    try:
                        s.close()
                    except Exception:
                        pass

    @staticmethod
    def _relay(tls_sock: ssl.SSLSocket, plain_sock: socket.socket, stopped) -> None:
        # SSLSocket buffers records internally, which doesn't compose with select(), so
        # use two simple blocking pump threads (one per direction) instead. When either
        # side closes, shut both down so the other pump unblocks and exits. Both sockets
        # carry a read timeout, so a pump that would otherwise block forever on a
        # half-open peer instead wakes periodically to re-check ``stopped()`` and the
        # peer's liveness, guaranteeing it terminates at teardown.
        def pump(src, dst):
            try:
                while not stopped():
                    try:
                        data = src.recv(65536)
                    except socket.timeout:
                        continue
                    if not data:
                        break
                    dst.sendall(data)
            except (OSError, ConnectionError, ssl.SSLError):
                pass
            finally:
                for s in (src, dst):
                    try:
                        s.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass

        t1 = threading.Thread(target=pump, args=(tls_sock, plain_sock), daemon=True)
        t2 = threading.Thread(target=pump, args=(plain_sock, tls_sock), daemon=True)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
