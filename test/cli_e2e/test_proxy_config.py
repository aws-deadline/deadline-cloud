# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
End-to-end test for the ``settings.https_proxy`` / ``settings.ca_bundle`` config
settings (feature request aws-deadline/deadline-cloud#1046).

It exercises the whole chain with the *real* ``deadline`` CLI as a subprocess:

    deadline farm list
      -> config: settings.https_proxy = http://127.0.0.1:<proxy>
                 settings.ca_bundle  = <throwaway CA>
      -> botocore routes the call to a realistic
         https://deadline.us-west-2.amazonaws.com endpoint THROUGH the proxy
      -> the CONNECT proxy redirects the tunnel to our localhost TLS mock
      -> the mock returns the seeded farm; the CLI prints it

The CLI endpoint is a realistic ``amazonaws.com`` host (NOT localhost), so the
only way the call reaches the mock is via the configured proxy. botocore also
bypasses proxies for ``localhost``/``127.0.0.1`` *targets*, which is why the
endpoint must be a non-local host. TLS is terminated by the mock using a cert
the CLI trusts only because ``settings.ca_bundle`` points at the matching CA -
so this single test covers both new settings.
"""

from __future__ import annotations

import os
import subprocess
import sys
import threading
from pathlib import Path
from typing import Iterator

import pytest

from _common.mock_deadline_backend import MockDeadlineBackend, start_server

from _constants import ACCESS_KEY, REGION, SECRET_KEY
from _proxy_helpers import (
    RedirectingConnectProxy,
    generate_ca_and_server_cert,
    server_ssl_context,
)

# The realistic endpoint the CLI is pointed at. botocore prepends a
# ``management.`` host prefix to Deadline calls; the cli_e2e sitecustomize shim
# strips it, but we add both names as SANs so TLS validation passes either way.
_DEADLINE_HOST = f"deadline.{REGION}.amazonaws.com"
_MANAGEMENT_HOST = f"management.deadline.{REGION}.amazonaws.com"
_DEADLINE_ENDPOINT = f"https://{_DEADLINE_HOST}"

# Reuse the cli_e2e shim that strips botocore's ``management.`` host prefix so
# the (proxied) request targets the host our cert is issued for.
from conftest import _SITECUSTOMIZE  # noqa: E402

# These tests stand up a threaded TLS HTTPServer + a hand-rolled CONNECT proxy
# and drive the CLI through them as a subprocess. On the Windows CI runners that
# topology fails to tear down cleanly: the job hangs near completion and is
# canceled with an orphaned python process (observed across all Windows Python
# versions on PR #1217, while Linux/macOS pass). The feature itself is
# platform-agnostic and its wiring is covered on every platform by the unit
# tests in test/unit/.../test_api_session.py; only this socket-level harness is
# Windows-incompatible, so we skip it there.
skip_on_windows = pytest.mark.skipif(
    sys.platform == "win32",
    reason="Threaded TLS-mock + CONNECT-proxy subprocess harness hangs on Windows CI; "
    "proxy/ca_bundle wiring is covered cross-platform by the unit tests.",
)


@pytest.fixture
def proxy_setup(tmp_path: Path) -> Iterator[tuple]:
    """
    Stand up the full proxy + TLS-mock topology and an isolated CLI env.

    A single throwaway CA signs the server cert the mock serves; that same CA is
    handed to the CLI via ``settings.ca_bundle`` so the two stay in sync.

    Yields ``(backend, env, proxy, ca_pem)``.
    """
    ca_pem, server_cert, server_key = generate_ca_and_server_cert(
        tmp_path, [_DEADLINE_HOST, _MANAGEMENT_HOST]
    )

    backend = MockDeadlineBackend()
    ctx = server_ssl_context(server_cert, server_key)
    server, _base_url, _ = start_server(backend, ssl_context=ctx)
    backend_port = server.server_address[1]
    # The mock backend is a single-threaded HTTPServer whose accept thread also
    # performs the (synchronous) TLS handshake inside ``socket.accept()``. A
    # half-open client connection -- e.g. a botocore connection-pool socket the
    # CONNECT proxy relayed but that never finishes a handshake -- would block
    # ``accept()`` indefinitely, wedging ``serve_forever``. Once wedged, the
    # ``server.shutdown()`` in this fixture's teardown blocks the main pytest
    # thread forever, which is exactly how this harness hung the macOS CI job
    # until it was cancelled (uniform ~6m57s, conclusion=cancelled). Bounding the
    # listening-socket handshake with a generous timeout guarantees the accept
    # thread always returns, so ``serve_forever``/``shutdown`` can never wedge.
    server.socket.settimeout(30)

    proxy = RedirectingConnectProxy("127.0.0.1", backend_port).start()

    config_file = tmp_path / "deadline.config"
    config_file.write_text("")
    shim_dir = tmp_path / "shim"
    shim_dir.mkdir()
    (shim_dir / "sitecustomize.py").write_text(_SITECUSTOMIZE)
    fake_home = tmp_path / "home"
    fake_home.mkdir()

    env = {
        **os.environ,
        "HOME": str(fake_home),
        "USERPROFILE": str(fake_home),
        "AWS_ENDPOINT_URL_DEADLINE": _DEADLINE_ENDPOINT,
        "AWS_ACCESS_KEY_ID": ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": SECRET_KEY,
        "AWS_DEFAULT_REGION": REGION,
        "DEADLINE_CONFIG_FILE_PATH": str(config_file),
        "PYTHONPATH": str(shim_dir) + os.pathsep + os.environ.get("PYTHONPATH", ""),
        # Make sure no ambient proxy / no_proxy interferes with the test.
        "NO_PROXY": "",
        "no_proxy": "",
        "HTTPS_PROXY": "",
        "https_proxy": "",
        "AWS_CA_BUNDLE": "",
        # Fail fast rather than retrying, so the negative-control case (no proxy,
        # endpoint unreachable) doesn't sit in botocore's retry loop.
        "AWS_MAX_ATTEMPTS": "1",
        "AWS_RETRY_MODE": "standard",
    }
    try:
        yield backend, env, proxy, ca_pem
    finally:
        proxy.stop()
        # ``server.shutdown()`` blocks until ``serve_forever`` observes the stop
        # flag. The socket timeout above keeps the accept thread live so this
        # returns promptly, but run it on a watchdog thread as defence in depth:
        # a wedged shutdown must never hang the main pytest thread (and thus the
        # whole CI job) -- the daemon server thread dies with the process anyway.
        shutdown_thread = threading.Thread(target=server.shutdown, daemon=True)
        shutdown_thread.start()
        shutdown_thread.join(timeout=10)
        server.server_close()


def _run(env: dict, *args: str, timeout: int = 60) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["deadline", *args], env=env, capture_output=True, text=True, timeout=timeout
    )


def _config_set(env: dict, key: str, value: str) -> None:
    r = _run(env, "config", "set", key, value)
    assert r.returncode == 0, f"config set {key} failed: {r.stderr or r.stdout}"


@skip_on_windows
@pytest.mark.timeout(120)
def test_farm_list_routes_through_configured_proxy(proxy_setup):
    """With https_proxy + ca_bundle set, the CLI reaches the mock via the proxy."""
    backend, env, proxy, ca_pem = proxy_setup

    farm = backend.create_farm(displayName="Proxied Farm")
    farm_id = farm["farmId"]

    _config_set(env, "telemetry.opt_out", "true")
    _config_set(env, "settings.https_proxy", proxy.url)
    _config_set(env, "settings.ca_bundle", str(ca_pem))
    _config_set(env, "defaults.farm_id", farm_id)

    r = _run(env, "farm", "list")

    assert r.returncode == 0, f"farm list failed: {r.stderr or r.stdout}"
    assert farm_id in r.stdout, r.stdout
    assert "Proxied Farm" in r.stdout, r.stdout
    # Proof the call genuinely traversed the proxy and targeted the real host.
    assert proxy.connect_targets, "no CONNECT reached the proxy"
    assert any(_DEADLINE_HOST in t for t in proxy.connect_targets), proxy.connect_targets


@skip_on_windows
@pytest.mark.timeout(120)
def test_farm_list_without_proxy_cannot_reach_backend(proxy_setup):
    """Negative control: without the proxy the endpoint is unreachable.

    The CA bundle is still configured, isolating the proxy as the single
    variable. The CLI should fail because the call cannot reach our mock, and
    the proxy must record no CONNECTs.

    To keep the suite fully offline, the endpoint is overridden to an
    unresolvable ``.invalid`` host (RFC 6761 reserves ``.invalid`` so it never
    resolves). Without the proxy the CLI fails at DNS resolution and never
    contacts the real AWS Deadline endpoint.
    """
    backend, env, proxy, ca_pem = proxy_setup
    env = {**env, "AWS_ENDPOINT_URL_DEADLINE": "https://deadline.us-west-2.amazonaws.invalid"}

    backend.create_farm(displayName="Proxied Farm")

    _config_set(env, "telemetry.opt_out", "true")
    _config_set(env, "settings.ca_bundle", str(ca_pem))
    # Deliberately do NOT set settings.https_proxy.

    r = _run(env, "farm", "list", timeout=60)

    assert r.returncode != 0, f"expected failure without proxy, got:\n{r.stdout}"
    assert not proxy.connect_targets, proxy.connect_targets
