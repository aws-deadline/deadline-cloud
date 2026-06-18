"""Pytest plugin: run deadline-cloud-python's `test/cli_e2e/` suite against the
Rust `deadline` binary.

Loaded with `pytest -p rust_conformance_plugin` from the deadline-cloud-python
repo root, with this directory on PYTHONPATH. It does two things:

1. Host rewrite (import-time). The Rust Deadline SDK injects the `management.`
   host prefix and — unlike every other AWS SDK — exposes no way to disable it.
   `management.127.0.0.1` is not a resolvable hostname, but `management.localhost`
   resolves to 127.0.0.1 per RFC 6761. The Python mock binds to 127.0.0.1, so we
   rewrite the endpoint URL it hands out to use `localhost`. This matches the
   trick deadline-cloud-rs's own L2 test harness already uses.

   The patch runs at plugin *import* time (not in `pytest_configure`): the
   cli_e2e conftest is loaded as an *initial* conftest and binds `start_server`
   before `pytest_configure` runs, so patching later would be too late.

2. Xfail allowlist. Tests listed in `xfail_allowlist.txt` (intentional or
   pending Rust-vs-Python differences) are marked xfail so only *new* drift
   turns the run red.
"""

import os
import pathlib
import sys

import pytest

# --- 1. Host rewrite (must happen before the cli_e2e conftest imports it) ----
# cwd is the deadline-cloud-python repo root when pytest runs.
sys.path.insert(0, os.path.abspath("test"))

import _common.mock_deadline_backend as _mock  # noqa: E402

if not getattr(_mock.start_server, "_rust_patched", False):
    _orig_start_server = _mock.start_server

    def _patched_start_server(backend, port=0):
        server, url, thread = _orig_start_server(backend, port)
        return server, url.replace("127.0.0.1", "localhost"), thread

    _patched_start_server._rust_patched = True
    _mock.start_server = _patched_start_server


# --- 2. Xfail allowlist ------------------------------------------------------
_ALLOWLIST_PATH = pathlib.Path(
    os.environ.get(
        "RUST_CONFORMANCE_XFAIL",
        str(pathlib.Path(__file__).parent / "xfail_allowlist.txt"),
    )
)


def _load_allowlist() -> dict[str, str]:
    """Map nodeid-substring -> reason. Lines are `<nodeid>  # reason`."""
    entries: dict[str, str] = {}
    if not _ALLOWLIST_PATH.exists():
        return entries
    for raw in _ALLOWLIST_PATH.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        nodeid, _, reason = line.partition("#")
        nodeid = nodeid.strip()
        if nodeid:
            entries[nodeid] = reason.strip() or "known Rust/Python CLI difference"
    return entries


def pytest_collection_modifyitems(config, items):
    entries = _load_allowlist()
    if not entries:
        return
    for item in items:
        for nodeid_substr, reason in entries.items():
            if nodeid_substr in item.nodeid:
                item.add_marker(pytest.mark.xfail(reason=reason, strict=False))
                break
