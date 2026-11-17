# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Shared fixtures for ``test/ui/`` — launches the real ``deadline`` GUI as a
subprocess pointed at an in-process MockDeadlineBackend and drives it
through the accessibility tree via xa11y.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Generator, Iterator

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _common.mock_deadline_backend import MockDeadlineBackend, start_server  # noqa: E402
from helpers import SAMPLE_TEMPLATE, SubmitterDialog, reap_all, warm_up_gui  # noqa: E402


@pytest.fixture(autouse=True)
def _reap_ui_subprocesses() -> Iterator[None]:
    """Kill any GUI subprocesses still alive at test end."""
    yield
    reap_all()


@pytest.fixture(scope="session", autouse=True)
def _warm_accessibility_bridge(_mock_backend_server, tmp_path_factory) -> None:
    """Warm the accessibility bridge + GUI process caches once per session.

    Runs before any test (autouse) so the one-time cold-start cost of bringing
    up the platform a11y bridge and the first PySide6 subprocess is paid in
    session setup, not absorbed by — and flaking — the first test. See
    ``helpers.warm_up_gui``.
    """
    _, deadline_url = _mock_backend_server

    warm_dir = tmp_path_factory.mktemp("warmup")
    config_file = warm_dir / "deadline.config"
    config_file.write_text("")
    shim_dir = warm_dir / "shim"
    shim_dir.mkdir()
    (shim_dir / "sitecustomize.py").write_text(_SITECUSTOMIZE)
    fake_home = warm_dir / "home"
    fake_home.mkdir()

    env = {
        **os.environ,
        "HOME": str(fake_home),
        "AWS_ENDPOINT_URL_DEADLINE": deadline_url,
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_DEFAULT_REGION": "us-west-2",
        "DEADLINE_CONFIG_FILE_PATH": str(config_file),
        "DEADLINE_CLOUD_TELEMETRY_OPT_OUT": "true",
        "PYTHONUNBUFFERED": "1",
        "PYTHONPATH": str(shim_dir) + os.pathsep + os.environ.get("PYTHONPATH", ""),
    }
    warm_up_gui(env)


# ---------------------------------------------------------------------------
# Subprocess shim — injected via PYTHONPATH/sitecustomize.py
# ---------------------------------------------------------------------------
# Two responsibilities:
#
#   1. Strip botocore's ``management.`` host-prefix so the CLI subprocess
#      talks directly to 127.0.0.1 (mirrors ``test/cli_e2e/conftest.py``).
#   2. Install a POSIX ``SIGTERM`` handler that calls
#      ``QApplication.quit()`` so tests can ask a running ``deadline``
#      subprocess to unwind cleanly (run ``_print_response``, flush
#      stdout) before exiting. A no-op QTimer ensures Python's signal
#      handler fires inside Qt's C++ event loop.
#
# Telemetry is disabled via DEADLINE_CLOUD_TELEMETRY_OPT_OUT=true in the
# env (see ``deadline_env`` below), so no telemetry patching is needed.
_SITECUSTOMIZE = """
import botocore.awsrequest as _ar
_orig_urljoin = _ar._urljoin
def _urljoin(endpoint_url, url_path, host_prefix):
    return _orig_urljoin(endpoint_url, url_path, None)
_ar._urljoin = _urljoin

import signal as _signal


def _on_sigterm(signum, frame):
    try:
        from qtpy.QtWidgets import QApplication as _QApp
        _inst = _QApp.instance()
        if _inst is not None:
            _inst.quit()
            return
    except Exception:
        pass
    import sys as _sys
    _sys.exit(0)


try:
    _signal.signal(_signal.SIGTERM, _on_sigterm)
except (ValueError, OSError):
    pass

# Qt's event loop blocks in C++, so Python signal handlers only fire at
# bytecode boundaries. A no-op QTimer gives Python a regular chance to
# run pending signal handlers inside app.exec(). Deferred via
# sys.meta_path so non-GUI subprocesses don't pay the Qt import cost.
import sys as _sys


class _QtPyPostImportPatcher:
    def find_spec(self, fullname, path, target=None):
        if fullname != "qtpy.QtWidgets":
            return None
        try:
            _sys.meta_path.remove(self)
        except ValueError:
            return None
        try:
            from qtpy import QtCore as _qc
            from qtpy import QtWidgets as _qw
        except ImportError:
            return None

        _orig_qa_init = _qw.QApplication.__init__

        def _qa_init(self, *args, **kwargs):
            _orig_qa_init(self, *args, **kwargs)
            self._sigterm_pulse = _qc.QTimer(self)
            self._sigterm_pulse.setInterval(100)
            self._sigterm_pulse.timeout.connect(lambda: None)
            self._sigterm_pulse.start()

        _qw.QApplication.__init__ = _qa_init
        return None


_sys.meta_path.insert(0, _QtPyPostImportPatcher())
"""


# ---------------------------------------------------------------------------
# Mock backend fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def _mock_backend_server() -> Iterator[tuple[MockDeadlineBackend, str]]:
    """Session-scoped MockDeadlineBackend + HTTP server."""
    backend = MockDeadlineBackend()
    server, base_url, _ = start_server(backend)
    try:
        yield backend, base_url
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture
def mock_backend(_mock_backend_server) -> Iterator[tuple[MockDeadlineBackend, str]]:
    """Per-test backend that clears state between tests."""
    backend, base_url = _mock_backend_server
    backend.clear()
    yield backend, base_url


@pytest.fixture
def deadline_env(tmp_path: Path, mock_backend) -> tuple[MockDeadlineBackend, dict]:
    """Env vars pointing the GUI subprocess at the mock backend with an
    isolated HOME and config file. Returns ``(backend, env)``."""
    backend, deadline_url = mock_backend

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
        "AWS_ENDPOINT_URL_DEADLINE": deadline_url,
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_DEFAULT_REGION": "us-west-2",
        "DEADLINE_CONFIG_FILE_PATH": str(config_file),
        "DEADLINE_CLOUD_TELEMETRY_OPT_OUT": "true",
        "PYTHONUNBUFFERED": "1",
        "PYTHONPATH": str(shim_dir) + os.pathsep + os.environ.get("PYTHONPATH", ""),
    }
    return backend, env


# ---------------------------------------------------------------------------
# Shared submitter fixtures (used by test_bundle_gui_submit*.py)
# ---------------------------------------------------------------------------


@pytest.fixture
def bundle_dir(tmp_path) -> str:
    """Create a minimal job bundle directory."""
    d = tmp_path / "bundle"
    d.mkdir()
    (d / "template.json").write_text(json.dumps(SAMPLE_TEMPLATE))
    return str(d)


@pytest.fixture
def submitter_env(deadline_env, tmp_path) -> dict:
    """Seed a farm + queue and point the deadline config at them."""
    backend, env = deadline_env
    farm = backend.create_farm(displayName="TestFarm", description="")
    queue = backend.create_queue(farmId=farm["farmId"], displayName="TestQueue", description="")

    job_history_dir = tmp_path / "job_history"
    export_dir = tmp_path / "exported_bundles"
    export_dir.mkdir()
    config = env["DEADLINE_CONFIG_FILE_PATH"]
    with open(config, "w") as f:
        f.write(
            "[defaults]\n"
            "aws_profile_name = (default)\n"
            "\n"
            "[profile-(default) defaults]\n"
            f"farm_id = {farm['farmId']}\n"
            "\n"
            f"[profile-(default) {farm['farmId']} defaults]\n"
            f"queue_id = {queue['queueId']}\n"
            "\n"
            "[profile-(default) settings]\n"
            f"job_history_dir = {job_history_dir}\n"
            f"job_bundle_default_directory = {export_dir.as_posix()}\n"
        )

    env["_JOB_HISTORY_DIR"] = str(job_history_dir)
    env["_EXPORT_DIR"] = str(export_dir)
    return env


@pytest.fixture
def gui_submit(bundle_dir, submitter_env) -> Generator[SubmitterDialog, None, None]:
    """Open the submitter dialog with farm/queue resolved."""
    with SubmitterDialog.open(bundle_dir, env=submitter_env) as app:
        app.wait_farm_resolved()
        yield app
