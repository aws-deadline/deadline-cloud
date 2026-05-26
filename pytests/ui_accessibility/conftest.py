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
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from _common.mock_deadline_backend import MockDeadlineBackend, start_server  # noqa: E402
from helpers import SAMPLE_TEMPLATE, SubmitterDialog, reap_all  # noqa: E402


@pytest.fixture(autouse=True)
def _reap_ui_subprocesses() -> Iterator[None]:
    """Kill any GUI subprocesses still alive at test end."""
    yield
    reap_all()


# ---------------------------------------------------------------------------
# Subprocess shim
# ---------------------------------------------------------------------------
# The Rust binary reads AWS_ENDPOINT_URL_DEADLINE directly (no botocore
# host-prefix patching needed). Qt handles SIGTERM natively on POSIX.
# We keep a minimal sitecustomize only for the edge case where Python
# tests also exercise the Python GUI (not the primary path).


# ---------------------------------------------------------------------------
# Mock backend fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def _mock_backend_server() -> Iterator[tuple[MockDeadlineBackend, str]]:
    """Session-scoped MockDeadlineBackend + HTTP server."""
    backend = MockDeadlineBackend()
    server, base_url, _ = start_server(backend)
    # Use localhost instead of 127.0.0.1 so the Rust SDK's "management."
    # host-prefix resolves correctly (management.localhost → 127.0.0.1).
    base_url = base_url.replace("127.0.0.1", "localhost")
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
        )

    env["_JOB_HISTORY_DIR"] = str(job_history_dir)
    return env


@pytest.fixture
def gui_submit(bundle_dir, submitter_env) -> Generator[SubmitterDialog, None, None]:
    """Open the submitter dialog with farm/queue resolved."""
    with SubmitterDialog.open(bundle_dir, env=submitter_env) as app:
        app.wait_farm_resolved()
        yield app
