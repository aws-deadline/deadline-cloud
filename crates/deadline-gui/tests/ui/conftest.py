# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Shared fixtures for ``crates/deadline-gui/tests/ui/`` — launches the
gui-test-harness binary (NOT the CLI) pointed at a MockDeadlineBackend
and drives it through the accessibility tree via xa11y.

This tests the GUI crate in isolation from deadline-cli.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Generator, Iterator

import pytest

# Add test/_common to path for MockDeadlineBackend
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT / "test" / "_common"))
sys.path.insert(0, str(_REPO_ROOT / "test" / "ui"))

from mock_deadline_backend import MockDeadlineBackend, start_server  # noqa: E402
from helpers import DeadlineApp, reap_all, SAMPLE_TEMPLATE  # noqa: E402


def _harness_binary() -> str:
    """Locate the gui-test-harness binary from cargo build output."""
    for profile in ("debug", "release"):
        candidate = _REPO_ROOT / "target" / profile / "gui-test-harness"
        if candidate.exists():
            return str(candidate)
    raise FileNotFoundError(
        "gui-test-harness binary not found. Run: cargo build -p deadline-gui"
    )


@pytest.fixture(autouse=True)
def _reap_ui_subprocesses() -> Iterator[None]:
    """Kill any GUI subprocesses still alive at test end."""
    yield
    reap_all()


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
def gui_env(tmp_path: Path, mock_backend) -> tuple[MockDeadlineBackend, dict]:
    """Env vars pointing the harness at the mock backend with isolated config."""
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


@pytest.fixture
def seeded_env(gui_env, tmp_path) -> dict:
    """Seed a farm + queue and point the config at them."""
    backend, env = gui_env
    farm = backend.create_farm(displayName="TestFarm", description="")
    queue = backend.create_queue(
        farmId=farm["farmId"], displayName="TestQueue", description=""
    )

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
        )
    return env


@pytest.fixture
def bundle_dir(tmp_path) -> str:
    """Create a minimal job bundle directory."""
    d = tmp_path / "bundle"
    d.mkdir()
    (d / "template.json").write_text(json.dumps(SAMPLE_TEMPLATE))
    return str(d)


class GUIHarness(DeadlineApp):
    """Page object for the gui-test-harness binary."""

    DIALOG = "AWS Deadline Cloud workstation configuration"

    @classmethod
    def open_config(cls, env: dict) -> "GUIHarness":
        """Launch the config dialog via the harness."""
        return cls.launch([_harness_binary(), "config"], env=env)

    @classmethod
    def open_submit(
        cls, bundle_dir: str, env: dict, extra_params: dict | None = None
    ) -> "GUIHarness":
        """Launch the submit dialog via the harness."""
        params = {"job_bundle_dir": bundle_dir}
        if extra_params:
            params.update(extra_params)
        args = [
            _harness_binary(),
            "submit",
            "--params-json",
            json.dumps(params),
        ]
        # Dialog name will be set once SubmitDialog is implemented
        return cls.launch(args, env=env, dialog_name="Submit to AWS Deadline Cloud")


@pytest.fixture
def config_dialog(seeded_env) -> Generator[GUIHarness, None, None]:
    """Open the config dialog via the harness."""
    with GUIHarness.open_config(env=seeded_env) as app:
        yield app
