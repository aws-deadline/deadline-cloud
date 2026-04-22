# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Subprocess-based tests for ``deadline fleet`` and ``deadline worker`` CLI commands.

Runs the real `deadline` executable against a local HTTP server that speaks the
Deadline Cloud REST protocol, exercising credential handling, HTTP, and output
formatting without any mocks in the CLI process itself.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Iterator

import pytest

from ..mock_deadline_backend import MockDeadlineBackend, start_server

# Python injects this into every child interpreter's path. It patches botocore
# so the `management.` host-prefix (applied to all Deadline API calls) is
# skipped and the client talks directly to our 127.0.0.1 mock server.
_SITECUSTOMIZE = """
import botocore.awsrequest as _ar
_orig = _ar._urljoin
def _urljoin(endpoint_url, url_path, host_prefix):
    return _orig(endpoint_url, url_path, None)
_ar._urljoin = _urljoin
"""

_FLEET_KWARGS = {
    "roleArn": "arn:aws:iam::000000000000:role/mock",
    "maxWorkerCount": 10,
    "configuration": {
        "customerManaged": {
            "mode": "NO_SCALING",
            "workerCapabilities": {
                "vCpuCount": {"min": 1},
                "memoryMiB": {"min": 1024},
                "osFamily": "LINUX",
                "cpuArchitectureType": "x86_64",
            },
        }
    },
}


@pytest.fixture
def mock_server() -> Iterator[tuple[MockDeadlineBackend, str]]:
    backend = MockDeadlineBackend()
    server, base_url, _ = start_server(backend)
    try:
        yield backend, base_url
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture
def deadline_env(mock_server, tmp_path: Path) -> tuple[MockDeadlineBackend, dict]:
    backend, base_url = mock_server
    config_path = tmp_path / "deadline_config"
    config_path.write_text("")
    shim_dir = tmp_path / "shim"
    shim_dir.mkdir()
    (shim_dir / "sitecustomize.py").write_text(_SITECUSTOMIZE)
    env = {
        **os.environ,
        "AWS_ENDPOINT_URL_DEADLINE": base_url,
        "AWS_ACCESS_KEY_ID": "test",
        "AWS_SECRET_ACCESS_KEY": "test",
        "AWS_DEFAULT_REGION": "us-west-2",
        "DEADLINE_CONFIG_FILE_PATH": str(config_path),
        "PYTHONPATH": str(shim_dir) + os.pathsep + os.environ.get("PYTHONPATH", ""),
    }
    return backend, env


def _run_deadline(env: dict, *args: str) -> subprocess.CompletedProcess:
    result = subprocess.run(
        ["deadline", *args], env=env, capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, f"deadline {args} failed:\n{result.stdout}\n{result.stderr}"
    return result


def _seed_farm_fleet_worker(backend: MockDeadlineBackend) -> tuple[str, str, str]:
    farm = backend.create_farm(displayName="Test Farm")
    fleet = backend.create_fleet(farmId=farm["farmId"], displayName="Test Fleet", **_FLEET_KWARGS)
    worker = backend.create_worker(farmId=farm["farmId"], fleetId=fleet["fleetId"])
    return farm["farmId"], fleet["fleetId"], worker["workerId"]


def test_fleet_list(deadline_env):
    backend, env = deadline_env
    farm_id, fleet_id, _ = _seed_farm_fleet_worker(backend)

    result = _run_deadline(env, "fleet", "list", "--farm-id", farm_id)

    assert f"fleetId: {fleet_id}" in result.stdout
    assert "displayName: Test Fleet" in result.stdout


def test_fleet_get(deadline_env):
    backend, env = deadline_env
    farm_id, fleet_id, _ = _seed_farm_fleet_worker(backend)

    result = _run_deadline(env, "fleet", "get", "--farm-id", farm_id, "--fleet-id", fleet_id)

    assert f"fleetId: {fleet_id}" in result.stdout
    assert f"farmId: {farm_id}" in result.stdout
    assert "displayName: Test Fleet" in result.stdout
    assert "status: ACTIVE" in result.stdout


def test_worker_list(deadline_env):
    backend, env = deadline_env
    farm_id, fleet_id, worker_id = _seed_farm_fleet_worker(backend)

    result = _run_deadline(env, "worker", "list", "--farm-id", farm_id, "--fleet-id", fleet_id)

    assert "Displaying 1 of 1 workers" in result.stdout
    assert f"workerId: {worker_id}" in result.stdout
    assert "status: CREATED" in result.stdout


def test_worker_get(deadline_env):
    backend, env = deadline_env
    farm_id, fleet_id, worker_id = _seed_farm_fleet_worker(backend)

    result = _run_deadline(
        env,
        "worker",
        "get",
        "--farm-id",
        farm_id,
        "--fleet-id",
        fleet_id,
        "--worker-id",
        worker_id,
    )

    assert f"workerId: {worker_id}" in result.stdout
    assert f"fleetId: {fleet_id}" in result.stdout
    assert f"farmId: {farm_id}" in result.stdout
    assert "status: CREATED" in result.stdout
