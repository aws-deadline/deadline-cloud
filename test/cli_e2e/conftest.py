# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Shared fixtures for `test/cli_e2e/` — end-to-end tests that invoke the
real `deadline` CLI as a subprocess against in-process HTTP mocks:

  * MockDeadlineBackend serves the Deadline API    (AWS_ENDPOINT_URL_DEADLINE)
  * ThreadedMotoServer serves S3 / STS / Logs      (AWS_ENDPOINT_URL_{S3,STS,LOGS})

No AWS account, credentials, or internet access are required. No patching
of CLI internals — we only control behavior via environment variables and
the HTTP mocks.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Iterator

import boto3
import pytest
from moto.server import ThreadedMotoServer

from _common.mock_deadline_backend import MockDeadlineBackend, start_server

from _constants import ACCESS_KEY, BUCKET, REGION, ROOT_PREFIX, SECRET_KEY

# `sitecustomize` shim: botocore appends a `management.` host prefix to
# Deadline API calls. Strip it so the CLI talks directly to 127.0.0.1.
# Also unconditionally starts coverage inside the subprocess so `--cov`
# measures the real CLI execution; `coverage.process_startup()` is a no-op
# when COVERAGE_PROCESS_START isn't set by the parent process.
_SITECUSTOMIZE = """
import botocore.awsrequest as _ar
_orig = _ar._urljoin
def _urljoin(endpoint_url, url_path, host_prefix):
    return _orig(endpoint_url, url_path, None)
_ar._urljoin = _urljoin

try:
    import coverage
    coverage.process_startup()
except Exception:
    pass
"""


@pytest.fixture(scope="session", autouse=True)
def _enable_subprocess_coverage() -> Iterator[None]:
    """Point subprocesses at the coverage config so the CLI's execution is
    measured by `--cov`. `sitecustomize.py` calls `coverage.process_startup()`
    which picks up `COVERAGE_PROCESS_START`. No-op when pytest-cov isn't
    active (the parent process simply won't combine any data files)."""
    pyproject = Path(__file__).resolve().parents[2] / "pyproject.toml"
    prior = os.environ.get("COVERAGE_PROCESS_START")
    os.environ["COVERAGE_PROCESS_START"] = str(pyproject)
    try:
        yield
    finally:
        if prior is None:
            os.environ.pop("COVERAGE_PROCESS_START", None)
        else:
            os.environ["COVERAGE_PROCESS_START"] = prior


@pytest.fixture(scope="session")
def moto_server() -> Iterator[str]:
    """ThreadedMotoServer serving S3 + STS + Logs for the whole session."""
    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server.get_host_and_port()
    url = f"http://127.0.0.1:{port}" if host == "0.0.0.0" else f"http://{host}:{port}"
    try:
        yield url
    finally:
        server.stop()


@pytest.fixture(scope="session")
def _mock_backend_session() -> Iterator[tuple]:
    """One MockDeadlineBackend + HTTP server per pytest (xdist worker) session."""
    backend = MockDeadlineBackend()
    server, base_url, _ = start_server(backend)
    try:
        yield backend, base_url
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture
def mock_backend(_mock_backend_session) -> tuple:
    backend, base_url = _mock_backend_session
    backend.clear()
    return backend, base_url


@pytest.fixture
def s3_client(moto_server: str):
    return boto3.client(
        "s3",
        endpoint_url=moto_server,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION,
    )


@pytest.fixture
def logs_client(moto_server: str):
    return boto3.client(
        "logs",
        endpoint_url=moto_server,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION,
    )


@pytest.fixture
def deadline_env(tmp_path: Path, moto_server: str, mock_backend) -> tuple:
    """
    Build the subprocess env vars that point the CLI at our mocks, with an
    isolated HOME + DEADLINE_CONFIG_FILE_PATH so tests don't collide.
    Returns (backend, env).
    """
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
        "AWS_ENDPOINT_URL_S3": moto_server,
        "AWS_ENDPOINT_URL_STS": moto_server,
        "AWS_ENDPOINT_URL_CLOUDWATCH_LOGS": moto_server,
        "AWS_ACCESS_KEY_ID": ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": SECRET_KEY,
        "AWS_DEFAULT_REGION": REGION,
        "DEADLINE_CONFIG_FILE_PATH": str(config_file),
        "PYTHONPATH": str(shim_dir) + os.pathsep + os.environ.get("PYTHONPATH", ""),
    }
    return backend, env


@pytest.fixture
def seeded_farm_queue(deadline_env, s3_client) -> tuple:
    """
    One-shot fixture most tests want: seed a farm + queue wired to our S3
    bucket, create the bucket, configure CLI defaults for farm/queue, and
    return (backend, farm_id, queue_id, env).
    """
    backend, env = deadline_env
    farm = backend.create_farm(displayName="Test Farm")
    farm_id = farm["farmId"]
    queue = backend.create_queue(
        farmId=farm_id,
        displayName="Test Queue",
        defaultBudgetAction="NONE",
        jobAttachmentSettings={"s3BucketName": BUCKET, "rootPrefix": ROOT_PREFIX},
    )
    queue_id = queue["queueId"]

    try:
        s3_client.create_bucket(
            Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": REGION}
        )
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        pass
    # Wipe any objects left over from a prior test on this xdist worker, so
    # assertions on bucket contents observe only what the current test creates.
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET):
        if page.get("Contents"):
            s3_client.delete_objects(
                Bucket=BUCKET,
                Delete={"Objects": [{"Key": o["Key"]} for o in page["Contents"]]},
            )

    configure_defaults(env, farm_id=farm_id, queue_id=queue_id)
    return backend, farm_id, queue_id, env


# ---- helpers exposed to tests ----------------------------------------------


def run_deadline(env: dict, *args: str, cwd: str | Path | None = None, timeout: int = 120):
    """Invoke the real `deadline` binary as a subprocess."""
    return subprocess.run(
        ["deadline", *args],
        env=env,
        cwd=str(cwd) if cwd is not None else None,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def configure_defaults(env: dict, *, farm_id: str | None = None, queue_id: str | None = None):
    pairs = [("telemetry.opt_out", "true")]
    if farm_id is not None:
        pairs.append(("defaults.farm_id", farm_id))
    if queue_id is not None:
        pairs.append(("defaults.queue_id", queue_id))
    for k, v in pairs:
        r = run_deadline(env, "config", "set", k, v)
        assert r.returncode == 0, f"config set {k}={v} failed: {r.stderr}"


@pytest.fixture
def run_cli():
    """Returns a `run(env, *args)` callable that invokes `deadline`."""
    return run_deadline


@pytest.fixture
def configure_cli_defaults():
    """Returns a `configure(env, farm_id=..., queue_id=...)` callable."""
    return configure_defaults
