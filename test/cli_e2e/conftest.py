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

import json
import os
import subprocess
import sys
from configparser import ConfigParser
from pathlib import Path
from typing import Iterator

import boto3
import pytest
from botocore.utils import generate_login_cache_key
from moto.server import ThreadedMotoServer

from _common.mock_deadline_backend import MockDeadlineBackend, start_server

from _constants import ACCESS_KEY, BUCKET, REGION, ROOT_PREFIX, SECRET_KEY

# test_proxy_config.py drives the CLI through a threaded TLS mock + CONNECT
# proxy as a subprocess. That socket/TLS/subprocess plumbing is only validated
# on the POSIX CI runners; it isn't exercised on Windows, where loopback TLS and
# process teardown behave differently. Don't collect it there. The proxy /
# ca_bundle wiring itself is platform-agnostic and is covered on every OS by the
# unit tests in test/unit/.../test_api_session.py and the config round-trip
# tests, so Windows coverage of the feature is not lost.
collect_ignore_glob = ["test_proxy_config.py"] if sys.platform == "win32" else []

# The `login_session` ARN an AWS Console sign-in profile is created with. Shared so the
# profile writer and the token-cache seeder derive the same cache key.
CONSOLE_LOGIN_SESSION_ARN = "arn:aws:sts::123456789012:assumed-role/Admin/someone"

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


def set_monitor_profile(env: dict, *, monitor_id: str, region: str = REGION) -> str:
    """
    Write a Deadline Cloud monitor-style AWS profile and point the CLI at it.

    The profile is written to an explicit file wired in via ``AWS_CONFIG_FILE``
    (mutating ``env``) rather than ``~/.aws/config``: boto3 resolves the default
    location from ``%USERPROFILE%`` on Windows, which the subprocess env doesn't
    set, so an explicit path is portable across platforms.

    The profile carries a ``monitor_id`` (the marker the client uses to detect a
    Deadline Cloud monitor login). Selecting a named profile disables botocore's
    env-var credential provider, so the profile also carries the static test
    credentials inline (a real Deadline Cloud monitor profile would instead use a
    ``credential_process``). Returns the profile name.
    """
    profile_name = "dcm-monitor"
    aws_dir = Path(env["HOME"]) / ".aws"
    aws_dir.mkdir(parents=True, exist_ok=True)
    aws_config = aws_dir / "config"
    aws_config.write_text(
        f"[profile {profile_name}]\n"
        f"region = {region}\n"
        f"monitor_id = {monitor_id}\n"
        f"aws_access_key_id = {ACCESS_KEY}\n"
        f"aws_secret_access_key = {SECRET_KEY}\n"
        "user_id = user-1234\n"
        "identity_store_id = d-abcdef0123\n"
    )
    env["AWS_CONFIG_FILE"] = str(aws_config)
    r = run_deadline(env, "config", "set", "defaults.aws_profile_name", profile_name)
    assert r.returncode == 0, f"set aws_profile_name failed: {r.stderr}"
    return profile_name


def set_console_login_profile(env: dict, *, region: str = REGION) -> str:
    """
    Write an AWS Console sign-in profile (as `aws login` / Deadline Cloud monitor's
    console sign-in flow creates) and point the CLI at it.

    The marker the client detects is the ``login_session`` key in the config file.
    Real console profiles keep no credentials there -- botocore's LoginProvider
    resolves them from the token cache under ``~/.aws/login/cache``, which needs the
    ``awscrt`` extra and a live browser handshake. Neither is available here, so the
    placeholder test credentials go in a shared credentials file instead: botocore's
    resolver reaches ``shared-credentials-file`` before the login provider, so API
    calls succeed while ``get_credentials_source`` still sees a console profile.

    Returns the profile name.
    """
    profile_name = "console-signin"
    aws_dir = Path(env["HOME"]) / ".aws"
    aws_dir.mkdir(parents=True, exist_ok=True)

    aws_config = aws_dir / "config"
    aws_config.write_text(
        f"[profile {profile_name}]\n"
        f"region = {region}\n"
        f"login_session = {CONSOLE_LOGIN_SESSION_ARN}\n"
    )
    # ACCESS_KEY/SECRET_KEY are the literal placeholder "testing" from _constants.py, in a
    # throwaway per-test HOME. Assembled via configparser rather than an f-string so the
    # secret-looking value is never a literal in a write call -- see the note in
    # set_deadline_cloud_monitor_profile for why the same shape appears twice.
    aws_credentials = aws_dir / "credentials"
    credentials = ConfigParser()
    credentials[profile_name] = {
        "aws_access_key_id": ACCESS_KEY,
        "aws_secret_access_key": SECRET_KEY,
    }
    with open(aws_credentials, "w") as f:
        credentials.write(f)
    # Set both explicitly: boto3 resolves the default locations from %USERPROFILE%
    # on Windows, which the subprocess env doesn't set.
    env["AWS_CONFIG_FILE"] = str(aws_config)
    env["AWS_SHARED_CREDENTIALS_FILE"] = str(aws_credentials)
    r = run_deadline(env, "config", "set", "defaults.aws_profile_name", profile_name)
    assert r.returncode == 0, f"set aws_profile_name failed: {r.stderr}"
    return profile_name


def seed_login_token_cache(
    tmp_path: Path, env: dict, *, login_session: str = CONSOLE_LOGIN_SESSION_ARN
) -> Path:
    """
    Write a cached login token for a console sign-in session and point the CLI's cache
    lookup at it. Returns the file `deadline auth logout` is expected to delete.

    botocore keys the cache by the sha256 of the ``login_session`` ARN and resolves the
    directory from ``AWS_LOGIN_CACHE_DIRECTORY``, so a temp directory keeps the test off
    the developer's real ``~/.aws/login/cache``.
    """
    cache_dir = tmp_path / "login-cache"
    cache_dir.mkdir(exist_ok=True)
    env["AWS_LOGIN_CACHE_DIRECTORY"] = str(cache_dir)

    cached_token = cache_dir / f"{generate_login_cache_key(login_session)}.json"
    cached_token.write_text(
        json.dumps({"accessToken": "cached-token", "expiresAt": "2999-01-01T00:00:00Z"})
    )
    return cached_token


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


@pytest.fixture
def set_cli_monitor_profile():
    """Returns a `set(env, monitor_id=..., region=...)` callable that writes a
    Deadline Cloud monitor AWS profile and selects it."""
    return set_monitor_profile


@pytest.fixture
def set_cli_console_login_profile():
    """Returns a `set(env, region=...)` callable that writes an AWS Console sign-in
    AWS profile and selects it."""
    return set_console_login_profile


@pytest.fixture
def seed_cli_login_token_cache(tmp_path: Path):
    """Returns a `seed(env)` callable that writes a cached console sign-in token into an
    isolated login cache directory and returns the file's path."""

    def seed(env: dict, *, login_session: str = CONSOLE_LOGIN_SESSION_ARN) -> Path:
        return seed_login_token_cache(tmp_path, env, login_session=login_session)

    return seed
