"""Shared fixtures for pytests/bindings/.

Starts the ffi-test-server binary (wiremock stub) once per session,
sets env vars so all FFI calls hit the stub instead of real AWS.
"""

import json
import os
import subprocess
from pathlib import Path

import pytest

# Env vars to remove so the host environment doesn't interfere
CLEAN_VARS = [
    "AWS_PROFILE",
    "AWS_DEFAULT_PROFILE",
    "AWS_CONFIG_FILE",
    "AWS_SHARED_CREDENTIALS_FILE",
    "AWS_SESSION_TOKEN",
    "AWS_SECURITY_TOKEN",
    "AWS_ENDPOINT_URL",
]


def _find_server_binary() -> str:
    """Find the ffi-test-server binary."""
    repo_root = Path(__file__).resolve().parent.parent.parent
    candidates = [
        repo_root / "target" / "debug" / "ffi-test-server",
        repo_root / "target" / "release" / "ffi-test-server",
    ]
    for c in candidates:
        if c.exists():
            return str(c)
    # Try building it
    subprocess.run(
        ["cargo", "build", "--bin", "ffi-test-server"],
        cwd=str(repo_root),
        check=True,
        capture_output=True,
    )
    if candidates[0].exists():
        return str(candidates[0])
    raise FileNotFoundError("Could not find ffi-test-server binary. Run: cargo build --bin ffi-test-server")


@pytest.fixture(scope="session")
def test_server():
    """Start the Rust stub server, yield connection info, kill on teardown."""
    binary = _find_server_binary()
    proc = subprocess.Popen(
        [binary],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Read the JSON connection info from stdout
    line = proc.stdout.readline()
    if not line:
        proc.terminate()
        stderr = proc.stderr.read().decode()
        raise RuntimeError(f"ffi-test-server failed to start: {stderr}")

    info = json.loads(line)
    endpoint = info["endpoint"]
    config_path = info["config_path"]

    # Save original env to restore later
    saved_env = {}
    env_settings = {
        "AWS_ENDPOINT_URL_DEADLINE": endpoint,
        "AWS_ENDPOINT_URL_STS": endpoint,
        "AWS_ENDPOINT_URL_S3": endpoint,
        "AWS_ENDPOINT_URL_CLOUDWATCHLOGS": endpoint,
        "AWS_ACCESS_KEY_ID": "AKIAIOSFODNN7EXAMPLE",
        "AWS_SECRET_ACCESS_KEY": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "AWS_DEFAULT_REGION": "us-west-2",
        "DEADLINE_CONFIG_FILE_PATH": config_path,
    }

    for var in CLEAN_VARS:
        saved_env[var] = os.environ.pop(var, None)
    for key, val in env_settings.items():
        saved_env[key] = os.environ.get(key)
        os.environ[key] = val

    yield info

    # Teardown: kill server, restore env
    proc.terminate()
    proc.wait(timeout=5)
    for key, val in saved_env.items():
        if val is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = val


@pytest.fixture
def tmp_config(tmp_path):
    """Create a temp config file with a known setting and return its path."""
    config = tmp_path / "config"
    config.write_text("[profile-(default) defaults]\nfarm_id = farm-test123\n")
    return str(config)


@pytest.fixture
def empty_config(tmp_path):
    """Create an empty temp config file and return its path."""
    config = tmp_path / "config"
    config.write_text("")
    return str(config)
