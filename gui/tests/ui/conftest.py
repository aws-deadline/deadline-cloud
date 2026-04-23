"""Shared fixtures for gui/tests/ui/ — ported from Python repo."""

import tempfile
import pytest


@pytest.fixture(scope="function")
def temp_job_bundle_dir():
    """Fixture to provide a temporary job bundle directory."""
    with tempfile.TemporaryDirectory() as job_bundle_dir:
        yield job_bundle_dir
