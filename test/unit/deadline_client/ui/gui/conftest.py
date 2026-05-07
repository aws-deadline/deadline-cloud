# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Shared fixtures for pytest-qt GUI tests."""

import importlib.util
import os

# Render Qt widgets to an in-memory buffer so tests run headlessly without a display server.
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

# Load MockDeadlineBackend by file path to avoid sys.path pollution
# (the test/unit/deadline_client/dataclasses/ package would shadow stdlib).
from pathlib import Path

_mock_path = str(Path(__file__).resolve().parents[4] / "_common" / "mock_deadline_backend.py")
_spec = importlib.util.spec_from_file_location("mock_deadline_backend", _mock_path)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
MockDeadlineBackend = _mod.MockDeadlineBackend


@pytest.fixture
def mock_deadline_backend():
    """Provide a fresh MockDeadlineBackend instance."""
    return MockDeadlineBackend(validate_params=False)
