# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Shared fixtures for pytest-qt GUI tests."""

import importlib.util
import os

import pytest

# Load MockDeadlineBackend by file path to avoid sys.path pollution
# (the test/unit/deadline_client/dataclasses/ package would shadow stdlib).
_mock_path = os.path.join(
    os.path.dirname(__file__),
    "..",
    "unit",
    "deadline_client",
    "mock_deadline_backend.py",
)
_spec = importlib.util.spec_from_file_location("mock_deadline_backend", os.path.abspath(_mock_path))
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
MockDeadlineBackend = _mod.MockDeadlineBackend


@pytest.fixture
def mock_deadline_backend():
    """Provide a fresh MockDeadlineBackend instance."""
    return MockDeadlineBackend(validate_params=False)
