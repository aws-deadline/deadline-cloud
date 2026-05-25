# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Shared fixtures for pytest-qt GUI tests."""

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

from _common.mock_deadline_backend import MockDeadlineBackend


@pytest.fixture
def mock_deadline_backend():
    """Provide a fresh MockDeadlineBackend instance."""
    return MockDeadlineBackend(validate_params=False)
