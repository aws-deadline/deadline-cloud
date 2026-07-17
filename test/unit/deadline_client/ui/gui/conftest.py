# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Shared fixtures for pytest-qt GUI tests."""

import importlib.util
import os

import pytest

from _common.mock_deadline_backend import MockDeadlineBackend

_has_pyside6 = importlib.util.find_spec("PySide6") is not None

_QT_TEST_FILES = [
    "test_gui_host_requirements.py",
    "test_gui_job_attachments.py",
    "test_gui_job_bundle_submitter.py",
    "test_gui_job_timeouts.py",
    "test_gui_openjd_parameters.py",
    "test_gui_shared_job_properties.py",
    "test_gui_shared_job_settings.py",
    "test_gui_submitter_bundles.py",
    "test_gui_utils_and_widgets.py",
    "test_settings_dialogue.py",
]

collect_ignore = [f for f in _QT_TEST_FILES] if not _has_pyside6 else []

if _has_pyside6:
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")


@pytest.fixture
def mock_deadline_backend():
    """Provide a fresh MockDeadlineBackend instance."""
    return MockDeadlineBackend(validate_params=False)
