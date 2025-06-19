# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Common fixtures for Deadline Client Library tests.
"""

import tempfile
import os

import pytest


@pytest.fixture(scope="function")
def temp_job_bundle_dir():
    """
    Fixture to provide a temporary job bundle directory.
    """

    with tempfile.TemporaryDirectory() as job_bundle_dir:
        yield job_bundle_dir


@pytest.fixture(scope="function")
def temp_assets_dir():
    """
    Fixture to provide a temporary directory for asset files.
    """

    with tempfile.TemporaryDirectory() as assets_dir:
        yield assets_dir


@pytest.fixture(scope="function")
def temp_cwd():
    """
    Fixture to provide a temporary current working directory.
    """

    with tempfile.TemporaryDirectory() as cwd:
        # Change the current working directory to the temporary directory
        original_cwd = os.getcwd()
        try:
            os.chdir(cwd)
            yield cwd
        finally:
            # Restore the original current working directory
            os.chdir(original_cwd)
