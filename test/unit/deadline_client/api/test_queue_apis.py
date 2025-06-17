# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
import pytest
from unittest.mock import patch

from deadline.client.api._queue_apis import (
    _validate_file_inputs_for_incremental_output_download,
)


# Fixtures for shared resources
@pytest.fixture(scope="module")
def checkpoint_dir(tmp_path_factory):
    """Create a checkpoint directory for all tests to use."""
    checkpoint_dir = tmp_path_factory.mktemp("checkpoint")
    yield str(checkpoint_dir)
    # No cleanup needed here as tmp_path_factory handles it automatically


@pytest.fixture
def path_mapping_rules_file(tmp_path_factory):
    """Create a rules file for tests that need it."""
    # Create in a separate directory to avoid conflicts
    rules_dir = tmp_path_factory.mktemp("rules")
    path_mapping_rules_file_path = os.path.join(str(rules_dir), "rules.json")
    yield path_mapping_rules_file_path
    # Clean up
    if os.path.exists(path_mapping_rules_file_path):
        os.remove(path_mapping_rules_file_path)


def test_validate_file_inputs_success(checkpoint_dir):
    """Test successful validation of file inputs"""
    # Act
    result = _validate_file_inputs_for_incremental_output_download(checkpoint_dir)

    # Assert
    assert result is True


def test_validate_file_inputs_invalid_directory(tmp_path):
    """Test validation when directory is invalid"""
    # Arrange
    nonexistent_dir = str(tmp_path / "nonexistent_directory")

    # Act & Assert
    with pytest.raises(RuntimeError) as excinfo:
        _validate_file_inputs_for_incremental_output_download(nonexistent_dir)

    assert "is not a valid directory" in str(excinfo.value)


@patch("os.access")
def test_validate_file_inputs_not_writable(mock_access, tmp_path):
    """Test validation when directory is not writable"""
    # Arrange
    readonly_dir = str(tmp_path / "readonly_dir")

    # Create the directory
    os.makedirs(readonly_dir, exist_ok=True)

    # Mock os.access to simulate a read-only directory
    def access_side_effect(path, mode):
        if path == readonly_dir and mode == os.W_OK:
            return False
        return True

    mock_access.side_effect = access_side_effect

    # Act & Assert
    with pytest.raises(RuntimeError) as excinfo:
        _validate_file_inputs_for_incremental_output_download(readonly_dir)

    assert "is not writable" in str(excinfo.value)


def test_validate_file_inputs_with_mapping_rules_success(checkpoint_dir, path_mapping_rules_file):
    """Test successful validation with path mapping rules"""
    # Create the rules file with valid content
    with open(path_mapping_rules_file, "w") as f:
        f.write('{"rules": []}')

    # Act
    result = _validate_file_inputs_for_incremental_output_download(
        checkpoint_dir, path_mapping_rules_file
    )

    # Assert
    assert result is True


def test_validate_file_inputs_mapping_rules_not_exist(checkpoint_dir, tmp_path):
    """Test validation when mapping rules file doesn't exist"""
    # Arrange
    nonexistent_rules = str(tmp_path / "nonexistent_rules.json")

    # Act & Assert
    with pytest.raises(RuntimeError) as excinfo:
        _validate_file_inputs_for_incremental_output_download(checkpoint_dir, nonexistent_rules)

    assert "does not exist" in str(excinfo.value)


@patch("os.access")
def test_validate_file_inputs_mapping_rules_not_readable(
    mock_access, checkpoint_dir, path_mapping_rules_file
):
    """Test validation when mapping rules file is not readable"""
    # Create the rules file
    with open(path_mapping_rules_file, "w") as f:
        f.write('{"rules": []}')

    # Mock os.access to simulate a non-readable rules file
    def access_side_effect(path, mode):
        if path == path_mapping_rules_file and mode == os.R_OK:
            return False
        return True

    mock_access.side_effect = access_side_effect

    # Act & Assert
    with pytest.raises(RuntimeError) as excinfo:
        _validate_file_inputs_for_incremental_output_download(
            checkpoint_dir, path_mapping_rules_file
        )

    assert "is not readable" in str(excinfo.value)
