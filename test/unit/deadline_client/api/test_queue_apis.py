# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.


import os
import pytest
from unittest.mock import patch, MagicMock

import boto3
from deadline.client.api._queue_apis import (
    _incremental_output_download,
    _validate_file_inputs_for_incremental_output_download,
)
from deadline.client.cli._groups.click_logger import ClickLogger


@patch("deadline.client.api._queue_apis._pid_utils.check_and_obtain_pid_lock_if_available")
def test_incremental_output_download_success(mock_pid_lock, tmp_path):
    """Test successful execution of _incremental_output_download"""
    # Arrange
    farm_id = "farm-0123456789abcdef"
    queue_id = "queue-0123456789abcdef"
    boto3_session = MagicMock(spec=boto3.Session)
    saved_progress_checkpoint_location = str(tmp_path / "checkpoint")
    logger = MagicMock(spec=ClickLogger)

    # Act
    _incremental_output_download(
        farm_id=farm_id,
        queue_id=queue_id,
        boto3_session=boto3_session,
        saved_progress_checkpoint_location=saved_progress_checkpoint_location,
        logger=logger,
    )

    # Assert
    mock_pid_lock.assert_called_once_with(saved_progress_checkpoint_location, logger)


@patch("deadline.client.api._queue_apis._pid_utils.check_and_obtain_pid_lock_if_available")
def test_incremental_output_download_runtime_error(mock_pid_lock, tmp_path):
    """Test _incremental_output_download when RuntimeError is raised"""
    # Arrange
    farm_id = "farm-0123456789abcdef"
    queue_id = "queue-0123456789abcdef"
    boto3_session = MagicMock(spec=boto3.Session)
    saved_progress_checkpoint_location = str(tmp_path / "checkpoint")
    logger = MagicMock(spec=ClickLogger)

    mock_pid_lock.side_effect = RuntimeError("Download already in progress")

    # Act
    _incremental_output_download(
        farm_id=farm_id,
        queue_id=queue_id,
        boto3_session=boto3_session,
        saved_progress_checkpoint_location=saved_progress_checkpoint_location,
        logger=logger,
    )

    # Assert
    mock_pid_lock.assert_called_once_with(saved_progress_checkpoint_location, logger)
    logger.echo.assert_called_once_with(
        "Download failed because of error : Download already in progress"
    )


@patch("deadline.client.api._queue_apis._pid_utils.check_and_obtain_pid_lock_if_available")
def test_incremental_output_download_generic_exception(mock_pid_lock, tmp_path):
    """Test _incremental_output_download when a generic Exception is raised"""
    # Arrange
    farm_id = "farm-0123456789abcdef"
    queue_id = "queue-0123456789abcdef"
    boto3_session = MagicMock(spec=boto3.Session)
    saved_progress_checkpoint_location = str(tmp_path / "checkpoint")
    logger = MagicMock(spec=ClickLogger)

    mock_pid_lock.side_effect = Exception("Unexpected error")

    # Act
    _incremental_output_download(
        farm_id=farm_id,
        queue_id=queue_id,
        boto3_session=boto3_session,
        saved_progress_checkpoint_location=saved_progress_checkpoint_location,
        logger=logger,
    )

    # Assert
    mock_pid_lock.assert_called_once_with(saved_progress_checkpoint_location, logger)
    logger.echo.assert_called_once()
    assert "Failed to obtain lock for download progress" in logger.echo.call_args[0][0]


@patch("os.path.isdir")
@patch("os.access")
def test_validate_file_inputs_success(mock_access, mock_isdir, tmp_path):
    """Test successful validation of file inputs"""
    # Arrange
    saved_progress_checkpoint_location = str(tmp_path / "checkpoint")
    mock_isdir.return_value = True
    mock_access.return_value = True

    # Act
    result = _validate_file_inputs_for_incremental_output_download(
        saved_progress_checkpoint_location
    )

    # Assert
    assert result is True
    mock_isdir.assert_called_once_with(saved_progress_checkpoint_location)
    mock_access.assert_called_once_with(saved_progress_checkpoint_location, os.W_OK)


@patch("os.path.isdir")
def test_validate_file_inputs_invalid_directory(mock_isdir, tmp_path):
    """Test validation when directory is invalid"""
    # Arrange
    saved_progress_checkpoint_location = str(tmp_path / "checkpoint")
    mock_isdir.return_value = False

    # Act & Assert
    with pytest.raises(RuntimeError) as excinfo:
        _validate_file_inputs_for_incremental_output_download(saved_progress_checkpoint_location)

    assert "is not a valid directory" in str(excinfo.value)
    mock_isdir.assert_called_once_with(saved_progress_checkpoint_location)


@patch("os.path.isdir")
@patch("os.access")
def test_validate_file_inputs_not_writable(mock_access, mock_isdir, tmp_path):
    """Test validation when directory is not writable"""
    # Arrange
    saved_progress_checkpoint_location = str(tmp_path / "checkpoint")
    mock_isdir.return_value = True
    mock_access.return_value = False

    # Act & Assert
    with pytest.raises(RuntimeError) as excinfo:
        _validate_file_inputs_for_incremental_output_download(saved_progress_checkpoint_location)

    assert "is not writable" in str(excinfo.value)
    mock_isdir.assert_called_once_with(saved_progress_checkpoint_location)
    mock_access.assert_called_once_with(saved_progress_checkpoint_location, os.W_OK)


@patch("os.path.isdir")
@patch("os.access")
@patch("os.path.isfile")
def test_validate_file_inputs_with_mapping_rules_success(
    mock_isfile, mock_access, mock_isdir, tmp_path
):
    """Test successful validation with path mapping rules"""
    # Arrange
    saved_progress_checkpoint_location = str(tmp_path / "checkpoint")
    path_mapping_rules = str(tmp_path / "rules.json")
    mock_isdir.return_value = True
    mock_access.return_value = True
    mock_isfile.return_value = True

    # Act
    result = _validate_file_inputs_for_incremental_output_download(
        saved_progress_checkpoint_location, path_mapping_rules
    )

    # Assert
    assert result is True
    mock_isdir.assert_called_once_with(saved_progress_checkpoint_location)
    mock_access.assert_any_call(saved_progress_checkpoint_location, os.W_OK)
    mock_isfile.assert_called_once_with(path_mapping_rules)
    mock_access.assert_any_call(path_mapping_rules, os.R_OK)


@patch("os.path.isdir")
@patch("os.access")
@patch("os.path.isfile")
def test_validate_file_inputs_mapping_rules_not_exist(
    mock_isfile, mock_access, mock_isdir, tmp_path
):
    """Test validation when mapping rules file doesn't exist"""
    # Arrange
    saved_progress_checkpoint_location = str(tmp_path / "checkpoint")
    path_mapping_rules = str(tmp_path / "rules.json")
    mock_isdir.return_value = True
    mock_access.return_value = True
    mock_isfile.return_value = False

    # Act & Assert
    with pytest.raises(RuntimeError) as excinfo:
        _validate_file_inputs_for_incremental_output_download(
            saved_progress_checkpoint_location, path_mapping_rules
        )

    assert "does not exist" in str(excinfo.value)
    mock_isdir.assert_called_once_with(saved_progress_checkpoint_location)
    mock_access.assert_called_once_with(saved_progress_checkpoint_location, os.W_OK)
    mock_isfile.assert_called_once_with(path_mapping_rules)


@patch("os.path.isdir")
@patch("os.access")
@patch("os.path.isfile")
def test_validate_file_inputs_mapping_rules_not_readable(
    mock_isfile, mock_access, mock_isdir, tmp_path
):
    """Test validation when mapping rules file is not readable"""
    # Arrange
    saved_progress_checkpoint_location = str(tmp_path / "checkpoint")
    path_mapping_rules = str(tmp_path / "rules.json")
    mock_isdir.return_value = True
    mock_isfile.return_value = True

    # Configure mock_access to return True for directory write check and False for file read check
    def access_side_effect(path, mode):
        if path == saved_progress_checkpoint_location and mode == os.W_OK:
            return True
        if path == path_mapping_rules and mode == os.R_OK:
            return False
        return True

    mock_access.side_effect = access_side_effect

    # Act & Assert
    with pytest.raises(RuntimeError) as excinfo:
        _validate_file_inputs_for_incremental_output_download(
            saved_progress_checkpoint_location, path_mapping_rules
        )

    assert "is not readable" in str(excinfo.value)
    mock_isdir.assert_called_once_with(saved_progress_checkpoint_location)
    mock_isfile.assert_called_once_with(path_mapping_rules)
