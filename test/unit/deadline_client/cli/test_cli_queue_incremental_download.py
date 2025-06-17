# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the CLI queue incremental output download command.
"""

import json
import os
import pytest
from unittest.mock import patch, MagicMock

import boto3
from freezegun import freeze_time
from click.testing import CliRunner
from deadline.client.cli import main


MOCK_FARM_ID = "farm-0123456789abcdef"
MOCK_QUEUE_ID = "queue-0123456789abcdef"


# Fixtures for shared resources
@pytest.fixture(scope="module")
def checkpoint_dir(tmp_path_factory):
    """Create a checkpoint directory for all tests to use."""
    checkpoint_dir = tmp_path_factory.mktemp("checkpoint")
    yield str(checkpoint_dir)
    # No cleanup needed here as tmp_path_factory handles it automatically


@pytest.fixture(scope="module")
def boto3_session():
    """Create a mock boto3 session for all tests to use."""
    return MagicMock(spec=boto3.Session)


@pytest.fixture
def progress_file(checkpoint_dir):
    """Create a progress file path for tests that need it.

    This has function scope so each test gets a fresh file.
    """
    progress_file_path = os.path.join(checkpoint_dir, f"{MOCK_QUEUE_ID}_download_progress.json")
    # File will be created by the test that needs it
    yield progress_file_path
    # Clean up after each test
    if os.path.exists(progress_file_path):
        os.remove(progress_file_path)


@pytest.fixture
def pid_lock_file(checkpoint_dir):
    """Create a PID lock file path for tests that need it."""
    pid_file_path = os.path.join(checkpoint_dir, f"{MOCK_QUEUE_ID}_incremental_output_download.pid")
    yield pid_file_path
    # Clean up
    if os.path.exists(pid_file_path):
        os.remove(pid_file_path)


@pytest.fixture
def path_mapping_rules_file(tmp_path_factory):
    """Create a path mapping rules file for tests that need it."""
    # Create in a separate directory to avoid conflicts
    rules_dir = tmp_path_factory.mktemp("rules")
    rules_file_path = os.path.join(str(rules_dir), "rules.json")
    yield rules_file_path
    # Clean up
    if os.path.exists(rules_file_path):
        os.remove(rules_file_path)


@pytest.fixture
def sample_progress_data():
    """Sample progress data for testing."""
    return {
        "lastLookbackTime": "2025-04-04T05:30:00",
        "jobs": [
            {
                "jobId": "job-1234353453443",
                "sessions": [
                    {
                        "sessionId": "session-1324324354354",
                        "sessionLifecycleStatus": "SUCCESSFUL",
                        "lastDownloadedSessActionId": 3,
                    },
                    {
                        "sessionId": "session-3423435435454",
                        "sessionLifecycleStatus": "RUNNING",
                        "lastDownloadedSessActionId": 6,
                    },
                ],
            },
            {
                "jobId": "Job-3234324354345",
                "sessions": [
                    {
                        "sessionId": "session-4235435434345",
                        "sessionLifecycleStatus": "FAILED",
                        "lastDownloadedSessActionId": 3,
                    }
                ],
            },
        ],
    }


@pytest.mark.skipif(
    os.environ.get("ENABLE_INCREMENTAL_OUTPUT_DOWNLOAD") is None,
    reason="Incremental output download is not enabled",
)
@freeze_time("2025-05-26 12:00:00+00:00")
@patch("deadline.client.api.get_boto3_session")
@patch.dict(os.environ, {"ENABLE_INCREMENTAL_OUTPUT_DOWNLOAD": "True"})
def test_incremental_output_download_success_load_from_progress(
    mock_get_boto3_session, checkpoint_dir, progress_file, sample_progress_data
):
    """Test successful execution of incremental_output_download with loading progress from state file"""
    # Create a real progress file with test data
    with open(progress_file, "w") as f:
        json.dump(sample_progress_data, f, indent=2)

    # Mock boto3 session
    mock_session = MagicMock(spec=boto3.Session)
    mock_get_boto3_session.return_value = mock_session

    # Run the CLI command
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "queue",
            "incremental-output-download",
            "--farm-id",
            MOCK_FARM_ID,
            "--queue-id",
            MOCK_QUEUE_ID,
            "--saved-progress-checkpoint-location",
            checkpoint_dir,
        ],
    )

    # Assert the command executed successfully
    assert result.exit_code == 0

    # Check that the progress file was updated
    with open(progress_file, "r") as f:
        updated_progress = json.load(f)

    # Verify the lastLookbackTime was updated to the frozen time
    assert updated_progress["lastLookbackTime"] == "2025-05-26T12:00:00+00:00"

    # Verify the job data was preserved
    assert len(updated_progress["jobs"]) == 2
    assert updated_progress["jobs"][0]["jobId"] == "job-1234353453443"


@pytest.mark.skipif(
    os.environ.get("ENABLE_INCREMENTAL_OUTPUT_DOWNLOAD") is None,
    reason="Incremental output download is not enabled",
)
@freeze_time("2025-05-26 12:00:00+00:00")
@patch("deadline.client.api.get_boto3_session")
@patch.dict(os.environ, {"ENABLE_INCREMENTAL_OUTPUT_DOWNLOAD": "True"})
@pytest.mark.parametrize("bootstrap_lookback_in_minutes", [60, None])
def test_incremental_output_download_success_with_force_bootstrap(
    mock_get_boto3_session, checkpoint_dir, progress_file, bootstrap_lookback_in_minutes
):
    """Test successful execution of incremental_output_download with bootstrapping"""
    # Create a file that should be ignored due to force_bootstrap=True
    with open(progress_file, "w") as f:
        json.dump({"lastLookbackTime": "2025-01-01T00:00:00", "jobs": []}, f)

    # Mock boto3 session
    mock_session = MagicMock(spec=boto3.Session)
    mock_get_boto3_session.return_value = mock_session

    # Run the CLI command
    runner = CliRunner()
    cmd = [
        "queue",
        "incremental-output-download",
        "--farm-id",
        MOCK_FARM_ID,
        "--queue-id",
        MOCK_QUEUE_ID,
        "--saved-progress-checkpoint-location",
        checkpoint_dir,
        "--force-bootstrap",
    ]

    if bootstrap_lookback_in_minutes is not None:
        cmd.extend(["--bootstrap-lookback-in-minutes", str(bootstrap_lookback_in_minutes)])

    result = runner.invoke(main, cmd)

    # Assert the command executed successfully
    assert result.exit_code == 0

    # Check that the progress file was updated
    with open(progress_file, "r") as f:
        updated_progress = json.load(f)

    # Verify the lastLookbackTime was updated to the frozen time
    assert updated_progress["lastLookbackTime"] == "2025-05-26T12:00:00+00:00"

    # Verify the jobs list is empty (as it would be with a fresh bootstrap)
    assert updated_progress["jobs"] == []


@pytest.mark.skipif(
    os.environ.get("ENABLE_INCREMENTAL_OUTPUT_DOWNLOAD") is None,
    reason="Incremental output download is not enabled",
)
@patch("psutil.pid_exists")
@patch.dict(os.environ, {"ENABLE_INCREMENTAL_OUTPUT_DOWNLOAD": "True"})
def test_incremental_output_download_pid_lock_already_held_error(
    mock_pid_exists, checkpoint_dir, pid_lock_file
):
    """Test incremental_output_download when PidLockAlreadyHeld is raised"""
    # Write a fake PID to the file
    with open(pid_lock_file, "w") as f:
        f.write("12345")  # Use a fake PID

    # Make psutil.pid_exists return True to simulate the process is running
    mock_pid_exists.return_value = True

    # Run the CLI command
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "queue",
            "incremental-output-download",
            "--farm-id",
            MOCK_FARM_ID,
            "--queue-id",
            MOCK_QUEUE_ID,
            "--saved-progress-checkpoint-location",
            checkpoint_dir,
        ],
    )

    # Assert the command executed successfully but with a message about another download in progress
    assert result.exit_code == 0
    assert f"Another download is in progress at {checkpoint_dir}" in result.output

    # Verify the PID file still exists since we're simulating another process holding the lock
    assert os.path.exists(pid_lock_file)


@pytest.mark.skipif(
    os.environ.get("ENABLE_INCREMENTAL_OUTPUT_DOWNLOAD") is None,
    reason="Incremental output download is not enabled",
)
@patch.dict(os.environ, {"ENABLE_INCREMENTAL_OUTPUT_DOWNLOAD": "True"})
def test_validate_file_inputs_success(checkpoint_dir):
    """Test successful validation of file inputs"""
    # Run the CLI command with a valid directory
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "queue",
            "incremental-output-download",
            "--farm-id",
            MOCK_FARM_ID,
            "--queue-id",
            MOCK_QUEUE_ID,
            "--saved-progress-checkpoint-location",
            checkpoint_dir,
        ],
    )

    # The command should execute
    assert result.exit_code == 0


@pytest.mark.skipif(
    os.environ.get("ENABLE_INCREMENTAL_OUTPUT_DOWNLOAD") is None,
    reason="Incremental output download is not enabled",
)
@patch.dict(os.environ, {"ENABLE_INCREMENTAL_OUTPUT_DOWNLOAD": "1"})
def test_validate_file_inputs_invalid_directory(checkpoint_dir):
    """Test validation when directory is invalid"""
    # Create a path to a non-existent directory
    nonexistent_dir = os.path.join(checkpoint_dir, "nonexistent_directory")

    # Run the CLI command with an invalid directory
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "queue",
            "incremental-output-download",
            "--farm-id",
            MOCK_FARM_ID,
            "--queue-id",
            MOCK_QUEUE_ID,
            "--saved-progress-checkpoint-location",
            nonexistent_dir,
        ],
    )

    # The command should execute but report the validation error
    assert result.exit_code == 0
    assert "Download failed" in result.output


@pytest.mark.skipif(
    os.environ.get("ENABLE_INCREMENTAL_OUTPUT_DOWNLOAD") is None,
    reason="Incremental output download is not enabled",
)
@patch.dict(os.environ, {"ENABLE_INCREMENTAL_OUTPUT_DOWNLOAD": "True"})
def test_validate_file_inputs_with_mapping_rules_success(checkpoint_dir, path_mapping_rules_file):
    """Test successful validation with path mapping rules"""
    # Create the rules file with valid content
    with open(path_mapping_rules_file, "w") as f:
        f.write('{"rules": []}')

    # Run the CLI command with valid rules file
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "queue",
            "incremental-output-download",
            "--farm-id",
            MOCK_FARM_ID,
            "--queue-id",
            MOCK_QUEUE_ID,
            "--saved-progress-checkpoint-location",
            checkpoint_dir,
            "--path-mapping-rules",
            path_mapping_rules_file,
        ],
    )

    # The command should execute without validation errors
    assert result.exit_code == 0
    assert "Download failed" not in result.output


@pytest.mark.skipif(
    os.environ.get("ENABLE_INCREMENTAL_OUTPUT_DOWNLOAD") is None,
    reason="Incremental output download is not enabled",
)
@patch.dict(os.environ, {"ENABLE_INCREMENTAL_OUTPUT_DOWNLOAD": "True"})
def test_validate_file_inputs_mapping_rules_not_exist(checkpoint_dir):
    """Test validation when mapping rules file doesn't exist"""
    # Create a path to a non-existent rules file
    nonexistent_rules = os.path.join(checkpoint_dir, "nonexistent_rules.json")

    # Run the CLI command with non-existent rules file
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "queue",
            "incremental-output-download",
            "--farm-id",
            MOCK_FARM_ID,
            "--queue-id",
            MOCK_QUEUE_ID,
            "--saved-progress-checkpoint-location",
            checkpoint_dir,
            "--path-mapping-rules",
            nonexistent_rules,
        ],
    )

    # The command should execute but report the validation error
    assert result.exit_code == 0
    assert "Download failed" in result.output


@pytest.mark.skipif(
    os.environ.get("ENABLE_INCREMENTAL_OUTPUT_DOWNLOAD") is None,
    reason="Incremental output download is not enabled",
)
@patch("os.access")
@patch.dict(os.environ, {"ENABLE_INCREMENTAL_OUTPUT_DOWNLOAD": "True"})
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

    # Run the CLI command with non-readable rules file
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "queue",
            "incremental-output-download",
            "--farm-id",
            MOCK_FARM_ID,
            "--queue-id",
            MOCK_QUEUE_ID,
            "--saved-progress-checkpoint-location",
            checkpoint_dir,
            "--path-mapping-rules",
            path_mapping_rules_file,
        ],
    )

    # The command should execute but report the validation error
    assert result.exit_code == 0
    assert "Download failed" in result.output
