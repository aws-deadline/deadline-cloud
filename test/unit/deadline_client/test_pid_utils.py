# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
import pytest
import psutil
from unittest.mock import patch, mock_open, MagicMock

from deadline.client._pid_utils import (
    check_and_obtain_pid_lock_if_available,
    _obtain_pid_lock_atomically,
)


class TestPidUtils:
    @pytest.fixture
    def mock_logger(self):
        """
        Fixture to create a mock logger object.
        """
        logger = MagicMock()
        logger.echo = MagicMock()
        return logger

    @pytest.fixture
    def test_paths(self):
        """
        Fixture to provide test file paths.
        """
        location = "/path/to/download/location"
        return {
            "location": location,
            "pid_file": os.path.join(location, "incremental_output_download.pid"),
        }

    def test_check_pid_lock_when_file_does_not_exist(self, mock_logger, test_paths):
        """
        Tests the scenario when no PID file exists.
        This is the case when the cli is being bootstrapped.
        """
        with patch("os.path.exists") as mock_exists, patch(
            "deadline.client._pid_utils._obtain_pid_lock_atomically"
        ) as mock_obtain_lock:
            mock_exists.return_value = False

            check_and_obtain_pid_lock_if_available(test_paths["location"], mock_logger)

            expected_pid_file = os.path.join(
                test_paths["location"], "incremental_output_download.pid"
            )
            mock_obtain_lock.assert_called_once_with(expected_pid_file, mock_logger)
            assert mock_logger.echo.called

    def test_check_pid_lock_when_process_not_running(self, mock_logger, test_paths):
        """
        Tests when PID file exists but process is not running.
        This is the case when a run was terminated mid-way causing a stale pid file to exist.
        """
        # Create a mock file that will be returned by the context manager
        mock_file = MagicMock()
        mock_file.read.return_value = "1234"

        # Create a context manager mock
        mock_cm = MagicMock()
        mock_cm.__enter__.return_value = mock_file

        with patch("os.path.exists") as mock_exists, patch("psutil.Process") as mock_process, patch(
            "builtins.open", return_value=mock_cm
        ), patch("deadline.client._pid_utils._obtain_pid_lock_atomically") as mock_obtain_lock:
            mock_exists.return_value = True
            mock_process.side_effect = psutil.NoSuchProcess(1234)

            check_and_obtain_pid_lock_if_available(test_paths["location"], mock_logger)

            expected_pid_file = os.path.join(
                test_paths["location"], "incremental_output_download.pid"
            )

            # Verify the file operations happened in the correct order
            mock_file.read.assert_called_once()
            mock_file.close.assert_called_once()
            mock_obtain_lock.assert_called_once_with(expected_pid_file, mock_logger)

    def test_check_pid_lock_when_process_running(self, mock_logger, test_paths):
        """
        Tests when PID file exists and process is running.
        This is the case when there is a run already ongoing for the CLI so the new one needs to be terminated.
        """
        with patch("os.path.exists") as mock_exists, patch("psutil.Process") as mock_process, patch(
            "builtins.open", mock_open(read_data="1234")
        ):
            mock_exists.return_value = True
            mock_process.return_value = MagicMock()  # Process exists

            with pytest.raises(RuntimeError) as exc_info:
                check_and_obtain_pid_lock_if_available(test_paths["location"], mock_logger)

            assert "Another download is in progress" in str(exc_info.value)
            assert mock_logger.echo.called

    def test_obtain_pid_lock_atomically(self, mock_logger, test_paths):
        """
        Tests the atomic file writing operation for the PID lock.
        """
        test_pid = 5678
        pid_file = os.path.join(test_paths["location"], "incremental_output_download.pid")
        tmp_file = f"{pid_file}~tmp"

        # Create the mock_open correctly
        mocked_open = mock_open()

        with patch("builtins.open", mocked_open) as mock_file_open, patch(
            "os.getpid"
        ) as mock_getpid, patch("os.replace") as mock_replace, patch("os.fsync") as mock_fsync:
            mock_getpid.return_value = test_pid

            _obtain_pid_lock_atomically(pid_file, mock_logger)

            # Verify file operations
            mock_file_open.assert_called_once_with(tmp_file, "w+")
            handle = mock_file_open()
            handle.write.assert_called_once_with(str(test_pid))
            handle.flush.assert_called_once()
            mock_fsync.assert_called_once_with(handle.fileno())
            mock_replace.assert_called_once_with(tmp_file, pid_file)
            mock_logger.echo.assert_called_with(
                f"Creating new pid file at {pid_file} with pid {test_pid}"
            )

    @pytest.mark.parametrize(
        ("error", "expected_message"),
        [
            (PermissionError("Access denied"), "Access denied"),
            (OSError("Disk full"), "Disk full"),
            (Exception("Unexpected error"), "Unexpected error"),
        ],
    )
    def test_check_pid_lock_with_errors(self, mock_logger, test_paths, error, expected_message):
        """
        Tests various error conditions when checking PID lock.
        """
        with patch("os.path.exists") as mock_exists, patch("builtins.open", side_effect=error):
            mock_exists.return_value = True

            with pytest.raises(type(error)) as exc_info:
                check_and_obtain_pid_lock_if_available(test_paths["location"], mock_logger)

            assert str(exc_info.value) == expected_message
