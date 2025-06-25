# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
import pytest
import tempfile
from unittest.mock import patch

import psutil

from deadline.client.cli._pid_file_lock import (
    _try_acquire_pid_lock,
    _release_pid_lock,
    _pid_lock_temp_file_path,
    PidFileLock,
    PidLockAlreadyHeld,
)
from deadline.client.cli import _pid_file_lock

# This value is larger than PIDs typically are to avoid accidentally having a real PID here
FAKE_PID = 2000000000


@pytest.fixture
def temp_dir():
    """
    Fixture to provide a temporary directory that is cleaned up after tests.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def temp_pid_lock_file(temp_dir):
    """
    Fixture to provide a pid file lock name in the temporary directory
    """
    yield os.path.join(temp_dir, "pid_lock_file.pid")


def test_pidlock_acquire_and_release(temp_pid_lock_file):
    """
    Tests normal pidlock acquisition/release.
    """
    # Lock file does not exist before acquiring the lock
    assert not os.path.exists(temp_pid_lock_file)

    _try_acquire_pid_lock(temp_pid_lock_file)

    # Lock file exists after acquiring the lock
    assert os.path.exists(temp_pid_lock_file)

    # Lock file contains the current process id
    with open(temp_pid_lock_file) as fh:
        assert int(fh.read()) == os.getpid()

    _release_pid_lock(temp_pid_lock_file)

    # Lock file is removed when releasing the lock
    assert not os.path.exists(temp_pid_lock_file)


def test_pidlock_contextmanager_acquire_and_release(temp_pid_lock_file):
    """
    Tests normal pidlock acquisition/release with the context manager.
    """
    # Lock file does not exist before acquiring the lock
    assert not os.path.exists(temp_pid_lock_file)

    with PidFileLock(temp_pid_lock_file):
        # Lock file exists after acquiring the lock
        assert os.path.exists(temp_pid_lock_file)

        # Lock file contains the current process id
        with open(temp_pid_lock_file) as fh:
            assert fh.read() == str(os.getpid())

    # Lock file is removed when releasing the lock
    assert not os.path.exists(temp_pid_lock_file)


def test_check_pid_lock_when_process_not_running(temp_pid_lock_file):
    """
    Tests when PID file exists but process is not running.
    This is the case when a run was terminated mid-way causing a stale pid file to exist.
    """
    # Write a PID that doesn't exist to the lock file
    with open(temp_pid_lock_file, "w") as fh:
        fh.write(str(FAKE_PID))

    # Patch the logger object to confirm it writes a warning message about the lock file
    with patch.object(_pid_file_lock, "logger") as logger_mock:
        _try_acquire_pid_lock(temp_pid_lock_file)

        # The function should have logged about deleting the lock file
        assert logger_mock.warning.call_count == 1
        warning_message = logger_mock.warning.call_args.args[0]
        assert (
            f"Process with pid {FAKE_PID} is not running. Deleted pid lock file." in warning_message
        )

        # The lock file should now contain the current pid
        with open(temp_pid_lock_file) as fh:
            assert fh.read() == str(os.getpid())


def test_check_pid_lock_with_corrupt_pidfile(temp_pid_lock_file):
    """
    Tests when PID file exists and contains corrupted data.
    In this case it should delete the file and log a warning.
    """
    # Write a PID that doesn't exist to the lock file
    with open(temp_pid_lock_file, "w") as fh:
        fh.write("bad_data")

    # Patch the logger object to confirm it writes a warning message about the lock file
    with patch.object(_pid_file_lock, "logger") as logger_mock:
        _try_acquire_pid_lock(temp_pid_lock_file)

        # The function should have logged about deleting the lock file
        assert logger_mock.warning.call_count == 1
        warning_message = logger_mock.warning.call_args.args[0]
        assert "Pid lock file contains incorrect data. Deleted pid lock file." in warning_message

        # The lock file should now contain the current pid
        with open(temp_pid_lock_file) as fh:
            assert fh.read() == str(os.getpid())


def test_check_pid_lock_when_process_running(temp_pid_lock_file):
    """
    Tests when PID file exists and process is running.
    This is the case when a process is already holding the lock.
    """
    # Write a PID that doesn't exist to the lock file (value is larger than PIDs are typically)
    with open(temp_pid_lock_file, "w") as fh:
        fh.write(str(FAKE_PID))

    # Patch psutil.pid_exists to return that the mock_pid exists
    def FAKE_PID_exists(pid):
        return pid == FAKE_PID

    with patch.object(psutil, "pid_exists", wraps=FAKE_PID_exists):
        with pytest.raises(PidLockAlreadyHeld) as excinfo:
            _try_acquire_pid_lock(temp_pid_lock_file)

        assert (
            f"Unable to perform the operation as process with pid {FAKE_PID} already holds the lock"
            in str(excinfo.value)
        )

        # The lock file should still contain the fake pid
        assert os.path.exists(temp_pid_lock_file)
        with open(temp_pid_lock_file) as fh:
            assert fh.read() == str(FAKE_PID)


def test_release_pid_lock_when_pid_does_not_match(temp_pid_lock_file):
    """
    Tests releasing a PID lock when the PID doesn't match the current process.
    This happens when another process started concurrently and incorrectly claimed
    the lock. Since the error is in a different process, it prints a warning and
    leaves the file.
    """
    # Write a PID that doesn't exist to the lock file (value is larger than PIDs are typically)
    with open(temp_pid_lock_file, "w") as fh:
        fh.write(str(FAKE_PID))

    # Patch psutil.pid_exists to return that the mock_pid exists
    def FAKE_PID_exists(pid):
        return pid == FAKE_PID

    with patch.object(psutil, "pid_exists", wraps=FAKE_PID_exists), patch.object(
        _pid_file_lock, "logger"
    ) as logger_mock:
        _release_pid_lock(temp_pid_lock_file)

        # The function should have logged about leaving the lock file
        assert logger_mock.warning.call_count == 1, f"{logger_mock.warning.call_args_list}"
        warning_message = logger_mock.warning.call_args.args[0]
        assert f"Another process with pid {FAKE_PID} claimed the pid lock" in warning_message
        assert f"while {os.getpid()} was holding it. Skipping pid file deletion." in warning_message

        # The lock file should still contain the fake pid
        assert os.path.exists(temp_pid_lock_file)
        with open(temp_pid_lock_file) as fh:
            assert fh.read() == str(FAKE_PID)


def test_release_pid_lock_when_lock_not_held(temp_pid_lock_file):
    """
    Tests releasing a PID lock when the lock isn't being held. Confirm a warning is generated.
    """
    # No pid lock file in this case
    assert not os.path.exists(temp_pid_lock_file)

    with patch.object(_pid_file_lock, "logger") as logger_mock:
        _release_pid_lock(temp_pid_lock_file)

        # The function should have logged about leaving the lock file
        assert logger_mock.warning.call_count == 1, f"{logger_mock.warning.call_args_list}"
        warning_message = logger_mock.warning.call_args.args[0]
        assert "Expected pid lock file does not exist at" in warning_message

        # The lock file should still not exist
        assert not os.path.exists(temp_pid_lock_file)


def test_release_pid_lock_cleans_up_stale_temp_file(temp_pid_lock_file):
    """
    Tests releasing a PID lock when the temp file for claiming the lock hung around.
    """
    # Create the internal implementation detail temporary file for claiming the PID lock
    temp_file_for_claiming = _pid_lock_temp_file_path(temp_pid_lock_file)
    with open(temp_file_for_claiming, "w") as fh:
        fh.write(str(os.getpid()))

    # Also create the pid lock file
    with open(temp_pid_lock_file, "w") as fh:
        fh.write(str(os.getpid()))

    with patch.object(_pid_file_lock, "logger") as logger_mock:
        _release_pid_lock(temp_pid_lock_file)

        # The function should have logged about leaving the lock file
        assert logger_mock.warning.call_count == 1, f"{logger_mock.warning.call_args_list}"
        warning_message = logger_mock.warning.call_args.args[0]
        assert "Cleaned up stale pid lock temporary file" in warning_message

        # The lock file should not exist anymore
        assert not os.path.exists(temp_pid_lock_file)

        # The temporary file for claiming the file should be gone now
        assert not os.path.exists(temp_file_for_claiming)
