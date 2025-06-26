# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
import sys
import psutil
from contextlib import contextmanager
from ..exceptions import DeadlineOperationError
import logging

__all__ = ["PidFileLock", "PidLockAlreadyHeld"]

logger = logging.getLogger(__name__)


class PidLockAlreadyHeld(DeadlineOperationError):
    """Error for when the pid lock is already by a process"""


def _pid_lock_temp_file_path(pid_file_path: str) -> str:
    """Construct the temporary file path used to populate the pid lock file before moving it to the lock file path."""
    return pid_file_path + f"{os.getpid()}~tmp"


def _claim_pid_lock_with_rename(tmp_file_name: str, pid_file_path: str) -> bool:
    # Atomic rename the temporary location to the pid lock file. This operation needs to be
    # atomic: it either succeeds or another process is holding the lock.
    #
    # If the lock is not claimed, the temporary file will remain.
    try:
        if sys.platform.startswith("win32") or sys.platform.startswith("cygwin"):
            # On Windows systems, rename will raise if the destination file exists
            os.rename(tmp_file_name, pid_file_path)
        else:
            # On POSIX systems, link will raise if the destination file exists
            os.link(tmp_file_name, pid_file_path)
            os.remove(tmp_file_name)

        # Successfully claimed the lock
        return True
    except FileExistsError:
        # Another process was holding the lock, as the file exists.
        return False


def _try_acquire_pid_lock(
    pid_file_path: str,
    operation_name: str = "the operation",
):
    """
    Checks if the specified pid lock file exists and executes as per the following:
    If the pid lock file does not exist:
        It creates a new pid file and acquires lock for this pid.
        It handles concurrent processes trying to obtain lock such that only one process gets the lock and others fail

    If the pid lock file exists:
        If the process with its pid is still running, it raises an exception that a download is in progress already
        If the process with its pid is not running it deletes the pid lock file after taking a primitive file lock on it
        to handle concurrent processes making the same check. This will not work if primitive file locks are disabled.

    :param pid_file_full_path: full path of the pid lock file
    :return: boolean, True if pid lock was obtained successfully, throws an exception otherwise
    """
    current_process_id: int = os.getpid()

    # Generate a tmp file for writing the pid file as a whole and prevent corrupt data
    tmp_file_path = _pid_lock_temp_file_path(pid_file_path)

    try:
        # Write the pid lock file to the temporary location
        with open(tmp_file_path, "w+") as f:
            f.write(str(current_process_id))

        # Claim the lock if possible. We do this first so that the normal path does the fewest operations.
        if _claim_pid_lock_with_rename(tmp_file_path, pid_file_path):
            return

        # If we could not claim the lock, inspect it to see whether it's stale and should be deleted,
        # for example if a previous run of the command was terminated from task manager and could not clean up.
        try:
            # Read the lock file. If it does not exist, this raises FileNotFoundError.
            with open(pid_file_path, "r") as f:
                try:
                    lock_holder_pid = int(f.read())
                except ValueError:
                    lock_holder_pid = -1
            # If the lock holder PID is not running, that means it exited without a proper shutdown,
            # and we should clear the lock. Under normal operation this will not occur. There is
            # a race if another process is running and discovers this at the same time. To minimize
            # the time window of the race, reading the lock, checking the pid, and removing the file are
            # done in sequence.
            if lock_holder_pid == -1:
                os.remove(pid_file_path)
                logger.warning("Pid lock file contains incorrect data. Deleted pid lock file.")
            elif not psutil.pid_exists(lock_holder_pid):
                os.remove(pid_file_path)
                logger.warning(
                    f"Process with pid {lock_holder_pid} is not running. Deleted pid lock file."
                )
            else:
                raise PidLockAlreadyHeld(
                    f"Unable to perform {operation_name} as process with pid {lock_holder_pid} already holds the lock {pid_file_path}"
                )
        except FileNotFoundError:
            # In this case, the pid lock is free to acquire
            pass

        # After possibly cleaning up a stale lock, try claiming it again
        if _claim_pid_lock_with_rename(tmp_file_path, pid_file_path):
            return
        else:
            raise PidLockAlreadyHeld(
                f"Unable to perform {operation_name} as process with pid {lock_holder_pid} already holds the lock {pid_file_path}"
            )

    finally:
        # Clean up the pid lock temporary file if necessary
        if os.path.exists(tmp_file_path):
            try:
                os.remove(tmp_file_path)
            except OSError as e:
                logger.warning(f"Failed to clean up pid lock temporary file: {e}")


def _release_pid_lock(pid_file_path: str):
    """
    Releases the pid lock by deleting the pid file.

    :param pid_file_full_path: full path of the pid lock file
    :return: boolean, True if pid lock released successfully
    """
    # Get the current process's id to obtain lock
    current_process_id: int = os.getpid()

    # If an issue occurred with the temporary file during acquisition, delete it here.
    tmp_file_name = _pid_lock_temp_file_path(pid_file_path)
    if os.path.exists(tmp_file_name):
        try:
            os.remove(tmp_file_name)
            logger.warning(f"Cleaned up stale pid lock temporary file: {tmp_file_name}")
        except OSError as e:
            logger.warning(f"Failed to clean up pid lock temporary file: {e}")

    # Check if pid lock file does not exist. Returns if file doesn't exist as there is no lock to be released.
    if not os.path.exists(pid_file_path):
        logger.warning(f"Expected pid lock file does not exist at {pid_file_path}")
        return

    # Try to open pid file at download progress location in read mode
    with open(pid_file_path, "r") as f:
        # Read pid file and obtain the process id from file contents
        lock_holder_pid = f.read()

    if lock_holder_pid == str(current_process_id):
        # Process pid from file is same as current process pid - release pid lock
        os.remove(pid_file_path)
    else:
        # Process pid from file is different from current process pid.
        logger.warning(
            f"Another process with pid {lock_holder_pid} claimed the pid lock {pid_file_path} while {current_process_id} was holding it. Skipping pid file deletion."
        )


@contextmanager
def PidFileLock(
    lock_file_path: str,
    operation_name: str = "the operation",
):
    """
    A context manager for holding a pid (process id) lock file during the scope of a 'with' statement.
    A pid lock file lets you prevent concurrent execution of the same CLI command. For example,
    a command to repeatedly download the new output available from a Deadline Cloud queue could
    use this to ensure only one running command is calculating and downloading what to output at a time.

    Example:
        with PidLockFile("/path/to/lock/file", operation_name="incremental output download"):
            # Code to load the checkpoint, do the download, save the new checkpoint...

    Args:
        lock_file_path (str): The file system path of the PID lock file.
        operation_name (Optional[str]): The name of the operation being performed in the lock, used for error messages.
    """
    _try_acquire_pid_lock(lock_file_path, operation_name)
    try:
        yield None
    finally:
        _release_pid_lock(lock_file_path)
