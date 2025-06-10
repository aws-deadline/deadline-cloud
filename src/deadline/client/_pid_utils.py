# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
import sys
import psutil
from typing import Callable
from deadline.job_attachments.incremental_downloads.exceptions import PidLockAlreadyHeld


def try_acquire_pid_lock(
    pid_file_full_path: str, print_function_callback: Callable[[str], None]
) -> bool:
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
    :param print_function_callback: Callback to print messages produced in this function.
                Used in the CLI to print to stdout using click.echo. By default, ignores messages.
    :return: boolean, True if pid lock was obtained successfully, throws an exception otherwise
    """
    print_function_callback(f"Checking if another download is in progress at {pid_file_full_path}")

    # 1. Get the current process's id to obtain lock
    current_process_pid: int = os.getpid()
    can_obtain_pid_lock: bool = False

    try:
        # 2. Check if pid file does not exist at pid file path in which case we can obtain a pid lock for this process
        if not os.path.exists(pid_file_full_path):
            print_function_callback(
                f"Download pid lock file does not exist at {pid_file_full_path}"
            )

            can_obtain_pid_lock = True

        # 3. Try to read existing pid file to verify process with the pid lock is active & hasn't ended pre-maturely
        else:
            with open(pid_file_full_path, "r") as f:
                pid = f.read()
                # Check if a process does not exist with the process id, so we can delete the pid file
                # Once we've determined the process in pid file isn't running we close the file and delete it
                # This will keep race conditions across concurrent processes trying this to be least possible without locking
                # TODO Try to minimize potential race conditions from concurrent processes trying to remove pid file
                if not psutil.pid_exists(int(pid)):
                    f.close()  # Required for windows
                    os.remove(pid_file_full_path)
                    print_function_callback(
                        f"Process with pid {pid} is not running. Deleted pid file."
                    )
                    can_obtain_pid_lock = True

                # Process with the pid in the file is active
                else:
                    print_function_callback(
                        f"Unable to acquire pid lock as process with pid {pid} exists on {pid_file_full_path}"
                    )
                    raise PidLockAlreadyHeld(
                        f"Unable to acquire pid lock as process with pid {pid} exists on {pid_file_full_path}"
                    )

    except Exception:
        # For any other unexpected exceptions, we should raise an error.
        raise

    # 4. If we've determined we can obtain lock for this process, we do so now.
    # We handle concurrency for processes racing for this pid lock by using atomic primitives
    # The pid lock is obtained successfully for one process and fails for the other process
    # We use os.rename() for windows and os.link() for linux/macOS as atomic primitives
    if can_obtain_pid_lock:
        # Generate a tmp file for writing the pid file as a whole and prevent corrupt data
        tmp_file_name = pid_file_full_path + f"{current_process_pid}~tmp"

        print_function_callback(
            f"Creating new pid file at {pid_file_full_path} with pid {current_process_pid}"
        )

        # Open tmp file in write mode and write the current process id to it
        with open(tmp_file_name, "w+") as f:
            f.write(str(current_process_pid))
            f.flush()
            os.fsync(f.fileno())

        # Atomic write to pid file while handling concurrency for parallel processes trying to race for the pid lock
        try:
            if sys.platform.startswith("win32") or sys.platform.startswith("cygwin"):
                os.rename(tmp_file_name, pid_file_full_path)
            else:
                os.link(tmp_file_name, pid_file_full_path)
                os.remove(tmp_file_name)
        except FileExistsError:
            print_function_callback(
                f"Concurrency issue when trying to obtain pid lock at {pid_file_full_path} for process id {current_process_pid}"
            )
            raise

    return True


def release_pid_lock(
    pid_file_full_path: str, print_function_callback: Callable[[str], None]
) -> bool:
    """
    Releases the pid lock by deleting the pid file.
    :param pid_file_full_path: full path of the pid lock file
    :param print_function_callback: print_function_callback (Callable str -> None, optional): Callback to print messages produced in this function.
                Used in the CLI to print to stdout using click.echo. By default, ignores messages.
    :return: boolean, True if pid lock released successfully
    """
    print_function_callback(f"Releasing pid lock at {pid_file_full_path}")

    # Get the current process's id to obtain lock
    current_process_pid: int = os.getpid()

    # Check if pid lock file does not exist. Returns True if file doesn't exist as there is no lock to be released.
    if not os.path.exists(pid_file_full_path):
        print_function_callback(f"Pid lock file does not exist at {pid_file_full_path}")
        return True

    # Try to open pid file at download progress location in read mode
    with open(pid_file_full_path, "r") as f:
        # Read pid file and obtain the process id from file contents
        pid = f.read()

    # Process pid from file is same as current process pid - release pid lock
    if int(pid) == current_process_pid:
        print_function_callback(
            f"Process with pid {pid} is the current process. Deleting pid file."
        )
        os.remove(pid_file_full_path)
        return True
    # Process pid from file is different from current process pid - do not release lock
    else:
        print_function_callback(
            f"Process with pid {pid} is not the current process. Skipping pid file deletion."
        )
        return False
