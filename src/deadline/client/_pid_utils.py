# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
import psutil
from deadline.client.cli._groups.click_logger import ClickLogger

PID_FILE_NAME = "incremental_output_download.pid"


def check_and_obtain_pid_lock_if_available(
    saved_progress_checkpoint_location: str, logger: ClickLogger
) -> bool:
    """
    Checks if the download progress file exists and if it does, it checks if the process is still running.
    If the process is still running, it raises an exception.
    If the process is not running, it deletes the download progress file.
    If the download progress file does not exist, it creates a new one.
    :param saved_progress_checkpoint_location: location of the download progress file
    :param logger: Click logger component
    :return:
    """
    logger.echo(
        f"Checking if another download is in progress at {saved_progress_checkpoint_location}"
    )
    # Get the full path of the pid file at download progress location
    pid_file_full_path = os.path.join(saved_progress_checkpoint_location, PID_FILE_NAME)
    try:
        # Check if download progress file does not exist.
        if not os.path.exists(pid_file_full_path):
            logger.echo(f"Download progress file does not exist at {pid_file_full_path}")
            # Create a new pid file with the current process id
            return _obtain_pid_lock_atomically(pid_file_full_path, logger)

        # Try to open pid file at download progress location in read mode
        with open(pid_file_full_path, "r") as f:
            # Read pid file and obtain the process id from file contents
            pid = f.read()
            try:
                # Get process using the process id
                psutil.Process(int(pid))
                # Process with the pid exists, so we cannot obtain a lock
                raise RuntimeError(
                    f"Another download is in progress at {saved_progress_checkpoint_location}, use --force-bootstrap or wait for previous download to finish"
                )
            except psutil.NoSuchProcess:
                # No such process exists with the process id, so we can delete the pid file
                logger.echo(f"Process with pid {pid} is not running. Deleting pid file.")

                f.close()  # Close the file before over-writing it

                # Create a new pid file with the current process id
                return _obtain_pid_lock_atomically(pid_file_full_path, logger)

    except Exception:
        # We already checked the file exists before reading it.
        # For any other unexpected exceptions, we should not delete the pid file and raise an error.
        raise


def _obtain_pid_lock_atomically(pid_file_full_path: str, logger: ClickLogger) -> bool:
    """
    Obtains a lock on the pid file by writing the current process id to the file.
    :param logger:
    :param pid_file_full_path:
    :return: boolean, True if pid lock was obtained successfully
    """

    # Generate a tmp file for writing the pid file as a whole and making the pid locking atomic
    tmp_file_name = pid_file_full_path + "~tmp"

    # Get the current process id to write to pid file
    text = str(os.getpid())

    logger.echo(f"Creating new pid file at {pid_file_full_path} with pid {text}")

    # Open tmp file in write mode and write the current process id to it
    with open(tmp_file_name, "w+") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())

    # Replace pid_file_full_path with tmp_file
    os.replace(tmp_file_name, pid_file_full_path)

    return True
