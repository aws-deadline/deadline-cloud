# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import json
import os
from datetime import datetime, timezone, timedelta
from typing import Optional, Callable, List


class IncrementalDownloadState:
    """
    Model representing the download progress state file structure.
    Incremental download state file structure:
    {
        "lastLookbackTime": "2025-04-04T05:30:00",
        "jobs":
        [
            {
                "jobId": "job-1234353453443",
                "sessions": [
                {
                    "sessionId": "session-1324324354354",
                    "sessionLifecycleStatus": "SUCCESSFUL",
                    "lastDownloadedSessActionId": 3
                },
                {
                    "sessionId": "session-3423435435454",
                    "sessionLifecycleStatus": "RUNNING",
                    "lastDownloadedSessActionId": 6
                }
                ]
            },
            {
                "jobId": "job-3234324354345",
                "sessions": [
                {
                    "sessionId": "session-4235435434345",
                    "sessionLifecycleStatus": "FAILED",
                    "lastDownloadedSessActionId": 3
                }
                ]
            }
        ]
    }
    """

    last_lookback_time: datetime
    jobs: List[dict]

    def __init__(self, last_lookback_time=None, jobs=None):
        """
        Initialize a IncrementalDownloadState instance.
        Args:
            last_lookback_time (datetime): datetime format timestamp of the last lookback time
            jobs (list): List of job dictionaries containing job_id and sessions information
        """
        self.last_lookback_time = last_lookback_time
        self.jobs = jobs or []

    @classmethod
    def from_dict(cls, data):
        """
        Create a IncrementalDownloadState instance from a dictionary.
        Args:
            data (dict): Dictionary containing state file data
        Returns:
            IncrementalDownloadState: A new instance populated with the data
        """
        if not data:
            return cls()

        return cls(last_lookback_time=data.get("lastLookbackTime"), jobs=data.get("jobs", []))

    def to_dict(self):
        """
        Convert the IncrementalDownloadState to a dictionary.
        Returns:
            dict: Dictionary representation of the state file model
        """
        return {"lastLookbackTime": self.last_lookback_time.isoformat(), "jobs": self.jobs}

    @classmethod
    def from_bootstrap(
        cls,
        bootstrap_lookback_in_minutes: Optional[int],
        print_function_callback: Callable[[str], None],
    ) -> "IncrementalDownloadState":
        """
        Bootstraps fresh download progress state using bootstrap lookback if provided or now
        :param bootstrap_lookback_in_minutes: lookback period in minutes
        :param print_function_callback: Callback to print messages produced in this function.
                Used in the CLI to print to stdout using click.echo. By default, ignores messages.
        :return: returns a fresh download progress state
        """
        current_download_progress: IncrementalDownloadState = IncrementalDownloadState()

        print_function_callback(
            "Bootstrapping command. Ignoring download progress location and creating new"
        )
        current_download_progress.last_lookback_time = datetime.now(timezone.utc) - timedelta(
            minutes=float(bootstrap_lookback_in_minutes or 0)
        )
        return current_download_progress

    @classmethod
    def from_file(
        cls,
        saved_progress_checkpoint_full_path: str,
        print_function_callback: Callable[[str], None],
    ) -> "IncrementalDownloadState":
        """
        Loads progress from state file saved at saved_progress_checkpoint_full_path
        :param saved_progress_checkpoint_full_path: full path of the saved progress checkpoint file
        :param print_function_callback: Callback to print messages produced in this function.
                Used in the CLI to print to stdout using click.echo. By default, ignores messages.
        :return: Returns the loaded state file,
        or throws an exception if we're unable to read it as we already validated its existence
        """
        try:
            state_data: dict = {}
            with open(saved_progress_checkpoint_full_path, "r") as file:
                state_data = json.load(file)

            current_download_progress: IncrementalDownloadState = (
                IncrementalDownloadState.from_dict(state_data)
            )
            print_function_callback(
                f"Loaded existing state file from download progress checkpoint location {saved_progress_checkpoint_full_path}"
            )
            return current_download_progress

        except Exception as e:
            print_function_callback(
                f"Failed to load existing state file from download progress checkpoint location {saved_progress_checkpoint_full_path}: {str(e)}"
            )
            # Raise as this is an unexpected exception in reading state file, we already checked its existence earlier
            raise

    def save_file(
        self,
        saved_progress_checkpoint_full_path: str,
        print_function_callback: Callable[[str], None],
    ) -> None:
        """
        Save the current download progress to a state file atomically.

        :param saved_progress_checkpoint_full_path: Absolute path of file with saved progress
        :param print_function_callback: Callback to print messages produced in this function.
                Used in the CLI to print to stdout using click.echo. By default, ignores messages
        :return: None if save was successful,
        or throws an exception if we're unable to save progress file to download location.
            Hard fail for customer's assets to be downloaded EXACTLY once.
        """

        try:
            # 1. Create directory if it doesn't exist
            os.makedirs(os.path.dirname(saved_progress_checkpoint_full_path), exist_ok=True)

            # 2. Convert the IncrementalDownloadState to a dictionary
            state_data = self.to_dict()

            # 3. Get a unique id for atomic file update and create a temporary file in the given directory
            unique_process_identifier = str(os.getpid())
            temp_file_path = (
                f"{saved_progress_checkpoint_full_path}_{unique_process_identifier}.tmp"
            )

            # 4. Write the state data to the temporary file
            with open(temp_file_path, "w") as file:
                json.dump(state_data, file, indent=2)

            # 5. Atomically replace the target file with the temporary file
            os.replace(temp_file_path, saved_progress_checkpoint_full_path)

            print_function_callback(
                f"Successfully saved state file to {saved_progress_checkpoint_full_path}"
            )
        except Exception as e:
            print_function_callback(
                f"Failed to save state file to {saved_progress_checkpoint_full_path}: {str(e)}"
            )

            # Raise as this is an unexpected exception in saving state file,
            # Hard fail for customer's files to be downloaded EXACTLY once.
            raise
