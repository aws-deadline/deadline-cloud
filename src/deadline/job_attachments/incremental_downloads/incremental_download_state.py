# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any, Optional, Callable
import tempfile


class IncrementalDownloadJob:
    """
    Model representing a job in the download progress state.
    """

    _required_dict_fields = ["jobId", "sessions"]

    job_id: str
    sessions: list

    def __init__(self, job_id: str, sessions: Optional[list] = None):
        """
        Initialize a Job instance.
        Args:
            job_id (str): The ID of the job
            sessions (list): List of JobSession objects
        """
        self.job_id = job_id
        self.sessions = sessions or []

    @classmethod
    def from_dict(cls, data: dict[str, Any]):
        """
        Create a Job instance from a dictionary.
        Args:
            data (dict): Dictionary containing job data
        Returns:
            Job: A new instance populated with the data
        """
        if not isinstance(data, dict):
            raise ValueError("Input must be a dict.")
        missing_fields = [field for field in cls._required_dict_fields if field not in data]
        if missing_fields:
            raise ValueError(f"Input is missing required fields: {missing_fields}")

        sessions = data["sessions"]
        return cls(job_id=data["jobId"], sessions=sessions)

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the Job to a dictionary.
        Returns:
            dict: Dictionary representation of the job
        """
        return {"jobId": self.job_id, "sessions": self.sessions}


class IncrementalDownloadState:
    """
    Model for tracking all the job attachments downloads to perform for a queue over time.
    A new download becomes available whenever a TASK_RUN session action completes. The state
    includes some informational fields that are not strictly necessary, to help make the data
    on disk easier to understand on inspection.

    * https://docs.aws.amazon.com/deadline-cloud/latest/APIReference/API_GetSessionAction.html#API_GetSessionAction_ResponseSyntax
    * https://docs.aws.amazon.com/deadline-cloud/latest/APIReference/API_SessionActionDefinition.html

    We track state at three levels, and use the resource state at one level to prune queries at lower levels when we can:

    1. Job - The jobs list contains every job that entered an active status within the time interval [downloads_started_timestamp, downloads_completed_timestamp],
            where it can generate new task runs, and has not exited as complete or failed.
    2. Session - Each session of a job represents a single worker running a sequence of tasks from the job. The sessions list in
            a job contains all the sessions that are active and from which we have downloaded some output.
    3. SessionAction - Session actions have sequential IDs, so for each session we track the highest index of session action
            for which we have performed the download.



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

    _required_dict_fields = [
        "downloadsStartedTimestamp",
        "downloadsCompletedTimestamp",
        "eventualConsistencyMaxSeconds",
        "jobs",
    ]

    downloads_started_timestamp: datetime
    """The timestamp of when the download state was bootstrapped."""
    downloads_completed_timestamp: Optional[datetime]
    """The timestamp up to which we are confident downloads are complete."""
    eventual_consistency_max_duration: timedelta = timedelta(seconds=120)
    """The duration for deadline:SearchJobs query overlap, to account for eventual consistency."""

    jobs: list[IncrementalDownloadJob]
    """The list of jobs that entered 'active' status between downloads_started_timestamp and downloads_completed_timestamp, and are not completed."""

    def __init__(
        self,
        downloads_started_timestamp: datetime,
        downloads_completed_timestamp: Optional[datetime] = None,
        jobs: Optional[list] = None,
        eventual_consistency_max_duration: Optional[timedelta] = None,
    ):
        """
        Initialize a IncrementalDownloadState instance. To bootstrap the state, construct with only the downloads_started_timestamp.

        Args:
            downloads_started_timestamp (datetime): The timestamp of when the download state was bootstrapped.
            downloads_completed_timestamp (datetime): The timestamp up to which we are confident downloads are complete.
            jobs (list[IncrementalDownloadJob]): The list of jobs that entered 'active' status between downloads_started_timestamp
                    and downloads_completed_timestamp, and are not completed.
            eventual_consistency_max_duration (Optional[timedelta]): The duration for deadline:SearchJobs query overlap, to account for eventual consistency.
        """
        self.downloads_started_timestamp = downloads_started_timestamp
        self.downloads_completed_timestamp = downloads_completed_timestamp
        if eventual_consistency_max_duration:
            self.eventual_consistency_max_duration = eventual_consistency_max_duration
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
        if not isinstance(data, dict):
            raise ValueError("Input must be a dict.")
        missing_fields = [field for field in cls._required_dict_fields if field not in data]
        if missing_fields:
            raise ValueError(f"Input is missing required fields: {missing_fields}")

        return cls(
            downloads_started_timestamp=datetime.fromisoformat(data["downloadsStartedTimestamp"]),
            downloads_completed_timestamp=datetime.fromisoformat(
                data["downloadsCompletedTimestamp"]
            ),
            eventual_consistency_max_duration=timedelta(
                seconds=int(data["eventualConsistencyMaxSeconds"])
            ),
            jobs=data["jobs"],
        )

    def to_dict(self):
        """
        Convert the IncrementalDownloadState to a dictionary.
        Returns:
            dict: Dictionary representation of the state file model
        """
        result = {
            "downloadsStartedTimestamp": self.downloads_started_timestamp.isoformat(),
            "eventualConsistencyMaxSeconds": self.eventual_consistency_max_duration.total_seconds(),
            "jobs": [job.to_dict() for job in self.jobs],
        }
        if self.downloads_completed_timestamp is not None:
            result["downloadsCompletedTimestamp"] = self.downloads_completed_timestamp.isoformat()

        return result

    @classmethod
    def from_file(
        cls,
        file_path: str,
        print_function_callback: Callable[[str], None] = print,
    ) -> "IncrementalDownloadState":
        """
        Loads progress from state file saved at saved_progress_checkpoint_full_path
        :param saved_progress_checkpoint_full_path: full path of the saved progress checkpoint file
        :param print_function_callback: Callback to print messages produced in this function.
                Used in the CLI to print to stdout using click.echo. By default, ignores messages.
        :return: Returns the loaded state file,
        or throws an exception if we're unable to read it as we already validated its existence
        """
        state_data: dict = {}
        with open(file_path, "r") as file:
            state_data = json.load(file)

        download_state = IncrementalDownloadState.from_dict(state_data)
        print_function_callback(
            f"Loaded existing state file from download progress checkpoint location {file_path}"
        )
        return download_state

    def save_file(
        self,
        file_path: str,
        print_function_callback: Callable[[str], None] = print,
    ) -> None:
        """
        Save the current download progress to a state file atomically.

        :param file_path: Where to save the file.
        :param print_function_callback: Callback to print messages produced in this function.
                Used in the CLI to print to stdout using click.echo. By default, ignores messages
        :return: None if save was successful, throws an exception if we're unable to save progress file to download location.
        """
        # Create directory if it doesn't exist
        file_dir = os.path.dirname(file_path)
        os.makedirs(file_dir, exist_ok=True)

        state_data = self.to_dict()

        # Write the data to a unique temporary filename
        with tempfile.NamedTemporaryFile(
            mode="w",
            dir=file_dir,
            prefix=os.path.basename(file_path),
            encoding="utf-8",
            delete=False,
        ) as tmpfile:
            json.dump(state_data, tmpfile.file, indent=2)

        # Atomically replace the target file with the temporary file
        os.replace(tmpfile.name, file_path)

        print_function_callback(f"Successfully saved state file to {file_path}")
