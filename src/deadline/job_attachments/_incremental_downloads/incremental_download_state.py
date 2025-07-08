# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Optional
import tempfile

# This as an upper bound to allow for eventual consistency into the materialized view that
# the deadline:SearchJobs API is based on. It's taken from numbers seen in heavy load testing,
# increased by a generous amount.
EVENTUAL_CONSISTENCY_MAX_SECONDS = 120


def _datetimes_to_str(obj: Any) -> Any:
    """Recursively applies the isoformat() function to all datetimes in the object"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, list):
        return [_datetimes_to_str(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: _datetimes_to_str(value) for key, value in obj.items()}
    else:
        return obj


class IncrementalDownloadJob:
    """
    Model representing a job in the download progress state.
    """

    _required_dict_fields = ["job"]

    job: dict[str, Any]
    session_ended_timestamp: Optional[datetime]
    session_completed_indexes: dict[str, int]

    def __init__(
        self,
        job: dict[str, Any],
        session_ended_timestamp: Optional[datetime],
        session_completed_indexes: Optional[dict[str, int]],
    ):
        """
        Initialize a Job instance.
        Args:
            job (dict[str, Any]): The job as returned by boto3 from deadline:SearchJobs.
            session_ended_timestamp (Optional[datetime]): The largest endedAt timestamp for a session
                whose output has been downloaded. This can be None only when the job lacks job attachments.
            session_completed_index (dict[str, int]): A mapping from session id to the index
                of the latest completed session action download.
        """
        self.job = _datetimes_to_str(job)
        self.session_ended_timestamp = session_ended_timestamp
        self.session_completed_indexes = session_completed_indexes or {}

    @property
    def job_id(self) -> str:
        return self.job["jobId"]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "IncrementalDownloadJob":
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

        job = data["job"]
        session_completed_indexes = data.get("sessionCompletedIndexes", {})
        session_ended_timestamp = (
            datetime.fromisoformat(data["sessionEndedTimestamp"])
            if data.get("sessionEndedTimestamp") is not None
            else None
        )
        return cls(
            job=job,
            session_ended_timestamp=session_ended_timestamp,
            session_completed_indexes=session_completed_indexes,
        )

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the Job to a dictionary.
        Returns:
            dict: Dictionary representation of the job
        """
        result: dict[str, Any] = {
            "job": self.job,
        }
        if self.session_ended_timestamp is not None:
            result["sessionEndedTimestamp"] = self.session_ended_timestamp.isoformat()
        if self.session_completed_indexes != {}:
            result["sessionCompletedIndexes"] = self.session_completed_indexes
        return result


class IncrementalDownloadState:
    """
    Model for tracking all the job attachments downloads to perform for a queue over time.
    A new download becomes available whenever a TASK_RUN session action completes.

    This class includes some informational fields that are not strictly necessary, to help make the data
    on disk easier to understand on inspection.

    * https://docs.aws.amazon.com/deadline-cloud/latest/APIReference/API_GetSessionAction.html#API_GetSessionAction_ResponseSyntax
    * https://docs.aws.amazon.com/deadline-cloud/latest/APIReference/API_SessionActionDefinition.html

    The Deadline Cloud APIs do not provide direct access to a stream of completed session actions, so we reconstruct such
    a stream by tracking state at three levels. Where possible, we use the resource state at one level to prune queries at lower levels:

    1. Job - The jobs list contains every job that is active and that we have downloaded output from in a previous incremental download command.
            When a job becomes inactive, it tracks a minimal stub including the sessionEndedTimestamp value, to use for detecting
            requeued jobs later.
    2. Session - Each session of a job represents a single worker running a sequence of tasks from the job. The sessionCompletedIndexes
            member of the IncrementalDownloadJob contains an entry for every session that is either still running, or whose
            endedAt field is >= the downloadsCompletedTimestamp. When a job gets requeued, the sessionEndedTimestamp stored in the minimal
            stub lets us skip sessions from before the job was requeued.
    3. SessionAction - Session actions have sequential IDs, so for each session we store the highest index of session action
            for which we have completed the download. A session action ID looks like "sessionaction-abc123-12" for session action
            index 12.
    """

    _required_dict_fields = [
        "downloadsStartedTimestamp",
        "downloadsCompletedTimestamp",
        "eventualConsistencyMaxSeconds",
        "jobs",
    ]

    downloads_started_timestamp: datetime
    """The timestamp of when the download state was bootstrapped."""
    downloads_completed_timestamp: datetime
    """The timestamp up to which we are confident downloads are complete."""
    eventual_consistency_max_seconds: int = EVENTUAL_CONSISTENCY_MAX_SECONDS
    """The duration for deadline:SearchJobs query overlap, to account for eventual consistency."""

    jobs: list[IncrementalDownloadJob]
    """The list of jobs that entered 'active' status between downloads_started_timestamp and downloads_completed_timestamp, and are not completed."""

    def __init__(
        self,
        downloads_started_timestamp: datetime,
        downloads_completed_timestamp: Optional[datetime] = None,
        jobs: Optional[list] = None,
        eventual_consistency_max_seconds: Optional[int] = None,
    ):
        """
        Initialize a IncrementalDownloadState instance. To bootstrap the state, construct with only the downloads_started_timestamp.

        Args:
            downloads_started_timestamp (datetime): The timestamp of when the download state was bootstrapped.
            downloads_completed_timestamp (datetime): The timestamp up to which we are confident downloads are complete.
            jobs (list[IncrementalDownloadJob]): The list of jobs that entered 'active' status between downloads_started_timestamp
                    and downloads_completed_timestamp, and are not completed.
            eventual_consistency_max_seconds (Optional[int]): The duration, in seconds, for deadline:SearchJobs query overlap, to account for eventual consistency.
        """
        self.downloads_started_timestamp = downloads_started_timestamp
        if downloads_completed_timestamp is not None:
            self.downloads_completed_timestamp = downloads_completed_timestamp
        else:
            self.downloads_completed_timestamp = downloads_started_timestamp
        if eventual_consistency_max_seconds:
            self.eventual_consistency_max_seconds = eventual_consistency_max_seconds
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
            eventual_consistency_max_seconds=int(data["eventualConsistencyMaxSeconds"]),
            jobs=[IncrementalDownloadJob.from_dict(job) for job in data["jobs"]],
        )

    def to_dict(self):
        """
        Convert the IncrementalDownloadState to a dictionary.
        Returns:
            dict: Dictionary representation of the state file model
        """
        result = {
            "downloadsStartedTimestamp": self.downloads_started_timestamp.isoformat(),
            "eventualConsistencyMaxSeconds": self.eventual_consistency_max_seconds,
            "jobs": [job.to_dict() for job in self.jobs],
        }
        if self.downloads_completed_timestamp is not None:
            result["downloadsCompletedTimestamp"] = self.downloads_completed_timestamp.isoformat()

        return result

    @classmethod
    def from_file(
        cls,
        file_path: str,
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
        return download_state

    def save_file(
        self,
        file_path: str,
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
