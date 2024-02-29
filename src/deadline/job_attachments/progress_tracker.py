# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from collections import Counter
from logging import Logger, LoggerAdapter

import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from threading import Lock
from typing import Callable, Dict, Optional, Union

from deadline.job_attachments._utils import _human_readable_file_size

CALLBACK_INTERVAL = 1  # in seconds
MAX_FILES_IN_CHUNK = 50
LOG_INTERVAL = 300  # in seconds
LOG_PERCENTAGE_THRESHOLD = 10  # in percentage


@dataclass
class SummaryStatistics:
    """
    A summary statistics metadata to be returned to the client when processing of files
    (hashing or uploading) has completed.
    The `skipped_files` refers to:
    - if this statistics is for hashing operation: the number of files whose hashing is
    skipped by the hash cache.
    - if this statistics is for uploading operation: the number of files that have already
    been uploaded to S3 bucket and thus skipped uploading.
    """

    total_time: float = 0.0  # time (in fractional seconds) taken to perform hashing or uploading
    total_files: int = 0
    total_bytes: int = 0
    processed_files: int = 0
    processed_bytes: int = 0
    skipped_files: int = 0
    skipped_bytes: int = 0
    transfer_rate: float = 0.0  # bytes/second

    def aggregate(self, other: SummaryStatistics) -> SummaryStatistics:
        """
        Aggregates other object of SummaryStatistics to this.
        """
        if not isinstance(other, self.__class__):
            raise TypeError("Only instances of the same type can be aggregated.")
        self.total_time += other.total_time
        self.total_files += other.total_files
        self.total_bytes += other.total_bytes
        self.processed_files += other.processed_files
        self.processed_bytes += other.processed_bytes
        self.skipped_files += other.skipped_files
        self.skipped_bytes += other.skipped_bytes
        self.transfer_rate = self.processed_bytes / self.total_time if self.total_time else 0.0

        return self

    def __str__(self):
        return (
            f"Processed {self.processed_files} file{'' if self.processed_files == 1 else 's'}"
            + f" totaling {_human_readable_file_size(self.processed_bytes)}.\n"
            + f"Skipped re-processing {self.skipped_files} files totaling"
            + f" {_human_readable_file_size(self.skipped_bytes)}.\n"
            + f"Total processing time of {round(self.total_time, ndigits=5)} seconds"
            + f" at {_human_readable_file_size(int(self.transfer_rate))}/s.\n"
        )


@dataclass
class DownloadSummaryStatistics(SummaryStatistics):
    """
    A summary statistics metadata to be returned to the client when the downloading files has
    completed. In addition to the general statistics, includes a dict mapping download locations
    to the number of downloaded files in each of those locations.
    """

    file_counts_by_root_directory: Dict[str, int] = field(default_factory=dict)

    def aggregate(self, other: SummaryStatistics) -> SummaryStatistics:
        """
        Aggregates other object of DownloadSummaryStatistics to this.
        """
        super().aggregate(other)
        if not hasattr(other, "file_counts_by_root_directory"):
            raise TypeError(
                f"{other.__class__.__name__} does not have a file_counts_by_root_directory field."
            )
        else:
            self.file_counts_by_root_directory = dict(
                Counter(self.file_counts_by_root_directory)
                + Counter(other.file_counts_by_root_directory)
            )

        return self

    def convert_to_summary_statistics(self) -> SummaryStatistics:
        """
        Converts this DownloadSummaryStatistics to a SummaryStatistics.
        """
        download_summary_statistics_dict = asdict(self)
        del download_summary_statistics_dict["file_counts_by_root_directory"]
        return SummaryStatistics(**download_summary_statistics_dict)


class ProgressStatus(Enum):
    """
    Represents the current stage of asset/file processing
    """

    NONE = ("NONE", "")
    """The asset manager is not assigned any work."""

    PREPARING_IN_PROGRESS = ("PREPARING_IN_PROGRESS", "Processed")
    """The asset manager is hashing files."""

    UPLOAD_IN_PROGRESS = ("UPLOAD_IN_PROGRESS", "Uploaded")
    """The asset manager is uploading files."""

    DOWNLOAD_IN_PROGRESS = ("DOWNLOAD_IN_PROGRESS", "Downloaded")
    """Downloading files"""

    def __init__(self, title, verb_in_message):
        self.title = title
        self.verb_in_message = verb_in_message


@dataclass
class ProgressReportMetadata:
    """
    A metadata (with defined key-value pairs) about the progress to be reported
    back to client during file upload/downloads. Within this metadata will be
    a status message and progress(%) of the hashing, uploads or downloads of files.
    """

    status: ProgressStatus
    progress: float  # percentage with one decimal place
    transferRate: float  # bytes/second
    progressMessage: str  # pylint: disable=invalid-name


@dataclass
class ProgressTracker:
    """
    A class that records the progress of file processing, and reports the
    progress data back to the client using callbacks passed from the client.
    The process is one of the following - hashing, uploading, or downloading.
    """

    def __init__(
        self,
        status: ProgressStatus,
        total_files: int,
        total_bytes: int,
        on_progress_callback: Optional[Callable[[ProgressReportMetadata], bool]] = None,
        callback_interval: int = CALLBACK_INTERVAL,
        max_files_in_chunk: int = MAX_FILES_IN_CHUNK,
        logger: Optional[Union[Logger, LoggerAdapter]] = None,
        log_interval: int = LOG_INTERVAL,
        log_percentage_threshold: int = LOG_PERCENTAGE_THRESHOLD,
    ) -> None:
        def do_nothing(*args, **kwargs) -> bool:
            return True

        if not on_progress_callback:
            on_progress_callback = do_nothing

        self.on_progress_callback = on_progress_callback
        self.continue_reporting = True

        self.reporting_interval = callback_interval
        self.reporting_files_per_chunk = 1
        self.max_files_in_chunk = max_files_in_chunk
        self.completed_files_in_chunk = 0
        self.last_report_time: Optional[float] = None
        self.last_report_processed_bytes: int = 0

        self.logger = logger
        self.log_interval = log_interval
        self.log_percentage_threshold = log_percentage_threshold
        self.last_logged_time: Optional[float] = None
        self.last_logged_completed_bytes: int = 0

        self.status = status
        self.total_files = total_files
        self.total_bytes = total_bytes
        if self.total_files >= self.max_files_in_chunk:
            self.reporting_files_per_chunk = self.max_files_in_chunk
        self.processed_files = 0
        self.processed_bytes = 0
        self.skipped_files = 0
        self.skipped_bytes = 0
        self.total_time = 0.0  # total time (in fractional seconds) taken for the process

        self._lock = Lock()

        def track_progress(bytes_amount: int, current_file_done: Optional[bool] = False) -> bool:
            """
            When uploading or downloading files using boto3, pass this to the `Callback` argument
            so that the progress can be updated with the amount of bytes processed.
            """
            with self._lock:
                self._initialize_timestamps_if_none()
                self.processed_bytes += bytes_amount
                if current_file_done:
                    self.processed_files += 1
                    self.completed_files_in_chunk += 1
                # Logs progress message to the logger (if exists)
                self._log_progress_message()
                # Invokes the callback with current progress data
                return self._report_progress()

        self.track_progress_callback = track_progress

    def set_total_files(self, total_files, total_bytes) -> None:
        """
        Stores the number and size of files to be processed.
        """
        self.total_files = total_files
        self.total_bytes = total_bytes
        if self.total_files >= self.max_files_in_chunk:
            self.reporting_files_per_chunk = self.max_files_in_chunk

    def _initialize_timestamps_if_none(self) -> None:
        """
        This is to initialize the `last_report_time` and `last_logged_time` to the
        current time in alignment with the start of process (i.e., hashing, uploading,
        or downloading.) These are used to calculate the current hashing or transfer
        rate for the first progress report, which is the first callback invocation or
        the first logging.
        """
        current_time = time.perf_counter()
        if self.last_report_time is None:
            self.last_report_time = current_time
        if self.last_logged_time is None:
            self.last_logged_time = current_time

    def increase_processed(self, num_files: int = 1, file_bytes: int = 0) -> None:
        """
        Adds the number and size of processed files.
        """
        with self._lock:
            self._initialize_timestamps_if_none()
            self.processed_files += num_files
            self.completed_files_in_chunk += num_files
            self.processed_bytes += file_bytes

    def increase_skipped(self, num_files: int = 1, file_bytes: int = 0) -> None:
        """
        Adds the number and size of skipped files.
        """
        with self._lock:
            self.skipped_files += num_files
            self.completed_files_in_chunk += num_files
            self.skipped_bytes += file_bytes

    def _report_progress(self) -> bool:
        """
        Invokes the callback with current progress metadata in one of the following cases:
        1. when a specific time interval has passed since the last call, (or since the
           process started,) or
        2. when a specific number of files (a chunk) has been processed, or
        3. when the progress is 100%, (including when all files were skipped during the process.)

        Sets the flag `continue_reporting` True if the operation should continue as normal,
        or False to cancel, and returns the flag.
        """
        if not self.continue_reporting:
            return False

        current_time = time.perf_counter()
        if (
            self.last_report_time is None
            or current_time - self.last_report_time >= self.reporting_interval
            or self.completed_files_in_chunk >= self.reporting_files_per_chunk
            or self.processed_files + self.skipped_files == self.total_files
        ):
            self.continue_reporting = self.on_progress_callback(
                self._get_progress_report_metadata()
            )
            self.last_report_processed_bytes = self.processed_bytes
            self.last_report_time = current_time
            self.completed_files_in_chunk = 0
        return self.continue_reporting

    def report_progress(self) -> bool:
        with self._lock:
            return self._report_progress()

    def _get_progress_report_metadata(self) -> ProgressReportMetadata:
        completed_bytes = self.processed_bytes + self.skipped_bytes
        percentage = round(
            completed_bytes / self.total_bytes * 100 if self.total_bytes > 0 else 0, 1
        )
        seconds_since_last_report = round(
            time.perf_counter() - self.last_report_time if self.last_report_time else 0, 2
        )
        transfer_rate = (
            (self.processed_bytes - self.last_report_processed_bytes) / seconds_since_last_report
            if seconds_since_last_report > 0
            else 0
        )
        transfer_rate_name = "Transfer rate"
        if self.status == ProgressStatus.PREPARING_IN_PROGRESS:
            transfer_rate_name = "Hashing speed"

        progress_message = (
            f"{self.status.verb_in_message}"
            f" {_human_readable_file_size(completed_bytes)} / {_human_readable_file_size(self.total_bytes)}"
            f" of {self.total_files} file{'' if self.total_files == 1 else 's'}"
            f" ({transfer_rate_name}: {_human_readable_file_size(int(transfer_rate))}/s)"
        )

        return ProgressReportMetadata(
            status=self.status,
            progress=percentage,
            transferRate=transfer_rate,
            progressMessage=progress_message,
        )

    def get_summary_statistics(self) -> SummaryStatistics:
        """
        Returns the summary statistics of hashing or upload operation.
        """
        transfer_rate = self.processed_bytes / self.total_time if self.total_time else 0.0

        return SummaryStatistics(
            total_time=self.total_time,
            total_files=self.total_files,
            total_bytes=self.total_bytes,
            processed_files=self.processed_files,
            processed_bytes=self.processed_bytes,
            skipped_files=self.skipped_files,
            skipped_bytes=self.skipped_bytes,
            transfer_rate=transfer_rate,
        )

    def get_download_summary_statistics(
        self,
        downloaded_files_paths_by_root: dict[str, list[str]],
    ) -> DownloadSummaryStatistics:
        """
        Returns the summary statistics of download operation.
        """
        summary_statistics_dict = asdict(self.get_summary_statistics())
        summary_statistics_dict["file_counts_by_root_directory"] = {
            root: len(paths) for root, paths in downloaded_files_paths_by_root.items()
        }
        return DownloadSummaryStatistics(**summary_statistics_dict)

    def _log_progress_message(self) -> None:
        """
        Logs progress message to the logger (if exists) on specific conditions:
        1. when the `log_interval` time has passed since the last call, (or since the
           process started,) or,
        2. when the tracked progress percentage difference from the last log exceeds
           the `log_percentage_threshold`, or
        3. when the process is fully completed (progress percentage reaches 100%).
        """
        if self.logger is None:
            return

        current_time = time.perf_counter()
        current_completed_bytes = self.processed_bytes + self.skipped_bytes
        progress_difference = (
            (current_completed_bytes - self.last_logged_completed_bytes) / self.total_bytes * 100
        )

        if (
            self.last_logged_time is None
            or current_time - self.last_logged_time >= self.log_interval
            or progress_difference >= self.log_percentage_threshold
            or self.processed_files + self.skipped_files == self.total_files
        ):
            self.logger.info(self._get_progress_report_metadata().progressMessage)
            self.last_logged_completed_bytes = current_completed_bytes
            self.last_logged_time = current_time
