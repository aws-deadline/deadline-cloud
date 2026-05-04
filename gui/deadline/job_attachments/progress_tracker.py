# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Progress tracking types for DCC submitter compatibility."""

from enum import Enum

from deadline.client._compat import ProgressReportMetadata


class ProgressStatus(Enum):
    """Progress status matching Python deadline-cloud's ProgressStatus."""

    NONE = ("NONE", "")
    PREPARING_IN_PROGRESS = ("PREPARING_IN_PROGRESS", "Processed")
    UPLOAD_IN_PROGRESS = ("UPLOAD_IN_PROGRESS", "Uploaded")
    DOWNLOAD_IN_PROGRESS = ("DOWNLOAD_IN_PROGRESS", "Downloaded")
    SNAPSHOT_IN_PROGRESS = ("SNAPSHOT_IN_PROGRESS", "Snapshotted")


__all__ = ["ProgressReportMetadata", "ProgressStatus"]
