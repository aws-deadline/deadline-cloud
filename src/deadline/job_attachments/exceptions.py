# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Exceptions that the Deadline Job Attachments library can raise.
"""


from typing import Optional

from deadline.job_attachments.progress_tracker import SummaryStatistics


COMMON_ERROR_GUIDANCE_FOR_S3 = {
    408: "Request timeout. Please consider retrying later, or ensure your network connection is stable.",
    500: "Internal server error. It might be an issue on AWS's side; please consider retrying later or contacting AWS support.",
    503: "Service unavailable. AWS S3 might be down or experiencing high traffic. Please consider retrying after some time.",
}


class AssetSyncError(Exception):
    """
    Exception for errors related to synching files to/from S3.
    """


class JobAttachmentsError(Exception):
    """
    Exception for errors related to the Deadline Service.
    """


class JobAttachmentsS3ClientError(AssetSyncError):
    """
    Exception for errors related to the S3 client.
    """

    def __init__(
        self,
        action,
        status_code,
        bucket_name: str,
        key_or_prefix: str,
        message: Optional[str] = None,
    ) -> None:
        self.action = action
        self.status_code = status_code
        self.bucket_name = bucket_name
        self.key_or_prefix = key_or_prefix

        message_parts = [
            f"Error {action} in bucket '{bucket_name}'. Target key or prefix: '{key_or_prefix}'.",
            f"HTTP Status Code: {status_code}",
        ]
        if message:
            message_parts.append(message)

        super().__init__(" ".join(message_parts))


class MissingS3BucketError(JobAttachmentsError):
    """
    Exception raised when attempting to use Job Attachments but the S3 bucket is not set in Queue.
    """


class MissingS3RootPrefixError(JobAttachmentsError):
    """
    Exception raised when attempting to use Job Attachments but the S3 root prefix is not set in Queue.
    """


class AssetOutsideOfRootError(JobAttachmentsError):
    """
    Exception for errors related to assets being outside of the asset root.
    """


class ManifestDecodeValidationError(JobAttachmentsError):
    """
    Exception for errors related to asset manifest decoding.
    """


class MissingManifestError(JobAttachmentsError):
    """
    Exception for when trying to retrieve asset manifests that don't exist.
    """


class MissingAssetRootError(JobAttachmentsError):
    """
    Exception for when trying to retrieve asset root from metatdata (in S3) that doesn't exist.
    """


class AssetSyncCancelledError(JobAttachmentsError):
    """
    Exception thrown when an operation (synching files to/from S3) has been cancelled.
    """

    summary_statistics: Optional[SummaryStatistics] = None

    def __init__(self, message, summary_statistics: Optional[SummaryStatistics] = None):
        super().__init__(message)
        if summary_statistics:
            self.summary_statistics = summary_statistics


class PathOutsideDirectoryError(JobAttachmentsError):
    """
    Exception thrown in the _ensure_paths_within_directory function to signal that a given
    file path, especially ones that may contain "..", does not reside in the specified root path.
    """


class Fus3ExecutableMissingError(JobAttachmentsError):
    """
    Exception for when trying to retrieve Fus3 executable path doesn't exist.
    """


class Fus3LaunchScriptMissingError(JobAttachmentsError):
    """
    Exception for when trying to retrieve Fus3 launch script path doesn't exist.
    """


class Fus3FailedToMountError(JobAttachmentsError):
    """
    Exception for when trying to mount Fus3 at a given path.
    """
