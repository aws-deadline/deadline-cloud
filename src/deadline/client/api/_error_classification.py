# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Shared classification of download/transfer exceptions into stable, non-identifying
error codes for telemetry and user-facing messaging.

Classification uses only *structured* signals — exception type, ``errno``, S3 HTTP
status codes, and boto error codes — which are reliable. Message-substring matching is
intentionally avoided: wrapped S3 errors carry verbose guidance text that produces
confidently wrong codes.
"""

import errno
from typing import Optional

from botocore.exceptions import ClientError

from ...job_attachments.exceptions import (
    JobAttachmentsS3ClientError,
    JobAttachmentS3BotoCoreError,
)


def _classify_single_error(e: BaseException) -> Optional[str]:
    """Classifies one exception from its own structured signals, or None if it has none."""
    # job_attachments surfaces S3 failures as JobAttachmentsS3ClientError with an HTTP
    # status code, and botocore transport failures as JobAttachmentS3BotoCoreError.
    if isinstance(e, JobAttachmentsS3ClientError):
        if e.status_code == 403:
            return "PERMISSION_DENIED"
        if e.status_code == 404:
            return "PATH_NOT_FOUND"
    if isinstance(e, JobAttachmentS3BotoCoreError):
        return "NETWORK_ERROR"

    if isinstance(e, PermissionError):
        return "PERMISSION_DENIED"
    if isinstance(e, OSError) and e.errno == errno.ENOSPC:
        return "DISK_FULL"
    if isinstance(e, FileNotFoundError):
        return "PATH_NOT_FOUND"
    if isinstance(e, (ConnectionError, TimeoutError)):
        return "NETWORK_ERROR"

    if isinstance(e, ClientError):
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code in ("AccessDenied", "AccessDeniedException", "403", "Forbidden"):
            return "PERMISSION_DENIED"
        if error_code in ("NoSuchKey", "NoSuchBucket", "404", "NotFound"):
            return "PATH_NOT_FOUND"
        if error_code in ("RequestTimeout", "RequestTimeTooSkewed"):
            return "NETWORK_ERROR"

    return None


def classify_error(e: BaseException) -> str:
    """Classifies a download/transfer exception into a standard error code.

    Uses only structured signals — exception type, errno, S3 HTTP status codes, and
    boto error codes — which are reliable. Message-substring matching is intentionally
    avoided: wrapped S3 errors carry verbose guidance text that produces confidently
    wrong codes. job_attachments wraps low-level failures (e.g. an OSError or a botocore
    ClientError) inside its own exception types, so we also walk the exception chain to
    reach the structured signal underneath. Anything without such a signal is UNKNOWN.
    """
    # Walk the chain iteratively, tracking visited exceptions by identity so a cyclic chain
    # (A raised `from` B and B raised `from` A) can't cause infinite recursion.
    #
    # __cause__ (explicit `raise ... from`) is preferred, but fall back to __context__: much of
    # job_attachments re-raises inside an `except` block without `from`, which records the
    # original only as __context__. Following __cause__ alone reports UNKNOWN for those.
    # __suppress_context__ (`raise ... from None`) means the author declared the inner error
    # irrelevant, so honor it and stop rather than attaching a misleading code.
    current: Optional[BaseException] = e
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        code = _classify_single_error(current)
        if code is not None:
            return code
        if current.__cause__ is not None:
            current = current.__cause__
        elif current.__suppress_context__:
            break
        else:
            current = current.__context__

    return "UNKNOWN"
