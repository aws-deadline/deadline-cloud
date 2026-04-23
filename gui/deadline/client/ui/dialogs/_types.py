# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from enum import Enum

# ---- Types to export


class JobBundlePurpose(str, Enum):
    EXPORT = "export"
    """A job bundle is being created for export."""

    SUBMISSION = "submission"
    """A job bundle is being created for immediate submission."""
