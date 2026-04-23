# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Compatibility types for the GUI package.

Defines enum types and data classes that the UI code references from
the Python api module. These are display-only — the actual logic lives
in Rust behind the FFI.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class AwsCredentialsSource(str, Enum):
    HOST_PROVIDED = "HOST_PROVIDED"
    DEADLINE_CLOUD_MONITOR_LOGIN = "DEADLINE_CLOUD_MONITOR_LOGIN"
    NOT_VALID = "NOT_VALID"


class AwsAuthenticationStatus(str, Enum):
    AUTHENTICATED = "AUTHENTICATED"
    CONFIGURATION_ERROR = "CONFIGURATION_ERROR"
    NEEDS_LOGIN = "NEEDS_LOGIN"


@dataclass
class ProgressReportMetadata:
    """Progress info passed from FFI callbacks during hashing/upload."""

    status: str = ""
    progress: float = 0.0
    transfer_rate: float = 0.0
    progress_message: str = ""
    processed_files: int = 0
