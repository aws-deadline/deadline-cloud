# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Timestamp formatting utilities for the Deadline CLI.
"""

from __future__ import annotations
import datetime
from enum import Enum


class TimestampFormat(str, Enum):
    """Enumeration of supported timestamp formats."""

    UTC = "utc"
    LOCAL = "local"
    RELATIVE = "relative"


class TimestampFormatter:
    """
    A utility class for formatting timestamps in different formats.

    Supports UTC ISO format, local timezone ISO format, and relative time
    from a reference timestamp.
    """

    def __init__(self, format_type: TimestampFormat, reference_start_time: datetime.datetime):
        """
        Initialize the TimestampFormatter.

        Args:
            format_type: The format type to use for timestamp formatting
            reference_start_time: Reference timestamp for relative formatting
        """
        self.format_type = format_type

        if reference_start_time.tzinfo is None:
            raise ValueError("Reference time must have a timezone")

        self.reference_start_time = reference_start_time

    def format_timestamp(self, timestamp: datetime.datetime) -> str:
        """
        Format a timestamp according to the specified format type.

        Args:
            timestamp: The datetime object to format. It must have a timezone.

        Returns:
            Formatted timestamp string

        Raises:
            ValueError: If format type is unsupported
        """
        if timestamp.tzinfo is None:
            raise ValueError("Timestamp must have a timezone")

        if self.format_type == TimestampFormat.UTC:
            utc_timestamp = timestamp.astimezone(datetime.timezone.utc)
            return utc_timestamp.isoformat()
        elif self.format_type == TimestampFormat.LOCAL:
            local_timestamp = timestamp.astimezone()
            return local_timestamp.isoformat()
        elif self.format_type == TimestampFormat.RELATIVE:
            time_delta = timestamp - self.reference_start_time
            # Return the string representation of timedelta which matches Python's format
            # This gives us "H:MM:SS.ffffff" format automatically
            return str(time_delta)
        else:
            raise ValueError(f"Unsupported timestamp format: {self.format_type}")
