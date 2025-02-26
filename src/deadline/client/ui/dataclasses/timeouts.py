# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from typing import Dict, Any
from dataclasses import dataclass
from datetime import timedelta

from ...exceptions import NonValidInputError


@dataclass
class TimeoutEntry:
    """
    Represents a single timeout configuration entry.

    Attributes:
        tooltip (str): Description or explanation of the timeout entry.
        is_activated (bool): Flag indicating if the timeout is activated. Defaults to True.
        seconds (int): Timeout duration in seconds. Defaults to 24 hours (86400 seconds).

    Raises:
        ValueError: If timeout seconds is <= 0 when activated or tooltip is empty.
    """

    tooltip: str
    is_activated: bool = True
    seconds: int = int(timedelta(days=1).total_seconds())

    def __post_init__(self) -> None:
        """Validates the timeout entry after initialization."""
        if self.is_activated and self.seconds <= 0:
            raise ValueError(f"Timeout value cannot be negative or zero: {self.seconds}")
        if not self.tooltip:
            raise ValueError("Timeout tooltip cannot be empty.")

    def to_sticky_settings_dict(self) -> Dict[str, Any]:
        """
        Returns the timeout entry as a dictionary suitable for sticky settings.

        The returned dictionary contains only the essential attributes needed for saving:
        - is_activated: The activation status
        - seconds: The timeout duration

        The tooltip is intentionally excluded as it's not necessary for sticky settings.

        Returns:
            Dict[str, Any]: Dictionary containing activation status and timeout duration.
        """
        return {"is_activated": self.is_activated, "seconds": self.seconds}


@dataclass
class TimeoutTableEntries:
    """
    Manages a collection of named timeout entries.

    This class provides functionality to handle multiple TimeoutEntry instances,
    including serialization to/from sticky settings and validation operations.
    It maintains a mapping of labels to timeout configurations and ensures
    all timeout values are valid when activated.

    Attributes:
        entries (Dict[str, TimeoutEntry]): Dictionary mapping labels to TimeoutEntry instances.
    """

    entries: Dict[str, TimeoutEntry]

    def to_sticky_settings_dict(self) -> Dict[str, Dict[str, Any]]:
        """
        Returns all timeout entries as a dictionary format suitable for sticky settings.

        Returns:
            Dict[str, Dict[str, Any]]: A dictionary mapping labels to entry configurations.
                Example: {
                    "Env1 enter": {"is_activated": True, "seconds": 300},
                    "Env1 exit": {"is_activated": True, "seconds": 60},
                    ...
                }
        """
        return {label: entry.to_sticky_settings_dict() for label, entry in self.entries.items()}

    def update_from_sticky_settings(self, data: Dict[str, Dict[str, Any]]) -> None:
        """
        Updates existing timeout entries with values from sticky settings.

        This method performs a selective update where:
        - Only existing entries are updated (unknown labels in input data are ignored)
        - Missing attributes preserve their current values. So adding a new timeout is
            still a backwards compatible change.

        Args:
            data (Dict[str, Dict[str, Any]]): Dictionary containing timeout configurations.
        """
        for label, saved_data in data.items():
            if label in self.entries:
                entry = self.entries[label]
                entry.is_activated = saved_data.get("is_activated", entry.is_activated)
                entry.seconds = saved_data.get("seconds", entry.seconds)

    def validate_entries(self) -> None:
        """
        Validates all timeout entries in the table.

        Performs validation checks for all entries:
        - Identifies any activated entries with zero duration
        - Collects labels of non valid entries
        - Provides detailed error message for configuration correction

        Raises:
            NonValidInputError: If any activated timeout has a zero duration. The error message
                includes the specific labels of non valid entries and instructions for correction.
        """
        zero_timeouts = [
            label
            for label, entry in self.entries.items()
            if entry.is_activated and entry.seconds == 0
        ]

        if zero_timeouts:
            msg = "The following timeout value(s) must be greater than 0: \n"
            msg += ", ".join(zero_timeouts)
            msg += "\n\nPlease configure these value(s) in the 'Job specific settings' tab."
            raise NonValidInputError(msg)
