# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from typing import Dict, Any
from dataclasses import dataclass
from datetime import timedelta

from deadline.client.exceptions import NonValidInputError


@dataclass
class TimeoutEntry:
    """
    Represents a single timeout configuration entry.

    Attributes:
        tooltip (str): Description or explanation of the timeout entry.
        is_activated (bool): Flag indicating if the timeout is active. Defaults to True.
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

    def to_dict(self) -> Dict[str, Any]:
        """
        Converts the timeout entry to a dictionary representation.
        We skip tooltip as it not necessary to be saved in sticky settings.

        Returns:
            Dict[str, Any]: Dictionary containing activation status and timeout duration.
        """
        return {"is_activated": self.is_activated, "seconds": self.seconds}


@dataclass
class TimeoutEntries:
    """
    Manages a collection of timeout entries.

    This class provides functionality to handle multiple TimeoutEntry instances,
    including serialization and validation operations.

    Attributes:
        entries (Dict[str, TimeoutEntry]): Dictionary mapping labels to TimeoutEntry instances.
    """

    entries: Dict[str, TimeoutEntry]

    def to_dict(self) -> Dict[str, Dict[str, Any]]:
        """
        Converts all timeout entries to a dictionary format.

        Returns:
            Dict[str, Dict[str, Any]]: Dictionary mapping labels to entry configurations.
        """
        return {label: entry.to_dict() for label, entry in self.entries.items()}

    def update_from_dict(self, data: Dict[str, Dict[str, Any]]) -> None:
        """
        Updates existing timeout entries from a dictionary of configurations.

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
        Validates all timeout entries in the collection.

        Raises:
            NonValidInputError: If any activated timeout has a zero duration.
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
