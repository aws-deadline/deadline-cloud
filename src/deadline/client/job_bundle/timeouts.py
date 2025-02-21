# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from dataclasses import dataclass

SECONDS_IN_A_MINUTE = 60
SECONDS_IN_AN_HOUR = 60 * 60
SECONDS_IN_A_DAY = SECONDS_IN_AN_HOUR * 24


@dataclass
class TimeoutEntry:
    label: str
    tooltip: str

    is_activated: bool = True
    seconds: int = SECONDS_IN_A_DAY

    def __post_init__(self):
        if self.is_activated and self.seconds <= 0:
            raise ValueError(f"Timeout value cannot be negative or zero: {self.seconds}")
        if len(self.label) == 0:
            raise ValueError("Timeout label cannot be empty.")
        if len(self.tooltip) == 0:
            raise ValueError("Timeout tooltip cannot be empty.")
