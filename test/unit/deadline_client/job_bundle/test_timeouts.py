# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
from deadline.client.job_bundle.timeouts import (
    TimeoutEntry,
    SECONDS_IN_A_DAY,
    SECONDS_IN_AN_HOUR,
)


@pytest.mark.parametrize(
    "label,tooltip,is_activated,seconds,expected",
    [
        pytest.param(
            "Test Timeout",
            "This is a test timeout",
            True,
            SECONDS_IN_A_DAY,
            {
                "label": "Test Timeout",
                "tooltip": "This is a test timeout",
                "is_activated": True,
                "seconds": SECONDS_IN_A_DAY,
            },
            id="default_day_timeout",
        ),
        pytest.param(
            "Custom",
            "Custom tooltip",
            False,
            SECONDS_IN_AN_HOUR,
            {
                "label": "Custom",
                "tooltip": "Custom tooltip",
                "is_activated": False,
                "seconds": SECONDS_IN_AN_HOUR,
            },
            id="custom_hour_timeout",
        ),
    ],
)
def test_valid_timeout_entries(label, tooltip, is_activated, seconds, expected):
    entry = TimeoutEntry(label=label, tooltip=tooltip, is_activated=is_activated, seconds=seconds)
    assert entry.label == expected["label"]
    assert entry.tooltip == expected["tooltip"]
    assert entry.is_activated == expected["is_activated"]
    assert entry.seconds == expected["seconds"]


@pytest.mark.parametrize(
    "seconds,error_message",
    [
        pytest.param(-1, "Timeout value cannot be negative or zero: -1", id="negative_seconds"),
        pytest.param(0, "Timeout value cannot be negative or zero: 0", id="zero_seconds"),
    ],
)
def test_non_valid_seconds(seconds, error_message):
    with pytest.raises(ValueError) as exc_info:
        TimeoutEntry(label="Test", tooltip="Test", seconds=seconds)
    assert str(exc_info.value) == error_message


@pytest.mark.parametrize(
    "label,tooltip,error_message",
    [
        pytest.param("", "Test", "Timeout label cannot be empty.", id="empty_label"),
        pytest.param("Test", "", "Timeout tooltip cannot be empty.", id="empty_tooltip"),
    ],
)
def test_empty_fields(label, tooltip, error_message):
    """Test that empty required fields raise appropriate errors."""
    with pytest.raises(ValueError) as exc_info:
        TimeoutEntry(label=label, tooltip=tooltip)
    assert str(exc_info.value) == error_message
