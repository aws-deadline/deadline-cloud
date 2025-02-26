# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
from deadline.client.ui.dataclasses.timeouts import TimeoutEntry, TimeoutTableEntries
from datetime import timedelta
from deadline.client.exceptions import NonValidInputError


@pytest.mark.parametrize(
    "tooltip,is_activated,seconds,expected",
    [
        pytest.param(
            "This is a test timeout",
            True,
            int(timedelta(days=1).total_seconds()),
            {
                "tooltip": "This is a test timeout",
                "is_activated": True,
                "seconds": int(timedelta(days=1).total_seconds()),
            },
            id="default_day_timeout",
        ),
        pytest.param(
            "Custom tooltip",
            False,
            int(timedelta(hours=1).total_seconds()),
            {
                "tooltip": "Custom tooltip",
                "is_activated": False,
                "seconds": int(timedelta(hours=1).total_seconds()),
            },
            id="custom_hour_timeout",
        ),
    ],
)
def test_valid_timeout_entries(tooltip, is_activated, seconds, expected):
    entry = TimeoutEntry(tooltip=tooltip, is_activated=is_activated, seconds=seconds)
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
        TimeoutEntry(tooltip="Test", seconds=seconds)
    assert str(exc_info.value) == error_message


@pytest.mark.parametrize(
    "tooltip,error_message",
    [
        pytest.param("", "Timeout tooltip cannot be empty.", id="empty_tooltip"),
    ],
)
def test_empty_fields(tooltip, error_message):
    with pytest.raises(ValueError) as exc_info:
        TimeoutEntry(tooltip=tooltip)
    assert str(exc_info.value) == error_message


@pytest.fixture
def valid_timeout_entries() -> TimeoutTableEntries:
    """Fixture providing a TimeoutTableEntries instance with valid entries."""
    entries = {
        "entry1": TimeoutEntry(tooltip="First timeout", is_activated=True, seconds=3600),
        "entry2": TimeoutEntry(tooltip="Second timeout", is_activated=False, seconds=7200),
    }
    return TimeoutTableEntries(entries=entries)


class TestTimeoutTableEntries:
    def test_to_sticky_settings_dict(self, valid_timeout_entries):
        """Test conversion of TimeoutTableEntries to dictionary format, including value checks."""
        result = valid_timeout_entries.to_sticky_settings_dict()

        assert isinstance(result, dict)
        assert len(result) == 2

        # Check entry1
        assert "entry1" in result
        assert result["entry1"] == {"is_activated": True, "seconds": 3600}

        # Check entry2
        assert "entry2" in result
        assert result["entry2"] == {"is_activated": False, "seconds": 7200}

    def test_update_from_sticky_settings(self, valid_timeout_entries):
        """Test updating TimeoutTableEntries from dictionary data."""
        update_data = {
            "entry1": {"is_activated": False, "seconds": 1800},
        }

        valid_timeout_entries.update_from_sticky_settings(update_data)

        # Check entry1 updates
        assert valid_timeout_entries.entries["entry1"].is_activated is False
        assert valid_timeout_entries.entries["entry1"].seconds == 1800

        # Check entry2 updates
        assert valid_timeout_entries.entries["entry2"].is_activated is False  # unchanged
        assert valid_timeout_entries.entries["entry2"].seconds == 7200

    def test_update_from_sticky_settings_with_nonexistent_entry(self, valid_timeout_entries):
        """Test updating with data for non-existent entries."""
        update_data = {"nonexistent_entry": {"is_activated": False, "seconds": 1800}}

        # Should not raise an error, should simply ignore non-existent entries
        valid_timeout_entries.update_from_sticky_settings(update_data)
        assert "nonexistent_entry" not in valid_timeout_entries.entries

    def test_validate_entries_success(self, valid_timeout_entries):
        """Test validation with valid entries."""
        valid_timeout_entries.validate_entries()  # Should not raise an exception

    def test_validate_entries_with_zero_timeout(self, valid_timeout_entries):
        """Test validation fails with zero timeout in activated entry."""
        valid_timeout_entries.entries["entry1"].seconds = 0

        with pytest.raises(NonValidInputError) as exc_info:
            valid_timeout_entries.validate_entries()

        assert "entry1" in str(exc_info.value)
        assert "must be greater than 0" in str(exc_info.value)

    def test_validate_entries_with_multiple_zero_timeouts(self, valid_timeout_entries):
        """Test validation fails with multiple zero timeouts."""
        valid_timeout_entries.entries["entry1"].seconds = 0
        valid_timeout_entries.entries["entry2"].seconds = 0

        with pytest.raises(NonValidInputError) as exc_info:
            valid_timeout_entries.validate_entries()

        # Check that only entry1 is in error.
        assert "entry1" in str(exc_info.value)
        assert "entry2" not in str(exc_info.value)

        valid_timeout_entries.entries["entry2"].is_activated = True
        with pytest.raises(NonValidInputError) as exc_info:
            valid_timeout_entries.validate_entries()

        # Check that both entries are in error.
        assert "entry1" in str(exc_info.value)
        assert "entry2" in str(exc_info.value)
