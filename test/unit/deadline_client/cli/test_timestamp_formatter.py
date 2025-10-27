# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the TimestampFormatter utility class.
"""

import datetime
import pytest

from deadline.client.cli._timestamp_formatter import TimestampFormatter, TimestampFormat

# Valid timezone offset patterns for testing
VALID_TIMEZONE_OFFSETS = [
    "+00",
    "+01",
    "+02",
    "+03",
    "+04",
    "+05",
    "+06",
    "+07",
    "+08",
    "+09",
    "+10",
    "+11",
    "+12",
    "-01",
    "-02",
    "-03",
    "-04",
    "-05",
    "-06",
    "-07",
    "-08",
    "-09",
    "-10",
    "-11",
    "-12",
]


class TestTimestampFormatter:
    """Test cases for the TimestampFormatter class."""

    def test_utc_format_with_utc_timestamp(self):
        """Test UTC format with a UTC timestamp."""
        formatter = TimestampFormatter(
            TimestampFormat.UTC, datetime.datetime.now(tz=datetime.timezone.utc)
        )
        timestamp = datetime.datetime(2023, 1, 1, 12, 0, 0, 123456, tzinfo=datetime.timezone.utc)

        result = formatter.format_timestamp(timestamp)

        assert result == "2023-01-01T12:00:00.123456+00:00"

    def test_utc_format_with_non_utc_timestamp(self):
        """Test UTC format with a non-UTC timestamp."""
        formatter = TimestampFormatter(
            TimestampFormat.UTC, datetime.datetime.now(tz=datetime.timezone.utc)
        )
        # Create a timestamp in EST (UTC-5)
        est_tz = datetime.timezone(datetime.timedelta(hours=-5))
        timestamp = datetime.datetime(2023, 1, 1, 7, 0, 0, 123456, tzinfo=est_tz)

        result = formatter.format_timestamp(timestamp)

        # Should be converted to UTC (7 AM EST = 12 PM UTC)
        assert result == "2023-01-01T12:00:00.123456+00:00"

    def test_local_format_with_utc_timestamp(self):
        """Test LOCAL format with a UTC timestamp."""
        formatter = TimestampFormatter(
            TimestampFormat.LOCAL, datetime.datetime.now(tz=datetime.timezone.utc)
        )
        timestamp = datetime.datetime(2023, 1, 1, 12, 0, 0, 123456, tzinfo=datetime.timezone.utc)

        result = formatter.format_timestamp(timestamp)

        # Result will depend on system timezone, but should be in ISO format
        assert "2023-01-01T" in result
        assert ".123456" in result
        # Should have timezone offset (either +HH:MM or -HH:MM format)
        assert result[-6:-3] in VALID_TIMEZONE_OFFSETS
        assert result.endswith(":00")

    def test_local_format_with_non_utc_timestamp(self):
        """Test LOCAL format with a non-UTC timestamp."""
        formatter = TimestampFormatter(
            TimestampFormat.LOCAL, datetime.datetime.now(tz=datetime.timezone.utc)
        )
        # Create a timestamp in EST (UTC-5)
        est_tz = datetime.timezone(datetime.timedelta(hours=-5))
        timestamp = datetime.datetime(2023, 1, 1, 7, 0, 0, 123456, tzinfo=est_tz)

        result = formatter.format_timestamp(timestamp)

        # Result will depend on system timezone, but should be in ISO format
        assert "2023-01-01T" in result
        assert ".123456" in result
        # Should have timezone offset
        assert result[-6:-3] in VALID_TIMEZONE_OFFSETS
        assert result.endswith(":00")

    def test_local_format_timezone_conversion_consistency(self):
        """Test LOCAL format converts different input timezones to same local time."""
        formatter = TimestampFormatter(
            TimestampFormat.LOCAL, datetime.datetime.now(tz=datetime.timezone.utc)
        )

        # Create the same moment in time using different timezone representations
        utc_timestamp = datetime.datetime(
            2023, 1, 1, 12, 0, 0, 123456, tzinfo=datetime.timezone.utc
        )
        est_tz = datetime.timezone(datetime.timedelta(hours=-5))
        est_timestamp = datetime.datetime(
            2023, 1, 1, 7, 0, 0, 123456, tzinfo=est_tz
        )  # Same moment as UTC 12:00
        pst_tz = datetime.timezone(datetime.timedelta(hours=-8))
        pst_timestamp = datetime.datetime(
            2023, 1, 1, 4, 0, 0, 123456, tzinfo=pst_tz
        )  # Same moment as UTC 12:00

        utc_result = formatter.format_timestamp(utc_timestamp)
        est_result = formatter.format_timestamp(est_timestamp)
        pst_result = formatter.format_timestamp(pst_timestamp)

        # All should convert to the same local time since they represent the same moment
        assert utc_result == est_result == pst_result

    def test_relative_format_with_positive_delta(self):
        """Test RELATIVE format with a positive time delta."""
        reference_start_time = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        formatter = TimestampFormatter(TimestampFormat.RELATIVE, reference_start_time)

        # Timestamp 1 hour, 23 minutes, 45 seconds, and 123456 microseconds after reference
        timestamp = datetime.datetime(2023, 1, 1, 13, 23, 45, 123456, tzinfo=datetime.timezone.utc)

        result = formatter.format_timestamp(timestamp)

        assert result == "1:23:45.123456"

    def test_relative_format_with_zero_delta(self):
        """Test RELATIVE format when timestamp equals reference time."""
        reference_start_time = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        formatter = TimestampFormatter(TimestampFormat.RELATIVE, reference_start_time)

        result = formatter.format_timestamp(reference_start_time)

        assert result == "0:00:00"

    def test_relative_format_with_negative_delta(self):
        """Test RELATIVE format with a negative time delta."""
        reference_start_time = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        formatter = TimestampFormatter(TimestampFormat.RELATIVE, reference_start_time)

        # Timestamp 30 minutes before reference
        timestamp = datetime.datetime(2023, 1, 1, 11, 30, 0, tzinfo=datetime.timezone.utc)

        result = formatter.format_timestamp(timestamp)

        assert result == "-1 day, 23:30:00"

    def test_relative_format_with_different_timezones(self):
        """Test RELATIVE format with timestamps in different timezones."""
        reference_start_time = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        formatter = TimestampFormatter(TimestampFormat.RELATIVE, reference_start_time)

        # Create timestamp in EST (UTC-5) that represents the same moment as 13:00 UTC
        est_tz = datetime.timezone(datetime.timedelta(hours=-5))
        timestamp = datetime.datetime(2023, 1, 1, 8, 0, 0, tzinfo=est_tz)  # 8 AM EST = 1 PM UTC

        result = formatter.format_timestamp(timestamp)

        assert result == "1:00:00"

    def test_relative_format_with_microseconds(self):
        """Test RELATIVE format preserves microseconds."""
        reference_start_time = datetime.datetime(
            2023, 1, 1, 12, 0, 0, 100000, tzinfo=datetime.timezone.utc
        )
        formatter = TimestampFormatter(TimestampFormat.RELATIVE, reference_start_time)

        # Timestamp 1.5 seconds after reference
        timestamp = datetime.datetime(2023, 1, 1, 12, 0, 1, 600000, tzinfo=datetime.timezone.utc)

        result = formatter.format_timestamp(timestamp)

        assert result == "0:00:01.500000"

    def test_init_relative_format_with_timezone_naive_reference(self):
        """Test that RELATIVE format requires reference time with timezone."""
        reference_start_time = datetime.datetime(2023, 1, 1, 12, 0, 0)  # No timezone

        with pytest.raises(ValueError, match="Reference time must have a timezone"):
            TimestampFormatter(TimestampFormat.RELATIVE, reference_start_time)

    def test_init_utc_format_with_reference_start_time(self):
        """Test that UTC format can be initialized with reference time (ignored)."""
        reference_start_time = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        formatter = TimestampFormatter(TimestampFormat.UTC, reference_start_time)

        assert formatter.reference_start_time == reference_start_time

    def test_init_local_format_with_reference_start_time(self):
        """Test that LOCAL format can be initialized with reference time (ignored)."""
        reference_start_time = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        formatter = TimestampFormatter(TimestampFormat.LOCAL, reference_start_time)

        assert formatter.reference_start_time == reference_start_time

    def test_format_timestamp_with_timezone_naive_timestamp(self):
        """Test that format_timestamp raises error for timezone-naive timestamps."""
        formatter = TimestampFormatter(
            TimestampFormat.UTC, datetime.datetime.now(tz=datetime.timezone.utc)
        )
        timestamp = datetime.datetime(2023, 1, 1, 12, 0, 0)  # No timezone

        with pytest.raises(ValueError, match="Timestamp must have a timezone"):
            formatter.format_timestamp(timestamp)

    def test_format_timestamp_with_timezone_naive_timestamp_relative(self):
        """Test that format_timestamp raises error for timezone-naive timestamps in relative mode."""
        reference_start_time = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        formatter = TimestampFormatter(TimestampFormat.RELATIVE, reference_start_time)
        timestamp = datetime.datetime(2023, 1, 1, 13, 0, 0)  # No timezone

        with pytest.raises(ValueError, match="Timestamp must have a timezone"):
            formatter.format_timestamp(timestamp)
