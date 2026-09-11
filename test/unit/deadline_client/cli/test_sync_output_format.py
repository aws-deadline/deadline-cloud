# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the terminal formatting helpers behind `deadline queue sync-output`.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
import click
import pytest

from deadline.client.cli import _sync_output_format
from deadline.client.cli._sync_output_format import (
    _MIN_REPORTED_SECONDS,
    _SyncOutputFormatter,
    _SyncOutputWriter,
    _format_duration,
    _format_interval,
    _format_path,
    _format_timestamp,
    _plural,
)


@pytest.mark.parametrize(
    "seconds,expected",
    [
        (0.0, "<0.1s"),
        (0.000032, "<0.1s"),
        (0.099, "<0.1s"),
        (0.1, "0.1s"),
        (0.606954, "0.6s"),
        (9.94, "9.9s"),
        (10.0, "10s"),
        (42.4, "42s"),
        (59.6, "60s"),
        (60.0, "1m"),
        (120.0, "2m"),
        (655.04, "10m55s"),
        (3600.0, "1h"),
        (7500.0, "2h05m"),
    ],
)
def test_format_duration(seconds, expected):
    assert _format_duration(seconds) == expected
    assert _format_duration(timedelta(seconds=seconds)) == expected


def test_format_duration_clamps_negative():
    """A clock adjustment can produce a negative delta; it must not render as garbage."""
    assert _format_duration(timedelta(seconds=-5)) == "<0.1s"


def test_format_timestamp_drops_microseconds():
    when = datetime(2026, 8, 21, 17, 16, 59, 319524, tzinfo=timezone.utc)
    formatted = _format_timestamp(when)

    assert formatted == when.astimezone().strftime("%Y-%m-%d %H:%M:%S")
    assert "." not in formatted


def test_format_interval_collapses_the_date_within_one_day():
    start = datetime(2026, 8, 21, 12, 0, 0, tzinfo=timezone.utc)
    end = start + timedelta(minutes=13)

    interval = _format_interval(start, end)

    assert interval.startswith(_format_timestamp(start))
    assert interval.endswith(end.astimezone().strftime("%H:%M:%S"))
    # The end has no date of its own when both ends land on the same local day.
    assert interval.count("-") == 2
    assert " to " in interval


def test_format_interval_keeps_both_dates_across_days():
    start = datetime(2026, 8, 21, 12, 0, 0, tzinfo=timezone.utc)
    end = start + timedelta(days=3)

    interval = _format_interval(start, end)

    assert _format_timestamp(start) in interval
    assert _format_timestamp(end) in interval


def test_format_path_collapses_home():
    path = os.path.join(os.path.expanduser("~"), ".deadline", "checkpoint.json")

    assert _format_path(path) == os.path.join("~", ".deadline", "checkpoint.json")


def test_format_path_leaves_other_paths_intact():
    """A long path is never truncated: it is printed so the operator can copy it."""
    path = os.path.join(os.sep, "mnt", "renders", "a" * 200, "checkpoint.json")

    assert _format_path(path) == path


def test_format_path_does_not_match_a_home_prefixed_sibling():
    home = os.path.expanduser("~")

    assert _format_path(home + "-backup") == home + "-backup"


@pytest.mark.parametrize(
    "count,expected",
    [(0, "0 jobs"), (1, "1 job"), (2, "2 jobs")],
)
def test_plural(count, expected):
    assert _plural(count, "job") == expected


def test_plural_with_an_irregular_plural():
    assert _plural(2, "entry", "entries") == "2 entries"


def _render(build) -> list[str]:
    """Runs `build` against a capturing formatter and returns the lines with styling removed.

    click.style always emits escape codes; click.echo is what strips them when stdout is not
    a terminal. Capturing the raw calls bypasses that, so unstyle here to assert on columns.
    """
    lines: list[str] = []
    build(_SyncOutputFormatter(lines.append))
    return [click.unstyle(line) for line in lines]


def test_section_is_preceded_by_a_blank_line():
    lines = _render(lambda fmt: fmt.section("Download"))

    assert lines[0] == ""
    assert "Download" in lines[1]


def test_section_heading_is_unindented():
    """Unindented headings over indented bodies is what carries structure without color."""
    lines = _render(lambda fmt: (fmt.section("Download"), fmt.line("4 files")))

    assert lines[1] == "Download"
    assert lines[2].startswith("  ")


def test_line_flushes_the_duration_to_a_fixed_column():
    lines = _render(lambda fmt: fmt.line("4 files", timedelta(seconds=1.08)))

    assert lines[0].endswith("1.1s")
    assert lines[0].index("1.1s") == _sync_output_format._TIMING_COLUMN


def test_line_keeps_one_space_when_the_text_overruns_the_timing_column():
    lines = _render(lambda fmt: fmt.line("x" * 90, timedelta(seconds=1)))

    assert lines[0].endswith("x 1.0s")


def test_line_hides_a_duration_that_is_too_short_to_be_informative():
    """Six steps each reporting <0.1s is noise that hides the one slow step."""
    lines = _render(lambda fmt: fmt.line("manifest S3 keys populated", timedelta(seconds=0.00003)))

    assert lines[0] == "  manifest S3 keys populated"


def test_line_shows_a_duration_at_the_reporting_threshold():
    lines = _render(lambda fmt: fmt.line("slow step", timedelta(seconds=_MIN_REPORTED_SECONDS)))

    assert lines[0].endswith("1.0s")


def test_line_without_a_duration_prints_no_timing():
    lines = _render(lambda fmt: fmt.line("checkpoint saved"))

    assert lines[0] == "  checkpoint saved"


def test_field_and_continuation_align():
    lines = _render(
        lambda fmt: (fmt.field("checkpoint", "/tmp/a.json"), fmt.field_continuation("/tmp/b.json"))
    )

    assert lines[0].index("/tmp/a.json") == lines[1].index("/tmp/b.json")


def test_entry_details_align_under_the_entry_title():
    lines = _render(
        lambda fmt: (
            fmt.entry("NEW", "my-job", "(4/4 tasks succeeded)"),
            fmt.entry_detail("job-1234"),
        )
    )

    assert lines[0].index("my-job") == lines[1].index("job-1234")
    assert "(4/4 tasks succeeded)" in lines[0]


def test_entry_detail_aligns_under_the_longest_tag():
    """UNCHANGED is the longest tag; it must not run into the job name."""
    lines = _render(lambda fmt: (fmt.entry("UNCHANGED", "my-job"), fmt.entry_detail("job-1234")))

    assert "UNCHANGED my-job" in lines[0]
    assert lines[0].index("my-job") == lines[1].index("job-1234")


def test_warning_and_error_use_text_prefixes():
    """The prefix, not a glyph, is what marks a problem, so it survives any encoding."""
    lines = _render(lambda fmt: (fmt.warning("careful"), fmt.error("broken")))

    assert lines[0] == "  WARNING: careful"
    assert lines[1] == "  ERROR: broken"


def test_output_is_pure_ascii():
    """No unicode anywhere: redirecting under a legacy Windows codepage must not fail."""
    lines = _render(
        lambda fmt: (
            fmt.section("Download"),
            fmt.field("checkpoint", "/tmp/a.json"),
            fmt.line("4 files", timedelta(seconds=2)),
            fmt.entry("RETIRED", "my-job", "(4/4 tasks succeeded)"),
            fmt.entry_detail("job-1234"),
            fmt.warning("careful"),
            fmt.error("broken"),
            fmt.summary_row("files", "4"),
            fmt.result("4 files in 2.0s"),
        )
    )

    joined = "\n".join(lines)
    assert joined.isascii(), [line for line in lines if not line.isascii()]
    joined.encode("cp1252")


def test_warning_and_error_record_a_problem_on_the_shared_writer():
    """The flag lives on the writer so warnings raised in any module reach one place."""
    lines: list[str] = []
    writer = _SyncOutputWriter(lines.append)
    fmt = _SyncOutputFormatter(writer)

    assert not fmt.had_problem
    fmt.warning("careful")

    assert fmt.had_problem
    assert writer.had_problem
    # A second formatter over the same writer sees the problem the first one reported.
    assert _SyncOutputFormatter(writer).had_problem


def test_had_problem_is_false_without_a_writer():
    """A plain callback sink has nowhere to record a problem, and must not crash."""
    fmt = _SyncOutputFormatter([].append)
    fmt.warning("careful")

    assert not fmt.had_problem
    assert not fmt.suppressed


def test_result_marks_success_and_failure_distinctly():
    lines = _render(lambda fmt: (fmt.result("4 files in 2s"), fmt.result("2 failed", failed=True)))

    assert lines[0] == "OK  4 files in 2s"
    assert lines[1] == "FAILED  2 failed"


def test_summary_row_pads_the_label():
    lines = _render(lambda fmt: (fmt.summary_row("files", "4"), fmt.summary_row("jobs", "1")))

    assert lines[0].index("4") == lines[1].index("1")
