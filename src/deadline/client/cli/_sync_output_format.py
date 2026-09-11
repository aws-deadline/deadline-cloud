# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Terminal formatting for the `deadline queue sync-output` command.

Output is plain ASCII. Color is used only to flag a problem (yellow for warnings, red
for errors), matching the rest of the CLI, and structure is carried by indentation so
the log reads the same when redirected, piped, or collected by a log agent.
"""

from __future__ import annotations

__all__ = [
    "_ENTRY_DETAIL_INDENT",
    "_SyncOutputFormatter",
    "_SyncOutputWriter",
    "_format_duration",
    "_format_interval",
    "_format_path",
    "_format_timestamp",
    "_plural",
]

import os
from datetime import datetime, timedelta
from typing import Any, Callable, Optional, Union

import click

# Column that right-aligned step timings are flushed to, in characters.
_TIMING_COLUMN = 56

# Timings below this are noise: a run reports six sub-millisecond bookkeeping steps whose
# durations say nothing, and hiding them lets the one genuinely slow step stand out.
_MIN_REPORTED_SECONDS = 1.0

# Width of the label column used by `field()` and `summary_row()`.
_LABEL_WIDTH = 12

# Layout of the tagged job entries printed beneath a step, e.g. `NEW       my-job`.
# Continuation lines indent past the tag so they sit under the entry title.
_ENTRY_INDENT = 4
_ENTRY_TAG_WIDTH = 9
_ENTRY_DETAIL_INDENT = _ENTRY_INDENT + _ENTRY_TAG_WIDTH + 1


def _format_duration(duration: Union[timedelta, float, int]) -> str:
    """Renders a duration compactly: `<0.1s`, `0.6s`, `42s`, `10m55s`, `2h05m`."""
    seconds = duration.total_seconds() if isinstance(duration, timedelta) else float(duration)
    seconds = max(seconds, 0.0)
    if seconds < 0.1:
        return "<0.1s"
    if seconds < 10:
        return f"{seconds:.1f}s"
    if seconds < 60:
        return f"{seconds:.0f}s"
    minutes, whole_seconds = divmod(int(round(seconds)), 60)
    if minutes < 60:
        return f"{minutes}m" if whole_seconds == 0 else f"{minutes}m{whole_seconds:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h" if minutes == 0 else f"{hours}h{minutes:02d}m"


def _is_worth_reporting(duration: Union[timedelta, float, int]) -> bool:
    """Returns True when a step took long enough that its duration is informative."""
    seconds = duration.total_seconds() if isinstance(duration, timedelta) else float(duration)
    return seconds >= _MIN_REPORTED_SECONDS


def _format_timestamp(when: datetime) -> str:
    """Renders a timestamp in local time to second precision."""
    return when.astimezone().strftime("%Y-%m-%d %H:%M:%S")


def _format_interval(start: datetime, end: datetime) -> str:
    """Renders a time interval, collapsing the date on the end when both fall on one day."""
    start_local = start.astimezone()
    end_local = end.astimezone()
    if start_local.date() == end_local.date():
        return f"{_format_timestamp(start)} to {end_local.strftime('%H:%M:%S')}"
    return f"{_format_timestamp(start)} to {_format_timestamp(end)}"


def _format_path(path: str) -> str:
    """Collapses the home directory to `~` so long checkpoint paths stay readable.

    Never truncates: these paths are printed so the operator can copy them.
    """
    home = os.path.expanduser("~")
    if home and home != os.sep:
        if path == home:
            return "~"
        if path.startswith(home + os.sep):
            return "~" + path[len(home) :]
    return path


def _plural(count: int, singular: str, plural: Optional[str] = None) -> str:
    """Renders `count` with the correctly pluralized noun, e.g. `1 job` / `2 jobs`."""
    if count == 1:
        return f"{count} {singular}"
    return f"{count} {plural if plural is not None else singular + 's'}"


class _SyncOutputWriter:
    """The output sink for one sync-output run: optional buffering, plus a problem flag.

    A run's output is produced from several modules, each of which builds its own
    formatter around a plain print callback. Routing them all through one writer is what
    lets the whole report share a single buffer and a single "did anything go wrong" flag.

    While buffering, lines are held rather than printed. The caller then calls `flush()`
    once it knows the run has something to report, or `discard()` to drop the report in
    favour of a one-line result. That is what keeps a downloader polling a queue on a
    schedule from writing a full report every interval when nothing changed.

    Instances are callable, so a writer can be passed anywhere a print callback is taken.
    """

    def __init__(self, echo: Callable[[Any], None], *, buffered: bool = False) -> None:
        self._echo = echo
        self._buffer: Optional[list[Any]] = [] if buffered else None
        self._suppressed = False
        self.had_problem = False

    def __call__(self, line: Any = "") -> None:
        if self._buffer is not None:
            self._buffer.append(line)
        else:
            self._echo(line)

    @property
    def suppressed(self) -> bool:
        """True when buffered lines were dropped instead of printed."""
        return self._suppressed

    def flush(self) -> None:
        """Prints anything held back and sends later output straight through."""
        if self._buffer is not None:
            held, self._buffer = self._buffer, None
            for line in held:
                self._echo(line)

    def discard(self) -> None:
        """Drops anything held back, for a run with nothing worth reporting."""
        if self._buffer is not None:
            self._suppressed = True
            self._buffer = None


class _SyncOutputFormatter:
    """Renders sync-output progress as indented plain-text sections.

    Every line goes through the caller's sink, so JSON mode stays silent and the CLI's
    logger keeps ownership of the stream. Pass a `_SyncOutputWriter` as the sink to take
    part in a run's shared buffering; a plain callback also works, and then the buffering
    controls below are no-ops.
    """

    def __init__(self, echo: Callable[[Any], None]) -> None:
        self._echo = echo

    @property
    def _writer(self) -> Optional[_SyncOutputWriter]:
        return self._echo if isinstance(self._echo, _SyncOutputWriter) else None

    @property
    def had_problem(self) -> bool:
        """True once a warning or error has been reported anywhere in this run."""
        writer = self._writer
        return writer.had_problem if writer is not None else False

    @property
    def suppressed(self) -> bool:
        """True when the buffered report was dropped in favour of a one-line result."""
        writer = self._writer
        return writer.suppressed if writer is not None else False

    def flush(self) -> None:
        writer = self._writer
        if writer is not None:
            writer.flush()

    def discard(self) -> None:
        writer = self._writer
        if writer is not None:
            writer.discard()

    def _emit(self, line: Any) -> None:
        self._echo(line)

    def mark_problem(self) -> None:
        """Records that a problem happened, for a warning reported through another sink."""
        self._mark_problem()

    def _mark_problem(self) -> None:
        writer = self._writer
        if writer is not None:
            writer.had_problem = True

    def blank(self) -> None:
        self._emit("")

    def section(self, title: str) -> None:
        """Starts a new section: an unindented heading preceded by a blank line."""
        self.blank()
        self._emit(title)

    def field(self, label: str, value: str) -> None:
        """Prints an aligned `label  value` pair under a heading."""
        self._emit(f"  {label:<{_LABEL_WIDTH}}{value}")

    def field_continuation(self, value: str) -> None:
        """Prints an extra value line aligned with the values printed by `field`."""
        self._emit(f"  {'':<{_LABEL_WIDTH}}{value}")

    def line(self, text: str, duration: Optional[Union[timedelta, float, int]] = None) -> None:
        """Prints a completed step, with its duration flushed right when slow enough."""
        if duration is None or not _is_worth_reporting(duration):
            self._emit(f"  {text}")
            return
        padding = max(1, _TIMING_COLUMN - (2 + len(text)))
        self._emit(f"  {text}{' ' * padding}{_format_duration(duration)}")

    def detail(self, text: str, indent: int = _ENTRY_INDENT) -> None:
        self._emit(f"{' ' * indent}{text}")

    def entry(self, tag: str, title: str, note: Optional[str] = None) -> None:
        """Prints a tagged job entry such as `NEW       my-job-name (4/4 tasks succeeded)`."""
        line = f"{' ' * _ENTRY_INDENT}{tag:<{_ENTRY_TAG_WIDTH}} {title}"
        if note:
            line = f"{line} {note}"
        self._emit(line)

    def entry_detail(self, text: str) -> None:
        """Prints a line aligned under the title of the entry above it."""
        self._emit(f"{' ' * _ENTRY_DETAIL_INDENT}{text}")

    def warning(self, text: str, indent: int = 2) -> None:
        self._mark_problem()
        self._emit(f"{' ' * indent}{click.style(f'WARNING: {text}', fg='yellow')}")

    def error(self, text: str, indent: int = 2) -> None:
        self._mark_problem()
        self._emit(f"{' ' * indent}{click.style(f'ERROR: {text}', fg='red')}")

    def summary_row(self, label: str, value: str) -> None:
        """Prints an aligned `label  value` row in the closing summary block."""
        self._emit(f"  {label:<{_LABEL_WIDTH}}{value}")

    def summary_continuation(self, value: str) -> None:
        """Prints an extra row aligned with the values printed by `summary_row`."""
        self._emit(f"  {'':<{_LABEL_WIDTH}}{value}")

    def result(self, text: str, *, failed: bool = False) -> None:
        """Prints the closing one-line result, unindented so it is easy to grep or tail."""
        self._emit(click.style(f"FAILED  {text}", fg="red") if failed else f"OK  {text}")
