# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""Unit tests for the ``deadline job trace-schedule`` helper functions.

These focus on partial / in-flight job data that previously crashed the
command with ``ZeroDivisionError`` or ``KeyError``.
"""

from __future__ import annotations

import datetime

import pytest

from deadline.client.cli._groups._trace_schedule import (
    _build_trace_events,
    _get_all_sessions,
    _print_summary,
)


def _zero_accumulators(**overrides):
    accumulators = {
        "sessionCount": 0,
        "sessionActionCount": 0,
        "taskRunCount": 0,
        "envActionCount": 0,
        "syncJobAttachmentsCount": 0,
        "sessionDuration": 0,
        "sessionActionDuration": 0,
        "taskRunDuration": 0,
        "envActionDuration": 0,
        "syncJobAttachmentsDuration": 0,
    }
    accumulators.update(overrides)
    return accumulators


def test_print_summary_no_completed_durations_does_not_divide_by_zero(capsys):
    """An in-flight job with no accumulated duration must not crash on the
    percentage math (session_total_duration == 0)."""
    _print_summary(_zero_accumulators())

    out = capsys.readouterr().out
    assert "Task Run Count: 0" in out
    # Percentages should degrade gracefully rather than crashing.
    assert "N/A" in out


def test_print_summary_zero_session_actions_does_not_divide_by_zero(capsys):
    """Overhead-per-action must not crash when there are zero session actions
    even if there is a non-zero session duration."""
    _print_summary(
        _zero_accumulators(
            sessionCount=1,
            sessionDuration=1_000_000,
            sessionActionCount=0,
        )
    )

    out = capsys.readouterr().out
    assert "Within-session Overhead Duration Per Action" in out


def test_get_all_sessions_handles_missing_started_at():
    """Sessions that have not started yet (no ``startedAt``) must not raise a
    KeyError while sorting."""

    class _FakeDeadline:
        def list_sessions(self, **kwargs):
            return {
                "sessions": [
                    {"sessionId": "session-2", "startedAt": _dt(60)},
                    {"sessionId": "session-1"},  # in-flight, no startedAt yet
                ]
            }

    sessions = _get_all_sessions(_FakeDeadline(), "farm-1", "queue-1", "job-1")

    assert {s["sessionId"] for s in sessions} == {"session-1", "session-2"}


def _dt(offset_seconds: int) -> datetime.datetime:
    return datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc) + datetime.timedelta(
        seconds=offset_seconds
    )


def test_build_trace_events_handles_session_without_step():
    """A session whose step could not be fetched (partial BatchGetStep) must
    not raise a KeyError on ``session['step']``."""
    started = _dt(0)
    ended = _dt(60)
    session = {
        "sessionId": "session-1",
        "workerId": "worker-0",
        "fleetId": "fleet-0",
        "lifecycleStatus": "ENDED",
        "startedAt": started,
        "endedAt": ended,
        "index": 0,
        "actions": [
            {
                "sessionActionId": "sessionaction-1",
                "status": "SUCCEEDED",
                "startedAt": started,
                "endedAt": ended,
                "definition": {"taskRun": {"stepId": "step-1", "taskId": "task-1"}},
            }
        ],
        # No "step" key: the step lookup failed / hasn't been attached.
    }
    workers = {"worker-0": 0}

    trace_events, accumulators = _build_trace_events([session], workers, started, ended)

    assert accumulators["sessionCount"] == 1
    assert accumulators["taskRunCount"] == 1
    # A trace event should still be produced for the session.
    assert any(event["cat"] == "SESSION" for event in trace_events)


def test_build_trace_events_skips_session_without_started_at():
    """A not-yet-started session (no ``startedAt``) must be skipped rather than
    raising a KeyError when building its trace events / timeline timestamp."""
    started = _dt(0)
    ended = _dt(60)
    started_session = {
        "sessionId": "session-1",
        "workerId": "worker-0",
        "fleetId": "fleet-0",
        "lifecycleStatus": "ENDED",
        "startedAt": started,
        "endedAt": ended,
        "index": 0,
        "actions": [],
    }
    not_started_session = {
        "sessionId": "session-2",
        "workerId": "worker-0",
        "fleetId": "fleet-0",
        "lifecycleStatus": "STARTING",
        "index": 1,
        "actions": [],
        # No "startedAt": this session has not begun executing yet.
    }
    workers = {"worker-0": 0}

    trace_events, accumulators = _build_trace_events(
        [started_session, not_started_session], workers, started, ended
    )

    # Only the started session is counted / emitted.
    assert accumulators["sessionCount"] == 1
    session_ids = {
        event["args"]["sessionId"]
        for event in trace_events
        if event["cat"] == "SESSION" and "sessionId" in event.get("args", {})
    }
    assert session_ids == {"session-1"}


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
