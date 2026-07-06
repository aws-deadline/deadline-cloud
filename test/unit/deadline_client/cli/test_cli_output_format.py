# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the TTY-aware ``--output`` default that is shared across the CLI
commands accepting an ``--output`` option.

The format auto-detection lives in ``deadline.client.cli._common``:

  * ``_stdout_is_tty()`` reports whether stdout is an interactive terminal.
  * ``_resolve_output_format()`` returns an explicit value when one is given, and
    otherwise picks ``verbose`` for a TTY and ``json`` for a non-TTY.

These tests cover the helpers directly and then verify that each in-scope command
honors the resolved default end-to-end. The non-TTY path is exercised by patching
``_stdout_is_tty`` to ``False`` (which overrides the autouse ``stdout_is_tty``
fixture from ``conftest.py``), since the click ``CliRunner`` captures stdout into a
non-TTY buffer.
"""

import json
import sys
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

import deadline.client.ui
from deadline.client import api, config
from deadline.client.cli import main
from deadline.client.cli import _common

from ..shared_constants import MOCK_FARM_ID, MOCK_QUEUE_ID

MOCK_JOB_ID = "job-0123456789abcdefabcdefabcdefabcd"


def _non_tty():
    """Patches the CLI to behave as though stdout is not a terminal."""
    return patch.object(_common, "_stdout_is_tty", return_value=False)


def _tty():
    """Patches the CLI to behave as though stdout is an interactive terminal."""
    return patch.object(_common, "_stdout_is_tty", return_value=True)


# ---------------------------------------------------------------------------
# _resolve_output_format / _stdout_is_tty unit tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "explicit,expected",
    [
        ("json", "json"),
        ("verbose", "verbose"),
        ("JSON", "json"),
        ("Verbose", "verbose"),
    ],
)
def test_resolve_output_format_explicit_value_wins(explicit, expected):
    """An explicit --output value is returned (lowercased) regardless of TTY state."""
    # Even when stdout looks like a non-TTY, an explicit value takes precedence...
    with _non_tty():
        assert _common._resolve_output_format(explicit) == expected
    # ...and the same is true when stdout is a TTY.
    with _tty():
        assert _common._resolve_output_format(explicit) == expected


def test_resolve_output_format_defaults_to_verbose_for_tty():
    with _tty():
        assert _common._resolve_output_format(None) == "verbose"


def test_resolve_output_format_defaults_to_json_for_non_tty():
    with _non_tty():
        assert _common._resolve_output_format(None) == "json"


@pytest.mark.real_stdout_isatty
def test_stdout_is_tty_reports_underlying_isatty():
    class FakeStdout:
        def __init__(self, isatty_result):
            self._isatty_result = isatty_result

        def isatty(self):
            return self._isatty_result

    with patch.object(_common.sys, "stdout", FakeStdout(True)):
        assert _common._stdout_is_tty() is True
    with patch.object(_common.sys, "stdout", FakeStdout(False)):
        assert _common._stdout_is_tty() is False


@pytest.mark.real_stdout_isatty
def test_stdout_is_tty_is_defensive_against_broken_streams():
    class NoIsatty:
        pass

    class RaisingIsatty:
        def isatty(self):
            raise ValueError("I/O operation on closed file")

    # A stream without isatty() (AttributeError) is treated as non-interactive.
    with patch.object(_common.sys, "stdout", NoIsatty()):
        assert _common._stdout_is_tty() is False
    # A stream whose isatty() raises (e.g. closed) is treated as non-interactive.
    with patch.object(_common.sys, "stdout", RaisingIsatty()):
        assert _common._stdout_is_tty() is False


# ---------------------------------------------------------------------------
# config show
# ---------------------------------------------------------------------------


def test_config_show_non_tty_defaults_to_json(fresh_deadline_config):
    with _non_tty():
        result = CliRunner().invoke(main, ["config", "show"])

    assert result.exit_code == 0, result.output
    # Output parses as JSON rather than the human-readable listing.
    parsed = json.loads(result.output)
    assert "settings.config_file_path" in parsed


def test_config_show_tty_defaults_to_verbose(fresh_deadline_config):
    with _tty():
        result = CliRunner().invoke(main, ["config", "show"])

    assert result.exit_code == 0, result.output
    assert "AWS Deadline Cloud configuration file:" in result.output
    with pytest.raises(json.JSONDecodeError):
        json.loads(result.output)


def test_config_show_explicit_verbose_wins_over_non_tty(fresh_deadline_config):
    with _non_tty():
        result = CliRunner().invoke(main, ["config", "show", "--output", "verbose"])

    assert result.exit_code == 0, result.output
    assert "AWS Deadline Cloud configuration file:" in result.output


def test_config_show_explicit_json_wins_over_tty(fresh_deadline_config):
    with _tty():
        result = CliRunner().invoke(main, ["config", "show", "--output", "json"])

    assert result.exit_code == 0, result.output
    json.loads(result.output)  # must be valid JSON


# ---------------------------------------------------------------------------
# auth status
# ---------------------------------------------------------------------------


def _invoke_auth_status(args):
    profile_name = "sandbox-us-west-2"
    config.set_setting("defaults.aws_profile_name", profile_name)
    with (
        patch.object(api._session, "get_boto3_session") as session_mock,
        patch.object(api, "get_boto3_session", new=session_mock),
        patch.object(
            api,
            "check_authentication_status",
            return_value=api.AwsAuthenticationStatus.AUTHENTICATED,
        ),
    ):
        session_mock().profile_name = profile_name
        return CliRunner().invoke(main, ["auth", "status", *args])


def test_auth_status_non_tty_defaults_to_json(fresh_deadline_config):
    with _non_tty():
        result = _invoke_auth_status([])

    assert result.exit_code == 0, result.output
    parsed = json.loads(result.output)
    assert "profile_name" in parsed
    assert "status" in parsed


def test_auth_status_tty_defaults_to_verbose(fresh_deadline_config):
    with _tty():
        result = _invoke_auth_status([])

    assert result.exit_code == 0, result.output
    assert "Profile Name: " in result.output
    with pytest.raises(json.JSONDecodeError):
        json.loads(result.output)


def test_auth_status_explicit_verbose_wins_over_non_tty(fresh_deadline_config):
    with _non_tty():
        result = _invoke_auth_status(["--output", "verbose"])

    assert result.exit_code == 0, result.output
    assert "Profile Name: " in result.output


# ---------------------------------------------------------------------------
# job wait
# ---------------------------------------------------------------------------


def _invoke_job_wait(args):
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)
    with (
        patch.object(api, "wait_for_job_completion") as mock_wait,
        patch.object(api, "get_boto3_client") as boto3_client_mock,
    ):
        mock_wait.return_value = api.JobCompletionResult(
            status="SUCCEEDED", failed_tasks=[], elapsed_time=10.5
        )
        boto3_client_mock().get_job.return_value = {"name": "Test Job Name"}
        return CliRunner().invoke(main, ["job", "wait", *args])


def test_job_wait_non_tty_defaults_to_json(fresh_deadline_config):
    with _non_tty():
        result = _invoke_job_wait([])

    assert result.exit_code == 0, result.output
    parsed = json.loads(result.output)
    assert parsed["jobId"] == MOCK_JOB_ID
    assert parsed["status"] == "SUCCEEDED"


def test_job_wait_tty_defaults_to_verbose(fresh_deadline_config):
    with _tty():
        result = _invoke_job_wait([])

    assert result.exit_code == 0, result.output
    assert "Job completed with status: SUCCEEDED" in result.output


def test_job_wait_explicit_verbose_wins_over_non_tty(fresh_deadline_config):
    with _non_tty():
        result = _invoke_job_wait(["--output", "verbose"])

    assert result.exit_code == 0, result.output
    assert "Job completed with status: SUCCEEDED" in result.output


# ---------------------------------------------------------------------------
# job logs
# ---------------------------------------------------------------------------


def _invoke_job_logs(args):
    from deadline.client.api._job_monitoring import SessionLogResult, LogEvent
    import datetime

    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    log_result = SessionLogResult(
        events=[
            LogEvent(
                timestamp=datetime.datetime(
                    2023, 1, 1, 12, 0, 0, 123456, tzinfo=datetime.timezone.utc
                ),
                message="Log message 1",
                ingestion_time=datetime.datetime(
                    2023, 1, 1, 12, 0, 10, 654321, tzinfo=datetime.timezone.utc
                ),
                event_id="event-1",
            ),
        ],
        next_token=None,
        log_group=f"/aws/deadline/{MOCK_FARM_ID}/{MOCK_QUEUE_ID}",
        log_stream="session-test-session",
        count=1,
    )

    with (
        patch("deadline.client.api.get_session_logs") as mock_get_logs,
        patch(
            "deadline.client.api._job_monitoring.get_user_and_identity_store_id"
        ) as mock_get_user,
        patch("deadline.client.api.get_boto3_client") as boto3_client_mock,
    ):
        mock_get_user.return_value = (None, None)
        mock_get_logs.return_value = log_result
        boto3_client_mock().get_job.return_value = {"name": "Test Job Name"}
        return CliRunner().invoke(main, ["job", "logs", "--session-id", "test-session", *args])


def test_job_logs_non_tty_defaults_to_json(fresh_deadline_config):
    with _non_tty():
        result = _invoke_job_logs([])

    assert result.exit_code == 0, result.output
    parsed = json.loads(result.output)
    assert parsed["jobId"] == MOCK_JOB_ID
    assert any(event["message"] == "Log message 1" for event in parsed["events"])


def test_job_logs_tty_defaults_to_verbose(fresh_deadline_config):
    with _tty():
        result = _invoke_job_logs([])

    assert result.exit_code == 0, result.output
    assert "Log message 1" in result.output
    # The verbose form prefixes the message with a bracketed timestamp.
    assert "[2023-01-01T12:00:00.123456+00:00] Log message 1" in result.output


def test_job_logs_explicit_json_wins_over_tty(fresh_deadline_config):
    with _tty():
        result = _invoke_job_logs(["--output", "json"])

    assert result.exit_code == 0, result.output
    parsed = json.loads(result.output)
    assert any(event["message"] == "Log message 1" for event in parsed["events"])


# ---------------------------------------------------------------------------
# bundle gui-submit
# ---------------------------------------------------------------------------


def _invoke_bundle_gui_submit(args):
    submitter_module = MagicMock()
    # The submitter returned by show_job_bundle_submitter reports a submitted job id.
    submitter = submitter_module.show_job_bundle_submitter.return_value
    submitter.job_id = MOCK_JOB_ID
    submitter.job_history_bundle_dir = "/tmp/history"

    with (
        patch.object(deadline.client.ui, "gui_context_for_cli"),
        patch.dict(
            sys.modules,
            {"deadline.client.ui.job_bundle_submitter": submitter_module},
        ),
    ):
        return CliRunner().invoke(main, ["bundle", "gui-submit", "--browse", *args])


def test_bundle_gui_submit_non_tty_defaults_to_json(fresh_deadline_config):
    with _non_tty():
        result = _invoke_bundle_gui_submit([])

    assert result.exit_code == 0, result.output
    parsed = json.loads(result.output)
    assert parsed["status"] == "SUBMITTED"
    assert parsed["jobId"] == MOCK_JOB_ID


def test_bundle_gui_submit_tty_defaults_to_verbose(fresh_deadline_config):
    with _tty():
        result = _invoke_bundle_gui_submit([])

    assert result.exit_code == 0, result.output
    assert "Submitted job bundle:" in result.output
    assert f"Job ID: {MOCK_JOB_ID}" in result.output


def test_bundle_gui_submit_explicit_json_wins_over_tty(fresh_deadline_config):
    with _tty():
        result = _invoke_bundle_gui_submit(["--output", "json"])

    assert result.exit_code == 0, result.output
    parsed = json.loads(result.output)
    assert parsed["jobId"] == MOCK_JOB_ID


# ---------------------------------------------------------------------------
# option help text
# ---------------------------------------------------------------------------


def test_output_help_text_documents_auto_detection(fresh_deadline_config):
    """The --output help should explain the TTY-aware default for discoverability."""
    result = CliRunner().invoke(main, ["job", "wait", "--help"])
    assert result.exit_code == 0, result.output
    assert "terminal" in result.output.lower()
