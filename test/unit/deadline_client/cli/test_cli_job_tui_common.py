# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""Tests for job TUI common utilities."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("rich", reason="TUI tests require the 'rich' package (deadline[tui])")

from unittest.mock import patch

from deadline.client.cli._groups._job_tui._common import (
    clear_screen,
    copy_to_clipboard,
    enter_alt_screen,
    format_short_id,
    format_size,
    format_time_ago,
    get_lifecycle_badge,
    get_status_style,
    get_terminal_page_size,
    leave_alt_screen,
    open_feedback_url,
    render_header,
    render_help_bar,
)


class TestFormatSize:
    def test_bytes(self):
        assert format_size(500) == "500.0 B"

    def test_kilobytes(self):
        assert format_size(2048) == "2.0 KB"

    def test_megabytes(self):
        assert format_size(5 * 1024 * 1024) == "5.0 MB"

    def test_gigabytes(self):
        assert format_size(3 * 1024 * 1024 * 1024) == "3.0 GB"

    def test_zero(self):
        assert format_size(0) == "0.0 B"


class TestFormatTimeAgo:
    def test_none(self):
        assert format_time_ago(None) == ""

    def test_just_now(self):
        now = datetime.now(timezone.utc)
        assert format_time_ago(now) == "just now"

    def test_minutes(self):
        dt = datetime.now(timezone.utc) - timedelta(minutes=5)
        assert format_time_ago(dt) == "5m ago"

    def test_hours(self):
        dt = datetime.now(timezone.utc) - timedelta(hours=3)
        assert format_time_ago(dt) == "3h ago"

    def test_days(self):
        dt = datetime.now(timezone.utc) - timedelta(days=2)
        assert format_time_ago(dt) == "2d ago"


class TestGetStatusStyle:
    def test_succeeded(self):
        color, icon = get_status_style("SUCCEEDED")
        assert color == "green"
        assert icon == "✓"

    def test_running(self):
        color, icon = get_status_style("RUNNING")
        assert color == "yellow"

    def test_failed(self):
        color, icon = get_status_style("FAILED")
        assert color == "red"
        assert icon == "✗"

    def test_canceled(self):
        color, icon = get_status_style("CANCELED")
        assert color == "red"

    def test_pending(self):
        color, icon = get_status_style("PENDING")
        assert color == "blue"

    def test_ready(self):
        color, icon = get_status_style("READY")
        assert color == "cyan"

    def test_suspended(self):
        color, icon = get_status_style("SUSPENDED")
        assert color == "magenta"

    def test_interrupting(self):
        color, icon = get_status_style("INTERRUPTING")
        assert color == "yellow"
        assert icon == "⚡"

    def test_not_compatible(self):
        color, icon = get_status_style("NOT_COMPATIBLE")
        assert color == "red"
        assert icon == "⚠"

    def test_assigned(self):
        color, icon = get_status_style("ASSIGNED")
        assert color == "yellow"

    def test_starting(self):
        color, icon = get_status_style("STARTING")
        assert color == "yellow"

    def test_scheduled(self):
        color, icon = get_status_style("SCHEDULED")
        assert color == "yellow"

    def test_unknown(self):
        color, icon = get_status_style("SOMETHING_ELSE")
        assert color == "dim"


class TestFormatShortId:
    def test_job_id(self):
        assert format_short_id("job-abcdef1234567890abcdef1234567890") == "job-abcdef"

    def test_step_id(self):
        assert format_short_id("step-abcdef1234567890abcdef1234567890") == "step-abcdef"

    def test_task_id(self):
        assert format_short_id("task-abcdef1234567890abcdef1234567890") == "task-abcdef"

    def test_no_dash(self):
        assert format_short_id("nohex") == "nohex"

    def test_short_hex(self):
        assert format_short_id("job-abc") == "job-abc"


class TestGetLifecycleBadge:
    def test_create_complete(self):
        assert get_lifecycle_badge("CREATE_COMPLETE") is None

    def test_update_in_progress(self):
        result = get_lifecycle_badge("UPDATE_IN_PROGRESS")
        assert result is not None
        badge, color = result
        assert badge == "[UPDATING]"
        assert color == "yellow"

    def test_update_failed(self):
        result = get_lifecycle_badge("UPDATE_FAILED")
        assert result is not None
        badge, color = result
        assert badge == "[UPD_FAIL]"
        assert color == "red"

    def test_update_succeeded(self):
        result = get_lifecycle_badge("UPDATE_SUCCEEDED")
        assert result is not None
        badge, color = result
        assert badge == "[UPDATED]"
        assert color == "green"

    def test_unknown(self):
        assert get_lifecycle_badge("UNKNOWN") is None


class TestClearScreen:
    """Tests for the two-mode clear_screen function."""

    @patch("deadline.client.cli._groups._job_tui._common.sys")
    def test_soft_clear_writes_cursor_home_only(self, mock_sys):
        """Soft clear (full=False) should only write cursor-home escape."""
        clear_screen(full=False)
        mock_sys.stdout.write.assert_called_once_with("\033[H")
        mock_sys.stdout.flush.assert_called_once()

    @patch("deadline.client.cli._groups._job_tui._common.sys")
    def test_hard_clear_writes_cursor_home_and_erase(self, mock_sys):
        """Hard clear (full=True) should write cursor-home + erase-screen."""
        clear_screen(full=True)
        mock_sys.stdout.write.assert_called_once_with("\033[H\033[2J")
        mock_sys.stdout.flush.assert_called_once()

    @patch("deadline.client.cli._groups._job_tui._common.sys")
    def test_default_is_full_clear(self, mock_sys):
        """Default call with no args should be full clear."""
        clear_screen()
        mock_sys.stdout.write.assert_called_once_with("\033[H\033[2J")


class TestCopyToClipboard:
    @patch("deadline.client.cli._groups._job_tui._common.subprocess")
    @patch("deadline.client.cli._groups._job_tui._common.sys")
    def test_darwin(self, mock_sys, mock_subprocess):
        mock_sys.platform = "darwin"
        assert copy_to_clipboard("test") is True
        mock_subprocess.run.assert_called_once()

    @patch("deadline.client.cli._groups._job_tui._common.subprocess")
    @patch("deadline.client.cli._groups._job_tui._common.sys")
    def test_linux(self, mock_sys, mock_subprocess):
        mock_sys.platform = "linux"
        assert copy_to_clipboard("test") is True

    @patch("deadline.client.cli._groups._job_tui._common.subprocess")
    @patch("deadline.client.cli._groups._job_tui._common.sys")
    def test_win32(self, mock_sys, mock_subprocess):
        mock_sys.platform = "win32"
        assert copy_to_clipboard("test") is True

    @patch("deadline.client.cli._groups._job_tui._common.subprocess")
    @patch("deadline.client.cli._groups._job_tui._common.sys")
    def test_unsupported_platform(self, mock_sys, mock_subprocess):
        mock_sys.platform = "freebsd"
        assert copy_to_clipboard("test") is False

    @patch("deadline.client.cli._groups._job_tui._common.subprocess")
    @patch("deadline.client.cli._groups._job_tui._common.sys")
    def test_subprocess_error(self, mock_sys, mock_subprocess):
        import subprocess as sp

        mock_sys.platform = "darwin"
        mock_subprocess.run.side_effect = sp.SubprocessError
        mock_subprocess.SubprocessError = sp.SubprocessError
        assert copy_to_clipboard("test") is False


class TestOpenFeedbackUrl:
    @patch("webbrowser.open", return_value=True)
    def test_success(self, mock_open):
        result = open_feedback_url()
        assert "Opened" in result

    @patch("webbrowser.open", return_value=False)
    def test_failure(self, mock_open):
        result = open_feedback_url()
        assert "feedback" in result.lower()

    @patch("webbrowser.open", side_effect=Exception("no browser"))
    def test_exception(self, mock_open):
        result = open_feedback_url()
        assert "feedback" in result.lower()


class TestEnterLeaveAltScreen:
    @patch("deadline.client.cli._groups._job_tui._common.sys")
    def test_enter_alt_screen(self, mock_sys):
        enter_alt_screen()
        mock_sys.stdout.write.assert_called_once()
        mock_sys.stdout.flush.assert_called_once()

    @patch("deadline.client.cli._groups._job_tui._common.sys")
    def test_leave_alt_screen(self, mock_sys):
        leave_alt_screen()
        mock_sys.stdout.write.assert_called_once()
        mock_sys.stdout.flush.assert_called_once()


class TestRenderHeader:
    @patch("deadline.client.cli._groups._job_tui._common.console")
    def test_render_header(self, mock_console):
        render_header("Title", "Sub")
        mock_console.print.assert_called_once()


class TestRenderHelpBar:
    @patch("deadline.client.cli._groups._job_tui._common.sys")
    @patch("deadline.client.cli._groups._job_tui._common.console")
    def test_render_help_bar(self, mock_console, mock_sys):
        render_help_bar([("q", "quit"), ("r", "refresh")])
        mock_console.print.assert_called_once()


class TestGetTerminalPageSize:
    @patch("deadline.client.cli._groups._job_tui._common.console")
    def test_returns_at_least_5(self, mock_console):
        mock_console.height = 10
        assert get_terminal_page_size(chrome_lines=18) == 5

    @patch("deadline.client.cli._groups._job_tui._common.console")
    def test_large_terminal(self, mock_console):
        mock_console.height = 50
        assert get_terminal_page_size(chrome_lines=18) == 32
