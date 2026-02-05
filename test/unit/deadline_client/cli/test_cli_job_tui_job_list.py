# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""Tests for JobListTUI."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch

import pytest

pytest.importorskip("rich", reason="TUI tests require the 'rich' package (deadline[tui])")

from deadline.client.cli._groups._job_tui._job_list import JobListTUI


@pytest.fixture
def mock_deadline_client():
    client = MagicMock()
    client.search_jobs.return_value = {
        "jobs": [
            {
                "jobId": "job-abcdef1234567890abcdef1234567890",
                "name": "Test Render Job",
                "taskRunStatus": "SUCCEEDED",
                "createdAt": datetime(2026, 2, 8, 10, 0, 0, tzinfo=timezone.utc),
            },
            {
                "jobId": "job-11111111111111111111111111111111",
                "name": "Running Job",
                "taskRunStatus": "RUNNING",
                "targetTaskRunStatus": "CANCELED",
                "createdAt": datetime(2026, 2, 8, 9, 0, 0, tzinfo=timezone.utc),
            },
        ],
        "totalResults": 2,
    }
    return client


class TestJobListTUI:
    def test_load_page(self, mock_deadline_client):
        tui = JobListTUI("farm-123", "queue-456", mock_deadline_client)
        tui.load_page()
        assert len(tui.jobs) == 2
        assert tui.total_jobs == 2
        mock_deadline_client.search_jobs.assert_called_once()

    def test_load_page_pagination(self, mock_deadline_client):
        tui = JobListTUI("farm-123", "queue-456", mock_deadline_client)
        tui.page = 2
        tui.load_page()
        call_kwargs = mock_deadline_client.search_jobs.call_args[1]
        assert call_kwargs["itemOffset"] == 2 * tui.page_size

    @patch("deadline.client.cli._groups._job_tui._job_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._job_list.console")
    def test_run_select_job(self, mock_console, mock_read_key, mock_deadline_client):
        mock_read_key.return_value = "enter"
        tui = JobListTUI("farm-123", "queue-456", mock_deadline_client)
        result = tui.run()
        assert result == ("select", "job-abcdef1234567890abcdef1234567890")

    @patch("deadline.client.cli._groups._job_tui._job_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._job_list.console")
    def test_run_quit(self, mock_console, mock_read_key, mock_deadline_client):
        mock_read_key.return_value = "q"
        tui = JobListTUI("farm-123", "queue-456", mock_deadline_client)
        result = tui.run()
        assert result is None

    @patch("deadline.client.cli._groups._job_tui._job_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._job_list.console")
    def test_run_attachments(self, mock_console, mock_read_key, mock_deadline_client):
        mock_read_key.return_value = "a"
        tui = JobListTUI("farm-123", "queue-456", mock_deadline_client)
        result = tui.run()
        assert result == ("attachments", "job-abcdef1234567890abcdef1234567890")

    @patch("deadline.client.cli._groups._job_tui._job_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._job_list.console")
    def test_run_right_arrow_selects(self, mock_console, mock_read_key, mock_deadline_client):
        mock_read_key.return_value = "right"
        tui = JobListTUI("farm-123", "queue-456", mock_deadline_client)
        result = tui.run()
        assert result == ("select", "job-abcdef1234567890abcdef1234567890")

    @patch("deadline.client.cli._groups._job_tui._job_list.copy_to_clipboard", return_value=True)
    @patch("deadline.client.cli._groups._job_tui._job_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._job_list.console")
    def test_run_copy_id(self, mock_console, mock_read_key, mock_copy, mock_deadline_client):
        # First press 'c' to copy, then 'q' to quit
        mock_read_key.side_effect = ["c", "q"]
        tui = JobListTUI("farm-123", "queue-456", mock_deadline_client)
        result = tui.run()
        mock_copy.assert_called_once_with("job-abcdef1234567890abcdef1234567890")
        assert result is None

    @patch("deadline.client.cli._groups._job_tui._job_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._job_list.console")
    def test_cursor_movement(self, mock_console, mock_read_key, mock_deadline_client):
        mock_read_key.side_effect = ["down", "enter"]
        tui = JobListTUI("farm-123", "queue-456", mock_deadline_client)
        result = tui.run()
        assert result == ("select", "job-11111111111111111111111111111111")

    @patch("deadline.client.cli._groups._job_tui._job_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._job_list.console")
    def test_cursor_does_not_go_below_zero(self, mock_console, mock_read_key, mock_deadline_client):
        mock_read_key.side_effect = ["up", "up", "enter"]
        tui = JobListTUI("farm-123", "queue-456", mock_deadline_client)
        result = tui.run()
        # Cursor should stay at 0
        assert result == ("select", "job-abcdef1234567890abcdef1234567890")

    @patch("deadline.client.cli._groups._job_tui._job_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._job_list.console")
    def test_empty_jobs(self, mock_console, mock_read_key, mock_deadline_client):
        mock_deadline_client.search_jobs.return_value = {"jobs": [], "totalResults": 0}
        mock_read_key.return_value = "q"
        tui = JobListTUI("farm-123", "queue-456", mock_deadline_client)
        result = tui.run()
        assert result is None


class TestJobListTUIScreenClearing:
    """Tests for the _needs_full_clear flag and clear_screen behavior."""

    @patch("deadline.client.cli._groups._job_tui._job_list.clear_screen")
    @patch("deadline.client.cli._groups._job_tui._job_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._job_list.console")
    def test_needs_full_clear_true_on_first_render(
        self, mock_console, mock_read_key, mock_clear, mock_deadline_client
    ):
        """First render after run() should call clear_screen(full=True)."""
        mock_read_key.return_value = "q"
        tui = JobListTUI("farm-123", "queue-456", mock_deadline_client)
        tui.run()
        # First call to clear_screen should be full=True
        assert mock_clear.call_args_list[0] == call(full=True)

    @patch("deadline.client.cli._groups._job_tui._job_list.clear_screen")
    @patch("deadline.client.cli._groups._job_tui._job_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._job_list.console")
    def test_needs_full_clear_false_after_first_render(
        self, mock_console, mock_read_key, mock_clear, mock_deadline_client
    ):
        """Second render (after a keypress) should call clear_screen(full=False)."""
        # Press down then quit — triggers two render() calls
        mock_read_key.side_effect = ["down", "q"]
        tui = JobListTUI("farm-123", "queue-456", mock_deadline_client)
        tui.run()
        assert len(mock_clear.call_args_list) >= 2
        # First render: full=True, second render: full=False
        assert mock_clear.call_args_list[0] == call(full=True)
        assert mock_clear.call_args_list[1] == call(full=False)

    @patch("deadline.client.cli._groups._job_tui._job_list.clear_screen")
    @patch("deadline.client.cli._groups._job_tui._job_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._job_list.console")
    def test_needs_full_clear_reset_on_rerun(
        self, mock_console, mock_read_key, mock_clear, mock_deadline_client
    ):
        """Calling run() again should reset _needs_full_clear to True."""
        mock_read_key.return_value = "q"
        tui = JobListTUI("farm-123", "queue-456", mock_deadline_client)

        # First run
        tui.run()
        first_run_first_call = mock_clear.call_args_list[0]
        assert first_run_first_call == call(full=True)

        mock_clear.reset_mock()

        # Second run — should hard-clear again on first render
        tui.run()
        assert mock_clear.call_args_list[0] == call(full=True)

    def test_needs_full_clear_initial_value(self, mock_deadline_client):
        """_needs_full_clear should be True after construction."""
        tui = JobListTUI("farm-123", "queue-456", mock_deadline_client)
        assert tui._needs_full_clear is True

    @patch("deadline.client.cli._groups._job_tui._job_list.clear_screen")
    @patch("deadline.client.cli._groups._job_tui._job_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._job_list.console")
    def test_subsequent_renders_all_soft_clear(
        self, mock_console, mock_read_key, mock_clear, mock_deadline_client
    ):
        """All render() calls after the first should use soft clear (full=False).

        Note: the quit handler also calls clear_screen() with no args (full=False
        by default), so we check all calls from render (indices 0..N-1) plus the
        final bare clear_screen() on quit.
        """
        # Navigate: down, down, up, quit — 4 renders after the initial one, plus quit clear
        mock_read_key.side_effect = ["down", "down", "up", "q"]
        tui = JobListTUI("farm-123", "queue-456", mock_deadline_client)
        tui.run()
        # First call is hard clear (from render)
        assert mock_clear.call_args_list[0] == call(full=True)
        # Remaining render calls use soft clear (full=False)
        # Last call is the bare clear_screen() from quit handler (no args)
        for c in mock_clear.call_args_list[1:-1]:
            assert c == call(full=False)

    @patch("deadline.client.cli._groups._job_tui._job_list.clear_screen")
    @patch("deadline.client.cli._groups._job_tui._job_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._job_list.console")
    def test_page_change_triggers_full_clear(
        self, mock_console, mock_read_key, mock_clear, mock_deadline_client
    ):
        """Pressing 'n' to go to next page should trigger a full clear."""
        mock_deadline_client.search_jobs.return_value = {
            "jobs": [
                {
                    "jobId": "job-aaaa",
                    "name": "Job A",
                    "taskRunStatus": "SUCCEEDED",
                },
            ],
            "totalResults": 50,
        }
        # Press n (next page), then q
        mock_read_key.side_effect = ["n", "q"]
        tui = JobListTUI("farm-123", "queue-456", mock_deadline_client)
        tui.run()
        # call 0: initial render (full=True)
        # call 1: render after page change (full=True again)
        assert mock_clear.call_args_list[0] == call(full=True)
        assert mock_clear.call_args_list[1] == call(full=True)
