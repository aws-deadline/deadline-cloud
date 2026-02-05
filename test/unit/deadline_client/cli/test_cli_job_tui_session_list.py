# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""Tests for SessionListTUI."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("rich", reason="TUI tests require the 'rich' package (deadline[tui])")

from deadline.client.cli._groups._job_tui._session_list import SessionListTUI


@pytest.fixture
def mock_deadline_client():
    client = MagicMock()
    client.list_sessions.return_value = {
        "sessions": [
            {
                "sessionId": "session-abcdef1234567890abcdef1234567890",
                "lifecycleStatus": "ENDED",
                "workerId": "worker-11111111111111111111111111111111",
                "startedAt": datetime(2026, 2, 8, 10, 0, 0, tzinfo=timezone.utc),
            },
        ],
    }
    client.list_session_actions.return_value = {
        "sessionActions": [
            {
                "definition": {
                    "taskRun": {
                        "stepId": "step-aaa",
                        "taskId": "task-bbb",
                    }
                }
            }
        ],
    }
    return client


class TestSessionListTUI:
    def test_load_sessions_finds_matching(self, mock_deadline_client):
        tui = SessionListTUI(
            "farm-1", "queue-1", "job-1", "step-aaa", "task-bbb", "Frame=1", mock_deadline_client
        )
        tui.load_sessions()
        assert len(tui.sessions) == 1

    def test_load_sessions_no_match(self, mock_deadline_client):
        tui = SessionListTUI(
            "farm-1", "queue-1", "job-1", "step-xxx", "task-yyy", "Frame=1", mock_deadline_client
        )
        tui.load_sessions()
        assert len(tui.sessions) == 0

    @patch("deadline.client.cli._groups._job_tui._session_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._session_list.console")
    def test_run_esc_returns(self, mock_console, mock_read_key, mock_deadline_client):
        mock_read_key.return_value = "esc"
        tui = SessionListTUI(
            "farm-1", "queue-1", "job-1", "step-aaa", "task-bbb", "Frame=1", mock_deadline_client
        )
        tui.run()  # Should return without error

    @patch("deadline.client.cli._groups._job_tui._session_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._session_list.console")
    def test_run_quit(self, mock_console, mock_read_key, mock_deadline_client):
        mock_read_key.return_value = "q"
        tui = SessionListTUI(
            "farm-1", "queue-1", "job-1", "step-aaa", "task-bbb", "Frame=1", mock_deadline_client
        )
        tui.run()  # Should return without error

    @patch(
        "deadline.client.cli._groups._job_tui._session_list.copy_to_clipboard",
        return_value=True,
    )
    @patch("deadline.client.cli._groups._job_tui._session_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._session_list.console")
    def test_copy_session_id(self, mock_console, mock_read_key, mock_copy, mock_deadline_client):
        mock_read_key.side_effect = ["c", "q"]
        tui = SessionListTUI(
            "farm-1", "queue-1", "job-1", "step-aaa", "task-bbb", "Frame=1", mock_deadline_client
        )
        tui.run()
        mock_copy.assert_called_once_with("session-abcdef1234567890abcdef1234567890")

    @patch("deadline.client.cli._groups._job_tui._session_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._session_list.console")
    def test_run_left_returns(self, mock_console, mock_read_key, mock_deadline_client):
        mock_read_key.return_value = "left"
        tui = SessionListTUI(
            "farm-1", "queue-1", "job-1", "step-aaa", "task-bbb", "Frame=1", mock_deadline_client
        )
        tui.run()

    @patch("deadline.client.cli._groups._job_tui._session_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._session_list.console")
    def test_cursor_navigation(self, mock_console, mock_read_key, mock_deadline_client):
        mock_read_key.side_effect = ["down", "up", "q"]
        tui = SessionListTUI(
            "farm-1", "queue-1", "job-1", "step-aaa", "task-bbb", "Frame=1", mock_deadline_client
        )
        tui.run()

    @patch(
        "deadline.client.cli._groups._job_tui._session_list.open_feedback_url", return_value="msg"
    )
    @patch("deadline.client.cli._groups._job_tui._session_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._session_list.console")
    def test_feedback(self, mock_console, mock_read_key, mock_feedback, mock_deadline_client):
        mock_read_key.side_effect = ["f", "q"]
        tui = SessionListTUI(
            "farm-1", "queue-1", "job-1", "step-aaa", "task-bbb", "Frame=1", mock_deadline_client
        )
        tui.run()
        mock_feedback.assert_called_once()

    @patch(
        "deadline.client.cli._groups._job_tui._session_list.copy_to_clipboard",
        return_value=False,
    )
    @patch("deadline.client.cli._groups._job_tui._session_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._session_list.console")
    def test_copy_clipboard_fail(
        self, mock_console, mock_read_key, mock_copy, mock_deadline_client
    ):
        mock_read_key.side_effect = ["c", "q"]
        tui = SessionListTUI(
            "farm-1", "queue-1", "job-1", "step-aaa", "task-bbb", "Frame=1", mock_deadline_client
        )
        tui.run()
        mock_copy.assert_called_once()

    def test_load_sessions_exception_skips(self, mock_deadline_client):
        """Sessions where list_session_actions raises should be skipped."""
        mock_deadline_client.list_session_actions.side_effect = Exception("access denied")
        tui = SessionListTUI(
            "farm-1", "queue-1", "job-1", "step-aaa", "task-bbb", "Frame=1", mock_deadline_client
        )
        tui.load_sessions()
        assert len(tui.sessions) == 0
