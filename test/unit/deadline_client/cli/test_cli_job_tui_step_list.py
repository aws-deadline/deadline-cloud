# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""Tests for StepListTUI."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("rich", reason="TUI tests require the 'rich' package (deadline[tui])")

from deadline.client.cli._groups._job_tui._step_list import StepListTUI


@pytest.fixture
def mock_deadline_client():
    client = MagicMock()
    client.list_steps.return_value = {
        "steps": [
            {
                "stepId": "step-abcdef1234567890abcdef1234567890",
                "name": "Render Frames",
                "taskRunStatus": "SUCCEEDED",
                "lifecycleStatus": "CREATE_COMPLETE",
                "taskRunStatusCounts": {"SUCCEEDED": 120},
            },
            {
                "stepId": "step-11111111111111111111111111111111",
                "name": "Cleanup",
                "taskRunStatus": "FAILED",
                "targetTaskRunStatus": "CANCELED",
                "lifecycleStatus": "UPDATE_FAILED",
                "taskRunStatusCounts": {"FAILED": 2, "SUCCEEDED": 1},
            },
        ],
    }
    return client


class TestStepListTUI:
    def test_load_page(self, mock_deadline_client):
        tui = StepListTUI(
            "farm-1", "queue-1", "job-1", "Test Job", "SUCCEEDED", mock_deadline_client
        )
        tui.prev_tokens = [None]
        tui.load_page()
        assert len(tui.steps) == 2

    @patch("deadline.client.cli._groups._job_tui._step_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._step_list.console")
    def test_run_select_step(self, mock_console, mock_read_key, mock_deadline_client):
        mock_read_key.return_value = "enter"
        tui = StepListTUI(
            "farm-1", "queue-1", "job-1", "Test Job", "SUCCEEDED", mock_deadline_client
        )
        result = tui.run()
        assert result == ("select", "step-abcdef1234567890abcdef1234567890", "Render Frames")

    @patch("deadline.client.cli._groups._job_tui._step_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._step_list.console")
    def test_run_back(self, mock_console, mock_read_key, mock_deadline_client):
        mock_read_key.return_value = "esc"
        tui = StepListTUI(
            "farm-1", "queue-1", "job-1", "Test Job", "SUCCEEDED", mock_deadline_client
        )
        result = tui.run()
        assert result == ("back", "")

    @patch("deadline.client.cli._groups._job_tui._step_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._step_list.console")
    def test_run_left_arrow_goes_back(self, mock_console, mock_read_key, mock_deadline_client):
        mock_read_key.return_value = "left"
        tui = StepListTUI(
            "farm-1", "queue-1", "job-1", "Test Job", "SUCCEEDED", mock_deadline_client
        )
        result = tui.run()
        assert result == ("back", "")

    @patch("deadline.client.cli._groups._job_tui._step_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._step_list.console")
    def test_run_right_arrow_selects(self, mock_console, mock_read_key, mock_deadline_client):
        mock_read_key.return_value = "right"
        tui = StepListTUI(
            "farm-1", "queue-1", "job-1", "Test Job", "SUCCEEDED", mock_deadline_client
        )
        result = tui.run()
        assert result == ("select", "step-abcdef1234567890abcdef1234567890", "Render Frames")

    @patch("deadline.client.cli._groups._job_tui._step_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._step_list.console")
    def test_run_quit(self, mock_console, mock_read_key, mock_deadline_client):
        mock_read_key.return_value = "q"
        tui = StepListTUI(
            "farm-1", "queue-1", "job-1", "Test Job", "SUCCEEDED", mock_deadline_client
        )
        result = tui.run()
        assert result is None

    @patch("deadline.client.cli._groups._job_tui._step_list.copy_to_clipboard", return_value=True)
    @patch("deadline.client.cli._groups._job_tui._step_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._step_list.console")
    def test_copy_step_id(self, mock_console, mock_read_key, mock_copy, mock_deadline_client):
        mock_read_key.side_effect = ["c", "q"]
        tui = StepListTUI(
            "farm-1", "queue-1", "job-1", "Test Job", "SUCCEEDED", mock_deadline_client
        )
        tui.run()
        mock_copy.assert_called_once_with("step-abcdef1234567890abcdef1234567890")

    @patch("deadline.client.cli._groups._job_tui._step_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._step_list.console")
    def test_cursor_navigation(self, mock_console, mock_read_key, mock_deadline_client):
        mock_read_key.side_effect = ["down", "enter"]
        tui = StepListTUI(
            "farm-1", "queue-1", "job-1", "Test Job", "SUCCEEDED", mock_deadline_client
        )
        result = tui.run()
        assert result == ("select", "step-11111111111111111111111111111111", "Cleanup")

    @patch("deadline.client.cli._groups._job_tui._step_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._step_list.console")
    def test_next_page(self, mock_console, mock_read_key, mock_deadline_client):
        mock_deadline_client.list_steps.return_value = {
            "steps": [
                {
                    "stepId": "step-aaa",
                    "name": "Step A",
                    "taskRunStatus": "SUCCEEDED",
                    "taskRunStatusCounts": {"SUCCEEDED": 1},
                }
            ],
            "nextToken": "token1",
        }
        mock_read_key.side_effect = ["n", "q"]
        tui = StepListTUI(
            "farm-1", "queue-1", "job-1", "Test Job", "SUCCEEDED", mock_deadline_client
        )
        tui.run()
        assert tui.page == 1

    @patch("deadline.client.cli._groups._job_tui._step_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._step_list.console")
    def test_prev_page(self, mock_console, mock_read_key, mock_deadline_client):
        mock_deadline_client.list_steps.return_value = {
            "steps": [
                {
                    "stepId": "step-aaa",
                    "name": "Step A",
                    "taskRunStatus": "SUCCEEDED",
                    "taskRunStatusCounts": {"SUCCEEDED": 1},
                }
            ],
            "nextToken": "token1",
        }
        mock_read_key.side_effect = ["n", "p", "q"]
        tui = StepListTUI(
            "farm-1", "queue-1", "job-1", "Test Job", "SUCCEEDED", mock_deadline_client
        )
        tui.run()
        assert tui.page == 0

    @patch("deadline.client.cli._groups._job_tui._step_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._step_list.console")
    def test_refresh(self, mock_console, mock_read_key, mock_deadline_client):
        mock_read_key.side_effect = ["r", "q"]
        tui = StepListTUI(
            "farm-1", "queue-1", "job-1", "Test Job", "SUCCEEDED", mock_deadline_client
        )
        tui.run()
        assert mock_deadline_client.list_steps.call_count == 2

    @patch("deadline.client.cli._groups._job_tui._step_list.open_feedback_url", return_value="msg")
    @patch("deadline.client.cli._groups._job_tui._step_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._step_list.console")
    def test_feedback(self, mock_console, mock_read_key, mock_feedback, mock_deadline_client):
        mock_read_key.side_effect = ["f", "q"]
        tui = StepListTUI(
            "farm-1", "queue-1", "job-1", "Test Job", "SUCCEEDED", mock_deadline_client
        )
        tui.run()
        mock_feedback.assert_called_once()

    @patch("deadline.client.cli._groups._job_tui._step_list.copy_to_clipboard", return_value=False)
    @patch("deadline.client.cli._groups._job_tui._step_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._step_list.console")
    def test_copy_clipboard_fail(
        self, mock_console, mock_read_key, mock_copy, mock_deadline_client
    ):
        mock_read_key.side_effect = ["c", "q"]
        tui = StepListTUI(
            "farm-1", "queue-1", "job-1", "Test Job", "SUCCEEDED", mock_deadline_client
        )
        tui.run()
        assert "step-abcdef" in tui.message or mock_copy.called

    @patch("deadline.client.cli._groups._job_tui._step_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._step_list.console")
    def test_down_wraps_to_next_page(self, mock_console, mock_read_key, mock_deadline_client):
        """Pressing down at the last item with a next token should go to next page."""
        mock_deadline_client.list_steps.side_effect = [
            {
                "steps": [
                    {
                        "stepId": "step-aaa",
                        "name": "A",
                        "taskRunStatus": "SUCCEEDED",
                        "taskRunStatusCounts": {"SUCCEEDED": 1},
                    }
                ],
                "nextToken": "tok",
            },
            {
                "steps": [
                    {
                        "stepId": "step-bbb",
                        "name": "B",
                        "taskRunStatus": "SUCCEEDED",
                        "taskRunStatusCounts": {"SUCCEEDED": 1},
                    }
                ],
            },
        ]
        mock_read_key.side_effect = ["down", "enter"]
        tui = StepListTUI(
            "farm-1", "queue-1", "job-1", "Test Job", "SUCCEEDED", mock_deadline_client
        )
        result = tui.run()
        assert result == ("select", "step-bbb", "B")

    @patch("deadline.client.cli._groups._job_tui._step_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._step_list.console")
    def test_up_wraps_to_prev_page(self, mock_console, mock_read_key, mock_deadline_client):
        """Pressing up at cursor 0 on page 1 should go back to previous page."""
        mock_deadline_client.list_steps.side_effect = [
            {
                "steps": [
                    {
                        "stepId": "step-aaa",
                        "name": "A",
                        "taskRunStatus": "SUCCEEDED",
                        "taskRunStatusCounts": {"SUCCEEDED": 1},
                    }
                ],
                "nextToken": "tok",
            },
            {
                "steps": [
                    {
                        "stepId": "step-bbb",
                        "name": "B",
                        "taskRunStatus": "SUCCEEDED",
                        "taskRunStatusCounts": {"SUCCEEDED": 1},
                    }
                ],
            },
            {
                "steps": [
                    {
                        "stepId": "step-aaa",
                        "name": "A",
                        "taskRunStatus": "SUCCEEDED",
                        "taskRunStatusCounts": {"SUCCEEDED": 1},
                    }
                ],
                "nextToken": "tok",
            },
        ]
        mock_read_key.side_effect = ["n", "up", "enter"]
        tui = StepListTUI(
            "farm-1", "queue-1", "job-1", "Test Job", "SUCCEEDED", mock_deadline_client
        )
        result = tui.run()
        assert result == ("select", "step-aaa", "A")
