# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""Tests for TaskListTUI."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("rich", reason="TUI tests require the 'rich' package (deadline[tui])")

from deadline.client.cli._groups._job_tui._task_list import TaskListTUI


@pytest.fixture
def mock_deadline_client():
    client = MagicMock()
    client.list_tasks.return_value = {
        "tasks": [
            {
                "taskId": "task-abcdef1234567890abcdef1234567890",
                "runStatus": "SUCCEEDED",
                "parameters": {"Frame": {"int": "1"}},
            },
            {
                "taskId": "task-11111111111111111111111111111111",
                "runStatus": "RUNNING",
                "targetRunStatus": "CANCELED",
                "parameters": {"Frame": {"int": "2"}},
            },
        ],
    }
    return client


class TestTaskListTUI:
    def test_load_page(self, mock_deadline_client):
        tui = TaskListTUI(
            "farm-1", "queue-1", "job-1", "Test Job", "step-1", "Render", mock_deadline_client
        )
        tui.prev_tokens = [None]
        tui.load_page()
        assert len(tui.tasks) == 2

    def test_format_task_params(self, mock_deadline_client):
        task = {"parameters": {"Frame": {"int": "1"}, "Chunk": {"string": "A"}}}
        result = TaskListTUI._format_task_params(task)
        assert "Frame=1" in result
        assert "Chunk=A" in result

    def test_format_task_params_empty(self, mock_deadline_client):
        task = {"taskId": "task-abcdef1234567890abcdef1234567890"}
        result = TaskListTUI._format_task_params(task)
        assert result == "34567890"  # last 8 chars of task ID

    @patch("deadline.client.cli._groups._job_tui._task_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._task_list.console")
    def test_run_sessions(self, mock_console, mock_read_key, mock_deadline_client):
        mock_read_key.return_value = "l"
        tui = TaskListTUI(
            "farm-1", "queue-1", "job-1", "Test Job", "step-1", "Render", mock_deadline_client
        )
        result = tui.run()
        assert result == ("sessions", "task-abcdef1234567890abcdef1234567890")

    @patch("deadline.client.cli._groups._job_tui._task_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._task_list.console")
    def test_run_attachments(self, mock_console, mock_read_key, mock_deadline_client):
        mock_read_key.return_value = "a"
        tui = TaskListTUI(
            "farm-1", "queue-1", "job-1", "Test Job", "step-1", "Render", mock_deadline_client
        )
        result = tui.run()
        assert result == ("attachments", "task-abcdef1234567890abcdef1234567890")

    @patch("deadline.client.cli._groups._job_tui._task_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._task_list.console")
    def test_run_back(self, mock_console, mock_read_key, mock_deadline_client):
        mock_read_key.return_value = "esc"
        tui = TaskListTUI(
            "farm-1", "queue-1", "job-1", "Test Job", "step-1", "Render", mock_deadline_client
        )
        result = tui.run()
        assert result == ("back", "")

    @patch("deadline.client.cli._groups._job_tui._task_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._task_list.console")
    def test_run_left_goes_back(self, mock_console, mock_read_key, mock_deadline_client):
        mock_read_key.return_value = "left"
        tui = TaskListTUI(
            "farm-1", "queue-1", "job-1", "Test Job", "step-1", "Render", mock_deadline_client
        )
        result = tui.run()
        assert result == ("back", "")

    @patch("deadline.client.cli._groups._job_tui._task_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._task_list.console")
    def test_run_quit(self, mock_console, mock_read_key, mock_deadline_client):
        mock_read_key.return_value = "q"
        tui = TaskListTUI(
            "farm-1", "queue-1", "job-1", "Test Job", "step-1", "Render", mock_deadline_client
        )
        result = tui.run()
        assert result is None

    @patch("deadline.client.cli._groups._job_tui._task_list.copy_to_clipboard", return_value=True)
    @patch("deadline.client.cli._groups._job_tui._task_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._task_list.console")
    def test_copy_task_id(self, mock_console, mock_read_key, mock_copy, mock_deadline_client):
        mock_read_key.side_effect = ["c", "q"]
        tui = TaskListTUI(
            "farm-1", "queue-1", "job-1", "Test Job", "step-1", "Render", mock_deadline_client
        )
        tui.run()
        mock_copy.assert_called_once_with("task-abcdef1234567890abcdef1234567890")

    @patch("deadline.client.cli._groups._job_tui._task_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._task_list.console")
    def test_cursor_navigation(self, mock_console, mock_read_key, mock_deadline_client):
        mock_read_key.side_effect = ["down", "l"]
        tui = TaskListTUI(
            "farm-1", "queue-1", "job-1", "Test Job", "step-1", "Render", mock_deadline_client
        )
        result = tui.run()
        assert result == ("sessions", "task-11111111111111111111111111111111")

    @patch("deadline.client.cli._groups._job_tui._task_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._task_list.console")
    def test_next_page(self, mock_console, mock_read_key, mock_deadline_client):
        mock_deadline_client.list_tasks.return_value = {
            "tasks": [
                {"taskId": "task-aaa", "runStatus": "SUCCEEDED", "parameters": {}},
            ],
            "nextToken": "token1",
        }
        mock_read_key.side_effect = ["n", "q"]
        tui = TaskListTUI(
            "farm-1", "queue-1", "job-1", "Test Job", "step-1", "Render", mock_deadline_client
        )
        tui.run()
        assert tui.page == 1

    @patch("deadline.client.cli._groups._job_tui._task_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._task_list.console")
    def test_prev_page(self, mock_console, mock_read_key, mock_deadline_client):
        mock_deadline_client.list_tasks.return_value = {
            "tasks": [
                {"taskId": "task-aaa", "runStatus": "SUCCEEDED", "parameters": {}},
            ],
            "nextToken": "token1",
        }
        mock_read_key.side_effect = ["n", "p", "q"]
        tui = TaskListTUI(
            "farm-1", "queue-1", "job-1", "Test Job", "step-1", "Render", mock_deadline_client
        )
        tui.run()
        assert tui.page == 0

    @patch("deadline.client.cli._groups._job_tui._task_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._task_list.console")
    def test_refresh(self, mock_console, mock_read_key, mock_deadline_client):
        mock_read_key.side_effect = ["r", "q"]
        tui = TaskListTUI(
            "farm-1", "queue-1", "job-1", "Test Job", "step-1", "Render", mock_deadline_client
        )
        tui.run()
        assert mock_deadline_client.list_tasks.call_count == 2

    @patch("deadline.client.cli._groups._job_tui._task_list.open_feedback_url", return_value="msg")
    @patch("deadline.client.cli._groups._job_tui._task_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._task_list.console")
    def test_feedback(self, mock_console, mock_read_key, mock_feedback, mock_deadline_client):
        mock_read_key.side_effect = ["f", "q"]
        tui = TaskListTUI(
            "farm-1", "queue-1", "job-1", "Test Job", "step-1", "Render", mock_deadline_client
        )
        tui.run()
        mock_feedback.assert_called_once()

    @patch("deadline.client.cli._groups._job_tui._task_list.copy_to_clipboard", return_value=False)
    @patch("deadline.client.cli._groups._job_tui._task_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._task_list.console")
    def test_copy_clipboard_fail(
        self, mock_console, mock_read_key, mock_copy, mock_deadline_client
    ):
        mock_read_key.side_effect = ["c", "q"]
        tui = TaskListTUI(
            "farm-1", "queue-1", "job-1", "Test Job", "step-1", "Render", mock_deadline_client
        )
        tui.run()
        mock_copy.assert_called_once()

    @patch("deadline.client.cli._groups._job_tui._task_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._task_list.console")
    def test_down_wraps_to_next_page(self, mock_console, mock_read_key, mock_deadline_client):
        mock_deadline_client.list_tasks.side_effect = [
            {
                "tasks": [{"taskId": "task-aaa", "runStatus": "SUCCEEDED", "parameters": {}}],
                "nextToken": "tok",
            },
            {
                "tasks": [{"taskId": "task-bbb", "runStatus": "SUCCEEDED", "parameters": {}}],
            },
        ]
        mock_read_key.side_effect = ["down", "l"]
        tui = TaskListTUI(
            "farm-1", "queue-1", "job-1", "Test Job", "step-1", "Render", mock_deadline_client
        )
        result = tui.run()
        assert result == ("sessions", "task-bbb")

    @patch("deadline.client.cli._groups._job_tui._task_list.read_key")
    @patch("deadline.client.cli._groups._job_tui._task_list.console")
    def test_up_wraps_to_prev_page(self, mock_console, mock_read_key, mock_deadline_client):
        mock_deadline_client.list_tasks.side_effect = [
            {
                "tasks": [{"taskId": "task-aaa", "runStatus": "SUCCEEDED", "parameters": {}}],
                "nextToken": "tok",
            },
            {
                "tasks": [{"taskId": "task-bbb", "runStatus": "SUCCEEDED", "parameters": {}}],
            },
            {
                "tasks": [{"taskId": "task-aaa", "runStatus": "SUCCEEDED", "parameters": {}}],
                "nextToken": "tok",
            },
        ]
        mock_read_key.side_effect = ["n", "up", "l"]
        tui = TaskListTUI(
            "farm-1", "queue-1", "job-1", "Test Job", "step-1", "Render", mock_deadline_client
        )
        result = tui.run()
        assert result == ("sessions", "task-aaa")
