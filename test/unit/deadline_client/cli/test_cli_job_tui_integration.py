# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""Tests for job_group TUI integration functions."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("rich", reason="TUI tests require the 'rich' package (deadline[tui])")

from deadline.client.cli._groups.job_group import (
    _tui_main_loop,
    _tui_setup_attachment_browser,
    _tui_step_loop,
    _tui_task_loop,
)


@pytest.fixture
def mock_deadline():
    client = MagicMock()
    client.get_job.return_value = {
        "name": "Test Job",
        "taskRunStatus": "SUCCEEDED",
    }
    client.get_queue.return_value = {
        "displayName": "Test Queue",
        "jobAttachmentSettings": {
            "s3BucketName": "bucket",
            "rootPrefix": "prefix",
        },
    }
    client.get_task.return_value = {
        "parameters": {"Frame": {"int": "1"}},
    }
    return client


class TestTuiMainLoop:
    @patch("deadline.client.cli._groups._job_tui._job_list.JobListTUI")
    def test_quit(self, MockJobList, mock_deadline):
        MockJobList.return_value.run.return_value = None
        _tui_main_loop("farm-1", "queue-1", mock_deadline, None)

    @patch("deadline.client.cli._groups.job_group._tui_step_loop", return_value=None)
    @patch("deadline.client.cli._groups._job_tui._job_list.JobListTUI")
    def test_select_job_then_back(self, MockJobList, mock_step_loop, mock_deadline):
        MockJobList.return_value.run.side_effect = [("select", "job-1"), None]
        _tui_main_loop("farm-1", "queue-1", mock_deadline, None)
        mock_step_loop.assert_called_once()

    @patch("deadline.client.cli._groups.job_group._tui_step_loop", return_value="quit")
    @patch("deadline.client.cli._groups._job_tui._job_list.JobListTUI")
    def test_select_job_then_quit(self, MockJobList, mock_step_loop, mock_deadline):
        MockJobList.return_value.run.side_effect = [("select", "job-1")]
        _tui_main_loop("farm-1", "queue-1", mock_deadline, None)

    @patch("deadline.client.cli._groups.job_group._tui_setup_attachment_browser")
    @patch("deadline.client.cli._groups._job_tui._job_list.JobListTUI")
    def test_attachments(self, MockJobList, mock_attach, mock_deadline):
        MockJobList.return_value.run.side_effect = [("attachments", "job-1"), None]
        _tui_main_loop("farm-1", "queue-1", mock_deadline, None)
        mock_attach.assert_called_once()


class TestTuiStepLoop:
    @patch("deadline.client.cli._groups._job_tui._step_list.StepListTUI")
    def test_quit(self, MockStepList, mock_deadline):
        MockStepList.return_value.run.return_value = None
        result = _tui_step_loop("farm-1", "queue-1", "job-1", mock_deadline, None)
        assert result == "quit"

    @patch("deadline.client.cli._groups._job_tui._step_list.StepListTUI")
    def test_back(self, MockStepList, mock_deadline):
        MockStepList.return_value.run.return_value = ("back", "")
        result = _tui_step_loop("farm-1", "queue-1", "job-1", mock_deadline, None)
        assert result is None

    @patch("deadline.client.cli._groups.job_group._tui_task_loop", return_value=None)
    @patch("deadline.client.cli._groups._job_tui._step_list.StepListTUI")
    def test_select_step_then_back(self, MockStepList, mock_task_loop, mock_deadline):
        MockStepList.return_value.run.side_effect = [
            ("select", "step-1", "Render"),
            ("back", ""),
        ]
        result = _tui_step_loop("farm-1", "queue-1", "job-1", mock_deadline, None)
        assert result is None

    @patch("deadline.client.cli._groups.job_group._tui_task_loop", return_value="quit")
    @patch("deadline.client.cli._groups._job_tui._step_list.StepListTUI")
    def test_select_step_then_quit(self, MockStepList, mock_task_loop, mock_deadline):
        MockStepList.return_value.run.side_effect = [("select", "step-1", "Render")]
        result = _tui_step_loop("farm-1", "queue-1", "job-1", mock_deadline, None)
        assert result == "quit"


class TestTuiTaskLoop:
    @patch("deadline.client.cli._groups._job_tui._task_list.TaskListTUI")
    def test_quit(self, MockTaskList, mock_deadline):
        MockTaskList.return_value.run.return_value = None
        result = _tui_task_loop(
            "farm-1", "queue-1", "job-1", "Test Job", "step-1", "Render", mock_deadline, None
        )
        assert result == "quit"

    @patch("deadline.client.cli._groups._job_tui._task_list.TaskListTUI")
    def test_back(self, MockTaskList, mock_deadline):
        MockTaskList.return_value.run.return_value = ("back", "")
        result = _tui_task_loop(
            "farm-1", "queue-1", "job-1", "Test Job", "step-1", "Render", mock_deadline, None
        )
        assert result is None

    @patch("deadline.client.cli._groups._job_tui._session_list.SessionListTUI")
    @patch("deadline.client.cli._groups._job_tui._task_list.TaskListTUI")
    def test_sessions(self, MockTaskList, MockSessionList, mock_deadline):
        MockTaskList.return_value.run.side_effect = [("sessions", "task-1"), ("back", "")]
        result = _tui_task_loop(
            "farm-1", "queue-1", "job-1", "Test Job", "step-1", "Render", mock_deadline, None
        )
        MockSessionList.return_value.run.assert_called_once()
        assert result is None

    @patch("deadline.client.cli._groups.job_group._tui_setup_attachment_browser")
    @patch("deadline.client.cli._groups._job_tui._task_list.TaskListTUI")
    def test_attachments(self, MockTaskList, mock_attach, mock_deadline):
        MockTaskList.return_value.run.side_effect = [("attachments", "task-1"), ("back", "")]
        _tui_task_loop(
            "farm-1", "queue-1", "job-1", "Test Job", "step-1", "Render", mock_deadline, None
        )
        mock_attach.assert_called_once()


class TestTuiSetupAttachmentBrowser:
    @patch("deadline.client.cli._groups._job_tui._attachment_browser.AttachmentBrowserTUI")
    @patch("deadline.client.cli._groups.job_group.api")
    def test_launches_browser(self, mock_api, MockBrowser, mock_deadline):
        _tui_setup_attachment_browser(None, "farm-1", "queue-1", "job-1", mock_deadline)
        MockBrowser.return_value.run.assert_called_once()

    @patch("deadline.client.cli._groups.job_group.click")
    @patch("deadline.client.cli._groups.job_group.api")
    def test_no_attachment_settings(self, mock_api, mock_click, mock_deadline):
        mock_deadline.get_queue.return_value = {"displayName": "Q"}
        _tui_setup_attachment_browser(None, "farm-1", "queue-1", "job-1", mock_deadline)
        mock_click.echo.assert_called_once()
