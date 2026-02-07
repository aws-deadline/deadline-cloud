# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the diagnostics API functions.
"""

import datetime
from unittest.mock import patch, MagicMock

import pytest

from deadline.client.api._mcp import (
    get_job,
    get_session,
    list_sessions,
    list_steps,
    list_tasks,
    search_jobs,
)

from ..shared_constants import (
    MOCK_FARM_ID,
    MOCK_JOB_ID,
    MOCK_QUEUE_ID,
    MOCK_STEP_ID,
)

MOCK_SESSION_ID = "session-0123456789abcdef"

MOCK_GET_JOB_RESPONSE = {
    "jobId": MOCK_JOB_ID,
    "name": "Test Job",
    "taskRunStatus": "FAILED",
    "taskRunStatusCounts": {"FAILED": 2, "SUCCEEDED": 10},
    "lifecycleStatus": "CREATE_COMPLETE",
    "createdAt": datetime.datetime(2023, 1, 27, 7, 34, 41),
    "startedAt": datetime.datetime(2023, 1, 27, 7, 37, 53),
    "endedAt": datetime.datetime(2023, 1, 27, 7, 39, 17),
}

MOCK_GET_SESSION_RESPONSE = {
    "sessionId": MOCK_SESSION_ID,
    "lifecycleStatus": "ENDED",
    "workerId": "worker-abc123",
    "log": {
        "logDriver": "awslogs",
        "options": {
            "logGroupName": f"/aws/deadline/{MOCK_FARM_ID}/{MOCK_QUEUE_ID}",
            "logStreamName": MOCK_SESSION_ID,
        },
    },
}

MOCK_LIST_SESSIONS_RESPONSE = {
    "sessions": [
        {
            "sessionId": "session-001",
            "lifecycleStatus": "ENDED",
            "workerId": "worker-abc",
        },
        {
            "sessionId": "session-002",
            "lifecycleStatus": "ENDED",
            "workerId": "worker-def",
        },
    ]
}

MOCK_LIST_STEPS_RESPONSE = {
    "steps": [
        {
            "stepId": "step-001",
            "name": "Render",
            "taskRunStatus": "FAILED",
            "taskRunStatusCounts": {"FAILED": 2, "SUCCEEDED": 8},
        },
        {
            "stepId": "step-002",
            "name": "Composite",
            "taskRunStatus": "SUCCEEDED",
            "taskRunStatusCounts": {"SUCCEEDED": 5},
        },
    ]
}

MOCK_LIST_TASKS_RESPONSE = {
    "tasks": [
        {
            "taskId": "task-001",
            "runStatus": "FAILED",
            "parameters": {"frame": {"int": "1"}},
            "latestSessionActionId": "sessionaction-abc-0",
        },
        {
            "taskId": "task-002",
            "runStatus": "SUCCEEDED",
            "parameters": {"frame": {"int": "2"}},
        },
    ]
}

MOCK_SEARCH_JOBS_RESPONSE = {
    "jobs": [
        {
            "jobId": "job-001",
            "name": "Failed Job 1",
            "taskRunStatus": "FAILED",
        },
        {
            "jobId": "job-002",
            "name": "Failed Job 2",
            "taskRunStatus": "FAILED",
        },
    ],
    "totalResults": 2,
    "nextItemOffset": None,
}


class TestGetJob:
    """Tests for get_job function."""

    def test_get_job_basic(self):
        """Test basic get_job call."""
        with patch("deadline.client.api._mcp.get_boto3_client") as mock_get_client:
            deadline_mock = MagicMock()
            deadline_mock.get_job.return_value = MOCK_GET_JOB_RESPONSE
            mock_get_client.return_value = deadline_mock

            result = get_job(
                farm_id=MOCK_FARM_ID,
                queue_id=MOCK_QUEUE_ID,
                job_id=MOCK_JOB_ID,
            )

            assert result["jobId"] == MOCK_JOB_ID
            assert result["name"] == "Test Job"
            assert result["taskRunStatus"] == "FAILED"
            assert result["taskRunStatusCounts"]["FAILED"] == 2

            deadline_mock.get_job.assert_called_once_with(
                farmId=MOCK_FARM_ID,
                queueId=MOCK_QUEUE_ID,
                jobId=MOCK_JOB_ID,
            )


class TestGetSession:
    """Tests for get_session function."""

    def test_get_session_basic(self):
        """Test basic get_session call."""
        with patch("deadline.client.api._mcp.get_boto3_client") as mock_get_client:
            deadline_mock = MagicMock()
            deadline_mock.get_session.return_value = MOCK_GET_SESSION_RESPONSE
            mock_get_client.return_value = deadline_mock

            result = get_session(
                farm_id=MOCK_FARM_ID,
                queue_id=MOCK_QUEUE_ID,
                job_id=MOCK_JOB_ID,
                session_id=MOCK_SESSION_ID,
            )

            assert result["sessionId"] == MOCK_SESSION_ID
            assert result["lifecycleStatus"] == "ENDED"
            assert result["workerId"] == "worker-abc123"

            deadline_mock.get_session.assert_called_once_with(
                farmId=MOCK_FARM_ID,
                queueId=MOCK_QUEUE_ID,
                jobId=MOCK_JOB_ID,
                sessionId=MOCK_SESSION_ID,
            )


class TestListSessions:
    """Tests for list_sessions function."""

    def test_list_sessions_basic(self):
        """Test basic list_sessions call."""
        with patch("deadline.client.api._mcp.get_boto3_client") as mock_get_client:
            deadline_mock = MagicMock()
            deadline_mock.list_sessions.return_value = MOCK_LIST_SESSIONS_RESPONSE
            mock_get_client.return_value = deadline_mock

            result = list_sessions(
                farm_id=MOCK_FARM_ID,
                queue_id=MOCK_QUEUE_ID,
                job_id=MOCK_JOB_ID,
            )

            assert len(result["sessions"]) == 2
            assert result["sessions"][0]["sessionId"] == "session-001"

            deadline_mock.list_sessions.assert_called_once_with(
                farmId=MOCK_FARM_ID,
                queueId=MOCK_QUEUE_ID,
                jobId=MOCK_JOB_ID,
            )

    def test_list_sessions_with_pagination(self):
        """Test list_sessions with pagination."""
        with patch("deadline.client.api._mcp.get_boto3_client") as mock_get_client:
            deadline_mock = MagicMock()

            # First page has nextToken, second page doesn't
            page1 = {
                "sessions": [{"sessionId": "session-001"}],
                "nextToken": "token-1",
            }
            page2 = {
                "sessions": [{"sessionId": "session-002"}],
            }
            deadline_mock.list_sessions.side_effect = [page1, page2]
            mock_get_client.return_value = deadline_mock

            result = list_sessions(
                farm_id=MOCK_FARM_ID,
                queue_id=MOCK_QUEUE_ID,
                job_id=MOCK_JOB_ID,
            )

            assert len(result["sessions"]) == 2
            assert result["sessions"][0]["sessionId"] == "session-001"
            assert result["sessions"][1]["sessionId"] == "session-002"

    def test_list_sessions_with_max_results(self):
        """Test list_sessions passes max_results to API."""
        with patch("deadline.client.api._mcp.get_boto3_client") as mock_get_client:
            deadline_mock = MagicMock()
            deadline_mock.list_sessions.return_value = {
                "sessions": [
                    {"sessionId": "session-001"},
                    {"sessionId": "session-002"},
                ]
            }
            mock_get_client.return_value = deadline_mock

            result = list_sessions(
                farm_id=MOCK_FARM_ID,
                queue_id=MOCK_QUEUE_ID,
                job_id=MOCK_JOB_ID,
                max_results=2,
            )

            assert len(result["sessions"]) == 2
            # Verify maxResults was passed to the API
            call_args = deadline_mock.list_sessions.call_args
            assert call_args.kwargs["maxResults"] == 2


class TestListSteps:
    """Tests for list_steps function."""

    def test_list_steps_basic(self):
        """Test basic list_steps call."""
        with patch("deadline.client.api._mcp.get_boto3_client") as mock_get_client:
            deadline_mock = MagicMock()
            deadline_mock.list_steps.return_value = MOCK_LIST_STEPS_RESPONSE
            mock_get_client.return_value = deadline_mock

            result = list_steps(
                farm_id=MOCK_FARM_ID,
                queue_id=MOCK_QUEUE_ID,
                job_id=MOCK_JOB_ID,
            )

            assert len(result["steps"]) == 2
            assert result["steps"][0]["name"] == "Render"
            assert result["steps"][0]["taskRunStatus"] == "FAILED"

            deadline_mock.list_steps.assert_called_once_with(
                farmId=MOCK_FARM_ID,
                queueId=MOCK_QUEUE_ID,
                jobId=MOCK_JOB_ID,
            )


class TestListTasks:
    """Tests for list_tasks function."""

    def test_list_tasks_basic(self):
        """Test basic list_tasks call."""
        with patch("deadline.client.api._mcp.get_boto3_client") as mock_get_client:
            deadline_mock = MagicMock()
            deadline_mock.list_tasks.return_value = MOCK_LIST_TASKS_RESPONSE
            mock_get_client.return_value = deadline_mock

            result = list_tasks(
                farm_id=MOCK_FARM_ID,
                queue_id=MOCK_QUEUE_ID,
                job_id=MOCK_JOB_ID,
                step_id=MOCK_STEP_ID,
            )

            assert len(result["tasks"]) == 2
            assert result["tasks"][0]["taskId"] == "task-001"
            assert result["tasks"][0]["runStatus"] == "FAILED"

            deadline_mock.list_tasks.assert_called_once_with(
                farmId=MOCK_FARM_ID,
                queueId=MOCK_QUEUE_ID,
                jobId=MOCK_JOB_ID,
                stepId=MOCK_STEP_ID,
            )


class TestSearchJobs:
    """Tests for search_jobs function."""

    def test_search_jobs_basic(self, fresh_deadline_config):
        """Test basic search_jobs call without filters."""
        with patch("deadline.client.api._mcp.get_boto3_client") as mock_get_client:
            deadline_mock = MagicMock()
            deadline_mock.search_jobs.return_value = MOCK_SEARCH_JOBS_RESPONSE
            mock_get_client.return_value = deadline_mock

            result = search_jobs(
                farm_id=MOCK_FARM_ID,
                queue_ids=[MOCK_QUEUE_ID],
            )

            assert len(result["jobs"]) == 2
            assert result["totalResults"] == 2

            deadline_mock.search_jobs.assert_called_once_with(
                farmId=MOCK_FARM_ID,
                queueIds=[MOCK_QUEUE_ID],
                pageSize=25,
                itemOffset=0,
            )

    def test_search_jobs_with_status_filter(self, fresh_deadline_config):
        """Test search_jobs with task_run_status filter."""
        with patch("deadline.client.api._mcp.get_boto3_client") as mock_get_client:
            deadline_mock = MagicMock()
            deadline_mock.search_jobs.return_value = MOCK_SEARCH_JOBS_RESPONSE
            mock_get_client.return_value = deadline_mock

            result = search_jobs(
                farm_id=MOCK_FARM_ID,
                queue_ids=[MOCK_QUEUE_ID],
                task_run_status="FAILED",
            )

            assert len(result["jobs"]) == 2

            # Verify filter was passed
            call_args = deadline_mock.search_jobs.call_args
            assert "filterExpressions" in call_args.kwargs
            filters = call_args.kwargs["filterExpressions"]["filters"]
            assert len(filters) == 1
            assert filters[0]["stringFilter"]["name"] == "TASK_RUN_STATUS"
            assert filters[0]["stringFilter"]["value"] == "FAILED"

    def test_search_jobs_with_name_filter(self, fresh_deadline_config):
        """Test search_jobs with name_contains filter."""
        with patch("deadline.client.api._mcp.get_boto3_client") as mock_get_client:
            deadline_mock = MagicMock()
            deadline_mock.search_jobs.return_value = MOCK_SEARCH_JOBS_RESPONSE
            mock_get_client.return_value = deadline_mock

            result = search_jobs(
                farm_id=MOCK_FARM_ID,
                queue_ids=[MOCK_QUEUE_ID],
                name_contains="Render",
            )

            assert len(result["jobs"]) == 2

            # Verify filter was passed
            call_args = deadline_mock.search_jobs.call_args
            assert "filterExpressions" in call_args.kwargs
            filters = call_args.kwargs["filterExpressions"]["filters"]
            assert len(filters) == 1
            assert filters[0]["searchTermFilter"]["searchTerm"] == "Render"

    def test_search_jobs_with_multiple_filters(self, fresh_deadline_config):
        """Test search_jobs with multiple filters."""
        with patch("deadline.client.api._mcp.get_boto3_client") as mock_get_client:
            deadline_mock = MagicMock()
            deadline_mock.search_jobs.return_value = MOCK_SEARCH_JOBS_RESPONSE
            mock_get_client.return_value = deadline_mock

            result = search_jobs(
                farm_id=MOCK_FARM_ID,
                queue_ids=[MOCK_QUEUE_ID],
                task_run_status="FAILED",
                name_contains="Render",
            )

            assert len(result["jobs"]) == 2

            # Verify both filters were passed
            call_args = deadline_mock.search_jobs.call_args
            assert "filterExpressions" in call_args.kwargs
            filters = call_args.kwargs["filterExpressions"]["filters"]
            assert len(filters) == 2
            assert call_args.kwargs["filterExpressions"]["operator"] == "AND"

    def test_search_jobs_pagination_params(self, fresh_deadline_config):
        """Test search_jobs with pagination parameters."""
        with patch("deadline.client.api._mcp.get_boto3_client") as mock_get_client:
            deadline_mock = MagicMock()
            deadline_mock.search_jobs.return_value = MOCK_SEARCH_JOBS_RESPONSE
            mock_get_client.return_value = deadline_mock

            search_jobs(
                farm_id=MOCK_FARM_ID,
                queue_ids=[MOCK_QUEUE_ID],
                page_size=50,
                item_offset=100,
            )

            call_args = deadline_mock.search_jobs.call_args
            assert call_args.kwargs["pageSize"] == 50
            assert call_args.kwargs["itemOffset"] == 100

    def test_search_jobs_clamps_page_size(self, fresh_deadline_config):
        """Test search_jobs clamps page_size to valid range."""
        with patch("deadline.client.api._mcp.get_boto3_client") as mock_get_client:
            deadline_mock = MagicMock()
            deadline_mock.search_jobs.return_value = MOCK_SEARCH_JOBS_RESPONSE
            mock_get_client.return_value = deadline_mock

            # Test page_size > 100 gets clamped
            search_jobs(
                farm_id=MOCK_FARM_ID,
                queue_ids=[MOCK_QUEUE_ID],
                page_size=200,
            )

            call_args = deadline_mock.search_jobs.call_args
            assert call_args.kwargs["pageSize"] == 100

            # Test page_size < 1 gets clamped
            search_jobs(
                farm_id=MOCK_FARM_ID,
                queue_ids=[MOCK_QUEUE_ID],
                page_size=0,
            )

            call_args = deadline_mock.search_jobs.call_args
            assert call_args.kwargs["pageSize"] == 1

    def test_search_jobs_uses_config_defaults(self, fresh_deadline_config):
        """Test search_jobs uses config defaults when farm_id/queue_ids not provided."""
        from deadline.client.config import config_file

        # Set defaults in config
        config_file.set_setting("defaults.farm_id", MOCK_FARM_ID)
        config_file.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

        with patch("deadline.client.api._mcp.get_boto3_client") as mock_get_client:
            deadline_mock = MagicMock()
            deadline_mock.search_jobs.return_value = MOCK_SEARCH_JOBS_RESPONSE
            mock_get_client.return_value = deadline_mock

            result = search_jobs()

            assert len(result["jobs"]) == 2
            deadline_mock.search_jobs.assert_called_once_with(
                farmId=MOCK_FARM_ID,
                queueIds=[MOCK_QUEUE_ID],
                pageSize=25,
                itemOffset=0,
            )

    def test_search_jobs_missing_farm_id_raises(self, fresh_deadline_config):
        """Test search_jobs raises error when farm_id is missing."""
        with pytest.raises(ValueError, match="farm_id is required"):
            search_jobs()

    def test_search_jobs_missing_queue_ids_raises(self, fresh_deadline_config):
        """Test search_jobs raises error when queue_ids is missing."""
        from deadline.client.config import config_file

        config_file.set_setting("defaults.farm_id", MOCK_FARM_ID)

        with pytest.raises(ValueError, match="queue_ids is required"):
            search_jobs()
