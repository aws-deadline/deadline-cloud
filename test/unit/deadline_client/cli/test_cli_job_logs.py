# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the CLI job logs command.
"""

import json
import datetime
from unittest.mock import ANY, MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from click.testing import CliRunner
from freezegun import freeze_time

from deadline.client import api
from deadline.client.cli import main
from deadline.client.cli._groups.job_group import _parse_session_action_id
from deadline.client.config import config_file as config
from deadline.client.api._job_monitoring import SessionLogResult, LogEvent
from deadline.client.exceptions import DeadlineOperationError

from ..shared_constants import (
    MOCK_FARM_ID,
    MOCK_QUEUE_ID,
)

# Mock constants for tests that don't use shared constants
MOCK_JOB_ID = "job-0123456789abcdefabcdefabcdefabcd"

# Sample log events for testing
SAMPLE_LOG_EVENTS = [
    LogEvent(
        timestamp=datetime.datetime(2023, 1, 1, 12, 0, 0, 123456, tzinfo=datetime.timezone.utc),
        message="Log message 1",
        ingestion_time=datetime.datetime(
            2023, 1, 1, 12, 0, 10, 654321, tzinfo=datetime.timezone.utc
        ),
        event_id="event-1",
    ),
    LogEvent(
        timestamp=datetime.datetime(2023, 1, 1, 12, 1, 0, 789012, tzinfo=datetime.timezone.utc),
        message="Log message 2",
        ingestion_time=datetime.datetime(
            2023, 1, 1, 12, 1, 10, 345678, tzinfo=datetime.timezone.utc
        ),
        event_id="event-2",
    ),
]

# Sample log result for testing
SAMPLE_LOG_RESULT = SessionLogResult(
    events=SAMPLE_LOG_EVENTS,
    next_token="next-token",
    log_group=f"/aws/deadline/{MOCK_FARM_ID}/{MOCK_QUEUE_ID}",
    log_stream="session-test-session",
    count=2,
)

# Sample empty log result for testing
EMPTY_LOG_RESULT = SessionLogResult(
    events=[],
    next_token=None,
    log_group=f"/aws/deadline/{MOCK_FARM_ID}/{MOCK_QUEUE_ID}",
    log_stream="session-test-session",
    count=0,
)


def test_cli_job_logs_verbose(fresh_deadline_config):
    """
    Test that logs CLI works correctly in verbose mode.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    with patch("deadline.client.api.get_session_logs") as mock_get_logs, patch(
        "deadline.client.api._job_monitoring.get_user_and_identity_store_id"
    ) as mock_get_user, patch("deadline.client.api.get_boto3_client") as boto3_client_mock:
        mock_get_user.return_value = (None, None)
        mock_get_logs.return_value = SAMPLE_LOG_RESULT

        # Mock get_job to return job name
        boto3_client_mock().get_job.return_value = {"name": "Test Job Name"}

        runner = CliRunner()
        result = runner.invoke(
            main, ["job", "logs", "--session-id", "test-session", "--limit", "100"]
        )

        assert "Retrieving logs for session" in result.output
        assert "Job ID: " + MOCK_JOB_ID in result.output
        assert "Job Name: Test Job Name" in result.output
        assert "[2023-01-01T12:00:00.123456+00:00] Log message 1" in result.output
        assert "[2023-01-01T12:01:00.789012+00:00] Log message 2" in result.output
        assert "Retrieved 2 log events" in result.output
        assert "More logs are available" in result.output
        assert result.exit_code == 0

        # Verify the API was called with correct parameters
        mock_get_logs.assert_called_once()
        _, kwargs = mock_get_logs.call_args
        assert kwargs["farm_id"] == MOCK_FARM_ID
        assert kwargs["queue_id"] == MOCK_QUEUE_ID
        assert kwargs["session_id"] == "test-session"
        assert kwargs["limit"] == 100


def test_cli_job_logs_json(fresh_deadline_config):
    """
    Test that logs CLI works correctly in JSON mode.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    with patch("deadline.client.api.get_session_logs") as mock_get_logs, patch(
        "deadline.client.api._job_monitoring.get_user_and_identity_store_id"
    ) as mock_get_user, patch("deadline.client.api.get_boto3_client") as boto3_client_mock:
        mock_get_user.return_value = (None, None)
        mock_get_logs.return_value = SAMPLE_LOG_RESULT

        # Mock get_job to return job name
        boto3_client_mock().get_job.return_value = {"name": "Test Job Name"}

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "logs",
                "--session-id",
                "test-session",
                "--limit",
                "100",
                "--output",
                "json",
            ],
        )

        # Verify the output is valid JSON
        output_json = json.loads(result.output)
        assert "events" in output_json
        assert len(output_json["events"]) == 2
        assert output_json["events"][0]["message"] == "Log message 1"
        assert output_json["events"][0]["timestamp"] == "2023-01-01T12:00:00.123456+00:00"
        assert output_json["events"][0]["ingestionTime"] == "2023-01-01T12:00:10.654321+00:00"
        assert output_json["events"][1]["message"] == "Log message 2"
        assert output_json["events"][1]["timestamp"] == "2023-01-01T12:01:00.789012+00:00"
        assert output_json["events"][1]["ingestionTime"] == "2023-01-01T12:01:10.345678+00:00"
        assert output_json["count"] == 2
        assert output_json["nextToken"] == "next-token"
        assert output_json["logGroup"] == f"/aws/deadline/{MOCK_FARM_ID}/{MOCK_QUEUE_ID}"
        assert output_json["logStream"] == "session-test-session"

        # Verify job information is included
        assert output_json["jobId"] == MOCK_JOB_ID
        assert output_json["jobName"] == "Test Job Name"

        # Verify no intermediate text output was produced
        assert "Retrieving logs for session" not in result.output

        assert result.exit_code == 0


def test_cli_job_logs_empty(fresh_deadline_config):
    """
    Test that logs CLI handles empty results correctly.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    with patch("deadline.client.api.get_session_logs") as mock_get_logs, patch(
        "deadline.client.api._job_monitoring.get_user_and_identity_store_id"
    ) as mock_get_user, patch("deadline.client.api.get_boto3_client") as boto3_client_mock:
        mock_get_user.return_value = (None, None)
        mock_get_logs.return_value = EMPTY_LOG_RESULT

        # Mock get_job to return job name
        boto3_client_mock().get_job.return_value = {"name": "Test Job Name"}

        runner = CliRunner()
        result = runner.invoke(
            main, ["job", "logs", "--session-id", "test-session", "--limit", "100"]
        )

        assert "No logs found for the specified session" in result.output
        assert result.exit_code == 0


def test_cli_job_logs_json_empty(fresh_deadline_config):
    """
    Test that logs CLI handles empty results correctly in JSON mode.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    with patch("deadline.client.api.get_session_logs") as mock_get_logs, patch(
        "deadline.client.api._job_monitoring.get_user_and_identity_store_id"
    ) as mock_get_user, patch("deadline.client.api.get_boto3_client") as boto3_client_mock:
        mock_get_user.return_value = (None, None)
        mock_get_logs.return_value = EMPTY_LOG_RESULT

        # Mock get_job to return job name
        boto3_client_mock().get_job.return_value = {"name": "Test Job Name"}

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "logs",
                "--session-id",
                "test-session",
                "--limit",
                "100",
                "--output",
                "json",
            ],
        )

        # Verify the output is valid JSON
        output_json = json.loads(result.output)
        assert "events" in output_json
        assert len(output_json["events"]) == 0
        assert output_json["count"] == 0
        assert output_json["nextToken"] is None

        # Verify job information is included
        assert output_json["jobId"] == MOCK_JOB_ID
        assert output_json["jobName"] == "Test Job Name"

        assert result.exit_code == 0


def test_cli_job_logs_json_error(fresh_deadline_config):
    """
    Test that logs CLI handles errors correctly in JSON mode.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

    with patch("deadline.client.api.get_session_logs") as mock_get_logs, patch(
        "deadline.client.api._job_monitoring.get_user_and_identity_store_id"
    ) as mock_get_user, patch("deadline.client.api.get_boto3_client") as boto3_client_mock:
        mock_get_user.return_value = (None, None)
        mock_get_logs.side_effect = Exception("Test error message")

        # Mock boto3 client to prevent AWS SDK calls
        boto3_client_mock.return_value = MagicMock()

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["job", "logs", "--session-id", "test-session", "--output", "json"],
        )

        # Verify the output contains an error message
        assert "error" in result.output
        # The actual error message is different in the test environment

        # Exit code should be non-zero for errors
        assert result.exit_code != 0


def test_cli_job_logs_with_time_params(fresh_deadline_config):
    """
    Test that logs CLI handles time parameters correctly.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    with patch("deadline.client.api.get_session_logs") as mock_get_logs, patch(
        "deadline.client.api._job_monitoring.get_user_and_identity_store_id"
    ) as mock_get_user, patch("deadline.client.api.get_boto3_client") as boto3_client_mock:
        mock_get_user.return_value = (None, None)
        mock_get_logs.return_value = SAMPLE_LOG_RESULT

        # Mock get_job to return job name
        boto3_client_mock().get_job.return_value = {"name": "Test Job Name"}

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "logs",
                "--session-id",
                "test-session",
                "--start-time",
                "2023-01-01T12:00:00Z",
                "--end-time",
                "2023-01-01T13:00:00Z",
            ],
        )

        assert result.exit_code == 0

        # Verify the API was called with correct parameters
        mock_get_logs.assert_called_once()
        _, kwargs = mock_get_logs.call_args
        assert kwargs["start_time"] == "2023-01-01T12:00:00Z"
        assert kwargs["end_time"] == "2023-01-01T13:00:00Z"


def test_cli_job_logs_with_next_token(fresh_deadline_config):
    """
    Test that logs CLI handles next_token parameter correctly.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    with patch("deadline.client.api.get_session_logs") as mock_get_logs, patch(
        "deadline.client.api._job_monitoring.get_user_and_identity_store_id"
    ) as mock_get_user, patch("deadline.client.api.get_boto3_client") as boto3_client_mock:
        mock_get_user.return_value = (None, None)
        mock_get_logs.return_value = SAMPLE_LOG_RESULT

        # Mock get_job to return job name
        boto3_client_mock().get_job.return_value = {"name": "Test Job Name"}

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "logs",
                "--session-id",
                "test-session",
                "--next-token",
                "test-token",
            ],
        )

        assert result.exit_code == 0

        # Verify the API was called with correct parameters
        mock_get_logs.assert_called_once()
        _, kwargs = mock_get_logs.call_args
        assert kwargs["next_token"] == "test-token"


def test_cli_job_logs_with_session_id(fresh_deadline_config):
    """
    Test job logs command with explicit session ID.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    with patch.object(api, "get_session_logs") as mock_get_logs, patch.object(
        api, "get_boto3_client"
    ) as boto3_client_mock:
        # Mock the API response
        mock_get_logs.return_value = SessionLogResult(
            events=[
                LogEvent(
                    timestamp=datetime.datetime(
                        2023, 1, 27, 7, 24, 45, tzinfo=datetime.timezone.utc
                    ),
                    message="Test log message 1",
                    ingestion_time=datetime.datetime(
                        2023, 1, 27, 7, 24, 46, tzinfo=datetime.timezone.utc
                    ),
                    event_id="event-1",
                ),
                LogEvent(
                    timestamp=datetime.datetime(
                        2023, 1, 27, 7, 24, 50, tzinfo=datetime.timezone.utc
                    ),
                    message="Test log message 2",
                    ingestion_time=datetime.datetime(
                        2023, 1, 27, 7, 24, 51, tzinfo=datetime.timezone.utc
                    ),
                    event_id="event-2",
                ),
            ],
            count=2,
            next_token=None,
            log_group=f"/aws/deadline/{MOCK_FARM_ID}/{MOCK_QUEUE_ID}",
            log_stream="session-1",
        )

        # Mock get_job to return job name
        boto3_client_mock().get_job.return_value = {"name": "Test Job Name"}

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "logs",
                "--session-id",
                "session-1",
            ],
        )

        # Verify the API was called correctly
        mock_get_logs.assert_called_once_with(
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            session_id="session-1",
            limit=100,
            start_time=None,
            end_time=None,
            next_token=None,
            config=ANY,
        )

        # Check output
        assert "Retrieving logs for session session-1" in result.output
        assert "Job ID: " + MOCK_JOB_ID in result.output
        assert "Job Name: Test Job Name" in result.output
        assert "2023-01-27T07:24:45+00:00" in result.output
        assert "Test log message 1" in result.output
        assert "2023-01-27T07:24:50+00:00" in result.output
        assert "Test log message 2" in result.output
        assert "Retrieved 2 log events" in result.output
        assert result.exit_code == 0


def test_cli_job_logs_with_job_id_single_session(fresh_deadline_config):
    """
    Test job logs command with job ID when there's only one session.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    with patch.object(api, "get_boto3_client") as boto3_client_mock, patch.object(
        api, "get_session_logs"
    ) as mock_get_logs:
        # Mock the paginator
        paginator_mock = MagicMock()
        boto3_client_mock().get_paginator.return_value = paginator_mock

        # Set up the paginator to return a single session
        paginator_mock.paginate.return_value = [{"sessions": [{"sessionId": "session-1"}]}]

        # Mock get_job to return job name
        boto3_client_mock().get_job.return_value = {"name": "Test Job Name"}

        # Mock the get_session_logs response
        mock_get_logs.return_value = api.SessionLogResult(
            events=[
                api.LogEvent(
                    timestamp=datetime.datetime(2023, 1, 27, 7, 24, 45),
                    message="Test log message",
                    ingestion_time=datetime.datetime(2023, 1, 27, 7, 24, 46),
                    event_id="event-1",
                ),
            ],
            count=1,
            next_token=None,
            log_group=f"/aws/deadline/{MOCK_FARM_ID}/{MOCK_QUEUE_ID}",
            log_stream="session-1",
        )

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "logs",
                "--job-id",
                MOCK_JOB_ID,
            ],
        )

        # Verify paginator was called correctly
        boto3_client_mock().get_paginator.assert_called_once_with("list_sessions")
        paginator_mock.paginate.assert_called_once_with(
            farmId=MOCK_FARM_ID, queueId=MOCK_QUEUE_ID, jobId=MOCK_JOB_ID
        )

        # Verify get_job was called to get job name
        boto3_client_mock().get_job.assert_called_once_with(
            farmId=MOCK_FARM_ID, queueId=MOCK_QUEUE_ID, jobId=MOCK_JOB_ID
        )

        # Verify get_session_logs was called with the correct session ID
        mock_get_logs.assert_called_once_with(
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            session_id="session-1",
            limit=100,
            start_time=None,
            end_time=None,
            next_token=None,
            config=ANY,
        )

        # Check output includes job information
        assert "Using the only available session: session-1" in result.output
        assert "Job ID: " + MOCK_JOB_ID in result.output
        assert "Job Name: Test Job Name" in result.output
        assert "Test log message" in result.output
        assert result.exit_code == 0


def test_cli_job_logs_with_job_id_no_sessions(fresh_deadline_config):
    """
    Test job logs command with job ID when there are no sessions.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

    with patch.object(api, "get_boto3_client") as boto3_client_mock:
        # Mock the paginator
        paginator_mock = MagicMock()
        boto3_client_mock().get_paginator.return_value = paginator_mock

        # Set up the paginator to return no sessions
        paginator_mock.paginate.return_value = [{"sessions": []}]

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "logs",
                "--job-id",
                MOCK_JOB_ID,
            ],
        )

        # Verify paginator was called correctly
        boto3_client_mock().get_paginator.assert_called_once_with("list_sessions")
        paginator_mock.paginate.assert_called_once_with(
            farmId=MOCK_FARM_ID, queueId=MOCK_QUEUE_ID, jobId=MOCK_JOB_ID
        )

        # Check error message
        assert f"No sessions found for job {MOCK_JOB_ID}" in result.output
        assert result.exit_code != 0


def test_cli_job_logs_with_pagination(fresh_deadline_config):
    """
    Test job logs command with pagination of sessions.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

    with patch.object(api, "get_boto3_client") as boto3_client_mock, patch.object(
        api, "get_session_logs"
    ) as mock_get_logs:
        # Mock the paginator
        paginator_mock = MagicMock()
        boto3_client_mock().get_paginator.return_value = paginator_mock

        # Set up the paginator to return sessions across multiple pages
        paginator_mock.paginate.return_value = [
            {
                "sessions": [
                    {
                        "sessionId": "session-1",
                        "endedAt": datetime.datetime(2023, 1, 27, 7, 0, 0),
                    }
                ]
            },
            {
                "sessions": [
                    {
                        "sessionId": "session-2",
                        "endedAt": datetime.datetime(2023, 1, 27, 8, 0, 0),
                    }
                ]
            },
        ]

        # Mock the get_session_logs response
        mock_get_logs.return_value = api.SessionLogResult(
            events=[
                api.LogEvent(
                    timestamp=datetime.datetime(2023, 1, 27, 8, 0, 0),
                    message="Test log message",
                    ingestion_time=datetime.datetime(2023, 1, 27, 8, 0, 1),
                    event_id="event-1",
                ),
            ],
            count=1,
            next_token=None,
            log_group=f"/aws/deadline/{MOCK_FARM_ID}/{MOCK_QUEUE_ID}",
            log_stream="session-2",
        )

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "logs",
                "--job-id",
                MOCK_JOB_ID,
            ],
        )

        # Verify paginator was called correctly
        boto3_client_mock().get_paginator.assert_called_once_with("list_sessions")
        paginator_mock.paginate.assert_called_once_with(
            farmId=MOCK_FARM_ID, queueId=MOCK_QUEUE_ID, jobId=MOCK_JOB_ID
        )

        # Verify get_session_logs was called with the latest session ID
        mock_get_logs.assert_called_once_with(
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            session_id="session-2",  # Should use the latest session
            limit=100,
            start_time=None,
            end_time=None,
            next_token=None,
            config=ANY,
        )

        # Check output
        assert "Using the latest session: session-2" in result.output
        assert "Test log message" in result.output
        assert result.exit_code == 0


def test_cli_job_logs_with_job_id_prioritizes_ongoing_sessions(fresh_deadline_config):
    """
    Test that ongoing sessions (no endedAt) are prioritized over completed sessions.
    """
    with patch.object(config, "get_setting") as mock_get_setting:
        mock_get_setting.side_effect = [
            MOCK_FARM_ID,  # _apply_cli_options_to_config check for farm_id
            MOCK_QUEUE_ID,  # _apply_cli_options_to_config check for queue_id
            MOCK_FARM_ID,  # defaults.farm_id
            MOCK_QUEUE_ID,  # defaults.queue_id
            MOCK_JOB_ID,  # defaults.job_id
        ]

        with patch("deadline.client.api.get_boto3_client") as boto3_client_mock:
            with patch("deadline.client.api.get_session_logs") as mock_get_logs:
                # Set up the paginator to return mix of ongoing and completed sessions
                paginator_mock = MagicMock()
                boto3_client_mock().get_paginator.return_value = paginator_mock
                paginator_mock.paginate.return_value = [
                    {
                        "sessions": [
                            {
                                "sessionId": "session-completed-recent",
                                "startedAt": datetime.datetime(2023, 1, 27, 8, 0, 0),
                                "endedAt": datetime.datetime(
                                    2023, 1, 27, 9, 0, 0
                                ),  # Most recent completion
                            },
                            {
                                "sessionId": "session-ongoing-older",
                                "startedAt": datetime.datetime(
                                    2023, 1, 27, 6, 0, 0
                                ),  # Ongoing but older
                                # No endedAt - this is ongoing
                            },
                            {
                                "sessionId": "session-ongoing-newer",
                                "startedAt": datetime.datetime(
                                    2023, 1, 27, 7, 30, 0
                                ),  # Ongoing and newer
                                # No endedAt - this is ongoing
                            },
                            {
                                "sessionId": "session-completed-older",
                                "startedAt": datetime.datetime(2023, 1, 27, 5, 0, 0),
                                "endedAt": datetime.datetime(
                                    2023, 1, 27, 6, 0, 0
                                ),  # Older completion
                            },
                        ]
                    }
                ]

                # Mock the get_session_logs response
                mock_get_logs.return_value = api.SessionLogResult(
                    events=[
                        api.LogEvent(
                            timestamp=datetime.datetime(2023, 1, 27, 7, 30, 0),
                            message="Ongoing session log message",
                            ingestion_time=datetime.datetime(2023, 1, 27, 7, 30, 1),
                            event_id="event-1",
                        ),
                    ],
                    count=1,
                    next_token=None,
                    log_group=f"/aws/deadline/{MOCK_FARM_ID}/{MOCK_QUEUE_ID}",
                    log_stream="session-ongoing-newer",
                )

                runner = CliRunner()
                result = runner.invoke(
                    main,
                    [
                        "job",
                        "logs",
                        "--job-id",
                        MOCK_JOB_ID,
                    ],
                )

                # Verify get_session_logs was called with the most recently started ongoing session
                mock_get_logs.assert_called_once_with(
                    farm_id=MOCK_FARM_ID,
                    queue_id=MOCK_QUEUE_ID,
                    session_id="session-ongoing-newer",  # Should prioritize ongoing session with most recent start
                    limit=100,
                    start_time=None,
                    end_time=None,
                    next_token=None,
                    config=ANY,
                )

                # Check output
                assert "Using the latest session: session-ongoing-newer" in result.output
                assert "Ongoing session log message" in result.output
                assert result.exit_code == 0


def test_cli_job_logs_with_job_id_selects_most_recent_completed_when_no_ongoing(
    fresh_deadline_config,
):
    """
    Test that when there are no ongoing sessions, the most recently completed session is selected.
    """
    with patch.object(config, "get_setting") as mock_get_setting:
        mock_get_setting.side_effect = [
            MOCK_FARM_ID,  # _apply_cli_options_to_config check for farm_id
            MOCK_QUEUE_ID,  # _apply_cli_options_to_config check for queue_id
            MOCK_FARM_ID,  # defaults.farm_id
            MOCK_QUEUE_ID,  # defaults.queue_id
            MOCK_JOB_ID,  # defaults.job_id
        ]

        with patch("deadline.client.api.get_boto3_client") as boto3_client_mock:
            with patch("deadline.client.api.get_session_logs") as mock_get_logs:
                # Set up the paginator to return only completed sessions
                paginator_mock = MagicMock()
                boto3_client_mock().get_paginator.return_value = paginator_mock
                paginator_mock.paginate.return_value = [
                    {
                        "sessions": [
                            {
                                "sessionId": "session-completed-older",
                                "startedAt": datetime.datetime(2023, 1, 27, 6, 0, 0),
                                "endedAt": datetime.datetime(
                                    2023, 1, 27, 7, 0, 0
                                ),  # Older completion
                            },
                            {
                                "sessionId": "session-completed-newer",
                                "startedAt": datetime.datetime(2023, 1, 27, 8, 0, 0),
                                "endedAt": datetime.datetime(
                                    2023, 1, 27, 9, 0, 0
                                ),  # Most recent completion
                            },
                        ]
                    }
                ]

                # Mock the get_session_logs response
                mock_get_logs.return_value = api.SessionLogResult(
                    events=[
                        api.LogEvent(
                            timestamp=datetime.datetime(2023, 1, 27, 9, 0, 0),
                            message="Most recent completed session log",
                            ingestion_time=datetime.datetime(2023, 1, 27, 9, 0, 1),
                            event_id="event-1",
                        ),
                    ],
                    count=1,
                    next_token=None,
                    log_group=f"/aws/deadline/{MOCK_FARM_ID}/{MOCK_QUEUE_ID}",
                    log_stream="session-completed-newer",
                )

                runner = CliRunner()
                result = runner.invoke(
                    main,
                    [
                        "job",
                        "logs",
                        "--job-id",
                        MOCK_JOB_ID,
                    ],
                )

                # Verify get_session_logs was called with the most recently completed session
                mock_get_logs.assert_called_once_with(
                    farm_id=MOCK_FARM_ID,
                    queue_id=MOCK_QUEUE_ID,
                    session_id="session-completed-newer",  # Should select most recently ended session
                    limit=100,
                    start_time=None,
                    end_time=None,
                    next_token=None,
                    config=ANY,
                )

                # Check output
                assert "Using the latest session: session-completed-newer" in result.output
                assert "Most recent completed session log" in result.output
                assert result.exit_code == 0


def test_cli_job_logs_with_job_id_selects_most_recent_among_multiple_ongoing(fresh_deadline_config):
    """
    Test that when there are multiple ongoing sessions, the most recently started one is selected.
    """
    with patch.object(config, "get_setting") as mock_get_setting:
        mock_get_setting.side_effect = [
            MOCK_FARM_ID,  # _apply_cli_options_to_config check for farm_id
            MOCK_QUEUE_ID,  # _apply_cli_options_to_config check for queue_id
            MOCK_FARM_ID,  # defaults.farm_id
            MOCK_QUEUE_ID,  # defaults.queue_id
            MOCK_JOB_ID,  # defaults.job_id
        ]

        with patch("deadline.client.api.get_boto3_client") as boto3_client_mock:
            with patch("deadline.client.api.get_session_logs") as mock_get_logs:
                # Set up the paginator to return multiple ongoing sessions
                paginator_mock = MagicMock()
                boto3_client_mock().get_paginator.return_value = paginator_mock
                paginator_mock.paginate.return_value = [
                    {
                        "sessions": [
                            {
                                "sessionId": "session-ongoing-oldest",
                                "startedAt": datetime.datetime(
                                    2023, 1, 27, 5, 0, 0
                                ),  # Oldest ongoing
                                # No endedAt - this is ongoing
                            },
                            {
                                "sessionId": "session-ongoing-middle",
                                "startedAt": datetime.datetime(
                                    2023, 1, 27, 6, 0, 0
                                ),  # Middle ongoing
                                # No endedAt - this is ongoing
                            },
                            {
                                "sessionId": "session-ongoing-newest",
                                "startedAt": datetime.datetime(
                                    2023, 1, 27, 7, 0, 0
                                ),  # Most recent ongoing
                                # No endedAt - this is ongoing
                            },
                        ]
                    }
                ]

                # Mock get_job to return job name
                boto3_client_mock().get_job.return_value = {"name": "Test Job Name"}

                # Mock the get_session_logs response
                mock_get_logs.return_value = api.SessionLogResult(
                    events=[
                        api.LogEvent(
                            timestamp=datetime.datetime(2023, 1, 27, 7, 0, 0),
                            message="Most recent ongoing session log",
                            ingestion_time=datetime.datetime(2023, 1, 27, 7, 0, 1),
                            event_id="event-1",
                        ),
                    ],
                    count=1,
                    next_token=None,
                    log_group=f"/aws/deadline/{MOCK_FARM_ID}/{MOCK_QUEUE_ID}",
                    log_stream="session-ongoing-newest",
                )

                runner = CliRunner()
                result = runner.invoke(
                    main,
                    [
                        "job",
                        "logs",
                        "--job-id",
                        MOCK_JOB_ID,
                    ],
                )

                # Verify get_job was called to get job name
                boto3_client_mock().get_job.assert_called_once_with(
                    farmId=MOCK_FARM_ID, queueId=MOCK_QUEUE_ID, jobId=MOCK_JOB_ID
                )

                # Verify get_session_logs was called with the most recently started ongoing session
                mock_get_logs.assert_called_once_with(
                    farm_id=MOCK_FARM_ID,
                    queue_id=MOCK_QUEUE_ID,
                    session_id="session-ongoing-newest",  # Should select most recently started ongoing session
                    limit=100,
                    start_time=None,
                    end_time=None,
                    next_token=None,
                    config=ANY,
                )

                # Check output includes job information
                assert "Using the latest session: session-ongoing-newest" in result.output
                assert "Job ID: " + MOCK_JOB_ID in result.output
                assert "Job Name: Test Job Name" in result.output
                assert "Most recent ongoing session log" in result.output
                assert result.exit_code == 0


def test_cli_job_logs_json_with_job_info(fresh_deadline_config):
    """
    Test that logs CLI includes job information in JSON mode when job-id is provided.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    with patch.object(api, "get_boto3_client") as boto3_client_mock, patch.object(
        api, "get_session_logs"
    ) as mock_get_logs:
        # Mock the paginator
        paginator_mock = MagicMock()
        boto3_client_mock().get_paginator.return_value = paginator_mock

        # Set up the paginator to return a single session
        paginator_mock.paginate.return_value = [{"sessions": [{"sessionId": "session-1"}]}]

        # Mock get_job to return job name
        boto3_client_mock().get_job.return_value = {"name": "Test Job Name"}

        # Mock the get_session_logs response
        mock_get_logs.return_value = SAMPLE_LOG_RESULT

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "logs",
                "--job-id",
                MOCK_JOB_ID,
                "--output",
                "json",
            ],
        )

        # Verify the output is valid JSON
        output_json = json.loads(result.output)
        assert "events" in output_json
        assert len(output_json["events"]) == 2
        assert output_json["events"][0]["message"] == "Log message 1"
        assert output_json["events"][1]["message"] == "Log message 2"
        assert output_json["count"] == 2
        assert output_json["nextToken"] == "next-token"
        assert output_json["logGroup"] == f"/aws/deadline/{MOCK_FARM_ID}/{MOCK_QUEUE_ID}"
        assert output_json["logStream"] == "session-test-session"

        # Verify job information is included
        assert output_json["jobId"] == MOCK_JOB_ID
        assert output_json["jobName"] == "Test Job Name"

        # Verify no intermediate text output was produced
        assert "Retrieving logs for session" not in result.output
        assert "Using the only available session" not in result.output

        assert result.exit_code == 0


def test_cli_job_logs_timezone_utc(fresh_deadline_config):
    """
    Test that logs CLI works correctly with UTC timezone (default).
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    with patch("deadline.client.api.get_session_logs") as mock_get_logs, patch(
        "deadline.client.api._job_monitoring.get_user_and_identity_store_id"
    ) as mock_get_user, patch("deadline.client.api.get_boto3_client") as boto3_client_mock:
        mock_get_user.return_value = (None, None)
        mock_get_logs.return_value = SAMPLE_LOG_RESULT

        # Mock get_job to return job name
        boto3_client_mock().get_job.return_value = {"name": "Test Job Name"}

        runner = CliRunner()
        result = runner.invoke(
            main, ["job", "logs", "--session-id", "test-session", "--timezone", "utc"]
        )

        # Verify UTC timestamps are displayed
        assert "[2023-01-01T12:00:00.123456+00:00] Log message 1" in result.output
        assert "[2023-01-01T12:01:00.789012+00:00] Log message 2" in result.output
        assert result.exit_code == 0


def test_cli_job_logs_timezone_local_verbose(fresh_deadline_config):
    """
    Test that logs CLI works correctly with local timezone in verbose mode.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    # Create timezone-aware datetime objects for testing
    import datetime

    utc_time1 = datetime.datetime(2023, 1, 1, 12, 0, 0, 123456, tzinfo=datetime.timezone.utc)
    utc_time2 = datetime.datetime(2023, 1, 1, 12, 1, 0, 789012, tzinfo=datetime.timezone.utc)

    timezone_aware_events = [
        LogEvent(
            timestamp=utc_time1,
            message="Log message 1",
            ingestion_time=datetime.datetime(
                2023, 1, 1, 12, 0, 10, 654321, tzinfo=datetime.timezone.utc
            ),
            event_id="event-1",
        ),
        LogEvent(
            timestamp=utc_time2,
            message="Log message 2",
            ingestion_time=datetime.datetime(
                2023, 1, 1, 12, 1, 10, 345678, tzinfo=datetime.timezone.utc
            ),
            event_id="event-2",
        ),
    ]

    timezone_aware_result = SessionLogResult(
        events=timezone_aware_events,
        next_token="next-token",
        log_group=f"/aws/deadline/{MOCK_FARM_ID}/{MOCK_QUEUE_ID}",
        log_stream="session-test-session",
        count=2,
    )

    with patch("deadline.client.api.get_session_logs") as mock_get_logs, patch(
        "deadline.client.api._job_monitoring.get_user_and_identity_store_id"
    ) as mock_get_user, patch("deadline.client.api.get_boto3_client") as boto3_client_mock:
        mock_get_user.return_value = (None, None)
        mock_get_logs.return_value = timezone_aware_result

        # Mock get_job to return job name
        boto3_client_mock().get_job.return_value = {"name": "Test Job Name"}

        runner = CliRunner()
        result = runner.invoke(
            main, ["job", "logs", "--session-id", "test-session", "--timezone", "local"]
        )

        # The exact local time will depend on the system timezone, but we can verify
        # that the format is ISO 8601 and that the command succeeds
        assert "Log message 1" in result.output
        assert "Log message 2" in result.output
        # Check that timestamps contain 'T' (ISO format) and timezone offset
        assert "T" in result.output
        assert "+" in result.output or "-" in result.output  # timezone offset
        assert result.exit_code == 0


def test_cli_job_logs_timezone_local_json(fresh_deadline_config):
    """
    Test that logs CLI works correctly with local timezone in JSON mode.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    # Create timezone-aware datetime objects for testing
    import datetime

    utc_time1 = datetime.datetime(2023, 1, 1, 12, 0, 0, 123456, tzinfo=datetime.timezone.utc)
    utc_time2 = datetime.datetime(2023, 1, 1, 12, 1, 0, 789012, tzinfo=datetime.timezone.utc)

    timezone_aware_events = [
        LogEvent(
            timestamp=utc_time1,
            message="Log message 1",
            ingestion_time=datetime.datetime(
                2023, 1, 1, 12, 0, 10, 654321, tzinfo=datetime.timezone.utc
            ),
            event_id="event-1",
        ),
        LogEvent(
            timestamp=utc_time2,
            message="Log message 2",
            ingestion_time=datetime.datetime(
                2023, 1, 1, 12, 1, 10, 345678, tzinfo=datetime.timezone.utc
            ),
            event_id="event-2",
        ),
    ]

    timezone_aware_result = SessionLogResult(
        events=timezone_aware_events,
        next_token="next-token",
        log_group=f"/aws/deadline/{MOCK_FARM_ID}/{MOCK_QUEUE_ID}",
        log_stream="session-test-session",
        count=2,
    )

    with patch("deadline.client.api.get_session_logs") as mock_get_logs, patch(
        "deadline.client.api._job_monitoring.get_user_and_identity_store_id"
    ) as mock_get_user, patch("deadline.client.api.get_boto3_client") as boto3_client_mock:
        mock_get_user.return_value = (None, None)
        mock_get_logs.return_value = timezone_aware_result

        # Mock get_job to return job name
        boto3_client_mock().get_job.return_value = {"name": "Test Job Name"}

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "logs",
                "--session-id",
                "test-session",
                "--timezone",
                "local",
                "--output",
                "json",
            ],
        )

        # Verify the output is valid JSON
        output_json = json.loads(result.output)
        assert "events" in output_json
        assert len(output_json["events"]) == 2
        assert output_json["events"][0]["message"] == "Log message 1"
        assert output_json["events"][1]["message"] == "Log message 2"

        # Verify timestamps are in ISO 8601 format with timezone info
        timestamp1 = output_json["events"][0]["timestamp"]
        timestamp2 = output_json["events"][1]["timestamp"]
        ingestion1 = output_json["events"][0]["ingestionTime"]
        ingestion2 = output_json["events"][1]["ingestionTime"]

        # Check ISO 8601 format (contains 'T' and timezone offset)
        assert "T" in timestamp1 and ("+" in timestamp1 or "-" in timestamp1)
        assert "T" in timestamp2 and ("+" in timestamp2 or "-" in timestamp2)
        assert "T" in ingestion1 and ("+" in ingestion1 or "-" in ingestion1)
        assert "T" in ingestion2 and ("+" in ingestion2 or "-" in ingestion2)

        assert result.exit_code == 0


def test_cli_job_logs_with_session_action_id_only(fresh_deadline_config, deadline_mock):
    """
    Test job logs command with only session action ID provided.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    session_id = "session-0123456789abcdefabcdefabcdefabcd"
    session_action_id = "sessionaction-0123456789abcdefabcdefabcdefabcd-1"

    # Mock get_job to return job name
    deadline_mock.get_job.return_value = {"name": "Test Job Name"}

    # Mock get_session_action to return session action details
    deadline_mock.get_session_action.return_value = {
        "sessionActionId": session_action_id,
        "status": "SUCCEEDED",
        "startedAt": datetime.datetime(2023, 1, 27, 7, 24, 0, tzinfo=datetime.timezone.utc),
        "endedAt": datetime.datetime(2023, 1, 27, 7, 25, 0, tzinfo=datetime.timezone.utc),
    }

    with patch.object(api, "get_session_logs") as mock_get_logs:
        # Mock the API response
        mock_get_logs.return_value = SessionLogResult(
            events=[
                LogEvent(
                    timestamp=datetime.datetime(
                        2023, 1, 27, 7, 24, 45, tzinfo=datetime.timezone.utc
                    ),
                    message="Test log message from session action",
                    ingestion_time=datetime.datetime(
                        2023, 1, 27, 7, 24, 46, tzinfo=datetime.timezone.utc
                    ),
                    event_id="event-1",
                ),
            ],
            count=1,
            next_token=None,
            log_group=f"/aws/deadline/{MOCK_FARM_ID}/{MOCK_QUEUE_ID}",
            log_stream=session_id,
        )

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "logs",
                "--session-action-id",
                session_action_id,
            ],
        )

        # Verify the API was called with the derived session ID and session action timestamps
        mock_get_logs.assert_called_once_with(
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            session_id=session_id,
            limit=100,
            start_time=datetime.datetime(2023, 1, 27, 7, 24, 0, tzinfo=datetime.timezone.utc),
            end_time=datetime.datetime(2023, 1, 27, 7, 25, 0, tzinfo=datetime.timezone.utc),
            next_token=None,
            config=ANY,
        )

        # Check output
        assert f"Retrieving logs for session action {session_action_id}" in result.output
        assert "Test log message from session action" in result.output
        assert result.exit_code == 0


def test_cli_job_logs_with_session_action_id_and_matching_session_id(
    fresh_deadline_config, deadline_mock
):
    """
    Test job logs command with both session action ID and matching session ID.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    session_action_id = "sessionaction-0123456789abcdefabcdefabcdefabcd-1"
    session_id = "session-0123456789abcdefabcdefabcdefabcd"

    # Mock get_job to return job name
    deadline_mock.get_job.return_value = {"name": "Test Job Name"}

    # Mock get_session_action to return session action details
    deadline_mock.get_session_action.return_value = {
        "sessionActionId": session_action_id,
        "status": "SUCCEEDED",
        "startedAt": datetime.datetime(2023, 1, 27, 7, 24, 0, tzinfo=datetime.timezone.utc),
        "endedAt": datetime.datetime(2023, 1, 27, 7, 25, 0, tzinfo=datetime.timezone.utc),
    }

    with patch.object(api, "get_session_logs") as mock_get_logs:
        # Mock the API response
        mock_get_logs.return_value = SessionLogResult(
            events=[
                LogEvent(
                    timestamp=datetime.datetime(
                        2023, 1, 27, 7, 24, 45, tzinfo=datetime.timezone.utc
                    ),
                    message="Test log message",
                    ingestion_time=datetime.datetime(
                        2023, 1, 27, 7, 24, 46, tzinfo=datetime.timezone.utc
                    ),
                    event_id="event-1",
                ),
            ],
            count=1,
            next_token=None,
            log_group=f"/aws/deadline/{MOCK_FARM_ID}/{MOCK_QUEUE_ID}",
            log_stream=session_id,
        )

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "logs",
                "--session-id",
                session_id,
                "--session-action-id",
                session_action_id,
            ],
        )

        # Verify the API was called with the session ID and session action timestamps
        mock_get_logs.assert_called_once_with(
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            session_id=session_id,
            limit=100,
            start_time=datetime.datetime(2023, 1, 27, 7, 24, 0, tzinfo=datetime.timezone.utc),
            end_time=datetime.datetime(2023, 1, 27, 7, 25, 0, tzinfo=datetime.timezone.utc),
            next_token=None,
            config=ANY,
        )

        # Check output
        assert "Test log message" in result.output
        assert result.exit_code == 0


def test_cli_job_logs_with_session_action_id_and_mismatching_session_id(
    fresh_deadline_config, deadline_mock
):
    """
    Test job logs command with session action ID and mismatching session ID raises error.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    session_action_id = "sessionaction-0123456789abcdefabcdefabcdefabcd-1"
    wrong_session_id = "session-ffffffffffffffffffffffffffffffff"

    # Mock get_job to return job name
    deadline_mock.get_job.return_value = {"name": "Test Job Name"}

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "job",
            "logs",
            "--session-id",
            wrong_session_id,
            "--session-action-id",
            session_action_id,
        ],
    )

    # Check error message
    assert "Session ID mismatch" in result.output
    assert wrong_session_id in result.output
    assert "session-0123456789abcdefabcdefabcdefabcd" in result.output
    assert session_action_id in result.output
    assert result.exit_code != 0


def test_cli_job_logs_with_invalid_session_action_id_format(fresh_deadline_config, deadline_mock):
    """
    Test job logs command with invalid session action ID format raises error.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    invalid_session_action_id = "invalid-format"

    # Mock get_job to return job name
    deadline_mock.get_job.return_value = {"name": "Test Job Name"}

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "job",
            "logs",
            "--session-action-id",
            invalid_session_action_id,
        ],
    )

    # Check error message
    assert "Invalid session action ID format" in result.output
    assert invalid_session_action_id in result.output
    assert "sessionaction-{uuid}-{number}" in result.output
    assert result.exit_code != 0


# Tests for _parse_session_action_id function


@pytest.mark.parametrize(
    "session_action_id,expected_session_id",
    [
        (
            "sessionaction-0123456789abcdef0123456789abcdef-0",
            "session-0123456789abcdef0123456789abcdef",
        ),
        (
            "sessionaction-abcdef0123456789abcdef0123456789-999999",
            "session-abcdef0123456789abcdef0123456789",
        ),
        (
            "sessionaction-ffffffffffffffffffffffffffffffff-123",
            "session-ffffffffffffffffffffffffffffffff",
        ),
        (
            "sessionaction-0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d-42",
            "session-0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d",
        ),
        (
            "sessionaction-0123456789abcdef0123456789abcdef-5",
            "session-0123456789abcdef0123456789abcdef",
        ),
        (
            "sessionaction-0123456789abcdef0123456789abcdef-12345",
            "session-0123456789abcdef0123456789abcdef",
        ),
    ],
    ids=[
        "standard_format",
        "large_number",
        "all_lowercase_hex",
        "mixed_hex",
        "single_digit_number",
        "multi_digit_number",
    ],
)
def test_parse_session_action_id_valid_formats(session_action_id, expected_session_id):
    """Test that _parse_session_action_id correctly parses valid session action IDs."""
    result = _parse_session_action_id(session_action_id)
    assert result == expected_session_id


@pytest.mark.parametrize(
    "session_action_id",
    [
        "session-0123456789abcdef0123456789abcdef-0",
        "wrongprefix-0123456789abcdef0123456789abcdef-0",
        "sessionaction-0123456789abcdef-0",
        "sessionaction-0123456789abcdef0123456789abcdef00-0",
        "sessionaction-0123456789ABCDEF0123456789ABCDEF-0",
        "sessionaction-0123456789abcdefghij0123456789ab-0",
        "sessionaction-0123456789abcdef0123456789abcdef",
        "sessionaction--0",
        "",
        "sessionaction-",
        "sessionaction-0123456789abcdef0123456789abcdef-0-extra",
        "sessionaction-0123456789abcdef0123456789abcdef-abc",
        "sessionaction-0123456789abcdef0123456789abcdef--1",
        "sessionaction-0123456789abcdef0123456789abcdef-0 ",
        " sessionaction-0123456789abcdef0123456789abcdef-0",
        "sessionaction-01234567-89ab-cdef-0123-456789abcdef-0",
        "not-a-valid-session-action-id",
    ],
    ids=[
        "invalid_prefix",
        "wrong_prefix",
        "uuid_too_short",
        "uuid_too_long",
        "uuid_uppercase",
        "uuid_non_hex",
        "missing_number",
        "missing_uuid",
        "empty_string",
        "only_prefix",
        "extra_dashes",
        "number_with_letters",
        "negative_number",
        "trailing_spaces",
        "leading_spaces",
        "uuid_with_dashes",
        "malformed_structure",
    ],
)
def test_parse_session_action_id_invalid_formats(session_action_id):
    """Test that _parse_session_action_id raises error for invalid session action IDs."""
    with pytest.raises(DeadlineOperationError) as exc_info:
        _parse_session_action_id(session_action_id)

    assert "Invalid session action ID format" in str(exc_info.value)


# ===== Tests for Session Action Retrieval =====


def test_session_action_retrieval_success_complete(fresh_deadline_config, deadline_mock):
    """
    Test successful retrieval of a completed session action with all timestamps.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    session_action_id = "sessionaction-0123456789abcdefabcdefabcdefabcd-1"
    session_id = "session-0123456789abcdefabcdefabcdefabcd"

    action_start = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
    action_end = datetime.datetime(2023, 1, 1, 13, 0, 0, tzinfo=datetime.timezone.utc)

    # Mock get_job to return job name
    deadline_mock.get_job.return_value = {"name": "Test Job Name"}

    # Mock get_session_action to return complete session action data
    deadline_mock.get_session_action.return_value = {
        "sessionActionId": session_action_id,
        "status": "SUCCEEDED",
        "startedAt": action_start,
        "endedAt": action_end,
        "sessionId": session_id,
        "definition": {"taskRun": {"taskId": "task-1"}},
    }

    with patch("deadline.client.api.get_session_logs") as mock_get_logs:
        # Mock get_session_logs response
        mock_get_logs.return_value = SAMPLE_LOG_RESULT

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "logs",
                "--session-action-id",
                session_action_id,
            ],
        )

        assert result.exit_code == 0

        # Verify get_session_action was called with correct parameters
        deadline_mock.get_session_action.assert_called_once_with(
            farmId=MOCK_FARM_ID,
            queueId=MOCK_QUEUE_ID,
            jobId=MOCK_JOB_ID,
            sessionActionId=session_action_id,
        )

        # Verify get_session_logs was called with the session action's time range
        mock_get_logs.assert_called_once()
        _, kwargs = mock_get_logs.call_args
        assert kwargs["farm_id"] == MOCK_FARM_ID
        assert kwargs["queue_id"] == MOCK_QUEUE_ID
        assert kwargs["session_id"] == session_id
        assert kwargs["start_time"] == action_start
        assert kwargs["end_time"] == action_end


def test_session_action_retrieval_success_in_progress(fresh_deadline_config, deadline_mock):
    """
    Test successful retrieval of an in-progress session action (no endedAt).
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    session_action_id = "sessionaction-0123456789abcdefabcdefabcdefabcd-1"
    session_id = "session-0123456789abcdefabcdefabcdefabcd"

    action_start = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)

    # Mock get_job to return job name
    deadline_mock.get_job.return_value = {"name": "Test Job Name"}

    # Mock get_session_action to return in-progress session action (no endedAt)
    deadline_mock.get_session_action.return_value = {
        "sessionActionId": session_action_id,
        "status": "RUNNING",
        "startedAt": action_start,
        # No endedAt - session action is in progress
        "sessionId": session_id,
        "definition": {"taskRun": {"taskId": "task-1"}},
    }

    with patch("deadline.client.api.get_session_logs") as mock_get_logs, patch(
        "deadline.client.cli._groups.job_group.datetime"
    ) as mock_datetime:
        # Mock datetime.now to return a fixed time
        mock_now = datetime.datetime(2023, 1, 1, 14, 0, 0, tzinfo=datetime.timezone.utc)
        mock_datetime.datetime.now.return_value = mock_now
        mock_datetime.timezone = datetime.timezone

        # Mock get_session_logs response
        mock_get_logs.return_value = SAMPLE_LOG_RESULT

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "logs",
                "--session-action-id",
                session_action_id,
            ],
        )

        assert result.exit_code == 0

        # Verify get_session_action was called with correct parameters
        deadline_mock.get_session_action.assert_called_once_with(
            farmId=MOCK_FARM_ID,
            queueId=MOCK_QUEUE_ID,
            jobId=MOCK_JOB_ID,
            sessionActionId=session_action_id,
        )

        # Verify get_session_logs was called with current time as end time
        mock_get_logs.assert_called_once()
        _, kwargs = mock_get_logs.call_args
        assert kwargs["farm_id"] == MOCK_FARM_ID
        assert kwargs["queue_id"] == MOCK_QUEUE_ID
        assert kwargs["session_id"] == session_id
        assert kwargs["start_time"] == action_start
        assert kwargs["end_time"] == mock_now


def test_session_action_retrieval_resource_not_found(fresh_deadline_config, deadline_mock):
    """
    Test handling of ResourceNotFoundException when session action doesn't exist.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    session_action_id = "sessionaction-0123456789abcdefabcdefabcdefabcd-1"

    # Mock get_job to return job name
    deadline_mock.get_job.return_value = {"name": "Test Job Name"}

    # Mock get_session_action to raise ResourceNotFoundException
    error_response = {"Error": {"Code": "ResourceNotFoundException", "Message": "Not found"}}
    deadline_mock.get_session_action.side_effect = ClientError(error_response, "GetSessionAction")

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "job",
            "logs",
            "--session-action-id",
            session_action_id,
        ],
    )

    # Verify error message is clear
    assert result.exit_code != 0
    assert f"Session action '{session_action_id}' not found" in result.output
    assert f"job '{MOCK_JOB_ID}'" in result.output


def test_session_action_retrieval_other_client_error(fresh_deadline_config, deadline_mock):
    """
    Test handling of other ClientError scenarios when retrieving session action.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    session_action_id = "sessionaction-0123456789abcdefabcdefabcdefabcd-1"

    # Mock get_job to return job name
    deadline_mock.get_job.return_value = {"name": "Test Job Name"}

    # Mock get_session_action to raise a different ClientError
    error_response = {"Error": {"Code": "AccessDeniedException", "Message": "Access denied"}}
    deadline_mock.get_session_action.side_effect = ClientError(error_response, "GetSessionAction")

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "job",
            "logs",
            "--session-action-id",
            session_action_id,
        ],
    )

    # Verify error message includes the session action ID
    assert result.exit_code != 0
    assert f"Failed to retrieve session action '{session_action_id}'" in result.output


def test_session_action_retrieval_not_started(fresh_deadline_config, deadline_mock):
    """
    Test handling of session action that hasn't started yet (no startedAt).
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    session_action_id = "sessionaction-0123456789abcdefabcdefabcdefabcd-1"
    session_id = "session-0123456789abcdefabcdefabcdefabcd"

    # Mock get_job to return job name
    deadline_mock.get_job.return_value = {"name": "Test Job Name"}

    # Mock get_session_action to return session action without startedAt
    deadline_mock.get_session_action.return_value = {
        "sessionActionId": session_action_id,
        "status": "SCHEDULED",
        # No startedAt - session action hasn't started yet
        "sessionId": session_id,
        "definition": {"taskRun": {"taskId": "task-1"}},
    }

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "job",
            "logs",
            "--session-action-id",
            session_action_id,
        ],
    )

    # Verify error message indicates no logs are available
    assert result.exit_code != 0
    assert f"Session action '{session_action_id}' has not started yet" in result.output
    assert "No logs are available" in result.output


def test_session_action_retrieval_verifies_api_parameters(fresh_deadline_config, deadline_mock):
    """
    Test that correct parameters are passed to get_session_action API.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    session_action_id = "sessionaction-abcdef0123456789abcdef0123456789-42"
    session_id = "session-abcdef0123456789abcdef0123456789"

    action_start = datetime.datetime(2023, 5, 15, 10, 30, 0, tzinfo=datetime.timezone.utc)
    action_end = datetime.datetime(2023, 5, 15, 11, 45, 0, tzinfo=datetime.timezone.utc)

    # Mock get_job to return job name
    deadline_mock.get_job.return_value = {"name": "Test Job Name"}

    # Mock get_session_action
    deadline_mock.get_session_action.return_value = {
        "sessionActionId": session_action_id,
        "status": "SUCCEEDED",
        "startedAt": action_start,
        "endedAt": action_end,
        "sessionId": session_id,
        "definition": {"envEnter": {"environmentId": "env-1"}},
    }

    with patch("deadline.client.api.get_session_logs") as mock_get_logs:
        # Mock get_session_logs response
        mock_get_logs.return_value = SAMPLE_LOG_RESULT

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "logs",
                "--session-action-id",
                session_action_id,
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--job-id",
                MOCK_JOB_ID,
            ],
        )

        assert result.exit_code == 0

        # Verify get_session_action was called with exact parameters
        deadline_mock.get_session_action.assert_called_once_with(
            farmId=MOCK_FARM_ID,
            queueId=MOCK_QUEUE_ID,
            jobId=MOCK_JOB_ID,
            sessionActionId=session_action_id,
        )

        # Verify the derived session ID was used correctly
        mock_get_logs.assert_called_once()
        _, kwargs = mock_get_logs.call_args
        assert kwargs["session_id"] == session_id


def test_time_range_intersection_only_session_action_timestamps(
    fresh_deadline_config, deadline_mock
):
    """
    Test time range intersection with only session action timestamps (no user times).
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    session_action_id = "sessionaction-abcdef0123456789abcdef0123456789-42"
    session_id = "session-abcdef0123456789abcdef0123456789"

    action_start = datetime.datetime(2023, 5, 15, 10, 30, 0, tzinfo=datetime.timezone.utc)
    action_end = datetime.datetime(2023, 5, 15, 11, 45, 0, tzinfo=datetime.timezone.utc)

    # Mock get_job to return job name
    deadline_mock.get_job.return_value = {"name": "Test Job Name"}

    # Mock get_session_action
    deadline_mock.get_session_action.return_value = {
        "sessionActionId": session_action_id,
        "status": "SUCCEEDED",
        "startedAt": action_start,
        "endedAt": action_end,
        "sessionId": session_id,
        "definition": {"taskRun": {"taskId": "task-1"}},
    }

    with patch("deadline.client.api.get_session_logs") as mock_get_logs:
        # Mock get_session_logs response
        mock_get_logs.return_value = SAMPLE_LOG_RESULT

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "logs",
                "--session-action-id",
                session_action_id,
            ],
        )

        assert result.exit_code == 0

        # Verify get_session_logs was called with session action timestamps
        mock_get_logs.assert_called_once()
        _, kwargs = mock_get_logs.call_args
        assert kwargs["start_time"] == action_start
        assert kwargs["end_time"] == action_end


def test_time_range_intersection_user_start_time_within_range(fresh_deadline_config, deadline_mock):
    """
    Test time range intersection with user start time within session action range.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    session_action_id = "sessionaction-abcdef0123456789abcdef0123456789-42"
    session_id = "session-abcdef0123456789abcdef0123456789"

    action_start = datetime.datetime(2023, 5, 15, 10, 0, 0, tzinfo=datetime.timezone.utc)
    action_end = datetime.datetime(2023, 5, 15, 12, 0, 0, tzinfo=datetime.timezone.utc)
    user_start = "2023-05-15T10:30:00Z"  # Within action range

    # Mock get_job to return job name
    deadline_mock.get_job.return_value = {"name": "Test Job Name"}

    # Mock get_session_action
    deadline_mock.get_session_action.return_value = {
        "sessionActionId": session_action_id,
        "status": "SUCCEEDED",
        "startedAt": action_start,
        "endedAt": action_end,
        "sessionId": session_id,
        "definition": {"taskRun": {"taskId": "task-1"}},
    }

    with patch("deadline.client.api.get_session_logs") as mock_get_logs:
        # Mock get_session_logs response
        mock_get_logs.return_value = SAMPLE_LOG_RESULT

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "logs",
                "--session-action-id",
                session_action_id,
                "--start-time",
                user_start,
            ],
        )

        assert result.exit_code == 0

        # Verify get_session_logs was called with max(user_start, action_start)
        mock_get_logs.assert_called_once()
        _, kwargs = mock_get_logs.call_args
        # User start is later, so it should be used
        expected_start = datetime.datetime(2023, 5, 15, 10, 30, 0, tzinfo=datetime.timezone.utc)
        assert kwargs["start_time"] == expected_start
        assert kwargs["end_time"] == action_end


def test_time_range_intersection_user_end_time_within_range(fresh_deadline_config, deadline_mock):
    """
    Test time range intersection with user end time within session action range.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    session_action_id = "sessionaction-abcdef0123456789abcdef0123456789-42"
    session_id = "session-abcdef0123456789abcdef0123456789"

    action_start = datetime.datetime(2023, 5, 15, 10, 0, 0, tzinfo=datetime.timezone.utc)
    action_end = datetime.datetime(2023, 5, 15, 12, 0, 0, tzinfo=datetime.timezone.utc)
    user_end = "2023-05-15T11:30:00Z"  # Within action range

    # Mock get_job to return job name
    deadline_mock.get_job.return_value = {"name": "Test Job Name"}

    # Mock get_session_action
    deadline_mock.get_session_action.return_value = {
        "sessionActionId": session_action_id,
        "status": "SUCCEEDED",
        "startedAt": action_start,
        "endedAt": action_end,
        "sessionId": session_id,
        "definition": {"taskRun": {"taskId": "task-1"}},
    }

    with patch("deadline.client.api.get_session_logs") as mock_get_logs:
        # Mock get_session_logs response
        mock_get_logs.return_value = SAMPLE_LOG_RESULT

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "logs",
                "--session-action-id",
                session_action_id,
                "--end-time",
                user_end,
            ],
        )

        assert result.exit_code == 0

        # Verify get_session_logs was called with min(user_end, action_end)
        mock_get_logs.assert_called_once()
        _, kwargs = mock_get_logs.call_args
        # User end is earlier, so it should be used
        expected_end = datetime.datetime(2023, 5, 15, 11, 30, 0, tzinfo=datetime.timezone.utc)
        assert kwargs["start_time"] == action_start
        assert kwargs["end_time"] == expected_end


def test_time_range_intersection_both_user_times_within_range(fresh_deadline_config, deadline_mock):
    """
    Test time range intersection with both user start and end times within session action range.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    session_action_id = "sessionaction-abcdef0123456789abcdef0123456789-42"
    session_id = "session-abcdef0123456789abcdef0123456789"

    action_start = datetime.datetime(2023, 5, 15, 10, 0, 0, tzinfo=datetime.timezone.utc)
    action_end = datetime.datetime(2023, 5, 15, 12, 0, 0, tzinfo=datetime.timezone.utc)
    user_start = "2023-05-15T10:30:00Z"  # Within action range
    user_end = "2023-05-15T11:30:00Z"  # Within action range

    # Mock get_job to return job name
    deadline_mock.get_job.return_value = {"name": "Test Job Name"}

    # Mock get_session_action
    deadline_mock.get_session_action.return_value = {
        "sessionActionId": session_action_id,
        "status": "SUCCEEDED",
        "startedAt": action_start,
        "endedAt": action_end,
        "sessionId": session_id,
        "definition": {"taskRun": {"taskId": "task-1"}},
    }

    with patch("deadline.client.api.get_session_logs") as mock_get_logs:
        # Mock get_session_logs response
        mock_get_logs.return_value = SAMPLE_LOG_RESULT

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "logs",
                "--session-action-id",
                session_action_id,
                "--start-time",
                user_start,
                "--end-time",
                user_end,
            ],
        )

        assert result.exit_code == 0

        # Verify get_session_logs was called with user times (both within range)
        mock_get_logs.assert_called_once()
        _, kwargs = mock_get_logs.call_args
        expected_start = datetime.datetime(2023, 5, 15, 10, 30, 0, tzinfo=datetime.timezone.utc)
        expected_end = datetime.datetime(2023, 5, 15, 11, 30, 0, tzinfo=datetime.timezone.utc)
        assert kwargs["start_time"] == expected_start
        assert kwargs["end_time"] == expected_end


def test_time_range_intersection_user_times_partially_overlap_start_before(
    fresh_deadline_config, deadline_mock
):
    """
    Test time range intersection when user start time is before action start (should use intersection).
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    session_action_id = "sessionaction-abcdef0123456789abcdef0123456789-42"
    session_id = "session-abcdef0123456789abcdef0123456789"

    action_start = datetime.datetime(2023, 5, 15, 10, 0, 0, tzinfo=datetime.timezone.utc)
    action_end = datetime.datetime(2023, 5, 15, 12, 0, 0, tzinfo=datetime.timezone.utc)
    user_start = "2023-05-15T09:00:00Z"  # Before action start
    user_end = "2023-05-15T11:00:00Z"  # Within action range

    # Mock get_job to return job name
    deadline_mock.get_job.return_value = {"name": "Test Job Name"}

    # Mock get_session_action
    deadline_mock.get_session_action.return_value = {
        "sessionActionId": session_action_id,
        "status": "SUCCEEDED",
        "startedAt": action_start,
        "endedAt": action_end,
        "sessionId": session_id,
        "definition": {"taskRun": {"taskId": "task-1"}},
    }

    with patch("deadline.client.api.get_session_logs") as mock_get_logs:
        # Mock get_session_logs response
        mock_get_logs.return_value = SAMPLE_LOG_RESULT

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "logs",
                "--session-action-id",
                session_action_id,
                "--start-time",
                user_start,
                "--end-time",
                user_end,
            ],
        )

        assert result.exit_code == 0

        # Verify get_session_logs was called with intersection
        mock_get_logs.assert_called_once()
        _, kwargs = mock_get_logs.call_args
        # Should use max(user_start, action_start) = action_start
        # Should use min(user_end, action_end) = user_end
        assert kwargs["start_time"] == action_start
        expected_end = datetime.datetime(2023, 5, 15, 11, 0, 0, tzinfo=datetime.timezone.utc)
        assert kwargs["end_time"] == expected_end


def test_time_range_intersection_user_times_partially_overlap_end_after(
    fresh_deadline_config, deadline_mock
):
    """
    Test time range intersection when user end time is after action end (should use intersection).
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    session_action_id = "sessionaction-abcdef0123456789abcdef0123456789-42"
    session_id = "session-abcdef0123456789abcdef0123456789"

    action_start = datetime.datetime(2023, 5, 15, 10, 0, 0, tzinfo=datetime.timezone.utc)
    action_end = datetime.datetime(2023, 5, 15, 12, 0, 0, tzinfo=datetime.timezone.utc)
    user_start = "2023-05-15T11:00:00Z"  # Within action range
    user_end = "2023-05-15T13:00:00Z"  # After action end

    # Mock get_job to return job name
    deadline_mock.get_job.return_value = {"name": "Test Job Name"}

    # Mock get_session_action
    deadline_mock.get_session_action.return_value = {
        "sessionActionId": session_action_id,
        "status": "SUCCEEDED",
        "startedAt": action_start,
        "endedAt": action_end,
        "sessionId": session_id,
        "definition": {"taskRun": {"taskId": "task-1"}},
    }

    with patch("deadline.client.api.get_session_logs") as mock_get_logs:
        # Mock get_session_logs response
        mock_get_logs.return_value = SAMPLE_LOG_RESULT

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "logs",
                "--session-action-id",
                session_action_id,
                "--start-time",
                user_start,
                "--end-time",
                user_end,
            ],
        )

        assert result.exit_code == 0

        # Verify get_session_logs was called with intersection
        mock_get_logs.assert_called_once()
        _, kwargs = mock_get_logs.call_args
        # Should use max(user_start, action_start) = user_start
        # Should use min(user_end, action_end) = action_end
        expected_start = datetime.datetime(2023, 5, 15, 11, 0, 0, tzinfo=datetime.timezone.utc)
        assert kwargs["start_time"] == expected_start
        assert kwargs["end_time"] == action_end


def test_time_range_intersection_user_times_no_overlap_before(fresh_deadline_config, deadline_mock):
    """
    Test time range intersection when user times don't overlap (before action range) - should raise error.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    session_action_id = "sessionaction-abcdef0123456789abcdef0123456789-42"
    session_id = "session-abcdef0123456789abcdef0123456789"

    action_start = datetime.datetime(2023, 5, 15, 10, 0, 0, tzinfo=datetime.timezone.utc)
    action_end = datetime.datetime(2023, 5, 15, 12, 0, 0, tzinfo=datetime.timezone.utc)
    user_start = "2023-05-15T08:00:00Z"  # Before action start
    user_end = "2023-05-15T09:00:00Z"  # Before action start

    # Mock get_job to return job name
    deadline_mock.get_job.return_value = {"name": "Test Job Name"}

    # Mock get_session_action
    deadline_mock.get_session_action.return_value = {
        "sessionActionId": session_action_id,
        "status": "SUCCEEDED",
        "startedAt": action_start,
        "endedAt": action_end,
        "sessionId": session_id,
        "definition": {"taskRun": {"taskId": "task-1"}},
    }

    with patch("deadline.client.api.get_session_logs") as mock_get_logs:
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "logs",
                "--session-action-id",
                session_action_id,
                "--start-time",
                user_start,
                "--end-time",
                user_end,
            ],
        )

        # Should fail with error about non-overlapping ranges
        assert result.exit_code != 0
        assert "does not overlap with the session action's time range" in result.output
        # Verify error message includes both time ranges
        assert "2023-05-15T10:00:00+00:00" in result.output  # action_start
        assert "2023-05-15T12:00:00+00:00" in result.output  # action_end
        assert user_start in result.output  # user_start
        assert user_end in result.output  # user_end

        # Verify get_session_logs was NOT called
        mock_get_logs.assert_not_called()


def test_time_range_intersection_user_times_no_overlap_after(fresh_deadline_config, deadline_mock):
    """
    Test time range intersection when user times don't overlap (after action range) - should raise error.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    session_action_id = "sessionaction-abcdef0123456789abcdef0123456789-42"
    session_id = "session-abcdef0123456789abcdef0123456789"

    action_start = datetime.datetime(2023, 5, 15, 10, 0, 0, tzinfo=datetime.timezone.utc)
    action_end = datetime.datetime(2023, 5, 15, 12, 0, 0, tzinfo=datetime.timezone.utc)
    user_start = "2023-05-15T13:00:00Z"  # After action end
    user_end = "2023-05-15T14:00:00Z"  # After action end

    # Mock get_job to return job name
    deadline_mock.get_job.return_value = {"name": "Test Job Name"}

    # Mock get_session_action
    deadline_mock.get_session_action.return_value = {
        "sessionActionId": session_action_id,
        "status": "SUCCEEDED",
        "startedAt": action_start,
        "endedAt": action_end,
        "sessionId": session_id,
        "definition": {"taskRun": {"taskId": "task-1"}},
    }

    with patch("deadline.client.api.get_session_logs") as mock_get_logs:
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "logs",
                "--session-action-id",
                session_action_id,
                "--start-time",
                user_start,
                "--end-time",
                user_end,
            ],
        )

        # Should fail with error about non-overlapping ranges
        assert result.exit_code != 0
        assert "does not overlap with the session action's time range" in result.output
        # Verify error message includes both time ranges
        assert "2023-05-15T10:00:00+00:00" in result.output  # action_start
        assert "2023-05-15T12:00:00+00:00" in result.output  # action_end
        assert user_start in result.output  # user_start
        assert user_end in result.output  # user_end

        # Verify get_session_logs was NOT called
        mock_get_logs.assert_not_called()


def test_time_range_intersection_in_progress_session_action(fresh_deadline_config, deadline_mock):
    """
    Test time range intersection with in-progress session action (no endedAt).
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    session_action_id = "sessionaction-abcdef0123456789abcdef0123456789-42"
    session_id = "session-abcdef0123456789abcdef0123456789"

    action_start = datetime.datetime(2023, 5, 15, 10, 0, 0, tzinfo=datetime.timezone.utc)
    # No endedAt - session action is in progress

    # Mock get_job to return job name
    deadline_mock.get_job.return_value = {"name": "Test Job Name"}

    # Mock get_session_action - no endedAt
    deadline_mock.get_session_action.return_value = {
        "sessionActionId": session_action_id,
        "status": "RUNNING",
        "startedAt": action_start,
        # No endedAt - in progress
        "sessionId": session_id,
        "definition": {"taskRun": {"taskId": "task-1"}},
    }

    # Mock current time
    mock_now = datetime.datetime(2023, 5, 15, 11, 30, 0, tzinfo=datetime.timezone.utc)

    with patch("deadline.client.api.get_session_logs") as mock_get_logs:
        # Mock get_session_logs response
        mock_get_logs.return_value = SAMPLE_LOG_RESULT

        with freeze_time(mock_now):
            runner = CliRunner()
            result = runner.invoke(
                main,
                [
                    "job",
                    "logs",
                    "--session-action-id",
                    session_action_id,
                ],
            )

            assert result.exit_code == 0

            # Verify get_session_logs was called with action_start and current time
            mock_get_logs.assert_called_once()
        _, kwargs = mock_get_logs.call_args
        assert kwargs["start_time"] == action_start
        assert kwargs["end_time"] == mock_now


def test_time_range_intersection_in_progress_with_user_end_time(
    fresh_deadline_config, deadline_mock
):
    """
    Test time range intersection with in-progress session action and user end time.
    Should use min(user_end, current_time).
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    session_action_id = "sessionaction-abcdef0123456789abcdef0123456789-42"
    session_id = "session-abcdef0123456789abcdef0123456789"

    action_start = datetime.datetime(2023, 5, 15, 10, 0, 0, tzinfo=datetime.timezone.utc)
    user_end = "2023-05-15T11:00:00Z"  # User wants logs up to this time

    # Mock get_job to return job name
    deadline_mock.get_job.return_value = {"name": "Test Job Name"}

    # Mock get_session_action - no endedAt
    deadline_mock.get_session_action.return_value = {
        "sessionActionId": session_action_id,
        "status": "RUNNING",
        "startedAt": action_start,
        # No endedAt - in progress
        "sessionId": session_id,
        "definition": {"taskRun": {"taskId": "task-1"}},
    }

    # Mock current time (later than user_end)
    mock_now = datetime.datetime(2023, 5, 15, 11, 30, 0, tzinfo=datetime.timezone.utc)

    with patch("deadline.client.api.get_session_logs") as mock_get_logs:
        # Mock get_session_logs response
        mock_get_logs.return_value = SAMPLE_LOG_RESULT

        with freeze_time(mock_now):
            runner = CliRunner()
            result = runner.invoke(
                main,
                [
                    "job",
                    "logs",
                    "--session-action-id",
                    session_action_id,
                    "--end-time",
                    user_end,
                ],
            )

            assert result.exit_code == 0

            # Verify get_session_logs was called with action_start and min(user_end, current_time)
            mock_get_logs.assert_called_once()
            _, kwargs = mock_get_logs.call_args
            assert kwargs["start_time"] == action_start
            # User end is earlier than current time, so it should be used
            expected_end = datetime.datetime(2023, 5, 15, 11, 0, 0, tzinfo=datetime.timezone.utc)
            assert kwargs["end_time"] == expected_end


def test_time_range_intersection_error_message_includes_both_ranges(
    fresh_deadline_config, deadline_mock
):
    """
    Test that error message includes both time ranges when they don't overlap.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    session_action_id = "sessionaction-abcdef0123456789abcdef0123456789-42"
    session_id = "session-abcdef0123456789abcdef0123456789"

    action_start = datetime.datetime(2023, 5, 15, 10, 0, 0, tzinfo=datetime.timezone.utc)
    action_end = datetime.datetime(2023, 5, 15, 12, 0, 0, tzinfo=datetime.timezone.utc)
    user_start = "2023-05-15T14:00:00Z"
    user_end = "2023-05-15T15:00:00Z"

    # Mock get_job to return job name
    deadline_mock.get_job.return_value = {"name": "Test Job Name"}

    # Mock get_session_action
    deadline_mock.get_session_action.return_value = {
        "sessionActionId": session_action_id,
        "status": "SUCCEEDED",
        "startedAt": action_start,
        "endedAt": action_end,
        "sessionId": session_id,
        "definition": {"taskRun": {"taskId": "task-1"}},
    }

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "job",
            "logs",
            "--session-action-id",
            session_action_id,
            "--start-time",
            user_start,
            "--end-time",
            user_end,
        ],
    )

    # Should fail with detailed error message
    assert result.exit_code != 0
    assert "does not overlap with the session action's time range" in result.output
    # Verify error message includes session action time range
    assert "Session action time range:" in result.output
    assert "2023-05-15T10:00:00+00:00" in result.output
    assert "2023-05-15T12:00:00+00:00" in result.output
    # Verify error message includes specified time range
    assert "Specified time range:" in result.output
    assert user_start in result.output
    assert user_end in result.output
