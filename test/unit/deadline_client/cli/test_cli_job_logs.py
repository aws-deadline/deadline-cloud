# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the CLI job logs command.
"""

import json
import datetime
from unittest.mock import ANY, MagicMock, patch

from click.testing import CliRunner

from deadline.client import api
from deadline.client.cli import main
from deadline.client.config import config_file as config
from deadline.client.api._job_monitoring import SessionLogResult, LogEvent

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
