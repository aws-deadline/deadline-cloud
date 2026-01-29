# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the search term functionality in `deadline job get`.
"""

import datetime
from datetime import timezone

from click.testing import CliRunner

from deadline.client import config
from deadline.client.cli import main

from ..shared_constants import MOCK_FARM_ID, MOCK_QUEUE_ID


MOCK_JOB_1 = {
    "jobId": "job-aaf4cdf8aae242f58fb84c5bb19f199b",
    "name": "Blender Render",
    "taskRunStatus": "SUCCEEDED",
    "taskRunStatusCounts": {"SUCCEEDED": 1},
    "lifecycleStatus": "CREATE_COMPLETE",
    "createdBy": "user-123",
    "createdAt": datetime.datetime(2023, 1, 27, 7, 34, 41, tzinfo=timezone.utc),
    "startedAt": datetime.datetime(2023, 1, 27, 7, 37, 53, tzinfo=timezone.utc),
    "endedAt": datetime.datetime(2023, 1, 27, 7, 39, 17, tzinfo=timezone.utc),
    "priority": 50,
}

MOCK_JOB_2 = {
    "jobId": "job-0d239749fa05435f90263b3a8be54144",
    "name": "Blender Render Scene 2",
    "taskRunStatus": "RUNNING",
    "taskRunStatusCounts": {"RUNNING": 1},
    "lifecycleStatus": "CREATE_COMPLETE",
    "createdBy": "user-123",
    "createdAt": datetime.datetime(2023, 1, 27, 7, 24, 22, tzinfo=timezone.utc),
    "startedAt": datetime.datetime(2023, 1, 27, 7, 27, 6, tzinfo=timezone.utc),
    "priority": 50,
}

MOCK_JOB_FULL_DETAILS = {
    **MOCK_JOB_1,
    "lifecycleStatusMessage": "Job completed successfully",
    "taskRunStatusCounts": {
        "PENDING": 0,
        "READY": 0,
        "SUCCEEDED": 1,
    },
    "maxFailedTasksCount": 20,
    "maxRetriesPerTask": 5,
}


class TestJobGetSearch:
    """Tests for job search functionality."""

    def test_search_single_result_shows_full_details(self, fresh_deadline_config, deadline_mock):
        """When search returns exactly one job, show full job details."""
        config.set_setting("defaults.farm_id", MOCK_FARM_ID)
        config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

        deadline_mock.search_jobs.return_value = {
            "jobs": [MOCK_JOB_1],
            "totalResults": 1,
        }
        deadline_mock.get_job.return_value = MOCK_JOB_FULL_DETAILS

        runner = CliRunner()
        result = runner.invoke(main, ["job", "get", "Blender Render"])

        assert result.exit_code == 0
        deadline_mock.get_job.assert_called_once_with(
            farmId=MOCK_FARM_ID, queueId=MOCK_QUEUE_ID, jobId=MOCK_JOB_1["jobId"]
        )
        assert "jobId: job-aaf4cdf8aae242f58fb84c5bb19f199b" in result.output
        assert "lifecycleStatusMessage:" in result.output

    def test_search_multiple_results_shows_summary(self, fresh_deadline_config, deadline_mock):
        """When search returns multiple jobs, show summary table."""
        config.set_setting("defaults.farm_id", MOCK_FARM_ID)
        config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

        deadline_mock.search_jobs.return_value = {
            "jobs": [MOCK_JOB_1, MOCK_JOB_2],
            "totalResults": 2,
        }

        runner = CliRunner()
        result = runner.invoke(main, ["job", "get", "Blender"])

        assert result.exit_code == 0
        deadline_mock.get_job.assert_not_called()
        assert 'Found 2 job(s) matching "Blender"' in result.output
        assert "job-aaf4cdf8aae242f58fb84c5bb19f199b" in result.output
        assert "job-0d239749fa05435f90263b3a8be54144" in result.output
        assert "To get details, run:" in result.output

    def test_search_no_results(self, fresh_deadline_config, deadline_mock):
        """When search returns no jobs, show appropriate message."""
        config.set_setting("defaults.farm_id", MOCK_FARM_ID)
        config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

        deadline_mock.search_jobs.return_value = {
            "jobs": [],
            "totalResults": 0,
        }

        runner = CliRunner()
        result = runner.invoke(main, ["job", "get", "nonexistent"])

        assert result.exit_code == 0
        assert 'No jobs found matching "nonexistent"' in result.output

    def test_job_id_as_positional_arg(self, fresh_deadline_config, deadline_mock):
        """When positional arg is a job ID, fetch that job directly."""
        config.set_setting("defaults.farm_id", MOCK_FARM_ID)
        config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

        job_id = "job-aaf4cdf8aae242f58fb84c5bb19f199b"
        deadline_mock.get_job.return_value = MOCK_JOB_FULL_DETAILS

        runner = CliRunner()
        result = runner.invoke(main, ["job", "get", job_id])

        assert result.exit_code == 0
        deadline_mock.search_jobs.assert_not_called()
        deadline_mock.get_job.assert_called_once_with(
            farmId=MOCK_FARM_ID, queueId=MOCK_QUEUE_ID, jobId=job_id
        )

    def test_job_id_with_flag(self, fresh_deadline_config, deadline_mock):
        """When --job-id is provided, fetch that job directly."""
        config.set_setting("defaults.farm_id", MOCK_FARM_ID)
        config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

        job_id = "job-aaf4cdf8aae242f58fb84c5bb19f199b"
        deadline_mock.get_job.return_value = MOCK_JOB_FULL_DETAILS

        runner = CliRunner()
        result = runner.invoke(main, ["job", "get", "--job-id", job_id])

        assert result.exit_code == 0
        deadline_mock.get_job.assert_called_once_with(
            farmId=MOCK_FARM_ID, queueId=MOCK_QUEUE_ID, jobId=job_id
        )

    def test_no_args_uses_default_job_id(self, fresh_deadline_config, deadline_mock):
        """When no args provided, use default job ID from config."""
        config.set_setting("defaults.farm_id", MOCK_FARM_ID)
        config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
        config.set_setting("defaults.job_id", str(MOCK_JOB_1["jobId"]))

        deadline_mock.get_job.return_value = MOCK_JOB_FULL_DETAILS

        runner = CliRunner()
        result = runner.invoke(main, ["job", "get"])

        assert result.exit_code == 0
        deadline_mock.get_job.assert_called_once_with(
            farmId=MOCK_FARM_ID, queueId=MOCK_QUEUE_ID, jobId=MOCK_JOB_1["jobId"]
        )

    def test_no_args_no_default_job_id_shows_error(self, fresh_deadline_config):
        """When no args and no default job ID, show error."""
        config.set_setting("defaults.farm_id", MOCK_FARM_ID)
        config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
        # No default job_id set

        runner = CliRunner()
        result = runner.invoke(main, ["job", "get"])

        assert result.exit_code != 0
        assert "Missing '--job-id' or default Job ID configuration" in result.output
