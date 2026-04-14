# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Integration tests for the `deadline job` CLI commands.

Tests cover: job list, job get, job cancel, and job trace-schedule.
"""

import json
import time

import boto3
import pytest
import yaml
from click.testing import CliRunner

from pathlib import Path

from deadline.client.cli import main

from .test_utils import DeadlineCliTest

SIMPLE_ECHO_BUNDLE = str(Path(__file__).parent / "job_bundles" / "simple_echo")


@pytest.fixture(scope="module")
def deadline_client():
    """Create a boto3 deadline client for the test module."""
    return boto3.client("deadline")


def _load_echo_template() -> str:
    """Load the simple_echo bundle template as a JSON string for create_job."""
    with open(f"{SIMPLE_ECHO_BUNDLE}/template.yaml") as f:
        return json.dumps(yaml.safe_load(f))


def _submit_job(
    deadline_client,
    farm_id: str,
    queue_id: str,
    name: str,
    target_task_run_status: str = "SUSPENDED",
) -> str:
    """Submit a job using the simple_echo template and return its ID.

    Defaults to SUSPENDED so the job doesn't consume fleet capacity.
    """
    return deadline_client.create_job(
        farmId=farm_id,
        queueId=queue_id,
        template=_load_echo_template(),
        templateType="JSON",
        priority=50,
        targetTaskRunStatus=target_task_run_status,
        nameOverride=name,
    )["jobId"]


def _wait_for_job_update_succeeded(
    deadline_client, farm_id: str, queue_id: str, job_id: str, max_retries: int = 5
) -> None:
    """Poll with exponential backoff until the job's lifecycleStatus reaches UPDATE_SUCCEEDED."""
    delay = 1
    for attempt in range(max_retries):
        resp = deadline_client.get_job(farmId=farm_id, queueId=queue_id, jobId=job_id)
        if resp["lifecycleStatus"] == "UPDATE_SUCCEEDED":
            return
        time.sleep(delay)
        delay *= 2
    raise TimeoutError(
        f"Job {job_id} did not reach UPDATE_SUCCEEDED after {max_retries} retries "
        f"(last status: {resp['lifecycleStatus']})"
    )


@pytest.fixture(scope="module")
def submitted_job_id(deadline_cli_test: DeadlineCliTest, deadline_client) -> str:
    """
    Submit a minimal job and return its ID.

    Submitted as SUSPENDED so it doesn't consume fleet capacity.
    Waits for creation to complete so all fields are available.
    """
    job_id = _submit_job(
        deadline_client,
        deadline_cli_test.farm_id,
        deadline_cli_test.queue_id,
        "integ-test-job-cli",
    )
    deadline_client.get_waiter("job_create_complete").wait(
        farmId=deadline_cli_test.farm_id,
        queueId=deadline_cli_test.queue_id,
        jobId=job_id,
    )
    return job_id


# ---------------------------------------------------------------------------
# job list
#
# These tests depend on submitted_job_id (and completed_job_id for page_size)
# to ensure jobs exist in the queue before listing.
# ---------------------------------------------------------------------------


def test_job_list(deadline_cli_test: DeadlineCliTest, submitted_job_id: str) -> None:
    """Verify `deadline job list` returns jobs with expected output format."""
    runner = CliRunner()

    result = runner.invoke(
        main,
        [
            "job",
            "list",
            "--farm-id",
            deadline_cli_test.farm_id,
            "--queue-id",
            deadline_cli_test.queue_id,
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Jobs starting at 0" in result.output
    assert "jobId:" in result.output
    assert "name:" in result.output or "displayName:" in result.output
    assert "createdAt:" in result.output


def test_job_list_page_size(
    deadline_cli_test: DeadlineCliTest, submitted_job_id: str, completed_job_id: str
) -> None:
    """Verify `--page-size` limits the number of jobs returned."""
    runner = CliRunner()

    result = runner.invoke(
        main,
        [
            "job",
            "list",
            "--farm-id",
            deadline_cli_test.farm_id,
            "--queue-id",
            deadline_cli_test.queue_id,
            "--page-size",
            "2",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Displaying 2 of" in result.output


# ---------------------------------------------------------------------------
# job get
# ---------------------------------------------------------------------------


def test_job_get(
    deadline_cli_test: DeadlineCliTest,
    submitted_job_id: str,
) -> None:
    """Verify `deadline job get` returns details for a specific job."""
    runner = CliRunner()

    result = runner.invoke(
        main,
        [
            "job",
            "get",
            "--farm-id",
            deadline_cli_test.farm_id,
            "--queue-id",
            deadline_cli_test.queue_id,
            "--job-id",
            submitted_job_id,
        ],
    )

    assert result.exit_code == 0, result.output
    assert f"jobId: {submitted_job_id}" in result.output
    assert "name: integ-test-job-cli" in result.output
    assert "lifecycleStatus: CREATE_COMPLETE" in result.output
    assert "priority: 50" in result.output
    assert "taskRunStatus: SUSPENDED" in result.output
    assert "taskRunStatusCounts:" in result.output
    assert "createdAt:" in result.output
    assert "createdBy:" in result.output


# ---------------------------------------------------------------------------
# job cancel
# ---------------------------------------------------------------------------


@pytest.fixture()
def cancelable_job_id(deadline_cli_test: DeadlineCliTest, deadline_client) -> str:
    """
    Submit a fresh job for the cancel test.

    Each cancel test gets its own job so we don't interfere with other tests.
    Submitted as SUSPENDED so it doesn't consume fleet capacity.
    Waits for creation to complete.
    """
    job_id = _submit_job(
        deadline_client,
        deadline_cli_test.farm_id,
        deadline_cli_test.queue_id,
        "integ-test-cancel-target",
    )
    deadline_client.get_waiter("job_create_complete").wait(
        farmId=deadline_cli_test.farm_id,
        queueId=deadline_cli_test.queue_id,
        jobId=job_id,
    )
    return job_id


def test_job_cancel(
    deadline_cli_test: DeadlineCliTest,
    deadline_client,
    cancelable_job_id: str,
) -> None:
    """Verify `deadline job cancel --yes` cancels a job."""
    runner = CliRunner()

    result = runner.invoke(
        main,
        [
            "job",
            "cancel",
            "--farm-id",
            deadline_cli_test.farm_id,
            "--queue-id",
            deadline_cli_test.queue_id,
            "--job-id",
            cancelable_job_id,
            # --yes is required to skip the interactive confirmation prompt
            "--yes",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Canceling job" in result.output

    # Poll until the cancel update completes.
    _wait_for_job_update_succeeded(
        deadline_client,
        deadline_cli_test.farm_id,
        deadline_cli_test.queue_id,
        cancelable_job_id,
    )
    job = deadline_client.get_job(
        farmId=deadline_cli_test.farm_id,
        queueId=deadline_cli_test.queue_id,
        jobId=cancelable_job_id,
    )
    assert job["taskRunStatus"] == "CANCELED", (
        f"Expected taskRunStatus CANCELED, got {job['taskRunStatus']}"
    )


# ---------------------------------------------------------------------------
# job trace-schedule
#
# trace-schedule requires a job that has actually started (has sessions).
# A SUSPENDED job won't work. We submit as READY and wait for completion.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def completed_job_id(deadline_cli_test: DeadlineCliTest, deadline_client) -> str:
    """
    Submit a job as READY and wait for it to complete.

    Needed for trace-schedule which requires a job with session history.
    """
    job_id = _submit_job(
        deadline_client,
        deadline_cli_test.farm_id,
        deadline_cli_test.queue_id,
        "integ-test-trace-schedule",
        target_task_run_status="READY",
    )
    deadline_client.get_waiter("job_succeeded").wait(
        farmId=deadline_cli_test.farm_id,
        queueId=deadline_cli_test.queue_id,
        jobId=job_id,
    )
    return job_id


def test_job_trace_schedule(
    deadline_cli_test: DeadlineCliTest,
    completed_job_id: str,
) -> None:
    """Verify `deadline job trace-schedule` produces a summary for a completed job."""
    runner = CliRunner()

    result = runner.invoke(
        main,
        [
            "job",
            "trace-schedule",
            "--farm-id",
            deadline_cli_test.farm_id,
            "--queue-id",
            deadline_cli_test.queue_id,
            "--job-id",
            completed_job_id,
        ],
    )

    assert result.exit_code == 0, result.output
    assert "SUMMARY" in result.output
    assert "Session Count:" in result.output
    assert "Task Run Count:" in result.output
