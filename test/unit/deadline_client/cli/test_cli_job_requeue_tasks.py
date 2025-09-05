# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the CLI job commands.
"""

import pytest
from unittest.mock import patch, call

import click
from click.testing import CliRunner

from deadline.client.cli import main
from ..shared_constants import (
    MOCK_FARM_ID,
    MOCK_JOB_ID,
    MOCK_QUEUE_ID,
    MOCK_STEP_ID,
)


MOCK_TASK_ID_PREFIX = MOCK_STEP_ID.replace("step-", "task-")


def add_mocks_for_job_requeue_tasks(deadline_mock):
    """
    Adds mock return values to the deadline_mock for sharing across
    the different 'deadline job requeue-tasks' tests.
    """
    # These mock returns only contain the properties that requeue-tasks needs
    deadline_mock.get_job.return_value = {
        "jobId": MOCK_JOB_ID,
        "name": "Mock Job",
        "taskRunStatus": "RUNNING",
        "taskRunStatusCounts": {
            "SUCCEEDED": 1,
            "SUSPENDED": 1,
            "CANCELED": 1,
            "FAILED": 1,
            "NOT_COMPATIBLE": 1,
            "RUNNING": 1,
        },
    }
    deadline_mock.list_steps.return_value = {
        "steps": [
            {
                "stepId": MOCK_STEP_ID,
                "name": "Step Name",
                "taskRunStatus": "RUNNING",
                "taskRunStatusCounts": {
                    "SUCCEEDED": 1,
                    "SUSPENDED": 1,
                    "CANCELED": 1,
                    "FAILED": 1,
                    "NOT_COMPATIBLE": 1,
                    "RUNNING": 1,
                },
            }
        ]
    }
    deadline_mock.list_tasks.return_value = {
        "tasks": [
            {
                "taskId": f"{MOCK_TASK_ID_PREFIX}-0",
                "runStatus": "SUCCEEDED",
                "parameters": {"TestCase": {"string": "SUCCEEDED task"}},
            },
            {
                "taskId": f"{MOCK_TASK_ID_PREFIX}-1",
                "runStatus": "SUSPENDED",
                "parameters": {"TestCase": {"string": "SUSPENDED task"}},
            },
            {
                "taskId": f"{MOCK_TASK_ID_PREFIX}-2",
                "runStatus": "CANCELED",
                "parameters": {"TestCase": {"string": "CANCELED task"}},
            },
            {
                "taskId": f"{MOCK_TASK_ID_PREFIX}-3",
                "runStatus": "FAILED",
                "parameters": {"TestCase": {"string": "FAILED task"}},
            },
            {
                "taskId": f"{MOCK_TASK_ID_PREFIX}-4",
                "runStatus": "NOT_COMPATIBLE",
                "parameters": {"TestCase": {"string": "NOT_COMPATIBLE task"}},
            },
            {
                "taskId": f"{MOCK_TASK_ID_PREFIX}-5",
                "runStatus": "RUNNING",
                "parameters": {"TestCase": {"string": "RUNNING task"}},
            },
        ]
    }
    deadline_mock.update_task.return_value = {}


def test_cli_job_requeue_tasks(fresh_deadline_config, deadline_mock):
    """
    Tests that 'deadline job requeue-tasks' requeues all the tasks of the correct run status.
    """
    add_mocks_for_job_requeue_tasks(deadline_mock)

    with patch.object(click, "confirm") as mock_confirm:
        mock_confirm.return_value = True
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "requeue-tasks",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--job-id",
                MOCK_JOB_ID,
            ],
        )

    assert (
        result.output
        == f"""Job: Mock Job ({MOCK_JOB_ID})
taskRunStatusCounts:
  SUCCEEDED: 1
  SUSPENDED: 1
  CANCELED: 1
  FAILED: 1
  NOT_COMPATIBLE: 1
  RUNNING: 1

Requeuing all tasks with run status among: CANCELED, FAILED, SUSPENDED
This action will requeue an estimated 3 total tasks (1 SUSPENDED tasks, 1 CANCELED tasks, 1 FAILED tasks)
Requeuing tasks...

Step: Step Name ({MOCK_STEP_ID})
  Requeuing an estimated 3 total tasks (1 SUSPENDED tasks, 1 CANCELED tasks, 1 FAILED tasks)...
    SUSPENDED TestCase=SUSPENDED task ({MOCK_TASK_ID_PREFIX}-1)
    CANCELED TestCase=CANCELED task ({MOCK_TASK_ID_PREFIX}-2)
    FAILED TestCase=FAILED task ({MOCK_TASK_ID_PREFIX}-3)

Requeued a total of 3 tasks.
"""
    )
    mock_confirm.assert_called_once_with(
        "Are you sure you want to requeue these tasks?", default=None
    )
    assert deadline_mock.update_task.call_args_list == [
        call(
            farmId=MOCK_FARM_ID,
            queueId=MOCK_QUEUE_ID,
            jobId=MOCK_JOB_ID,
            stepId=MOCK_STEP_ID,
            taskId=f"{MOCK_TASK_ID_PREFIX}-1",
            targetRunStatus="PENDING",
        ),
        call(
            farmId=MOCK_FARM_ID,
            queueId=MOCK_QUEUE_ID,
            jobId=MOCK_JOB_ID,
            stepId=MOCK_STEP_ID,
            taskId=f"{MOCK_TASK_ID_PREFIX}-2",
            targetRunStatus="PENDING",
        ),
        call(
            farmId=MOCK_FARM_ID,
            queueId=MOCK_QUEUE_ID,
            jobId=MOCK_JOB_ID,
            stepId=MOCK_STEP_ID,
            taskId=f"{MOCK_TASK_ID_PREFIX}-3",
            targetRunStatus="PENDING",
        ),
    ]


def test_cli_job_requeue_tasks_user_says_no(fresh_deadline_config, deadline_mock):
    """
    Tests that 'deadline job requeue-tasks' requeues nothing if the user says "no" to the confirmation prompt.
    """
    add_mocks_for_job_requeue_tasks(deadline_mock)

    with patch.object(click, "confirm") as mock_confirm:
        mock_confirm.return_value = False
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "requeue-tasks",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--job-id",
                MOCK_JOB_ID,
            ],
        )

    assert (
        result.output
        == f"""Job: Mock Job ({MOCK_JOB_ID})
taskRunStatusCounts:
  SUCCEEDED: 1
  SUSPENDED: 1
  CANCELED: 1
  FAILED: 1
  NOT_COMPATIBLE: 1
  RUNNING: 1

Requeuing all tasks with run status among: CANCELED, FAILED, SUSPENDED
This action will requeue an estimated 3 total tasks (1 SUSPENDED tasks, 1 CANCELED tasks, 1 FAILED tasks)
No tasks were requeued.
"""
    )
    mock_confirm.assert_called_once_with(
        "Are you sure you want to requeue these tasks?", default=None
    )
    assert deadline_mock.update_task.call_args_list == []


@pytest.mark.parametrize(
    "run_status,task_id",
    [
        ("SUCCEEDED", f"{MOCK_TASK_ID_PREFIX}-0"),
        ("SUSPENDED", f"{MOCK_TASK_ID_PREFIX}-1"),
        ("CANCELED", f"{MOCK_TASK_ID_PREFIX}-2"),
        ("FAILED", f"{MOCK_TASK_ID_PREFIX}-3"),
        ("NOT_COMPATIBLE", f"{MOCK_TASK_ID_PREFIX}-4"),
    ],
)
def test_cli_job_requeue_tasks_of_each_status(
    run_status, task_id, fresh_deadline_config, deadline_mock
):
    """
    Tests that 'deadline job requeue-tasks' requeues the one task of each selected run status.
    """
    add_mocks_for_job_requeue_tasks(deadline_mock)

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "job",
            "requeue-tasks",
            "--farm-id",
            MOCK_FARM_ID,
            "--queue-id",
            MOCK_QUEUE_ID,
            "--job-id",
            MOCK_JOB_ID,
            "--yes",
            "--run-status",
            run_status,
        ],
    )

    assert (
        result.output
        == f"""Job: Mock Job ({MOCK_JOB_ID})
taskRunStatusCounts:
  SUCCEEDED: 1
  SUSPENDED: 1
  CANCELED: 1
  FAILED: 1
  NOT_COMPATIBLE: 1
  RUNNING: 1

Requeuing all tasks with run status among: {run_status}
Estimated 1 total tasks (1 {run_status} tasks) to requeue.

Step: Step Name ({MOCK_STEP_ID})
  Requeuing an estimated 1 total tasks (1 {run_status} tasks)...
    {run_status} TestCase={run_status} task ({task_id})

Requeued a total of 1 tasks.
"""
    )
    assert deadline_mock.update_task.call_args_list == [
        call(
            farmId=MOCK_FARM_ID,
            queueId=MOCK_QUEUE_ID,
            jobId=MOCK_JOB_ID,
            stepId=MOCK_STEP_ID,
            taskId=task_id,
            targetRunStatus="PENDING",
        ),
    ]


def test_cli_job_requeue_tasks_multiple_statuses(fresh_deadline_config, deadline_mock):
    """
    Tests that 'deadline job requeue-tasks' requeues all statuses provided with repeated
    --run-status options.
    """
    add_mocks_for_job_requeue_tasks(deadline_mock)

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "job",
            "requeue-tasks",
            "--farm-id",
            MOCK_FARM_ID,
            "--queue-id",
            MOCK_QUEUE_ID,
            "--job-id",
            MOCK_JOB_ID,
            "--run-status",
            "CANCELED",
            "--yes",
            "--run-status",
            "FAILED",
        ],
    )

    assert (
        result.output
        == f"""Job: Mock Job ({MOCK_JOB_ID})
taskRunStatusCounts:
  SUCCEEDED: 1
  SUSPENDED: 1
  CANCELED: 1
  FAILED: 1
  NOT_COMPATIBLE: 1
  RUNNING: 1

Requeuing all tasks with run status among: CANCELED, FAILED
Estimated 2 total tasks (1 CANCELED tasks, 1 FAILED tasks) to requeue.

Step: Step Name ({MOCK_STEP_ID})
  Requeuing an estimated 2 total tasks (1 CANCELED tasks, 1 FAILED tasks)...
    CANCELED TestCase=CANCELED task ({MOCK_TASK_ID_PREFIX}-2)
    FAILED TestCase=FAILED task ({MOCK_TASK_ID_PREFIX}-3)

Requeued a total of 2 tasks.
"""
    )
    assert deadline_mock.update_task.call_args_list == [
        call(
            farmId=MOCK_FARM_ID,
            queueId=MOCK_QUEUE_ID,
            jobId=MOCK_JOB_ID,
            stepId=MOCK_STEP_ID,
            taskId=f"{MOCK_TASK_ID_PREFIX}-2",
            targetRunStatus="PENDING",
        ),
        call(
            farmId=MOCK_FARM_ID,
            queueId=MOCK_QUEUE_ID,
            jobId=MOCK_JOB_ID,
            stepId=MOCK_STEP_ID,
            taskId=f"{MOCK_TASK_ID_PREFIX}-3",
            targetRunStatus="PENDING",
        ),
    ]
