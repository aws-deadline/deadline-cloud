# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for trace-schedule using MockDeadlineBackend.
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from deadline.client import config
from deadline.client.cli import main
from ..mock_deadline_backend import MockDeadlineBackend

pytestmark = pytest.mark.skipif(
    sys.version_info < (3, 9), reason="openjd-model requires Python 3.9+"
)

SINGLE_TASK_TEMPLATE = """
specificationVersion: jobtemplate-2023-09
name: Test
steps:
  - name: Render
    script:
      actions:
        onRun:
          command: echo
          args: ["test"]
"""

SIX_TASK_TEMPLATE = """
specificationVersion: jobtemplate-2023-09
name: Test
steps:
  - name: Render
    parameterSpace:
      taskParameterDefinitions:
        - name: Frame
          type: INT
          range: "1-6"
    script:
      actions:
        onRun:
          command: echo
          args: ["{{Task.Param.Frame}}"]
"""

MULTI_PARAM_TEMPLATE = """
specificationVersion: jobtemplate-2023-09
name: Test
steps:
  - name: Render
    parameterSpace:
      taskParameterDefinitions:
        - name: Frame
          type: INT
          range: "42"
        - name: Camera
          type: STRING
          range: ["main"]
    script:
      actions:
        onRun:
          command: echo
          args: ["{{Task.Param.Frame}} {{Task.Param.Camera}}"]
"""

CHUNKED_TEMPLATE = """
specificationVersion: jobtemplate-2023-09
extensions:
  - TASK_CHUNKING
name: Test
steps:
  - name: RenderFrames
    parameterSpace:
      taskParameterDefinitions:
        - name: Frame
          type: CHUNK[INT]
          range: "1-30"
          chunks:
            defaultTaskCount: 3
            rangeConstraint: CONTIGUOUS
    script:
      actions:
        onRun:
          command: echo
          args: ["{{Task.Param.Frame}}"]
"""

JOB_ENV_TEMPLATE = """
specificationVersion: jobtemplate-2023-09
name: Test
jobEnvironments:
  - name: Conda
    variables:
      CONDA_ENV: test
steps:
  - name: Render
    script:
      actions:
        onRun:
          command: echo
          args: ["test"]
"""


class TestTraceScheduleWithMockBackend:
    """Tests for trace-schedule command using MockDeadlineBackend."""

    def setup_job(
        self, backend: MockDeadlineBackend, deadline_mock: MagicMock, template: str
    ) -> tuple[str, str, str]:
        """Create a farm/queue/job structure with given template."""
        farm = deadline_mock.create_farm(displayName="Test Farm")
        queue = deadline_mock.create_queue(
            farmId=farm["farmId"],
            displayName="Test Queue",
            jobAttachmentSettings={
                "s3BucketName": "test-bucket",
                "rootPrefix": "test-prefix",
            },
        )
        job = deadline_mock.create_job(
            farmId=farm["farmId"],
            queueId=queue["queueId"],
            template=template,
        )
        return farm["farmId"], queue["queueId"], job["jobId"]

    def get_step(
        self, backend: MockDeadlineBackend, farm_id: str, queue_id: str, job_id: str
    ) -> str:
        """Get the first step for a job."""
        step_key = next(k for k in backend.steps if k[:3] == (farm_id, queue_id, job_id))
        return step_key[3]

    def get_tasks_for_step(
        self, backend: MockDeadlineBackend, farm_id: str, queue_id: str, job_id: str, step_id: str
    ) -> list[str]:
        """Get all task IDs for a step."""
        task_keys = [k for k in backend.tasks if k[:4] == (farm_id, queue_id, job_id, step_id)]
        return [k[4] for k in task_keys]

    def test_single_session_single_task(self, fresh_deadline_config: str, deadline_mock: MagicMock):
        """Test trace-schedule with minimal data: 1 session, 1 task."""
        backend = MockDeadlineBackend()
        backend.set_mock_methods(deadline_mock)

        farm_id, queue_id, job_id = self.setup_job(backend, deadline_mock, SINGLE_TASK_TEMPLATE)
        config.set_setting("defaults.farm_id", farm_id)
        config.set_setting("defaults.queue_id", queue_id)

        step_id = self.get_step(backend, farm_id, queue_id, job_id)
        backend.simulate_task_runs(job_id, step_id)

        runner = CliRunner()
        result = runner.invoke(main, ["job", "trace-schedule", "--job-id", job_id])

        assert result.exit_code == 0
        assert "Session Count: 1" in result.output
        assert "Task Run Count: 1" in result.output

    def test_multiple_sessions_multiple_workers(
        self, fresh_deadline_config: str, deadline_mock: MagicMock
    ):
        """Test trace-schedule with multiple sessions across workers."""
        backend = MockDeadlineBackend()
        backend.set_mock_methods(deadline_mock)

        farm_id, queue_id, job_id = self.setup_job(backend, deadline_mock, SIX_TASK_TEMPLATE)
        config.set_setting("defaults.farm_id", farm_id)
        config.set_setting("defaults.queue_id", queue_id)

        step_id = self.get_step(backend, farm_id, queue_id, job_id)
        task_ids = self.get_tasks_for_step(backend, farm_id, queue_id, job_id, step_id)

        # 3 workers, each runs 2 tasks
        backend.simulate_task_runs(job_id, step_id, task_ids[0:2], worker_id="worker-0")
        backend.simulate_task_runs(job_id, step_id, task_ids[2:4], worker_id="worker-1")
        backend.simulate_task_runs(job_id, step_id, task_ids[4:6], worker_id="worker-2")

        runner = CliRunner()
        result = runner.invoke(main, ["job", "trace-schedule", "--job-id", job_id])

        assert result.exit_code == 0
        assert "Session Count: 3" in result.output
        assert "Task Run Count: 6" in result.output

    def test_with_env_actions(self, fresh_deadline_config: str, deadline_mock: MagicMock):
        """Test trace-schedule with environment enter/exit actions."""
        backend = MockDeadlineBackend()
        backend.set_mock_methods(deadline_mock)

        farm_id, queue_id, job_id = self.setup_job(backend, deadline_mock, JOB_ENV_TEMPLATE)
        config.set_setting("defaults.farm_id", farm_id)
        config.set_setting("defaults.queue_id", queue_id)

        step_id = self.get_step(backend, farm_id, queue_id, job_id)
        backend.simulate_task_runs(job_id, step_id)

        runner = CliRunner()
        result = runner.invoke(main, ["job", "trace-schedule", "--job-id", job_id])

        assert result.exit_code == 0
        assert "Session Count: 1" in result.output
        assert "Task Run Count: 1" in result.output
        assert "Env Action Count: 2" in result.output

    def test_with_task_parameters(self, fresh_deadline_config: str, deadline_mock: MagicMock):
        """Test trace-schedule displays task parameters correctly."""
        backend = MockDeadlineBackend()
        backend.set_mock_methods(deadline_mock)

        farm_id, queue_id, job_id = self.setup_job(backend, deadline_mock, MULTI_PARAM_TEMPLATE)
        config.set_setting("defaults.farm_id", farm_id)
        config.set_setting("defaults.queue_id", queue_id)

        step_id = self.get_step(backend, farm_id, queue_id, job_id)
        backend.simulate_task_runs(job_id, step_id)

        runner = CliRunner()
        result = runner.invoke(main, ["job", "trace-schedule", "--job-id", job_id])

        assert result.exit_code == 0
        assert "Task Run Count: 1" in result.output

    def test_chunked_task_parameters(self, fresh_deadline_config: str, deadline_mock: MagicMock):
        """Test trace-schedule with chunked task parameters (frame ranges)."""
        backend = MockDeadlineBackend()
        backend.set_mock_methods(deadline_mock)

        farm_id, queue_id, job_id = self.setup_job(backend, deadline_mock, CHUNKED_TEMPLATE)
        config.set_setting("defaults.farm_id", farm_id)
        config.set_setting("defaults.queue_id", queue_id)

        step_id = self.get_step(backend, farm_id, queue_id, job_id)

        # Verify chunked tasks were created with chunkInt parameters
        task_key = next(k for k in backend.tasks if k[3] == step_id)
        task = backend.tasks[task_key]
        assert "chunkInt" in task["parameters"]["Frame"]

        task_ids = self.get_tasks_for_step(backend, farm_id, queue_id, job_id, step_id)
        backend.simulate_task_runs(job_id, step_id, task_ids[:3])

        runner = CliRunner()
        result = runner.invoke(main, ["job", "trace-schedule", "--job-id", job_id])

        assert result.exit_code == 0
        assert "Session Count: 1" in result.output
        assert "Task Run Count: 3" in result.output

    def test_durations_reported(self, fresh_deadline_config: str, deadline_mock: MagicMock):
        """Test trace-schedule reports durations correctly."""
        backend = MockDeadlineBackend()
        backend.set_mock_methods(deadline_mock)

        farm_id, queue_id, job_id = self.setup_job(backend, deadline_mock, JOB_ENV_TEMPLATE)
        config.set_setting("defaults.farm_id", farm_id)
        config.set_setting("defaults.queue_id", queue_id)

        step_id = self.get_step(backend, farm_id, queue_id, job_id)
        # 1 task @ 60s, 1 env enter @ 5s, 1 env exit @ 5s = 70s total actions
        backend.simulate_task_runs(job_id, step_id, duration_seconds=60, env_duration_seconds=5)

        runner = CliRunner()
        result = runner.invoke(main, ["job", "trace-schedule", "--job-id", job_id])

        assert result.exit_code == 0
        # Session duration = 70s (1:10)
        assert "Session Total Duration: 0:01:10" in result.output
        # 3 actions total (1 env enter + 1 task + 1 env exit)
        assert "Session Action Count: 3" in result.output
        assert "Session Action Total Duration: 0:01:10" in result.output
        # Task run = 60s
        assert "Task Run Count: 1" in result.output
        assert "Task Run Total Duration: 0:01:00" in result.output
        # Env actions = 10s (5s enter + 5s exit)
        assert "Env Action Count: 2" in result.output
        assert "Env Action Total Duration: 0:00:10" in result.output

    def test_overhead_reported(self, fresh_deadline_config: str, deadline_mock: MagicMock):
        """Test trace-schedule reports within-session overhead correctly."""
        backend = MockDeadlineBackend()
        backend.set_mock_methods(deadline_mock)

        farm_id, queue_id, job_id = self.setup_job(backend, deadline_mock, SINGLE_TASK_TEMPLATE)
        config.set_setting("defaults.farm_id", farm_id)
        config.set_setting("defaults.queue_id", queue_id)

        step_id = self.get_step(backend, farm_id, queue_id, job_id)
        # 1 task @ 40s with 20s overhead at start = 60s session, 40s actions
        backend.simulate_task_runs(job_id, step_id, duration_seconds=40, overhead_seconds=20)

        runner = CliRunner()
        result = runner.invoke(main, ["job", "trace-schedule", "--job-id", job_id])

        assert result.exit_code == 0
        assert "Session Total Duration: 0:01:00" in result.output
        assert "Session Action Total Duration: 0:00:40" in result.output
        assert "Within-session Overhead Duration: 0:00:20" in result.output
        assert "Within-session Overhead Duration Per Action: 0:00:20" in result.output

    def test_zero_counts_reported(self, fresh_deadline_config: str, deadline_mock: MagicMock):
        """Test trace-schedule reports zero counts for unused action types."""
        backend = MockDeadlineBackend()
        backend.set_mock_methods(deadline_mock)

        farm_id, queue_id, job_id = self.setup_job(backend, deadline_mock, SINGLE_TASK_TEMPLATE)
        config.set_setting("defaults.farm_id", farm_id)
        config.set_setting("defaults.queue_id", queue_id)

        step_id = self.get_step(backend, farm_id, queue_id, job_id)
        backend.simulate_task_runs(job_id, step_id)

        runner = CliRunner()
        result = runner.invoke(main, ["job", "trace-schedule", "--job-id", job_id])

        assert result.exit_code == 0
        # No env actions in SINGLE_TASK_TEMPLATE
        assert "Env Action Count: 0" in result.output
        assert "Env Action Total Duration: 0:00:00" in result.output
        # No sync job attachments
        assert "Sync Job Attachments Count: 0" in result.output
        assert "Sync Job Attachments Total Duration: 0:00:00" in result.output
        # No non-task runs
        assert "Non-Task Run Count: 0" in result.output
        assert "Non-Task Run Total Duration: 0:00:00" in result.output


class TestTraceScheduleBatchBehavior:
    """Tests that exercise the batch-get integration specifically."""

    def _setup(self, deadline_mock: MagicMock, template: str):
        backend = MockDeadlineBackend()
        backend.set_mock_methods(deadline_mock)
        helper = TestTraceScheduleWithMockBackend()
        farm_id, queue_id, job_id = helper.setup_job(backend, deadline_mock, template)
        config.set_setting("defaults.farm_id", farm_id)
        config.set_setting("defaults.queue_id", queue_id)
        return backend, helper, farm_id, queue_id, job_id

    def test_uses_batch_get_task_and_batch_get_step(
        self, fresh_deadline_config: str, deadline_mock: MagicMock
    ):
        backend, helper, farm_id, queue_id, job_id = self._setup(deadline_mock, SIX_TASK_TEMPLATE)
        step_id = helper.get_step(backend, farm_id, queue_id, job_id)
        task_ids = helper.get_tasks_for_step(backend, farm_id, queue_id, job_id, step_id)
        # 2 sessions of 3 tasks each so there are multiple unique tasks.
        backend.simulate_task_runs(job_id, step_id, task_ids[0:3], worker_id="worker-0")
        backend.simulate_task_runs(job_id, step_id, task_ids[3:6], worker_id="worker-1")

        runner = CliRunner()
        result = runner.invoke(main, ["job", "trace-schedule", "--job-id", job_id])

        assert result.exit_code == 0, result.output
        # Single batch each is enough for 1 step and 6 tasks.
        assert deadline_mock.batch_get_step.call_count == 1
        assert deadline_mock.batch_get_task.call_count == 1
        # Legacy per-item calls must NOT be used.
        assert deadline_mock.get_step.call_count == 0
        assert deadline_mock.get_task.call_count == 0

    def test_batch_chunks_tasks_over_100(
        self, fresh_deadline_config: str, deadline_mock: MagicMock
    ):
        # 150 tasks => 2 batches (100 + 50).
        template = """
specificationVersion: jobtemplate-2023-09
name: Test
steps:
  - name: Render
    parameterSpace:
      taskParameterDefinitions:
        - name: Frame
          type: INT
          range: "1-150"
    script:
      actions:
        onRun:
          command: echo
          args: ["{{Task.Param.Frame}}"]
"""
        backend, helper, farm_id, queue_id, job_id = self._setup(deadline_mock, template)
        step_id = helper.get_step(backend, farm_id, queue_id, job_id)
        backend.simulate_task_runs(job_id, step_id)  # all 150 tasks

        runner = CliRunner()
        result = runner.invoke(main, ["job", "trace-schedule", "--job-id", job_id])

        assert result.exit_code == 0, result.output
        assert deadline_mock.batch_get_task.call_count == 2
        sizes = [len(c.kwargs["identifiers"]) for c in deadline_mock.batch_get_task.call_args_list]
        assert sorted(sizes) == [50, 100]
        assert "Task Run Count: 150" in result.output

    def test_transient_error_is_retried_and_trace_succeeds(
        self, fresh_deadline_config: str, deadline_mock: MagicMock
    ):
        backend, helper, farm_id, queue_id, job_id = self._setup(deadline_mock, SIX_TASK_TEMPLATE)
        step_id = helper.get_step(backend, farm_id, queue_id, job_id)
        task_ids = helper.get_tasks_for_step(backend, farm_id, queue_id, job_id, step_id)
        backend.simulate_task_runs(job_id, step_id, task_ids)

        # One task throttles once, then succeeds.
        backend.inject_batch_failure(
            "BatchGetTask",
            {
                "farmId": farm_id,
                "queueId": queue_id,
                "jobId": job_id,
                "stepId": step_id,
                "taskId": task_ids[2],
            },
            code="ThrottlingException",
            attempts=1,
        )

        runner = CliRunner()
        # Patch time.sleep via the batch_get module to avoid slowing the test.
        from deadline.client.cli._groups import _batch_get as bg

        original_sleep = bg.time.sleep
        bg.time.sleep = lambda _s: None
        try:
            result = runner.invoke(main, ["job", "trace-schedule", "--job-id", job_id])
        finally:
            bg.time.sleep = original_sleep

        assert result.exit_code == 0, result.output
        assert "Task Run Count: 6" in result.output
        # Two total calls: initial plus one retry for the injected failure.
        assert deadline_mock.batch_get_task.call_count == 2

    def test_terminal_error_warns_and_continues(
        self, fresh_deadline_config: str, deadline_mock: MagicMock
    ):
        backend, helper, farm_id, queue_id, job_id = self._setup(deadline_mock, SIX_TASK_TEMPLATE)
        step_id = helper.get_step(backend, farm_id, queue_id, job_id)
        task_ids = helper.get_tasks_for_step(backend, farm_id, queue_id, job_id, step_id)
        backend.simulate_task_runs(job_id, step_id, task_ids)

        # One task is terminally missing.
        backend.inject_batch_failure(
            "BatchGetTask",
            {
                "farmId": farm_id,
                "queueId": queue_id,
                "jobId": job_id,
                "stepId": step_id,
                "taskId": task_ids[0],
            },
            code="ResourceNotFoundException",
            attempts=10,  # Effectively permanent.
        )

        runner = CliRunner()
        result = runner.invoke(main, ["job", "trace-schedule", "--job-id", job_id])

        assert result.exit_code == 0, result.output
        # The warning goes to stderr; click's CliRunner mixes it with .output
        # by default.
        assert "could not retrieve 1 task" in result.output
        # All 6 task-run actions are still counted (the missing task just
        # gets "<No Task Params>" as its event name).
        assert "Task Run Count: 6" in result.output
