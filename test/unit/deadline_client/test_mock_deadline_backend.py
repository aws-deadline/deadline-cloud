# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for MockDeadlineBackend.
"""

import sys

import pytest
from botocore.exceptions import ClientError

from .mock_deadline_backend import MockDeadlineBackend

pytestmark = pytest.mark.skipif(
    sys.version_info < (3, 9), reason="openjd-model requires Python 3.9+"
)

MINIMAL_TEMPLATE = """
specificationVersion: jobtemplate-2023-09
name: Test
steps:
  - name: Step
    script:
      actions:
        onRun:
          command: echo
          args: ["test"]
"""


class TestMockDeadlineBackend:
    """Tests for MockDeadlineBackend basic functionality."""

    def test_create_and_get_farm(self):
        backend = MockDeadlineBackend()
        result = backend.create_farm(displayName="Test Farm")

        assert "farmId" in result
        assert result["farmId"].startswith("farm-")

        farm = backend.get_farm(farmId=result["farmId"])
        assert farm["displayName"] == "Test Farm"

    def test_create_and_get_queue(self):
        backend = MockDeadlineBackend()
        farm = backend.create_farm(displayName="Test Farm")
        result = backend.create_queue(farmId=farm["farmId"], displayName="Test Queue")

        assert "queueId" in result
        assert result["queueId"].startswith("queue-")

        queue = backend.get_queue(farmId=farm["farmId"], queueId=result["queueId"])
        assert queue["displayName"] == "Test Queue"

    def test_create_and_get_job(self):
        backend = MockDeadlineBackend()
        farm = backend.create_farm(displayName="Test Farm")
        queue = backend.create_queue(farmId=farm["farmId"], displayName="Test Queue")
        result = backend.create_job(
            farmId=farm["farmId"],
            queueId=queue["queueId"],
            template=MINIMAL_TEMPLATE,
            nameOverride="Test Job",
        )

        assert "jobId" in result
        assert result["jobId"].startswith("job-")

        job = backend.get_job(
            farmId=farm["farmId"],
            queueId=queue["queueId"],
            jobId=result["jobId"],
        )
        assert job["name"] == "Test Job"

    def test_get_nonexistent_farm_raises_error(self):
        backend = MockDeadlineBackend()

        with pytest.raises(ClientError) as exc_info:
            backend.get_farm(farmId="farm-00000000000000000000000000000000")

        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"
        assert "farm" in exc_info.value.response["Error"]["Message"]

    def test_get_nonexistent_job_raises_error(self):
        backend = MockDeadlineBackend()
        farm = backend.create_farm(displayName="Test Farm")
        queue = backend.create_queue(farmId=farm["farmId"], displayName="Test Queue")

        with pytest.raises(ClientError) as exc_info:
            backend.get_job(
                farmId=farm["farmId"],
                queueId=queue["queueId"],
                jobId="job-00000000000000000000000000000000",
            )

        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_parameter_validation_catches_invalid_type(self):
        backend = MockDeadlineBackend(validate_params=True)

        with pytest.raises(ValueError) as exc_info:
            backend.create_job(
                farmId="farm-123",
                queueId="queue-456",
                template=123,  # type: ignore[arg-type]  # Intentionally wrong type
            )

        assert "CreateJob" in str(exc_info.value)

    def test_parameter_validation_can_be_disabled(self):
        backend = MockDeadlineBackend(validate_params=False)
        # Should not raise even with invalid params
        backend.create_job(
            farmId="invalid",
            queueId="invalid",
            template=MINIMAL_TEMPLATE,
        )

    def test_list_sessions_empty(self):
        backend = MockDeadlineBackend()
        farm = backend.create_farm(displayName="Test Farm")
        queue = backend.create_queue(farmId=farm["farmId"], displayName="Test Queue")
        job = backend.create_job(
            farmId=farm["farmId"],
            queueId=queue["queueId"],
            template=MINIMAL_TEMPLATE,
        )

        result = backend.list_sessions(
            farmId=farm["farmId"],
            queueId=queue["queueId"],
            jobId=job["jobId"],
        )

        assert result["sessions"] == []

    def test_simulate_task_run_creates_session(self):
        backend = MockDeadlineBackend()
        farm = backend.create_farm(displayName="Test Farm")
        queue = backend.create_queue(farmId=farm["farmId"], displayName="Test Queue")
        job = backend.create_job(
            farmId=farm["farmId"],
            queueId=queue["queueId"],
            template=MINIMAL_TEMPLATE,
        )

        step_id = list(backend.steps.values())[0]["stepId"]
        backend.simulate_task_runs(job["jobId"], step_id, worker_id="worker-1")

        result = backend.list_sessions(
            farmId=farm["farmId"],
            queueId=queue["queueId"],
            jobId=job["jobId"],
        )

        assert len(result["sessions"]) == 1
        assert result["sessions"][0]["workerId"] == "worker-1"

    def test_search_jobs(self):
        backend = MockDeadlineBackend()
        farm = backend.create_farm(displayName="Test Farm")
        queue = backend.create_queue(farmId=farm["farmId"], displayName="Test Queue")

        # Create multiple jobs
        for i in range(3):
            backend.create_job(
                farmId=farm["farmId"],
                queueId=queue["queueId"],
                template=MINIMAL_TEMPLATE,
                nameOverride=f"Job {i}",
            )

        result = backend.search_jobs(
            farmId=farm["farmId"],
            queueIds=[queue["queueId"]],
        )

        assert result["totalResults"] == 3
        assert len(result["jobs"]) == 3

    def test_create_job_invalid_template_raises_validation_error(self):
        backend = MockDeadlineBackend()
        farm = backend.create_farm(displayName="Test Farm")
        queue = backend.create_queue(farmId=farm["farmId"], displayName="Test Queue")

        with pytest.raises(ClientError) as exc_info:
            backend.create_job(
                farmId=farm["farmId"],
                queueId=queue["queueId"],
                template="{}",  # Invalid - missing required fields
            )

        assert exc_info.value.response["Error"]["Code"] == "ValidationException"


class TestMockDeadlineBackendTemplateParser:
    """Tests for job template parsing functionality."""

    def test_create_job_parses_template_name(self):
        backend = MockDeadlineBackend()
        farm = backend.create_farm(displayName="Test Farm")
        queue = backend.create_queue(farmId=farm["farmId"], displayName="Test Queue")

        template = """
specificationVersion: jobtemplate-2023-09
name: Template Job Name
steps:
  - name: Step1
    script:
      actions:
        onRun:
          command: echo
          args: ["hello"]
"""
        job = backend.create_job(
            farmId=farm["farmId"],
            queueId=queue["queueId"],
            template=template,
        )

        job_data = backend.get_job(
            farmId=farm["farmId"],
            queueId=queue["queueId"],
            jobId=job["jobId"],
        )
        assert job_data["name"] == "Template Job Name"

    def test_create_job_creates_steps(self):
        backend = MockDeadlineBackend()
        farm = backend.create_farm(displayName="Test Farm")
        queue = backend.create_queue(farmId=farm["farmId"], displayName="Test Queue")

        template = """
specificationVersion: jobtemplate-2023-09
name: Test
steps:
  - name: RenderStep
    script:
      actions:
        onRun:
          command: echo
          args: ["hello"]
"""
        backend.create_job(
            farmId=farm["farmId"],
            queueId=queue["queueId"],
            template=template,
        )

        assert len(backend.steps) == 1
        step = list(backend.steps.values())[0]
        assert step["name"] == "RenderStep"

    def test_create_job_creates_tasks_from_parameter_space(self):
        backend = MockDeadlineBackend()
        farm = backend.create_farm(displayName="Test Farm")
        queue = backend.create_queue(farmId=farm["farmId"], displayName="Test Queue")

        template = """
specificationVersion: jobtemplate-2023-09
name: Test
steps:
  - name: Render
    parameterSpace:
      taskParameterDefinitions:
        - name: Frame
          type: INT
          range: "1-5"
    script:
      actions:
        onRun:
          command: echo
          args: ["{{Task.Param.Frame}}"]
"""
        backend.create_job(
            farmId=farm["farmId"],
            queueId=queue["queueId"],
            template=template,
        )

        assert len(backend.tasks) == 5
        # Check task parameters
        task_frames = [t["parameters"]["Frame"]["int"] for t in backend.tasks.values()]
        assert sorted(task_frames) == ["1", "2", "3", "4", "5"]

    def test_create_job_with_multi_dimensional_parameter_space(self):
        backend = MockDeadlineBackend()
        farm = backend.create_farm(displayName="Test Farm")
        queue = backend.create_queue(farmId=farm["farmId"], displayName="Test Queue")

        template = """
specificationVersion: jobtemplate-2023-09
name: Test
steps:
  - name: Render
    parameterSpace:
      taskParameterDefinitions:
        - name: Frame
          type: INT
          range: "1-2"
        - name: Layer
          type: INT
          range: "1-3"
    script:
      actions:
        onRun:
          command: echo
          args: ["{{Task.Param.Frame}}-{{Task.Param.Layer}}"]
"""
        backend.create_job(
            farmId=farm["farmId"],
            queueId=queue["queueId"],
            template=template,
        )

        # 2 frames * 3 layers = 6 tasks
        assert len(backend.tasks) == 6

    def test_create_job_creates_one_task_without_parameter_space(self):
        backend = MockDeadlineBackend()
        farm = backend.create_farm(displayName="Test Farm")
        queue = backend.create_queue(farmId=farm["farmId"], displayName="Test Queue")

        template = """
specificationVersion: jobtemplate-2023-09
name: Test
steps:
  - name: SingleTask
    script:
      actions:
        onRun:
          command: echo
          args: ["hello"]
"""
        backend.create_job(
            farmId=farm["farmId"],
            queueId=queue["queueId"],
            template=template,
        )

        # Should create exactly 1 task even without parameter space
        assert len(backend.tasks) == 1
        task = list(backend.tasks.values())[0]
        assert task["parameters"] == {}

    def test_name_override_takes_precedence(self):
        backend = MockDeadlineBackend()
        farm = backend.create_farm(displayName="Test Farm")
        queue = backend.create_queue(farmId=farm["farmId"], displayName="Test Queue")

        template = """
specificationVersion: jobtemplate-2023-09
name: Template Name
steps:
  - name: Step1
    script:
      actions:
        onRun:
          command: echo
          args: ["hello"]
"""
        job = backend.create_job(
            farmId=farm["farmId"],
            queueId=queue["queueId"],
            template=template,
            nameOverride="Override Name",
        )

        job_data = backend.get_job(
            farmId=farm["farmId"],
            queueId=queue["queueId"],
            jobId=job["jobId"],
        )
        assert job_data["name"] == "Override Name"

    def test_create_job_with_chunked_parameter_space(self):
        backend = MockDeadlineBackend()
        farm = backend.create_farm(displayName="Test Farm")
        queue = backend.create_queue(farmId=farm["farmId"], displayName="Test Queue")

        template = """
specificationVersion: jobtemplate-2023-09
extensions:
  - TASK_CHUNKING
name: Test
steps:
  - name: Render
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
        backend.create_job(
            farmId=farm["farmId"],
            queueId=queue["queueId"],
            template=template,
        )

        # Should create 30 tasks (one per frame value)
        assert len(backend.tasks) == 30
        # Check that parameters use chunkInt type with individual values
        task_values = sorted(
            [t["parameters"]["Frame"]["chunkInt"] for t in backend.tasks.values()], key=int
        )
        assert task_values == [str(i) for i in range(1, 31)]

    def test_simulate_task_run_includes_job_and_step_env_actions(self):
        """Test that simulation includes env enter/exit for job and step environments."""
        backend = MockDeadlineBackend()
        farm = backend.create_farm(displayName="Test Farm")
        queue = backend.create_queue(farmId=farm["farmId"], displayName="Test Queue")

        template = """
specificationVersion: jobtemplate-2023-09
name: Test
jobEnvironments:
  - name: JobEnv1
    variables:
      VAR1: value1
  - name: JobEnv2
    variables:
      VAR2: value2
steps:
  - name: Render
    stepEnvironments:
      - name: StepEnv1
        variables:
          STEP_VAR: step_value
    script:
      actions:
        onRun:
          command: echo
          args: ["test"]
"""
        job = backend.create_job(
            farmId=farm["farmId"],
            queueId=queue["queueId"],
            template=template,
        )

        step_id = list(backend.steps.values())[0]["stepId"]
        session_id = backend.simulate_task_runs(job["jobId"], step_id)

        actions = backend.list_session_actions(
            farmId=farm["farmId"],
            queueId=queue["queueId"],
            jobId=job["jobId"],
            sessionId=session_id,
        )["sessionActions"]

        # Should have: 3 env enters + 1 task run + 3 env exits = 7 actions
        assert len(actions) == 7

        # Extract action types in order
        action_types = []
        for action in actions:
            defn = action["definition"]
            if "envEnter" in defn:
                action_types.append(("enter", defn["envEnter"]["environmentId"]))
            elif "envExit" in defn:
                action_types.append(("exit", defn["envExit"]["environmentId"]))
            elif "taskRun" in defn:
                action_types.append(("task", defn["taskRun"]["taskId"]))

        # Env enters in order: JobEnv1, JobEnv2, StepEnv1
        assert action_types[0] == ("enter", "jobenv:JobEnv1")
        assert action_types[1] == ("enter", "jobenv:JobEnv2")
        assert action_types[2] == ("enter", "stepenv:StepEnv1")

        # Task run
        assert action_types[3][0] == "task"

        # Env exits in reverse order: StepEnv1, JobEnv2, JobEnv1
        assert action_types[4] == ("exit", "stepenv:StepEnv1")
        assert action_types[5] == ("exit", "jobenv:JobEnv2")
        assert action_types[6] == ("exit", "jobenv:JobEnv1")


class TestBatchGetApis:
    """Tests for batch_get_step / batch_get_task on MockDeadlineBackend."""

    def _setup_job(self):
        backend = MockDeadlineBackend()
        farm = backend.create_farm(displayName="F")
        queue = backend.create_queue(farmId=farm["farmId"], displayName="Q")
        job = backend.create_job(
            farmId=farm["farmId"],
            queueId=queue["queueId"],
            template=MINIMAL_TEMPLATE,
            nameOverride="J",
        )
        (step_key,) = [
            k for k in backend.steps if k[:3] == (farm["farmId"], queue["queueId"], job["jobId"])
        ]
        step_id = backend.steps[step_key]["stepId"]
        task_keys = [
            k
            for k in backend.tasks
            if k[:4] == (farm["farmId"], queue["queueId"], job["jobId"], step_id)
        ]
        task_id = backend.tasks[task_keys[0]]["taskId"]
        return backend, farm["farmId"], queue["queueId"], job["jobId"], step_id, task_id

    def test_batch_get_step_returns_existing_and_error_for_missing(self):
        backend, farm_id, queue_id, job_id, step_id, _ = self._setup_job()
        result = backend.batch_get_step(
            identifiers=[
                {"farmId": farm_id, "queueId": queue_id, "jobId": job_id, "stepId": step_id},
                {
                    "farmId": farm_id,
                    "queueId": queue_id,
                    "jobId": job_id,
                    "stepId": "step-does-not-exist",
                },
            ]
        )
        assert len(result["steps"]) == 1
        assert result["steps"][0]["stepId"] == step_id
        assert len(result["errors"]) == 1
        assert result["errors"][0]["code"] == "ResourceNotFoundException"
        assert result["errors"][0]["stepId"] == "step-does-not-exist"

    def test_batch_get_task_returns_existing_and_error_for_missing(self):
        backend, farm_id, queue_id, job_id, step_id, task_id = self._setup_job()
        result = backend.batch_get_task(
            identifiers=[
                {
                    "farmId": farm_id,
                    "queueId": queue_id,
                    "jobId": job_id,
                    "stepId": step_id,
                    "taskId": task_id,
                },
                {
                    "farmId": farm_id,
                    "queueId": queue_id,
                    "jobId": job_id,
                    "stepId": step_id,
                    "taskId": "task-missing",
                },
            ]
        )
        assert len(result["tasks"]) == 1
        assert result["tasks"][0]["taskId"] == task_id
        assert len(result["errors"]) == 1
        assert result["errors"][0]["code"] == "ResourceNotFoundException"
        assert result["errors"][0]["taskId"] == "task-missing"

    def test_inject_batch_failure_returns_injected_code(self):
        backend, farm_id, queue_id, job_id, step_id, task_id = self._setup_job()
        ident = {
            "farmId": farm_id,
            "queueId": queue_id,
            "jobId": job_id,
            "stepId": step_id,
            "taskId": task_id,
        }
        backend.inject_batch_failure("BatchGetTask", ident, "ThrottlingException", attempts=1)

        # First call: injected error, no item.
        result = backend.batch_get_task(identifiers=[ident])
        assert result["tasks"] == []
        assert len(result["errors"]) == 1
        assert result["errors"][0]["code"] == "ThrottlingException"
        assert result["errors"][0]["taskId"] == task_id

        # Second call: injection exhausted, returns real data.
        result = backend.batch_get_task(identifiers=[ident])
        assert len(result["tasks"]) == 1
        assert result["errors"] == []

    def test_batch_get_task_validates_max_100_identifiers(self):
        backend = MockDeadlineBackend()
        # Build 101 identifiers. The service schema enforces max 100.
        identifiers = [
            {
                "farmId": "farm-0",
                "queueId": "queue-0",
                "jobId": "job-0",
                "stepId": "step-0",
                "taskId": f"task-{i}",
            }
            for i in range(101)
        ]
        with pytest.raises(ClientError) as exc_info:
            backend.batch_get_task(identifiers=identifiers)
        assert "100" in str(exc_info.value)
