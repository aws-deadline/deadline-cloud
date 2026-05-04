# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""End-to-end tests for `deadline job trace-schedule`."""

from __future__ import annotations

import json
import sys

import pytest


pytestmark = pytest.mark.skipif(
    sys.version_info < (3, 9), reason="openjd-model requires Python 3.9+"
)


SINGLE_TASK_TEMPLATE = """\
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

SIX_TASK_TEMPLATE = """\
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

MULTI_PARAM_TEMPLATE = """\
specificationVersion: jobtemplate-2023-09
name: Test
steps:
  - name: Render
    parameterSpace:
      taskParameterDefinitions:
        - name: Frame
          type: INT
          range: "1-3"
        - name: Camera
          type: STRING
          range: ["cam1", "cam2"]
    script:
      actions:
        onRun:
          command: echo
          args: ["{{Task.Param.Frame}}-{{Task.Param.Camera}}"]
"""

CHUNKED_TEMPLATE = """\
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
          range: "1-10"
          chunks:
            defaultTaskCount: 3
            rangeConstraint: CONTIGUOUS
    script:
      actions:
        onRun:
          command: echo
          args: ["{{Task.Param.Frame}}"]
"""

JOB_ENV_TEMPLATE = """\
specificationVersion: jobtemplate-2023-09
name: Test
jobEnvironments:
  - name: MyEnv
    script:
      actions:
        onEnter:
          command: echo
          args: ["enter"]
        onExit:
          command: echo
          args: ["exit"]
steps:
  - name: Render
    script:
      actions:
        onRun:
          command: echo
          args: ["test"]
"""


def _step_id(backend, farm_id, queue_id, job_id):
    return next(k[3] for k in backend.steps if k[:3] == (farm_id, queue_id, job_id))


def _task_ids(backend, farm_id, queue_id, job_id, step_id):
    return [k[4] for k in backend.tasks if k[:4] == (farm_id, queue_id, job_id, step_id)]


def _seed(seeded_farm_queue, template: str):
    backend, farm_id, queue_id, env = seeded_farm_queue
    job = backend.create_job(farmId=farm_id, queueId=queue_id, template=template, priority=50)
    job_id = job["jobId"]
    return backend, farm_id, queue_id, job_id, env


def test_cli_single_session_single_task(seeded_farm_queue, run_cli):
    backend, farm_id, queue_id, job_id, env = _seed(seeded_farm_queue, SINGLE_TASK_TEMPLATE)
    backend.simulate_task_runs(job_id, _step_id(backend, farm_id, queue_id, job_id))

    r = run_cli(env, "job", "trace-schedule", "--job-id", job_id)
    assert r.returncode == 0, r.stderr or r.stdout
    assert "Session Count: 1" in r.stdout
    assert "Task Run Count: 1" in r.stdout


def test_cli_multiple_sessions_multiple_workers(seeded_farm_queue, run_cli):
    backend, farm_id, queue_id, job_id, env = _seed(seeded_farm_queue, SIX_TASK_TEMPLATE)
    step_id = _step_id(backend, farm_id, queue_id, job_id)
    task_ids = _task_ids(backend, farm_id, queue_id, job_id, step_id)
    backend.simulate_task_runs(job_id, step_id, task_ids[0:2], worker_id="worker-0")
    backend.simulate_task_runs(job_id, step_id, task_ids[2:4], worker_id="worker-1")
    backend.simulate_task_runs(job_id, step_id, task_ids[4:6], worker_id="worker-2")

    r = run_cli(env, "job", "trace-schedule", "--job-id", job_id)
    assert r.returncode == 0, r.stderr or r.stdout
    assert "Session Count: 3" in r.stdout
    assert "Task Run Count: 6" in r.stdout


def test_cli_with_env_actions(seeded_farm_queue, run_cli):
    backend, farm_id, queue_id, job_id, env = _seed(seeded_farm_queue, JOB_ENV_TEMPLATE)
    backend.simulate_task_runs(job_id, _step_id(backend, farm_id, queue_id, job_id))

    r = run_cli(env, "job", "trace-schedule", "--job-id", job_id)
    assert r.returncode == 0, r.stderr or r.stdout
    assert "Session Count: 1" in r.stdout
    assert "Task Run Count: 1" in r.stdout
    assert "Env Action Count: 2" in r.stdout


def test_cli_with_task_parameters(seeded_farm_queue, run_cli):
    backend, farm_id, queue_id, job_id, env = _seed(seeded_farm_queue, MULTI_PARAM_TEMPLATE)
    backend.simulate_task_runs(job_id, _step_id(backend, farm_id, queue_id, job_id))

    r = run_cli(env, "job", "trace-schedule", "--job-id", job_id)
    assert r.returncode == 0, r.stderr or r.stdout
    assert "Task Run Count: 6" in r.stdout  # 3 frames * 2 cameras


def test_cli_chunked_task_parameters(seeded_farm_queue, run_cli):
    backend, farm_id, queue_id, job_id, env = _seed(seeded_farm_queue, CHUNKED_TEMPLATE)
    step_id = _step_id(backend, farm_id, queue_id, job_id)
    # Verify chunked tasks were created with chunkInt parameters.
    task_key = next(k for k in backend.tasks if k[3] == step_id)
    assert "chunkInt" in backend.tasks[task_key]["parameters"]["Frame"]
    task_ids = _task_ids(backend, farm_id, queue_id, job_id, step_id)
    backend.simulate_task_runs(job_id, step_id, task_ids[:3])

    r = run_cli(env, "job", "trace-schedule", "--job-id", job_id)
    assert r.returncode == 0, r.stderr or r.stdout
    assert "Session Count: 1" in r.stdout
    assert "Task Run Count: 3" in r.stdout


def test_cli_durations_reported(seeded_farm_queue, run_cli):
    backend, farm_id, queue_id, job_id, env = _seed(seeded_farm_queue, JOB_ENV_TEMPLATE)
    backend.simulate_task_runs(
        job_id,
        _step_id(backend, farm_id, queue_id, job_id),
        duration_seconds=60,
        env_duration_seconds=5,
    )

    r = run_cli(env, "job", "trace-schedule", "--job-id", job_id)
    assert r.returncode == 0, r.stderr or r.stdout
    # 1 task @ 60s, 1 env enter @ 5s, 1 env exit @ 5s = 70s total actions.
    assert "Session Total Duration: 0:01:10" in r.stdout
    assert "Session Action Count: 3" in r.stdout
    assert "Session Action Total Duration: 0:01:10" in r.stdout
    assert "Task Run Count: 1" in r.stdout
    assert "Task Run Total Duration: 0:01:00" in r.stdout
    assert "Env Action Count: 2" in r.stdout
    assert "Env Action Total Duration: 0:00:10" in r.stdout


def test_cli_overhead_reported(seeded_farm_queue, run_cli):
    backend, farm_id, queue_id, job_id, env = _seed(seeded_farm_queue, SINGLE_TASK_TEMPLATE)
    backend.simulate_task_runs(
        job_id,
        _step_id(backend, farm_id, queue_id, job_id),
        duration_seconds=40,
        overhead_seconds=20,
    )

    r = run_cli(env, "job", "trace-schedule", "--job-id", job_id)
    assert r.returncode == 0, r.stderr or r.stdout
    assert "Session Total Duration: 0:01:00" in r.stdout
    assert "Session Action Total Duration: 0:00:40" in r.stdout
    assert "Within-session Overhead Duration: 0:00:20" in r.stdout
    assert "Within-session Overhead Duration Per Action: 0:00:20" in r.stdout


def test_cli_zero_counts_reported(seeded_farm_queue, run_cli):
    backend, farm_id, queue_id, job_id, env = _seed(seeded_farm_queue, SINGLE_TASK_TEMPLATE)
    backend.simulate_task_runs(job_id, _step_id(backend, farm_id, queue_id, job_id))

    r = run_cli(env, "job", "trace-schedule", "--job-id", job_id)
    assert r.returncode == 0, r.stderr or r.stdout
    assert "Env Action Count: 0" in r.stdout
    assert "Env Action Total Duration: 0:00:00" in r.stdout
    assert "Sync Job Attachments Count: 0" in r.stdout
    assert "Sync Job Attachments Total Duration: 0:00:00" in r.stdout
    assert "Non-Task Run Count: 0" in r.stdout
    assert "Non-Task Run Total Duration: 0:00:00" in r.stdout


def test_cli_chrome_trace_file(seeded_farm_queue, run_cli, tmp_path):
    backend, farm_id, queue_id, job_id, env = _seed(seeded_farm_queue, SIX_TASK_TEMPLATE)
    step_id = _step_id(backend, farm_id, queue_id, job_id)
    task_ids = _task_ids(backend, farm_id, queue_id, job_id, step_id)
    backend.simulate_task_runs(job_id, step_id, task_ids[0:3], worker_id="worker-0")
    backend.simulate_task_runs(job_id, step_id, task_ids[3:6], worker_id="worker-1")

    trace_file = tmp_path / "trace.json"
    r = run_cli(
        env,
        "job",
        "trace-schedule",
        "--job-id",
        job_id,
        "--trace-format",
        "chrome",
        "--trace-file",
        str(trace_file),
    )
    assert r.returncode == 0, r.stderr or r.stdout
    assert trace_file.exists()
    data = json.loads(trace_file.read_text())
    events = data["traceEvents"] if isinstance(data, dict) else data
    assert isinstance(events, list) and events


def test_cli_uses_batch_get_task_and_batch_get_step(seeded_farm_queue, run_cli):
    """Regression guard: trace-schedule uses BatchGetStep/BatchGetTask, not
    per-item GetStep/GetTask."""
    backend, farm_id, queue_id, job_id, env = _seed(seeded_farm_queue, SIX_TASK_TEMPLATE)
    step_id = _step_id(backend, farm_id, queue_id, job_id)
    task_ids = _task_ids(backend, farm_id, queue_id, job_id, step_id)
    backend.simulate_task_runs(job_id, step_id, task_ids[0:3], worker_id="worker-0")
    backend.simulate_task_runs(job_id, step_id, task_ids[3:6], worker_id="worker-1")

    r = run_cli(env, "job", "trace-schedule", "--job-id", job_id)
    assert r.returncode == 0, r.stderr or r.stdout
    # Single batch each is enough for 1 step and 6 tasks.
    assert backend.call_counts.get("BatchGetStep", 0) == 1
    assert backend.call_counts.get("BatchGetTask", 0) == 1
    # Legacy per-item calls must NOT be used.
    assert backend.call_counts.get("GetStep", 0) == 0
    assert backend.call_counts.get("GetTask", 0) == 0


def test_cli_large_task_count_uses_chunked_batches(seeded_farm_queue, run_cli):
    """150 tasks => exactly 2 BatchGetTask batches (chunk size 100)."""
    template = """\
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
    backend, farm_id, queue_id, job_id, env = _seed(seeded_farm_queue, template)
    backend.simulate_task_runs(job_id, _step_id(backend, farm_id, queue_id, job_id))

    r = run_cli(env, "job", "trace-schedule", "--job-id", job_id)
    assert r.returncode == 0, r.stderr or r.stdout
    assert "Task Run Count: 150" in r.stdout
    assert backend.call_counts.get("BatchGetTask", 0) == 2
    assert sorted(backend.batch_call_sizes.get("BatchGetTask", [])) == [50, 100]


def test_cli_transient_error_is_retried_and_trace_succeeds(seeded_farm_queue, run_cli):
    """ThrottlingException on one task: CLI retries and completes."""
    backend, farm_id, queue_id, job_id, env = _seed(seeded_farm_queue, SIX_TASK_TEMPLATE)
    step_id = _step_id(backend, farm_id, queue_id, job_id)
    task_ids = _task_ids(backend, farm_id, queue_id, job_id, step_id)
    backend.simulate_task_runs(job_id, step_id, task_ids)
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

    r = run_cli(env, "job", "trace-schedule", "--job-id", job_id)
    assert r.returncode == 0, r.stderr or r.stdout
    assert "Task Run Count: 6" in r.stdout
    # Initial call + one retry for the injected failure.
    assert backend.call_counts.get("BatchGetTask", 0) == 2


def test_cli_terminal_error_warns_and_continues(seeded_farm_queue, run_cli):
    """Injected ResourceNotFoundException on one task: CLI warns but completes."""
    backend, farm_id, queue_id, job_id, env = _seed(seeded_farm_queue, SIX_TASK_TEMPLATE)
    step_id = _step_id(backend, farm_id, queue_id, job_id)
    task_ids = _task_ids(backend, farm_id, queue_id, job_id, step_id)
    backend.simulate_task_runs(job_id, step_id, task_ids)
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
        attempts=10,
    )

    r = run_cli(env, "job", "trace-schedule", "--job-id", job_id)
    assert r.returncode == 0, r.stderr or r.stdout
    combined = r.stdout + r.stderr
    assert "could not retrieve 1 task" in combined
    assert "Task Run Count: 6" in r.stdout


def test_cli_trace_schedule_help(deadline_env, run_cli):
    _, env = deadline_env
    r = run_cli(env, "job", "trace-schedule", "--help")
    assert r.returncode == 0
