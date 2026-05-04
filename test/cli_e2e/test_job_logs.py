# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""End-to-end tests for `deadline job logs` (uses moto CloudWatch Logs)."""

import json
import sys
import time

import pytest

# Every test in this module seeds a job via MockDeadlineBackend.create_job,
# which parses the template using openjd-model. openjd-model requires
# Python >= 3.9, so skip the module on 3.8.
pytestmark = pytest.mark.skipif(
    sys.version_info < (3, 9),
    reason="openjd-model (used to parse job templates) requires Python >= 3.9",
)


_MINIMAL_TEMPLATE = """\
specificationVersion: 'jobtemplate-2023-09'
name: test-job
steps:
- name: step1
  script:
    actions:
      onRun:
        command: "echo"
        args: ["hi"]
"""


@pytest.fixture
def seeded_job_with_session_and_logs(seeded_farm_queue, logs_client):
    backend, farm_id, queue_id, env = seeded_farm_queue
    job = backend.create_job(
        farmId=farm_id, queueId=queue_id, template=_MINIMAL_TEMPLATE, priority=50
    )
    job_id = job["jobId"]
    step_id = next(k[3] for k in backend.steps if k[:3] == (farm_id, queue_id, job_id))
    session_id = backend.simulate_task_runs(job_id=job_id, step_id=step_id, duration_seconds=1)

    log_group = f"/aws/deadline/{farm_id}/{queue_id}"
    try:
        logs_client.create_log_group(logGroupName=log_group)
    except logs_client.exceptions.ResourceAlreadyExistsException:
        pass
    # Ensure a clean stream per test: backend.clear() resets id counters so
    # session_id is reused across tests on the same xdist worker, and moto's
    # log streams persist for the whole session.
    try:
        logs_client.delete_log_stream(logGroupName=log_group, logStreamName=session_id)
    except logs_client.exceptions.ResourceNotFoundException:
        pass
    logs_client.create_log_stream(logGroupName=log_group, logStreamName=session_id)
    logs_client.put_log_events(
        logGroupName=log_group,
        logStreamName=session_id,
        logEvents=[
            {"timestamp": int(time.time() * 1000), "message": "hello from mock logs line 1"},
            {"timestamp": int(time.time() * 1000), "message": "hello from mock logs line 2"},
        ],
    )
    return backend, farm_id, queue_id, job_id, session_id, env


def test_cli_job_logs_by_session_id(seeded_job_with_session_and_logs, run_cli):
    _, _, _, job_id, session_id, env = seeded_job_with_session_and_logs
    r = run_cli(env, "job", "logs", "--job-id", job_id, "--session-id", session_id)
    assert r.returncode == 0, r.stderr or r.stdout
    assert "hello from mock logs line 1" in r.stdout
    assert "hello from mock logs line 2" in r.stdout


def test_cli_job_logs_auto_selects_session(seeded_job_with_session_and_logs, run_cli):
    _, _, _, job_id, session_id, env = seeded_job_with_session_and_logs
    r = run_cli(env, "job", "logs", "--job-id", job_id)
    assert r.returncode == 0, r.stderr or r.stdout
    assert session_id in r.stdout
    assert "hello from mock logs" in r.stdout


def test_cli_job_logs_json_output(seeded_job_with_session_and_logs, run_cli):
    _, _, _, job_id, session_id, env = seeded_job_with_session_and_logs
    r = run_cli(
        env, "job", "logs", "--job-id", job_id, "--session-id", session_id, "--output", "json"
    )
    assert r.returncode == 0, r.stderr or r.stdout
    payload = json.loads(r.stdout)
    assert payload["logStream"] == session_id
    assert len(payload["events"]) == 2


def test_cli_job_logs_limit(seeded_job_with_session_and_logs, run_cli):
    _, _, _, job_id, session_id, env = seeded_job_with_session_and_logs
    # Verbose path works with the mock; use it to confirm --limit is honored.
    r = run_cli(
        env,
        "job",
        "logs",
        "--job-id",
        job_id,
        "--session-id",
        session_id,
        "--limit",
        "1",
    )
    assert r.returncode == 0, r.stderr or r.stdout
    lines = [line for line in r.stdout.splitlines() if "hello from mock logs" in line]
    assert len(lines) == 1


def test_cli_job_logs_missing_stream_returns_empty(seeded_job_with_session_and_logs, run_cli):
    _, _, _, job_id, _, env = seeded_job_with_session_and_logs
    r = run_cli(
        env,
        "job",
        "logs",
        "--job-id",
        job_id,
        "--session-id",
        "session-00000000000000000000000000000999",
    )
    # CLI tolerates missing log group/stream and returns 0 with empty output.
    # When the session itself doesn't exist, the CLI errors out.
    assert r.returncode != 0
    assert "Failed" in r.stdout or "Failed" in r.stderr


def test_cli_job_logs_with_start_and_end_time(seeded_job_with_session_and_logs, run_cli):
    _, _, _, job_id, session_id, env = seeded_job_with_session_and_logs
    r = run_cli(
        env,
        "job",
        "logs",
        "--job-id",
        job_id,
        "--session-id",
        session_id,
        "--start-time",
        "2000-01-01T00:00:00Z",
        "--end-time",
        "2100-01-01T00:00:00Z",
    )
    assert r.returncode == 0, r.stderr or r.stdout


def test_cli_job_logs_invalid_session_action_id_fails(seeded_job_with_session_and_logs, run_cli):
    _, _, _, job_id, _, env = seeded_job_with_session_and_logs
    r = run_cli(
        env,
        "job",
        "logs",
        "--job-id",
        job_id,
        "--session-action-id",
        "not-a-valid-id",
    )
    assert r.returncode != 0


def test_cli_job_logs_timestamp_format_relative(seeded_job_with_session_and_logs, run_cli):
    _, _, _, job_id, session_id, env = seeded_job_with_session_and_logs
    r = run_cli(
        env,
        "job",
        "logs",
        "--job-id",
        job_id,
        "--session-id",
        session_id,
        "--timestamp-format",
        "relative",
    )
    assert r.returncode == 0, r.stderr or r.stdout
