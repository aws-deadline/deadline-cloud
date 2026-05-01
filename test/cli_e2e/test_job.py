# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""End-to-end tests for `deadline job` subcommands that don't need S3."""

import json
import sys
from datetime import timedelta

import pytest

# MockDeadlineBackend.create_job parses the job template via openjd-model,
# which dropped Python 3.8 support. Tests that seed jobs into the backend
# carry this skip; tests that only read already-present state do not.
requires_openjd = pytest.mark.skipif(
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


def _seed_job(backend, farm_id, queue_id, *, name="test-job", status="RUNNING"):
    job = backend.create_job(
        farmId=farm_id, queueId=queue_id, template=_MINIMAL_TEMPLATE, priority=50
    )
    job_id = job["jobId"]
    # Flip status to something terminal-adjacent so downstream assertions are stable.
    backend.jobs[(farm_id, queue_id, job_id)].update(
        {
            "name": name,
            "taskRunStatus": status,
            "taskRunStatusCounts": {"PENDING": 0, "RUNNING": 1, "SUCCEEDED": 0, "FAILED": 0},
            "startedAt": backend._now(),
        }
    )
    return job_id


@pytest.fixture
def one_job(seeded_farm_queue):
    if sys.version_info < (3, 9):
        pytest.skip("openjd-model (used to parse job templates) requires Python >= 3.9")
    backend, farm_id, queue_id, env = seeded_farm_queue
    job_id = _seed_job(backend, farm_id, queue_id)
    return backend, farm_id, queue_id, job_id, env


def test_cli_job_list(one_job, run_cli):
    _, _, _, job_id, env = one_job
    r = run_cli(env, "job", "list")
    assert r.returncode == 0, r.stderr or r.stdout
    assert job_id in r.stdout


def test_cli_job_list_empty(seeded_farm_queue, run_cli):
    _, _, _, env = seeded_farm_queue
    r = run_cli(env, "job", "list")
    assert r.returncode == 0, r.stderr or r.stdout
    assert "0 of 0" in r.stdout or "Displaying 0" in r.stdout


def test_cli_job_get_by_id_positional(one_job, run_cli):
    _, _, _, job_id, env = one_job
    r = run_cli(env, "job", "get", job_id)
    assert r.returncode == 0, r.stderr or r.stdout
    assert job_id in r.stdout


def test_cli_job_get_by_flag(one_job, run_cli):
    _, _, _, job_id, env = one_job
    r = run_cli(env, "job", "get", "--job-id", job_id)
    assert r.returncode == 0, r.stderr or r.stdout
    assert job_id in r.stdout


def test_cli_job_get_unknown_fails(seeded_farm_queue, run_cli):
    _, _, _, env = seeded_farm_queue
    r = run_cli(env, "job", "get", "--job-id", "job-00000000000000000000000000000999")
    assert r.returncode != 0


def test_cli_job_get_search_term_finds_match(one_job, run_cli):
    _, _, _, job_id, env = one_job
    # A search term that isn't a job-id triggers _resolve_job_search.
    r = run_cli(env, "job", "get", "test-job")
    assert r.returncode == 0, r.stderr or r.stdout
    assert job_id in r.stdout


def test_cli_job_get_without_id_or_search_fails(seeded_farm_queue, run_cli):
    _, _, _, env = seeded_farm_queue
    r = run_cli(env, "job", "get")
    assert r.returncode != 0


def test_cli_job_cancel_without_yes_aborts(one_job, run_cli):
    backend, farm_id, queue_id, job_id, env = one_job
    # stdin closed -> click.confirm returns False / aborts; returncode != 0.
    r = run_cli(env, "job", "cancel", "--job-id", job_id)
    assert r.returncode != 0
    # Job should NOT be canceled.
    assert backend.jobs[(farm_id, queue_id, job_id)]["taskRunStatus"] != "CANCELED"


def test_cli_job_cancel_updates_backend(one_job, run_cli):
    backend, farm_id, queue_id, job_id, env = one_job
    r = run_cli(env, "job", "cancel", "--job-id", job_id, "--yes")
    assert r.returncode == 0, r.stderr or r.stdout
    assert backend.jobs[(farm_id, queue_id, job_id)]["taskRunStatus"] == "CANCELED"


def test_cli_job_cancel_mark_as_suspended(one_job, run_cli):
    backend, farm_id, queue_id, job_id, env = one_job
    r = run_cli(env, "job", "cancel", "--job-id", job_id, "--mark-as", "SUSPENDED", "--yes")
    assert r.returncode == 0, r.stderr or r.stdout
    assert backend.jobs[(farm_id, queue_id, job_id)]["taskRunStatus"] == "SUSPENDED"


def test_cli_job_requeue_tasks_no_matches(one_job, run_cli):
    _, _, _, job_id, env = one_job
    # Status counts seeded above have no FAILED/CANCELED/SUSPENDED tasks.
    r = run_cli(env, "job", "requeue-tasks", "--job-id", job_id, "--yes")
    assert r.returncode == 0, r.stderr or r.stdout
    assert "No tasks to requeue" in r.stdout


@requires_openjd
def test_cli_job_requeue_tasks_with_matching(seeded_farm_queue, run_cli):
    backend, farm_id, queue_id, env = seeded_farm_queue
    # Create a job whose single task is FAILED so there's something to requeue.
    job = backend.create_job(
        farmId=farm_id, queueId=queue_id, template=_MINIMAL_TEMPLATE, priority=50
    )
    job_id = job["jobId"]
    backend.jobs[(farm_id, queue_id, job_id)].update(
        {
            "taskRunStatus": "FAILED",
            "taskRunStatusCounts": {"FAILED": 1, "RUNNING": 0},
        }
    )
    # Reflect FAILED status on the underlying task + step so `list_tasks` returns a requeue-able entry.
    for key in list(backend.tasks):
        if key[:3] == (farm_id, queue_id, job_id):
            backend.tasks[key] = {**backend.tasks[key], "runStatus": "FAILED"}
    for key in list(backend.steps):
        if key[:3] == (farm_id, queue_id, job_id):
            backend.steps[key]["taskRunStatusCounts"] = {"FAILED": 1}

    r = run_cli(env, "job", "requeue-tasks", "--job-id", job_id, "--yes")
    assert r.returncode == 0, r.stderr or r.stdout
    assert "Requeued a total of 1 tasks" in r.stdout


@requires_openjd
def test_cli_job_wait_for_already_succeeded(seeded_farm_queue, run_cli):
    backend, farm_id, queue_id, env = seeded_farm_queue
    job = backend.create_job(
        farmId=farm_id, queueId=queue_id, template=_MINIMAL_TEMPLATE, priority=50
    )
    job_id = job["jobId"]
    backend.jobs[(farm_id, queue_id, job_id)].update(
        {
            "taskRunStatus": "SUCCEEDED",
            "taskRunStatusCounts": {"SUCCEEDED": 1},
            "endedAt": backend._now() + timedelta(seconds=1),
        }
    )

    r = run_cli(env, "job", "wait", "--job-id", job_id, "--max-poll-interval", "1")
    assert r.returncode == 0, r.stderr or r.stdout
    assert "SUCCEEDED" in r.stdout


@requires_openjd
def test_cli_job_wait_json_output(seeded_farm_queue, run_cli):
    backend, farm_id, queue_id, env = seeded_farm_queue
    job = backend.create_job(
        farmId=farm_id, queueId=queue_id, template=_MINIMAL_TEMPLATE, priority=50
    )
    job_id = job["jobId"]
    backend.jobs[(farm_id, queue_id, job_id)].update(
        {
            "taskRunStatus": "SUCCEEDED",
            "taskRunStatusCounts": {"SUCCEEDED": 1},
            "endedAt": backend._now() + timedelta(seconds=1),
        }
    )

    r = run_cli(
        env, "job", "wait", "--job-id", job_id, "--output", "json", "--max-poll-interval", "1"
    )
    assert r.returncode == 0, r.stderr or r.stdout
    payload = json.loads(r.stdout[r.stdout.find("{") :])
    assert payload["jobId"] == job_id
    assert payload["status"] == "SUCCEEDED"


def test_cli_job_list_unknown_queue_fails(seeded_farm_queue, run_cli):
    _, farm_id, _, env = seeded_farm_queue
    r = run_cli(
        env,
        "job",
        "list",
        "--farm-id",
        farm_id,
        "--queue-id",
        "queue-00000000000000000000000000000999",
    )
    assert r.returncode != 0
    combined = r.stdout + r.stderr
    assert "queue-00000000000000000000000000000999" in combined or "does not exist" in combined


def test_cli_job_cancel_unknown_id_fails(seeded_farm_queue, run_cli):
    _, _, _, env = seeded_farm_queue
    r = run_cli(
        env,
        "job",
        "cancel",
        "--job-id",
        "job-00000000000000000000000000000999",
        "--yes",
    )
    assert r.returncode != 0


def test_cli_job_requeue_tasks_unknown_id_fails(seeded_farm_queue, run_cli):
    _, _, _, env = seeded_farm_queue
    r = run_cli(
        env,
        "job",
        "requeue-tasks",
        "--job-id",
        "job-00000000000000000000000000000999",
        "--yes",
    )
    assert r.returncode != 0


def test_cli_job_wait_unknown_id_fails(seeded_farm_queue, run_cli):
    _, _, _, env = seeded_farm_queue
    r = run_cli(
        env,
        "job",
        "wait",
        "--job-id",
        "job-00000000000000000000000000000999",
        "--max-poll-interval",
        "1",
    )
    assert r.returncode != 0


def test_cli_job_help(deadline_env, run_cli):
    _, env = deadline_env
    r = run_cli(env, "job", "--help")
    assert r.returncode == 0
    for sub in ("list", "get", "cancel", "requeue-tasks", "wait", "download-output", "logs"):
        assert sub in r.stdout
