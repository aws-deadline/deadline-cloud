# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests that `queue sync-output` waits for a session's tasks to finish before downloading the
session, because a session action can succeed before its task does.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any, NamedTuple, Optional
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner
from freezegun import freeze_time

from deadline.client.cli import main
from deadline.client.cli import _incremental_download as mod
from deadline.job_attachments._incremental_downloads._manifest_s3_downloads import (
    _get_manifests_to_download,
)
from deadline.job_attachments._incremental_downloads.incremental_download_state import (
    IncrementalDownloadState,
)
from deadline.job_attachments.asset_manifests.hash_algorithms import HashAlgorithm
from deadline.job_attachments.asset_manifests.v2023_03_03.asset_manifest import (
    AssetManifest,
    ManifestPath,
)
from deadline.job_attachments.exceptions import JobAttachmentsS3ClientError

from ..mock_deadline_job_apis import (
    create_fake_job_list,
    mock_get_job_for_set,
    mock_search_jobs_for_set,
)
from ..shared_constants import (
    MOCK_BUCKET_NAME,
    MOCK_FARM_ID,
    MOCK_FLEET_ID,
    MOCK_QUEUE_ID,
    MOCK_WORKER_ID,
)

T0 = datetime.fromisoformat("2025-05-26 12:00:00+00:00")
STEP_ID = "step-b1764261dff54214aace3932bde8ae7e"
JOB_A = "job-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
JOB_B = "job-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
SESSION_1 = "session-11111111111111111111111111111111"
SESSION_2 = "session-22222222222222222222222222222222"
SESSION_3 = "session-33333333333333333333333333333333"

ACTIVE_STATUSES = ("READY", "ASSIGNED", "STARTING", "SCHEDULED", "RUNNING")


def _task(n: int) -> str:
    return f"task-{STEP_ID[5:]}-{n}"


class _Action(NamedTuple):
    action_index: int
    task_id: str
    status: str = "SUCCEEDED"


class _Session(NamedTuple):
    session_id: str
    actions: list
    ended_at: Optional[datetime] = T0 - timedelta(minutes=10)


class _SyncEnv:
    """A queue whose jobs, sessions, task run statuses and manifests a test edits between runs.

    Each taskRun session action writes one output file named after its job and task, so a
    test can tell from the downloaded paths exactly which task runs were downloaded.
    """

    def __init__(self, deadline_mock: MagicMock, tmp_path, checkpoint_dir: str) -> None:
        self.deadline_mock = deadline_mock
        self.tmp_path = tmp_path
        self.checkpoint_dir = checkpoint_dir
        self.jobs: dict[str, dict[str, Any]] = {}
        self.sessions: dict[str, list[_Session]] = {}
        # (job_id, task_id) -> the task's runStatus as BatchGetTask reports it
        self.task_status: dict[tuple[str, str], str] = {}
        # (job_id, task_id) -> BatchGetTask per-item error code, overriding task_status
        self.task_errors: dict[tuple[str, str], str] = {}
        # (job_id, task_id) -> S3 status code returned when fetching that task's output manifest
        self.manifest_errors: dict[tuple[str, str], int] = {}
        # Leave skipped manifests out of the returned list instead of leaving None in their slot.
        self.compact_skipped_manifests = False
        # (job_id, session_id, action_index) of taskRun actions listed without a taskId
        self.drop_task_id_from: set[tuple[str, str, int]] = set()
        self.downloaded: list[str] = []
        self.batch_get_task_calls: list[list[dict]] = []
        self.last_output = ""

    def set_job(self, job_id: str, counts: dict[str, int], ended_at: Optional[datetime] = None):
        job = self.jobs.get(job_id)
        if job is None:
            job = create_fake_job_list(1)[0]
            job["jobId"] = job_id
            job["name"] = f"Job {job_id[-1].upper()}"
            job["attachments"] = {
                "manifests": [
                    {
                        "rootPath": str(self.tmp_path / "out"),
                        "rootPathFormat": "posix",
                        "outputRelativeDirectories": ["."],
                    }
                ],
                "fileSystem": "COPIED",
            }
            self.jobs[job_id] = job
        job["taskRunStatusCounts"] = dict(counts)
        active = [status for status in ACTIVE_STATUSES if counts.get(status)]
        job["taskRunStatus"] = active[0] if active else "SUCCEEDED"
        if ended_at is None:
            job.pop("endedAt", None)
        else:
            job["endedAt"] = ended_at

    def output_path(self, job_id: str, task_id: str) -> str:
        return str(self.tmp_path / "out" / job_id / f"{task_id}.exr")

    def checkpoint(self) -> IncrementalDownloadState:
        path = os.path.join(
            self.checkpoint_dir,
            f"{MOCK_QUEUE_ID}_ignore-storage-profiles_download_checkpoint.json",
        )
        return IncrementalDownloadState.from_file(path)

    def checkpoint_text(self) -> str:
        path = os.path.join(
            self.checkpoint_dir,
            f"{MOCK_QUEUE_ID}_ignore-storage-profiles_download_checkpoint.json",
        )
        with open(path) as f:
            return f.read()

    def completed_indexes(self, job_id: str) -> dict[str, int]:
        for job in self.checkpoint().jobs:
            if job.job_id == job_id:
                return job.session_completed_indexes
        raise AssertionError(f"{job_id} is not in the checkpoint")

    def failed_jobs(self) -> dict[str, int]:
        path = os.path.join(
            self.checkpoint_dir, f"{MOCK_QUEUE_ID}_ignore-storage-profiles_failed_jobs.json"
        )
        if not os.path.exists(path):
            return {}
        with open(path) as f:
            return json.load(f)

    def _list_sessions(self, farmId, queueId, jobId, **kwargs):
        sessions = []
        for session in self.sessions.get(jobId, []):
            item: dict[str, Any] = {
                "sessionId": session.session_id,
                "fleetId": MOCK_FLEET_ID,
                "workerId": MOCK_WORKER_ID,
                "startedAt": T0 - timedelta(minutes=30),
                "lifecycleStatus": "ENDED" if session.ended_at else "STARTED",
            }
            if session.ended_at is not None:
                item["endedAt"] = session.ended_at
            sessions.append(item)
        return {"sessions": sessions}

    def _list_session_actions(self, farmId, queueId, jobId, sessionId, **kwargs):
        session = next(s for s in self.sessions[jobId] if s.session_id == sessionId)
        response = {
            "sessionActions": [
                {
                    "sessionActionId": f"sessionaction-{sessionId[8:]}-{action.action_index}",
                    "status": action.status,
                    "startedAt": T0 - timedelta(minutes=20),
                    "definition": {"taskRun": {"taskId": action.task_id, "stepId": STEP_ID}},
                    "manifests": [
                        {
                            "outputManifestPath": f"{MOCK_FARM_ID}/{MOCK_QUEUE_ID}/{jobId}/"
                            f"{STEP_ID}/{action.task_id}/"
                            f"2025-05-26T11:50:00.000000Z_sessionaction-{sessionId[8:]}-"
                            f"{action.action_index}/0123456789abcdef_output"
                        }
                    ],
                }
                for action in session.actions
            ]
        }
        for action, item in zip(session.actions, response["sessionActions"]):
            if (jobId, sessionId, action.action_index) in self.drop_task_id_from:
                del item["definition"]["taskRun"]["taskId"]
        return response

    def _batch_get_task(self, identifiers):
        self.batch_get_task_calls.append(list(identifiers))
        tasks, errors = [], []
        for ident in identifiers:
            key = (ident["jobId"], ident["taskId"])
            status = self.task_status.get(key)
            if key in self.task_errors or status is None:
                code = self.task_errors.get(key, "ResourceNotFoundException")
                errors.append({**ident, "code": code, "message": "task error"})
            else:
                tasks.append({**ident, "runStatus": status})
        return {"tasks": tasks, "errors": errors}

    def _download_all_manifests(
        self,
        queue,
        download_candidate_jobs,
        job_sessions,
        path_mapping_rule_appliers,
        output_unmapped_paths,
        boto3_session_for_s3,
        print_function_callback=lambda msg: None,
        *,
        output_missing_manifest_job_ids: Optional[set[str]] = None,
    ):
        """Stands in for the job attachments manifest download, including its handling of
        missing (404) and forbidden (403) manifests."""
        to_download = _get_manifests_to_download(
            queue["jobAttachmentSettings"]["rootPrefix"],
            download_candidate_jobs,
            job_sessions,
            path_mapping_rule_appliers,
        )
        result: list = []
        skipped: list[tuple[str, JobAttachmentsS3ClientError]] = []
        for _, job_id, _, key in to_download:
            task_id = mod._extract_task_id_from_s3_key(key)
            assert task_id is not None, key
            status_code = self.manifest_errors.get((job_id, task_id))
            if status_code is not None:
                error = JobAttachmentsS3ClientError(
                    "get_object", status_code, MOCK_BUCKET_NAME, key, "manifest error"
                )
                if output_missing_manifest_job_ids is None:
                    raise error
                skipped.append((job_id, error))
                result.append(None)
                continue
            path = self.output_path(job_id, task_id)
            result.append(
                (
                    T0 - timedelta(minutes=5),
                    AssetManifest(
                        hash_alg=HashAlgorithm.XXH128,
                        total_size=1,
                        paths=[ManifestPath(path=path, hash="h", size=1, mtime=1)],
                    ),
                )
            )
        if skipped:
            if len(skipped) == len(to_download) and all(e.status_code == 403 for _, e in skipped):
                raise skipped[0][1]
            assert output_missing_manifest_job_ids is not None
            output_missing_manifest_job_ids.update(job_id for job_id, _ in skipped)
            if self.compact_skipped_manifests:
                result = [item for item in result if item is not None]
        return result

    def _download_manifest_paths(self, files, *args, **kwargs):
        for f in files:
            self.downloaded.append(f.path)
            os.makedirs(os.path.dirname(f.path), exist_ok=True)
            with open(f.path, "w") as fh:
                fh.write("output")

    def run(self, now: datetime, *extra: str):
        self.downloaded = []
        self.batch_get_task_calls = []
        jobs = list(self.jobs.values())
        self.deadline_mock.search_jobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, jobs)
        self.deadline_mock.get_job = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, jobs)
        self.deadline_mock.list_sessions.side_effect = self._list_sessions
        self.deadline_mock.list_session_actions.side_effect = self._list_session_actions
        self.deadline_mock.batch_get_task.side_effect = self._batch_get_task
        with (
            patch.object(
                mod,
                "_download_all_manifests_with_absolute_paths",
                side_effect=self._download_all_manifests,
            ),
            patch.object(
                mod, "_download_manifest_paths", side_effect=self._download_manifest_paths
            ),
            freeze_time(now),
        ):
            result = CliRunner().invoke(
                main,
                [
                    "queue",
                    "sync-output",
                    "--ignore-storage-profiles",
                    "--bootstrap-lookback-minutes",
                    "120",
                    "--farm-id",
                    MOCK_FARM_ID,
                    "--queue-id",
                    MOCK_QUEUE_ID,
                    "--checkpoint-dir",
                    self.checkpoint_dir,
                    *extra,
                ],
            )
        self.last_output = result.output
        return result


@pytest.fixture
def env(fresh_deadline_config, deadline_mock, tmp_path, tmp_path_factory) -> _SyncEnv:
    return _SyncEnv(deadline_mock, tmp_path, str(tmp_path_factory.mktemp("checkpoint")))


def _hold_job_a_with_task_1_running(env: _SyncEnv, extra_ready_tasks: int = 0) -> None:
    """Job A's session 1 ran task 1 then task 2, and both session actions succeeded, but
    task 1 has not finished. Job B finished normally in its own session."""
    counts = {"SUCCEEDED": 1, "RUNNING": 1}
    if extra_ready_tasks:
        counts["READY"] = extra_ready_tasks
    env.set_job(JOB_A, counts)
    env.sessions[JOB_A] = [_Session(SESSION_1, [_Action(0, _task(1)), _Action(1, _task(2))])]
    env.task_status[(JOB_A, _task(1))] = "RUNNING"
    env.task_status[(JOB_A, _task(2))] = "SUCCEEDED"

    env.set_job(JOB_B, {"SUCCEEDED": 1}, ended_at=T0 - timedelta(minutes=8))
    env.sessions[JOB_B] = [_Session(SESSION_2, [_Action(0, _task(9))])]
    env.task_status[(JOB_B, _task(9))] = "SUCCEEDED"


def _assert_ok(result) -> None:
    assert result.exit_code == 0, result.output


def test_session_with_unfinished_task_is_held_then_downloaded(env):
    """Run 1 skips the whole session while one of its tasks is unfinished, without advancing
    its index, and still saves the checkpoint and downloads other jobs. Run 2 downloads
    both of the session's tasks once the task finishes."""
    _hold_job_a_with_task_1_running(env)

    result = env.run(T0)
    _assert_ok(result)
    assert env.downloaded == [env.output_path(JOB_B, _task(9))]
    assert env.completed_indexes(JOB_A) == {}
    assert env.completed_indexes(JOB_B) == {SESSION_2: 0}
    assert env.failed_jobs() == {}

    env.task_status[(JOB_A, _task(1))] = "SUCCEEDED"
    env.set_job(JOB_A, {"SUCCEEDED": 2}, ended_at=T0 + timedelta(minutes=4))
    result = env.run(T0 + timedelta(minutes=5))
    _assert_ok(result)
    assert sorted(env.downloaded) == sorted(
        [env.output_path(JOB_A, _task(1)), env.output_path(JOB_A, _task(2))]
    )
    assert env.completed_indexes(JOB_A) == {SESSION_1: 1}


def test_one_batch_get_task_call_covers_every_job(env):
    """Task run statuses for all candidate jobs come from one BatchGetTask request."""
    _hold_job_a_with_task_1_running(env)

    _assert_ok(env.run(T0))

    assert len(env.batch_get_task_calls) == 1
    assert sorted((i["jobId"], i["taskId"]) for i in env.batch_get_task_calls[0]) == sorted(
        [(JOB_A, _task(1)), (JOB_A, _task(2)), (JOB_B, _task(9))]
    )


def test_no_batch_get_task_call_when_nothing_is_new(env):
    """Already downloaded session actions are filtered out before task statuses are fetched,
    so a run with no new session actions costs no extra API calls."""
    env.set_job(JOB_A, {"SUCCEEDED": 1, "RUNNING": 1})
    env.sessions[JOB_A] = [_Session(SESSION_1, [_Action(0, _task(1))])]
    env.task_status[(JOB_A, _task(1))] = "SUCCEEDED"
    _assert_ok(env.run(T0))
    assert env.downloaded == [env.output_path(JOB_A, _task(1))]

    # The job changes, but its only session has no session action newer than the checkpoint.
    env.set_job(JOB_A, {"SUCCEEDED": 1, "FAILED": 1}, ended_at=T0 + timedelta(minutes=4))
    _assert_ok(env.run(T0 + timedelta(minutes=5)))

    assert "1 session across 1 job" in env.last_output
    assert env.downloaded == []
    assert env.batch_get_task_calls == []


def test_held_task_fails_then_later_task_succeeds(env):
    """A held task that ends FAILED is never downloaded and does not hold its session; the
    rest of the session downloads on the next run that sees the job change."""
    _hold_job_a_with_task_1_running(env, extra_ready_tasks=2)
    _assert_ok(env.run(T0))
    assert env.completed_indexes(JOB_A) == {}

    env.task_status[(JOB_A, _task(1))] = "FAILED"
    env.task_status[(JOB_A, _task(3))] = "SUCCEEDED"
    env.sessions[JOB_A].append(
        _Session(SESSION_3, [_Action(0, _task(3))], ended_at=T0 + timedelta(minutes=3))
    )
    env.set_job(JOB_A, {"SUCCEEDED": 2, "FAILED": 1, "READY": 1})
    result = env.run(T0 + timedelta(minutes=5))

    _assert_ok(result)
    assert sorted(env.downloaded) == sorted(
        [env.output_path(JOB_A, _task(2)), env.output_path(JOB_A, _task(3))]
    )
    assert env.output_path(JOB_A, _task(1)) not in env.downloaded
    assert env.completed_indexes(JOB_A) == {SESSION_1: 1, SESSION_3: 0}


def test_held_task_fails_and_session_downloads_when_job_ends(env):
    """When the held task fails and the job's succeeded count stays the same, the job is
    pruned as unchanged (an accepted delay). The rest of the session downloads once the
    job ends, and only once."""
    _hold_job_a_with_task_1_running(env, extra_ready_tasks=1)
    _assert_ok(env.run(T0))

    env.task_status[(JOB_A, _task(1))] = "FAILED"
    env.set_job(JOB_A, {"SUCCEEDED": 1, "FAILED": 1, "READY": 1})
    _assert_ok(env.run(T0 + timedelta(minutes=5)))
    assert env.downloaded == []

    env.set_job(
        JOB_A, {"SUCCEEDED": 1, "FAILED": 1, "CANCELED": 1}, ended_at=T0 + timedelta(minutes=9)
    )
    _assert_ok(env.run(T0 + timedelta(minutes=10)))
    assert env.downloaded == [env.output_path(JOB_A, _task(2))]

    _assert_ok(env.run(T0 + timedelta(minutes=15)))
    assert env.downloaded == []


def test_only_the_session_with_an_unfinished_task_is_held(env):
    """Two sessions in one job: the held one is skipped and the other downloads normally.
    The job's saved session-ended timestamp stays at or before the held session's end, so
    the held session is still listed when its task finishes."""
    held_ended_at = T0 - timedelta(minutes=60)
    env.set_job(JOB_A, {"SUCCEEDED": 1, "RUNNING": 1})
    env.sessions[JOB_A] = [
        _Session(SESSION_1, [_Action(0, _task(1))], ended_at=held_ended_at),
        _Session(SESSION_2, [_Action(0, _task(2))], ended_at=T0 - timedelta(minutes=10)),
    ]
    env.task_status[(JOB_A, _task(1))] = "RUNNING"
    env.task_status[(JOB_A, _task(2))] = "SUCCEEDED"

    _assert_ok(env.run(T0))
    assert env.downloaded == [env.output_path(JOB_A, _task(2))]
    assert env.completed_indexes(JOB_A) == {SESSION_2: 0}
    (job_a,) = [job for job in env.checkpoint().jobs if job.job_id == JOB_A]
    assert job_a.session_ended_timestamp is not None
    assert job_a.session_ended_timestamp <= held_ended_at

    env.task_status[(JOB_A, _task(1))] = "SUCCEEDED"
    env.set_job(JOB_A, {"SUCCEEDED": 2}, ended_at=T0 + timedelta(minutes=4))
    _assert_ok(env.run(T0 + timedelta(minutes=5)))
    assert env.downloaded == [env.output_path(JOB_A, _task(1))]
    assert env.completed_indexes(JOB_A) == {SESSION_1: 0, SESSION_2: 0}


def test_requeued_task_holds_its_earlier_session_until_it_finishes(env):
    """A task requeued after its session action succeeded holds that session until the
    requeued task finishes, then its output downloads once."""
    env.set_job(JOB_A, {"SUCCEEDED": 1, "READY": 1})
    env.sessions[JOB_A] = [_Session(SESSION_1, [_Action(0, _task(1)), _Action(1, _task(2))])]
    env.task_status[(JOB_A, _task(1))] = "READY"
    env.task_status[(JOB_A, _task(2))] = "SUCCEEDED"

    _assert_ok(env.run(T0))
    assert env.downloaded == []
    assert env.completed_indexes(JOB_A) == {}

    # The requeued task is running again in a new session: still held.
    env.task_status[(JOB_A, _task(1))] = "RUNNING"
    env.sessions[JOB_A].append(
        _Session(SESSION_2, [_Action(0, _task(1), status="RUNNING")], ended_at=None)
    )
    env.set_job(JOB_A, {"SUCCEEDED": 1, "RUNNING": 1})
    _assert_ok(env.run(T0 + timedelta(minutes=5)))
    assert env.downloaded == []

    env.task_status[(JOB_A, _task(1))] = "SUCCEEDED"
    env.sessions[JOB_A][1] = _Session(
        SESSION_2, [_Action(0, _task(1))], ended_at=T0 + timedelta(minutes=8)
    )
    env.set_job(JOB_A, {"SUCCEEDED": 2}, ended_at=T0 + timedelta(minutes=9))
    _assert_ok(env.run(T0 + timedelta(minutes=10)))
    assert sorted(env.downloaded) == sorted(
        [env.output_path(JOB_A, _task(1)), env.output_path(JOB_A, _task(2))]
    )
    assert env.completed_indexes(JOB_A) == {SESSION_1: 1, SESSION_2: 0}


def test_task_whose_status_cannot_be_read_holds_its_session(env):
    """A task BatchGetTask reports an error for is treated as unfinished, so nothing is
    downloaded early, and its job is retried like a failed download."""
    env.set_job(JOB_A, {"SUCCEEDED": 1, "RUNNING": 1})
    env.sessions[JOB_A] = [_Session(SESSION_1, [_Action(0, _task(1)), _Action(1, _task(2))])]
    env.task_status[(JOB_A, _task(2))] = "SUCCEEDED"

    _assert_ok(env.run(T0))

    assert env.downloaded == []
    assert JOB_A not in {job.job_id for job in env.checkpoint().jobs}
    assert env.failed_jobs() == {JOB_A: 1}


def test_missing_get_task_permission_fails_the_run(env):
    """Without permission to read tasks, the run fails with a message naming the permission,
    instead of recording every job as failed until the retry cap abandons them."""
    env.set_job(JOB_A, {"SUCCEEDED": 1, "RUNNING": 1})
    env.sessions[JOB_A] = [_Session(SESSION_1, [_Action(0, _task(1))])]
    env.task_status[(JOB_A, _task(1))] = "SUCCEEDED"
    _assert_ok(env.run(T0))
    checkpoint_before = env.checkpoint_text()

    env.sessions[JOB_A].append(
        _Session(SESSION_2, [_Action(0, _task(2))], ended_at=T0 + timedelta(minutes=3))
    )
    env.task_errors[(JOB_A, _task(2))] = "AccessDeniedException"
    env.set_job(JOB_A, {"SUCCEEDED": 2}, ended_at=T0 + timedelta(minutes=4))
    result = env.run(T0 + timedelta(minutes=5))

    assert result.exit_code != 0, result.output
    assert "deadline:GetTask" in result.output, result.output
    assert env.downloaded == []
    assert env.checkpoint_text() == checkpoint_before
    assert env.failed_jobs() == {}


def test_unreadable_task_status_on_an_ended_job_is_retried(env):
    """The job ends in the same run that a task's status can't be read. Nothing else would
    make a later run look at the job again, so it is retried and downloads once the status
    can be read."""
    env.set_job(JOB_A, {"SUCCEEDED": 2}, ended_at=T0 - timedelta(minutes=8))
    env.sessions[JOB_A] = [_Session(SESSION_1, [_Action(0, _task(1)), _Action(1, _task(2))])]
    env.task_status[(JOB_A, _task(2))] = "SUCCEEDED"

    _assert_ok(env.run(T0))
    assert env.downloaded == []

    env.task_status[(JOB_A, _task(1))] = "SUCCEEDED"
    _assert_ok(env.run(T0 + timedelta(minutes=5)))

    assert sorted(env.downloaded) == sorted(
        [env.output_path(JOB_A, _task(1)), env.output_path(JOB_A, _task(2))]
    )
    assert env.completed_indexes(JOB_A) == {SESSION_1: 1}
    assert env.failed_jobs() == {}


def test_task_run_without_a_task_id_is_downloaded(env):
    """A taskRun action that names no task can't be checked, so it downloads as before
    instead of failing the run."""
    env.set_job(JOB_A, {"SUCCEEDED": 1}, ended_at=T0 - timedelta(minutes=8))
    env.sessions[JOB_A] = [_Session(SESSION_1, [_Action(0, _task(1))])]
    env.task_status[(JOB_A, _task(1))] = "SUCCEEDED"
    env.drop_task_id_from = {(JOB_A, SESSION_1, 0)}

    result = env.run(T0)

    _assert_ok(result)
    assert env.downloaded == [env.output_path(JOB_A, _task(1))]
    assert env.batch_get_task_calls == []


def test_task_that_later_failed_does_not_clear_its_farm_failed_entry(env):
    """A task whose first session action succeeded but which was requeued and then failed
    stays farm_failed in the status file."""
    env.set_job(JOB_A, {"SUCCEEDED": 1, "FAILED": 1}, ended_at=T0 - timedelta(minutes=8))
    env.sessions[JOB_A] = [
        _Session(SESSION_1, [_Action(0, _task(1))], ended_at=T0 - timedelta(minutes=20)),
        _Session(SESSION_2, [_Action(0, _task(1), status="FAILED")]),
        _Session(SESSION_3, [_Action(0, _task(2))]),
    ]
    env.task_status[(JOB_A, _task(1))] = "FAILED"
    env.task_status[(JOB_A, _task(2))] = "SUCCEEDED"

    _assert_ok(env.run(T0))

    assert env.downloaded == [env.output_path(JOB_A, _task(2))]
    with open(
        os.path.join(
            env.checkpoint_dir, f"{MOCK_QUEUE_ID}_ignore-storage-profiles_download_status.json"
        )
    ) as f:
        status = json.load(f)
    tasks = status["jobs"][JOB_A]["tasks"]
    assert tasks[_task(1)]["download_status"] == "farm_failed", tasks


@pytest.mark.parametrize("status_code", [404, 403])
@pytest.mark.parametrize("compact", [False, True], ids=["none-in-slot", "compacted"])
def test_missing_manifest_fails_only_its_job(env, status_code, compact):
    """A manifest that is missing (404) or forbidden (403) for one job: other jobs download,
    the checkpoint is saved, and the job is dropped from the checkpoint and recorded for
    retry."""
    env.compact_skipped_manifests = compact
    env.set_job(JOB_A, {"SUCCEEDED": 1}, ended_at=T0 - timedelta(minutes=8))
    env.sessions[JOB_A] = [_Session(SESSION_1, [_Action(0, _task(1))])]
    env.task_status[(JOB_A, _task(1))] = "SUCCEEDED"
    env.manifest_errors[(JOB_A, _task(1))] = status_code
    env.set_job(JOB_B, {"SUCCEEDED": 1}, ended_at=T0 - timedelta(minutes=8))
    env.sessions[JOB_B] = [_Session(SESSION_2, [_Action(0, _task(9))])]
    env.task_status[(JOB_B, _task(9))] = "SUCCEEDED"

    result = env.run(T0)

    _assert_ok(result)
    assert env.downloaded == [env.output_path(JOB_B, _task(9))]
    assert env.failed_jobs() == {JOB_A: 1}
    job_ids = {job.job_id for job in env.checkpoint().jobs}
    assert JOB_A not in job_ids
    assert JOB_B in job_ids
    assert env.checkpoint().downloads_completed_timestamp == T0 - timedelta(
        seconds=env.checkpoint().eventual_consistency_max_seconds
    )


def test_every_manifest_forbidden_fails_the_run(env):
    """When every manifest is forbidden the run fails with the permissions error and the
    checkpoint is not advanced."""
    env.set_job(JOB_A, {"SUCCEEDED": 1, "RUNNING": 1})
    env.sessions[JOB_A] = [_Session(SESSION_1, [_Action(0, _task(1))])]
    env.task_status[(JOB_A, _task(1))] = "SUCCEEDED"
    _assert_ok(env.run(T0))
    checkpoint_before = env.checkpoint_text()

    env.sessions[JOB_A].append(
        _Session(SESSION_2, [_Action(0, _task(2))], ended_at=T0 + timedelta(minutes=3))
    )
    env.task_status[(JOB_A, _task(2))] = "SUCCEEDED"
    env.manifest_errors[(JOB_A, _task(2))] = 403
    env.set_job(JOB_A, {"SUCCEEDED": 2}, ended_at=T0 + timedelta(minutes=4))
    result = env.run(T0 + timedelta(minutes=5))

    assert result.exit_code != 0, result.output
    assert "JobAttachmentsS3ClientError" in result.output, result.output
    assert "HTTP Status Code: 403" in result.output, result.output
    assert env.downloaded == []
    assert env.checkpoint_text() == checkpoint_before


@pytest.mark.parametrize(
    "run_status, expected",
    [
        ("SUCCEEDED", "download"),
        ("FAILED", "skip"),
        ("CANCELED", "skip"),
        ("SUSPENDED", "skip"),
        ("NOT_COMPATIBLE", "skip"),
        ("RUNNING", "hold"),
        ("READY", "hold"),
        ("ASSIGNED", "hold"),
        ("SCHEDULED", "hold"),
        ("STARTING", "hold"),
        ("INTERRUPTING", "hold"),
        ("PENDING", "hold"),
        (None, "hold"),
    ],
)
def test_task_run_disposition(run_status, expected):
    """Unfinished and unknown statuses hold the session; finished ones do not."""
    assert mod._task_run_disposition(run_status) == expected
