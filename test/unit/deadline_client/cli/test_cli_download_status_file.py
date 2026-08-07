# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the CLI download status file module.
"""

import json
import os
import time
from typing import Any, Optional

from deadline.client.cli._download_status_file import (
    _atomic_write_json,
    _build_status_file_content,
    _determine_job_download_status,
    _get_status_file_paths,
    _status_file_lock,
    write_download_status_file,
)
from deadline.client.cli._incremental_download import (
    CategorizedJobIds,
    _FailedJobsTracker,
    _MAX_FAILED_JOB_RETRIES,
)
from deadline.client.cli._download_status_file import _is_job_fully_complete

from ..shared_constants import MOCK_QUEUE_ID, MOCK_STORAGE_PROFILE_ID, MOCK_JOB_ID


MOCK_JOB_ID_2 = "job-aaaabbbbccccddddeeeeffffaaaabbbb"
MOCK_JOB_ID_3 = "job-11112222333344445555666677778888"
MOCK_JOB_ID_4 = "job-99998888777766665555444433332222"


def _make_categorized_job_ids(**kwargs) -> CategorizedJobIds:
    """Helper to create a CategorizedJobIds with specified sets."""
    cjids = CategorizedJobIds()
    cjids.added = kwargs.get("added", set())
    cjids.updated = kwargs.get("updated", set())
    cjids.unchanged = kwargs.get("unchanged", set())
    cjids.completed = kwargs.get("completed", set())
    cjids.inactive = kwargs.get("inactive", set())
    cjids.attachments_free = kwargs.get("attachments_free", set())
    cjids.missing_storage_profile = kwargs.get("missing_storage_profile", set())
    return cjids


def _make_job(
    job_id: str,
    succeeded: int = 1,
    total: int = 1,
    ended: bool = True,
    attachments: bool = True,
    storage_profile_id: Optional[str] = MOCK_STORAGE_PROFILE_ID,
    failed: int = 0,
) -> dict[str, Any]:
    """Helper to create a fake job dict.

    Any tasks not accounted for by ``succeeded`` or ``failed`` are treated as still
    RUNNING (i.e. active). Pass ``failed`` to model tasks that failed on the farm — a
    failed task is terminal, so it does not count as active for completeness checks.
    """
    job: dict[str, Any] = {
        "jobId": job_id,
        "name": f"test-job-{job_id[-8:]}",
        "taskRunStatusCounts": {
            "SUCCEEDED": succeeded,
            "FAILED": failed,
            "RUNNING": total - succeeded - failed,
            "READY": 0,
            "PENDING": 0,
            "ASSIGNED": 0,
            "STARTING": 0,
            "SCHEDULED": 0,
            "INTERRUPTING": 0,
            "SUSPENDED": 0,
            "CANCELED": 0,
            "NOT_COMPATIBLE": 0,
        },
        "attachments": {"manifests": []} if attachments else None,
        "storageProfileId": storage_profile_id,
    }
    if ended:
        job["endedAt"] = "2026-06-15T12:00:00+00:00"
    return job


class TestDetermineJobDownloadStatus:
    """Tests for _determine_job_download_status."""

    def test_completed_job_returns_downloaded(self):
        cjids = _make_categorized_job_ids(completed={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID)
        result = _determine_job_download_status(MOCK_JOB_ID, job, cjids)
        assert result["download_status"] == "downloaded"

    def test_added_job_all_tasks_succeeded_no_active_returns_downloaded(self):
        """Added job with all tasks succeeded and no active tasks is truly done."""
        cjids = _make_categorized_job_ids(added={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID, succeeded=5, total=5, ended=True)
        result = _determine_job_download_status(MOCK_JOB_ID, job, cjids)
        assert result["download_status"] == "downloaded"

    def test_added_job_requeued_with_active_tasks_returns_in_progress(self):
        """Requeued job has active tasks (READY) so should be in_progress even if endedAt is set."""
        cjids = _make_categorized_job_ids(added={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID, succeeded=5, total=5, ended=True)
        job["taskRunStatusCounts"]["READY"] = 3
        job["taskRunStatusCounts"]["RUNNING"] = 0
        result = _determine_job_download_status(MOCK_JOB_ID, job, cjids)
        assert result["download_status"] == "in_progress"

    def test_added_job_partial_tasks_returns_in_progress(self):
        cjids = _make_categorized_job_ids(added={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID, succeeded=3, total=10, ended=False)
        result = _determine_job_download_status(MOCK_JOB_ID, job, cjids)
        assert result["download_status"] == "in_progress"

    def test_updated_job_partial_tasks_returns_in_progress(self):
        cjids = _make_categorized_job_ids(updated={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID, succeeded=7, total=10, ended=False)
        result = _determine_job_download_status(MOCK_JOB_ID, job, cjids)
        assert result["download_status"] == "in_progress"

    def test_unchanged_job_all_succeeded_returns_downloaded(self):
        cjids = _make_categorized_job_ids(unchanged={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID, succeeded=10, total=10, ended=True)
        result = _determine_job_download_status(MOCK_JOB_ID, job, cjids)
        assert result["download_status"] == "downloaded"

    def test_unchanged_job_partial_returns_in_progress(self):
        cjids = _make_categorized_job_ids(unchanged={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID, succeeded=5, total=10, ended=False)
        result = _determine_job_download_status(MOCK_JOB_ID, job, cjids)
        assert result["download_status"] == "in_progress"

    def test_attachments_free_returns_skipped(self):
        cjids = _make_categorized_job_ids(attachments_free={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID, attachments=False)
        result = _determine_job_download_status(MOCK_JOB_ID, job, cjids)
        assert result["download_status"] == "skipped"

    def test_missing_storage_profile_returns_skipped(self):
        cjids = _make_categorized_job_ids(missing_storage_profile={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID, storage_profile_id=None)
        result = _determine_job_download_status(MOCK_JOB_ID, job, cjids)
        assert result["download_status"] == "skipped"

    def test_result_has_all_required_fields(self):
        cjids = _make_categorized_job_ids(completed={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID)
        result = _determine_job_download_status(MOCK_JOB_ID, job, cjids)
        assert "download_status" in result
        assert "total_files" in result
        assert "downloaded_files" in result
        assert "failed_files" in result
        assert "last_updated" in result
        assert "error_code" in result
        assert "error_message" in result
        assert "skip_reason" in result
        assert "tasks" in result


class TestIsJobFullyComplete:
    """Tests for _is_job_fully_complete — the completeness gate used to decide
    downloaded vs in_progress."""

    def test_ended_with_no_active_tasks_is_complete(self):
        job = _make_job(MOCK_JOB_ID, succeeded=5, total=5, ended=True)
        assert _is_job_fully_complete(job) is True

    def test_not_ended_is_not_complete(self):
        job = _make_job(MOCK_JOB_ID, succeeded=5, total=5, ended=False)
        assert _is_job_fully_complete(job) is False

    def test_ended_but_active_tasks_remain_is_not_complete(self):
        """A requeued job can carry endedAt yet still have READY/RUNNING tasks."""
        job = _make_job(MOCK_JOB_ID, succeeded=2, total=5, ended=True)  # 3 RUNNING
        assert _is_job_fully_complete(job) is False

    def test_ended_with_failed_tasks_but_none_active_is_complete(self):
        """Pins the _is_job_fully_complete change: a FAILED task is terminal, not active,
        so a job that ended with some farm-failed tasks and no still-running tasks is
        complete — all downloadable output (from succeeded tasks) is available."""
        job = _make_job(MOCK_JOB_ID, succeeded=3, failed=2, total=5, ended=True)
        assert job["taskRunStatusCounts"]["RUNNING"] == 0
        assert _is_job_fully_complete(job) is True

    def test_ended_with_failed_and_still_running_is_not_complete(self):
        job = _make_job(MOCK_JOB_ID, succeeded=1, failed=1, total=5, ended=True)  # 3 RUNNING
        assert _is_job_fully_complete(job) is False


class TestFailedJobsTracker:
    """Tests for _FailedJobsTracker — the per-job retry/abandon bookkeeping."""

    def _tracker(self, tmp_path):
        return _FailedJobsTracker(str(tmp_path / "failed_jobs.json"))

    def test_record_failure_increments_and_is_tracked(self, tmp_path):
        tracker = self._tracker(tmp_path)
        tracker.record_failures({MOCK_JOB_ID}, print_function_callback=lambda *_: None)
        assert MOCK_JOB_ID in tracker.get_tracked_job_ids()
        assert tracker.is_abandoned(MOCK_JOB_ID) is False

    def test_abandoned_after_max_retries_and_warns(self, tmp_path):
        tracker = self._tracker(tmp_path)
        warnings: list[str] = []
        for _ in range(_MAX_FAILED_JOB_RETRIES):
            tracker.record_failures({MOCK_JOB_ID}, print_function_callback=warnings.append)
        assert tracker.is_abandoned(MOCK_JOB_ID) is True
        # An abandoned job is no longer offered for retry...
        assert MOCK_JOB_ID not in tracker.get_tracked_job_ids()
        # ...but it is NOT dropped, so a timestamp-window rediscovery stays suppressed.
        assert any("no longer be retried" in w for w in warnings)

    def test_record_success_clears_job(self, tmp_path):
        tracker = self._tracker(tmp_path)
        tracker.record_failures({MOCK_JOB_ID}, print_function_callback=lambda *_: None)
        tracker.record_successes({MOCK_JOB_ID})
        assert MOCK_JOB_ID not in tracker.get_tracked_job_ids()
        assert tracker.is_abandoned(MOCK_JOB_ID) is False

    def test_save_and_load_round_trips_counts(self, tmp_path):
        path = str(tmp_path / "failed_jobs.json")
        tracker = _FailedJobsTracker(path)
        tracker.record_failures({MOCK_JOB_ID}, print_function_callback=lambda *_: None)
        tracker.record_failures({MOCK_JOB_ID}, print_function_callback=lambda *_: None)
        tracker.save()

        reloaded = _FailedJobsTracker(path)
        # A fresh tracker over the same file sees the persisted count (2, below the cap).
        assert MOCK_JOB_ID in reloaded.get_tracked_job_ids()
        reloaded.record_failures({MOCK_JOB_ID}, print_function_callback=lambda *_: None)
        assert reloaded._counts[MOCK_JOB_ID] == 3

    def test_load_tolerates_corrupt_file(self, tmp_path):
        path = str(tmp_path / "failed_jobs.json")
        with open(path, "w") as f:
            f.write("{ not valid json")
        tracker = _FailedJobsTracker(path)  # must not raise
        assert tracker.get_tracked_job_ids() == set()


class TestGetStatusFilePaths:
    """Tests for _get_status_file_paths."""

    def test_no_storage_profile_returns_local_path(self):
        paths = _get_status_file_paths(
            queue_id=MOCK_QUEUE_ID,
            local_storage_profile_id=None,
            local_storage_profile=None,
            checkpoint_dir="/home/user/.deadline/incremental_download",
        )
        assert len(paths) == 1
        assert "ignore-storage-profiles" in paths[0]
        assert MOCK_QUEUE_ID in paths[0]
        assert paths[0].endswith("_download_status.json")

    def test_storage_profile_single_location(self, tmp_path):
        renders_dir = tmp_path / "renders"
        renders_dir.mkdir()
        profile = {
            "fileSystemLocations": [
                {"name": "renders", "path": str(renders_dir)},
            ]
        }
        paths = _get_status_file_paths(
            queue_id=MOCK_QUEUE_ID,
            local_storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            local_storage_profile=profile,
            checkpoint_dir="/home/user/.deadline/incremental_download",
        )
        assert len(paths) == 1
        expected = os.path.join(
            str(renders_dir), ".deadline", f"{MOCK_QUEUE_ID}_download_status.json"
        )
        assert paths[0] == expected

    def test_storage_profile_multiple_locations(self, tmp_path):
        renders_dir = tmp_path / "renders"
        projects_dir = tmp_path / "projects"
        tools_dir = tmp_path / "tools"
        for d in [renders_dir, projects_dir, tools_dir]:
            d.mkdir()
        profile = {
            "fileSystemLocations": [
                {"name": "renders", "path": str(renders_dir)},
                {"name": "projects", "path": str(projects_dir)},
                {"name": "tools", "path": str(tools_dir)},
            ]
        }
        paths = _get_status_file_paths(
            queue_id=MOCK_QUEUE_ID,
            local_storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            local_storage_profile=profile,
            checkpoint_dir="/home/user/.deadline/incremental_download",
        )
        assert len(paths) == 3
        assert (
            os.path.join(str(renders_dir), ".deadline", f"{MOCK_QUEUE_ID}_download_status.json")
            in paths
        )
        assert (
            os.path.join(str(projects_dir), ".deadline", f"{MOCK_QUEUE_ID}_download_status.json")
            in paths
        )
        assert (
            os.path.join(str(tools_dir), ".deadline", f"{MOCK_QUEUE_ID}_download_status.json")
            in paths
        )

    def test_unmounted_location_excluded(self, tmp_path):
        """Locations whose root does not exist are excluded to avoid phantom writes."""
        renders_dir = tmp_path / "renders"
        renders_dir.mkdir()
        profile = {
            "fileSystemLocations": [
                {"name": "renders", "path": str(renders_dir)},
                {"name": "unmounted", "path": "/nonexistent/mount/point"},
            ]
        }
        paths = _get_status_file_paths(
            queue_id=MOCK_QUEUE_ID,
            local_storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            local_storage_profile=profile,
            checkpoint_dir="/home/user/.deadline/incremental_download",
        )
        assert len(paths) == 1
        assert str(renders_dir) in paths[0]


class TestBuildStatusFileContent:
    """Tests for _build_status_file_content."""

    def test_builds_valid_structure(self):
        cjids = _make_categorized_job_ids(completed={MOCK_JOB_ID})
        jobs = {MOCK_JOB_ID: _make_job(MOCK_JOB_ID)}
        result = _build_status_file_content(
            queue_id=MOCK_QUEUE_ID,
            storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            categorized_job_ids=cjids,
            download_candidate_jobs=jobs,
        )
        assert result["schema_version"] == 1
        assert result["sync_metadata"]["queue_id"] == MOCK_QUEUE_ID
        assert result["sync_metadata"]["storage_profile_id"] == MOCK_STORAGE_PROFILE_ID
        assert result["sync_metadata"]["last_run_status"] == "success"
        assert "hostname" in result["sync_metadata"]
        assert "last_sync_completed_at" in result["sync_metadata"]
        assert MOCK_JOB_ID in result["jobs"]

    def test_multiple_jobs_all_included(self):
        cjids = _make_categorized_job_ids(
            completed={MOCK_JOB_ID},
            added={MOCK_JOB_ID_2},
            attachments_free={MOCK_JOB_ID_3},
        )
        jobs = {
            MOCK_JOB_ID: _make_job(MOCK_JOB_ID),
            MOCK_JOB_ID_2: _make_job(MOCK_JOB_ID_2, succeeded=3, total=10, ended=False),
            MOCK_JOB_ID_3: _make_job(MOCK_JOB_ID_3, attachments=False),
        }
        result = _build_status_file_content(
            queue_id=MOCK_QUEUE_ID,
            storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            categorized_job_ids=cjids,
            download_candidate_jobs=jobs,
        )
        assert len(result["jobs"]) == 3
        assert result["jobs"][MOCK_JOB_ID]["download_status"] == "downloaded"
        assert result["jobs"][MOCK_JOB_ID_2]["download_status"] == "in_progress"
        assert result["jobs"][MOCK_JOB_ID_3]["download_status"] == "skipped"

    def test_no_storage_profile_sets_null(self):
        cjids = _make_categorized_job_ids(completed={MOCK_JOB_ID})
        jobs = {MOCK_JOB_ID: _make_job(MOCK_JOB_ID)}
        result = _build_status_file_content(
            queue_id=MOCK_QUEUE_ID,
            storage_profile_id=None,
            categorized_job_ids=cjids,
            download_candidate_jobs=jobs,
        )
        assert result["sync_metadata"]["storage_profile_id"] is None

    def test_merges_with_existing_jobs(self):
        """Old jobs that dropped out of tracking are preserved in the status file."""
        existing_jobs = {
            "job-old-completed-aaaaaa": {
                "download_status": "downloaded",
                "total_files": 5,
                "downloaded_files": 5,
                "failed_files": 0,
                "last_updated": "2026-06-10T12:00:00+00:00",
                "error_code": None,
                "error_message": None,
            }
        }
        cjids = _make_categorized_job_ids(completed={MOCK_JOB_ID})
        jobs = {MOCK_JOB_ID: _make_job(MOCK_JOB_ID)}
        result = _build_status_file_content(
            queue_id=MOCK_QUEUE_ID,
            storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            categorized_job_ids=cjids,
            download_candidate_jobs=jobs,
            existing_jobs=existing_jobs,
        )
        assert len(result["jobs"]) == 2
        assert "job-old-completed-aaaaaa" in result["jobs"]
        assert result["jobs"]["job-old-completed-aaaaaa"]["download_status"] == "downloaded"
        assert result["jobs"][MOCK_JOB_ID]["download_status"] == "downloaded"

    def test_current_run_overwrites_existing_entry(self):
        """If a job exists in the file and is also in the current run, current run wins."""
        existing_jobs = {
            MOCK_JOB_ID: {
                "download_status": "downloaded",
                "total_files": 5,
                "downloaded_files": 5,
                "failed_files": 0,
                "last_updated": "2026-06-10T12:00:00+00:00",
                "error_code": None,
                "error_message": None,
            }
        }
        cjids = _make_categorized_job_ids(added={MOCK_JOB_ID})
        jobs = {MOCK_JOB_ID: _make_job(MOCK_JOB_ID, succeeded=2, total=10, ended=False)}
        result = _build_status_file_content(
            queue_id=MOCK_QUEUE_ID,
            storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            categorized_job_ids=cjids,
            download_candidate_jobs=jobs,
            existing_jobs=existing_jobs,
        )
        assert result["jobs"][MOCK_JOB_ID]["download_status"] == "in_progress"


class TestAtomicWriteJson:
    """Tests for _atomic_write_json."""

    def test_writes_valid_json(self, tmp_path):
        file_path = str(tmp_path / "test_status.json")
        data = {"schema_version": 1, "jobs": {}}
        _atomic_write_json(file_path, data)

        with open(file_path, "r") as f:
            loaded = json.load(f)
        assert loaded == data

    def test_creates_parent_directories(self, tmp_path):
        file_path = str(tmp_path / "nested" / "dir" / "status.json")
        data = {"test": True}
        _atomic_write_json(file_path, data)

        assert os.path.exists(file_path)
        with open(file_path, "r") as f:
            assert json.load(f) == data

    def test_overwrites_existing_file(self, tmp_path):
        file_path = str(tmp_path / "status.json")
        _atomic_write_json(file_path, {"version": 1})
        _atomic_write_json(file_path, {"version": 2})

        with open(file_path, "r") as f:
            assert json.load(f)["version"] == 2


class TestWriteDownloadStatusFile:
    """Tests for the main write_download_status_file function."""

    def test_writes_to_storage_profile_locations(self, tmp_path):
        renders_dir = tmp_path / "renders"
        renders_dir.mkdir()
        profile = {
            "fileSystemLocations": [
                {"name": "renders", "path": str(renders_dir)},
            ]
        }
        cjids = _make_categorized_job_ids(completed={MOCK_JOB_ID})
        jobs = {MOCK_JOB_ID: _make_job(MOCK_JOB_ID)}
        messages: list[str] = []

        write_download_status_file(
            queue_id=MOCK_QUEUE_ID,
            categorized_job_ids=cjids,
            download_candidate_jobs=jobs,
            local_storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            local_storage_profile=profile,
            checkpoint_dir=str(tmp_path / "checkpoint"),
            print_function_callback=messages.append,
        )

        status_file = renders_dir / ".deadline" / f"{MOCK_QUEUE_ID}_download_status.json"
        assert status_file.exists()
        with open(status_file) as f:
            data = json.load(f)
        assert data["jobs"][MOCK_JOB_ID]["download_status"] == "downloaded"
        assert any("saved" in msg for msg in messages)

    def test_writes_to_ignore_storage_profiles_path(self, tmp_path):
        checkpoint_dir = tmp_path / "checkpoint"
        checkpoint_dir.mkdir()
        cjids = _make_categorized_job_ids(added={MOCK_JOB_ID})
        jobs = {MOCK_JOB_ID: _make_job(MOCK_JOB_ID, succeeded=3, total=10, ended=False)}
        messages: list[str] = []

        write_download_status_file(
            queue_id=MOCK_QUEUE_ID,
            categorized_job_ids=cjids,
            download_candidate_jobs=jobs,
            local_storage_profile_id=None,
            local_storage_profile=None,
            checkpoint_dir=str(checkpoint_dir),
            print_function_callback=messages.append,
        )

        status_file = (
            checkpoint_dir / f"{MOCK_QUEUE_ID}_ignore-storage-profiles_download_status.json"
        )
        assert status_file.exists()
        with open(status_file) as f:
            data = json.load(f)
        assert data["jobs"][MOCK_JOB_ID]["download_status"] == "in_progress"

    def test_writes_to_multiple_locations(self, tmp_path):
        renders_dir = tmp_path / "renders"
        projects_dir = tmp_path / "projects"
        renders_dir.mkdir()
        projects_dir.mkdir()
        profile = {
            "fileSystemLocations": [
                {"name": "renders", "path": str(renders_dir)},
                {"name": "projects", "path": str(projects_dir)},
            ]
        }
        cjids = _make_categorized_job_ids(completed={MOCK_JOB_ID})
        jobs = {MOCK_JOB_ID: _make_job(MOCK_JOB_ID)}

        write_download_status_file(
            queue_id=MOCK_QUEUE_ID,
            categorized_job_ids=cjids,
            download_candidate_jobs=jobs,
            local_storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            local_storage_profile=profile,
            checkpoint_dir=str(tmp_path / "checkpoint"),
        )

        assert (renders_dir / ".deadline" / f"{MOCK_QUEUE_ID}_download_status.json").exists()
        assert (projects_dir / ".deadline" / f"{MOCK_QUEUE_ID}_download_status.json").exists()

    def test_warns_on_write_failure_does_not_raise(self, tmp_path):
        # Location root exists (passes the mount check) but .deadline subdir creation fails
        # because a file with that name already exists.
        renders_dir = tmp_path / "renders"
        renders_dir.mkdir()
        deadline_blocker = renders_dir / ".deadline"
        deadline_blocker.write_text("not a directory")  # blocks os.makedirs inside the lock
        profile = {
            "fileSystemLocations": [
                {"name": "renders", "path": str(renders_dir)},
            ]
        }
        cjids = _make_categorized_job_ids(completed={MOCK_JOB_ID})
        jobs = {MOCK_JOB_ID: _make_job(MOCK_JOB_ID)}
        messages: list[str] = []

        write_download_status_file(
            queue_id=MOCK_QUEUE_ID,
            categorized_job_ids=cjids,
            download_candidate_jobs=jobs,
            local_storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            local_storage_profile=profile,
            checkpoint_dir=str(tmp_path),
            print_function_callback=messages.append,
        )

        assert any("WARNING" in msg for msg in messages)

    def test_json_is_valid_and_parseable(self, tmp_path):
        checkpoint_dir = tmp_path / "checkpoint"
        checkpoint_dir.mkdir()
        cjids = _make_categorized_job_ids(
            completed={MOCK_JOB_ID},
            added={MOCK_JOB_ID_2},
            attachments_free={MOCK_JOB_ID_3},
        )
        jobs = {
            MOCK_JOB_ID: _make_job(MOCK_JOB_ID),
            MOCK_JOB_ID_2: _make_job(MOCK_JOB_ID_2, succeeded=2, total=5, ended=False),
            MOCK_JOB_ID_3: _make_job(MOCK_JOB_ID_3, attachments=False),
        }

        write_download_status_file(
            queue_id=MOCK_QUEUE_ID,
            categorized_job_ids=cjids,
            download_candidate_jobs=jobs,
            local_storage_profile_id=None,
            local_storage_profile=None,
            checkpoint_dir=str(checkpoint_dir),
        )

        status_file = (
            checkpoint_dir / f"{MOCK_QUEUE_ID}_ignore-storage-profiles_download_status.json"
        )
        with open(status_file) as f:
            data = json.load(f)

        assert data["schema_version"] == 1
        assert len(data["jobs"]) == 3
        assert data["jobs"][MOCK_JOB_ID]["download_status"] == "downloaded"
        assert data["jobs"][MOCK_JOB_ID_2]["download_status"] == "in_progress"
        assert data["jobs"][MOCK_JOB_ID_3]["download_status"] == "skipped"


class TestStatusFileLock:
    """Tests for _status_file_lock cooperative NAS lock."""

    def test_lock_file_created_and_removed(self, tmp_path):
        """Lock file exists during context and is removed after."""
        status_file = str(tmp_path / "status.json")
        lock_file = status_file + ".lock"

        with _status_file_lock(status_file):
            assert os.path.exists(lock_file)

        assert not os.path.exists(lock_file)

    def test_lock_file_removed_on_exception(self, tmp_path):
        """Lock file is cleaned up even if an exception occurs inside the context."""
        status_file = str(tmp_path / "status.json")
        lock_file = status_file + ".lock"

        try:
            with _status_file_lock(status_file):
                raise RuntimeError("simulated error")
        except RuntimeError:
            pass

        assert not os.path.exists(lock_file)

    def test_stale_lock_is_overwritten(self, tmp_path):
        """A lock file older than TTL is treated as stale and overwritten."""
        status_file = str(tmp_path / "status.json")
        lock_file = status_file + ".lock"

        # Write a stale lock file (mtime in the past)
        os.makedirs(os.path.dirname(lock_file), exist_ok=True)
        with open(lock_file, "w") as f:
            f.write('{"hostname": "old-machine", "time": 0}')
        os.utime(lock_file, (0, 0))  # set mtime to epoch (definitely stale)

        # Should acquire successfully despite stale lock
        with _status_file_lock(status_file):
            assert os.path.exists(lock_file)
            # Verify it's our lock, not the old one
            with open(lock_file) as f:
                content = json.load(f)
            assert content["time"] > 0

        assert not os.path.exists(lock_file)

    def test_lock_contains_hostname(self, tmp_path):
        """Lock file contains hostname for debugging."""
        import socket

        status_file = str(tmp_path / "status.json")
        lock_file = status_file + ".lock"

        with _status_file_lock(status_file):
            with open(lock_file) as f:
                content = json.load(f)
            assert content["hostname"] == socket.gethostname()

    def test_slow_holder_does_not_unlink_takeover_lock(self, tmp_path, monkeypatch):
        """A holder whose hold outlives the TTL must not delete the lock a second writer took over.

        The first writer acquired legitimately, then stalled past the TTL. A second machine
        saw a stale lock and took ownership. When the first writer finally exits it must
        leave the takeover lock alone — unlinking it would let a third writer in while the
        second is mid-write, which is the lost-update the lock exists to prevent.
        """
        status_file = str(tmp_path / "status.json")
        lock_file = status_file + ".lock"

        monkeypatch.setattr(
            "deadline.client.cli._download_status_file._STATUS_FILE_LOCK_TTL_SECONDS", 0
        )

        with _status_file_lock(status_file):
            # Simulate a second machine treating our now-expired lock as stale and taking over.
            with open(lock_file, "w") as f:
                json.dump({"hostname": "other-machine", "time": 1.0}, f)

        # The takeover lock survives — release only unlinks a lock whose stored time matches.
        assert os.path.exists(lock_file), "slow holder deleted another writer's lock"
        with open(lock_file) as f:
            assert json.load(f)["hostname"] == "other-machine"

    def test_acquisition_timeout_proceeds_unlocked_with_warning(
        self, tmp_path, monkeypatch, caplog
    ):
        """When the lock can never be acquired the writer proceeds anyway and warns.

        A permanently-held lock (crashed peer whose clock keeps its lock looking fresh, or an
        O_EXCL that never succeeds on a flaky NAS) must not block the sync forever. Writing
        unlocked risks a lost update; hanging risks never reporting status at all. The chosen
        behavior is to warn and write.
        """
        import logging

        status_file = str(tmp_path / "status.json")
        lock_file = status_file + ".lock"

        # A fresh lock that never goes stale and never gets released.
        os.makedirs(os.path.dirname(lock_file), exist_ok=True)
        with open(lock_file, "w") as f:
            json.dump({"hostname": "peer", "time": time.time()}, f)

        # Collapse the wait so the timeout branch is reached without a 90s test.
        monkeypatch.setattr(
            "deadline.client.cli._download_status_file._STATUS_FILE_LOCK_MAX_WAIT_SECONDS", 0.05
        )
        monkeypatch.setattr(
            "deadline.client.cli._download_status_file._STATUS_FILE_LOCK_RETRY_INTERVAL_SECONDS",
            0.01,
        )

        entered = False
        with caplog.at_level(logging.WARNING, logger="deadline.client.cli._download_status_file"):
            with _status_file_lock(status_file):
                entered = True

        assert entered, "timeout must proceed into the body, not raise or hang"
        assert any("proceeding without lock" in r.message for r in caplog.records), caplog.records
        # The peer's lock is untouched — we never owned it, so we must not release it.
        with open(lock_file) as f:
            assert json.load(f)["hostname"] == "peer"

    def test_concurrent_writers_do_not_lose_updates_or_corrupt_json(self, tmp_path):
        """Real threads writing the same status file concurrently: every job survives and the
        file always parses.

        Each writer merges the on-disk jobs before writing, so with the lock held the writes
        serialize and the final file contains all of them. Without the lock the last writer's
        read would predate the others' writes and their entries would vanish.
        """
        import threading

        status_file = str(tmp_path / "status.json")
        writer_count = 8
        errors: list[Exception] = []
        barrier = threading.Barrier(writer_count)

        def writer(index: int) -> None:
            job_id = f"job-{index:032d}"
            try:
                barrier.wait(timeout=30)
                with _status_file_lock(status_file):
                    existing = {}
                    if os.path.exists(status_file):
                        with open(status_file) as f:
                            existing = json.load(f).get("jobs", {})
                    existing[job_id] = {"download_status": "downloaded"}
                    _atomic_write_json(status_file, {"schema_version": 1, "jobs": existing})
            except Exception as e:  # noqa: BLE001 - surfaced via assert below
                errors.append(e)

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(writer_count)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=120)

        assert not errors, errors
        with open(status_file) as f:
            data = json.load(f)  # Never a partial write — _atomic_write_json renames into place.
        assert set(data["jobs"]) == {f"job-{i:032d}" for i in range(writer_count)}, data["jobs"]
        assert not os.path.exists(status_file + ".lock"), "every writer released its lock"

    def test_corrupt_existing_status_file_is_replaced_not_fatal(self, tmp_path):
        """A truncated status file (killed mid-write on a filesystem without atomic rename)
        must not abort the run — the unreadable jobs are dropped and this run's are written."""
        status_dir = tmp_path / "renders"
        status_dir.mkdir()
        status_file = os.path.join(
            str(status_dir), ".deadline", f"{MOCK_QUEUE_ID}_download_status.json"
        )
        os.makedirs(os.path.dirname(status_file), exist_ok=True)
        with open(status_file, "w") as f:
            f.write('{"schema_version": 1, "jobs": {"job-old": {"download_status": "dow')

        cjids = _make_categorized_job_ids(completed={MOCK_JOB_ID})
        write_download_status_file(
            queue_id=MOCK_QUEUE_ID,
            categorized_job_ids=cjids,
            download_candidate_jobs={MOCK_JOB_ID: _make_job(MOCK_JOB_ID)},
            local_storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            local_storage_profile={
                "fileSystemLocations": [{"name": "renders", "path": str(status_dir)}]
            },
            checkpoint_dir=str(tmp_path / "checkpoint"),
        )

        with open(status_file) as f:
            data = json.load(f)
        assert data["jobs"][MOCK_JOB_ID]["download_status"] == "downloaded"
        # The corrupt content was unrecoverable, so the stale entry is gone rather than
        # blocking the write. Losing an unparseable entry beats reporting no status at all.
        assert "job-old" not in data["jobs"], data["jobs"]

    def test_unmounted_location_skipped_while_others_are_written(self, tmp_path):
        """One storage-profile location unmounted: the mounted locations are still written.

        A single unavailable NAS mount must not suppress status for the mounts that are up,
        and must not create a phantom status file under the empty mount point.
        """
        mounted = tmp_path / "mounted"
        mounted.mkdir()
        unmounted = tmp_path / "unmounted"  # deliberately not created

        cjids = _make_categorized_job_ids(completed={MOCK_JOB_ID})
        write_download_status_file(
            queue_id=MOCK_QUEUE_ID,
            categorized_job_ids=cjids,
            download_candidate_jobs={MOCK_JOB_ID: _make_job(MOCK_JOB_ID)},
            local_storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            local_storage_profile={
                "fileSystemLocations": [
                    {"name": "mounted", "path": str(mounted)},
                    {"name": "unmounted", "path": str(unmounted)},
                ]
            },
            checkpoint_dir=str(tmp_path / "checkpoint"),
        )

        assert os.path.exists(
            os.path.join(str(mounted), ".deadline", f"{MOCK_QUEUE_ID}_download_status.json")
        )
        assert not os.path.exists(str(unmounted)), "must not materialize an unmounted location"


class TestPerJobFileCountsAndErrors:
    """Tests for per-job file counts, error isolation, and failed status."""

    def test_file_counts_populated_from_download_results(self):
        """Job download results populate total_files and downloaded_files."""
        cjids = _make_categorized_job_ids(completed={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID)
        download_results = {
            MOCK_JOB_ID: {
                "total_files": 10,
                "downloaded_files": 10,
                "failed_files": 0,
                "error_code": None,
                "error_message": None,
            }
        }
        result = _determine_job_download_status(MOCK_JOB_ID, job, cjids, download_results)
        assert result["download_status"] == "downloaded"
        assert result["total_files"] == 10
        assert result["downloaded_files"] == 10
        assert result["failed_files"] == 0

    def test_failed_job_returns_failed_status(self):
        """A job with failed files returns 'failed' status with error info."""
        cjids = _make_categorized_job_ids(completed={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID)
        download_results = {
            MOCK_JOB_ID: {
                "total_files": 5,
                "downloaded_files": 0,
                "failed_files": 5,
                "error_code": "PERMISSION_DENIED",
                "error_message": "Access denied to S3 object",
            }
        }
        result = _determine_job_download_status(MOCK_JOB_ID, job, cjids, download_results)
        assert result["download_status"] == "failed"
        assert result["total_files"] == 5
        assert result["failed_files"] == 5
        assert result["error_code"] == "PERMISSION_DENIED"
        assert result["error_message"] == "Access denied to S3 object"

    def test_failed_status_overrides_category(self):
        """Even if categorized as 'completed', a failed download shows 'failed'."""
        cjids = _make_categorized_job_ids(added={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID, succeeded=3, total=10, ended=False)
        download_results = {
            MOCK_JOB_ID: {
                "total_files": 8,
                "downloaded_files": 0,
                "failed_files": 8,
                "error_code": "DISK_FULL",
                "error_message": "No space left on device",
            }
        }
        result = _determine_job_download_status(MOCK_JOB_ID, job, cjids, download_results)
        assert result["download_status"] == "failed"
        assert result["error_code"] == "DISK_FULL"

    def test_no_download_results_defaults_to_zero(self):
        """Without download results, file counts default to 0."""
        cjids = _make_categorized_job_ids(completed={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID)
        result = _determine_job_download_status(MOCK_JOB_ID, job, cjids, None)
        assert result["total_files"] == 0
        assert result["downloaded_files"] == 0
        assert result["failed_files"] == 0

    def test_last_run_status_failed_when_any_job_fails(self):
        """sync_metadata.last_run_status is 'failed' when any job has errors."""
        cjids = _make_categorized_job_ids(
            completed={MOCK_JOB_ID},
            added={MOCK_JOB_ID_2},
        )
        jobs = {
            MOCK_JOB_ID: _make_job(MOCK_JOB_ID),
            MOCK_JOB_ID_2: _make_job(MOCK_JOB_ID_2, succeeded=3, total=5, ended=False),
        }
        download_results: dict[str, dict[str, Any]] = {
            MOCK_JOB_ID: {
                "total_files": 3,
                "downloaded_files": 3,
                "failed_files": 0,
                "error_code": None,
                "error_message": None,
            },
            MOCK_JOB_ID_2: {
                "total_files": 5,
                "downloaded_files": 0,
                "failed_files": 5,
                "error_code": "NETWORK_ERROR",
                "error_message": "Connection timeout",
            },
        }
        result = _build_status_file_content(
            queue_id=MOCK_QUEUE_ID,
            storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            categorized_job_ids=cjids,
            download_candidate_jobs=jobs,
            job_download_results=download_results,
        )
        assert result["sync_metadata"]["last_run_status"] == "failed"
        assert result["jobs"][MOCK_JOB_ID]["download_status"] == "downloaded"
        assert result["jobs"][MOCK_JOB_ID_2]["download_status"] == "failed"

    def test_last_run_status_success_when_all_jobs_succeed(self):
        """sync_metadata.last_run_status is 'success' when no errors."""
        cjids = _make_categorized_job_ids(completed={MOCK_JOB_ID})
        jobs = {MOCK_JOB_ID: _make_job(MOCK_JOB_ID)}
        download_results = {
            MOCK_JOB_ID: {
                "total_files": 3,
                "downloaded_files": 3,
                "failed_files": 0,
                "error_code": None,
                "error_message": None,
            },
        }
        result = _build_status_file_content(
            queue_id=MOCK_QUEUE_ID,
            storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            categorized_job_ids=cjids,
            download_candidate_jobs=jobs,
            job_download_results=download_results,
        )
        assert result["sync_metadata"]["last_run_status"] == "success"
        assert result["jobs"][MOCK_JOB_ID]["total_files"] == 3
        assert result["jobs"][MOCK_JOB_ID]["downloaded_files"] == 3


class TestFileCountPreservation:
    """Tests for preserving file counts when new entry has no download results."""

    def test_existing_file_counts_not_zeroed_by_inactive_job(self):
        """An inactive job without download results should not zero out existing file counts."""
        existing_jobs = {
            MOCK_JOB_ID: {
                "download_status": "downloaded",
                "total_files": 4,
                "downloaded_files": 4,
                "failed_files": 0,
                "last_updated": "2026-06-18T12:00:00+00:00",
                "error_code": None,
                "error_message": None,
            }
        }
        cjids = _make_categorized_job_ids(inactive={MOCK_JOB_ID})
        jobs = {MOCK_JOB_ID: _make_job(MOCK_JOB_ID)}
        result = _build_status_file_content(
            queue_id=MOCK_QUEUE_ID,
            storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            categorized_job_ids=cjids,
            download_candidate_jobs=jobs,
            existing_jobs=existing_jobs,
            job_download_results={},
        )
        assert result["jobs"][MOCK_JOB_ID]["total_files"] == 4
        assert result["jobs"][MOCK_JOB_ID]["downloaded_files"] == 4

    def test_status_update_preserved_when_file_counts_kept(self):
        """If status changes but no file counts, update status but keep counts."""
        existing_jobs = {
            MOCK_JOB_ID: {
                "download_status": "downloaded",
                "total_files": 3,
                "downloaded_files": 3,
                "failed_files": 0,
                "last_updated": "2026-06-18T12:00:00+00:00",
                "error_code": None,
                "error_message": None,
            }
        }
        cjids = _make_categorized_job_ids(updated={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID, succeeded=2, total=5, ended=False)
        jobs = {MOCK_JOB_ID: job}
        result = _build_status_file_content(
            queue_id=MOCK_QUEUE_ID,
            storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            categorized_job_ids=cjids,
            download_candidate_jobs=jobs,
            existing_jobs=existing_jobs,
            job_download_results={},
        )
        assert result["jobs"][MOCK_JOB_ID]["download_status"] == "in_progress"
        assert result["jobs"][MOCK_JOB_ID]["total_files"] == 3

    def test_new_download_results_overwrite_existing(self):
        """When new download results have real counts, they overwrite existing."""
        existing_jobs = {
            MOCK_JOB_ID: {
                "download_status": "downloaded",
                "total_files": 3,
                "downloaded_files": 3,
                "failed_files": 0,
                "last_updated": "2026-06-18T12:00:00+00:00",
                "error_code": None,
                "error_message": None,
            }
        }
        cjids = _make_categorized_job_ids(added={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID, succeeded=5, total=5, ended=True)
        jobs = {MOCK_JOB_ID: job}
        download_results = {
            MOCK_JOB_ID: {
                "total_files": 5,
                "downloaded_files": 5,
                "failed_files": 0,
                "error_code": None,
                "error_message": None,
            }
        }
        result = _build_status_file_content(
            queue_id=MOCK_QUEUE_ID,
            storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            categorized_job_ids=cjids,
            download_candidate_jobs=jobs,
            existing_jobs=existing_jobs,
            job_download_results=download_results,
        )
        assert result["jobs"][MOCK_JOB_ID]["total_files"] == 5
        assert result["jobs"][MOCK_JOB_ID]["downloaded_files"] == 5


class TestSkipReason:
    """Tests for skip_reason field in skipped job entries."""

    def test_attachments_free_has_no_attachments_reason(self):
        cjids = _make_categorized_job_ids(attachments_free={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID, attachments=False)
        result = _determine_job_download_status(MOCK_JOB_ID, job, cjids)
        assert result["skip_reason"] == "no_attachments"

    def test_missing_storage_profile_has_reason(self):
        cjids = _make_categorized_job_ids(missing_storage_profile={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID, storage_profile_id=None)
        result = _determine_job_download_status(MOCK_JOB_ID, job, cjids)
        assert result["skip_reason"] == "missing_storage_profile"

    def test_downloaded_job_has_null_skip_reason(self):
        cjids = _make_categorized_job_ids(completed={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID)
        result = _determine_job_download_status(MOCK_JOB_ID, job, cjids)
        assert result["skip_reason"] is None


class TestExtractTaskId:
    """Tests for _extract_task_id_from_s3_key."""

    def test_extracts_task_id_from_s3_key(self):
        from deadline.client.cli._incremental_download import _extract_task_id_from_s3_key

        key = "DeadlineCloud/Manifests/farm-abc/queue-abc/job-abc/step-abc/task-abc-0/2026-01-01T00:00:00Z_sessionaction-abc-2/hash_output"
        assert _extract_task_id_from_s3_key(key) == "task-abc-0"

    def test_returns_none_for_key_without_task_id(self):
        from deadline.client.cli._incremental_download import _extract_task_id_from_s3_key

        assert (
            _extract_task_id_from_s3_key(
                "DeadlineCloud/Manifests/farm-abc/queue-abc/job-abc/hash_output"
            )
            is None
        )

    def test_extracts_task_id_with_different_suffix(self):
        from deadline.client.cli._incremental_download import _extract_task_id_from_s3_key

        key = "DeadlineCloud/Manifests/farm-abc/queue-abc/job-abc/step-abc/task-xyz123-3/ts_sessionaction/hash_output"
        assert _extract_task_id_from_s3_key(key) == "task-xyz123-3"

    def test_ignores_task_substring_in_customer_root_prefix(self):
        """A rootPrefix containing 'task-' must not be mistaken for the real task id."""
        from deadline.client.cli._incremental_download import _extract_task_id_from_s3_key

        key = "my-task-outputs/Manifests/farm-abc/queue-abc/job-abc/step-abc/task-real-1/ts_sessionaction/hash_output"
        assert _extract_task_id_from_s3_key(key) == "task-real-1"


class TestPerTaskTracking:
    """Tests for per-task download tracking in status file."""

    def test_task_download_results_added_to_job_entry(self):
        """Tasks from this run appear in the job's tasks dict."""
        cjids = _make_categorized_job_ids(completed={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID, succeeded=2, total=2, ended=True)
        jobs = {MOCK_JOB_ID: job}
        task_results = {
            MOCK_JOB_ID: {
                "task-abc-0": {
                    "total_files": 3,
                    "downloaded_files": 3,
                    "error_code": None,
                    "error_message": None,
                },
                "task-abc-1": {
                    "total_files": 3,
                    "downloaded_files": 3,
                    "error_code": None,
                    "error_message": None,
                },
            }
        }
        result = _build_status_file_content(
            queue_id=MOCK_QUEUE_ID,
            storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            categorized_job_ids=cjids,
            download_candidate_jobs=jobs,
            task_download_results=task_results,
        )
        tasks = result["jobs"][MOCK_JOB_ID]["tasks"]
        assert "task-abc-0" in tasks
        assert "task-abc-1" in tasks
        assert tasks["task-abc-0"]["download_status"] == "downloaded"
        assert tasks["task-abc-0"]["total_files"] == 3

    def test_failed_task_download_results_in_failed_status(self):
        """Tasks with error_code show download_status failed."""
        cjids = _make_categorized_job_ids(added={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID, succeeded=1, total=2, ended=False)
        jobs = {MOCK_JOB_ID: job}
        task_results = {
            MOCK_JOB_ID: {
                "task-abc-0": {
                    "total_files": 3,
                    "downloaded_files": 0,
                    "error_code": "PERMISSION_DENIED",
                    "error_message": "denied",
                },
            }
        }
        result = _build_status_file_content(
            queue_id=MOCK_QUEUE_ID,
            storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            categorized_job_ids=cjids,
            download_candidate_jobs=jobs,
            task_download_results=task_results,
        )
        tasks = result["jobs"][MOCK_JOB_ID]["tasks"]
        assert tasks["task-abc-0"]["download_status"] == "failed"
        assert tasks["task-abc-0"]["error_code"] == "PERMISSION_DENIED"

    def test_farm_failed_task_recorded_as_farm_failed(self):
        """A task that failed on the farm is recorded with an explicit "farm_failed" status
        (not "downloaded"/"failed") and carries no error code — nothing failed to download,
        the render produced nothing to fetch."""
        cjids = _make_categorized_job_ids(completed={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID, succeeded=2, total=3, ended=True)
        jobs = {MOCK_JOB_ID: job}
        task_results: dict[str, dict[str, dict[str, Any]]] = {
            MOCK_JOB_ID: {
                "task-abc-0": {
                    "total_files": 3,
                    "downloaded_files": 3,
                    "error_code": None,
                    "error_message": None,
                },
                # Farm-failed task: explicit farm_failed status, no error code.
                "task-abc-1": {
                    "total_files": 0,
                    "downloaded_files": 0,
                    "error_code": None,
                    "error_message": None,
                    "download_status": "farm_failed",
                },
            }
        }
        result = _build_status_file_content(
            queue_id=MOCK_QUEUE_ID,
            storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            categorized_job_ids=cjids,
            download_candidate_jobs=jobs,
            task_download_results=task_results,
        )
        tasks = result["jobs"][MOCK_JOB_ID]["tasks"]
        assert tasks["task-abc-0"]["download_status"] == "downloaded"
        assert tasks["task-abc-1"]["download_status"] == "farm_failed"
        assert tasks["task-abc-1"]["error_code"] is None

    def test_farm_failed_does_not_clobber_previously_downloaded_task(self):
        """A task requeued after a farm failure, then succeeded and downloaded, must stay
        "downloaded". The FAILED taskRun from the earlier attempt keeps being reported by the
        API on every no-op run, so a later run re-collects it as farm_failed — but the download
        already succeeded and its output is on disk, so farm_failed must not overwrite it."""
        existing_jobs = {
            MOCK_JOB_ID: {
                "download_status": "downloaded",
                "total_files": 3,
                "downloaded_files": 3,
                "failed_files": 0,
                "last_updated": "2026-01-01T00:00:00+00:00",
                "error_code": None,
                "error_message": None,
                "skip_reason": None,
                "tasks": {
                    "task-abc-0": {
                        "download_status": "downloaded",
                        "total_files": 3,
                        "downloaded_files": 3,
                        "error_code": None,
                        "error_message": None,
                    },
                },
            }
        }
        cjids = _make_categorized_job_ids(unchanged={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID, succeeded=1, total=1, ended=True)
        jobs = {MOCK_JOB_ID: job}
        # This run re-collects the stale earlier FAILED attempt as farm_failed for the same task.
        task_results: dict[str, dict[str, dict[str, Any]]] = {
            MOCK_JOB_ID: {
                "task-abc-0": {
                    "total_files": 0,
                    "downloaded_files": 0,
                    "error_code": None,
                    "error_message": None,
                    "download_status": "farm_failed",
                },
            }
        }
        result = _build_status_file_content(
            queue_id=MOCK_QUEUE_ID,
            storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            categorized_job_ids=cjids,
            download_candidate_jobs=jobs,
            existing_jobs=existing_jobs,
            task_download_results=task_results,
        )
        task = result["jobs"][MOCK_JOB_ID]["tasks"]["task-abc-0"]
        assert task["download_status"] == "downloaded", task
        assert task["downloaded_files"] == 3, task

    def test_existing_tasks_preserved_when_no_new_download(self):
        """Tasks from previous runs are preserved when no new download this run."""
        existing_jobs = {
            MOCK_JOB_ID: {
                "download_status": "in_progress",
                "total_files": 3,
                "downloaded_files": 3,
                "failed_files": 0,
                "last_updated": "2026-01-01T00:00:00+00:00",
                "error_code": None,
                "error_message": None,
                "skip_reason": None,
                "tasks": {
                    "task-abc-0": {
                        "download_status": "downloaded",
                        "total_files": 3,
                        "downloaded_files": 3,
                        "error_code": None,
                        "error_message": None,
                    },
                },
            }
        }
        cjids = _make_categorized_job_ids(unchanged={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID, succeeded=1, total=2, ended=False)
        jobs = {MOCK_JOB_ID: job}
        result = _build_status_file_content(
            queue_id=MOCK_QUEUE_ID,
            storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            categorized_job_ids=cjids,
            download_candidate_jobs=jobs,
            existing_jobs=existing_jobs,
            task_download_results={},
        )
        # Existing task preserved
        assert "task-abc-0" in result["jobs"][MOCK_JOB_ID]["tasks"]

    def test_new_task_overwrites_existing_same_id(self):
        """New task result overwrites existing entry with same task ID."""
        existing_jobs = {
            MOCK_JOB_ID: {
                "download_status": "downloaded",
                "total_files": 3,
                "downloaded_files": 3,
                "failed_files": 0,
                "last_updated": "2026-01-01T00:00:00+00:00",
                "error_code": None,
                "error_message": None,
                "skip_reason": None,
                "tasks": {
                    "task-abc-0": {
                        "download_status": "downloaded",
                        "total_files": 3,
                        "downloaded_files": 3,
                        "error_code": None,
                        "error_message": None,
                    },
                },
            }
        }
        cjids = _make_categorized_job_ids(added={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID, succeeded=1, total=1, ended=True)
        jobs = {MOCK_JOB_ID: job}
        task_results = {
            MOCK_JOB_ID: {
                "task-abc-0": {
                    "total_files": 3,
                    "downloaded_files": 3,
                    "error_code": None,
                    "error_message": None,
                },
            }
        }
        result = _build_status_file_content(
            queue_id=MOCK_QUEUE_ID,
            storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            categorized_job_ids=cjids,
            download_candidate_jobs=jobs,
            existing_jobs=existing_jobs,
            task_download_results=task_results,
        )
        assert result["jobs"][MOCK_JOB_ID]["tasks"]["task-abc-0"]["download_status"] == "downloaded"

    def test_zero_file_tasks_included_as_downloaded(self):
        """Tasks with zero output files are included with downloaded status so they count toward the progress bar."""
        cjids = _make_categorized_job_ids(completed={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID)
        jobs = {MOCK_JOB_ID: job}
        task_results = {
            MOCK_JOB_ID: {
                "task-abc-0": {
                    "total_files": 0,
                    "downloaded_files": 0,
                    "error_code": None,
                    "error_message": None,
                },
            }
        }
        result = _build_status_file_content(
            queue_id=MOCK_QUEUE_ID,
            storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            categorized_job_ids=cjids,
            download_candidate_jobs=jobs,
            task_download_results=task_results,
        )
        assert "task-abc-0" in result["jobs"][MOCK_JOB_ID]["tasks"]
        assert result["jobs"][MOCK_JOB_ID]["tasks"]["task-abc-0"]["download_status"] == "downloaded"

    def test_skipped_job_has_empty_tasks(self):
        """Skipped jobs always have an empty tasks dict."""
        cjids = _make_categorized_job_ids(attachments_free={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID, attachments=False)
        jobs = {MOCK_JOB_ID: job}
        result = _build_status_file_content(
            queue_id=MOCK_QUEUE_ID,
            storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            categorized_job_ids=cjids,
            download_candidate_jobs=jobs,
        )
        assert result["jobs"][MOCK_JOB_ID]["tasks"] == {}


class TestJobTaskStatusConsistency:
    """The job-level badge must never contradict its own task entries.

    The job's status comes from its checkpoint category (did it finish on the farm), while
    task statuses come from download results. Those are independent signals, so they can
    disagree — a job can be "complete on the farm" while one of its tasks' files never
    reached disk. The consumer renders both, so an entry claiming "downloaded" over a
    "failed" task shows a green job with a red task and hides a real failure.
    """

    def _existing_entry(self, status: str, tasks: dict[str, Any], **overrides) -> dict[str, Any]:
        entry: dict[str, Any] = {
            "download_status": status,
            "total_files": 2,
            "downloaded_files": 1,
            "failed_files": 1,
            "last_updated": "2026-01-01T00:00:00+00:00",
            "error_code": "PERMISSION_DENIED" if status == "failed" else None,
            "error_message": "denied" if status == "failed" else None,
            "skip_reason": None,
            "tasks": tasks,
        }
        entry.update(overrides)
        return entry

    def _failed_task(self, error_code: str = "PERMISSION_DENIED") -> dict[str, Any]:
        return {
            "download_status": "failed",
            "total_files": 1,
            "downloaded_files": 0,
            "error_code": error_code,
            "error_message": "denied",
        }

    def _downloaded_task(self) -> dict[str, Any]:
        return {
            "download_status": "downloaded",
            "total_files": 1,
            "downloaded_files": 1,
            "error_code": None,
            "error_message": None,
        }

    def test_job_stays_failed_while_a_task_still_carries_an_error(self):
        """A no-op run must not flip a job to "downloaded" over a still-failed task.

        The job ended on the farm, so its category says complete; but the task that failed
        to download last run produced no new result this run, so its "failed" entry persists.
        The job must report failed too, otherwise the artist sees a fully-green job and never
        learns the frame is missing.
        """
        existing_jobs = {
            MOCK_JOB_ID: self._existing_entry(
                "failed",
                {"task-abc-0": self._downloaded_task(), "task-abc-1": self._failed_task()},
            )
        }
        cjids = _make_categorized_job_ids(unchanged={MOCK_JOB_ID})
        result = _build_status_file_content(
            queue_id=MOCK_QUEUE_ID,
            storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            categorized_job_ids=cjids,
            download_candidate_jobs={MOCK_JOB_ID: _make_job(MOCK_JOB_ID, succeeded=2, total=2)},
            existing_jobs=existing_jobs,
            job_download_results={},
            task_download_results={},
        )
        entry = result["jobs"][MOCK_JOB_ID]
        assert entry["download_status"] == "failed", entry
        assert entry["error_code"] == "PERMISSION_DENIED", entry

    def test_inactive_flip_does_not_mask_a_failed_task(self):
        """An in_progress → downloaded flip for a job that dropped out of tracking must not
        overwrite the evidence that one of its tasks failed to download."""
        existing_jobs = {
            MOCK_JOB_ID: self._existing_entry(
                "in_progress", {"task-abc-1": self._failed_task("DISK_FULL")}
            )
        }
        cjids = _make_categorized_job_ids(inactive={MOCK_JOB_ID})
        result = _build_status_file_content(
            queue_id=MOCK_QUEUE_ID,
            storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            categorized_job_ids=cjids,
            download_candidate_jobs={},
            existing_jobs=existing_jobs,
            job_download_results={},
            task_download_results={},
        )
        entry = result["jobs"][MOCK_JOB_ID]
        assert entry["download_status"] == "failed", entry
        assert entry["error_code"] == "DISK_FULL", entry

    def test_successful_retry_clears_job_and_task_errors_together(self):
        """The reconcile must not be a one-way ratchet: when the task actually re-downloads,
        both levels go back to downloaded with the stale error fields cleared."""
        existing_jobs = {
            MOCK_JOB_ID: self._existing_entry("failed", {"task-abc-0": self._failed_task()})
        }
        cjids = _make_categorized_job_ids(unchanged={MOCK_JOB_ID})
        result = _build_status_file_content(
            queue_id=MOCK_QUEUE_ID,
            storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            categorized_job_ids=cjids,
            download_candidate_jobs={MOCK_JOB_ID: _make_job(MOCK_JOB_ID)},
            existing_jobs=existing_jobs,
            job_download_results={
                MOCK_JOB_ID: {
                    "total_files": 1,
                    "downloaded_files": 1,
                    "failed_files": 0,
                    "error_code": None,
                    "error_message": None,
                }
            },
            task_download_results={
                MOCK_JOB_ID: {
                    "task-abc-0": {
                        "total_files": 1,
                        "downloaded_files": 1,
                        "error_code": None,
                        "error_message": None,
                    }
                }
            },
        )
        entry = result["jobs"][MOCK_JOB_ID]
        assert entry["download_status"] == "downloaded", entry
        assert entry["error_code"] is None, entry
        assert entry["tasks"]["task-abc-0"]["download_status"] == "downloaded", entry

    def test_farm_failed_task_does_not_drag_job_to_download_failed(self):
        """A task that failed on the farm is not a download failure.

        The render itself failed, so there was never any output to fetch. Reporting the job as
        a download failure would send the artist to retry a download that can't help — the fix
        is to re-render. farm_failed carries no error_code, which is what keeps it out.
        """
        cjids = _make_categorized_job_ids(completed={MOCK_JOB_ID})
        result = _build_status_file_content(
            queue_id=MOCK_QUEUE_ID,
            storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            categorized_job_ids=cjids,
            download_candidate_jobs={MOCK_JOB_ID: _make_job(MOCK_JOB_ID, succeeded=1, failed=1)},
            task_download_results={
                MOCK_JOB_ID: {
                    "task-abc-1": {
                        "total_files": 0,
                        "downloaded_files": 0,
                        "error_code": None,
                        "error_message": None,
                        "download_status": "farm_failed",
                    }
                }
            },
        )
        entry = result["jobs"][MOCK_JOB_ID]
        assert entry["download_status"] == "downloaded", entry
        assert entry["error_code"] is None, entry
        assert entry["tasks"]["task-abc-1"]["download_status"] == "farm_failed", entry

    def test_requeued_job_still_running_is_not_reconciled_to_downloaded(self):
        """A job that was requeued and is actively re-running stays in_progress.

        Reconciliation fires only on the settled no-failure statuses, so it must not touch — or
        falsely settle — a job whose tasks are still on the farm. "in_progress" claims nothing
        and the download may still be retried before the job ends.
        """
        existing_jobs = {
            MOCK_JOB_ID: self._existing_entry("downloaded", {"task-abc-0": self._downloaded_task()})
        }
        cjids = _make_categorized_job_ids(updated={MOCK_JOB_ID})
        result = _build_status_file_content(
            queue_id=MOCK_QUEUE_ID,
            storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            categorized_job_ids=cjids,
            download_candidate_jobs={
                MOCK_JOB_ID: _make_job(MOCK_JOB_ID, succeeded=1, total=3, ended=False)
            },
            existing_jobs=existing_jobs,
            job_download_results={},
            task_download_results={},
        )
        entry = result["jobs"][MOCK_JOB_ID]
        assert entry["download_status"] == "in_progress", entry
        # The already-downloaded task keeps its terminal status — its output is on disk.
        assert entry["tasks"]["task-abc-0"]["download_status"] == "downloaded", entry

    def test_inactive_job_with_no_downloads_reports_failure_not_skipped(self):
        """A job stopped before any file landed, whose task failed to download, is not "skipped".

        Going inactive with downloaded_files == 0 normally means "stopped before producing
        output", which reports skipped. But a task carrying a download error means output DID
        exist and we failed to fetch it — reporting skipped tells the artist there was nothing
        to get, hiding a DISK_FULL they could actually fix.
        """
        existing_jobs = {
            MOCK_JOB_ID: self._existing_entry(
                "in_progress",
                {"task-abc-0": self._failed_task("DISK_FULL")},
                downloaded_files=0,
                skip_reason=None,
            )
        }
        cjids = _make_categorized_job_ids(inactive={MOCK_JOB_ID})
        result = _build_status_file_content(
            queue_id=MOCK_QUEUE_ID,
            storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            categorized_job_ids=cjids,
            download_candidate_jobs={},
            existing_jobs=existing_jobs,
            job_download_results={},
            task_download_results={},
        )
        entry = result["jobs"][MOCK_JOB_ID]
        assert entry["download_status"] == "failed", entry
        assert entry["error_code"] == "DISK_FULL", entry
        # The failure has to reach the job's own counters, not just sit on the task row: the
        # monitor's job list reads failed_files to decide whether to surface the job at all.
        assert entry["failed_files"] == 1, entry

    def test_skipped_job_with_no_attachments_keeps_its_skip_reason(self):
        """The skipped→failed override must not fire on a genuine skip.

        A job with no attachments has no tasks at all, so there is no error evidence to
        override. Its skip_reason has to survive, or every attachment-free job would lose the
        explanation for why it was skipped.
        """
        cjids = _make_categorized_job_ids(attachments_free={MOCK_JOB_ID})
        result = _build_status_file_content(
            queue_id=MOCK_QUEUE_ID,
            storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            categorized_job_ids=cjids,
            download_candidate_jobs={MOCK_JOB_ID: _make_job(MOCK_JOB_ID)},
            job_download_results={},
            task_download_results={},
        )
        entry = result["jobs"][MOCK_JOB_ID]
        assert entry["download_status"] == "skipped", entry
        assert entry["skip_reason"] == "no_attachments", entry
        assert entry["error_code"] is None, entry

    def test_still_running_job_keeps_in_progress_over_an_errored_task(self):
        """An errored task under a job that is still running does not settle the job to failed.

        This pins the one status deliberately left out of the override. "in_progress" makes no
        claim that the output is on disk, so it does not contradict a failed task the way
        "downloaded" and "skipped" do — and the job's remaining tasks have yet to be downloaded,
        so calling the job failed would settle it early and, once the retry succeeds, report a
        failure that no longer exists. The failed task row still surfaces the error either way,
        so nothing is hidden by waiting.
        """
        existing_jobs = {
            MOCK_JOB_ID: self._existing_entry(
                "in_progress", {"task-abc-0": self._failed_task("NETWORK_ERROR")}
            )
        }
        result = _build_status_file_content(
            queue_id=MOCK_QUEUE_ID,
            storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            categorized_job_ids=_make_categorized_job_ids(updated={MOCK_JOB_ID}),
            download_candidate_jobs={
                MOCK_JOB_ID: _make_job(MOCK_JOB_ID, succeeded=1, total=3, ended=False)
            },
            existing_jobs=existing_jobs,
            job_download_results={},
            task_download_results={},
        )
        entry = result["jobs"][MOCK_JOB_ID]
        assert entry["download_status"] == "in_progress", entry
        assert entry["error_code"] is None, entry
        # The task-level error stays visible — the job just isn't settled yet.
        assert entry["tasks"]["task-abc-0"]["error_code"] == "NETWORK_ERROR", entry

    def test_reconciled_failure_drops_a_stale_skip_reason(self):
        """A job reconciled to failed must not still carry the skip_reason it was skipped for.

        A job whose storage profile disappeared reports skipped/missing_storage_profile, but if
        an earlier run already failed to download one of its tasks, the entry gets reconciled to
        failed. Keeping skip_reason would emit an entry that claims both "there was nothing to
        fetch" and "fetching errored" — and the consumer keys its "why is this file missing"
        copy off skip_reason, so it would show the wrong explanation for a fixable error.
        """
        for category, stale_reason in (
            ("missing_storage_profile", "missing_storage_profile"),
            ("attachments_free", "no_attachments"),
        ):
            # total_files == 0 routes this through the branch that rebuilds the entry from the
            # category, which is what reintroduces the skip_reason over the carried-forward task.
            existing_jobs = {
                MOCK_JOB_ID: self._existing_entry(
                    "failed",
                    {"task-abc-0": self._failed_task("PERMISSION_DENIED")},
                    total_files=0,
                    downloaded_files=0,
                )
            }
            result = _build_status_file_content(
                queue_id=MOCK_QUEUE_ID,
                storage_profile_id=MOCK_STORAGE_PROFILE_ID,
                categorized_job_ids=_make_categorized_job_ids(**{category: {MOCK_JOB_ID}}),
                download_candidate_jobs={MOCK_JOB_ID: _make_job(MOCK_JOB_ID)},
                existing_jobs=existing_jobs,
                job_download_results={},
                task_download_results={},
            )
            entry = result["jobs"][MOCK_JOB_ID]
            assert entry["download_status"] == "failed", (category, entry)
            assert entry["error_code"] == "PERMISSION_DENIED", (category, entry)
            assert entry["skip_reason"] is None, (
                f"{category} entry claims both failed and skip_reason={stale_reason!r}: {entry}"
            )

    def test_carried_forward_failure_does_not_mark_this_run_failed(self):
        """A job-level failure inherited from an earlier run leaves last_run_status success.

        The two fields answer different questions: the job entry says "is this job's output on
        disk", last_run_status says "did this sync attempt hit an error". A sync that downloaded
        nothing new and hit no error is a success even while it carries an old failure forward —
        otherwise one unresolved failure would report every future sync as broken.
        """
        existing_jobs = {
            MOCK_JOB_ID: self._existing_entry(
                "failed", {"task-abc-0": self._failed_task("DISK_FULL")}
            )
        }
        cjids = _make_categorized_job_ids(unchanged={MOCK_JOB_ID})
        result = _build_status_file_content(
            queue_id=MOCK_QUEUE_ID,
            storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            categorized_job_ids=cjids,
            download_candidate_jobs={MOCK_JOB_ID: _make_job(MOCK_JOB_ID, succeeded=2, total=2)},
            existing_jobs=existing_jobs,
            job_download_results={},
            task_download_results={},
        )
        assert result["jobs"][MOCK_JOB_ID]["download_status"] == "failed", result["jobs"]
        assert result["sync_metadata"]["last_run_status"] == "success", result["sync_metadata"]

    def test_no_settled_job_entry_hides_an_errored_task(self):
        """Invariant sweep over a mixed run: no entry claims a settled, no-failure status
        ("downloaded" or "skipped") while any of its tasks carries a non-null error_code."""
        cjids = _make_categorized_job_ids(
            completed={MOCK_JOB_ID},
            added={MOCK_JOB_ID_2},
            unchanged={MOCK_JOB_ID_3},
            inactive={MOCK_JOB_ID_4},
        )
        jobs = {
            MOCK_JOB_ID: _make_job(MOCK_JOB_ID, succeeded=2, total=2),
            MOCK_JOB_ID_2: _make_job(MOCK_JOB_ID_2, succeeded=1, total=1),
            MOCK_JOB_ID_3: _make_job(MOCK_JOB_ID_3, succeeded=1, failed=1, total=2),
        }
        existing_jobs = {
            MOCK_JOB_ID_3: self._existing_entry(
                "failed", {"task-c-0": self._failed_task("NETWORK_ERROR")}
            ),
            # Job 4 exercises the "skipped" half of the sweep: inactive with zero downloads
            # reports skipped, so without reconciliation its PATH_NOT_FOUND task would sit
            # under a job entry claiming there was nothing to fetch.
            MOCK_JOB_ID_4: self._existing_entry(
                "in_progress",
                {"task-d-0": self._failed_task("PATH_NOT_FOUND")},
                downloaded_files=0,
            ),
        }
        task_results: dict[str, dict[str, dict[str, Any]]] = {
            MOCK_JOB_ID: {
                "task-a-0": {
                    "total_files": 1,
                    "downloaded_files": 1,
                    "error_code": None,
                    "error_message": None,
                },
                "task-a-1": {
                    "total_files": 1,
                    "downloaded_files": 0,
                    "error_code": "DISK_FULL",
                    "error_message": "full",
                },
            },
            MOCK_JOB_ID_2: {
                "task-b-0": {
                    "total_files": 1,
                    "downloaded_files": 1,
                    "error_code": None,
                    "error_message": None,
                },
            },
        }
        result = _build_status_file_content(
            queue_id=MOCK_QUEUE_ID,
            storage_profile_id=MOCK_STORAGE_PROFILE_ID,
            categorized_job_ids=cjids,
            download_candidate_jobs=jobs,
            existing_jobs=existing_jobs,
            job_download_results={},
            task_download_results=task_results,
        )
        # Sweep both settled no-failure statuses, not just "downloaded": a job that goes
        # inactive reports "skipped", which hides an errored task just as effectively.
        for job_id, entry in result["jobs"].items():
            if entry["download_status"] in ("downloaded", "skipped"):
                errored = {
                    t_id: t["error_code"]
                    for t_id, t in entry["tasks"].items()
                    if t.get("error_code")
                }
                assert not errored, (
                    f"{job_id} claims {entry['download_status']} over errored tasks {errored}"
                )
        # Every job with an errored task is reported failed; the clean one stays downloaded.
        assert result["jobs"][MOCK_JOB_ID]["download_status"] == "failed"
        assert result["jobs"][MOCK_JOB_ID_2]["download_status"] == "downloaded"
        assert result["jobs"][MOCK_JOB_ID_3]["download_status"] == "failed"
        assert result["jobs"][MOCK_JOB_ID_4]["download_status"] == "failed"


class TestClassifyError:
    """Tests for _classify_error."""

    def test_permission_denied_by_type(self):
        from deadline.client.cli._incremental_download import _classify_error

        assert _classify_error(PermissionError("cannot write")) == "PERMISSION_DENIED"

    def test_disk_full_by_type(self):
        from deadline.client.cli._incremental_download import _classify_error

        e = OSError(28, "No space left on device")
        assert _classify_error(e) == "DISK_FULL"

    def test_path_not_found_by_type(self):
        from deadline.client.cli._incremental_download import _classify_error

        assert _classify_error(FileNotFoundError("missing")) == "PATH_NOT_FOUND"

    def test_network_error_by_type(self):
        from deadline.client.cli._incremental_download import _classify_error

        assert _classify_error(ConnectionError("reset")) == "NETWORK_ERROR"
        assert _classify_error(TimeoutError("timed out")) == "NETWORK_ERROR"

    def test_client_error_access_denied(self):
        from deadline.client.cli._incremental_download import _classify_error
        from botocore.exceptions import ClientError

        e = ClientError({"Error": {"Code": "AccessDenied", "Message": "nope"}}, "GetObject")
        assert _classify_error(e) == "PERMISSION_DENIED"

    def test_client_error_no_such_key(self):
        from deadline.client.cli._incremental_download import _classify_error
        from botocore.exceptions import ClientError

        e = ClientError({"Error": {"Code": "NoSuchKey", "Message": "gone"}}, "GetObject")
        assert _classify_error(e) == "PATH_NOT_FOUND"

    def test_client_error_request_timeout(self):
        from deadline.client.cli._incremental_download import _classify_error
        from botocore.exceptions import ClientError

        e = ClientError({"Error": {"Code": "RequestTimeout", "Message": "slow"}}, "GetObject")
        assert _classify_error(e) == "NETWORK_ERROR"

    def test_job_attachments_s3_client_error_by_status_code(self):
        """The actual download path wraps S3 errors in JobAttachmentsS3ClientError,
        which carries a structured HTTP status code — classify from that, not the message."""
        from deadline.client.cli._incremental_download import _classify_error
        from deadline.job_attachments.exceptions import JobAttachmentsS3ClientError

        forbidden = JobAttachmentsS3ClientError(
            action="downloading file", status_code=403, bucket_name="b", key_or_prefix="k"
        )
        assert _classify_error(forbidden) == "PERMISSION_DENIED"

        not_found = JobAttachmentsS3ClientError(
            action="downloading file", status_code=404, bucket_name="b", key_or_prefix="k"
        )
        assert _classify_error(not_found) == "PATH_NOT_FOUND"

    def test_job_attachments_botocore_error_is_network(self):
        from deadline.client.cli._incremental_download import _classify_error
        from deadline.job_attachments.exceptions import JobAttachmentS3BotoCoreError

        e = JobAttachmentS3BotoCoreError(action="downloading file", error_details="conn reset")
        assert _classify_error(e) == "NETWORK_ERROR"

    def test_wrapped_cause_is_classified(self):
        """job_attachments wraps low-level failures (AssetSyncError(original)); the classifier
        walks __cause__ to reach the structured signal instead of guessing from the message."""
        from deadline.client.cli._incremental_download import _classify_error
        from deadline.job_attachments.exceptions import AssetSyncError

        wrapped = AssetSyncError("File download failed.")
        wrapped.__cause__ = PermissionError("denied")
        assert _classify_error(wrapped) == "PERMISSION_DENIED"

    def test_cyclic_cause_chain_does_not_recurse_forever(self):
        """A cyclic __cause__ chain must terminate at UNKNOWN, not blow the stack."""
        from deadline.client.cli._incremental_download import _classify_error

        a = Exception("a")
        b = Exception("b")
        a.__cause__ = b
        b.__cause__ = a  # cycle: neither carries a structured signal
        assert _classify_error(a) == "UNKNOWN"

    def test_message_only_errors_are_unknown(self):
        """Message-substring matching is intentionally not used — wrapped S3 errors carry
        verbose text that produces confidently-wrong codes, so anything without a structured
        signal (exception type or S3 error code) is classified as UNKNOWN."""
        from deadline.client.cli._incremental_download import _classify_error

        assert _classify_error(Exception("Access Denied")) == "UNKNOWN"
        assert _classify_error(Exception("No space left on device")) == "UNKNOWN"
        assert _classify_error(Exception("No such file or directory")) == "UNKNOWN"
        assert _classify_error(Exception("Connection timeout")) == "UNKNOWN"

    def test_unknown_error(self):
        from deadline.client.cli._incremental_download import _classify_error

        assert _classify_error(Exception("something unexpected")) == "UNKNOWN"

    def test_implicit_context_chain_is_classified(self):
        """A `raise X` inside an `except` block sets __context__, not __cause__.

        job_attachments does this — e.g. download.py raises AssetSyncCancelledError from inside
        an `except CancelledError` handler with no `from`. Following only __cause__ would report
        UNKNOWN for a failure whose real signal is one frame away, sending the artist to the
        generic troubleshooting page instead of "the disk is full".
        """
        from deadline.client.cli._incremental_download import _classify_error

        try:
            try:
                raise OSError(28, "No space left on device")
            except OSError:
                raise RuntimeError("write failed")  # implicit chain: __context__ only
        except RuntimeError as e:
            assert e.__cause__ is None, "guard: this models an implicit chain, not `raise from`"
            assert _classify_error(e) == "DISK_FULL"

    def test_suppressed_context_is_not_followed(self):
        """`raise X from None` explicitly suppresses the chain — respect that and stop.

        Author intent is that the inner error is not the explanation, so inheriting its code
        would attach a misleading cause to the entry.
        """
        from deadline.client.cli._incremental_download import _classify_error

        try:
            try:
                raise PermissionError("denied")
            except PermissionError:
                raise RuntimeError("unrelated failure") from None
        except RuntimeError as e:
            assert _classify_error(e) == "UNKNOWN"

    def test_cyclic_context_chain_does_not_recurse_forever(self):
        """A cyclic __context__ chain must terminate, like the __cause__ cycle guard."""
        from deadline.client.cli._incremental_download import _classify_error

        a = Exception("a")
        b = Exception("b")
        a.__context__ = b
        b.__context__ = a
        assert _classify_error(a) == "UNKNOWN"

    def test_s3_client_error_with_other_status_code_is_unknown(self):
        """A JobAttachmentsS3ClientError status code we don't map (500, 503, …) is UNKNOWN.

        Only 403 and 404 have an unambiguous artist-facing meaning. Guessing on a 5xx would
        blame the user's permissions for what is actually a service-side failure.
        """
        from deadline.client.cli._incremental_download import _classify_error
        from deadline.job_attachments.exceptions import JobAttachmentsS3ClientError

        for status_code in (500, 503):
            e = JobAttachmentsS3ClientError(
                action="downloading file",
                status_code=status_code,
                bucket_name="b",
                key_or_prefix="k",
            )
            assert _classify_error(e) == "UNKNOWN", status_code

    def test_verbose_s3_guidance_message_alone_is_unknown(self):
        """Regression guard on the removed substring matching.

        job_attachments embeds long guidance text ("Not found. Please check your bucket
        name…") in messages. Matching on it classified a 403 as PATH_NOT_FOUND because the
        wrapper interpolates every status code's guidance into one string.
        """
        from deadline.client.cli._incremental_download import _classify_error

        verbose = (
            "Not found. Please check your bucket name and object key, and ensure that they "
            "exist in the AWS account. Forbidden or Access denied. Please check your AWS "
            "credentials and Job Attachments S3 bucket encryption settings."
        )
        assert _classify_error(Exception(verbose)) == "UNKNOWN"


class TestRetrieveSessionActionsFarmFailures:
    """Tests that _retrieve_session_actions_for_session collects farm-failed task IDs
    separately without adding them to the downloadable session-action list."""

    def _make_deadline_mock(self, session_actions):
        from unittest.mock import MagicMock

        deadline = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [{"sessionActions": session_actions}]
        deadline.get_paginator.return_value = paginator
        return deadline

    def test_failed_taskrun_collected_and_not_downloaded(self):
        from deadline.client.cli._incremental_download import (
            _retrieve_session_actions_for_session,
        )

        session_actions = [
            {
                "sessionActionId": "sessionaction-abc-0",
                "status": "SUCCEEDED",
                "definition": {"taskRun": {"taskId": "task-abc-0", "stepId": "step-abc"}},
                "manifests": [{"outputManifestPath": "task-abc-0/m"}],
            },
            {
                "sessionActionId": "sessionaction-abc-1",
                "status": "FAILED",
                "definition": {"taskRun": {"taskId": "task-abc-1", "stepId": "step-abc"}},
            },
        ]
        deadline = self._make_deadline_mock(session_actions)
        output_session: dict[str, Any] = {"sessionId": "session-abc"}
        farm_failed: set[str] = set()

        _retrieve_session_actions_for_session(
            deadline,
            {},
            "farm-1",
            "queue-1",
            "job-1",
            output_session,
            farm_failed,
        )

        # The failed taskRun is collected separately.
        assert farm_failed == {"task-abc-1"}
        # Only the succeeded taskRun is in the downloadable list (failed one excluded).
        downloaded_ids = {
            sa["definition"]["taskRun"]["taskId"] for sa in output_session["sessionActions"]
        }
        assert downloaded_ids == {"task-abc-0"}

    def test_no_failed_set_ignores_failed_actions(self):
        """Backward compatible: without the output set, failed actions are simply skipped."""
        from deadline.client.cli._incremental_download import (
            _retrieve_session_actions_for_session,
        )

        session_actions = [
            {
                "sessionActionId": "sessionaction-abc-0",
                "status": "FAILED",
                "definition": {"taskRun": {"taskId": "task-abc-0", "stepId": "step-abc"}},
            },
        ]
        deadline = self._make_deadline_mock(session_actions)
        output_session: dict[str, Any] = {"sessionId": "session-abc"}

        _retrieve_session_actions_for_session(
            deadline, {}, "farm-1", "queue-1", "job-1", output_session
        )

        # No sessionActions populated (nothing succeeded), no crash.
        assert "sessionActions" not in output_session
