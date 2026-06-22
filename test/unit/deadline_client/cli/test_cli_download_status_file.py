# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the CLI download status file module.
"""

import json
import os
from typing import Any, Optional

from deadline.client.cli._download_status_file import (
    _atomic_write_json,
    _build_status_file_content,
    _determine_job_download_status,
    _get_status_file_paths,
    write_download_status_file,
)
from deadline.client.cli._incremental_download import CategorizedJobIds

from ..shared_constants import MOCK_QUEUE_ID, MOCK_STORAGE_PROFILE_ID, MOCK_JOB_ID


MOCK_JOB_ID_2 = "job-aaaabbbbccccddddeeeeffffaaaabbbb"
MOCK_JOB_ID_3 = "job-11112222333344445555666677778888"


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
) -> dict[str, Any]:
    """Helper to create a fake job dict."""
    job: dict[str, Any] = {
        "jobId": job_id,
        "name": f"test-job-{job_id[-8:]}",
        "taskRunStatusCounts": {
            "SUCCEEDED": succeeded,
            "FAILED": 0,
            "RUNNING": total - succeeded,
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
        result = _determine_job_download_status(MOCK_JOB_ID, job, cjids, MOCK_STORAGE_PROFILE_ID)
        assert result["download_status"] == "downloaded"

    def test_added_job_all_tasks_succeeded_no_active_returns_downloaded(self):
        """Added job with all tasks succeeded and no active tasks is truly done."""
        cjids = _make_categorized_job_ids(added={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID, succeeded=5, total=5, ended=True)
        result = _determine_job_download_status(MOCK_JOB_ID, job, cjids, MOCK_STORAGE_PROFILE_ID)
        assert result["download_status"] == "downloaded"

    def test_added_job_requeued_with_active_tasks_returns_in_progress(self):
        """Requeued job has active tasks (READY) so should be in_progress even if endedAt is set."""
        cjids = _make_categorized_job_ids(added={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID, succeeded=5, total=5, ended=True)
        job["taskRunStatusCounts"]["READY"] = 3
        job["taskRunStatusCounts"]["RUNNING"] = 0
        result = _determine_job_download_status(MOCK_JOB_ID, job, cjids, MOCK_STORAGE_PROFILE_ID)
        assert result["download_status"] == "in_progress"

    def test_added_job_partial_tasks_returns_in_progress(self):
        cjids = _make_categorized_job_ids(added={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID, succeeded=3, total=10, ended=False)
        result = _determine_job_download_status(MOCK_JOB_ID, job, cjids, MOCK_STORAGE_PROFILE_ID)
        assert result["download_status"] == "in_progress"

    def test_updated_job_partial_tasks_returns_in_progress(self):
        cjids = _make_categorized_job_ids(updated={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID, succeeded=7, total=10, ended=False)
        result = _determine_job_download_status(MOCK_JOB_ID, job, cjids, MOCK_STORAGE_PROFILE_ID)
        assert result["download_status"] == "in_progress"

    def test_unchanged_job_all_succeeded_returns_downloaded(self):
        cjids = _make_categorized_job_ids(unchanged={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID, succeeded=10, total=10, ended=True)
        result = _determine_job_download_status(MOCK_JOB_ID, job, cjids, MOCK_STORAGE_PROFILE_ID)
        assert result["download_status"] == "downloaded"

    def test_unchanged_job_partial_returns_in_progress(self):
        cjids = _make_categorized_job_ids(unchanged={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID, succeeded=5, total=10, ended=False)
        result = _determine_job_download_status(MOCK_JOB_ID, job, cjids, MOCK_STORAGE_PROFILE_ID)
        assert result["download_status"] == "in_progress"

    def test_inactive_job_all_succeeded_returns_downloaded(self):
        cjids = _make_categorized_job_ids(inactive={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID, succeeded=5, total=5)
        result = _determine_job_download_status(MOCK_JOB_ID, job, cjids, MOCK_STORAGE_PROFILE_ID)
        assert result["download_status"] == "downloaded"

    def test_inactive_job_not_all_succeeded_returns_skipped(self):
        """Canceled/failed inactive jobs should not be marked as downloaded."""
        cjids = _make_categorized_job_ids(inactive={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID, succeeded=2, total=5, ended=True)
        result = _determine_job_download_status(MOCK_JOB_ID, job, cjids, MOCK_STORAGE_PROFILE_ID)
        assert result["download_status"] == "skipped"

    def test_attachments_free_returns_skipped(self):
        cjids = _make_categorized_job_ids(attachments_free={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID, attachments=False)
        result = _determine_job_download_status(MOCK_JOB_ID, job, cjids, MOCK_STORAGE_PROFILE_ID)
        assert result["download_status"] == "skipped"

    def test_missing_storage_profile_returns_skipped(self):
        cjids = _make_categorized_job_ids(missing_storage_profile={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID, storage_profile_id=None)
        result = _determine_job_download_status(MOCK_JOB_ID, job, cjids, MOCK_STORAGE_PROFILE_ID)
        assert result["download_status"] == "skipped"

    def test_result_has_all_required_fields(self):
        cjids = _make_categorized_job_ids(completed={MOCK_JOB_ID})
        job = _make_job(MOCK_JOB_ID)
        result = _determine_job_download_status(MOCK_JOB_ID, job, cjids, MOCK_STORAGE_PROFILE_ID)
        assert "download_status" in result
        assert "total_files" in result
        assert "downloaded_files" in result
        assert "failed_files" in result
        assert "last_updated" in result
        assert "error_code" in result
        assert "error_message" in result


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

    def test_storage_profile_single_location(self):
        profile = {
            "fileSystemLocations": [
                {"name": "renders", "path": "/mnt/nas/renders"},
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
            "/mnt/nas/renders", ".deadline", f"{MOCK_QUEUE_ID}_download_status.json"
        )
        assert paths[0] == expected

    def test_storage_profile_multiple_locations(self):
        profile = {
            "fileSystemLocations": [
                {"name": "renders", "path": "/mnt/nas/renders"},
                {"name": "projects", "path": "/mnt/nas/projects"},
                {"name": "tools", "path": "/mnt/nas/tools"},
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
            os.path.join("/mnt/nas/renders", ".deadline", f"{MOCK_QUEUE_ID}_download_status.json")
            in paths
        )
        assert (
            os.path.join("/mnt/nas/projects", ".deadline", f"{MOCK_QUEUE_ID}_download_status.json")
            in paths
        )
        assert (
            os.path.join("/mnt/nas/tools", ".deadline", f"{MOCK_QUEUE_ID}_download_status.json")
            in paths
        )


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
            local_storage_profile_id=MOCK_STORAGE_PROFILE_ID,
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
            local_storage_profile_id=MOCK_STORAGE_PROFILE_ID,
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
            local_storage_profile_id=None,
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
            local_storage_profile_id=MOCK_STORAGE_PROFILE_ID,
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
            local_storage_profile_id=MOCK_STORAGE_PROFILE_ID,
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
        # Use a path nested under a file (not a directory) to guarantee failure on all platforms
        blocker_file = tmp_path / "blocker"
        blocker_file.write_text("not a directory")
        profile = {
            "fileSystemLocations": [
                {"name": "renders", "path": str(blocker_file / "nested" / "path")},
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
