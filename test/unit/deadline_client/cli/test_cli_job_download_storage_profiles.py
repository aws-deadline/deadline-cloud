# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
End-to-end CLI tests for `deadline job download-output` storage profile support.

These tests exercise the full CLI path from `job_download_output()` through
`_download_job_output()`, mocking only the Deadline API client, OutputDownloader,
and the queue session. Each test corresponds to a case in
docs/design/storage-profile-cli-test-cases.md.
"""

from __future__ import annotations

import sys
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from deadline.client import api, config
from deadline.client.cli import main
from deadline.client.cli._groups import job_group
from deadline.job_attachments.models import (
    FileSystemLocation,
    FileSystemLocationType,
    PathFormat,
    StorageProfile,
    StorageProfileOperatingSystemFamily,
)
from deadline.job_attachments.progress_tracker import DownloadSummaryStatistics

MOCK_FARM_ID = "farm-0123456789abcdefabcdefabcdefabcd"
MOCK_QUEUE_ID = "queue-0123456789abcdefabcdefabcdefabcd"
MOCK_JOB_ID = "job-0123456789abcdefabcdefabcdefabcd"

MOCK_GET_QUEUE_RESPONSE: dict[str, Any] = {
    "queueId": MOCK_QUEUE_ID,
    "displayName": "Test Queue",
    "description": "",
    "farmId": MOCK_FARM_ID,
    "status": "ACTIVE",
    "jobAttachmentSettings": {
        "s3BucketName": "deadline-job-attachments-mock-bucket",
        "rootPrefix": "AWS Deadline Cloud",
    },
}

MOCK_LOCAL_PROFILE = StorageProfile(
    storageProfileId="sp-local-111",
    displayName="Local Linux Profile",
    osFamily=StorageProfileOperatingSystemFamily.LINUX,
    fileSystemLocations=[
        FileSystemLocation(name="shared", path="/mnt/shared", type=FileSystemLocationType.SHARED),
        FileSystemLocation(name="temp", path="/tmp/render", type=FileSystemLocationType.LOCAL),
    ],
)

MOCK_JOB_PROFILE = StorageProfile(
    storageProfileId="sp-job-222",
    displayName="Job Windows Profile",
    osFamily=StorageProfileOperatingSystemFamily.WINDOWS,
    fileSystemLocations=[
        FileSystemLocation(name="shared", path="Z:\\shared", type=FileSystemLocationType.SHARED),
        FileSystemLocation(name="temp", path="C:\\temp\\render", type=FileSystemLocationType.LOCAL),
    ],
)

MOCK_JOB_PROFILE_NO_MATCH = StorageProfile(
    storageProfileId="sp-job-333",
    displayName="Job No Match Profile",
    osFamily=StorageProfileOperatingSystemFamily.WINDOWS,
    fileSystemLocations=[
        FileSystemLocation(
            name="ProjectFiles", path="D:\\projects", type=FileSystemLocationType.LOCAL
        ),
        FileSystemLocation(name="OutputDir", path="D:\\output", type=FileSystemLocationType.LOCAL),
    ],
)


def _make_job_response(
    storage_profile_id: str | None = None,
    root_path: str = "/root/path",
    root_path_format: str | None = None,
) -> dict[str, Any]:
    """Build a mock get_job response."""
    if root_path_format is None:
        root_path_format = PathFormat.get_host_path_format()
    result: dict[str, Any] = {
        "name": "Mock Job",
        "attachments": {
            "manifests": [
                {
                    "rootPath": root_path,
                    "rootPathFormat": root_path_format,
                    "outputRelativeDirectories": ["."],
                }
            ]
        },
    }
    if storage_profile_id:
        result["storageProfileId"] = storage_profile_id
    return result


def _make_download_mocks(
    boto3_client_mock: MagicMock,
    mock_output_downloader: MagicMock,
    job_response: dict[str, Any],
    root_paths: dict[str, list[str]],
) -> None:
    """Wire up the common mocks for a download-output test."""
    boto3_client_mock().get_queue.return_value = MOCK_GET_QUEUE_RESPONSE
    boto3_client_mock().get_job.return_value = job_response

    mock_download = MagicMock()
    mock_download.return_value = DownloadSummaryStatistics(
        total_time=1, processed_files=1, processed_bytes=100
    )
    mock_output_downloader.return_value.download_job_output = mock_download
    mock_output_downloader.return_value.get_output_paths_by_root.return_value = root_paths


# ─── Case 1: Both profiles exist, locations match → automatic mapping ────────


def test_case1_both_profiles_match_auto_mapping(fresh_deadline_config: str) -> None:
    """Both profiles exist with matching location names → absolute-path mapping via manifests."""
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("settings.storage_profile_id", "sp-local-111")
    config.set_setting("settings.auto_accept", "true")

    with patch.object(api, "get_boto3_client") as boto3_client_mock, patch.object(
        job_group, "OutputDownloader"
    ) as mock_downloader, patch.object(
        job_group, "_get_conflicting_filenames", return_value=[]
    ), patch.object(job_group, "round", return_value=0), patch.object(
        api, "get_queue_user_boto3_session"
    ), patch.object(api, "get_storage_profile_for_queue") as mock_get_profile, patch.object(
        job_group, "get_output_manifests_by_asset_root", return_value={}
    ) as mock_get_manifests, patch.object(job_group, "_download_mapped_manifests"):
        mock_get_profile.side_effect = [MOCK_LOCAL_PROFILE, MOCK_JOB_PROFILE]

        root = "C:\\temp\\render"
        _make_download_mocks(
            boto3_client_mock,
            mock_downloader,
            _make_job_response(
                storage_profile_id="sp-job-222",
                root_path=root,
                root_path_format=PathFormat.WINDOWS,
            ),
            {root: ["output.exr"]},
        )

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["job", "download-output", "--job-id", MOCK_JOB_ID, "--output", "verbose"],
        )

        assert result.exit_code == 0, result.output
        assert "Using storage profile: Local Linux Profile" in result.output
        # With the new absolute-path approach, we call get_output_manifests_by_asset_root
        # instead of set_root_path
        mock_get_manifests.assert_called_once()


# ─── Case 2: Both profiles exist, locations don't match → no mapping ─────────


def test_case2_both_profiles_no_matching_locations(fresh_deadline_config: str) -> None:
    """Both profiles exist but location names don't match → no mapping, original paths."""
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("settings.storage_profile_id", "sp-local-111")
    config.set_setting("settings.auto_accept", "true")

    mock_root = "/root/path" if sys.platform != "win32" else "C:\\Users\\username"

    with patch.object(api, "get_boto3_client") as boto3_client_mock, patch.object(
        job_group, "OutputDownloader"
    ) as mock_downloader, patch.object(
        job_group, "_get_conflicting_filenames", return_value=[]
    ), patch.object(job_group, "round", return_value=0), patch.object(
        api, "get_queue_user_boto3_session"
    ), patch.object(api, "get_storage_profile_for_queue") as mock_get_profile:
        mock_get_profile.side_effect = [MOCK_LOCAL_PROFILE, MOCK_JOB_PROFILE_NO_MATCH]

        _make_download_mocks(
            boto3_client_mock,
            mock_downloader,
            _make_job_response(storage_profile_id="sp-job-333", root_path=mock_root),
            {mock_root: ["output.exr"]},
        )

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["job", "download-output", "--job-id", MOCK_JOB_ID, "--output", "verbose"],
        )

        assert result.exit_code == 0, result.output
        assert "Using storage profile: Local Linux Profile" in result.output
        # No set_root_path calls because no location names matched
        assert "No path mapping rules could be generated" in result.output
        mock_downloader.return_value.set_root_path.assert_not_called()


# ─── Case 3: Job has profile, local does not → warning, manual fallback ──────


def test_case3_no_local_profile_job_has_profile_warning_fallback(
    fresh_deadline_config: str,
) -> None:
    """Job has a storage profile but local machine doesn't → warning, manual fallback."""
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("settings.auto_accept", "true")

    mock_root = "/root/path" if sys.platform != "win32" else "C:\\Users\\username"

    with patch.object(api, "get_boto3_client") as boto3_client_mock, patch.object(
        job_group, "OutputDownloader"
    ) as mock_downloader, patch.object(
        job_group, "_get_conflicting_filenames", return_value=[]
    ), patch.object(job_group, "round", return_value=0), patch.object(
        api, "get_queue_user_boto3_session"
    ):
        _make_download_mocks(
            boto3_client_mock,
            mock_downloader,
            _make_job_response(storage_profile_id="sp-job-222", root_path=mock_root),
            {mock_root: ["output.exr"]},
        )

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["job", "download-output", "--job-id", MOCK_JOB_ID, "--output", "verbose"],
        )

        assert result.exit_code == 0, result.output
        assert "Warning" in result.output
        assert "no local storage profile" in result.output
        assert "deadline config" in result.output


# ─── Case 4: Neither has a profile → no mapping, direct download ─────────────


def test_case4_no_profiles_either_side(fresh_deadline_config: str) -> None:
    """Neither submitter nor downloader has a profile → download to original paths."""
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("settings.auto_accept", "true")

    mock_root = "/root/path" if sys.platform != "win32" else "C:\\Users\\username"

    with patch.object(api, "get_boto3_client") as boto3_client_mock, patch.object(
        job_group, "OutputDownloader"
    ) as mock_downloader, patch.object(
        job_group, "_get_conflicting_filenames", return_value=[]
    ), patch.object(job_group, "round", return_value=0), patch.object(
        api, "get_queue_user_boto3_session"
    ):
        _make_download_mocks(
            boto3_client_mock,
            mock_downloader,
            _make_job_response(root_path=mock_root),  # no storageProfileId
            {mock_root: ["output.exr"]},
        )

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["job", "download-output", "--job-id", MOCK_JOB_ID, "--output", "verbose"],
        )

        assert result.exit_code == 0, result.output
        assert "Using storage profile" not in result.output
        mock_downloader.return_value.set_root_path.assert_not_called()


# ─── Case 5: Local profile configured, job has none → warning, skip mapping ──


def test_case5_local_profile_job_has_none_warning(fresh_deadline_config: str) -> None:
    """Local profile configured but job submitted without one → warning, no mapping."""
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("settings.storage_profile_id", "sp-local-111")
    config.set_setting("settings.auto_accept", "true")

    mock_root = "/root/path" if sys.platform != "win32" else "C:\\Users\\username"

    with patch.object(api, "get_boto3_client") as boto3_client_mock, patch.object(
        job_group, "OutputDownloader"
    ) as mock_downloader, patch.object(
        job_group, "_get_conflicting_filenames", return_value=[]
    ), patch.object(job_group, "round", return_value=0), patch.object(
        api, "get_queue_user_boto3_session"
    ):
        _make_download_mocks(
            boto3_client_mock,
            mock_downloader,
            _make_job_response(root_path=mock_root),  # no storageProfileId
            {mock_root: ["output.exr"]},
        )

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["job", "download-output", "--job-id", MOCK_JOB_ID, "--output", "verbose"],
        )

        assert result.exit_code == 0, result.output
        assert "Warning" in result.output
        assert "submitted without one" in result.output
        mock_downloader.return_value.set_root_path.assert_not_called()


# ─── Case 6: --ignore-storage-profiles → skip everything ─────────────────────


def test_case6_ignore_storage_profiles_flag(fresh_deadline_config: str) -> None:
    """--ignore-storage-profiles skips all storage profile logic."""
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("settings.storage_profile_id", "sp-local-111")
    config.set_setting("settings.auto_accept", "true")

    mock_root = "/root/path" if sys.platform != "win32" else "C:\\Users\\username"

    with patch.object(api, "get_boto3_client") as boto3_client_mock, patch.object(
        job_group, "OutputDownloader"
    ) as mock_downloader, patch.object(
        job_group, "_get_conflicting_filenames", return_value=[]
    ), patch.object(job_group, "round", return_value=0), patch.object(
        api, "get_queue_user_boto3_session"
    ), patch.object(api, "get_storage_profile_for_queue") as mock_get_profile:
        _make_download_mocks(
            boto3_client_mock,
            mock_downloader,
            _make_job_response(storage_profile_id="sp-job-222", root_path=mock_root),
            {mock_root: ["output.exr"]},
        )

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "download-output",
                "--job-id",
                MOCK_JOB_ID,
                "--ignore-storage-profiles",
                "--output",
                "verbose",
            ],
        )

        assert result.exit_code == 0, result.output
        assert "Using storage profile" not in result.output
        # get_storage_profile_for_queue should never be called
        mock_get_profile.assert_not_called()
        mock_downloader.return_value.set_root_path.assert_not_called()


# ─── Case 7: Same profile on both sides → no mapping needed ──────────────────


def test_case7_same_profile_both_sides(fresh_deadline_config: str) -> None:
    """Same storage profile on submitter and downloader → no mapping rules generated."""
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("settings.storage_profile_id", "sp-local-111")
    config.set_setting("settings.auto_accept", "true")

    mock_root = "/mnt/shared" if sys.platform != "win32" else "C:\\Users\\username"

    with patch.object(api, "get_boto3_client") as boto3_client_mock, patch.object(
        job_group, "OutputDownloader"
    ) as mock_downloader, patch.object(
        job_group, "_get_conflicting_filenames", return_value=[]
    ), patch.object(job_group, "round", return_value=0), patch.object(
        api, "get_queue_user_boto3_session"
    ), patch.object(api, "get_storage_profile_for_queue") as mock_get_profile:
        # Both calls return the same profile
        mock_get_profile.side_effect = [MOCK_LOCAL_PROFILE, MOCK_LOCAL_PROFILE]

        _make_download_mocks(
            boto3_client_mock,
            mock_downloader,
            _make_job_response(storage_profile_id="sp-local-111", root_path=mock_root),
            {mock_root: ["output.exr"]},
        )

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["job", "download-output", "--job-id", MOCK_JOB_ID, "--output", "verbose"],
        )

        assert result.exit_code == 0, result.output
        assert "Using storage profile: Local Linux Profile" in result.output
        # Same profile → empty rules → no set_root_path calls
        mock_downloader.return_value.set_root_path.assert_not_called()


# ─── Case 8-E: Nested file system locations → most specific rule wins ────────

# Profiles with nested locations: "special" is a subdirectory of "projects"
MOCK_NESTED_JOB_PROFILE = StorageProfile(
    storageProfileId="sp-nested-job-444",
    displayName="Nested Windows Profile",
    osFamily=StorageProfileOperatingSystemFamily.WINDOWS,
    fileSystemLocations=[
        FileSystemLocation(name="projects", path="C:\\Projects", type=FileSystemLocationType.LOCAL),
        FileSystemLocation(
            name="special", path="C:\\Projects\\Special", type=FileSystemLocationType.LOCAL
        ),
    ],
)

MOCK_NESTED_LOCAL_PROFILE = StorageProfile(
    storageProfileId="sp-nested-local-555",
    displayName="Nested Linux Profile",
    osFamily=StorageProfileOperatingSystemFamily.LINUX,
    fileSystemLocations=[
        FileSystemLocation(
            name="projects", path="/mnt/projects", type=FileSystemLocationType.LOCAL
        ),
        FileSystemLocation(name="special", path="/opt/special", type=FileSystemLocationType.LOCAL),
    ],
)


def _make_mock_manifest(paths: list[tuple[str, int]]) -> MagicMock:
    """Create a mock BaseAssetManifest with the given (relative_path, size) pairs."""
    manifest = MagicMock()
    manifest.hashAlg = "xxh128"
    mock_paths = []
    for path, size in paths:
        mp = MagicMock()
        mp.path = path
        mp.size = size
        mp.hash = "abc123"
        mp.mtime = 1000000
        mock_paths.append(mp)
    manifest.paths = mock_paths
    manifest.totalSize = sum(s for _, s in paths)
    return manifest


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX destination test")
def test_case8e_nested_locations_most_specific_rule_wins(
    fresh_deadline_config: str,
) -> None:
    """Nested file system locations: the most specific rule wins via absolute-path transformation.

    Source profile (Windows):
      - "projects": C:\\Projects
      - "special":  C:\\Projects\\Special

    Destination profile (Linux):
      - "projects": /mnt/projects
      - "special":  /opt/special

    Job output root: C:\\Projects, relative path: Special\\data.txt
    Absolute path: C:\\Projects\\Special\\data.txt → should match "special" rule → /opt/special/data.txt
    NOT /mnt/projects/Special/data.txt (which root-only transformation would produce).
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("settings.storage_profile_id", "sp-nested-local-555")
    config.set_setting("settings.auto_accept", "true")

    root = "C:\\Projects"
    mock_manifest = _make_mock_manifest([("Special\\data.txt", 100)])

    with patch.object(api, "get_boto3_client") as boto3_client_mock, patch.object(
        job_group, "OutputDownloader"
    ) as mock_downloader, patch.object(
        job_group, "_get_conflicting_filenames", return_value=[]
    ), patch.object(job_group, "round", return_value=0), patch.object(
        api, "get_queue_user_boto3_session"
    ), patch.object(api, "get_storage_profile_for_queue") as mock_get_profile, patch.object(
        job_group,
        "get_output_manifests_by_asset_root",
        return_value={root: [mock_manifest]},
    ), patch.object(job_group, "_download_mapped_manifests") as mock_download_manifests:
        mock_get_profile.side_effect = [MOCK_NESTED_LOCAL_PROFILE, MOCK_NESTED_JOB_PROFILE]
        mock_download_manifests.return_value = DownloadSummaryStatistics(
            total_time=1, processed_files=1, processed_bytes=100
        )

        _make_download_mocks(
            boto3_client_mock,
            mock_downloader,
            _make_job_response(
                storage_profile_id="sp-nested-job-444",
                root_path=root,
                root_path_format=PathFormat.WINDOWS,
            ),
            {root: ["Special\\data.txt"]},
        )

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["job", "download-output", "--job-id", MOCK_JOB_ID, "--output", "verbose"],
        )

        assert result.exit_code == 0, result.output
        assert "Using storage profile: Nested Linux Profile" in result.output
        assert "Mapped 1 output file(s)" in result.output

        # Verify the manifest path was transformed to the SPECIFIC rule destination
        # The "special" rule (C:\Projects\Special -> /opt/special) should win over
        # the "projects" rule (C:\Projects -> /mnt/projects)
        assert mock_manifest.paths[0].path == "/opt/special/data.txt"

        # Verify mapped manifest download was called (not OutputDownloader)
        mock_download_manifests.assert_called_once()
        mock_downloader.return_value.download_job_output.assert_not_called()


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX destination test")
def test_case8e_nested_locations_broader_rule_for_non_nested_path(
    fresh_deadline_config: str,
) -> None:
    """With nested locations, a file NOT under the nested path uses the broader rule.

    Same profiles as Case 8-E, but relative path is "other\\file.txt" (not under Special).
    Absolute path: C:\\Projects\\other\\file.txt → matches "projects" rule → /mnt/projects/other/file.txt
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("settings.storage_profile_id", "sp-nested-local-555")
    config.set_setting("settings.auto_accept", "true")

    root = "C:\\Projects"
    mock_manifest = _make_mock_manifest([("other\\file.txt", 50)])

    with patch.object(api, "get_boto3_client") as boto3_client_mock, patch.object(
        job_group, "OutputDownloader"
    ) as mock_downloader, patch.object(
        job_group, "_get_conflicting_filenames", return_value=[]
    ), patch.object(job_group, "round", return_value=0), patch.object(
        api, "get_queue_user_boto3_session"
    ), patch.object(api, "get_storage_profile_for_queue") as mock_get_profile, patch.object(
        job_group,
        "get_output_manifests_by_asset_root",
        return_value={root: [mock_manifest]},
    ), patch.object(job_group, "_download_mapped_manifests") as mock_download_manifests:
        mock_get_profile.side_effect = [MOCK_NESTED_LOCAL_PROFILE, MOCK_NESTED_JOB_PROFILE]
        mock_download_manifests.return_value = DownloadSummaryStatistics(
            total_time=1, processed_files=1, processed_bytes=100
        )

        _make_download_mocks(
            boto3_client_mock,
            mock_downloader,
            _make_job_response(
                storage_profile_id="sp-nested-job-444",
                root_path=root,
                root_path_format=PathFormat.WINDOWS,
            ),
            {root: ["other\\file.txt"]},
        )

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["job", "download-output", "--job-id", MOCK_JOB_ID, "--output", "verbose"],
        )

        assert result.exit_code == 0, result.output
        # The broader "projects" rule should apply
        assert mock_manifest.paths[0].path == "/mnt/projects/other/file.txt"
        mock_download_manifests.assert_called_once()


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX destination test")
def test_case8e_nested_locations_mixed_paths(
    fresh_deadline_config: str,
) -> None:
    """With nested locations, files under different depths get the correct rules.

    Two files in the same manifest:
      - Special\\data.txt → "special" rule → /opt/special/data.txt
      - other\\file.txt   → "projects" rule → /mnt/projects/other/file.txt
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("settings.storage_profile_id", "sp-nested-local-555")
    config.set_setting("settings.auto_accept", "true")

    root = "C:\\Projects"
    mock_manifest = _make_mock_manifest(
        [
            ("Special\\data.txt", 100),
            ("other\\file.txt", 50),
        ]
    )

    with patch.object(api, "get_boto3_client") as boto3_client_mock, patch.object(
        job_group, "OutputDownloader"
    ) as mock_downloader, patch.object(
        job_group, "_get_conflicting_filenames", return_value=[]
    ), patch.object(job_group, "round", return_value=0), patch.object(
        api, "get_queue_user_boto3_session"
    ), patch.object(api, "get_storage_profile_for_queue") as mock_get_profile, patch.object(
        job_group,
        "get_output_manifests_by_asset_root",
        return_value={root: [mock_manifest]},
    ), patch.object(job_group, "_download_mapped_manifests") as mock_download_manifests:
        mock_get_profile.side_effect = [MOCK_NESTED_LOCAL_PROFILE, MOCK_NESTED_JOB_PROFILE]
        mock_download_manifests.return_value = DownloadSummaryStatistics(
            total_time=1, processed_files=1, processed_bytes=100
        )

        _make_download_mocks(
            boto3_client_mock,
            mock_downloader,
            _make_job_response(
                storage_profile_id="sp-nested-job-444",
                root_path=root,
                root_path_format=PathFormat.WINDOWS,
            ),
            {root: ["Special\\data.txt", "other\\file.txt"]},
        )

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["job", "download-output", "--job-id", MOCK_JOB_ID, "--output", "verbose"],
        )

        assert result.exit_code == 0, result.output
        assert "Mapped 2 output file(s)" in result.output

        # Verify each file got the correct rule
        mapped_paths = {p.path for p in mock_manifest.paths}
        assert "/opt/special/data.txt" in mapped_paths
        assert "/mnt/projects/other/file.txt" in mapped_paths


# ─── Rules exist but no output files match → fallback to OutputDownloader ────


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX destination test")
def test_rules_exist_but_no_files_match_falls_back_to_downloader(
    fresh_deadline_config: str,
) -> None:
    """When rules are generated but no output files match any rule, fall back to
    the OutputDownloader path and download to original (unmapped) paths.

    This covers the case where the job's output root is outside all file system
    locations in the storage profile. The rules exist but don't apply to the
    actual output paths. Rather than skipping the download entirely, we fall
    through to the existing OutputDownloader code path.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("settings.storage_profile_id", "sp-local-111")
    config.set_setting("settings.auto_accept", "true")

    # Output root is NOT under any of the profile's locations
    # (shared=Z:\shared, temp=C:\temp\render) — it's at D:\unrelated
    root = "D:\\unrelated\\output"
    mock_manifest = _make_mock_manifest([("result.exr", 100)])

    with patch.object(api, "get_boto3_client") as boto3_client_mock, patch.object(
        job_group, "OutputDownloader"
    ) as mock_downloader, patch.object(
        job_group, "_get_conflicting_filenames", return_value=[]
    ), patch.object(job_group, "round", return_value=0), patch.object(
        api, "get_queue_user_boto3_session"
    ), patch.object(api, "get_storage_profile_for_queue") as mock_get_profile, patch.object(
        job_group,
        "get_output_manifests_by_asset_root",
        return_value={root: [mock_manifest]},
    ), patch.object(job_group, "_download_mapped_manifests") as mock_download_manifests:
        mock_get_profile.side_effect = [MOCK_LOCAL_PROFILE, MOCK_JOB_PROFILE]

        mock_root = "/root/path"
        _make_download_mocks(
            boto3_client_mock,
            mock_downloader,
            _make_job_response(
                storage_profile_id="sp-job-222",
                root_path=root,
                root_path_format=PathFormat.WINDOWS,
            ),
            {mock_root: ["result.exr"]},
        )

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["job", "download-output", "--job-id", MOCK_JOB_ID, "--output", "verbose"],
        )

        assert result.exit_code == 0, result.output
        assert "Using storage profile: Local Linux Profile" in result.output
        # No files matched the rules, so mapped manifest download should NOT be called
        mock_download_manifests.assert_not_called()
        # Instead, the OutputDownloader path should have been used
        mock_downloader.return_value.download_job_output.assert_called_once()


# ─── Mapped download path: progress callback and conflict resolution ─────────


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX destination test")
def test_mapped_download_passes_progress_callback(
    fresh_deadline_config: str,
) -> None:
    """When downloading via the mapped manifest path, _download_mapped_manifests is called
    which handles progress reporting internally."""
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("settings.storage_profile_id", "sp-local-111")
    config.set_setting("settings.auto_accept", "true")

    root = "C:\\temp\\render"
    mock_manifest = _make_mock_manifest([("output.exr", 100)])

    with patch.object(api, "get_boto3_client") as boto3_client_mock, patch.object(
        job_group, "OutputDownloader"
    ) as mock_downloader, patch.object(
        job_group, "_get_conflicting_filenames", return_value=[]
    ), patch.object(job_group, "round", return_value=0), patch.object(
        api, "get_queue_user_boto3_session"
    ), patch.object(api, "get_storage_profile_for_queue") as mock_get_profile, patch.object(
        job_group,
        "get_output_manifests_by_asset_root",
        return_value={root: [mock_manifest]},
    ), patch.object(job_group, "_download_mapped_manifests") as mock_download_mapped:
        mock_get_profile.side_effect = [MOCK_LOCAL_PROFILE, MOCK_JOB_PROFILE]
        mock_download_mapped.return_value = DownloadSummaryStatistics(
            total_time=1, processed_files=1, processed_bytes=100
        )

        _make_download_mocks(
            boto3_client_mock,
            mock_downloader,
            _make_job_response(
                storage_profile_id="sp-job-222",
                root_path=root,
                root_path_format=PathFormat.WINDOWS,
            ),
            {root: ["output.exr"]},
        )

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["job", "download-output", "--job-id", MOCK_JOB_ID, "--output", "verbose"],
        )

        assert result.exit_code == 0, result.output
        mock_download_mapped.assert_called_once()
        call_kwargs = mock_download_mapped.call_args[1]
        # is_json_format=False for verbose mode
        assert call_kwargs["is_json_format"] is False
        # Conflict resolution setting must be passed
        assert "conflict_resolution_setting" in call_kwargs


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX destination test")
def test_mapped_download_uses_configured_conflict_resolution(
    fresh_deadline_config: str,
) -> None:
    """When conflict_resolution is set in config, _download_mapped_manifests receives it."""
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("settings.storage_profile_id", "sp-local-111")
    config.set_setting("settings.auto_accept", "true")
    config.set_setting("settings.conflict_resolution", "SKIP")

    root = "C:\\temp\\render"
    mock_manifest = _make_mock_manifest([("output.exr", 100)])

    with patch.object(api, "get_boto3_client") as boto3_client_mock, patch.object(
        job_group, "OutputDownloader"
    ) as mock_downloader, patch.object(
        job_group, "_get_conflicting_filenames", return_value=[]
    ), patch.object(job_group, "round", return_value=0), patch.object(
        api, "get_queue_user_boto3_session"
    ), patch.object(api, "get_storage_profile_for_queue") as mock_get_profile, patch.object(
        job_group,
        "get_output_manifests_by_asset_root",
        return_value={root: [mock_manifest]},
    ), patch.object(job_group, "_download_mapped_manifests") as mock_download_mapped:
        mock_get_profile.side_effect = [MOCK_LOCAL_PROFILE, MOCK_JOB_PROFILE]
        mock_download_mapped.return_value = DownloadSummaryStatistics(
            total_time=1, processed_files=1, processed_bytes=100
        )

        _make_download_mocks(
            boto3_client_mock,
            mock_downloader,
            _make_job_response(
                storage_profile_id="sp-job-222",
                root_path=root,
                root_path_format=PathFormat.WINDOWS,
            ),
            {root: ["output.exr"]},
        )

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["job", "download-output", "--job-id", MOCK_JOB_ID, "--output", "verbose"],
        )

        assert result.exit_code == 0, result.output
        call_kwargs = mock_download_mapped.call_args[1]
        assert call_kwargs["conflict_resolution_setting"] == "SKIP"


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX destination test")
def test_mapped_download_json_mode_emits_progress(
    fresh_deadline_config: str,
) -> None:
    """In JSON mode, _download_mapped_manifests receives is_json_format=True."""
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("settings.storage_profile_id", "sp-local-111")
    config.set_setting("settings.auto_accept", "true")

    root = "C:\\temp\\render"
    mock_manifest = _make_mock_manifest([("output.exr", 100)])

    with patch.object(api, "get_boto3_client") as boto3_client_mock, patch.object(
        job_group, "OutputDownloader"
    ) as mock_downloader, patch.object(
        job_group, "_get_conflicting_filenames", return_value=[]
    ), patch.object(job_group, "round", return_value=0), patch.object(
        api, "get_queue_user_boto3_session"
    ), patch.object(api, "get_storage_profile_for_queue") as mock_get_profile, patch.object(
        job_group,
        "get_output_manifests_by_asset_root",
        return_value={root: [mock_manifest]},
    ), patch.object(job_group, "_download_mapped_manifests") as mock_download_mapped:
        mock_get_profile.side_effect = [MOCK_LOCAL_PROFILE, MOCK_JOB_PROFILE]
        mock_download_mapped.return_value = DownloadSummaryStatistics(
            total_time=1, processed_files=1, processed_bytes=100
        )

        _make_download_mocks(
            boto3_client_mock,
            mock_downloader,
            _make_job_response(
                storage_profile_id="sp-job-222",
                root_path=root,
                root_path_format=PathFormat.WINDOWS,
            ),
            {root: ["output.exr"]},
        )

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["job", "download-output", "--job-id", MOCK_JOB_ID, "--output", "json"],
        )

        assert result.exit_code == 0, result.output
        mock_download_mapped.assert_called_once()
        call_kwargs = mock_download_mapped.call_args[1]
        # JSON mode should pass is_json_format=True
        assert call_kwargs["is_json_format"] is True
