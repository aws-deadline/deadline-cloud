# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the _job_download_helpers module.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from click.testing import CliRunner

from deadline.client import api, config
from deadline.client.cli import main
from deadline.client.cli._groups import job_group
from deadline.client.cli._groups._job_download_helpers import (
    ResolvedStorageProfiles,
    _apply_path_mappings_to_roots,
    _resolve_storage_profiles,
)
from deadline.job_attachments._path_mapping import _generate_path_mapping_rules
from deadline.job_attachments.models import (
    FileSystemLocation,
    FileSystemLocationType,
    PathFormat,
    PathMappingRule,
    StorageProfile,
    StorageProfileOperatingSystemFamily,
)
from deadline.job_attachments.progress_tracker import DownloadSummaryStatistics


MOCK_FARM_ID = "farm-0123456789abcdefabcdefabcdefabcd"
MOCK_QUEUE_ID = "queue-0123456789abcdefabcdefabcdefabcd"

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

MOCK_SAME_PROFILE = StorageProfile(
    storageProfileId="sp-local-111",
    displayName="Local Linux Profile",
    osFamily=StorageProfileOperatingSystemFamily.LINUX,
    fileSystemLocations=[
        FileSystemLocation(name="shared", path="/mnt/shared", type=FileSystemLocationType.SHARED),
        FileSystemLocation(name="temp", path="/tmp/render", type=FileSystemLocationType.LOCAL),
    ],
)


# ─── _resolve_storage_profiles ───────────────────────────────────────────────


class TestResolveStorageProfiles:
    def test_ignore_storage_profiles_returns_none(self) -> None:
        result = _resolve_storage_profiles(
            config=None,
            deadline=MagicMock(),
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            job={},
            ignore_storage_profiles=True,
        )
        assert result is None

    @patch("deadline.client.cli._groups._job_download_helpers.config_file")
    def test_no_profiles_on_either_side_returns_none(self, mock_config_file: MagicMock) -> None:
        mock_config_file.get_setting.return_value = ""
        result = _resolve_storage_profiles(
            config=None,
            deadline=MagicMock(),
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            job={},  # no storageProfileId
            ignore_storage_profiles=False,
        )
        assert result is None

    @patch("deadline.client.cli._groups._job_download_helpers.config_file")
    def test_no_local_profile_but_job_has_profile_warns_and_returns_none(
        self, mock_config_file: MagicMock
    ) -> None:
        mock_config_file.get_setting.return_value = ""
        result = _resolve_storage_profiles(
            config=None,
            deadline=MagicMock(),
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            job={"storageProfileId": "sp-job-222"},
            ignore_storage_profiles=False,
        )
        assert result is None

    @patch("deadline.client.cli._groups._job_download_helpers.config_file")
    def test_local_profile_but_job_has_no_profile_returns_none_with_warning(
        self, mock_config_file: MagicMock
    ) -> None:
        mock_config_file.get_setting.return_value = "sp-local-111"
        result = _resolve_storage_profiles(
            config=None,
            deadline=MagicMock(),
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            job={},  # no storageProfileId
            ignore_storage_profiles=False,
        )
        assert result is None

    @patch("deadline.client.cli._groups._job_download_helpers.api")
    @patch("deadline.client.cli._groups._job_download_helpers.config_file")
    def test_both_profiles_exist_returns_resolved(
        self, mock_config_file: MagicMock, mock_api: MagicMock
    ) -> None:
        mock_config_file.get_setting.return_value = "sp-local-111"
        mock_api.get_storage_profile_for_queue.side_effect = [
            MOCK_LOCAL_PROFILE,
            MOCK_JOB_PROFILE,
        ]

        result = _resolve_storage_profiles(
            config=None,
            deadline=MagicMock(),
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            job={"storageProfileId": "sp-job-222"},
            ignore_storage_profiles=False,
        )

        assert result is not None
        assert isinstance(result, ResolvedStorageProfiles)
        assert result.local_profile == MOCK_LOCAL_PROFILE
        assert result.job_profile == MOCK_JOB_PROFILE

    @patch("deadline.client.cli._groups._job_download_helpers.api")
    @patch("deadline.client.cli._groups._job_download_helpers.config_file")
    def test_api_error_propagates(self, mock_config_file: MagicMock, mock_api: MagicMock) -> None:
        mock_config_file.get_setting.return_value = "sp-local-111"
        mock_api.get_storage_profile_for_queue.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "Not found"}},
            "GetStorageProfileForQueue",
        )

        with pytest.raises(ClientError):
            _resolve_storage_profiles(
                config=None,
                deadline=MagicMock(),
                farm_id=MOCK_FARM_ID,
                queue_id=MOCK_QUEUE_ID,
                job={"storageProfileId": "sp-job-222"},
                ignore_storage_profiles=False,
            )


# ─── _apply_path_mappings_to_roots ──────────────────────────────────────────


class TestApplyPathMappingsToRoots:
    def test_empty_rules_is_noop(self) -> None:
        downloader = MagicMock()
        output_paths = {"/original/root": ["file1.txt"]}
        _apply_path_mappings_to_roots(downloader, output_paths, [])
        downloader.set_root_path.assert_not_called()

    @pytest.mark.skipif(sys.platform == "win32", reason="POSIX destination test")
    def test_single_mapping_applied_posix(self) -> None:
        """On Linux/macOS: source is Windows, destination is local POSIX."""
        downloader = MagicMock()
        output_paths = {"C:\\temp\\render": ["output.exr"]}
        rules = [
            PathMappingRule(
                source_path_format=PathFormat.WINDOWS.value,
                source_path="C:\\temp\\render",
                destination_path="/tmp/render",
            )
        ]
        _apply_path_mappings_to_roots(downloader, output_paths, rules)
        downloader.set_root_path.assert_called_once_with("C:\\temp\\render", "/tmp/render")

    @pytest.mark.skipif(sys.platform != "win32", reason="Windows destination test")
    def test_single_mapping_applied_windows(self) -> None:
        """On Windows: source is POSIX, destination is local Windows."""
        downloader = MagicMock()
        output_paths = {"/tmp/render": ["output.exr"]}
        rules = [
            PathMappingRule(
                source_path_format=PathFormat.POSIX.value,
                source_path="/tmp/render",
                destination_path="C:\\temp\\render",
            )
        ]
        _apply_path_mappings_to_roots(downloader, output_paths, rules)
        downloader.set_root_path.assert_called_once_with("/tmp/render", "C:\\temp\\render")

    @pytest.mark.skipif(sys.platform == "win32", reason="POSIX destination test")
    def test_multiple_mappings_applied_posix(self) -> None:
        """On Linux/macOS: multiple Windows→POSIX mappings."""
        downloader = MagicMock()
        output_paths = {
            "C:\\temp\\render": ["output.exr"],
            "Z:\\shared": ["data.bin"],
        }
        rules = [
            PathMappingRule(
                source_path_format=PathFormat.WINDOWS.value,
                source_path="C:\\temp\\render",
                destination_path="/tmp/render",
            ),
            PathMappingRule(
                source_path_format=PathFormat.WINDOWS.value,
                source_path="Z:\\shared",
                destination_path="/mnt/shared",
            ),
        ]
        _apply_path_mappings_to_roots(downloader, output_paths, rules)
        assert downloader.set_root_path.call_count == 2

    @pytest.mark.skipif(sys.platform != "win32", reason="Windows destination test")
    def test_multiple_mappings_applied_windows(self) -> None:
        """On Windows: multiple POSIX→Windows mappings."""
        downloader = MagicMock()
        output_paths = {
            "/tmp/render": ["output.exr"],
            "/mnt/shared": ["data.bin"],
        }
        rules = [
            PathMappingRule(
                source_path_format=PathFormat.POSIX.value,
                source_path="/tmp/render",
                destination_path="C:\\temp\\render",
            ),
            PathMappingRule(
                source_path_format=PathFormat.POSIX.value,
                source_path="/mnt/shared",
                destination_path="Z:\\shared",
            ),
        ]
        _apply_path_mappings_to_roots(downloader, output_paths, rules)
        assert downloader.set_root_path.call_count == 2

    def test_no_matching_rule_skips_root(self) -> None:
        downloader = MagicMock()
        if sys.platform == "win32":
            output_paths = {"D:\\other\\path": ["file.txt"]}
            rules = [
                PathMappingRule(
                    source_path_format=PathFormat.WINDOWS.value,
                    source_path="C:\\temp\\render",
                    destination_path="D:\\mapped",
                )
            ]
        else:
            output_paths = {"/some/other/path": ["file.txt"]}
            rules = [
                PathMappingRule(
                    source_path_format=PathFormat.POSIX.value,
                    source_path="/mnt/shared",
                    destination_path="/opt/shared",
                )
            ]
        _apply_path_mappings_to_roots(downloader, output_paths, rules)
        downloader.set_root_path.assert_not_called()

    def test_root_already_matches_destination_skips(self) -> None:
        downloader = MagicMock()
        if sys.platform == "win32":
            # Root is already the Windows destination — no source match
            output_paths = {"C:\\temp\\render": ["file.txt"]}
            rules = [
                PathMappingRule(
                    source_path_format=PathFormat.POSIX.value,
                    source_path="/tmp/render",
                    destination_path="C:\\temp\\render",
                )
            ]
        else:
            # Root is already the POSIX destination — no source match
            output_paths = {"/tmp/render": ["file.txt"]}
            rules = [
                PathMappingRule(
                    source_path_format=PathFormat.WINDOWS.value,
                    source_path="C:\\temp\\render",
                    destination_path="/tmp/render",
                )
            ]
        _apply_path_mappings_to_roots(downloader, output_paths, rules)
        downloader.set_root_path.assert_not_called()


# ─── _generate_path_mapping_rules with StorageProfile dataclass ─────────────


class TestGeneratePathMappingRulesWithDataclass:
    def test_same_profile_returns_empty(self) -> None:
        rules = _generate_path_mapping_rules(MOCK_LOCAL_PROFILE, MOCK_SAME_PROFILE)
        assert rules == []

    def test_cross_os_profiles_generate_rules(self) -> None:
        rules = _generate_path_mapping_rules(MOCK_JOB_PROFILE, MOCK_LOCAL_PROFILE)
        assert len(rules) == 2
        # "shared" location maps Z:\shared -> /mnt/shared
        assert PathMappingRule(PathFormat.WINDOWS.value, "Z:\\shared", "/mnt/shared") in rules
        # "temp" location maps C:\temp\render -> /tmp/render
        assert PathMappingRule(PathFormat.WINDOWS.value, "C:\\temp\\render", "/tmp/render") in rules

    def test_mixed_dict_and_dataclass(self) -> None:
        """Verify that passing one dict and one dataclass still works."""
        job_dict: dict[str, Any] = {
            "storageProfileId": "sp-job-222",
            "osFamily": StorageProfileOperatingSystemFamily.WINDOWS.value,
            "fileSystemLocations": [
                {"name": "shared", "path": "Z:\\shared", "type": "SHARED"},
                {"name": "temp", "path": "C:\\temp\\render", "type": "LOCAL"},
            ],
        }
        rules = _generate_path_mapping_rules(job_dict, MOCK_LOCAL_PROFILE)
        assert len(rules) == 2

    def test_no_matching_locations_returns_empty(self) -> None:
        no_match_profile = StorageProfile(
            storageProfileId="sp-nomatch-333",
            displayName="No Match",
            osFamily=StorageProfileOperatingSystemFamily.LINUX,
            fileSystemLocations=[
                FileSystemLocation(
                    name="different", path="/mnt/different", type=FileSystemLocationType.LOCAL
                ),
            ],
        )
        rules = _generate_path_mapping_rules(MOCK_JOB_PROFILE, no_match_profile)
        assert rules == []


# ─── CLI-level tests for download-output with storage profile options ────────

MOCK_JOB_ID = "job-0123456789abcdefabcdefabcdefabcd"

MOCK_GET_QUEUE_RESPONSE = {
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


class TestCliDownloadOutputStorageProfileOptions:
    def test_ignore_storage_profiles_skips_mapping(
        self, fresh_deadline_config: str, tmp_path: Path
    ) -> None:
        """--ignore-storage-profiles should skip storage profile resolution."""
        config.set_setting("defaults.farm_id", MOCK_FARM_ID)
        config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
        config.set_setting("settings.auto_accept", "true")

        mock_root = "/root/path" if sys.platform != "win32" else "C:\\Users\\username"
        mock_host_format = PathFormat.get_host_path_format()

        with patch.object(api, "get_boto3_client") as boto3_client_mock, patch.object(
            job_group, "OutputDownloader"
        ) as MockOutputDownloader, patch.object(
            job_group, "_get_conflicting_filenames", return_value=[]
        ), patch.object(job_group, "round", return_value=0), patch.object(
            api, "get_queue_user_boto3_session"
        ), patch.object(job_group, "_resolve_storage_profiles", return_value=None) as mock_resolve:
            mock_download = MagicMock()
            mock_download.return_value = DownloadSummaryStatistics(
                total_time=1, processed_files=1, processed_bytes=100
            )
            MockOutputDownloader.return_value.download_job_output = mock_download
            MockOutputDownloader.return_value.get_output_paths_by_root.return_value = {
                mock_root: ["file.txt"]
            }

            boto3_client_mock().get_queue.return_value = MOCK_GET_QUEUE_RESPONSE
            boto3_client_mock().get_job.return_value = {
                "name": "Mock Job",
                "attachments": {
                    "manifests": [
                        {
                            "rootPath": mock_root,
                            "rootPathFormat": mock_host_format,
                            "outputRelativeDirectories": ["."],
                        }
                    ]
                },
            }

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

            assert result.exit_code == 0
            mock_resolve.assert_called_once()
            # Verify ignore_storage_profiles=True was passed (positional arg index 5)
            call_args = mock_resolve.call_args
            assert call_args[0][5] is True
