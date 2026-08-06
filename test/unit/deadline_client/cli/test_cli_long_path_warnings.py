# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests that the CLI's Windows long-path warnings are not suppressed by the
LongPathsEnabled registry setting.

That setting only takes effect for processes declaring longPathAware in their
application manifest, so it says nothing about whether a path is safe for the tools
and applications that ultimately open these files. Gating the warnings on it silenced
them on exactly the hosts where paths are longest.
"""

from __future__ import annotations

from unittest.mock import ANY, patch

import pytest

from deadline.client import api
from deadline.client.cli import main
from deadline.client.cli._groups import job_group, manifest_group
from deadline.client.config import set_setting
from deadline.job_attachments._utils import WINDOWS_MAX_PATH_LENGTH
from deadline.job_attachments.models import PathFormat
from click.testing import CliRunner

REGISTRY_HELPER = "_is_windows_long_path_registry_enabled"

MOCK_FARM_ID = "farm-0123456789abcdefabcdefabcdefabcd"
MOCK_QUEUE_ID = "queue-0123456789abcdefabcdefabcdefabcd"
MOCK_JOB_ID = "job-0123456789abcdefabcdefabcdefabcd"
MOCK_GET_QUEUE_RESPONSE = {
    "queueId": MOCK_QUEUE_ID,
    "displayName": "Mock Queue",
    "jobAttachmentSettings": {"s3BucketName": "mock_bucket", "rootPrefix": "mock_deadline"},
}


@pytest.mark.parametrize("module", [job_group, manifest_group], ids=["job", "manifest"])
def test_registry_helper_no_longer_used(module):
    """
    The warnings are unconditional on Windows now, so neither module should still pull in
    the registry helper. Guards against the check being reintroduced.
    """
    assert not hasattr(module, REGISTRY_HELPER)


class TestDownloadOutputLongPathWarning:
    """
    Exercises `_check_and_warn_long_output_paths` by running the real
    `deadline job download-output` command with a stubbed downloader.
    """

    def _invoke(self, fresh_deadline_config, output_paths_by_root, registry_enabled: bool):
        set_setting("settings.auto_accept", "true")

        with (
            patch.object(api, "get_boto3_client") as boto3_client_mock,
            patch.object(job_group, "OutputDownloader") as MockOutputDownloader,
            patch.object(job_group, "_resolve_storage_profiles", return_value=None),
            patch.object(job_group.sys, "platform", "win32"),
            patch(
                f"deadline.job_attachments._utils.{REGISTRY_HELPER}",
                return_value=registry_enabled,
            ),
        ):
            downloader = MockOutputDownloader.return_value
            downloader.get_paths_by_root.return_value = output_paths_by_root
            downloader.download.return_value = ANY

            boto3_client_mock().get_job.return_value = {
                "name": "Mock Job",
                "attachments": {
                    "manifests": [
                        {
                            "rootPath": next(iter(output_paths_by_root)),
                            "rootPathFormat": PathFormat.WINDOWS,
                            "outputRelativeDirectories": ["."],
                        }
                    ],
                },
            }
            boto3_client_mock().get_queue.side_effect = [MOCK_GET_QUEUE_RESPONSE]

            return CliRunner().invoke(
                main,
                [
                    "job",
                    "download-output",
                    "--farm-id",
                    MOCK_FARM_ID,
                    "--queue-id",
                    MOCK_QUEUE_ID,
                    "--job-id",
                    MOCK_JOB_ID,
                ],
            )

    @pytest.mark.parametrize("registry_enabled", [True, False])
    def test_long_output_path_warns_regardless_of_registry(
        self, fresh_deadline_config, registry_enabled
    ):
        """
        The regression test: with the registry setting on, the warning used to be silenced.
        """
        root = "C:\\root\\"
        long_path = "a" * WINDOWS_MAX_PATH_LENGTH

        result = self._invoke(fresh_deadline_config, {root: [long_path]}, registry_enabled)

        assert "exceed Windows path length limit" in result.output

    @pytest.mark.parametrize("registry_enabled", [True, False])
    def test_short_output_path_does_not_warn(self, fresh_deadline_config, registry_enabled):
        result = self._invoke(
            fresh_deadline_config, {"C:\\root\\": ["short.exr"]}, registry_enabled
        )

        assert "exceed Windows path length limit" not in result.output


class TestLongPathMessages:
    def test_download_warning_message_names_the_problem(self):
        """The user-facing text should name the problem and point at the fix."""
        message = job_group._get_long_path_found_message(is_json_format=False)

        assert "exceed Windows path length limit" in message
        assert "maximum-file-path-limitation" in message
