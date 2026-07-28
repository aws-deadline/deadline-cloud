# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the known asset paths functionality in the bundle_submit CLI command.
"""

import os
import json
import tempfile
from pathlib import Path
from unittest.mock import patch, ANY, call

import click
from click.testing import CliRunner
import pytest

from deadline.client import config
from deadline.client.cli import main
from deadline.client.api._submit_job_bundle import (
    _filter_redundant_known_paths,
    _generate_message_for_asset_paths,
    _is_known_path,
)
from deadline.job_attachments.models import AssetRootGroup, AssetUploadGroup

from ..api.test_job_bundle_submission import (
    MOCK_FARM_ID,
    MOCK_QUEUE_ID,
    MOCK_JOB_TEMPLATE_CASES,
)
from ..testing_utilities import patch_calls_for_create_job_from_job_bundle


@pytest.mark.parametrize(
    "input, expected",
    [
        ([], []),
        (["/a", "/a", "/a"], ["/a"]),
        (["/a/b", "/a/b/c", "/a/", "/a"], ["/a"]),
        (["/a", "/"], ["/"]),
        (["/a", "/b", "/a"], ["/a", "/b"]),
        (["/a", "/b", "/c"], ["/a", "/b", "/c"]),
        (["/a", "/b", "/a", "/c", "/b", "/a"], ["/a", "/b", "/c"]),
    ],
)
def test_filter_redundant_known_paths(input, expected):
    assert sorted(_filter_redundant_known_paths(input)) == expected
    if os.name == "nt":
        assert sorted(_filter_redundant_known_paths(path.replace("/", "\\") for path in input)) == [
            path.replace("/", "\\") for path in expected
        ]
        assert sorted(
            _filter_redundant_known_paths("C:" + path.replace("/", "\\") for path in input)
        ) == ["C:" + path.replace("/", "\\") for path in expected]


@pytest.mark.parametrize(
    "path, roots, expected",
    [
        # The root itself is contained.
        (
            os.path.join(os.sep, "trusted", "project"),
            [os.path.join(os.sep, "trusted", "project")],
            True,
        ),
        # A genuine descendant is contained.
        (
            os.path.join(os.sep, "trusted", "project", "sub", "file"),
            [os.path.join(os.sep, "trusted", "project")],
            True,
        ),
        # Core regression: a sibling sharing a string prefix is NOT contained.
        (
            os.path.join(os.sep, "trusted", "project-secret", "f"),
            [os.path.join(os.sep, "trusted", "project")],
            False,
        ),
        # An unrelated path is not contained.
        (
            os.path.join(os.sep, "somewhere", "else"),
            [os.path.join(os.sep, "trusted", "project")],
            False,
        ),
        # Contained by one of several roots.
        (
            os.path.join(os.sep, "b", "file"),
            [os.path.join(os.sep, "a"), os.path.join(os.sep, "b")],
            True,
        ),
        # Mixed absolute/relative (commonpath raises ValueError) -> not contained.
        (
            os.path.join("relative", "file"),
            [os.path.join(os.sep, "trusted", "project")],
            False,
        ),
        # No roots -> nothing is contained.
        (os.path.join(os.sep, "trusted", "project"), [], False),
        # A candidate for which the root is a string prefix with no separator
        # (/trusted/projectextra) is NOT contained.
        (
            os.path.join(os.sep, "trusted", "project") + "extra",
            [os.path.join(os.sep, "trusted", "project")],
            False,
        ),
        # A '..' traversal that escapes the root is NOT contained.
        (
            os.path.join(os.sep, "trusted", "project", "..", "project-secret", "f"),
            [os.path.join(os.sep, "trusted", "project")],
            False,
        ),
        # A '..' round-trip that stays inside the root IS contained.
        (
            os.path.join(os.sep, "trusted", "project", "sub", "..", "f"),
            [os.path.join(os.sep, "trusted", "project")],
            True,
        ),
        # A trailing separator on the root does not defeat sibling-prefix rejection.
        (
            os.path.join(os.sep, "trusted", "project-secret", "f"),
            [os.path.join(os.sep, "trusted", "project") + os.sep],
            False,
        ),
        # A trailing separator on the root still accepts a genuine descendant.
        (
            os.path.join(os.sep, "trusted", "project", "f"),
            [os.path.join(os.sep, "trusted", "project") + os.sep],
            True,
        ),
        # The parent directory of a root is NOT contained.
        (
            os.path.join(os.sep, "trusted"),
            [os.path.join(os.sep, "trusted", "project")],
            False,
        ),
        # Case-variant alias: os.path.commonpath compares case-insensitively on
        # Windows (matching the filesystem) and case-sensitively on POSIX, where
        # the mismatch fails closed (warning still fires).
        (
            os.path.join(os.sep, "Trusted", "Project", "f"),
            [os.path.join(os.sep, "trusted", "project")],
            os.name == "nt",
        ),
    ],
)
def test_is_known_path(path, roots, expected):
    assert _is_known_path(path, roots) is expected


def test_generate_message_for_asset_paths_sibling_prefix_is_unknown():
    """
    Security regression test: a known root must NOT "contain" a sibling path that
    merely shares a string prefix. e.g. known root '/trusted/project' must not
    suppress the warning for '/trusted/project-secret/file', which lives outside
    the trusted root. An unanchored prefix match would wrongly treat it as inside.
    """
    known_root = os.path.join(os.sep, "trusted", "project")
    sibling_file = os.path.join(os.sep, "trusted", "project-secret", "file")

    upload_group = AssetUploadGroup(
        asset_groups=[AssetRootGroup(root_path=known_root, inputs={Path(sibling_file)})],
        total_input_files=1,
        total_input_bytes=12,
    )

    message, no_warnings = _generate_message_for_asset_paths(
        upload_group, storage_profile=None, known_asset_paths=[known_root]
    )

    # The sibling file is outside the trusted root, so the warning must fire.
    assert no_warnings is False, message
    assert "WARNING: Files were specified outside of known asset paths." in message, message
    assert sibling_file in message, message


def test_generate_message_for_asset_paths_descendant_is_known():
    """
    A file that is a genuine descendant of a known root must be treated as inside
    (no warning), confirming the anchored containment check does not over-reject.
    """
    known_root = os.path.join(os.sep, "trusted", "project")
    inside_file = os.path.join(known_root, "sub", "file")

    upload_group = AssetUploadGroup(
        asset_groups=[AssetRootGroup(root_path=known_root, inputs={Path(inside_file)})],
        total_input_files=1,
        total_input_bytes=12,
    )

    message, no_warnings = _generate_message_for_asset_paths(
        upload_group, storage_profile=None, known_asset_paths=[known_root]
    )

    assert no_warnings is True, message
    assert "WARNING: Files were specified outside of known asset paths." not in message, message


def test_cli_bundle_known_paths_combine(fresh_deadline_config, temp_job_bundle_dir):
    """
    Test that the CLI combines known upload paths from both the CLI parameter
    and the configuration setting.
    """
    # Set up configuration
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("settings.auto_accept", "true")

    # Set known paths in config using OS-specific path separator
    # Note: on Windows, backslashes are normalized to forward slashes in the config file,
    # but get_setting converts them back to native format on read.
    config_path1 = "/path/from/config/1" if os.name != "nt" else "C:\\path\\from\\config\\1"
    config_path2 = "/path/from/config/2" if os.name != "nt" else "C:\\path\\from\\config\\2"
    config.set_setting("settings.known_asset_paths", os.pathsep.join([config_path1, config_path2]))

    # Write a JSON template
    with open(os.path.join(temp_job_bundle_dir, "template.json"), "w", encoding="utf8") as f:
        f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1])

    # Create a file outside the job bundle directory
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(b"test content")
        external_file_path = temp_file.name

    try:
        # Create asset references pointing to the external file
        with open(
            os.path.join(temp_job_bundle_dir, "asset_references.json"), "w", encoding="utf8"
        ) as f:
            json.dump(
                {
                    "assetReferences": {
                        "inputs": {
                            "filenames": [external_file_path],
                            "directories": [],
                        },
                        "outputs": {"directories": []},
                    }
                },
                f,
            )

        with patch_calls_for_create_job_from_job_bundle() as mock:
            # Create OS-specific CLI paths
            cli_path1 = "/path/from/cli/1" if os.name != "nt" else "C:\\path\\from\\cli\\1"
            cli_path2 = os.path.dirname(external_file_path)

            # Run the CLI command with known upload paths
            runner = CliRunner()
            result = runner.invoke(
                main,
                [
                    "bundle",
                    "submit",
                    "--yes",
                    temp_job_bundle_dir,
                    "--known-asset-path",
                    cli_path1,
                    "--known-asset-path",
                    cli_path2,
                ],
            )

            # Verify the command succeeded
            assert result.exit_code == 0, result.output

            # Verify create_job_from_job_bundle was called with combined paths
            mock.create_job_from_job_bundle.assert_called_once()
            _, kwargs = mock.create_job_from_job_bundle.call_args
            assert "known_asset_paths" in kwargs
            known_paths = kwargs["known_asset_paths"]
            assert cli_path1 in known_paths, result.output
            assert cli_path2 in known_paths, result.output
            assert config_path1 not in known_paths, result.output
            assert config_path2 not in known_paths, result.output

            # Check that both the CLI and config paths are provided to generate_message_for_asset_paths
            mock.generate_message_for_asset_paths.assert_called_once()
            (_, _, known_paths), _ = mock.generate_message_for_asset_paths.call_args
            assert cli_path1 in known_paths, result.output
            assert cli_path2 in known_paths, result.output
            assert config_path1 in known_paths, result.output
            assert config_path2 in known_paths, result.output
    finally:
        # Clean up the temporary file
        os.unlink(external_file_path)


def test_cli_bundle_storage_profile_known_paths(fresh_deadline_config, temp_job_bundle_dir):
    """
    Test that known paths from a storage profile are included when submitting a job bundle.
    """
    # Set up configuration
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("settings.auto_accept", "true")

    # Set a storage profile ID in the config
    storage_profile_id = "mock-storage-profile-id"
    config.set_setting("settings.storage_profile_id", storage_profile_id)

    # Write a JSON template
    with open(os.path.join(temp_job_bundle_dir, "template.json"), "w", encoding="utf8") as f:
        f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1])

    # Create a file outside the job bundle directory
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(b"test content")
        external_file_path = temp_file.name

    try:
        # Create asset references pointing to the external file
        with open(
            os.path.join(temp_job_bundle_dir, "asset_references.json"), "w", encoding="utf8"
        ) as f:
            json.dump(
                {
                    "assetReferences": {
                        "inputs": {
                            "filenames": [external_file_path],
                            "directories": [],
                        },
                        "outputs": {"directories": []},
                    }
                },
                f,
            )

        # Create a mock storage profile with file system locations
        local_storage_profile_path = os.path.dirname(external_file_path)
        shared_storage_profile_path = (
            "/shared/path/from/storage/profile"
            if os.name != "nt"
            else "C:\\shared\\path\\from\\storage\\profile"
        )

        # Create the StorageProfile object directly
        os_family = "WINDOWS" if os.name == "nt" else "LINUX"
        storage_profile_response = {
            "storageProfileId": storage_profile_id,
            "displayName": "Mock Storage Profile",
            "osFamily": os_family,
            "fileSystemLocations": [
                {
                    "name": "mock-local-location",
                    "path": local_storage_profile_path,
                    "type": "LOCAL",
                },
                {
                    "name": "mock-shared-location",
                    "path": shared_storage_profile_path,
                    "type": "SHARED",
                },
            ],
        }

        with patch_calls_for_create_job_from_job_bundle() as mock:
            mock.get_boto3_client().get_storage_profile_for_queue.return_value = (
                storage_profile_response
            )

            # Run the CLI command with known upload paths
            runner = CliRunner()
            result = runner.invoke(
                main,
                [
                    "bundle",
                    "submit",
                    "--yes",
                    temp_job_bundle_dir,
                ],
            )

            # Verify the command succeeded
            assert result.exit_code == 0, result.output

            # Verify get_storage_profile_for_queue was called with correct parameters
            assert mock.get_boto3_client().get_storage_profile_for_queue.mock_calls == [
                call(
                    farmId=MOCK_FARM_ID, queueId=MOCK_QUEUE_ID, storageProfileId=storage_profile_id
                )
            ], result.output

            # Verify create_job_from_job_bundle was called
            mock.create_job_from_job_bundle.assert_called_once()
            _, kwargs = mock.create_job_from_job_bundle.call_args
            assert "known_asset_paths" in kwargs
            known_paths = kwargs["known_asset_paths"]

            # Verify the storage profile path is not in the known paths passed to create_job_in_job_bundle
            assert local_storage_profile_path not in known_paths, result.output
            assert shared_storage_profile_path not in known_paths, result.output

            # Check that the LOCAL storage profile paths are provided to generate_message_for_asset_paths
            mock.generate_message_for_asset_paths.assert_called_once()
            (_, _, known_paths), _ = mock.generate_message_for_asset_paths.call_args
            assert local_storage_profile_path in known_paths, result.output
            # The SHARED storage location should not be in the known paths list
            assert shared_storage_profile_path not in known_paths, result.output
    finally:
        # Clean up the temporary file
        os.unlink(external_file_path)


def test_cli_bundle_warning_suppression(fresh_deadline_config, temp_job_bundle_dir):
    """
    Test that warnings are suppressed for paths in known_asset_paths.
    """
    # Set up configuration
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("settings.auto_accept", "false")

    # Write a JSON template
    with open(os.path.join(temp_job_bundle_dir, "template.json"), "w", encoding="utf8") as f:
        f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1])

    # Create a file in the job bundle directory
    job_bundle_file_path = os.path.join(temp_job_bundle_dir, "test_file.txt")
    with open(job_bundle_file_path, "wb") as f:
        f.write(b"test content")

    # Create a file outside the job bundle directory
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(b"test content")
        external_file_path = temp_file.name

    try:
        # Create asset references pointing to the external file
        with open(
            os.path.join(temp_job_bundle_dir, "asset_references.json"), "w", encoding="utf8"
        ) as f:
            json.dump(
                {
                    "assetReferences": {
                        "inputs": {
                            "filenames": [external_file_path, job_bundle_file_path],
                            "directories": [],
                        },
                        "outputs": {"directories": []},
                    }
                },
                f,
            )

        with patch_calls_for_create_job_from_job_bundle():
            # First test: without known_asset_path - should show warning and succeed when user confirms
            with patch.object(click, "confirm", return_value=True) as mock_confirm:
                runner = CliRunner()
                result = runner.invoke(
                    main,
                    [
                        "bundle",
                        "submit",
                        temp_job_bundle_dir,
                    ],
                )

                # Verify warning was shown
                assert result.exit_code == 0, result.output
                mock_confirm.assert_called_once_with(ANY, default=False)
                # The warning message should say there are two input files, but only one has an issue
                warning_message = mock_confirm.call_args[0][0]
                assert "Job submission contains 2 input files" in warning_message, warning_message
                assert (
                    f"Unknown locations for upload:\n  {external_file_path}" in warning_message
                ), warning_message

            # Second test: without known_asset_path - should show warning and fail when user cancels
            with patch.object(click, "confirm", return_value=False) as mock_confirm:
                runner = CliRunner()
                result = runner.invoke(
                    main,
                    [
                        "bundle",
                        "submit",
                        temp_job_bundle_dir,
                    ],
                )

                # Verify warning was shown
                assert result.exit_code == 1, result.output
                mock_confirm.assert_called_once_with(ANY, default=False)
                # The warning message should say there are two input files, but only one has an issue
                warning_message = mock_confirm.call_args[0][0]
                assert "Job submission contains 2 input files" in warning_message, warning_message
                assert (
                    f"Unknown locations for upload:\n  {external_file_path}" in warning_message
                ), warning_message

            # Third test: with known_asset_path - should not show warning
            with patch.object(click, "confirm", return_value=True) as mock_confirm:
                runner = CliRunner()
                result = runner.invoke(
                    main,
                    [
                        "bundle",
                        "submit",
                        temp_job_bundle_dir,
                        "--known-asset-path",
                        os.path.dirname(external_file_path),
                    ],
                )

                # Verify warning was not shown
                assert result.exit_code == 0, result.output
                mock_confirm.assert_called_once_with(ANY, default=True)
    finally:
        # Clean up the temporary file
        os.unlink(external_file_path)
