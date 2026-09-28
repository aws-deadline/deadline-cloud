# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the known asset paths functionality in the bundle_submit CLI command.
"""

import ntpath
import os
import json
import tempfile
from pathlib import Path
from unittest.mock import patch, ANY, call

import click
from click.testing import CliRunner
import pytest

from .._legacy_ntpath import PreThreeElevenNtpath
from deadline.client import config
from deadline.client.cli import main
from deadline.client.api import _submit_job_bundle as sjb
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
    if os.name != "nt":
        assert sorted(_filter_redundant_known_paths(input)) == expected
        return

    # On Windows these POSIX-style paths are root-relative rather than absolute: '\a'
    # resolves against whichever drive the process is on, so it is dropped as unanchored --
    # see test_filter_redundant_known_paths_drops_unanchored_paths. Only the drive-qualified
    # spelling is a usable root here, so that is the one carrying the redundancy cases.
    assert _filter_redundant_known_paths(input) == []
    assert _filter_redundant_known_paths(path.replace("/", "\\") for path in input) == []
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


@pytest.mark.parametrize(
    "path, roots, expected",
    [
        # Regression for https://github.com/aws-deadline/deadline-cloud/issues/1321:
        # a host-level UNC root must contain paths under any of its shares.
        (
            r"\\192.168.20.20\projects\assets\FA_Anim\260304_FA_Anim.c4d",
            [r"\\192.168.20.20"],
            True,
        ),
        (r"\\host\share\file", [r"\\host"], True),
        (r"\\host\share\file", ["\\\\host\\"], True),
        (r"\\host\share\file", [r"\\host\share"], True),
        # Neither a different nor a prefix-sharing host is contained.
        (r"\\other\share\file", [r"\\host"], False),
        (r"\\host2\share\file", [r"\\host"], False),
        (r"\\host\share2\file", [r"\\host\share"], False),
        # A UNC candidate is not contained by a drive-letter root, and vice versa.
        (r"\\host\share\file", [r"C:\trusted"], False),
        (r"C:\trusted\file", [r"\\host\share"], False),
        # Contained by the second of several roots, including a mismatched-drive first root.
        (r"\\host\share\file", [r"D:\other", r"\\host"], True),
        # A bare UNC anchor names no server, so it must not trust every reachable share. It
        # passes the isabs filter, so '--known-asset-path \\' reaches here as a root.
        (r"\\corp\finance\salaries.xlsx", ["\\\\"], False),
        (r"\\corp\finance\salaries.xlsx", ["//"], False),
        (r"\\corp\finance\salaries.xlsx", ["\\\\?\\UNC\\"], False),
        # A useless root must not shadow a real one that follows it. _is_known_path is only
        # half the story here -- the submit flow runs _filter_redundant_known_paths first,
        # where the bare anchor used to prefix and so delete every real UNC root. See
        # test_filter_redundant_known_paths_drops_the_bare_unc_anchor and
        # test_generate_message_for_asset_paths_bare_anchor_does_not_shadow_a_real_root.
        (r"\\host\share\file", ["\\\\", r"\\host"], True),
    ],
)
def test_is_known_path_windows_semantics(path, roots, expected):
    """Windows path semantics, exercised via ntpath so the cases run on every platform."""
    with patch.object(sjb.os, "path", ntpath):
        assert _is_known_path(path, roots) is expected


@pytest.mark.parametrize(
    "input, expected",
    [
        # A host-level root makes its shares redundant.
        ([r"\\host", r"\\host\share"], [r"\\host"]),
        ([r"\\host\share", r"\\host"], [r"\\host"]),
        ([r"\\host\share\a", r"\\host"], [r"\\host"]),
        # Distinct hosts and shares are all kept.
        ([r"\\host\s1", r"\\host\s2"], [r"\\host\s1", r"\\host\s2"]),
        ([r"\\host1", r"\\host2"], [r"\\host1", r"\\host2"]),
        # A host sharing a string prefix is not made redundant.
        ([r"\\host", r"\\host2\share"], [r"\\host", r"\\host2\share"]),
        # Case variants of the same location are redundant on Windows.
        ([r"\\host\Share", r"\\HOST\share\sub"], [r"\\host\Share"]),
        ([r"C:\proj", r"c:\PROJ\sub"], [r"C:\proj"]),
        # Drive-letter roots stay separate from UNC roots.
        ([r"C:\proj", r"\\host\share"], [r"C:\proj", r"\\host\share"]),
        # Ties keep input order, so of two spellings of one location the caller's first --
        # highest precedence -- is the one retained. Every case above differs in depth, so
        # this is what pins the documented tie-break.
        ([r"C:\Proj", r"c:\proj"], [r"C:\Proj"]),
        ([r"c:\proj", r"C:\Proj"], [r"c:\proj"]),
        # The retained entry is the *normalized* spelling of the first input, so a
        # trailing separator on it does not survive.
        (["\\\\host\\", r"\\host"], [r"\\host"]),
    ],
)
def test_filter_redundant_known_paths_windows_semantics(input, expected):
    with patch.object(sjb.os, "path", ntpath):
        assert _filter_redundant_known_paths(input) == expected


def test_filter_redundant_known_paths_survives_pre_3_11_normpath():
    """A host-level UNC root must still subsume its shares on the interpreters where
    ``normpath`` collapses the leading pair.

    ``os.path.normpath(r"\\host")`` returned ``\host`` before 3.11, moving the root out
    of the UNC space so it matched none of its own shares. The filter normalizes with the
    UNC-aware helper instead; injected here so the 3.9 and 3.10 behavior is asserted on
    every interpreter rather than only on those matrix legs.
    """
    legacy = PreThreeElevenNtpath()
    assert legacy.normpath(r"\\host") == r"\host", "proxy no longer reproduces the old behavior"
    with patch.object(sjb.os, "path", legacy):
        assert _filter_redundant_known_paths([r"\\host", r"\\host\share"]) == [r"\\host"]
        assert _filter_redundant_known_paths([r"\\host\share", r"\\host"]) == [r"\\host"]


def test_filter_redundant_known_paths_expands_user_paths():
    """
    A '~'-prefixed root has to be expanded to match an absolute candidate. Such a root
    reaches here from the config file and the CLI job submitter's default data
    directory, neither of which goes through shell expansion.
    """
    home_root = os.path.join("~", "projects")
    expected_home = os.path.join(os.path.expanduser("~"), "projects")

    assert _filter_redundant_known_paths([home_root]) == [expected_home]
    assert _is_known_path(os.path.join(expected_home, "scene.ma"), [expected_home]) is True

    # Expanding must not defeat redundancy filtering: '~/projects' and its subdirectory
    # name the same tree, so only the ancestor survives.
    assert _filter_redundant_known_paths([home_root, os.path.join(home_root, "sub")]) == [
        expected_home
    ]


@pytest.mark.parametrize(
    "known_path",
    [
        # An empty known path reaches this code from `--known-asset-path ""`, from the
        # MCP tool's unvalidated JSON array, and from a PATH/FILE job parameter whose
        # allowedValues suppressed absolutization (os.path.dirname("scene.ma") == "").
        "",
        # Relative roots, including the Windows root-relative and drive-relative forms.
        "assets",
        os.path.join("..", "shared"),
        "\\projects",
        "C:rel",
    ],
)
def test_filter_redundant_known_paths_drops_unanchored_paths(known_path):
    """
    A root that names no absolute location must be dropped, not resolved against the cwd.

    It matches no candidate either way, but dropping it at the boundary means a future
    caller cannot turn it into a trusted tree: os.path.abspath("") is the whole working
    directory, which would suppress the unknown-asset-path warning and let a
    non-interactive submit upload undesignated files.
    """
    assert _filter_redundant_known_paths([known_path]) == []

    # A real root alongside an unanchored one still survives.
    real_root = os.path.abspath(os.path.join(os.sep, "trusted", "project"))
    assert _filter_redundant_known_paths([known_path, real_root]) == [real_root]


def test_filter_redundant_known_paths_unanchored_path_does_not_trust_cwd():
    """The working directory must not become a known root via an empty path."""
    cwd_file = os.path.join(os.getcwd(), "unrelated_secret.txt")
    assert _is_known_path(cwd_file, _filter_redundant_known_paths([""])) is False


@pytest.mark.parametrize(
    "input, expected",
    [
        # The bare anchor is a single component, so it sorts first and would prefix -- and
        # therefore delete -- every real UNC root in the trie, leaving only a root that
        # matches nothing. It is dropped instead.
        (["\\\\", r"\\host"], [r"\\host"]),
        ([r"\\host", "\\\\"], [r"\\host"]),
        (["\\\\", r"\\server\share", r"\\other\share"], [r"\\server\share", r"\\other\share"]),
        # It arrives from more spellings than it looks like: '//' and '\\?\UNC\' both
        # normalize to it, the latter via _fold_extended_length_prefix.
        (["//", r"\\host"], [r"\\host"]),
        ([r"\\?\UNC\\", r"\\host"], [r"\\host"]),
        # On its own it leaves no roots at all, which is correct: it contains nothing, so
        # every path is unknown and the warning is the right outcome.
        (["\\\\"], []),
    ],
)
def test_filter_redundant_known_paths_drops_the_bare_unc_anchor(input, expected):
    """The bare anchor is anchored but names no location, unlike every other absolute root."""
    with patch.object(sjb.os, "path", ntpath):
        assert _filter_redundant_known_paths(input) == expected


def test_generate_message_for_asset_paths_bare_anchor_does_not_shadow_a_real_root():
    """End-to-end: the filter runs before containment, so the two must agree about '\\\\'.

    The halves are covered separately above; this pins them together, because the bug this
    guards against was invisible to either one alone -- _is_known_path handles the bare
    anchor correctly, and the filter deleted the real root before it ever got there.
    """
    upload_group = AssetUploadGroup(
        asset_groups=[
            AssetRootGroup(
                root_path=r"\\host\projects",
                inputs={r"\\host\projects\scene.ma"},  # type: ignore[arg-type]
            )
        ],
        total_input_files=1,
        total_input_bytes=12,
    )

    with patch("deadline.client.api._submit_job_bundle.os.path", ntpath):
        known_asset_paths = _filter_redundant_known_paths(["\\\\", r"\\host"])
        message, no_warnings = _generate_message_for_asset_paths(
            upload_group, storage_profile=None, known_asset_paths=known_asset_paths
        )

    assert known_asset_paths == [r"\\host"], known_asset_paths
    assert no_warnings is True, message
    assert "WARNING: Files were specified outside of known asset paths." not in message, message


def test_generate_message_for_asset_paths_unc_host_root_is_known():
    """
    Regression for issue #1321: files on a share under a host-level UNC known root
    must not trigger the unknown-path warning.
    """
    known_root = r"\\192.168.20.20"
    inside_file = r"\\192.168.20.20\projects\assets\FA_Anim\260304_FA_Anim.c4d"

    upload_group = AssetUploadGroup(
        asset_groups=[AssetRootGroup(root_path=r"\\192.168.20.20\projects", inputs={inside_file})],  # type: ignore[arg-type]
        total_input_files=1,
        total_input_bytes=12,
    )

    with patch("deadline.client.api._submit_job_bundle.os.path", ntpath):
        message, no_warnings = _generate_message_for_asset_paths(
            upload_group, storage_profile=None, known_asset_paths=[known_root]
        )

    assert no_warnings is True, message
    assert "WARNING: Files were specified outside of known asset paths." not in message, message


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
