# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for 'deadline bundle submit --save-debug-snapshot'.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch
import os
import shutil
import difflib

from click.testing import CliRunner

from deadline.client import config
from deadline.client.cli import main
import deadline.job_attachments.models
from deadline.job_attachments._utils import _get_long_path_compatible_path


from ..api.test_job_bundle_submission import (
    MOCK_FARM_ID,
    MOCK_QUEUE_ID,
)


def normalize_job_bundle_timestamps(job_bundle_dir):
    """Sets the timestamps of all the files in a job bundle directory to one fixed value."""
    for root, _, files in os.walk(job_bundle_dir):
        for filename in files:
            full_filename = os.path.join(root, filename)
            os.utime(full_filename, (1756710000, 1756710000))


def normalize_snapshot(snapshot_dir):
    """Normalizes the platform-specific directories into named tokens, and '\\' on Windows to '/'"""
    # Normalize to LF line endings, and then on Windows back to CRLF for consistent comparison with git checkout of snapshot
    replacements: list[tuple[str, str]] = []
    if os.name != "nt":
        replacements.append(("\r\n", "\n"))

    with open(
        _get_long_path_compatible_path(snapshot_dir / "create_job_args.json"), encoding="utf-8"
    ) as fh:
        create_job_args = json.load(fh)

    # Process all the manifests to figure out renamings that normalize the snapshot
    for index, manifest in enumerate(create_job_args["attachments"]["manifests"]):
        root_path = manifest["rootPath"]
        replacements.append((root_path + os.sep, f"ROOT_PATH_{index}/"))
        if os.name == "nt":
            # Also match double-\\ patterns for JSON-encoded values on Windows
            replacements.append(
                (root_path.replace(os.sep, 2 * os.sep) + 2 * os.sep, f"ROOT_PATH_{index}/")
            )
        replacements.append((root_path, f"ROOT_PATH_{index}"))
        if os.name == "nt":
            # Also match double-\\ patterns for JSON-encoded values on Windows
            replacements.append((root_path.replace(os.sep, 2 * os.sep), f"ROOT_PATH_{index}"))

        manifest_path = manifest["inputManifestPath"]
        manifest_name = manifest_path.rsplit("/", 1)[-1]
        replacements.append((manifest_name, f"MANIFEST_NAME_{index}"))

        replacements.append(
            (
                f'"rootPathFormat": "{manifest["rootPathFormat"]}"',
                f'"rootPathFormat": "ROOT_PATH_FORMAT_{index}"',
            )
        )

        # Rename the manifest file so the snapshot remains self-consistent
        manifest_old_name = _get_long_path_compatible_path(
            snapshot_dir / "Manifests" / manifest_path
        )
        manifest_new_name = _get_long_path_compatible_path(
            (snapshot_dir / "Manifests" / manifest_path).parent / f"MANIFEST_NAME_{index}"
        )
        os.rename(manifest_old_name, manifest_new_name)

    # Process every file in the snapshot directory to apply the replacements
    for filename in os.listdir(snapshot_dir):
        full_filename = snapshot_dir / filename
        if not os.path.isfile(_get_long_path_compatible_path(full_filename)):
            continue
        with open(_get_long_path_compatible_path(full_filename), "rb") as f:
            contents = f.read()
        for src, dst in replacements:
            contents = contents.replace(src.encode("utf-8"), dst.encode("utf-8"))
        with open(_get_long_path_compatible_path(full_filename), "wb") as f:
            f.write(contents)


def assert_directories_equal(snapshot_dir, expected_dir):
    # Walk through the two directories together and compare
    snap_extra: list[str] = []
    exp_extra: list[str] = []
    files_differ: list[tuple[str, str]] = []
    for (snap_root, snap_dirs, snap_files), (exp_root, exp_dirs, exp_files) in zip(
        os.walk(snapshot_dir), os.walk(expected_dir)
    ):
        # Find directory mismatches
        snap_extra.extend(os.path.join(snap_root, dir) for dir in (set(snap_dirs) - set(exp_dirs)))
        exp_extra.extend(os.path.join(exp_root, dir) for dir in (set(exp_dirs) - set(snap_dirs)))
        # Update os.walk to intersection of dirs
        snap_dirs[:] = exp_dirs[:] = set(snap_dirs).intersection(exp_dirs)
        # Find file mismatches
        snap_extra.extend(
            os.path.join(snap_root, file) for file in (set(snap_files) - set(exp_files))
        )
        exp_extra.extend(
            os.path.join(exp_root, file) for file in (set(exp_files) - set(snap_files))
        )
        # Compare the files
        for file in set(snap_files).intersection(exp_files):
            with open(_get_long_path_compatible_path(os.path.join(snap_root, file)), "rb") as f:
                contents1 = f.read()
            with open(_get_long_path_compatible_path(os.path.join(exp_root, file)), "rb") as f:
                contents2 = f.read()
            if contents1 != contents2:
                files_differ.append((os.path.join(snap_root, file), os.path.join(exp_root, file)))

    if files_differ:
        for snap_file, exp_file in files_differ:
            with open(snap_file, "r", encoding="utf-8") as snap_fh, open(
                exp_file, "r", encoding="utf-8"
            ) as exp_fh:
                for line in difflib.unified_diff(
                    snap_fh.readlines(), exp_fh.readlines(), fromfile=snap_file, tofile=exp_file
                ):
                    print(line, end="")
            print()

    assert {"snap_extra": snap_extra, "exp_extra": exp_extra, "files_differ": files_differ} == {
        "snap_extra": [],
        "exp_extra": [],
        "files_differ": [],
    }


def test_cli_bundle_submit_debug_snapshot(fresh_deadline_config, deadline_mock, tmp_path):
    """
    Confirm that CLI bundle submit makes the right create_job call from a simple JSON template.
    """
    # Make sure the temporary path has no symlinks
    tmp_path = tmp_path.resolve()

    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

    job_bundle_dir = Path(__file__).parent / "test_data" / "job_bundle_with_data"
    job_bundle_dir = job_bundle_dir.resolve()
    expected_snapshot_dir = Path(__file__).parent / "test_data" / "job_bundle_with_data_snapshot"
    expected_snapshot_dir = expected_snapshot_dir.resolve()

    normalize_job_bundle_timestamps(job_bundle_dir)

    # You can temporarily set this to True to regenerate the snapshot
    regenerate_snapshot = False
    if regenerate_snapshot:
        tmp_path = expected_snapshot_dir
        shutil.rmtree(tmp_path)

    with patch.object(
        deadline.job_attachments.models,
        "_generate_random_guid",
        return_value="00000000000000000000000000000000",
    ):
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "bundle",
                "submit",
                str(job_bundle_dir),
                "--yes",
                "--save-debug-snapshot",
                str(tmp_path),
            ],
        )

    deadline_mock.create_job.assert_not_called()
    assert "Snapshotting submission to Queue: Mock Queue" in result.output, result.output
    assert "Submitting to Queue: Mock Queue" not in result.output, result.output
    assert result.exit_code == 0, result.output

    normalize_snapshot(tmp_path)
    assert_directories_equal(tmp_path, expected_snapshot_dir)
