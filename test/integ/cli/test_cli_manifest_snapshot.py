# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Integ tests for the CLI asset commands.
"""

import json
import math
import os
from pathlib import Path, WindowsPath
import sys
from typing import List
from click.testing import CliRunner
from deadline.job_attachments._utils import WINDOWS_MAX_PATH_LENGTH
import pytest
import tempfile
from deadline.client.cli import main


class TestManifestSnapshot:
    @pytest.fixture
    def temp_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir_path:
            yield tmpdir_path

    def _create_test_manifest(self, tmp_path: str, root_dir: str) -> str:
        """
        Create some test files in the temp dir, snapshot it and return the manifest file.
        """
        TEST_MANIFEST_DIR = "manifest_dir"

        # Given
        manifest_dir = os.path.join(tmp_path, TEST_MANIFEST_DIR)
        os.makedirs(manifest_dir)

        subdir1 = os.path.join(root_dir, "subdir1")
        subdir2 = os.path.join(root_dir, "subdir2")
        os.makedirs(subdir1)
        os.makedirs(subdir2)
        Path(os.path.join(subdir1, "file1.txt")).touch()
        Path(os.path.join(subdir2, "file2.txt")).touch()

        # When snapshot is called.
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "manifest",
                "snapshot",
                "--root",
                root_dir,
                "--destination",
                manifest_dir,
                "--name",
                "test",
            ],
        )
        assert result.exit_code == 0, result.output

        manifest_files = os.listdir(manifest_dir)
        assert len(manifest_files) == 1, (
            f"Expected exactly one manifest file, but got {len(manifest_files)}"
        )
        manifest = manifest_files[0]
        assert "test" in manifest, f"Expected test in manifest file name, got {manifest}"

        # Return the manifest that we found.
        return os.path.join(manifest_dir, manifest)

    @pytest.mark.parametrize(
        "json_output",
        [
            pytest.param(True),
            pytest.param(False),
        ],
    )
    def test_manifest_diff(self, tmp_path: str, json_output: bool):
        """
        Tests if manifest diff CLI works, basic case. Variation on JSON as printout.
        Business logic testing will be done at the API level where we can check the outputs.
        """
        TEST_ROOT_DIR = "root_dir"

        # Given a created manifest file...
        root_dir = os.path.join(tmp_path, TEST_ROOT_DIR)
        manifest = self._create_test_manifest(tmp_path, root_dir)

        # Lets add another file.
        new_file = "file3.txt"
        Path(os.path.join(root_dir, new_file)).touch()

        # When
        runner = CliRunner()
        args = ["manifest", "diff", "--root", root_dir, "--manifest", manifest]
        if json_output:
            args.append("--json")
        result = runner.invoke(main, args)

        # Then
        assert result.exit_code == 0, result.output
        if json_output:
            # If JSON mode was specified, make sure the output is JSON and contains the new file.
            diff = json.loads(result.output)
            assert len(diff["new"]) == 1
            assert new_file in diff["new"]

    @pytest.mark.integ
    @pytest.mark.skipif(
        sys.platform != "win32",
        reason="This test is related to Windows file path length limit, skipping this if os not Windows",
    )
    def test_manifest_snapshot_over_windows_path_limit(self, tmp_path: WindowsPath):
        """
        Tests that if the snapshot root directory is almost as long as the Windows path length limit,
        the snapshot will still work despite reaching over the Windows path length limit.
        See https://learn.microsoft.com/en-us/windows/win32/fileio/maximum-file-path-limitation
        """
        # Given

        tmp_path_len: int = len(str(tmp_path))

        # Make the file root directory
        root_directory: str = os.path.join(tmp_path, "root")
        os.makedirs(root_directory)
        Path(os.path.join(root_directory, "test.txt")).touch()

        # Create a manifest directory that is almost as long as the Windows path length limit.
        manifest_directory_remaining_length: int = WINDOWS_MAX_PATH_LENGTH - tmp_path_len - 30
        manifest_directory: str = os.path.join(
            tmp_path,
            *["path"]
            * math.floor(
                manifest_directory_remaining_length / 5
            ),  # Create a temp path that barely does not exceed the windows path limit
        )

        os.makedirs(manifest_directory)

        assert len(manifest_directory) <= WINDOWS_MAX_PATH_LENGTH - 30

        runner = CliRunner()

        result = runner.invoke(
            main,
            [
                "manifest",
                "snapshot",
                "--root",
                root_directory,
                "--destination",
                manifest_directory,
                "--name",
                "testLongPath",
            ],
        )

        assert result.exit_code == 0, result.output
        assert "warning" in result.stdout.lower()
        files: List[str] = os.listdir(manifest_directory)

        assert len(files) == 1, f"Expected exactly one manifest file, but got {len(files)}"

        manifest: str = files[0]
        assert "testLongPath" in manifest, (
            f"Expected testLongPath in manifest file name, got {manifest}"
        )

        assert len(os.path.join(manifest_directory, manifest)) > 260, (
            f"Expected full manifest file path to be over the windows path length limit of {WINDOWS_MAX_PATH_LENGTH}, got {len(os.path.join(manifest_directory, manifest))}"
        )

    @pytest.mark.integ
    @pytest.mark.skipif(
        sys.platform != "win32",
        reason="This test is related to Windows file path length limit, skipping this if os not Windows",
    )
    def test_manifest_snapshot_over_windows_path_limit_json(self, tmp_path: WindowsPath):
        """
        Tests that if the snapshot root directory is almost as long as the Windows path length limit,
        the snapshot will still work despite reaching over the Windows path length limit.
        This test uses json output in the cli options and verifies that the output json is expected.
        See https://learn.microsoft.com/en-us/windows/win32/fileio/maximum-file-path-limitation
        """
        # Given

        tmp_path_len: int = len(str(tmp_path))

        # Make the file root directory
        root_directory: str = os.path.join(tmp_path, "root")
        os.makedirs(root_directory)
        Path(os.path.join(root_directory, "test.txt")).touch()

        # Create a manifest directory that is almost as long as the Windows path length limit.
        manifest_directory_remaining_length: int = WINDOWS_MAX_PATH_LENGTH - tmp_path_len - 30
        manifest_directory: str = os.path.join(
            tmp_path,
            *["path"]
            * math.floor(
                manifest_directory_remaining_length / 5
            ),  # Create a temp path that barely does not exceed the windows path limit
        )

        os.makedirs(manifest_directory)

        assert len(manifest_directory) <= WINDOWS_MAX_PATH_LENGTH - 30

        runner = CliRunner()

        result = runner.invoke(
            main,
            [
                "manifest",
                "snapshot",
                "--root",
                root_directory,
                "--destination",
                manifest_directory,
                "--name",
                "testLongPath",
                "--json",
            ],
        )

        assert result.exit_code == 0, result.output
        assert json.loads(result.stdout).get("warning") is not None

        files: List[str] = os.listdir(manifest_directory)

        assert len(files) == 1, f"Expected exactly one manifest file, but got {len(files)}"

        manifest: str = files[0]
        assert "testLongPath" in manifest, (
            f"Expected testLongPath in manifest file name, got {manifest}"
        )

        assert len(os.path.join(manifest_directory, manifest)) > 260, (
            f"Expected full manifest file path to be over the windows path length limit of {WINDOWS_MAX_PATH_LENGTH}, got {len(os.path.join(manifest_directory, manifest))}"
        )
