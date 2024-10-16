# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Integ tests for the CLI asset commands.
"""
import os
import json
from click.testing import CliRunner
from deadline.client.cli._groups.manifest_group import cli_manifest
import pytest
import tempfile
from deadline.job_attachments.asset_manifests.hash_algorithms import hash_file, HashAlgorithm

from deadline.client.cli import main


TEST_FILE_CONTENT = "test file content"
TEST_SUB_DIR_FILE_CONTENT = "subdir file content"
TEST_ROOT_DIR_FILE_CONTENT = "root file content"

TEST_ROOT_FILE = "root_file.txt"
TEST_SUB_FILE = "subdir_file.txt"

TEST_ROOT_DIR = "root_dir"
TEST_MANIFEST_DIR = "manifest_dir"
TEST_SUB_DIR_1 = "subdir1"
TEST_SUB_DIR_2 = "subdir2"


@pytest.mark.integ
class TestManifestSnapshot:

    @pytest.fixture
    def temp_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir_path:
            yield tmpdir_path

    def test_snapshot_basic(self, temp_dir):
        """
        Snapshot with a valid root directory containing one file, and no other parameters. Basic test the CLI calls into the API.
        Deeper testing is done at the API layer.
        """
        # Given
        root_dir = os.path.join(temp_dir, TEST_ROOT_DIR)
        os.makedirs(root_dir)
        manifest_dir = os.path.join(temp_dir, TEST_MANIFEST_DIR)
        os.makedirs(manifest_dir)
        file_path = os.path.join(root_dir, TEST_ROOT_FILE)
        with open(file_path, "w") as f:
            f.write(TEST_FILE_CONTENT)

        # When
        runner = CliRunner()
        # Temporary, always add cli_manifest until launched.
        main.add_command(cli_manifest)
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
                "--json",
            ],
        )
        assert result.exit_code == 0, f"Non-Zeo exit code, CLI output {result.output}"

        # Then
        # Check manifest file details to match correct content
        manifest_files = os.listdir(manifest_dir)
        assert (
            len(manifest_files) == 1
        ), f"Expected exactly one manifest file, but got {len(manifest_files)}"
        manifest_file_name = manifest_files[0]
        assert (
            "test" in manifest_file_name
        ), f"Expected test in manifest file name, got {manifest_file_name}"

        manifest_file_path = os.path.join(manifest_dir, manifest_file_name)

        with open(manifest_file_path, "r") as f:
            manifest_data = json.load(f)

        expected_hash = hash_file(file_path, HashAlgorithm("xxh128"))  # hashed with xxh128
        manifest_data_paths = manifest_data["paths"]
        assert (
            len(manifest_data_paths) == 1
        ), f"Expected exactly one path inside manifest, but got {len(manifest_data_paths)}"
        assert manifest_data_paths[0]["path"] == TEST_ROOT_FILE
        assert manifest_data_paths[0]["hash"] == expected_hash
