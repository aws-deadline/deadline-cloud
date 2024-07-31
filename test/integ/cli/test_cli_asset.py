# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Integ tests for the CLI asset commands.
"""
import os
import json
from click.testing import CliRunner
import pytest
import tempfile
import posixpath
from deadline.job_attachments.asset_manifests.hash_algorithms import hash_file, HashAlgorithm

from deadline.client.cli import main


TEST_FILE_CONTENT = "test file content"
TEST_SUB_DIR_FILE_CONTENT = "subdir file content"
TEST_ROOT_DIR_FILE_CONTENT = "root file content"

TEST_ROOT_FILE = "root_file.txt"
TEST_SUB_FILE = "subdir_file.txt"

TEST_ROOT_DIR = "root_dir"
TEST_SUB_DIR_1 = "subdir1"
TEST_SUB_DIR_2 = "subdir2"


class TestSnapshot:

    @pytest.fixture
    def temp_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir_path:
            yield tmpdir_path

    def test_root_dir_basic(self, temp_dir):
        """
        Snapshot with a valid root directory containing one file, and no other parameters
        """
        root_dir = os.path.join(temp_dir, TEST_ROOT_DIR)
        os.makedirs(root_dir)
        file_path = os.path.join(root_dir, TEST_ROOT_FILE)
        with open(file_path, "w") as f:
            f.write(TEST_FILE_CONTENT)

        runner = CliRunner()
        runner.invoke(main, ["asset", "snapshot", "--root-dir", root_dir])

        # Check manifest file details to match correct content
        # since manifest file name is hashed depending on source location, we have to list out manifest
        manifest_folder_path = os.path.join(root_dir, f"{os.path.basename(root_dir)}_manifests")
        manifest_files = os.listdir(manifest_folder_path)
        assert (
            len(manifest_files) == 1
        ), f"Expected exactly one manifest file, but got {len(manifest_files)}"

        manifest_file_name = manifest_files[0]
        manifest_file_path = os.path.join(manifest_folder_path, manifest_file_name)

        with open(manifest_file_path, "r") as f:
            manifest_data = json.load(f)

        expected_hash = hash_file(file_path, HashAlgorithm("xxh128"))  # hashed with xxh128
        manifest_data_paths = manifest_data["paths"]
        assert (
            len(manifest_data_paths) == 1
        ), f"Expected exactly one path inside manifest, but got {len(manifest_data_paths)}"
        assert manifest_data_paths[0]["path"] == TEST_ROOT_FILE
        assert manifest_data_paths[0]["hash"] == expected_hash

    def test_root_dir_not_recursive(self, temp_dir):
        """
        Snapshot with valid root directory with subdirectory and multiple files, but doesn't recursively snapshot.
        """
        root_dir = os.path.join(temp_dir, TEST_ROOT_DIR)

        # Create a file in the root directory
        root_file_path = os.path.join(root_dir, TEST_ROOT_FILE)
        os.makedirs(os.path.dirname(root_file_path), exist_ok=True)
        with open(root_file_path, "w") as f:
            f.write(TEST_ROOT_DIR_FILE_CONTENT)

        # Create a file in the subdirectory (should not be included)
        subdir_file_path = os.path.join(root_dir, TEST_SUB_DIR_1, TEST_SUB_DIR_2, TEST_SUB_FILE)
        os.makedirs(os.path.dirname(subdir_file_path), exist_ok=True)
        with open(subdir_file_path, "w") as f:
            f.write(TEST_SUB_DIR_FILE_CONTENT)

        runner = CliRunner()
        runner.invoke(main, ["asset", "snapshot", "--root-dir", root_dir])

        # Check manifest file details to match correct content
        manifest_folder_path = os.path.join(root_dir, f"{os.path.basename(root_dir)}_manifests")
        manifest_files = os.listdir(manifest_folder_path)
        assert (
            len(manifest_files) == 1
        ), f"Expected exactly one manifest file, but got {len(manifest_files)}"

        manifest_file_name = manifest_files[0]
        manifest_file_path = os.path.join(manifest_folder_path, manifest_file_name)

        with open(manifest_file_path, "r") as f:
            manifest_data = json.load(f)

        # should ignore subdirectories
        expected_hash = hash_file(root_file_path, HashAlgorithm("xxh128"))  # hashed with xxh128
        manifest_data_paths = manifest_data["paths"]
        assert (
            len(manifest_data_paths) == 1
        ), f"Expected exactly one path inside manifest, but got {len(manifest_data_paths)}"
        assert manifest_data_paths[0]["path"] == TEST_ROOT_FILE
        assert manifest_data_paths[0]["hash"] == expected_hash

    def test_root_dir_recursive(self, temp_dir):
        """
        Snapshot with valid root directory with subdirectory and multiple files, and recursively snapshots files.
        """
        root_dir = os.path.join(temp_dir, TEST_ROOT_DIR)

        # Create a file in the root directory
        root_file_path = os.path.join(root_dir, TEST_ROOT_FILE)
        os.makedirs(os.path.dirname(root_file_path), exist_ok=True)
        with open(root_file_path, "w") as f:
            f.write(TEST_ROOT_DIR_FILE_CONTENT)

        # Create a file in the subdirectory
        subdir_file_path = os.path.join(root_dir, TEST_SUB_DIR_1, TEST_SUB_DIR_2, TEST_SUB_FILE)
        os.makedirs(os.path.dirname(subdir_file_path), exist_ok=True)
        with open(subdir_file_path, "w") as f:
            f.write(TEST_SUB_DIR_FILE_CONTENT)

        runner = CliRunner()
        runner.invoke(main, ["asset", "snapshot", "--root-dir", root_dir, "--recursive"])

        # Check manifest file details to match correct content
        # since manifest file name is hashed depending on source location, we have to list out manifest
        manifest_folder_path = os.path.join(root_dir, f"{os.path.basename(root_dir)}_manifests")
        manifest_files = os.listdir(manifest_folder_path)
        assert (
            len(manifest_files) == 1
        ), f"Expected exactly one manifest file, but got {len(manifest_files)}"

        root_manifest_file_name = [file for file in manifest_files][0]
        root_manifest_file_path = os.path.join(manifest_folder_path, root_manifest_file_name)

        with open(root_manifest_file_path, "r") as f:
            manifest_data = json.load(f)

        root_file_hash = hash_file(root_file_path, HashAlgorithm("xxh128"))  # hashed with xxh128
        subdir_file_hash = hash_file(
            subdir_file_path, HashAlgorithm("xxh128")
        )  # hashed with xxh128
        manifest_data_paths = manifest_data["paths"]
        assert (
            len(manifest_data_paths) == 2
        ), f"Expected exactly 2 paths inside manifest, but got {len(manifest_data_paths)}"
        assert manifest_data_paths[0]["path"] == TEST_ROOT_FILE
        assert manifest_data_paths[0]["hash"] == root_file_hash
        assert manifest_data_paths[1]["path"] == posixpath.join(
            TEST_SUB_DIR_1, TEST_SUB_DIR_2, TEST_SUB_FILE
        )
        assert manifest_data_paths[1]["hash"] == subdir_file_hash

    def test_specified_manifest_out(self, temp_dir):
        """
        Snapshot with valid root directory, checks if manifest is created in the specified --manifest-out location
        """
        root_dir = os.path.join(temp_dir, TEST_ROOT_DIR)
        os.makedirs(root_dir)
        manifest_out_dir = os.path.join(temp_dir, "manifest_out")
        os.makedirs(manifest_out_dir)
        file_path = os.path.join(root_dir, TEST_ROOT_FILE)
        with open(file_path, "w") as f:
            f.write(TEST_FILE_CONTENT)

        runner = CliRunner()
        runner.invoke(
            main, ["asset", "snapshot", "--root-dir", root_dir, "--manifest-out", manifest_out_dir]
        )

        # Check manifest file details to match correct content
        # since manifest file name is hashed depending on source location, we have to list out manifest
        manifest_folder_path = os.path.join(
            manifest_out_dir, f"{os.path.basename(root_dir)}_manifests"
        )
        manifest_files = os.listdir(manifest_folder_path)
        assert (
            len(manifest_files) == 1
        ), f"Expected exactly one manifest file, but got {len(manifest_files)}"

        manifest_file_name = manifest_files[0]

        manifest_file_path = os.path.join(manifest_folder_path, manifest_file_name)

        with open(manifest_file_path, "r") as f:
            manifest_data = json.load(f)

        expected_hash = hash_file(file_path, HashAlgorithm("xxh128"))  # hashed with xxh128
        manifest_data_paths = manifest_data["paths"]
        assert (
            len(manifest_data_paths) == 1
        ), f"Expected exactly one path inside manifest, but got {len(manifest_data_paths)}"
        assert manifest_data_paths[0]["path"] == TEST_ROOT_FILE
        assert manifest_data_paths[0]["hash"] == expected_hash
