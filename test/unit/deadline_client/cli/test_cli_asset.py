# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the CLI asset commands.
"""
import os
import json
from click.testing import CliRunner
import pytest
import tempfile
import posixpath

from deadline.client.cli import main


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdirname:
        yield tmpdirname


def test_cli_asset_snapshot_valid_root_dir(temp_dir):
    root_dir = os.path.join(temp_dir, "root_dir")
    os.makedirs(root_dir)
    file_path = os.path.join(root_dir, "file.txt")
    with open(file_path, "w") as f:
        f.write("test file content")

    runner = CliRunner()
    runner.invoke(main, ["asset", "snapshot", "--root-dir", root_dir])

    # Check manifest file details to match correct content
    # since manifest file name is hashed depending on source location, we have to list out manifest
    manifest_folder_path = os.path.join(root_dir, f"{os.path.basename(root_dir)}_manifests")
    manifest_file_name = os.listdir(manifest_folder_path)[0]
    manifest_file_path = os.path.join(manifest_folder_path, manifest_file_name)

    with open(manifest_file_path, "r") as f:
        manifest_data = json.load(f)
        print(manifest_data["paths"][0]["hash"])
        assert manifest_data["paths"][0]["path"] == "file.txt"
        assert manifest_data["paths"][0]["hash"] == "0741993e50c8bc250cefba3959c81eb8"


def test_cli_asset_snapshot_invalid_root_dir(temp_dir):
    invalid_root_dir = os.path.join(temp_dir, "invalid_root_dir")
    runner = CliRunner()
    result = runner.invoke(main, ["asset", "snapshot", "--root-dir", invalid_root_dir])
    assert result.exit_code == 1


def test_cli_asset_snapshot_recursive(temp_dir):
    root_dir = os.path.join(temp_dir, "root_dir")
    os.makedirs(os.path.join(root_dir, "subdir1", "subdir2"))

    # Create a file in the root directory
    root_file_path = os.path.join(root_dir, "root_file.txt")
    with open(root_file_path, "w") as f:
        f.write("root file content")

    # Create a file in the subdirectory
    subdir_file_path = os.path.join(root_dir, "subdir1", "subdir2", "subdir_file.txt")
    os.makedirs(os.path.dirname(subdir_file_path), exist_ok=True)
    with open(subdir_file_path, "w") as f:
        f.write("subdir file content")

    runner = CliRunner()
    runner.invoke(main, ["asset", "snapshot", "--root-dir", root_dir, "--recursive"])

    # Check manifest file details to match correct content
    # since manifest file name is hashed depending on source location, we have to list out manifest
    manifest_folder_path = os.path.join(root_dir, f"{os.path.basename(root_dir)}_manifests")
    root_manifest_file_name = [file for file in os.listdir(manifest_folder_path)][0]
    root_manifest_file_path = os.path.join(manifest_folder_path, root_manifest_file_name)
    with open(root_manifest_file_path, "r") as f:
        manifest_data = json.load(f)
        assert manifest_data["paths"][0]["path"] == "root_file.txt"
        assert manifest_data["paths"][0]["hash"] == "a5fc4a07191e2c90364319d2fd503cc1"
        assert manifest_data["paths"][1]["path"] == posixpath.join(
            "subdir1", "subdir2", "subdir_file.txt"
        )
        assert manifest_data["paths"][1]["hash"] == "a3ede7fa4c2694d59ff09ed553fcc806"


def test_cli_asset_snapshot_valid_manifest_out_dir(temp_dir):
    root_dir = os.path.join(temp_dir, "root_dir")
    os.makedirs(root_dir)
    manifest_out_dir = os.path.join(temp_dir, "manifest_out")
    os.makedirs(manifest_out_dir)
    file_path = os.path.join(root_dir, "file.txt")
    with open(file_path, "w") as f:
        f.write("test file content")

    runner = CliRunner()
    runner.invoke(
        main, ["asset", "snapshot", "--root-dir", root_dir, "--manifest-out", manifest_out_dir]
    )

    # Check manifest file details to match correct content
    # since manifest file name is hashed depending on source location, we have to list out manifest
    manifest_folder_path = os.path.join(manifest_out_dir, f"{os.path.basename(root_dir)}_manifests")
    manifest_file_name = os.listdir(manifest_folder_path)[0]
    manifest_file_path = os.path.join(manifest_folder_path, manifest_file_name)

    with open(manifest_file_path, "r") as f:
        manifest_data = json.load(f)
        print(manifest_data["paths"][0]["hash"])
        assert manifest_data["paths"][0]["path"] == "file.txt"
        assert manifest_data["paths"][0]["hash"] == "0741993e50c8bc250cefba3959c81eb8"
