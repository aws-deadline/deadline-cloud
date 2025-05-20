# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import json
import os
from pathlib import Path
import tempfile
from typing import List, Optional, Set
from deadline.job_attachments.api.manifest import _manifest_snapshot
from deadline.job_attachments.exceptions import ManifestCreationException
from deadline.job_attachments.models import ManifestSnapshot
from deadline.job_attachments._utils import _retry
import pytest


class TestSnapshotAPI:
    @pytest.fixture
    def temp_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir_path:
            yield tmpdir_path

    def get_manifest_files(self, manifest_path) -> Set[str]:
        """Helper method to extract file paths from a manifest"""
        with open(manifest_path, "r") as manifest_file:
            manifest_payload = json.load(manifest_file)
            return {item["path"] for item in manifest_payload["paths"]}

    def test_snapshot_empty_folder(self, temp_dir):
        """
        Snapshot with an invalid folder. Should find nothing and no manifest.
        """

        # Given foobar folder
        root_dir = os.path.join(temp_dir, "foobar")
        os.makedirs(root_dir)

        # When, Then.
        with pytest.raises(ManifestCreationException):
            _manifest_snapshot(root=root_dir, destination=temp_dir, name="test")

    def test_snapshot_folder(self, temp_dir):
        """
        Snapshot with a folder and a single file in it. Should generate a manifest containing 1 file.
        """

        # Given snapshot folder and 1 test file
        root_dir = os.path.join(temp_dir, "snapshot")

        test_file_name = "test_file"
        test_file = os.path.join(root_dir, test_file_name)
        os.makedirs(os.path.dirname(test_file), exist_ok=True)
        with open(test_file, "w") as f:
            f.write("testing123")

        # When
        manifest: Optional[ManifestSnapshot] = _manifest_snapshot(
            root=root_dir, destination=temp_dir, name="test"
        )

        # Then
        assert manifest is not None
        assert manifest.root is not None
        assert manifest.root == root_dir
        assert manifest.manifest is not None
        with open(manifest.manifest, "r") as manifest_file:
            manifest_payload = json.load(manifest_file)
            assert len(manifest_payload["paths"]) == 1
            assert manifest_payload["paths"][0]["path"] == test_file_name

    def test_snapshot_recursive_folder(self, temp_dir):
        """
        Snapshot with a folder a file, a nested folder and a file in the nested folder.
        """

        # Given snapshot folder and 1 test file
        root_dir = os.path.join(temp_dir, "snapshot")

        test_file_name = "test_file"
        test_file = os.path.join(root_dir, test_file_name)
        os.makedirs(os.path.dirname(test_file), exist_ok=True)
        with open(test_file, "w") as f:
            f.write("testing123")

        nested_test_file_name = "nested_file"
        nested_folder = "nested"
        nested_test_file = os.path.join(root_dir, nested_folder, nested_test_file_name)
        os.makedirs(os.path.dirname(nested_test_file), exist_ok=True)
        with open(nested_test_file, "w") as f:
            f.write("testing123")

        # When
        manifest: Optional[ManifestSnapshot] = _manifest_snapshot(
            root=root_dir, destination=temp_dir, name="test"
        )

        # Then
        assert manifest is not None
        assert manifest.root is not None
        assert manifest.root == root_dir
        assert manifest.manifest is not None
        with open(manifest.manifest, "r") as manifest_file:
            manifest_payload = json.load(manifest_file)
            assert len(manifest_payload["paths"]) == 2
            files = set()
            for item in manifest_payload["paths"]:
                files.add(item["path"])

            assert test_file_name in files
            assert f"{nested_folder}/{nested_test_file_name}" in files

    @pytest.mark.parametrize(
        "includes, excludes, results",
        [
            pytest.param(
                ["test_file", "**/nested_file"], None, ["test_file", "nested/nested_file"]
            ),
            pytest.param(
                ["nested/**"], None, ["nested/excluded_nested_file", "nested/nested_file"]
            ),
            pytest.param(
                None,
                ["excluded_test_file", "**/excluded_nested_file"],
                ["test_file", "nested/nested_file"],
            ),
            pytest.param(
                ["test_file"], ["excluded_test_file", "**/excluded_nested_file"], ["test_file"]
            ),
            pytest.param(
                ["**/nested_file"],
                ["excluded_test_file", "**/excluded_nested_file"],
                ["nested/nested_file"],
            ),
        ],
    )
    def test_snapshot_includes_excludes(
        self, temp_dir, includes: List[str], excludes: List[str], results: List[str]
    ):
        """
        Snapshot with a folder a file, a nested folder and a file in the nested folder.
        Include glob includes "test_file", "nested_file".
        Should not pick up "excluded".
        """

        # Given snapshot folder and 1 test file
        root_dir = os.path.join(temp_dir, "snapshot")

        test_file_name = "test_file"
        test_file = os.path.join(root_dir, test_file_name)
        os.makedirs(os.path.dirname(test_file), exist_ok=True)
        with open(test_file, "w") as f:
            f.write("testing123")

        excluded_test_file_name = "excluded_test_file"
        excluded_test_file = os.path.join(root_dir, excluded_test_file_name)
        with open(excluded_test_file, "w") as f:
            f.write("testing123")

        nested_test_file_name = "nested_file"
        nested_folder = "nested"
        nested_test_file = os.path.join(root_dir, nested_folder, nested_test_file_name)
        os.makedirs(os.path.dirname(nested_test_file), exist_ok=True)
        with open(nested_test_file, "w") as f:
            f.write("testing123")

        nested_excluded_test_file_name = "excluded_nested_file"
        nested_excluded_test_file = os.path.join(
            root_dir, nested_folder, nested_excluded_test_file_name
        )
        with open(nested_excluded_test_file, "w") as f:
            f.write("testing123")

        # When
        manifest: Optional[ManifestSnapshot] = _manifest_snapshot(
            root=root_dir,
            destination=temp_dir,
            name="test",
            include=includes,
            exclude=excludes,
        )

        # Then
        assert manifest is not None
        assert manifest.root is not None
        assert manifest.root == root_dir
        assert manifest.manifest is not None
        with open(manifest.manifest, "r") as manifest_file:
            manifest_payload = json.load(manifest_file)
            assert len(manifest_payload["paths"]) == len(results)
            files = set()
            for item in manifest_payload["paths"]:
                files.add(item["path"])

            for result in results:
                assert result in files

    def test_snapshot_diff(self, temp_dir):
        """
        Create a snapshot with 1 file. Add a second file and make a diff manifest.
        Only the second file should be found.
        """
        # Given snapshot folder and 1 test file
        root_dir = os.path.join(temp_dir, "snapshot")

        test_file_name = "test_file"
        test_file = os.path.join(root_dir, test_file_name)
        os.makedirs(os.path.dirname(test_file), exist_ok=True)
        with open(test_file, "w") as f:
            f.write("testing123")

        # When
        manifest: Optional[ManifestSnapshot] = _manifest_snapshot(
            root=root_dir, destination=temp_dir, name="test"
        )

        # Then
        assert manifest is not None
        assert manifest.root is not None
        assert manifest.manifest is not None

        # Given a second new file.
        second_test_file_name = "second_file"
        second_test_file = os.path.join(root_dir, second_test_file_name)
        os.makedirs(os.path.dirname(second_test_file), exist_ok=True)
        with open(second_test_file, "w") as f:
            f.write("second123")

        # When snapshot again.
        diffed_manifest: Optional[ManifestSnapshot] = _manifest_snapshot(
            root=root_dir, destination=temp_dir, name="test", diff=manifest.manifest
        )

        # Then. We should find only the second file.
        assert diffed_manifest is not None
        assert diffed_manifest.manifest is not None
        with open(diffed_manifest.manifest, "r") as diff_manifest_file:
            manifest_payload = json.load(diff_manifest_file)
            assert len(manifest_payload["paths"]) == 1
            files = set()
            for item in manifest_payload["paths"]:
                files.add(item["path"])

            assert second_test_file_name in files

    def test_snapshot_time_diff(self, temp_dir):
        """
        Create a snapshot with 1 file. Change the time stamp of the file.
        The diff manifest should contain the file again.
        """

        # Given snapshot folder and 1 test file
        root_dir = os.path.join(temp_dir, "snapshot")

        test_file_name = "test_file"
        test_file = os.path.join(root_dir, test_file_name)
        os.makedirs(os.path.dirname(test_file), exist_ok=True)
        with open(test_file, "w") as f:
            f.write("testing123")

        # When
        manifest: Optional[ManifestSnapshot] = _manifest_snapshot(
            root=root_dir, destination=temp_dir, name="test"
        )

        # Then
        assert manifest is not None
        assert manifest.root is not None
        assert manifest.manifest is not None

        # Given the file's timestamp is updated.
        os.utime(test_file, (1234567890, 1234567890))

        # When snapshot again.
        diffed_manifest: Optional[ManifestSnapshot] = _manifest_snapshot(
            root=root_dir, destination=temp_dir, name="test", diff=manifest.manifest
        )

        # Then. We should find the file again.
        assert diffed_manifest is not None
        assert diffed_manifest.manifest is not None
        with open(diffed_manifest.manifest, "r") as diff_manifest_file:
            manifest_payload = json.load(diff_manifest_file)
            assert len(manifest_payload["paths"]) == 1
            files = set()
            for item in manifest_payload["paths"]:
                files.add(item["path"])

            assert test_file_name in files

    def test_snapshot_size_diff(self, temp_dir):
        """
        Create a snapshot with 1 file. Change the contents of the file.
        The diff manifest should contain the file again.
        """

        # Given snapshot folder and 1 test file
        root_dir = os.path.join(temp_dir, "snapshot")

        test_file_name = "test_file"
        test_file = os.path.join(root_dir, test_file_name)
        os.makedirs(os.path.dirname(test_file), exist_ok=True)
        with open(test_file, "w") as f:
            f.write("testing123")

        # When
        manifest: Optional[ManifestSnapshot] = _manifest_snapshot(
            root=root_dir, destination=temp_dir, name="test"
        )

        # Then
        assert manifest is not None
        assert manifest.root is not None
        assert manifest.manifest is not None

        # Given the file's contents is updated.
        with open(test_file, "w") as f:
            f.write("testing123testing123testing123")

        # When snapshot again.
        diffed_manifest: Optional[ManifestSnapshot] = _manifest_snapshot(
            root=root_dir, destination=temp_dir, name="test", diff=manifest.manifest
        )

        # Then. We should find the file again.
        assert diffed_manifest is not None
        assert diffed_manifest.manifest is not None
        with open(diffed_manifest.manifest, "r") as diff_manifest_file:
            manifest_payload = json.load(diff_manifest_file)
            assert len(manifest_payload["paths"]) == 1
            files = set()
            for item in manifest_payload["paths"]:
                files.add(item["path"])

            assert test_file_name in files

    def test_snapshot_diff_no_diff(self, temp_dir):
        """
        Create a snapshot with 1 file. Snapshot again and diff. It should have no manifest.
        """
        # Given snapshot folder and 1 test file
        root_dir = os.path.join(temp_dir, "snapshot")

        test_file_name = "test_file"
        test_file = os.path.join(root_dir, test_file_name)
        os.makedirs(os.path.dirname(test_file), exist_ok=True)
        with open(test_file, "w") as f:
            f.write("testing123")

        # When
        manifest: Optional[ManifestSnapshot] = _manifest_snapshot(
            root=root_dir, destination=temp_dir, name="test"
        )

        # Then
        assert manifest is not None
        assert manifest.root is not None
        assert manifest.manifest is not None

        # When snapshot again.
        diffed_manifest: Optional[ManifestSnapshot] = _manifest_snapshot(
            root=root_dir, destination=temp_dir, name="test", diff=manifest.manifest
        )

        # Then. We should find no new manifest, there were no files to snapshot
        assert diffed_manifest is None

    @_retry(
        tries=2, delay=0.1, backoff=0.1
    )  # os.utime may take time for the file system to stablize.
    def test_snapshot_diff_no_diff_modified_mtime(self, temp_dir):
        """
        Create a snapshot with 1 file. Modify the mtime of the snapshot to simulate the attachment download operation.
        Snapshot again and diff. It should have no manifest.
        """
        # Given snapshot folder and 1 test file
        root_dir = os.path.join(temp_dir, "snapshot")

        test_file_name = "test_file"
        test_file = os.path.join(root_dir, test_file_name)
        os.makedirs(os.path.dirname(test_file), exist_ok=True)
        with open(test_file, "w") as f:
            f.write("testing123")

        # When
        manifest: Optional[ManifestSnapshot] = _manifest_snapshot(
            root=root_dir, destination=temp_dir, name="test"
        )

        # Then
        assert manifest is not None
        assert manifest.root is not None
        assert manifest.manifest is not None

        with open(manifest.manifest, "r") as manifest_file:
            manifest_payload = json.load(manifest_file)
            assert len(manifest_payload["paths"]) == 1
            modified_time_override = manifest_payload["paths"][0]["mtime"] / 1000000

        # When simulate the file timestamp override from downloaded asset
        os.utime(test_file, (modified_time_override, modified_time_override))
        assert Path(test_file).stat().st_mtime == modified_time_override

        # When snapshot again.
        diffed_manifest: Optional[ManifestSnapshot] = _manifest_snapshot(
            root=root_dir, destination=temp_dir, name="test", diff=manifest.manifest
        )
        # Then. We should find no new manifest, there were no files to snapshot
        assert diffed_manifest is None

    @pytest.mark.parametrize(
        "test_case,initial_include,initial_exclude,diff_include,diff_exclude,modified_files,new_files,expected_diff_files",
        [
            # Test case 1: Include filter with same filter in diff
            (
                "include_filter",
                None,  # initial include
                None,  # initial exclude
                ["subdir1/**"],  # diff include
                None,  # diff exclude
                ["subdir1/file1.txt"],  # files to modify
                [
                    ("subdir1/file2.txt", "new txt"),
                    ("subdir1/file1.txt", "new dat"),
                ],  # new files to add
                {"subdir1/file1.txt", "subdir1/file2.txt"},  # expected files in diff
            ),
            # Test case 2: Exclude filter with same filter in diff
            (
                "exclude_filter",
                None,  # initial include
                ["*.dat"],  # initial exclude
                None,  # diff include
                ["*.dat"],  # diff exclude
                ["file1.txt", "file1.dat"],  # files to modify
                [],  # no new files
                {"file1.txt"},  # expected files in diff (dat file excluded)
            ),
        ],
    )
    def test_diff_with_includes_excludes(
        self,
        temp_dir,
        test_case,
        initial_include,
        initial_exclude,
        diff_include,
        diff_exclude,
        modified_files,
        new_files,
        expected_diff_files,
    ):
        """
        Parametrized test for different filter scenarios with diff:
        1. Create initial snapshot with specified include/exclude filters
        2. Modify specified files and add new files
        3. Create diff snapshot with specified include/exclude filters
        4. Verify the diff contains the expected files
        """
        # Setup test directory
        root_dir = os.path.join(temp_dir, "snapshot")
        os.makedirs(root_dir, exist_ok=True)

        # Create initial files
        subdir1 = os.path.join(root_dir, "subdir1")
        subdir2 = os.path.join(root_dir, "subdir2")
        os.makedirs(subdir1)
        os.makedirs(subdir2)
        Path(os.path.join(subdir1, "file1.txt")).touch()
        Path(os.path.join(subdir2, "file2.txt")).touch()

        # Create initial snapshot with specified filters
        initial_manifest = _manifest_snapshot(
            root=root_dir,
            destination=temp_dir,
            name=f"initial_{test_case}",
            include=initial_include,
            exclude=initial_exclude,
        )

        assert initial_manifest is not None
        initial_paths = self.get_manifest_files(initial_manifest.manifest)
        assert len(initial_paths) == 2

        # Modify specified files
        for filename in modified_files:
            with open(os.path.join(root_dir, filename), "w") as f:
                f.write(f"modified {filename}")

        # Add new files
        for filename, content in new_files:
            with open(os.path.join(root_dir, filename), "w") as f:
                f.write(content)

        # Create diff snapshot with specified filters
        diff_manifest = _manifest_snapshot(
            root=root_dir,
            destination=temp_dir,
            name=f"diff_{test_case}",
            include=diff_include,
            exclude=diff_exclude,
            diff=initial_manifest.manifest,
        )

        assert diff_manifest is not None
        diff_files = self.get_manifest_files(diff_manifest.manifest)

        # Verify diff manifest contains expected files
        assert diff_files == expected_diff_files
