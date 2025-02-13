# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import json
import os
from pathlib import Path
import tempfile
from typing import Optional
from deadline.job_attachments.api.manifest import _manifest_diff, _manifest_snapshot
from deadline.job_attachments.models import ManifestDiff, ManifestSnapshot
import pytest


TEST_FILE = "test_file"


class TestDiffAPI:
    @pytest.fixture
    def temp_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir_path:
            yield tmpdir_path

    def _snapshot_folder_helper(self, temp_dir, root_dir) -> str:
        """
        Snapshot with a folder and a single file in it. Should generate a manifest containing 1 file.
        """

        # Given snapshot folder and 1 test file
        test_file_name = TEST_FILE
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
        assert manifest.manifest is not None
        with open(manifest.manifest, "r") as manifest_file:
            manifest_payload = json.load(manifest_file)
            assert len(manifest_payload["paths"]) == 1
            assert manifest_payload["paths"][0]["path"] == test_file_name

        # Return the tested manifest.
        return manifest.manifest

    def test_diff_no_change(self, temp_dir):
        """
        Diff with the same folder, no new files. Should return with all empty no diff result.
        """
        # Given
        root_dir = os.path.join(temp_dir, "snapshot")
        manifest_file = self._snapshot_folder_helper(temp_dir=temp_dir, root_dir=root_dir)

        # When
        manifest_diff: ManifestDiff = _manifest_diff(root=root_dir, manifest=manifest_file)
        assert len(manifest_diff.deleted) == 0
        assert len(manifest_diff.modified) == 0
        assert len(manifest_diff.new) == 0

    def test_diff_new_files(self, temp_dir):
        """
        Diff with the same folder, new files. Should return with all empty no diff result.
        """
        # Given
        root_dir = os.path.join(temp_dir, "snapshot")
        manifest_file = self._snapshot_folder_helper(temp_dir=temp_dir, root_dir=root_dir)

        # When
        # Make 2 new files, one in the snapshot dir, another in a nested dir.
        new_file_name = "new_file"
        new_file = os.path.join(root_dir, new_file_name)
        Path(new_file).touch()

        new_dir = "new_dir"
        new_file2_name = "new_file2"
        new_file2 = os.path.join(root_dir, new_dir, new_file2_name)
        os.makedirs(os.path.dirname(new_file2), exist_ok=True)
        Path(new_file2).touch()

        # Then
        manifest_diff: ManifestDiff = _manifest_diff(root=root_dir, manifest=manifest_file)
        assert len(manifest_diff.deleted) == 0
        assert len(manifest_diff.modified) == 0
        assert len(manifest_diff.new) == 2
        assert new_file_name in manifest_diff.new
        assert f"{new_dir}/{new_file2_name}" in manifest_diff.new

    def test_diff_deleted_file(self, temp_dir):
        """
        Diff with the same folder, delete the test file. It should be found by delete.
        """
        # Given
        root_dir = os.path.join(temp_dir, "snapshot")
        manifest_file = self._snapshot_folder_helper(temp_dir=temp_dir, root_dir=root_dir)

        # When
        os.remove(os.path.join(root_dir, TEST_FILE))
        manifest_diff: ManifestDiff = _manifest_diff(root=root_dir, manifest=manifest_file)

        # Then
        assert len(manifest_diff.modified) == 0
        assert len(manifest_diff.new) == 0
        assert len(manifest_diff.deleted) == 1
        assert TEST_FILE in manifest_diff.deleted

    def test_diff_modified_file_size(self, temp_dir):
        """
        Diff with the same folder, modified the test file. It should be found by modified.
        """
        # Given
        root_dir = os.path.join(temp_dir, "snapshot")
        manifest_file = self._snapshot_folder_helper(temp_dir=temp_dir, root_dir=root_dir)

        # When
        test_file = os.path.join(root_dir, TEST_FILE)
        with open(test_file, "w") as f:
            f.write("something_different")

        manifest_diff: ManifestDiff = _manifest_diff(root=root_dir, manifest=manifest_file)

        # Then
        assert len(manifest_diff.new) == 0
        assert len(manifest_diff.deleted) == 0
        assert len(manifest_diff.modified) == 1
        assert TEST_FILE in manifest_diff.modified
