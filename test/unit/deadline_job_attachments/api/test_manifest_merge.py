# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
import json

from typing import Optional

from deadline.job_attachments.api.manifest import _manifest_merge
from deadline.job_attachments.models import ManifestMerge


class TestMergeAPI:

    def test_merge_same_file(self, temp_dir, test_manifest_one):
        """
        Merge with one manifest file
        """
        # Given
        merge_dir = os.path.join(temp_dir, "merge")
        manifest_one = os.path.join(temp_dir, "manifest_1")

        with open(manifest_one, "w", encoding="utf8") as f:
            json.dump(test_manifest_one, f)

        # When
        manifest_merge: Optional[ManifestMerge] = _manifest_merge(
            root=temp_dir, manifest_files=[manifest_one], destination=merge_dir, name="merge"
        )

        # Then
        assert manifest_merge is not None
        assert manifest_merge.manifest_root == temp_dir
        assert merge_dir in manifest_merge.local_manifest_path

    def test_merge_different_files(self, temp_dir, test_manifest_one, test_manifest_two):
        """
        Merge two different manifest files
        """
        # Given
        manifest_one = os.path.join(temp_dir, "manifest_1")
        manifest_two = os.path.join(temp_dir, "manifest_2")
        merge_dir = os.path.join(temp_dir, "merge")

        with open(manifest_one, "w", encoding="utf8") as f:
            json.dump(test_manifest_one, f)

        with open(manifest_two, "w", encoding="utf8") as f:
            json.dump(test_manifest_two, f)

        # When
        manifest_merge: Optional[ManifestMerge] = _manifest_merge(
            root=temp_dir,
            manifest_files=[manifest_one, manifest_two],
            destination=merge_dir,
            name="merge",
        )

        # Then
        assert manifest_merge is not None
        assert manifest_merge.manifest_root == temp_dir
        assert merge_dir in manifest_merge.local_manifest_path
