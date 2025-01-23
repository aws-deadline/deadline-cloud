# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
import json
from typing import Dict
import pytest
from unittest.mock import patch

from deadline.client.exceptions import NonValidInputError
from deadline.job_attachments.asset_manifests.base_manifest import BaseAssetManifest
from deadline.job_attachments.api._utils import _read_manifests


class TestReadManifests:
    def test_valid_manifests(self, temp_dir, test_manifest_one):
        """Test valid manifest file for read

        Args:
            temp_dir: a temporary directory
            test_manifest_one: test manifest
        """

        # Given
        manifest_file_name = "manifest_1"
        file_path = os.path.join(temp_dir, manifest_file_name)

        with open(file_path, "w", encoding="utf8") as f:
            json.dump(test_manifest_one, f)

        # When
        result: Dict[str, BaseAssetManifest] = _read_manifests([file_path])

        # Then
        assert len(result) == 1
        assert result.get(manifest_file_name) is not None

        manifest = result.get(manifest_file_name)
        assert isinstance(manifest, BaseAssetManifest)
        assert len(manifest.paths) == 3

    def test_invalid_file_path(self):
        """
        Test with non-existent file
        """

        with patch("os.path.isfile", return_value=False):
            with pytest.raises(NonValidInputError) as exc_info:
                _read_manifests(["/path/to/nonexistent.json"])

            assert "not valid" in str(exc_info.value)

    def test_empty_manifest_list(self):
        """
        Test with empty input
        """

        # When
        result = _read_manifests([])

        # Then
        assert isinstance(result, dict)
        assert len(result) == 0
