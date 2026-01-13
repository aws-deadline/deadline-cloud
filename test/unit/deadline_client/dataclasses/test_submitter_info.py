# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the SubmitterInfo dataclass.
"""

from deadline.client.dataclasses.submitter_info import SubmitterInfo


class TestSubmitterInfo:
    """Test cases for SubmitterInfo dataclass."""

    def test_minimal_submitter_info(self):
        """Test creating SubmitterInfo with only required fields."""
        info = SubmitterInfo(submitter_name="TestSubmitter")

        assert info.submitter_name == "TestSubmitter"
        assert info.submitter_package_name is None
        assert info.submitter_package_version is None
        assert info.host_application_name is None
        assert info.host_application_version is None
        assert info.additional_info is None

    def test_full_submitter_info(self):
        """Test creating SubmitterInfo with all fields populated."""
        from typing import Dict, Any

        additional_info: Dict[str, Any] = {
            "render_engine": "Cycles",
            "loaded_plugins": {"Plugin 1": "1.0.0", "Plugin 2": "2.0.0"},
        }

        info = SubmitterInfo(
            submitter_name="Blender",
            submitter_package_name="deadline-cloud-for-blender",
            submitter_package_version="0.5.0",
            host_application_name="Blender",
            host_application_version="4.5.21",
            additional_info=additional_info,
        )

        assert info.submitter_name == "Blender"
        assert info.submitter_package_name == "deadline-cloud-for-blender"
        assert info.submitter_package_version == "0.5.0"
        assert info.host_application_name == "Blender"
        assert info.host_application_version == "4.5.21"
        assert info.additional_info == additional_info
