# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the job_bundle_submitter module.
"""

import pytest

# Skip all tests in this module if Qt is not available
job_bundle_submitter = pytest.importorskip("deadline.client.ui.job_bundle_submitter")


class TestExtractNameParameter:
    """Tests for the _extract_name_parameter helper function."""

    def test_simple_param_reference(self):
        """Test extraction of a simple parameter reference."""
        assert job_bundle_submitter._extract_name_parameter("{{Param.JobName}}") == "JobName"

    def test_param_reference_with_whitespace(self):
        """Test extraction with leading/trailing whitespace."""
        assert job_bundle_submitter._extract_name_parameter("  {{Param.JobName}}  ") == "JobName"

    def test_different_param_name(self):
        """Test extraction of different parameter names."""
        assert (
            job_bundle_submitter._extract_name_parameter("{{Param.MyCustomName}}") == "MyCustomName"
        )
        assert job_bundle_submitter._extract_name_parameter("{{Param.Name123}}") == "Name123"

    def test_non_parametrized_name(self):
        """Test that non-parametrized names return None."""
        assert job_bundle_submitter._extract_name_parameter("My Job Name") is None
        assert job_bundle_submitter._extract_name_parameter("") is None

    def test_partial_param_reference(self):
        """Test that partial parameter references return None."""
        # Name with additional text around the parameter
        assert job_bundle_submitter._extract_name_parameter("Prefix {{Param.JobName}}") is None
        assert job_bundle_submitter._extract_name_parameter("{{Param.JobName}} Suffix") is None
        assert (
            job_bundle_submitter._extract_name_parameter("Prefix {{Param.JobName}} Suffix") is None
        )

    def test_multiple_param_references(self):
        """Test that multiple parameter references return None."""
        assert job_bundle_submitter._extract_name_parameter("{{Param.A}} {{Param.B}}") is None

    def test_invalid_param_syntax(self):
        """Test that invalid parameter syntax returns None."""
        assert job_bundle_submitter._extract_name_parameter("{{Param.}}") is None
        assert job_bundle_submitter._extract_name_parameter("{{Param}}") is None
        assert job_bundle_submitter._extract_name_parameter("{Param.JobName}") is None
        assert (
            job_bundle_submitter._extract_name_parameter("{{param.JobName}}") is None
        )  # lowercase 'param'
