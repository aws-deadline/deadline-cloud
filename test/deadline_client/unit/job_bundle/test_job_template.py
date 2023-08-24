# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests the job template-related code in job_bundle.job_template.
"""

import pytest

from deadline.client.job_bundle import job_template


@pytest.mark.parametrize(
    "control_name",
    [
        "LINE_EDIT",
        "MULTILINE_EDIT",
        "INT_SPIN_BOX",
        "FLOAT_SPIN_BOX",
        "DROPDOWN_LIST",
        "CHOOSE_INPUT_FILE",
        "CHOOSE_OUTPUT_FILE",
        "CHOOSE_DIRECTORY",
        "CHECK_BOX",
        "HIDDEN",
    ],
)
def test_job_template_ui_controls_enum(control_name):
    """Test that the ControlType num has all the UI controls listed"""
    assert hasattr(job_template.ControlType, control_name)
    assert job_template.ControlType[control_name].name == control_name
