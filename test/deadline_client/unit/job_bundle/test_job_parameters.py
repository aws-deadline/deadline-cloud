# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests the deadline.client.job_bundle.parameters functions for working with
job bundle parameters
"""
from __future__ import annotations

import pytest

from deadline.client.job_bundle import parameters
from deadline.client import exceptions


@pytest.mark.parametrize(
    "job_parameters, job_bundle_dir, job_bundle_parameters, asset_references",
    [
        pytest.param(
            [],
            ".",
            [
                {
                    "name": "TestParameterName",
                    "type": "PATH",
                    "dataFlow": "INOUT",
                    "userInterface": {
                        "control": "CHOOSE_DIRECTORY",
                        "label": "Input/Output Data Directory",
                    },
                }
            ],
            {
                "inputs": {"directories": [], "filenames": []},
                "outputs": {"directories": []},
            },
            id="PATH param type",
        ),
        pytest.param(
            [],
            ".",
            [
                {
                    "name": "TestParameterName",
                    "type": "STRING",
                }
            ],
            {
                "inputs": {"directories": [], "filenames": []},
                "outputs": {"directories": []},
            },
            id="STRING param type",
        ),
        pytest.param(
            [
                {
                    "name": "ParameterWithValue",
                    "value": "value",
                }
            ],
            "./assetsdir",
            [
                {
                    "name": "ParameterWithValue",
                    "type": "STRING",
                },
                {
                    "name": "TestParameterName",
                    "type": "STRING",
                },
            ],
            {
                "inputs": {"directories": [], "filenames": []},
                "outputs": {"directories": []},
            },
            id="Parameter no default with value first",
        ),
    ],
)
def test_apply_job_parameters_parameter_without_value(
    job_parameters, job_bundle_dir, job_bundle_parameters, asset_references
):
    """
    Test that a job bundle parameter with no default value, and without
    a provided value, raises an error.
    """
    with pytest.raises(exceptions.DeadlineOperationError) as excinfo:
        parameters.apply_job_parameters(
            job_parameters, job_bundle_dir, job_bundle_parameters, asset_references
        )
    assert "TestParameterName" in str(excinfo)
    assert "no default value" in str(excinfo).lower()
    assert "no parameter value" in str(excinfo).lower()
