# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests the deadline.client.job_bundle.submission functions for building up the
arguments to call CreateJob with.
"""
from __future__ import annotations
from typing import Any, Dict

import pytest

from deadline.client.job_bundle import submission
from deadline.client.job_bundle.parameters import JobParameter
from deadline.client.job_bundle.submission import AssetReferences

MOCK_FARM_ID = "farm-0123456789abcdef0123456789abcdef"
MOCK_QUEUE_ID = "queue-0123456789abcdef0123456789abcdef"


@pytest.mark.parametrize(
    "assets_input, expected_output",
    [
        pytest.param(
            {},
            AssetReferences(),
        ),
        pytest.param(
            {
                "assetReferences": {
                    "inputs": {"directories": [], "filenames": []},
                    "outputs": {"directories": []},
                }
            },
            AssetReferences(),
        ),
        pytest.param(
            {
                "assetReferences": {
                    "inputs": {
                        "directories": [],
                        "filenames": ["input_file_1.txt", "input_file_2.txt"],
                    },
                    "outputs": {"directories": ["output_dir_1"]},
                }
            },
            AssetReferences(
                input_filenames=set(["input_file_1.txt", "input_file_2.txt"]),
                output_directories=set(["output_dir_1"]),
            ),
        ),
        pytest.param(
            {
                "assetReferences": {
                    "inputs": {
                        "directories": ["input_dir_1"],
                        "filenames": ["input_file_1.txt", "input_file_2.txt"],
                    },
                    "outputs": {"directories": ["output_dir_1"]},
                }
            },
            AssetReferences(
                input_filenames=set(
                    [
                        "input_file_1.txt",
                        "input_file_2.txt",
                    ]
                ),
                input_directories=set(["input_dir_1"]),
                output_directories=set(["output_dir_1"]),
            ),
        ),
    ],
)
def test_flatten_asset_references(
    assets_input: Dict[str, Any],
    expected_output: AssetReferences,
) -> None:
    """
    Test that FlatAssetReferences.from_dict creates a FlatAssetReferences object with
    all of the filenames/directories from an input.
    """
    response = AssetReferences.from_dict(assets_input)

    assert response.input_filenames == expected_output.input_filenames
    assert response.input_directories == expected_output.input_directories
    assert response.output_directories == expected_output.output_directories


def test_split_parameter_args() -> None:
    """
    Tests that split_parameter_args parses the input job bundle paramters
    and creates properly formatted app/job parameter dictionaries as returns.
    """

    input_bundle_params: list[JobParameter] = [
        {"name": "param_1", "value": "TESTING", "type": "STRING"},
        {"name": "param_2", "value": 10, "type": "INT"},
        {"name": "param_3", "value": 10.123, "type": "FLOAT"},
        # The deadline app prefix should go in expected_app_params
        {"name": "deadline:priority", "value": "55"},
        # Other app prefixes should be dropped
        {"name": "otherrenderfarm:priority", "value": "72"},
        {"name": "otherrenderfarm:customparam", "value": "medium"},
    ]
    expected_app_params = {"priority": "55"}
    expected_job_params = {
        "param_1": {"string": "TESTING"},
        "param_2": {"int": "10"},
        "param_3": {"float": "10.123"},
    }
    app_params, job_params = submission.split_parameter_args(input_bundle_params, "test_bundle")

    assert app_params == expected_app_params
    assert job_params == expected_job_params


def test_split_parameter_args_no_parameters() -> None:
    """
    Tests split_parameter_args returns empty dictionaries if there are
    no job bundle parameters provided.
    """
    expected_app_params: Dict[str, Any] = {}
    expected_job_params: Dict[str, Any] = {}

    app_params, job_params = submission.split_parameter_args([], "test_bundle")

    assert app_params == expected_app_params
    assert job_params == expected_job_params


def test_split_parameter_args_custom_app() -> None:
    """
    Tests that split_parameter_args parses the input job bundle paramters
    and creates properly formatted app/job parameter dictionaries as returns.
    This case provides a custom app name for the prefix.
    """

    input_bundle_params: list[JobParameter] = [
        {"name": "param_1", "value": "TESTING", "type": "STRING"},
        {"name": "param_2", "value": 10, "type": "INT"},
        {"name": "param_3", "value": 10.123, "type": "FLOAT"},
        {"name": "deadline:priority", "value": "55"},
        {"name": "curio:priority", "value": "61"},
        # Only the selected app prefix should be seen in the output
        {"name": "otherrenderfarm:customparam", "value": "medium"},
    ]
    expected_app_params = {"customparam": "medium"}
    expected_job_params = {
        "param_1": {"string": "TESTING"},
        "param_2": {"int": "10"},
        "param_3": {"float": "10.123"},
    }
    app_params, job_params = submission.split_parameter_args(
        input_bundle_params,
        "test_bundle",
        app_name="otherrenderfarm",
        supported_app_parameter_names=["customparam"],
    )

    assert app_params == expected_app_params
    assert job_params == expected_job_params
