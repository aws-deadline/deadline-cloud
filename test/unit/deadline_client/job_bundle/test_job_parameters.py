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
            job_parameters, job_bundle_dir, job_bundle_parameters, [], asset_references
        )
    assert "TestParameterName" in str(excinfo)
    assert "no default value" in str(excinfo).lower()
    assert "no parameter value" in str(excinfo).lower()


@pytest.mark.parametrize(
    "parameter_def, expected_control",
    [
        # All the defaults
        ({"name": "X", "type": "STRING"}, "LINE_EDIT"),
        ({"name": "X", "type": "PATH"}, "CHOOSE_DIRECTORY"),
        ({"name": "X", "type": "INT"}, "SPIN_BOX"),
        ({"name": "X", "type": "FLOAT"}, "SPIN_BOX"),
        # When there is an allowedValues list
        ({"name": "X", "type": "STRING", "allowedValues": ["A", "B"]}, "DROPDOWN_LIST"),
        ({"name": "X", "type": "PATH", "allowedValues": ["/A", "/B"]}, "DROPDOWN_LIST"),
        ({"name": "X", "type": "INT", "allowedValues": [1, 2]}, "DROPDOWN_LIST"),
        ({"name": "X", "type": "FLOAT", "allowedValues": [1.0, 2.5]}, "DROPDOWN_LIST"),
        # Variations for PATH parameters
        ({"name": "X", "type": "PATH", "objectType": "FILE"}, "CHOOSE_INPUT_FILE"),
        (
            {"name": "X", "type": "PATH", "objectType": "FILE", "dataFlow": "NONE"},
            "CHOOSE_INPUT_FILE",
        ),
        (
            {"name": "X", "type": "PATH", "objectType": "FILE", "dataFlow": "IN"},
            "CHOOSE_INPUT_FILE",
        ),
        (
            {"name": "X", "type": "PATH", "objectType": "FILE", "dataFlow": "INOUT"},
            "CHOOSE_INPUT_FILE",
        ),
        (
            {"name": "X", "type": "PATH", "objectType": "FILE", "dataFlow": "OUT"},
            "CHOOSE_OUTPUT_FILE",
        ),
        ({"name": "X", "type": "PATH", "objectType": "DIRECTORY"}, "CHOOSE_DIRECTORY"),
        # When the control is specified explicitly for STRING
        ({"name": "X", "type": "STRING", "userInterface": {"control": "LINE_EDIT"}}, "LINE_EDIT"),
        (
            {"name": "X", "type": "STRING", "userInterface": {"control": "MULTILINE_EDIT"}},
            "MULTILINE_EDIT",
        ),
        (
            {
                "name": "X",
                "type": "STRING",
                "userInterface": {"control": "DROPDOWN_LIST"},
                "allowedValues": ["A", "B"],
            },
            "DROPDOWN_LIST",
        ),
        (
            {
                "name": "X",
                "type": "STRING",
                "userInterface": {"control": "CHECK_BOX"},
                "allowedValues": ["true", "false"],
            },
            "CHECK_BOX",
        ),
        ({"name": "X", "type": "STRING", "userInterface": {"control": "HIDDEN"}}, "HIDDEN"),
        # When the control is specified explicitly for PATH
        (
            {"name": "X", "type": "PATH", "userInterface": {"control": "CHOOSE_INPUT_FILE"}},
            "CHOOSE_INPUT_FILE",
        ),
        (
            {
                "name": "X",
                "type": "PATH",
                "objectType": "FILE",
                "dataFlow": "NONE",
                "userInterface": {"control": "CHOOSE_INPUT_FILE"},
            },
            "CHOOSE_INPUT_FILE",
        ),
        (
            {
                "name": "X",
                "type": "PATH",
                "objectType": "FILE",
                "dataFlow": "NONE",
                "userInterface": {"control": "CHOOSE_OUTPUT_FILE"},
            },
            "CHOOSE_OUTPUT_FILE",
        ),
        (
            {
                "name": "X",
                "type": "PATH",
                "objectType": "FILE",
                "dataFlow": "IN",
                "userInterface": {"control": "CHOOSE_INPUT_FILE"},
            },
            "CHOOSE_INPUT_FILE",
        ),
        (
            {
                "name": "X",
                "type": "PATH",
                "objectType": "FILE",
                "dataFlow": "INOUT",
                "userInterface": {"control": "CHOOSE_INPUT_FILE"},
            },
            "CHOOSE_INPUT_FILE",
        ),
        (
            {
                "name": "X",
                "type": "PATH",
                "objectType": "FILE",
                "dataFlow": "INOUT",
                "userInterface": {"control": "CHOOSE_OUTPUT_FILE"},
            },
            "CHOOSE_OUTPUT_FILE",
        ),
        (
            {
                "name": "X",
                "type": "PATH",
                "objectType": "FILE",
                "dataFlow": "OUT",
                "userInterface": {"control": "CHOOSE_OUTPUT_FILE"},
            },
            "CHOOSE_OUTPUT_FILE",
        ),
        (
            {
                "name": "X",
                "type": "PATH",
                "objectType": "DIRECTORY",
                "userInterface": {"control": "CHOOSE_DIRECTORY"},
            },
            "CHOOSE_DIRECTORY",
        ),
        (
            {
                "name": "X",
                "type": "PATH",
                "userInterface": {"control": "DROPDOWN_LIST"},
                "allowedValues": ["/A", "/B"],
            },
            "DROPDOWN_LIST",
        ),
        ({"name": "X", "type": "PATH", "userInterface": {"control": "HIDDEN"}}, "HIDDEN"),
        # When the control is specified explicitly for INT
        ({"name": "X", "type": "INT", "userInterface": {"control": "SPIN_BOX"}}, "SPIN_BOX"),
        (
            {
                "name": "X",
                "type": "INT",
                "userInterface": {"control": "DROPDOWN_LIST"},
                "allowedValues": [1, 2],
            },
            "DROPDOWN_LIST",
        ),
        ({"name": "X", "type": "INT", "userInterface": {"control": "HIDDEN"}}, "HIDDEN"),
        # When the control is specified explicitly for FLOAT
        ({"name": "X", "type": "FLOAT", "userInterface": {"control": "SPIN_BOX"}}, "SPIN_BOX"),
        (
            {
                "name": "X",
                "type": "FLOAT",
                "userInterface": {"control": "DROPDOWN_LIST"},
                "allowedValues": [1, 2],
            },
            "DROPDOWN_LIST",
        ),
        ({"name": "X", "type": "FLOAT", "userInterface": {"control": "HIDDEN"}}, "HIDDEN"),
    ],
)
def test_ui_control_for_parameter_definition(parameter_def, expected_control):
    """Test that the correct UI control for a parameter definition is returned."""
    assert parameters.get_ui_control_for_parameter_definition(parameter_def) == expected_control


@pytest.mark.parametrize(
    "parameter_def",
    [
        # Unsupported type
        ({"name": "X", "type": "UNSUPPORTED"}),
        # Dropdown list requires allowedValues
        ({"name": "X", "type": "STRING", "userInterface": {"control": "DROPDOWN_LIST"}}),
        ({"name": "X", "type": "PATH", "userInterface": {"control": "DROPDOWN_LIST"}}),
        ({"name": "X", "type": "INT", "userInterface": {"control": "DROPDOWN_LIST"}}),
        ({"name": "X", "type": "FLOAT", "userInterface": {"control": "DROPDOWN_LIST"}}),
        # Supported controls, but not for the STRING type
        ({"name": "X", "type": "STRING", "userInterface": {"control": "CHOOSE_INPUT_FILE"}}),
        ({"name": "X", "type": "STRING", "userInterface": {"control": "CHOOSE_OUTPUT_FILE"}}),
        ({"name": "X", "type": "STRING", "userInterface": {"control": "CHOOSE_DIRECTORY"}}),
        ({"name": "X", "type": "STRING", "userInterface": {"control": "SPIN_BOX"}}),
        # Supported controls, but not for the PATH type
        ({"name": "X", "type": "PATH", "userInterface": {"control": "LINE_EDIT"}}),
        ({"name": "X", "type": "PATH", "userInterface": {"control": "MULTILINE_EDIT"}}),
        ({"name": "X", "type": "PATH", "userInterface": {"control": "CHECK_BOX"}}),
        ({"name": "X", "type": "PATH", "userInterface": {"control": "SPIN_BOX"}}),
        # Supported controls, but not for the INT type
        ({"name": "X", "type": "INT", "userInterface": {"control": "LINE_EDIT"}}),
        ({"name": "X", "type": "INT", "userInterface": {"control": "MULTILINE_EDIT"}}),
        ({"name": "X", "type": "INT", "userInterface": {"control": "CHECK_BOX"}}),
        ({"name": "X", "type": "INT", "userInterface": {"control": "CHOOSE_INPUT_FILE"}}),
        ({"name": "X", "type": "INT", "userInterface": {"control": "CHOOSE_OUTPUT_FILE"}}),
        ({"name": "X", "type": "INT", "userInterface": {"control": "CHOOSE_DIRECTORY"}}),
        # Supported controls, but not for the FLOAT type
        ({"name": "X", "type": "FLOAT", "userInterface": {"control": "LINE_EDIT"}}),
        ({"name": "X", "type": "FLOAT", "userInterface": {"control": "MULTILINE_EDIT"}}),
        ({"name": "X", "type": "FLOAT", "userInterface": {"control": "CHECK_BOX"}}),
        ({"name": "X", "type": "FLOAT", "userInterface": {"control": "CHOOSE_INPUT_FILE"}}),
        ({"name": "X", "type": "FLOAT", "userInterface": {"control": "CHOOSE_OUTPUT_FILE"}}),
        ({"name": "X", "type": "FLOAT", "userInterface": {"control": "CHOOSE_DIRECTORY"}}),
    ],
)
def test_ui_control_for_parameter_definition_errors(parameter_def):
    """Test that an error is raised with incorrect parameter definition controls."""
    with pytest.raises(exceptions.DeadlineOperationError):
        parameters.get_ui_control_for_parameter_definition(parameter_def)


@pytest.mark.parametrize(
    "parameter1, parameter2, expected_difference",
    [
        # Cases where they match
        ({"name": "X", "type": "STRING"}, {"name": "X", "type": "STRING"}, []),
        (
            {"name": "X", "type": "STRING"},
            {"name": "X", "type": "STRING", "userInterface": {"control": "HIDDEN"}},
            [],
        ),
        (
            {"name": "X", "type": "STRING", "allowedValues": ["A", "B"]},
            {"name": "X", "type": "STRING", "allowedValues": ["B", "A"]},
            [],
        ),
        # Different name
        ({"name": "X", "type": "STRING"}, {"name": "Y", "type": "STRING"}, ["name"]),
        # Different type
        ({"name": "X", "type": "STRING"}, {"name": "X", "type": "PATH"}, ["type"]),
        # Different minValue
        (
            {"name": "X", "type": "INT", "minValue": 3},
            {"name": "X", "type": "INT", "minValue": 2},
            ["minValue"],
        ),
        # Different maxValue
        (
            {"name": "X", "type": "INT", "maxValue": 3},
            {"name": "X", "type": "INT", "maxValue": 2},
            ["maxValue"],
        ),
        # Different minLength
        (
            {"name": "X", "type": "STRING", "minLength": 3},
            {"name": "X", "type": "STRING", "minLength": 2},
            ["minLength"],
        ),
        # Different maxLength
        (
            {"name": "X", "type": "STRING", "maxLength": 3},
            {"name": "X", "type": "STRING", "maxLength": 2},
            ["maxLength"],
        ),
        # Different dataFlow
        (
            {
                "name": "X",
                "type": "PATH",
                "dataFlow": "NONE",
            },
            {
                "name": "X",
                "type": "PATH",
                "dataFlow": "IN",
            },
            ["dataFlow"],
        ),
        (
            {
                "name": "X",
                "type": "PATH",
                "dataFlow": "IN",
            },
            {
                "name": "X",
                "type": "PATH",
            },
            ["dataFlow"],
        ),
        # Different objectType
        (
            {
                "name": "X",
                "type": "PATH",
                "objectType": "FILE",
            },
            {
                "name": "X",
                "type": "PATH",
                "objectType": "DIRECTORY",
            },
            ["objectType"],
        ),
        # Different allowedValues
        (
            {"name": "X", "type": "STRING", "allowedValues": ["A", "B"]},
            {"name": "X", "type": "STRING", "allowedValues": ["B", "C"]},
            ["allowedValues"],
        ),
        # Many differences
        (
            {
                "name": "X",
                "type": "PATH",
                "dataFlow": "IN",
                "objectType": "FILE",
                "minLength": 3,
                "maxLength": 5,
            },
            {
                "name": "Y",
                "type": "STRING",
                "allowedValues": ["B", "C"],
                "minLength": 2,
            },
            ["name", "type", "minLength", "maxLength", "dataFlow", "objectType", "allowedValues"],
        ),
    ],
)
def test_parameter_definition_difference(parameter1, parameter2, expected_difference):
    """Test that parameter_definition_difference returns expected differences."""
    assert parameters.parameter_definition_difference(parameter1, parameter2) == expected_difference
