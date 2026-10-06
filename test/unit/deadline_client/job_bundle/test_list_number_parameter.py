# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Test cases for LIST[INT] and LIST[FLOAT] job parameters: definition and value validation,
UI control selection, bundle loading, queue parameter merging, and CreateJob request formatting."""

from __future__ import annotations

import json
import os
from typing import Any

import pytest

from deadline.client.exceptions import DeadlineOperationError
from deadline.client.job_bundle import submission
from deadline.client.job_bundle.parameters import (
    JobParameter,
    get_ui_control_for_parameter_definition,
    merge_queue_job_parameters,
    parameter_definition_difference,
    read_job_bundle_parameters,
    validate_job_parameter,
    validate_job_parameter_value,
)

INT64_MAX = 2**63 - 1

# The valid parameter definitions from the OpenJD conformance tests
# conformance-tests/2023-09/EXPR/job_templates/2.13--list-int-param.yaml,
# 2.13--list-int-item-int64-max.yaml and 2.14--list-float-param.yaml
CONFORMANCE_VALID_DEFINITIONS: list[dict[str, Any]] = [
    {"name": "Minimal", "type": "LIST[INT]"},
    {"name": "WithDefault", "type": "LIST[INT]", "default": [1, 2, 3]},
    {"name": "EmptyDefault", "type": "LIST[INT]", "default": []},
    {"name": "SingleElement", "type": "LIST[INT]", "default": [42]},
    {"name": "Negatives", "type": "LIST[INT]", "default": [-10, -1, 0, 1, 10]},
    {"name": "LongList", "type": "LIST[INT]", "default": list(range(20))},
    {"name": "Int64Max", "type": "LIST[INT]", "default": [1, INT64_MAX]},
    {"name": "WithMinMax", "type": "LIST[INT]", "default": [1, 2], "minLength": 1, "maxLength": 50},
    {
        "name": "ItemAllowed",
        "type": "LIST[INT]",
        "default": [1, 3],
        "item": {"allowedValues": [1, 2, 3, 4, 5]},
    },
    {
        "name": "ItemRange",
        "type": "LIST[INT]",
        "default": [5, 10],
        "item": {"minValue": 0, "maxValue": 100},
    },
    {
        "name": "FullyConstrained",
        "type": "LIST[INT]",
        "default": [10, 20],
        "minLength": 1,
        "maxLength": 10,
        "item": {"minValue": 0, "maxValue": 100},
    },
    {
        "name": "WithDescription",
        "type": "LIST[INT]",
        "default": [1],
        "description": "A list of integers",
    },
    {
        "name": "WithSpinBoxList",
        "type": "LIST[INT]",
        "default": [1],
        "userInterface": {
            "control": "SPIN_BOX_LIST",
            "label": "Values",
            "groupLabel": "Numbers",
            "singleStepDelta": 5,
        },
    },
    {
        "name": "WithHidden",
        "type": "LIST[INT]",
        "default": [1],
        "userInterface": {"control": "HIDDEN"},
    },
    {"name": "FMinimal", "type": "LIST[FLOAT]"},
    {"name": "FWithDefault", "type": "LIST[FLOAT]", "default": [0.5, 1.0, 1.5]},
    {"name": "FEmptyDefault", "type": "LIST[FLOAT]", "default": []},
    {"name": "FSingleElement", "type": "LIST[FLOAT]", "default": [3.14]},
    {"name": "FIntValues", "type": "LIST[FLOAT]", "default": [1, 2, 3]},
    {"name": "FNegatives", "type": "LIST[FLOAT]", "default": [-1.5, 0.0, 1.5]},
    {"name": "FHighPrecision", "type": "LIST[FLOAT]", "default": [0.123456789, 9.87654321]},
    {
        "name": "FWithMinMax",
        "type": "LIST[FLOAT]",
        "default": [1.0, 2.0],
        "minLength": 1,
        "maxLength": 50,
    },
    {
        "name": "FItemAllowed",
        "type": "LIST[FLOAT]",
        "default": [0.25, 0.75],
        "item": {"allowedValues": [0.25, 0.5, 0.75, 1.0]},
    },
    {
        "name": "FItemRange",
        "type": "LIST[FLOAT]",
        "default": [0.5, 5.0],
        "item": {"minValue": 0.0, "maxValue": 10.0},
    },
    {
        "name": "FWithSpinBoxList",
        "type": "LIST[FLOAT]",
        "default": [1.0],
        "userInterface": {
            "control": "SPIN_BOX_LIST",
            "label": "Weights",
            "decimals": 2,
            "singleStepDelta": 0.1,
        },
    },
    {
        "name": "FWithHidden",
        "type": "LIST[FLOAT]",
        "default": [1.0],
        "userInterface": {"control": "HIDDEN"},
    },
]


@pytest.mark.parametrize(
    "definition", [pytest.param(d, id=d["name"]) for d in CONFORMANCE_VALID_DEFINITIONS]
)
def test_validate_job_parameter_list_number_valid(definition: dict[str, Any]) -> None:
    # Does not raise.
    validate_job_parameter(definition)


# The invalid job templates from the OpenJD conformance tests
# conformance-tests/2023-09/EXPR/job_templates/2.1[34]--list-{int,float}-*.invalid.yaml
@pytest.mark.parametrize(
    ("definition", "error_type", "match"),
    [
        pytest.param(
            {"type": "LIST[INT]", "default": 5},
            TypeError,
            'got int for "default" but type "LIST\\[INT\\]" expects list',
            id="int-scalar-not-list",
        ),
        pytest.param(
            {"type": "LIST[INT]", "default": ["not", "ints"]},
            TypeError,
            "item 0 is 'not'",
            id="int-wrong-item-type",
        ),
        pytest.param(
            {"type": "LIST[INT]", "default": [1.5]},
            TypeError,
            "item 0 is 1.5",
            id="int-float-item",
        ),
        pytest.param(
            {"type": "LIST[INT]", "default": [True]},
            TypeError,
            "item 0 is True",
            id="int-bool-item",
        ),
        pytest.param(
            {"type": "LIST[INT]", "default": [2**63]},
            ValueError,
            "outside the 64-bit integer range",
            id="int-above-int64",
        ),
        pytest.param(
            {"type": "LIST[INT]", "default": [], "minLength": 1},
            ValueError,
            "has 0 items but minLength is 1",
            id="int-too-short",
        ),
        pytest.param(
            {"type": "LIST[INT]", "default": [1, 2, 3], "maxLength": 2},
            ValueError,
            "has 3 items but at most 2 are allowed",
            id="int-too-long",
        ),
        pytest.param(
            {"type": "LIST[INT]", "default": [-5], "item": {"minValue": 0}},
            ValueError,
            "item 0 -5 is less than item minValue 0",
            id="int-item-below-min",
        ),
        pytest.param(
            {"type": "LIST[INT]", "default": [11], "item": {"maxValue": 10}},
            ValueError,
            "item 0 11 is greater than item maxValue 10",
            id="int-item-above-max",
        ),
        pytest.param(
            {"type": "LIST[INT]", "default": [7], "item": {"allowedValues": [1, 2]}},
            ValueError,
            "item 0 7 is not an allowed value",
            id="int-item-not-in-allowed",
        ),
        pytest.param(
            {"type": "LIST[FLOAT]", "default": 1.0},
            TypeError,
            'got float for "default" but type "LIST\\[FLOAT\\]" expects list',
            id="float-scalar-not-list",
        ),
        pytest.param(
            {"type": "LIST[FLOAT]", "default": ["not", "floats"]},
            TypeError,
            "item 0 is 'not'",
            id="float-wrong-item-type",
        ),
        pytest.param(
            {"type": "LIST[FLOAT]", "default": [float("nan")]},
            ValueError,
            "is not a finite number",
            id="float-nan",
        ),
        pytest.param(
            {"type": "LIST[FLOAT]", "default": [], "minLength": 1},
            ValueError,
            "has 0 items but minLength is 1",
            id="float-too-short",
        ),
        pytest.param(
            {"type": "LIST[FLOAT]", "default": [1.0, 2.0, 3.0], "maxLength": 2},
            ValueError,
            "has 3 items but at most 2 are allowed",
            id="float-too-long",
        ),
        pytest.param(
            {"type": "LIST[FLOAT]", "default": [-0.5], "item": {"minValue": 0.0}},
            ValueError,
            "item 0 -0.5 is less than item minValue 0.0",
            id="float-item-below-min",
        ),
    ],
)
def test_validate_job_parameter_list_number_invalid_default(
    definition: dict[str, Any], error_type: type, match: str
) -> None:
    if not match.startswith("got "):
        match = f'In "default": .*{match}'
    with pytest.raises(error_type, match=match):
        validate_job_parameter({"name": "Values", **definition})


@pytest.mark.parametrize("list_type", ["LIST[INT]", "LIST[FLOAT]"])
@pytest.mark.parametrize(
    ("field", "field_value"),
    [
        ("allowedValues", [1]),
        ("minValue", 0),
        ("maxValue", 10),
        ("objectType", "FILE"),
        ("dataFlow", "IN"),
    ],
)
def test_validate_job_parameter_list_number_disallowed_fields(
    list_type: str, field: str, field_value: Any
) -> None:
    with pytest.raises(
        ValueError,
        match=f'has "{field}" but type "{list_type}" does not support it'.replace(
            "[", "\\["
        ).replace("]", "\\]"),
    ):
        validate_job_parameter({"name": "Values", "type": list_type, field: field_value})


@pytest.mark.parametrize(
    ("list_type", "item", "error_type", "match"),
    [
        pytest.param(
            "LIST[INT]", [], TypeError, 'got list for "item" but expected dict', id="not-dict"
        ),
        pytest.param(
            "LIST[INT]",
            {"minLength": 1},
            ValueError,
            'has "item" -> "minLength" but type',
            id="int-string-field",
        ),
        pytest.param(
            "LIST[FLOAT]",
            {"maxLength": 1},
            ValueError,
            'has "item" -> "maxLength" but type',
            id="float-string-field",
        ),
        pytest.param(
            "LIST[INT]",
            {"minValue": 1.5},
            TypeError,
            'got float for "item" -> "minValue" but expected int',
            id="int-float-min",
        ),
        pytest.param(
            "LIST[INT]",
            {"maxValue": "10"},
            TypeError,
            'got str for "item" -> "maxValue"',
            id="int-str-max",
        ),
        pytest.param(
            "LIST[INT]",
            {"minValue": True},
            TypeError,
            'got bool for "item" -> "minValue"',
            id="int-bool-min",
        ),
        pytest.param(
            "LIST[INT]",
            {"maxValue": 2**63},
            ValueError,
            "outside the 64-bit integer range",
            id="int-max-above-int64",
        ),
        pytest.param(
            "LIST[FLOAT]",
            {"maxValue": float("inf")},
            ValueError,
            "not a finite number",
            id="float-inf-max",
        ),
        pytest.param(
            "LIST[FLOAT]", {"minValue": "0"}, TypeError, "expected int or float", id="float-str-min"
        ),
        pytest.param(
            "LIST[INT]",
            {"allowedValues": 1},
            TypeError,
            'got int for "item" -> "allowedValues" but expected list',
            id="allowed-not-list",
        ),
        pytest.param(
            "LIST[INT]",
            {"allowedValues": []},
            ValueError,
            'empty "item" -> "allowedValues"',
            id="allowed-empty",
        ),
        pytest.param(
            "LIST[INT]",
            {"allowedValues": [1, "2"]},
            TypeError,
            '"allowedValues" \\[1\\] but expected int',
            id="allowed-wrong-type",
        ),
        pytest.param(
            "LIST[INT]",
            {"minValue": 5, "maxValue": 1},
            ValueError,
            '"minValue" 5 greater than "item" -> "maxValue" 1',
            id="min-above-max",
        ),
        pytest.param(
            "LIST[FLOAT]",
            {"allowedValues": [0.5, 2.0], "maxValue": 1.0},
            ValueError,
            '"allowedValues" \\[1\\] 2.0 outside the item value range',
            id="allowed-outside-range",
        ),
    ],
)
def test_validate_job_parameter_list_number_invalid_item(
    list_type: str, item: Any, error_type: type, match: str
) -> None:
    with pytest.raises(error_type, match=match):
        validate_job_parameter({"name": "Values", "type": list_type, "item": item})


@pytest.mark.parametrize("list_type", ["LIST[INT]", "LIST[FLOAT]"])
def test_validate_job_parameter_list_number_min_length_above_cap(list_type: str) -> None:
    with pytest.raises(ValueError, match="greater than the maximum item count of 512"):
        validate_job_parameter({"name": "Values", "type": list_type, "minLength": 513})


@pytest.mark.parametrize(
    ("list_type", "value", "expected"),
    [
        pytest.param("LIST[INT]", [3, 1, 3], [3, 1, 3], id="int-list"),
        pytest.param("LIST[INT]", "[1, -2]", [1, -2], id="int-json"),
        pytest.param("LIST[INT]", "[]", [], id="int-json-empty"),
        pytest.param("LIST[FLOAT]", [1, 2.5], [1.0, 2.5], id="float-ints-become-float"),
        pytest.param("LIST[FLOAT]", "[0.5, 2]", [0.5, 2.0], id="float-json"),
        pytest.param("LIST[FLOAT]", "[1e3]", [1000.0], id="float-json-exponent"),
    ],
)
def test_validate_job_parameter_value_list_number(
    list_type: str, value: Any, expected: list
) -> None:
    result = validate_job_parameter_value({"name": "Values", "type": list_type}, value)
    assert result == expected
    assert all(type(v) is (float if list_type == "LIST[FLOAT]" else int) for v in result)  # type: ignore[union-attr]


@pytest.mark.parametrize(
    ("list_type", "value", "error_type", "match"),
    [
        pytest.param(
            "LIST[INT]",
            "1,2",
            ValueError,
            "not a JSON array of integers, such as \\[1, 2\\]",
            id="int-not-json",
        ),
        pytest.param(
            "LIST[FLOAT]",
            "0.5",
            ValueError,
            "0.5 which is not a JSON array",
            id="float-json-scalar",
        ),
        pytest.param("LIST[INT]", 5, TypeError, "got value 5 of type", id="int-scalar"),
        pytest.param("LIST[INT]", ["1"], TypeError, "item 0 is '1'", id="int-string-item"),
        pytest.param("LIST[INT]", "[1.0]", TypeError, "item 0 is 1.0", id="int-json-float-item"),
        pytest.param("LIST[FLOAT]", [False], TypeError, "item 0 is False", id="float-bool-item"),
        pytest.param(
            "LIST[FLOAT]", "[NaN]", ValueError, "not a finite number", id="float-json-nan"
        ),
        pytest.param(
            "LIST[INT]", [0] * 513, ValueError, "at most 512 are allowed", id="int-service-cap"
        ),
    ],
)
def test_validate_job_parameter_value_list_number_invalid(
    list_type: str, value: Any, error_type: type, match: str
) -> None:
    with pytest.raises(error_type, match=match):
        validate_job_parameter_value({"name": "Values", "type": list_type}, value)


def test_validate_job_parameter_value_list_float_allowed_values_match_ints() -> None:
    """An int item of a LIST[FLOAT] matches an equal float allowed value."""
    definition: Any = {
        "name": "Values",
        "type": "LIST[FLOAT]",
        "item": {"allowedValues": [1.0, 2.5]},
    }
    assert validate_job_parameter_value(definition, [1]) == [1.0]


@pytest.mark.parametrize(
    ("definition", "expected"),
    [
        pytest.param({"name": "X", "type": "LIST[INT]"}, "SPIN_BOX_LIST", id="int-default"),
        pytest.param({"name": "X", "type": "LIST[FLOAT]"}, "SPIN_BOX_LIST", id="float-default"),
        pytest.param(
            {"name": "X", "type": "LIST[INT]", "item": {"allowedValues": [1, 2]}},
            "SPIN_BOX_LIST",
            id="item-allowed-values-is-not-dropdown",
        ),
        pytest.param(
            {"name": "X", "type": "LIST[FLOAT]", "userInterface": {"control": "HIDDEN"}},
            "HIDDEN",
            id="hidden",
        ),
    ],
)
def test_ui_control_for_list_number(definition: Any, expected: str) -> None:
    assert get_ui_control_for_parameter_definition(definition) == expected


@pytest.mark.parametrize("list_type", ["LIST[INT]", "LIST[FLOAT]"])
@pytest.mark.parametrize("control", ["SPIN_BOX", "LINE_EDIT_LIST", "LINE_EDIT", "DROPDOWN_LIST"])
def test_ui_control_for_list_number_unsupported(list_type: str, control: str) -> None:
    definition: Any = {"name": "X", "type": list_type, "userInterface": {"control": control}}
    with pytest.raises(DeadlineOperationError, match="unsupported control"):
        get_ui_control_for_parameter_definition(definition)


@pytest.mark.parametrize(
    "other_type", ["STRING", "PATH", "INT", "FLOAT", "BOOL", "RANGE_EXPR", "LIST[STRING]"]
)
def test_spin_box_list_control_rejected_for_other_types(other_type: str) -> None:
    definition: Any = {
        "name": "X",
        "type": other_type,
        "userInterface": {"control": "SPIN_BOX_LIST"},
    }
    with pytest.raises(DeadlineOperationError, match="unsupported control 'SPIN_BOX_LIST'"):
        get_ui_control_for_parameter_definition(definition)


@pytest.mark.parametrize(
    "value",
    [pytest.param([1, 2, 3], id="list"), pytest.param("[1, 2, 3]", id="json-string")],
)
def test_split_parameter_args_list_int(value: Any) -> None:
    params: list[JobParameter] = [
        {"name": "Frames", "type": "LIST[INT]", "value": value},
        {"name": "Big", "type": "LIST[INT]", "value": [-INT64_MAX - 1, INT64_MAX]},
        {"name": "None", "type": "LIST[INT]", "value": []},
    ]
    _, job_params = submission.split_parameter_args(params, "test_bundle")
    assert job_params == {
        "Frames": {"intList": ["1", "2", "3"]},
        "Big": {"intList": [str(-INT64_MAX - 1), str(INT64_MAX)]},
        "None": {"intList": []},
    }


def test_split_parameter_args_list_float() -> None:
    params: list[JobParameter] = [
        {"name": "Scales", "type": "LIST[FLOAT]", "value": [0.1, 2, -1.5, 1e16, 1e-7]},
    ]
    _, job_params = submission.split_parameter_args(params, "test_bundle")
    items = job_params["Scales"]["floatList"]
    assert items == ["0.1", "2.0", "-1.5", "1e+16", "1e-07"]
    # Every item matches the CreateJob FloatString pattern and round-trips exactly.
    import re

    pattern = re.compile(r"[-]?(0|[1-9][0-9]*)([.][0-9]+)?([eE][+-]?[0-9]+)?")
    assert all(pattern.fullmatch(item) and 1 <= len(item) <= 26 for item in items)
    assert [float(item) for item in items] == [0.1, 2.0, -1.5, 1e16, 1e-7]


@pytest.mark.parametrize(
    ("list_type", "value", "match"),
    [
        pytest.param("LIST[INT]", "1-10", "not a JSON array of integers", id="int-not-json"),
        pytest.param("LIST[INT]", [1.5], "item 0 is 1.5", id="int-float-item"),
        pytest.param("LIST[FLOAT]", ["0.5"], "item 0 is '0.5'", id="float-str-item"),
    ],
)
def test_split_parameter_args_rejects_invalid_list_number(
    list_type: str, value: Any, match: str
) -> None:
    params: list[JobParameter] = [{"name": "Values", "type": list_type, "value": value}]
    with pytest.raises(DeadlineOperationError, match=f"(?s){match}.*From job bundle:\ntest_bundle"):
        submission.split_parameter_args(params, "test_bundle")


LIST_NUMBER_TEMPLATE = """
specificationVersion: jobtemplate-2023-09
extensions:
- EXPR
name: ListJob
parameterDefinitions:
- name: Frames
  type: list[int]
  default: [1, 5, 10]
  item:
    minValue: 1
- name: Scales
  type: List[Float]
  default: [0.5, 2]
steps:
- name: Render
  parameterSpace:
    taskParameterDefinitions:
    - name: Frame
      type: INT
      range: "{{Param.Frames}}"
  script:
    actions:
      onRun:
        command: echo
        args: ["{{Task.Param.Frame}}"]
"""


def _write_bundle(bundle_dir: str, template: str, parameter_values: Any = None) -> None:
    with open(os.path.join(bundle_dir, "template.yaml"), "w", encoding="utf8") as f:
        f.write(template)
    if parameter_values is not None:
        with open(os.path.join(bundle_dir, "parameter_values.json"), "w", encoding="utf8") as f:
            json.dump({"parameterValues": parameter_values}, f)


def test_read_job_bundle_parameters_list_number(fresh_deadline_config, temp_job_bundle_dir) -> None:
    _write_bundle(
        temp_job_bundle_dir,
        LIST_NUMBER_TEMPLATE,
        [{"name": "Frames", "value": [3, 3, 7]}],
    )

    frames, scales = read_job_bundle_parameters(temp_job_bundle_dir)

    # The EXPR extension makes the type name case-insensitive.
    assert frames["type"] == "LIST[INT]"
    assert frames["value"] == [3, 3, 7]
    assert scales["type"] == "LIST[FLOAT]"
    assert scales["default"] == [0.5, 2]


def test_read_job_bundle_parameters_list_int_invalid_default(
    fresh_deadline_config, temp_job_bundle_dir
) -> None:
    _write_bundle(temp_job_bundle_dir, LIST_NUMBER_TEMPLATE.replace("[1, 5, 10]", "[1, 0]"))

    with pytest.raises(ValueError, match="item 1 0 is less than item minValue 1"):
        read_job_bundle_parameters(temp_job_bundle_dir)


def test_merge_queue_job_parameters_list_int_item_mismatch() -> None:
    queue_param: JobParameter = {"name": "Frames", "type": "LIST[INT]", "item": {"minValue": 0}}
    job_param: JobParameter = {"name": "Frames", "type": "LIST[INT]", "item": {"minValue": 1}}

    with pytest.raises(
        DeadlineOperationError, match="Frames: differences for fields \"\\['item'\\]\""
    ):
        merge_queue_job_parameters(job_parameters=[job_param], queue_parameters=[queue_param])


def test_parameter_definition_difference_list_number_items() -> None:
    lhs: Any = {
        "name": "S",
        "type": "LIST[FLOAT]",
        "item": {"allowedValues": [0.5, 1.0], "maxValue": 1.0},
    }
    rhs: Any = {
        "name": "S",
        "type": "LIST[FLOAT]",
        "item": {"maxValue": 1.0, "allowedValues": [1.0, 0.5]},
    }
    assert parameter_definition_difference(lhs, rhs) == []
    narrower: Any = {**rhs, "item": {"allowedValues": [0.5, 1.0]}}
    assert parameter_definition_difference(lhs, narrower) == ["item"]
    # A LIST[INT] and LIST[FLOAT] of the same name are different types.
    other_type: Any = {**rhs, "type": "LIST[INT]"}
    assert "type" in parameter_definition_difference(lhs, other_type)


@pytest.mark.parametrize("list_type", ["LIST[INT]", "list[float]"])
def test_bundle_browser_labels_number_lists(list_type: str) -> None:
    try:
        from deadline.client.ui.dialogs.job_bundle_browser_dialog import _friendly_param_type
    except ImportError:
        pytest.skip("GUI dependencies are not installed")
    assert _friendly_param_type(list_type) == "Number list"
