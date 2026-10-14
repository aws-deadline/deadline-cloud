# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Test cases for LIST[BOOL] job parameters: definition and value validation, UI control
selection, bundle loading, queue parameter merging, and CreateJob request formatting."""

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

# The parameter definitions from the OpenJD conformance test
# conformance-tests/2023-09/EXPR/job_templates/2.15--list-bool-param.yaml
CONFORMANCE_VALID_DEFINITIONS: list[dict[str, Any]] = [
    {"name": "Minimal", "type": "LIST[BOOL]"},
    {"name": "WithDefault", "type": "LIST[BOOL]", "default": [True, False, True]},
    {"name": "EmptyDefault", "type": "LIST[BOOL]", "default": []},
    {"name": "SingleElement", "type": "LIST[BOOL]", "default": [True]},
    {"name": "IntValues", "type": "LIST[BOOL]", "default": [1, 0, 1, 0]},
    {"name": "FloatValues", "type": "LIST[BOOL]", "default": [1.0, 0.0]},
    {"name": "StringTrueFalse", "type": "LIST[BOOL]", "default": ["true", "false"]},
    {"name": "StringYesNo", "type": "LIST[BOOL]", "default": ["yes", "no"]},
    {"name": "StringOnOff", "type": "LIST[BOOL]", "default": ["on", "off"]},
    {"name": "String10", "type": "LIST[BOOL]", "default": ["1", "0"]},
    {
        "name": "MixedCaseStrings",
        "type": "LIST[BOOL]",
        "default": ["TRUE", "False", "YES", "no", "On", "OFF"],
    },
    {"name": "MixedTypes", "type": "LIST[BOOL]", "default": [True, 1, "yes", 0.0, "off"]},
    {
        "name": "WithMinMax",
        "type": "LIST[BOOL]",
        "default": [True, False],
        "minLength": 1,
        "maxLength": 50,
    },
    {"name": "LongList", "type": "LIST[BOOL]", "default": [True, False] * 10},
    {
        "name": "WithDescription",
        "type": "LIST[BOOL]",
        "default": [True],
        "description": "A list of booleans",
    },
    {
        "name": "WithCheckBoxList",
        "type": "LIST[BOOL]",
        "default": [True],
        "userInterface": {"control": "CHECK_BOX_LIST", "label": "Flags", "groupLabel": "Options"},
    },
    {
        "name": "WithHidden",
        "type": "LIST[BOOL]",
        "default": [True],
        "userInterface": {"control": "HIDDEN"},
    },
]


@pytest.mark.parametrize(
    "definition", [pytest.param(d, id=d["name"]) for d in CONFORMANCE_VALID_DEFINITIONS]
)
def test_validate_job_parameter_list_bool_valid(definition: dict[str, Any]) -> None:
    # Does not raise.
    validate_job_parameter(definition)


# The invalid job templates from the OpenJD conformance tests
# conformance-tests/2023-09/EXPR/job_templates/2.15--list-bool-*.invalid.yaml
@pytest.mark.parametrize(
    ("definition", "error_type", "match"),
    [
        pytest.param(
            {"default": [True, False], "item": {"allowedValues": [True, False]}},
            ValueError,
            'has "item" but type "LIST\\[BOOL\\]" does not support it',
            id="item-constraints",
        ),
        pytest.param(
            {"default": True},
            TypeError,
            'got bool for "default" but type "LIST\\[BOOL\\]" expects list',
            id="scalar-not-list",
        ),
        pytest.param(
            {"default": [True, False, True], "maxLength": 2},
            ValueError,
            'In "default": .*has 3 items but at most 2 are allowed',
            id="too-long",
        ),
        pytest.param(
            {"default": [], "minLength": 1},
            ValueError,
            'In "default": .*has 0 items but minLength is 1',
            id="too-short",
        ),
        pytest.param(
            {"default": ["maybe", "perhaps"]},
            ValueError,
            "In \"default\": .*item 0 is 'maybe' which is not boolean",
            id="wrong-item-type",
        ),
    ],
)
def test_validate_job_parameter_list_bool_conformance_invalid(
    definition: dict[str, Any], error_type: type, match: str
) -> None:
    with pytest.raises(error_type, match=match):
        validate_job_parameter({"name": "Flags", "type": "LIST[BOOL]", **definition})


@pytest.mark.parametrize(
    ("field", "field_value"),
    [
        ("allowedValues", [True]),
        ("minValue", 0),
        ("maxValue", 1),
        ("objectType", "FILE"),
        ("dataFlow", "IN"),
    ],
)
def test_validate_job_parameter_list_bool_disallowed_fields(field: str, field_value: Any) -> None:
    with pytest.raises(
        ValueError, match=f'has "{field}" but type "LIST\\[BOOL\\]" does not support it'
    ):
        validate_job_parameter({"name": "Flags", "type": "LIST[BOOL]", field: field_value})


def test_validate_job_parameter_list_bool_empty_item_rejected() -> None:
    """The schema has no "item" at all, so even an empty one is rejected."""
    with pytest.raises(ValueError, match='has "item" but type "LIST\\[BOOL\\]"'):
        validate_job_parameter({"name": "Flags", "type": "LIST[BOOL]", "item": {}})


def test_validate_job_parameter_list_bool_min_length_above_cap() -> None:
    with pytest.raises(ValueError, match="greater than the maximum item count of 512"):
        validate_job_parameter({"name": "Flags", "type": "LIST[BOOL]", "minLength": 513})


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param([True, False], [True, False], id="bools"),
        pytest.param([1, 0, 1.0, 0.0], [True, False, True, False], id="numbers"),
        pytest.param(
            ["true", "FALSE", "Yes", "no", "ON", "off", "1", "0"],
            [True, False, True, False, True, False, True, False],
            id="strings",
        ),
        pytest.param("[true, false]", [True, False], id="json"),
        pytest.param('[1, "no", "On"]', [True, False, True], id="json-mixed"),
        pytest.param("[]", [], id="json-empty"),
        pytest.param([], [], id="empty"),
    ],
)
def test_validate_job_parameter_value_list_bool(value: Any, expected: list) -> None:
    result = validate_job_parameter_value({"name": "Flags", "type": "LIST[BOOL]"}, value)
    assert result == expected
    assert all(type(v) is bool for v in result)  # type: ignore[union-attr]


@pytest.mark.parametrize(
    ("value", "error_type", "match"),
    [
        pytest.param(
            "true,false",
            ValueError,
            "not a JSON array of booleans, such as \\[true, false\\]",
            id="not-json",
        ),
        pytest.param("true", ValueError, "true which is not a JSON array", id="json-scalar"),
        pytest.param(True, TypeError, "got value True of type", id="scalar"),
        pytest.param(["maybe"], ValueError, "item 0 is 'maybe' which is not boolean", id="word"),
        pytest.param([2], ValueError, "item 0 is 2 which is not boolean", id="int-not-0-or-1"),
        pytest.param([0.5], ValueError, "item 0 is 0.5 which is not boolean", id="fraction"),
        pytest.param([""], ValueError, "item 0 is '' which is not boolean", id="empty-string"),
        pytest.param([None], TypeError, "item 0 is None which is not boolean", id="none"),
        pytest.param("[[true]]", TypeError, "item 0 is \\[True\\]", id="nested"),
        pytest.param([True] * 513, ValueError, "at most 512 are allowed", id="service-cap"),
    ],
)
def test_validate_job_parameter_value_list_bool_invalid(
    value: Any, error_type: type, match: str
) -> None:
    with pytest.raises(error_type, match=match):
        validate_job_parameter_value({"name": "Flags", "type": "LIST[BOOL]"}, value)


def test_validate_job_parameter_value_list_bool_length_constraints() -> None:
    definition: Any = {"name": "Flags", "type": "LIST[BOOL]", "minLength": 1, "maxLength": 2}
    assert validate_job_parameter_value(definition, [True]) == [True]
    with pytest.raises(ValueError, match="has 0 items but minLength is 1"):
        validate_job_parameter_value(definition, [])
    with pytest.raises(ValueError, match="has 3 items but at most 2 are allowed"):
        validate_job_parameter_value(definition, [True, False, True])


@pytest.mark.parametrize(
    ("definition", "expected"),
    [
        pytest.param({"name": "X", "type": "LIST[BOOL]"}, "CHECK_BOX_LIST", id="default"),
        pytest.param(
            {"name": "X", "type": "LIST[BOOL]", "userInterface": {"control": "CHECK_BOX_LIST"}},
            "CHECK_BOX_LIST",
            id="explicit",
        ),
        pytest.param(
            {"name": "X", "type": "LIST[BOOL]", "userInterface": {"control": "HIDDEN"}},
            "HIDDEN",
            id="hidden",
        ),
    ],
)
def test_ui_control_for_list_bool(definition: Any, expected: str) -> None:
    assert get_ui_control_for_parameter_definition(definition) == expected


@pytest.mark.parametrize(
    "control", ["CHECK_BOX", "LINE_EDIT_LIST", "SPIN_BOX_LIST", "LINE_EDIT", "DROPDOWN_LIST"]
)
def test_ui_control_for_list_bool_unsupported(control: str) -> None:
    definition: Any = {"name": "X", "type": "LIST[BOOL]", "userInterface": {"control": control}}
    with pytest.raises(DeadlineOperationError, match="unsupported control"):
        get_ui_control_for_parameter_definition(definition)


@pytest.mark.parametrize(
    "other_type",
    ["STRING", "PATH", "INT", "FLOAT", "BOOL", "RANGE_EXPR", "LIST[STRING]", "LIST[INT]"],
)
def test_check_box_list_control_rejected_for_other_types(other_type: str) -> None:
    definition: Any = {
        "name": "X",
        "type": other_type,
        "userInterface": {"control": "CHECK_BOX_LIST"},
    }
    with pytest.raises(DeadlineOperationError, match="unsupported control 'CHECK_BOX_LIST'"):
        get_ui_control_for_parameter_definition(definition)


@pytest.mark.parametrize(
    "value",
    [
        pytest.param([True, False, True], id="list"),
        pytest.param("[true, false, true]", id="json-string"),
        pytest.param(["yes", 0, "ON"], id="coerced"),
    ],
)
def test_split_parameter_args_list_bool(value: Any) -> None:
    params: list[JobParameter] = [
        {"name": "Flags", "type": "LIST[BOOL]", "value": value},
        {"name": "None", "type": "LIST[BOOL]", "value": []},
    ]
    _, job_params = submission.split_parameter_args(params, "test_bundle")
    assert job_params == {
        "Flags": {"boolList": ["true", "false", "true"]},
        "None": {"boolList": []},
    }


def test_split_parameter_args_list_bool_items_match_boolean_string_pattern() -> None:
    import re

    params: list[JobParameter] = [
        {"name": "Flags", "type": "LIST[BOOL]", "value": [True, "off", 1.0, "No"]}
    ]
    _, job_params = submission.split_parameter_args(params, "test_bundle")
    pattern = re.compile(
        r"([Tt][Rr][Uu][Ee]|[Ff][Aa][Ll][Ss][Ee]|[Yy][Ee][Ss]|[Nn][Oo]|[Oo][Nn]|[Oo][Ff][Ff]|[01])"
    )
    assert all(pattern.fullmatch(item) for item in job_params["Flags"]["boolList"])


@pytest.mark.parametrize(
    ("value", "match"),
    [
        pytest.param("true,false", "not a JSON array of booleans", id="not-json"),
        pytest.param(["maybe"], "item 0 is 'maybe' which is not boolean", id="word"),
        pytest.param([None], "item 0 is None which is not boolean", id="none"),
    ],
)
def test_split_parameter_args_rejects_invalid_list_bool(value: Any, match: str) -> None:
    params: list[JobParameter] = [{"name": "Flags", "type": "LIST[BOOL]", "value": value}]
    with pytest.raises(DeadlineOperationError, match=f"(?s){match}.*From job bundle:\ntest_bundle"):
        submission.split_parameter_args(params, "test_bundle")


LIST_BOOL_TEMPLATE = """
specificationVersion: jobtemplate-2023-09
extensions:
- EXPR
name: ListJob
parameterDefinitions:
- name: Flags
  type: list[bool]
  default: [true, "no", 1]
  maxLength: 4
steps:
- name: Render
  script:
    actions:
      onRun:
        command: echo
        args: ["{{Param.Flags}}"]
"""


def _write_bundle(bundle_dir: str, template: str, parameter_values: Any = None) -> None:
    with open(os.path.join(bundle_dir, "template.yaml"), "w", encoding="utf8") as f:
        f.write(template)
    if parameter_values is not None:
        with open(os.path.join(bundle_dir, "parameter_values.json"), "w", encoding="utf8") as f:
            json.dump({"parameterValues": parameter_values}, f)


def test_read_job_bundle_parameters_list_bool(fresh_deadline_config, temp_job_bundle_dir) -> None:
    _write_bundle(temp_job_bundle_dir, LIST_BOOL_TEMPLATE, [{"name": "Flags", "value": [False]}])

    (flags,) = read_job_bundle_parameters(temp_job_bundle_dir)

    # The EXPR extension makes the type name case-insensitive.
    assert flags["type"] == "LIST[BOOL]"
    assert flags["value"] == [False]
    # The template's default is kept as written; it is converted when it is used.
    assert flags["default"] == [True, "no", 1]


def test_read_job_bundle_parameters_list_bool_invalid_default(
    fresh_deadline_config, temp_job_bundle_dir
) -> None:
    _write_bundle(temp_job_bundle_dir, LIST_BOOL_TEMPLATE.replace('"no"', '"nope"'))

    with pytest.raises(ValueError, match="item 1 is 'nope' which is not boolean"):
        read_job_bundle_parameters(temp_job_bundle_dir)


def test_merge_queue_job_parameters_list_bool_type_mismatch() -> None:
    queue_param: JobParameter = {"name": "Flags", "type": "LIST[BOOL]"}
    job_param: JobParameter = {"name": "Flags", "type": "LIST[INT]"}

    with pytest.raises(
        DeadlineOperationError, match="Flags: differences for fields \"\\['type'\\]\""
    ):
        merge_queue_job_parameters(job_parameters=[job_param], queue_parameters=[queue_param])


def test_parameter_definition_difference_list_bool() -> None:
    lhs: Any = {"name": "F", "type": "LIST[BOOL]", "maxLength": 3, "default": [True]}
    rhs: Any = {"name": "F", "type": "LIST[BOOL]", "maxLength": 3, "default": [False]}
    assert parameter_definition_difference(lhs, rhs) == []
    longer: Any = {**rhs, "maxLength": 4}
    assert parameter_definition_difference(lhs, longer) == ["maxLength"]


def test_bundle_browser_labels_list_bool() -> None:
    try:
        from deadline.client.ui.dialogs.job_bundle_browser_dialog import _friendly_param_type
    except ImportError:
        pytest.skip("GUI dependencies are not installed")
    assert _friendly_param_type("list[bool]") == "Checkbox list"
