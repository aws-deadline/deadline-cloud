# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Test cases for LIST[STRING] job parameters: definition and value validation, UI control
selection, bundle loading, and CreateJob request formatting."""

from __future__ import annotations

import json
import os
from typing import Any

import pytest

from deadline.client.exceptions import DeadlineOperationError
from deadline.client.job_bundle import submission
from deadline.client.job_bundle._repository import extract_bundle_info
from deadline.client.job_bundle.parameters import (
    JobParameter,
    get_ui_control_for_parameter_definition,
    merge_queue_job_parameters,
    parameter_definition_difference,
    read_job_bundle_parameters,
    validate_job_parameter,
    validate_job_parameter_value,
)

# The valid parameter definitions from the OpenJD conformance test
# conformance-tests/2023-09/EXPR/job_templates/2.11--list-string-param.yaml
CONFORMANCE_VALID_DEFINITIONS: list[dict[str, Any]] = [
    {"name": "Minimal", "type": "LIST[STRING]"},
    {"name": "WithDefault", "type": "LIST[STRING]", "default": ["a", "b", "c"]},
    {"name": "EmptyDefault", "type": "LIST[STRING]", "default": []},
    {"name": "SingleElement", "type": "LIST[STRING]", "default": ["only"]},
    {"name": "LongList", "type": "LIST[STRING]", "default": list("abcdefghijklmnopqrst")},
    {
        "name": "WithMinMax",
        "type": "LIST[STRING]",
        "default": ["a", "b"],
        "minLength": 1,
        "maxLength": 50,
    },
    {"name": "MinOnly", "type": "LIST[STRING]", "default": ["a"], "minLength": 0},
    {"name": "MaxOnly", "type": "LIST[STRING]", "default": ["a", "b", "c"], "maxLength": 100},
    {
        "name": "ItemAllowed",
        "type": "LIST[STRING]",
        "default": ["alpha", "beta"],
        "item": {"allowedValues": ["alpha", "beta", "gamma"]},
    },
    {
        "name": "ItemLength",
        "type": "LIST[STRING]",
        "default": ["hi", "bye"],
        "item": {"minLength": 1, "maxLength": 10},
    },
    {
        "name": "FullyConstrained",
        "type": "LIST[STRING]",
        "default": ["hello", "world"],
        "minLength": 1,
        "maxLength": 5,
        "item": {"allowedValues": ["hello", "world", "foo"], "minLength": 3, "maxLength": 5},
    },
    {
        "name": "WithDescription",
        "type": "LIST[STRING]",
        "default": ["x"],
        "description": "A list of string values",
    },
    {
        "name": "WithLineEditList",
        "type": "LIST[STRING]",
        "default": ["a"],
        "userInterface": {"control": "LINE_EDIT_LIST", "label": "Items", "groupLabel": "Options"},
    },
    {
        "name": "WithHidden",
        "type": "LIST[STRING]",
        "default": ["a"],
        "userInterface": {"control": "HIDDEN"},
    },
]


@pytest.mark.parametrize(
    "definition", [pytest.param(d, id=d["name"]) for d in CONFORMANCE_VALID_DEFINITIONS]
)
def test_validate_job_parameter_list_string_valid(definition: dict[str, Any]) -> None:
    # Does not raise.
    validate_job_parameter(definition)


# The invalid job templates from the OpenJD conformance tests
# conformance-tests/2023-09/EXPR/job_templates/2.11--list-string-*.invalid.yaml
@pytest.mark.parametrize(
    ("definition", "error_type", "match"),
    [
        pytest.param(
            {"type": "LIST[STRING]", "default": "a"},
            TypeError,
            'got str for "default" but type "LIST\\[STRING\\]" expects list',
            id="scalar-not-list",
        ),
        pytest.param(
            {"type": "LIST[STRING]", "default": [123]},
            TypeError,
            "item 0 is 123",
            id="wrong-item-type",
        ),
        pytest.param(
            {"type": "LIST[STRING]", "default": [], "minLength": 1},
            ValueError,
            "has 0 items but minLength is 1",
            id="too-short",
        ),
        pytest.param(
            {"type": "LIST[STRING]", "default": ["a", "b", "c"], "maxLength": 2},
            ValueError,
            "has 3 items but at most 2 are allowed",
            id="too-long",
        ),
        pytest.param(
            {"type": "LIST[STRING]", "default": [""], "item": {"minLength": 1}},
            ValueError,
            "item 0 '' is shorter than item minLength 1",
            id="item-too-short",
        ),
        pytest.param(
            {"type": "LIST[STRING]", "default": ["abcdef"], "item": {"maxLength": 3}},
            ValueError,
            "item 0 'abcdef' is longer than the maximum of 3 characters",
            id="item-too-long",
        ),
        pytest.param(
            {"type": "LIST[STRING]", "default": ["x"], "item": {"allowedValues": ["a", "b"]}},
            ValueError,
            "item 0 'x' is not an allowed value",
            id="item-not-in-allowed",
        ),
    ],
)
def test_validate_job_parameter_list_string_invalid_default(
    definition: dict[str, Any], error_type: type, match: str
) -> None:
    with pytest.raises(error_type, match=match) as ctx:
        validate_job_parameter({"name": "Items", **definition})
    # Constraint violations say they came from the default rather than a submitted value.
    if isinstance(definition["default"], list):
        assert str(ctx.value).startswith('In "default": ')


@pytest.mark.parametrize(
    ("field", "field_value"),
    [
        pytest.param("allowedValues", ["a", "b"], id="allowedValues"),
        pytest.param("minValue", 0, id="minValue"),
        pytest.param("maxValue", 1, id="maxValue"),
        pytest.param("objectType", "FILE", id="objectType"),
        pytest.param("dataFlow", "IN", id="dataFlow"),
    ],
)
def test_validate_job_parameter_list_string_disallowed_fields(field: str, field_value: Any) -> None:
    with pytest.raises(ValueError) as ctx:
        validate_job_parameter({"name": "foo", "type": "LIST[STRING]", field: field_value})
    assert (
        str(ctx.value)
        == f'Job parameter "foo" has "{field}" but type "LIST[STRING]" does not support it'
    )


@pytest.mark.parametrize(
    ("item", "error_type", "match"),
    [
        pytest.param(["a"], TypeError, 'got list for "item" but expected dict', id="not-dict"),
        pytest.param(
            {"minValue": 1}, ValueError, '"item" -> "minValue" but type', id="unknown-field"
        ),
        pytest.param(
            {"allowedValues": "a"},
            TypeError,
            '"item" -> "allowedValues" but expected list',
            id="allowed-not-list",
        ),
        pytest.param(
            {"allowedValues": []}, ValueError, 'empty "item" -> "allowedValues"', id="allowed-empty"
        ),
        pytest.param(
            {"allowedValues": ["a", 1]},
            TypeError,
            '"item" -> "allowedValues" \\[1\\] but expected str',
            id="allowed-non-str",
        ),
        pytest.param(
            {"minLength": "1"}, TypeError, '"item" -> "minLength" but expected int', id="min-str"
        ),
        pytest.param(
            {"maxLength": True}, TypeError, '"item" -> "maxLength" but expected int', id="max-bool"
        ),
        pytest.param(
            {"minLength": -1}, ValueError, '"item" -> "minLength" but the value must be non-neg'
        ),
    ],
)
def test_validate_job_parameter_list_string_invalid_item(
    item: Any, error_type: type, match: str
) -> None:
    with pytest.raises(error_type, match=match):
        validate_job_parameter({"name": "foo", "type": "LIST[STRING]", "item": item})


@pytest.mark.parametrize(
    ("definition", "match"),
    [
        pytest.param(
            {"minLength": 3, "maxLength": 2},
            '"minLength" 3 greater than the maximum item count of 2',
            id="min-over-max",
        ),
        pytest.param(
            {"minLength": 65},
            '"minLength" 65 greater than the maximum item count of 64',
            id="min-over-cap",
        ),
        pytest.param(
            {"item": {"minLength": 5, "maxLength": 4}},
            '"item" -> "minLength" 5 greater than the maximum length of 4',
            id="item-min-over-max",
        ),
        pytest.param(
            {"item": {"minLength": 1025}},
            '"item" -> "minLength" 1025 greater than the maximum length of 1024',
            id="item-min-over-cap",
        ),
        pytest.param(
            {"item": {"allowedValues": ["ok", "toolong"], "maxLength": 4}},
            '"item" -> "allowedValues" \\[1\\] of length 7, outside the length range 0-4',
            id="allowed-too-long",
        ),
        pytest.param(
            {"item": {"allowedValues": ["a", "abc"], "minLength": 2}},
            '"item" -> "allowedValues" \\[0\\] of length 1, outside the length range 2-1024',
            id="allowed-too-short",
        ),
        pytest.param(
            {"item": {"allowedValues": ["x" * 1025]}},
            '"item" -> "allowedValues" \\[0\\] of length 1025, outside the length range 0-1024',
            id="allowed-over-cap",
        ),
    ],
)
def test_validate_job_parameter_list_string_unsatisfiable(definition: Any, match: str) -> None:
    """Constraints that no value could satisfy are rejected at bundle load."""
    with pytest.raises(ValueError, match=match):
        validate_job_parameter({"name": "Items", "type": "LIST[STRING]", **definition})


@pytest.mark.parametrize(
    "definition",
    [
        pytest.param({"minLength": 2, "maxLength": 2}, id="equal-lengths"),
        pytest.param({"minLength": 64}, id="min-at-cap"),
        pytest.param({"maxLength": 0}, id="always-empty"),
        pytest.param({"item": {"minLength": 3, "maxLength": 3}}, id="item-equal-lengths"),
        pytest.param({"item": {"allowedValues": ["abc"], "minLength": 3}}, id="allowed-at-min"),
    ],
)
def test_validate_job_parameter_list_string_boundary_constraints(definition: Any) -> None:
    # Does not raise.
    validate_job_parameter({"name": "Items", "type": "LIST[STRING]", **definition})


def test_validate_job_parameter_item_rejected_for_scalar_types() -> None:
    with pytest.raises(ValueError, match='has "item" but type "STRING" does not support it'):
        validate_job_parameter({"name": "foo", "type": "STRING", "item": {"minLength": 1}})


@pytest.mark.parametrize("scalar_type", ["STRING", "PATH", "INT", "FLOAT", "RANGE_EXPR"])
def test_validate_job_parameter_value_rejects_list_for_scalar_types(scalar_type: str) -> None:
    with pytest.raises(TypeError, match=f"has type {scalar_type} but got"):
        validate_job_parameter_value({"name": "foo", "type": scalar_type}, ["1"])


LIST_PARAM: JobParameter = {"name": "Cameras", "type": "LIST[STRING]"}


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(["main", "closeup"], ["main", "closeup"], id="list"),
        pytest.param([], [], id="empty-list"),
        pytest.param(["", "a b", "Ã¼"], ["", "a b", "Ã¼"], id="unusual-items"),
        pytest.param(["dup", "dup"], ["dup", "dup"], id="duplicates-kept"),
        pytest.param('["main", "closeup"]', ["main", "closeup"], id="json-string"),
        pytest.param("[]", [], id="json-empty"),
        pytest.param(' [ "a" ] ', ["a"], id="json-whitespace"),
    ],
)
def test_validate_job_parameter_value_list_string(value: Any, expected: list[str]) -> None:
    result = validate_job_parameter_value(LIST_PARAM, value)
    assert result == expected
    assert type(result) is list


def test_validate_job_parameter_value_list_string_returns_copy() -> None:
    value = ["a"]
    result = validate_job_parameter_value(LIST_PARAM, value)
    assert result == value and result is not value


@pytest.mark.parametrize(
    ("value", "error_type", "match"),
    [
        pytest.param("main", ValueError, "is not a JSON array of strings", id="bare-string"),
        pytest.param("", ValueError, "is not a JSON array of strings", id="empty-string"),
        pytest.param(
            '"main"', ValueError, 'got value "main" which is not a JSON array', id="json-str"
        ),
        pytest.param('{"a": 1}', ValueError, "which is not a JSON array", id="json-object"),
        pytest.param("[1, 2]", TypeError, "item 0 is 1", id="json-ints"),
        pytest.param(["a", None], TypeError, "item 1 is None", id="none-item"),
        pytest.param(["a", 5], TypeError, "item 1 is 5", id="int-item"),
        pytest.param(("a",), TypeError, "has type LIST\\[STRING\\]", id="tuple"),
        pytest.param(5, TypeError, "has type LIST\\[STRING\\]", id="int"),
        pytest.param(None, TypeError, "has type LIST\\[STRING\\]", id="none"),
    ],
)
def test_validate_job_parameter_value_list_string_invalid(
    value: Any, error_type: type, match: str
) -> None:
    with pytest.raises(error_type, match=match):
        validate_job_parameter_value(LIST_PARAM, value)


def test_validate_job_parameter_value_list_string_constraints() -> None:
    param: JobParameter = {
        "name": "Cameras",
        "type": "LIST[STRING]",
        "minLength": 1,
        "maxLength": 3,
        "item": {"allowedValues": ["main", "closeup", "wide"], "minLength": 4, "maxLength": 7},
    }
    assert validate_job_parameter_value(param, ["main", "wide"]) == ["main", "wide"]
    with pytest.raises(ValueError, match="has 0 items but minLength is 1"):
        validate_job_parameter_value(param, [])
    with pytest.raises(ValueError, match="has 4 items but at most 3 are allowed"):
        validate_job_parameter_value(param, ["main"] * 4)
    with pytest.raises(ValueError, match="item 1 'top' is shorter than item minLength 4"):
        validate_job_parameter_value(param, ["main", "top"])
    with pytest.raises(ValueError, match="item 0 'closeups' is longer than the maximum of 7"):
        validate_job_parameter_value(param, ["closeups"])
    with pytest.raises(ValueError, match="item 0 'side' is not an allowed value"):
        validate_job_parameter_value(param, ["side"])


def test_validate_job_parameter_value_list_string_service_caps() -> None:
    """Without explicit limits, CreateJob's stringList caps of 64 items of at most 1024
    characters still apply, so the client rejects what the service would."""
    validate_job_parameter_value(LIST_PARAM, ["x"] * 64)
    with pytest.raises(ValueError, match="has 65 items but at most 64 are allowed"):
        validate_job_parameter_value(LIST_PARAM, ["x"] * 65)
    # An explicit maxLength above the service cap does not raise it.
    with pytest.raises(ValueError, match="at most 64 are allowed"):
        validate_job_parameter_value({**LIST_PARAM, "maxLength": 100}, ["x"] * 65)

    validate_job_parameter_value(LIST_PARAM, ["x" * 1024])
    with pytest.raises(ValueError, match=r"'x{40}\.\.\.' is longer than the maximum of 1024"):
        validate_job_parameter_value(LIST_PARAM, ["x" * 1025])


@pytest.mark.parametrize(
    ("definition", "expected"),
    [
        ({"name": "X", "type": "LIST[STRING]"}, "LINE_EDIT_LIST"),
        (
            {"name": "X", "type": "LIST[STRING]", "userInterface": {"control": "LINE_EDIT_LIST"}},
            "LINE_EDIT_LIST",
        ),
        ({"name": "X", "type": "LIST[STRING]", "userInterface": {"control": "HIDDEN"}}, "HIDDEN"),
    ],
)
def test_ui_control_for_list_string(definition: Any, expected: str) -> None:
    assert get_ui_control_for_parameter_definition(definition) == expected


@pytest.mark.parametrize(
    "control", ["LINE_EDIT", "MULTILINE_EDIT", "DROPDOWN_LIST", "SPIN_BOX", "CHECK_BOX"]
)
def test_ui_control_for_list_string_unsupported(control: str) -> None:
    definition: Any = {"name": "X", "type": "LIST[STRING]", "userInterface": {"control": control}}
    with pytest.raises(DeadlineOperationError, match="unsupported control"):
        get_ui_control_for_parameter_definition(definition)


@pytest.mark.parametrize("scalar_type", ["STRING", "PATH", "INT", "FLOAT", "BOOL", "RANGE_EXPR"])
def test_line_edit_list_control_rejected_for_scalar_types(scalar_type: str) -> None:
    definition: Any = {
        "name": "X",
        "type": scalar_type,
        "userInterface": {"control": "LINE_EDIT_LIST"},
    }
    with pytest.raises(DeadlineOperationError, match="unsupported control 'LINE_EDIT_LIST'"):
        get_ui_control_for_parameter_definition(definition)


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(["main", "closeup"], id="list"),
        pytest.param('["main", "closeup"]', id="json-string"),
    ],
)
def test_split_parameter_args_list_string(value: Any) -> None:
    params: list[JobParameter] = [
        {"name": "Cameras", "type": "LIST[STRING]", "value": value},
        {"name": "None", "type": "LIST[STRING]", "value": []},
    ]
    _, job_params = submission.split_parameter_args(params, "test_bundle")
    assert job_params == {
        "Cameras": {"stringList": ["main", "closeup"]},
        "None": {"stringList": []},
    }


@pytest.mark.parametrize(
    ("value", "match"),
    [
        pytest.param("main", "not a JSON array of strings", id="not-json"),
        pytest.param([1], "item 0 is 1", id="non-str-item"),
        pytest.param(["x"] * 65, "at most 64", id="too-many"),
    ],
)
def test_split_parameter_args_rejects_invalid_list_string(value: Any, match: str) -> None:
    params: list[JobParameter] = [{"name": "Cameras", "type": "LIST[STRING]", "value": value}]
    with pytest.raises(DeadlineOperationError, match=f"(?s){match}.*From job bundle:\ntest_bundle"):
        submission.split_parameter_args(params, "test_bundle")


LIST_STRING_TEMPLATE = """
specificationVersion: jobtemplate-2023-09
extensions:
- EXPR
name: ListJob
parameterDefinitions:
- name: Cameras
  type: list[string]
  default: ["main", "closeup"]
  item:
    minLength: 1
steps:
- name: Render
  parameterSpace:
    taskParameterDefinitions:
    - name: Camera
      type: STRING
      range: "{{Param.Cameras}}"
  script:
    actions:
      onRun:
        command: echo
        args: ["{{Task.Param.Camera}}"]
"""


def _write_bundle(bundle_dir: str, template: str, parameter_values: Any = None) -> None:
    with open(os.path.join(bundle_dir, "template.yaml"), "w", encoding="utf8") as f:
        f.write(template)
    if parameter_values is not None:
        with open(os.path.join(bundle_dir, "parameter_values.json"), "w", encoding="utf8") as f:
            json.dump({"parameterValues": parameter_values}, f)


def test_read_job_bundle_parameters_list_string(fresh_deadline_config, temp_job_bundle_dir) -> None:
    _write_bundle(
        temp_job_bundle_dir,
        LIST_STRING_TEMPLATE,
        [{"name": "Cameras", "value": ["wide", "top", "wide"]}],
    )

    (param,) = read_job_bundle_parameters(temp_job_bundle_dir)

    # The EXPR extension makes the type name case-insensitive.
    assert param["type"] == "LIST[STRING]"
    assert param["default"] == ["main", "closeup"]
    assert param["value"] == ["wide", "top", "wide"]


def test_read_job_bundle_parameters_list_string_invalid_default(
    fresh_deadline_config, temp_job_bundle_dir
) -> None:
    _write_bundle(
        temp_job_bundle_dir,
        LIST_STRING_TEMPLATE.replace('["main", "closeup"]', '["main", ""]'),
    )

    with pytest.raises(ValueError, match="item 1 '' is shorter than item minLength 1"):
        read_job_bundle_parameters(temp_job_bundle_dir)


def test_merge_queue_job_parameters_list_string_item_mismatch() -> None:
    """A queue and job bundle LIST[STRING] parameter with different item constraints conflict."""
    queue_param: JobParameter = {"name": "Cameras", "type": "LIST[STRING]", "default": []}
    job_param: JobParameter = {
        "name": "Cameras",
        "type": "LIST[STRING]",
        "item": {"minLength": 1},
    }

    with pytest.raises(
        DeadlineOperationError, match="Cameras: differences for fields \"\\['item'\\]\""
    ):
        merge_queue_job_parameters(job_parameters=[job_param], queue_parameters=[queue_param])

    merged = merge_queue_job_parameters(
        job_parameters=[{"name": "Cameras", "value": ["a"]}], queue_parameters=[queue_param]
    )
    assert merged == [{**queue_param, "value": ["a"]}]


@pytest.mark.parametrize(
    ("queue_item", "job_item"),
    [
        pytest.param(
            {"allowedValues": ["a", "b"], "minLength": 1},
            {"minLength": 1, "allowedValues": ["b", "a"]},
            id="allowed-values-reordered",
        ),
        pytest.param(None, {}, id="absent-vs-empty"),
    ],
)
def test_merge_queue_job_parameters_list_string_equivalent_items(queue_item, job_item) -> None:
    """item constraints that differ only in allowedValues order, or an empty "item" versus
    none, are the same definition, like the top-level allowedValues."""
    queue_param: Any = {"name": "Cameras", "type": "LIST[STRING]", "default": ["a"]}
    if queue_item is not None:
        queue_param["item"] = queue_item
    job_param: Any = {"name": "Cameras", "type": "LIST[STRING]", "item": job_item}

    merge_queue_job_parameters(job_parameters=[job_param], queue_parameters=[queue_param])
    assert parameter_definition_difference(queue_param, job_param) == []
    narrower: Any = {**queue_param, "item": {"allowedValues": ["a"]}}
    assert parameter_definition_difference(narrower, job_param) == ["item"]


def test_bundle_preview_shows_list_values_as_json() -> None:
    template = {
        "parameterDefinitions": [
            {"name": "Cameras", "type": "LIST[STRING]", "default": ["main", "closeup"]},
            {"name": "Layers", "type": "LIST[STRING]", "default": ["beauty"]},
        ]
    }
    info = extract_bundle_info(
        template, "/bundle", {"parameterValues": [{"name": "Layers", "value": ["a", "Ã¼"]}]}
    )
    display = {p["name"]: p["_display_value"] for p in info.parameters}
    assert display == {"Cameras": '["main", "closeup"]', "Layers": '["a", "Ã¼"]'}


@pytest.mark.parametrize(
    "yaml_value",
    [
        pytest.param("&loop [*loop]", id="self-referencing-anchor"),
        pytest.param("[2026-09-29]", id="yaml-date"),
    ],
)
def test_bundle_preview_survives_lists_json_cannot_encode(yaml_value: str) -> None:
    """A parameter_values file comes from a possibly untrusted bundle, and YAML can hold
    lists that JSON can't encode. The preview falls back to str() instead of raising."""
    import yaml

    value = yaml.safe_load(yaml_value)
    template = {"parameterDefinitions": [{"name": "Cameras", "type": "LIST[STRING]"}]}
    info = extract_bundle_info(
        template, "/bundle", {"parameterValues": [{"name": "Cameras", "value": value}]}
    )
    assert info.parameters[0]["_display_value"] == str(value)
