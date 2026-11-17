# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Test cases for LIST[LIST[INT]] job parameters: definition and value validation, UI control
selection, bundle loading, queue parameter merging, and CreateJob request formatting."""

from __future__ import annotations

import json
import os
import re
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from deadline.client import api
from deadline.client.api import _queue_parameters
from deadline.client.cli import main
from deadline.client.config import config_file
from deadline.client.exceptions import DeadlineOperationError
from deadline.client.job_bundle import submission
from deadline.client.job_bundle.parameters import (
    JobParameter,
    apply_job_parameters,
    get_ui_control_for_parameter_definition,
    merge_queue_job_parameters,
    parameter_definition_difference,
    read_job_bundle_parameters,
    validate_job_parameter,
    validate_job_parameter_value,
)
from deadline.client.job_bundle.submission import AssetReferences

from ..shared_constants import MOCK_FARM_ID, MOCK_QUEUE_ID
from ..testing_utilities import (
    MOCK_CREATE_JOB_RESPONSE,
    MOCK_GET_JOB_RESPONSE,
    patch_calls_for_create_job_from_job_bundle,
)

INT64_MAX = 2**63 - 1
INT64_MIN = -(2**63)
TYPE = "LIST[LIST[INT]]"
ESCAPED_TYPE = re.escape(TYPE)

# The parameter definitions from the OpenJD conformance test
# conformance-tests/2023-09/EXPR/job_templates/2.16--list-list-int-param.yaml
CONFORMANCE_VALID_DEFINITIONS: list[dict[str, Any]] = [
    {"name": "Minimal", "type": TYPE},
    {"name": "WithDefault", "type": TYPE, "default": [[1, 2], [3], [0, 1]]},
    {"name": "EmptyOuter", "type": TYPE, "default": []},
    {"name": "EmptyInner", "type": TYPE, "default": [[], [], []]},
    {"name": "SingleInner", "type": TYPE, "default": [[1, 2, 3]]},
    {"name": "SingleElements", "type": TYPE, "default": [[1], [2], [3]]},
    {"name": "Negatives", "type": TYPE, "default": [[-10, -1], [0], [1, 10]]},
    {"name": "LongOuter", "type": TYPE, "default": [[i] for i in range(1, 16)]},
    {"name": "LongInner", "type": TYPE, "default": [list(range(20))]},
    {"name": "OuterMinMax", "type": TYPE, "default": [[1], [2]], "minLength": 1, "maxLength": 50},
    {
        "name": "InnerMinMax",
        "type": TYPE,
        "default": [[1, 2, 3]],
        "item": {"minLength": 1, "maxLength": 20},
    },
    {
        "name": "InnerItemRange",
        "type": TYPE,
        "default": [[5, 10]],
        "item": {"item": {"minValue": 0, "maxValue": 100}},
    },
    {
        "name": "InnerItemAllowed",
        "type": TYPE,
        "default": [[1, 3]],
        "item": {"item": {"allowedValues": [1, 2, 3, 4, 5]}},
    },
    {
        "name": "FullyConstrained",
        "type": TYPE,
        "default": [[1, 2], [3, 4]],
        "minLength": 1,
        "maxLength": 10,
        "item": {"minLength": 1, "maxLength": 5, "item": {"minValue": 0, "maxValue": 100}},
    },
    {
        "name": "WithDescription",
        "type": TYPE,
        "default": [[1]],
        "description": "Adjacency list for task dependencies",
    },
    {"name": "WithHidden", "type": TYPE, "default": [[1]], "userInterface": {"control": "HIDDEN"}},
    # conformance-tests/2023-09/EXPR/job_templates/2.16--list-list-int-inner-item-int64-max.yaml
    {"name": "Matrix", "type": TYPE, "default": [[1, INT64_MAX], [INT64_MAX - 1]]},
    # conformance-tests/2023-09/EXPR/jobs/2.16--list-list-int-param-runtime.test.yaml
    {"name": "RuntimeMatrix", "type": TYPE, "default": [[1, 2], [3, 4], [5, 6]]},
]


@pytest.mark.parametrize(
    "definition", [pytest.param(d, id=d["name"]) for d in CONFORMANCE_VALID_DEFINITIONS]
)
def test_validate_job_parameter_list_list_int_valid(definition: dict[str, Any]) -> None:
    # Does not raise.
    validate_job_parameter(definition)


# The invalid job templates from the OpenJD conformance tests
# conformance-tests/2023-09/EXPR/job_templates/2.16--list-list-int-*.invalid.yaml
CONFORMANCE_INVALID_DEFINITIONS = [
    pytest.param(
        {"default": [[999]], "item": {"item": {"maxValue": 100}}},
        ValueError,
        "item 0 element 0 999 is greater than item -> item maxValue 100",
        id="inner-item-above-max",
    ),
    pytest.param(
        {"default": [[-5]], "item": {"item": {"minValue": 0}}},
        ValueError,
        "item 0 element 0 -5 is less than item -> item minValue 0",
        id="inner-item-below-min",
    ),
    pytest.param(
        {"default": [[99]], "item": {"item": {"allowedValues": [1, 2, 3]}}},
        ValueError,
        "item 0 element 0 99 is not an allowed value from \\(1, 2, 3\\)",
        id="inner-item-not-in-allowed",
    ),
    pytest.param(
        {"default": [[1, 2, 3, 4, 5]], "item": {"maxLength": 3}},
        ValueError,
        "item 0 has 5 elements but at most 3 are allowed",
        id="inner-too-long",
    ),
    pytest.param(
        {"default": [[]], "item": {"minLength": 1}},
        ValueError,
        "item 0 has 0 elements but item minLength is 1",
        id="inner-too-short",
    ),
    pytest.param(
        {"default": [[1, 2, 3], 3]},
        TypeError,
        "item 1 is 3 of type <class 'int'>, not a list of integers",
        id="ragged-scalar-in-outer",
    ),
    pytest.param(
        {"default": 42},
        TypeError,
        f'got int for "default" but type "{ESCAPED_TYPE}" expects list',
        id="scalar-not-list",
    ),
    pytest.param(
        {"default": [[1, "a"]]},
        TypeError,
        "item 0 element 1 is 'a'",
        id="string-in-inner",
    ),
    pytest.param(
        {"default": [[1], [2], [3]], "maxLength": 2},
        ValueError,
        "has 3 items but at most 2 are allowed",
        id="too-long",
    ),
    pytest.param(
        {"default": [], "minLength": 1},
        ValueError,
        "has 0 items but minLength is 1",
        id="too-short",
    ),
]


@pytest.mark.parametrize(("definition", "error_type", "match"), CONFORMANCE_INVALID_DEFINITIONS)
def test_validate_job_parameter_list_list_int_conformance_invalid(
    definition: dict[str, Any], error_type: type, match: str
) -> None:
    if not match.startswith("got "):
        match = f'In "default": .*{match}'
    with pytest.raises(error_type, match=match):
        validate_job_parameter({"name": "AdjList", "type": TYPE, **definition})


@pytest.mark.parametrize(
    ("default", "error_type", "match"),
    [
        pytest.param([[INT64_MAX + 1]], ValueError, "outside the 64-bit integer range", id="int64"),
        pytest.param([[INT64_MIN - 1]], ValueError, "outside the 64-bit integer range", id="neg"),
        pytest.param([[True]], TypeError, "item 0 element 0 is True", id="bool-element"),
        pytest.param([[1.0]], TypeError, "item 0 element 0 is 1.0", id="float-element"),
        pytest.param([["1"]], TypeError, "item 0 element 0 is '1'", id="int-like-string"),
        pytest.param([[None]], TypeError, "item 0 element 0 is None", id="none-element"),
        pytest.param([[[1]]], TypeError, "item 0 element 0 is \\[1\\]", id="too-deep"),
        pytest.param([1, 2], TypeError, "item 0 is 1 of type", id="flat-list"),
        pytest.param([(1, 2)], TypeError, "item 0 is \\(1, 2\\)", id="tuple-inner"),
        pytest.param([[0]] * 513, ValueError, "at most 512 are allowed", id="outer-service-cap"),
        pytest.param([[0] * 65], ValueError, "65 elements but at most 64", id="inner-cap"),
    ],
)
def test_validate_job_parameter_list_list_int_invalid_default(
    default: Any, error_type: type, match: str
) -> None:
    with pytest.raises(error_type, match=f'In "default": .*{match}'):
        validate_job_parameter({"name": "AdjList", "type": TYPE, "default": default})


def test_validate_job_parameter_list_list_int_accepts_int64_bounds() -> None:
    validate_job_parameter({"name": "AdjList", "type": TYPE, "default": [[INT64_MIN, INT64_MAX]]})


def test_validate_job_parameter_list_list_int_default_must_be_native_list() -> None:
    """Unlike a submitted value, a template default may not be a JSON string."""
    with pytest.raises(TypeError, match='got str for "default"'):
        validate_job_parameter({"name": "AdjList", "type": TYPE, "default": "[[1]]"})


@pytest.mark.parametrize(
    ("field", "field_value"),
    [
        ("allowedValues", [[1]]),
        ("minValue", 0),
        ("maxValue", 10),
        ("objectType", "FILE"),
        ("dataFlow", "IN"),
    ],
)
def test_validate_job_parameter_list_list_int_disallowed_fields(
    field: str, field_value: Any
) -> None:
    with pytest.raises(
        ValueError, match=f'has "{field}" but type "{ESCAPED_TYPE}" does not support it'
    ):
        validate_job_parameter({"name": "AdjList", "type": TYPE, field: field_value})


@pytest.mark.parametrize(
    ("item", "error_type", "match"),
    [
        pytest.param([], TypeError, 'got list for "item" but expected dict', id="not-dict"),
        pytest.param(
            {"allowedValues": [1]},
            ValueError,
            f'has "item" -> "allowedValues" but type "{ESCAPED_TYPE}" only supports '
            '\\("minLength", "maxLength", "item"\\)',
            id="item-allowed-values",
        ),
        pytest.param(
            {"minValue": 0},
            ValueError,
            'has "item" -> "minValue" but type',
            id="item-min-value",
        ),
        pytest.param({"bogus": 1}, ValueError, 'has "item" -> "bogus"', id="item-unknown"),
        pytest.param(
            {"minLength": "1"},
            TypeError,
            'got str for "item" -> "minLength" but expected int',
            id="inner-min-length-str",
        ),
        pytest.param(
            {"maxLength": True},
            TypeError,
            'got bool for "item" -> "maxLength" but expected int',
            id="inner-max-length-bool",
        ),
        pytest.param(
            {"maxLength": 2.0},
            TypeError,
            'got float for "item" -> "maxLength"',
            id="inner-max-length-float",
        ),
        pytest.param(
            {"minLength": -1},
            ValueError,
            'got -1 for "item" -> "minLength" but the value must be non-negative',
            id="inner-min-length-negative",
        ),
        pytest.param(
            {"minLength": 3, "maxLength": 2},
            ValueError,
            '"item" -> "minLength" 3 greater than the maximum inner list item count of 2',
            id="inner-min-above-max",
        ),
        pytest.param(
            {"minLength": 65},
            ValueError,
            "greater than the maximum inner list item count of 64",
            id="inner-min-above-service-cap",
        ),
        pytest.param(
            {"item": [1]},
            TypeError,
            'got list for "item" -> "item" but expected dict',
            id="element-not-dict",
        ),
        pytest.param(
            {"item": {"minLength": 1}},
            ValueError,
            f'has "item" -> "item" -> "minLength" but type "{ESCAPED_TYPE}" only supports '
            '\\("allowedValues", "minValue", "maxValue"\\)',
            id="element-string-field",
        ),
        pytest.param(
            {"item": {"item": {}}},
            ValueError,
            'has "item" -> "item" -> "item" but type',
            id="element-nested-item",
        ),
        pytest.param(
            {"item": {"minValue": 1.5}},
            TypeError,
            'got float for "item" -> "item" -> "minValue" but expected int',
            id="element-float-min",
        ),
        pytest.param(
            {"item": {"maxValue": "10"}},
            TypeError,
            'got str for "item" -> "item" -> "maxValue" but expected int',
            id="element-str-max",
        ),
        pytest.param(
            {"item": {"minValue": True}},
            TypeError,
            'got bool for "item" -> "item" -> "minValue"',
            id="element-bool-min",
        ),
        pytest.param(
            {"item": {"maxValue": 2**63}},
            ValueError,
            '"item" -> "item" -> "maxValue" which is outside the 64-bit integer range',
            id="element-max-above-int64",
        ),
        pytest.param(
            {"item": {"allowedValues": 1}},
            TypeError,
            'got int for "item" -> "item" -> "allowedValues" but expected list',
            id="element-allowed-not-list",
        ),
        pytest.param(
            {"item": {"allowedValues": []}},
            ValueError,
            'empty "item" -> "item" -> "allowedValues" list',
            id="element-allowed-empty",
        ),
        pytest.param(
            {"item": {"allowedValues": [1, "2"]}},
            TypeError,
            '"item" -> "item" -> "allowedValues" \\[1\\] but expected int',
            id="element-allowed-wrong-type",
        ),
        pytest.param(
            {"item": {"allowedValues": [1, False]}},
            TypeError,
            'got bool for "item" -> "item" -> "allowedValues" \\[1\\]',
            id="element-allowed-bool",
        ),
        pytest.param(
            {"item": {"minValue": 5, "maxValue": 1}},
            ValueError,
            '"item" -> "item" -> "minValue" 5 greater than "item" -> "item" -> "maxValue" 1',
            id="element-min-above-max",
        ),
        pytest.param(
            {"item": {"allowedValues": [1, 20], "maxValue": 10}},
            ValueError,
            '"item" -> "item" -> "allowedValues" \\[1\\] 20 outside the item value range',
            id="element-allowed-outside-range",
        ),
    ],
)
def test_validate_job_parameter_list_list_int_invalid_item(
    item: Any, error_type: type, match: str
) -> None:
    with pytest.raises(error_type, match=match):
        validate_job_parameter({"name": "AdjList", "type": TYPE, "item": item})


def test_validate_job_parameter_list_list_int_outer_length_range() -> None:
    with pytest.raises(ValueError, match="greater than the maximum item count of 512"):
        validate_job_parameter({"name": "AdjList", "type": TYPE, "minLength": 513})
    with pytest.raises(ValueError, match="greater than the maximum item count of 1"):
        validate_job_parameter({"name": "AdjList", "type": TYPE, "minLength": 2, "maxLength": 1})


def test_validate_job_parameter_list_list_int_unknown_ui_control() -> None:
    """A template can only name an OpenJD control, not the client's own JSON editor."""
    with pytest.raises(ValueError, match='"userInterface" -> "control" but got JSON_EDIT'):
        validate_job_parameter(
            {"name": "AdjList", "type": TYPE, "userInterface": {"control": "JSON_EDIT"}}
        )


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param([[1, 2], [3]], [[1, 2], [3]], id="list"),
        pytest.param("[[1, 2], [3]]", [[1, 2], [3]], id="json"),
        pytest.param(" [ [ -1 ] , [ ] ] ", [[-1], []], id="json-whitespace"),
        pytest.param("[]", [], id="json-empty"),
        pytest.param("[[]]", [[]], id="json-empty-inner"),
        pytest.param([], [], id="empty"),
        pytest.param([[INT64_MIN, INT64_MAX]], [[INT64_MIN, INT64_MAX]], id="int64-bounds"),
        pytest.param([[0] * 64] * 512, [[0] * 64] * 512, id="service-caps"),
    ],
)
def test_validate_job_parameter_value_list_list_int(value: Any, expected: list) -> None:
    result = validate_job_parameter_value({"name": "AdjList", "type": TYPE}, value)
    assert result == expected
    assert all(type(e) is int for inner in result for e in inner)  # type: ignore[union-attr]


def test_validate_job_parameter_value_list_list_int_returns_new_lists() -> None:
    value = [[1, 2], [3]]
    result: Any = validate_job_parameter_value({"name": "AdjList", "type": TYPE}, value)
    assert result is not value
    assert all(r is not v for r, v in zip(result, value))


@pytest.mark.parametrize(
    ("value", "error_type", "match"),
    [
        pytest.param(
            "[[1, 2], [3]",
            ValueError,
            "not a JSON array of lists of integers, such as \\[\\[1, 2\\], \\[3\\]\\]",
            id="not-json",
        ),
        pytest.param("1,2", ValueError, "not a JSON array of lists of integers", id="csv"),
        pytest.param("5", ValueError, "5 which is not a JSON array", id="json-scalar"),
        pytest.param('{"a": [1]}', ValueError, "which is not a JSON array", id="json-object"),
        pytest.param(5, TypeError, "got value 5 of type", id="scalar"),
        pytest.param("[1, 2]", TypeError, "item 0 is 1 of type", id="json-flat"),
        pytest.param('[["1"]]', TypeError, "item 0 element 0 is '1'", id="json-string-element"),
        pytest.param("[[true]]", TypeError, "item 0 element 0 is True", id="json-bool-element"),
        pytest.param("[[1.0]]", TypeError, "item 0 element 0 is 1.0", id="json-float-element"),
        pytest.param("[[NaN]]", TypeError, "item 0 element 0 is nan", id="json-nan-element"),
        pytest.param("[[null]]", TypeError, "item 0 element 0 is None", id="json-null-element"),
        pytest.param("[null]", TypeError, "item 0 is None", id="json-null-inner"),
        pytest.param(
            f"[[{INT64_MAX + 1}]]", ValueError, "outside the 64-bit integer range", id="int64"
        ),
        pytest.param([[0]] * 513, ValueError, "513 items but at most 512", id="outer-cap"),
        pytest.param([[0] * 65], ValueError, "65 elements but at most 64", id="inner-cap"),
    ],
)
def test_validate_job_parameter_value_list_list_int_invalid(
    value: Any, error_type: type, match: str
) -> None:
    with pytest.raises(error_type, match=match):
        validate_job_parameter_value({"name": "AdjList", "type": TYPE}, value)


def test_validate_job_parameter_value_list_list_int_constraints() -> None:
    definition: Any = {
        "name": "AdjList",
        "type": TYPE,
        "minLength": 1,
        "maxLength": 2,
        "item": {"minLength": 1, "maxLength": 2, "item": {"allowedValues": [1, 2, 3, 30]}},
    }
    assert validate_job_parameter_value(definition, "[[1], [2, 3]]") == [[1], [2, 3]]
    for value, match in [
        ([], "has 0 items but minLength is 1"),
        ([[1], [2], [3]], "has 3 items but at most 2 are allowed"),
        ([[1], []], "item 1 has 0 elements but item minLength is 1"),
        ([[1, 2, 3]], "item 0 has 3 elements but at most 2 are allowed"),
        ([[1], [4]], "item 1 element 0 4 is not an allowed value from \\(1, 2, 3, 30\\)"),
    ]:
        with pytest.raises(ValueError, match=match):
            validate_job_parameter_value(definition, value)

    ranged: Any = {
        "name": "AdjList",
        "type": TYPE,
        "item": {"item": {"minValue": 0, "maxValue": 9}},
    }
    assert validate_job_parameter_value(ranged, [[0, 9]]) == [[0, 9]]
    with pytest.raises(ValueError, match="item 0 element 1 10 is greater than"):
        validate_job_parameter_value(ranged, [[0, 10]])
    with pytest.raises(ValueError, match="item 0 element 0 -1 is less than"):
        validate_job_parameter_value(ranged, [[-1]])


@pytest.mark.parametrize(
    ("definition", "expected"),
    [
        pytest.param({"name": "X", "type": TYPE}, "MULTILINE_EDIT", id="default"),
        pytest.param(
            {"name": "X", "type": TYPE, "item": {"item": {"allowedValues": [1, 2]}}},
            "MULTILINE_EDIT",
            id="item-allowed-values-is-not-dropdown",
        ),
        pytest.param(
            {"name": "X", "type": TYPE, "userInterface": {"label": "Edges"}},
            "MULTILINE_EDIT",
            id="label-only",
        ),
        pytest.param(
            {"name": "X", "type": TYPE, "userInterface": {"control": "HIDDEN"}},
            "HIDDEN",
            id="hidden",
        ),
    ],
)
def test_ui_control_for_list_list_int(definition: Any, expected: str) -> None:
    assert get_ui_control_for_parameter_definition(definition) == expected


@pytest.mark.parametrize(
    "control",
    [
        "MULTILINE_EDIT",
        "LINE_EDIT",
        "LINE_EDIT_LIST",
        "SPIN_BOX",
        "SPIN_BOX_LIST",
        "DROPDOWN_LIST",
        "CHECK_BOX_LIST",
    ],
)
def test_ui_control_for_list_list_int_unsupported(control: str) -> None:
    """OpenJD only allows HIDDEN, so a template cannot select the client's default control."""
    definition: Any = {"name": "X", "type": TYPE, "userInterface": {"control": control}}
    with pytest.raises(
        DeadlineOperationError,
        match=f"unsupported control '{control}' for its type '{ESCAPED_TYPE}'",
    ):
        get_ui_control_for_parameter_definition(definition)


@pytest.mark.parametrize(
    "value",
    [pytest.param([[1, 2], [3]], id="list"), pytest.param("[[1, 2], [3]]", id="json-string")],
)
def test_split_parameter_args_list_list_int(value: Any) -> None:
    params: list[JobParameter] = [
        {"name": "Edges", "type": TYPE, "value": value},
        {"name": "Big", "type": TYPE, "value": [[INT64_MIN], [INT64_MAX, 0]]},
        {"name": "EmptyInner", "type": TYPE, "value": [[]]},
        {"name": "None", "type": TYPE, "value": []},
    ]
    _, job_params = submission.split_parameter_args(params, "test_bundle")
    assert job_params == {
        "Edges": {"intListList": [["1", "2"], ["3"]]},
        "Big": {"intListList": [[str(INT64_MIN)], [str(INT64_MAX), "0"]]},
        "EmptyInner": {"intListList": [[]]},
        "None": {"intListList": []},
    }


def test_split_parameter_args_list_list_int_items_match_int_string_shape() -> None:
    """Each item matches CreateJob's IntString: 1-20 characters of pattern [-]?(0|[1-9][0-9]*)."""
    params: list[JobParameter] = [
        {"name": "Edges", "type": TYPE, "value": [[0, -1, 10, INT64_MIN, INT64_MAX]]}
    ]
    _, job_params = submission.split_parameter_args(params, "test_bundle")
    pattern = re.compile(r"[-]?(0|[1-9][0-9]*)")
    (items,) = job_params["Edges"]["intListList"]
    assert all(pattern.fullmatch(item) and 1 <= len(item) <= 20 for item in items)


@pytest.mark.parametrize(
    ("value", "match"),
    [
        pytest.param("[[1, 2]", "not a JSON array of lists of integers", id="not-json"),
        pytest.param([[1, "2"]], "item 0 element 1 is '2'", id="string-element"),
        pytest.param([3], "item 0 is 3 of type", id="flat"),
    ],
)
def test_split_parameter_args_rejects_invalid_list_list_int(value: Any, match: str) -> None:
    params: list[JobParameter] = [{"name": "Edges", "type": TYPE, "value": value}]
    with pytest.raises(DeadlineOperationError, match=f"(?s){match}.*From job bundle:\ntest_bundle"):
        submission.split_parameter_args(params, "test_bundle")


def test_apply_job_parameters_list_list_int() -> None:
    """A -p value replaces the bundle's value as given, and adds no asset references."""
    parameters: list[JobParameter] = [{"name": "Edges", "type": TYPE, "default": [[1]]}]
    asset_references = AssetReferences()
    apply_job_parameters(
        [{"name": "Edges", "value": "[[1, 2], [3]]"}], "bundle", parameters, asset_references
    )
    assert parameters[0]["value"] == "[[1, 2], [3]]"
    assert asset_references.to_dict() == AssetReferences().to_dict()


LIST_LIST_INT_TEMPLATE = """
specificationVersion: jobtemplate-2023-09
extensions:
- EXPR
name: ListJob
parameterDefinitions:
- name: Edges
  type: {type_name}
  default: [[1, 2], [3]]
  item:
    maxLength: 4
    item:
      minValue: 0
steps:
- name: Render
  script:
    actions:
      onRun:
        command: echo
        args: ["{{{{Param.Edges}}}}"]
"""


def _write_bundle(bundle_dir: str, template: str, parameter_values: Any = None) -> None:
    with open(os.path.join(bundle_dir, "template.yaml"), "w", encoding="utf8") as f:
        f.write(template)
    if parameter_values is not None:
        with open(os.path.join(bundle_dir, "parameter_values.json"), "w", encoding="utf8") as f:
            json.dump({"parameterValues": parameter_values}, f)


@pytest.mark.parametrize("type_name", ["LIST[LIST[INT]]", "list[list[int]]", "List[List[Int]]"])
def test_read_job_bundle_parameters_list_list_int(
    fresh_deadline_config, temp_job_bundle_dir, type_name: str
) -> None:
    _write_bundle(
        temp_job_bundle_dir,
        LIST_LIST_INT_TEMPLATE.format(type_name=type_name),
        [{"name": "Edges", "value": [[4], []]}],
    )

    (edges,) = read_job_bundle_parameters(temp_job_bundle_dir)

    # The EXPR extension makes the type name case-insensitive.
    assert edges["type"] == TYPE
    assert edges["value"] == [[4], []]
    assert edges["default"] == [[1, 2], [3]]


def test_read_job_bundle_parameters_list_list_int_case_sensitive_without_expr(
    fresh_deadline_config, temp_job_bundle_dir
) -> None:
    template = LIST_LIST_INT_TEMPLATE.format(type_name="list[list[int]]").replace(
        "extensions:\n- EXPR\n", ""
    )
    _write_bundle(temp_job_bundle_dir, template)

    with pytest.raises(ValueError, match='had "type" list\\[list\\[int\\]\\]'):
        read_job_bundle_parameters(temp_job_bundle_dir)


def test_read_job_bundle_parameters_list_list_int_invalid_default(
    fresh_deadline_config, temp_job_bundle_dir
) -> None:
    _write_bundle(
        temp_job_bundle_dir,
        LIST_LIST_INT_TEMPLATE.format(type_name=TYPE).replace("[[1, 2], [3]]", "[[1, -2], [3]]"),
    )

    with pytest.raises(
        ValueError, match="item 0 element 1 -2 is less than item -> item minValue 0"
    ):
        read_job_bundle_parameters(temp_job_bundle_dir)


def test_read_job_bundle_parameters_hidden_list_list_int_needs_a_value(
    fresh_deadline_config, temp_job_bundle_dir
) -> None:
    template = (
        LIST_LIST_INT_TEMPLATE.format(type_name=TYPE)
        .replace("  default: [[1, 2], [3]]\n", "")
        .replace("  item:\n", "  userInterface:\n    control: HIDDEN\n  item:\n", 1)
    )
    _write_bundle(temp_job_bundle_dir, template)

    with pytest.raises(DeadlineOperationError, match='Hidden parameter "Edges" is missing a value'):
        read_job_bundle_parameters(temp_job_bundle_dir)


def test_merge_queue_job_parameters_list_list_int_type_mismatch() -> None:
    queue_param: JobParameter = {"name": "Edges", "type": TYPE}
    job_param: JobParameter = {"name": "Edges", "type": "LIST[INT]"}

    with pytest.raises(
        DeadlineOperationError, match="Edges: differences for fields \"\\['type'\\]\""
    ):
        merge_queue_job_parameters(job_parameters=[job_param], queue_parameters=[queue_param])


def test_merge_queue_job_parameters_list_list_int_element_mismatch() -> None:
    queue_param: Any = {"name": "Edges", "type": TYPE, "item": {"item": {"maxValue": 9}}}
    job_param: Any = {"name": "Edges", "type": TYPE, "item": {"item": {"maxValue": 10}}}

    with pytest.raises(
        DeadlineOperationError, match="Edges: differences for fields \"\\['item'\\]\""
    ):
        merge_queue_job_parameters(job_parameters=[job_param], queue_parameters=[queue_param])


def test_parameter_definition_difference_list_list_int_items() -> None:
    lhs: Any = {
        "name": "E",
        "type": TYPE,
        "item": {"maxLength": 3, "item": {"allowedValues": [1, 2, 3]}},
        "default": [[1]],
    }
    rhs: Any = {
        "name": "E",
        "type": TYPE,
        "item": {"maxLength": 3, "item": {"allowedValues": [3, 2, 1]}},
        "default": [[2]],
    }
    # Nested allowedValues compare as sets, and the default is not compared.
    assert parameter_definition_difference(lhs, rhs) == []
    shallower: Any = {**rhs, "item": {"maxLength": 3}}
    assert parameter_definition_difference(lhs, shallower) == ["item"]
    longer: Any = {**rhs, "item": {"maxLength": 4, "item": {"allowedValues": [1, 2, 3]}}}
    assert parameter_definition_difference(lhs, longer) == ["item"]
    assert (
        parameter_definition_difference(
            {"name": "E", "type": TYPE, "item": {"item": {}}}, {"name": "E", "type": TYPE}
        )
        == []
    )


def test_queue_parameter_list_list_int_gets_no_control_it_cannot_use() -> None:
    """A queue environment parameter without a userInterface is given a group label. Its
    control is left unset, since the client's default control is not one OpenJD allows."""
    env_template = """
specificationVersion: environment-2023-09
extensions: [EXPR]
parameterDefinitions:
- name: Edges
  type: list[list[int]]
  default: [[1, 2]]
- name: Frames
  type: list[int]
  default: [1]
environment:
  name: Env
  script:
    actions:
      onEnter:
        command: echo
"""
    deadline_client = MagicMock()
    deadline_client.list_queue_environments.return_value = {
        "environments": [{"queueEnvironmentId": "queueenv-1", "name": "Env", "priority": 1}]
    }
    deadline_client.get_queue_environment.return_value = {
        "queueEnvironmentId": "queueenv-1",
        "name": "Env",
        "priority": 1,
        "templateType": "YAML",
        "template": env_template,
    }

    with patch.object(_queue_parameters, "get_boto3_client", return_value=deadline_client):
        edges, frames = _queue_parameters.get_queue_parameter_definitions(
            farmId=MOCK_FARM_ID, queueId=MOCK_QUEUE_ID
        )

    assert edges["type"] == TYPE
    assert edges["userInterface"] == {"groupLabel": "Queue Environment: Env"}
    assert get_ui_control_for_parameter_definition(edges) == "MULTILINE_EDIT"
    assert frames["userInterface"] == {
        "control": "SPIN_BOX_LIST",
        "groupLabel": "Queue Environment: Env",
    }


_CREATE_JOB_TEMPLATE = {
    "specificationVersion": "jobtemplate-2023-09",
    "extensions": ["EXPR"],
    "name": "TestJob",
    "parameterDefinitions": [
        {"name": "Edges", "type": "list[list[int]]", "default": [[1, 2], [3]]}
    ],
    "steps": [{"name": "Step", "bash": {"script": "echo {{Param.Edges}}"}}],
}


@pytest.mark.parametrize(
    ("cli_value", "expected"),
    [
        # The service applies the default itself.
        pytest.param(None, None, id="default"),
        pytest.param([[1, 2], [3]], {"Edges": {"intListList": [["1", "2"], ["3"]]}}, id="list"),
        pytest.param(
            "[[-4], [], [5, 6]]",
            {"Edges": {"intListList": [["-4"], [], ["5", "6"]]}},
            id="json-string",
        ),
    ],
)
def test_create_job_from_job_bundle_list_list_int(
    fresh_deadline_config, temp_job_bundle_dir, cli_value, expected
):
    """A LIST[LIST[INT]] value, as a list or from the CLI -p option as JSON, is sent to
    CreateJob in the intListList member."""
    config_file.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config_file.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    with open(os.path.join(temp_job_bundle_dir, "template.json"), "w", encoding="utf8") as f:
        json.dump(_CREATE_JOB_TEMPLATE, f)

    job_parameters = [] if cli_value is None else [{"name": "Edges", "value": cli_value}]
    with patch_calls_for_create_job_from_job_bundle() as mock:
        api.create_job_from_job_bundle(
            temp_job_bundle_dir, job_parameters=job_parameters, queue_parameter_definitions=[]
        )
        create_job_kwargs = mock.get_boto3_client().create_job.call_args.kwargs
        if expected is None:
            assert "parameters" not in create_job_kwargs
        else:
            assert create_job_kwargs["parameters"] == expected
        # The template is sent as written, with its type name's case unchanged.
        assert json.loads(create_job_kwargs["template"]) == _CREATE_JOB_TEMPLATE


def test_create_job_from_job_bundle_rejects_invalid_list_list_int(
    fresh_deadline_config, temp_job_bundle_dir
):
    """An invalid LIST[LIST[INT]] value is reported before CreateJob is called."""
    config_file.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config_file.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    with open(os.path.join(temp_job_bundle_dir, "template.json"), "w", encoding="utf8") as f:
        json.dump(_CREATE_JOB_TEMPLATE, f)

    with patch_calls_for_create_job_from_job_bundle() as mock:
        with pytest.raises(DeadlineOperationError, match="item 0 element 0 is 'a'"):
            api.create_job_from_job_bundle(
                temp_job_bundle_dir,
                job_parameters=[{"name": "Edges", "value": '[["a"]]'}],
                queue_parameter_definitions=[],
            )
        mock.get_boto3_client().create_job.assert_not_called()


def test_cli_bundle_submit_list_list_int(fresh_deadline_config, deadline_mock, temp_job_bundle_dir):
    """'deadline bundle submit -p Edges=...' accepts a LIST[LIST[INT]] value as JSON."""
    with open(os.path.join(temp_job_bundle_dir, "template.json"), "w", encoding="utf8") as f:
        json.dump(_CREATE_JOB_TEMPLATE, f)
    deadline_mock.create_job.return_value = MOCK_CREATE_JOB_RESPONSE
    deadline_mock.get_job.return_value = MOCK_GET_JOB_RESPONSE

    result = CliRunner().invoke(
        main,
        [
            "bundle",
            "submit",
            temp_job_bundle_dir,
            "--farm-id",
            MOCK_FARM_ID,
            "--queue-id",
            MOCK_QUEUE_ID,
            "-p",
            "Edges=[[1,2],[3]]",
        ],
    )

    assert result.exit_code == 0, result.output
    assert deadline_mock.create_job.call_args.kwargs["parameters"] == {
        "Edges": {"intListList": [["1", "2"], ["3"]]}
    }


def test_bundle_browser_labels_list_list_int() -> None:
    try:
        from deadline.client.ui.dialogs.job_bundle_browser_dialog import _friendly_param_type
    except ImportError:
        pytest.skip("GUI dependencies are not installed")
    assert _friendly_param_type("list[list[int]]") == "Nested number list"
