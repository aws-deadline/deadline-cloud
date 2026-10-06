# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

__all__ = [
    "JobParameter",
    "apply_job_parameters",
    "get_ui_control_for_parameter_definition",
    "parameter_definition_difference",
    "read_job_bundle_parameters",
]

import json
import logging
import math
import os
from collections import namedtuple
from typing import Any, TYPE_CHECKING, cast, Optional, Union

# typing_extensions is only needed for type-checking. It fails to import at run-time in Python 3.7
# so provide stubs at run-time.
if TYPE_CHECKING:
    from typing_extensions import NotRequired, TypedDict
    from ..job_bundle.submission import AssetReferences
else:
    NotRequired = object
    TypedDict = object

from .._path_utils import is_absolute_path, is_path_contained, is_uri
from ..exceptions import DeadlineOperationError
from ._range_expr import parse_int_range_expr
from .loader import read_yaml_or_json_object

_logger = logging.getLogger(__name__)

_VALID_PARAMETER_TYPES = (
    "STRING",
    "PATH",
    "INT",
    "FLOAT",
    "BOOL",
    "RANGE_EXPR",
    "LIST[STRING]",
    "LIST[PATH]",
    "LIST[INT]",
    "LIST[FLOAT]",
    "LIST[BOOL]",
)
_BOOL_DISALLOWED_FIELDS = (
    "allowedValues",
    "minLength",
    "maxLength",
    "minValue",
    "maxValue",
)
_RANGE_EXPR_DISALLOWED_FIELDS = (
    "allowedValues",
    "minValue",
    "maxValue",
)
# The list types constrain their items through the nested "item" object instead.
_LIST_DISALLOWED_FIELDS = (
    "allowedValues",
    "minValue",
    "maxValue",
)
# Like a PATH, a LIST[PATH] describes the objects its paths name and how the job uses them.
_PATH_ONLY_FIELDS = (
    "objectType",
    "dataFlow",
)
# LIST[BOOL] has no "item" object, since a boolean has nothing to constrain.
_LIST_ITEM_FIELDS = {
    "LIST[STRING]": ("allowedValues", "minLength", "maxLength"),
    "LIST[PATH]": ("allowedValues", "minLength", "maxLength"),
    "LIST[INT]": ("allowedValues", "minValue", "maxValue"),
    "LIST[FLOAT]": ("allowedValues", "minValue", "maxValue"),
}
_LIST_TYPES = (*_LIST_ITEM_FIELDS, "LIST[BOOL]")
# The list types whose items are strings, constrained by length.
_STRING_ITEM_LIST_TYPES = ("LIST[STRING]", "LIST[PATH]")
# CreateJob's JobParameter.stringList and pathList are each a list of 0-64 strings of 0-1024
# characters.
_MAX_STRING_LIST_ITEMS = 64
_MAX_STRING_LIST_ITEM_LENGTH = 1024
# CreateJob's JobParameter.intList, floatList and boolList each hold 0-512 items.
_MAX_NUMBER_LIST_ITEMS = 512
_MAX_BOOL_LIST_ITEMS = 512
_MAX_LIST_ITEMS = {
    "LIST[STRING]": _MAX_STRING_LIST_ITEMS,
    "LIST[PATH]": _MAX_STRING_LIST_ITEMS,
    "LIST[INT]": _MAX_NUMBER_LIST_ITEMS,
    "LIST[FLOAT]": _MAX_NUMBER_LIST_ITEMS,
    "LIST[BOOL]": _MAX_BOOL_LIST_ITEMS,
}
# The case-insensitive strings that a BOOL or LIST[BOOL] value accepts.
_TRUE_STRINGS = ("true", "yes", "on", "1")
_FALSE_STRINGS = ("false", "no", "off", "0")
# OpenJD ints are 64-bit.
_MIN_INT64 = -(2**63)
_MAX_INT64 = 2**63 - 1
# How each list type describes its items in error messages, with an example value.
_LIST_ITEM_DESCRIPTIONS = {
    "LIST[STRING]": ("strings", '["a", "b"]'),
    "LIST[PATH]": ("paths", '["scenes/a.blend", "scenes/b.blend"]'),
    "LIST[INT]": ("integers", "[1, 2]"),
    "LIST[FLOAT]": ("numbers", "[0.5, 2]"),
    "LIST[BOOL]": ("booleans", "[true, false]"),
}
_VALID_UI_CONTROLS = (
    "CHECK_BOX",
    "CHECK_BOX_LIST",
    "CHOOSE_DIRECTORY",
    "CHOOSE_DIRECTORY_LIST",
    "CHOOSE_INPUT_FILE",
    "CHOOSE_INPUT_FILE_LIST",
    "CHOOSE_OUTPUT_FILE",
    "CHOOSE_OUTPUT_FILE_LIST",
    "DROPDOWN_LIST",
    "LINE_EDIT",
    "LINE_EDIT_LIST",
    "MULTILINE_EDIT",
    "SPIN_BOX",
    "SPIN_BOX_LIST",
    "HIDDEN",
)


class UserInterfaceFileFilter(TypedDict):
    label: str
    patterns: list[str]


class UserInterfaceSpec(TypedDict):
    control: NotRequired[str]
    decimal: NotRequired[int]
    groupLabel: NotRequired[str]
    label: NotRequired[str]
    singleStepDelta: NotRequired[float]
    fileFilters: NotRequired[list[UserInterfaceFileFilter]]
    fileFilterDefault: NotRequired[UserInterfaceFileFilter]


class JobParameterItemConstraints(TypedDict):
    allowedValues: NotRequired[Union[list[str], list[int], list[float]]]
    minLength: NotRequired[int]
    maxLength: NotRequired[int]
    minValue: NotRequired[Union[int, float]]
    maxValue: NotRequired[Union[int, float]]


class JobParameter(TypedDict):
    name: str
    type: NotRequired[str]
    description: NotRequired[str]
    value: NotRequired[Any]
    default: NotRequired[Any]
    allowedValues: NotRequired[list[Any]]
    dataFlow: NotRequired[str]
    objectType: NotRequired[str]
    maxLength: NotRequired[int]
    minLength: NotRequired[int]
    maxValue: NotRequired[Union[int, float, str]]
    minValue: NotRequired[Union[int, float, str]]
    item: NotRequired[JobParameterItemConstraints]
    userInterface: NotRequired[UserInterfaceSpec]


def _has_expr_extension(template: Any) -> bool:
    """Whether a job template declares OpenJD's EXPR extension."""
    if not isinstance(template, dict):
        return False
    extensions = template.get("extensions")
    return isinstance(extensions, list) and "EXPR" in extensions


def _normalize_parameter_type_case(template: dict[str, Any]) -> None:
    """Upper-cases the "type" of each job parameter definition in place when the template
    declares the EXPR extension, which makes OpenJD parameter type names case-insensitive.

    Only the client's in-memory parameter definitions are normalized. The template
    submitted to CreateJob is read separately and left unchanged, so task parameter
    type names reach the service as written.
    """
    if not _has_expr_extension(template):
        return
    parameter_definitions = template.get("parameterDefinitions")
    if not isinstance(parameter_definitions, list):
        return
    for parameter in parameter_definitions:
        if isinstance(parameter, dict) and isinstance(parameter.get("type"), str):
            parameter["type"] = parameter["type"].upper()


def validate_job_parameter(
    input: Any,
    *,
    type_required: bool = False,
    default_required: bool = False,
) -> JobParameter:
    """Validates a job parameter as defined by Open Job Description. The validation allows for the
    union of all possible fields. Per-type checks are applied for BOOL, RANGE_EXPR and
    the LIST[STRING], LIST[PATH], LIST[INT], LIST[FLOAT] and LIST[BOOL] types, whose constraint
    fields and defaults differ from the other types; the other types are not checked per type
    (e.g. minValue is not limited to "INT" / "FLOAT").

    name: <Identifier>
    type: "PATH"
    description: <Description> # @optional
    default: <JobParameterStringValue> # @optional
    allowedValues: [ <JobParameterStringValue>, ... ] # @optional
    minLength: <integer> # @optional
    maxLength: <integer> # @optional
    minValue: <integer> | <intstr> | <float> | <floatstring> # @optional
    maxValue: <integer> | <intstr> | <float> | <floatstring> # @optional
    objectType: enum("FILE", "DIRECTORY") # @optional
    dataFlow: enum("NONE", "IN", "OUT", "INOUT") # @optional
    userInterface: # @optional
        # ...

    Parameters
    ----------
    input : Any
        The input to validate
    type_required : bool = False
        Whether the "type" field is required. This is important for job bundles which may contain
        app-specific parameter values without accompanying metadata such as the parameter type.
    default_required : bool = False
        Whether the "default" field is required. In queue environments, defaults are required. In
        job bundles, defaults are not required.

    Raises
    ------
    ValueError
        The input contains a non-valid value
    TypeError
        The input or one of its fields are not of the expected type

    Returns
    -------
    JobParameter
        A type-cast version of the input. This is the same object reference to the input, but the
        data has been validated.
    """
    if not isinstance(input, dict):
        raise TypeError(f"Expected a dict for job parameter, but got {type(input).__name__}")

    # Validate "name"
    if "name" not in input:
        raise ValueError(f'No "name" field in job parameter. Got {input}')
    name = input["name"]
    if not isinstance(name, str):
        raise TypeError(f'Job parameter had {type(name).__name__} for "name" but expected str')
    elif name == "":
        raise ValueError("Job parameter has an empty name")

    # Validate "description"
    if "description" in input:
        description = input["description"]
        if not isinstance(description, str):
            raise TypeError(
                f'Job parameter "{name}" had {type(description).__name__} for "description" but expected str'
            )

    # Validate "type"
    if "type" in input:
        typ = input["type"]
        if typ not in _VALID_PARAMETER_TYPES:
            quoted = (f'"{valid_param_type}"' for valid_param_type in _VALID_PARAMETER_TYPES)
            raise ValueError(
                f'Job parameter "{name}" had "type" {typ} but expected one of ({", ".join(quoted)})'
            )
    elif type_required:
        raise ValueError(f'Job parameter "{name}" is missing required key "type"')

    # Validate "default"
    if "default" in input:
        default = input["default"]
        if default is None:
            raise ValueError(f'Job parameter "{name}" had None for "default" but expected a value')
    elif default_required:
        raise ValueError(f'Job parameter "{name}" is missing required key "default"')

    # A boolean already enumerates its own domain, and has no length or numeric
    # ordering, so OpenJD does not permit these constraints on BOOL parameters.
    if input.get("type") == "BOOL":
        for field in _BOOL_DISALLOWED_FIELDS:
            if field in input:
                raise ValueError(
                    f'Job parameter "{name}" has "{field}" but type "BOOL" does not support it'
                )

    if input.get("type") == "RANGE_EXPR":
        for field in _RANGE_EXPR_DISALLOWED_FIELDS:
            if field in input:
                raise ValueError(
                    f'Job parameter "{name}" has "{field}" but type "RANGE_EXPR" does not support it'
                )
        if "default" in input:
            default = input["default"]
            if not isinstance(default, str):
                raise TypeError(
                    f'Job parameter "{name}" got {type(default).__name__} for "default" but type "RANGE_EXPR" expects str'
                )
            try:
                parse_int_range_expr(default)
            except ValueError as e:
                raise ValueError(
                    f'Job parameter "{name}" has "default" that is not a valid range expression: {e}'
                ) from e

    if input.get("type") in _LIST_TYPES:
        list_type = input["type"]
        disallowed_fields: tuple[str, ...] = _LIST_DISALLOWED_FIELDS
        if list_type != "LIST[PATH]":
            disallowed_fields += _PATH_ONLY_FIELDS
        for field in disallowed_fields:
            if field in input:
                raise ValueError(
                    f'Job parameter "{name}" has "{field}" but type "{list_type}" does not support it'
                )
        if "item" in input:
            if list_type not in _LIST_ITEM_FIELDS:
                raise ValueError(
                    f'Job parameter "{name}" has "item" but type "{list_type}" does not support it'
                )
            elif list_type in _STRING_ITEM_LIST_TYPES:
                _validate_list_string_item_constraints(
                    input["item"], list_type=list_type, parameter_name=name
                )
            else:
                _validate_list_number_item_constraints(
                    input["item"], list_type=list_type, parameter_name=name
                )
    elif "item" in input:
        raise ValueError(
            f'Job parameter "{name}" has "item" but type "{input.get("type")}" does not support it'
        )

    if "allowedValues" in input:
        allowed_values = input["allowedValues"]
        if not isinstance(allowed_values, list):
            raise TypeError(
                f'Job parameter "{name}" got {type(allowed_values).__name__} for "allowedValues" but expected list'
            )

    # Validate "dataFlow"
    if "dataFlow" in input:
        data_flow = input["dataFlow"]
        if data_flow not in ("NONE", "IN", "OUT", "INOUT"):
            raise ValueError(
                f'Job parameter "{name}" got "{data_flow}" for "dataFlow" but expected one of ("NONE", "IN", "OUT", "INOUT")'
            )

    # Validate "minLength"
    if "minLength" in input:
        min_length = input["minLength"]
        if type(min_length) is not int:  # noqa: E721
            raise TypeError(
                f'Job parameter "{name}" got {type(min_length).__name__} for "minLength" but expected int'
            )
        if min_length < 0:
            raise ValueError(
                f'Job parameter "{name}" got {min_length} for "minLength" but the value must be non-negative'
            )

    # Validate "maxLength"
    if "maxLength" in input:
        max_length = input["maxLength"]
        if type(max_length) is not int:  # noqa: E721
            raise TypeError(
                f'Job parameter "{name}" got "{type(max_length).__name__}" for "maxLength" but expected int'
            )
        if max_length < 0:
            raise ValueError(
                f'Job parameter "{name}" got {max_length} for "maxLength" but the value must be non-negative'
            )

    # Validate "minValue"
    if "minValue" in input:
        min_value = input["minValue"]
        if isinstance(min_value, str):
            try:
                float(min_value)
            except ValueError:
                raise ValueError(
                    f'Job parameter "{name}" has a non-numeric string value for "minValue": {min_value}'
                )
        elif type(min_value) not in (int, float):  # noqa: E721
            raise TypeError(
                f'Job parameter "{name}" got {type(min_value).__name__} for "minValue" but expected int'
            )

    # Validate "maxValue"
    if "maxValue" in input:
        max_value = input["maxValue"]
        if isinstance(max_value, str):
            try:
                float(max_value)
            except ValueError:
                raise ValueError(
                    f'Job parameter "{name}" has a non-numeric string value for "maxValue": {max_value}'
                )
        elif type(max_value) not in (int, float):  # noqa: E721
            raise TypeError(
                f'Job parameter "{name}" got {type(max_value).__name__} for "maxValue" but expected int'
            )

    # Validate "objectType"
    if "objectType" in input:
        object_type = input["objectType"]
        if object_type not in ("FILE", "DIRECTORY"):
            raise ValueError(
                f'Job parameter "{name}" got {object_type} for "objectType" but expected one of ("FILE", "DIRECTORY")'
            )

    # Validate "userInterface"
    if "userInterface" in input:
        validate_user_interface_spec(
            input["userInterface"],
            parameter_name=name,
        )

    # Checked last so the list and item constraints it applies are already validated.
    if input.get("type") == "PATH":
        # A PATH is constrained exactly as one item of a LIST[PATH] is.
        _validate_string_constraints(input, parameter_name=name)
        if "default" in input:
            path_constraints = {
                field: input[field]
                for field in ("allowedValues", "minLength", "maxLength")
                if field in input
            }
            try:
                validate_job_parameter_value(
                    cast(JobParameter, {"name": name, "type": "PATH", **path_constraints}),
                    input["default"],
                )
            except (ValueError, TypeError) as e:
                raise type(e)(f'In "default": {e}') from e
    if input.get("type") in _LIST_TYPES:
        list_type = input["type"]
        _validate_list_length_range(input, parameter_name=name)
        if "default" in input:
            default = input["default"]
            # Unlike a submitted value, a template default must be a native list, not a JSON string.
            if not isinstance(default, list):
                raise TypeError(
                    f'Job parameter "{name}" got {type(default).__name__} for "default" but type "{list_type}" expects list'
                )
            try:
                validate_job_parameter_value(cast(JobParameter, input), default)
            except (ValueError, TypeError) as e:
                raise type(e)(f'In "default": {e}') from e

    return cast(JobParameter, input)


def _validate_list_string_item_constraints(
    item: Any, *, list_type: str, parameter_name: str
) -> None:
    """Validates the "item" object of a LIST[STRING] or LIST[PATH] job parameter definition."""
    if not isinstance(item, dict):
        raise TypeError(
            f'Job parameter "{parameter_name}" got {type(item).__name__} for "item" but expected dict'
        )
    item_fields = _LIST_ITEM_FIELDS[list_type]
    for field in item:
        if field not in item_fields:
            quoted = ", ".join(f'"{f}"' for f in item_fields)
            raise ValueError(
                f'Job parameter "{parameter_name}" has "item" -> "{field}" but type "{list_type}" only supports ({quoted})'
            )
    _validate_string_constraints(item, parameter_name=parameter_name, field_prefix='"item" -> ')


def _validate_string_constraints(
    constraints: dict, *, parameter_name: str, field_prefix: str = ""
) -> None:
    """Validates the allowedValues, minLength and maxLength that constrain one string: a PATH
    value, or with ``field_prefix='"item" -> '`` each item of a LIST[STRING] or LIST[PATH]."""
    if "allowedValues" in constraints:
        allowed_values = constraints["allowedValues"]
        if not isinstance(allowed_values, list):
            raise TypeError(
                f'Job parameter "{parameter_name}" got {type(allowed_values).__name__} for {field_prefix}"allowedValues" but expected list'
            )
        if not allowed_values:
            raise ValueError(
                f'Job parameter "{parameter_name}" has an empty {field_prefix}"allowedValues" list'
            )
        for i, allowed_value in enumerate(allowed_values):
            if not isinstance(allowed_value, str):
                raise TypeError(
                    f'Job parameter "{parameter_name}" got {type(allowed_value).__name__} for {field_prefix}"allowedValues" [{i}] but expected str'
                )
    for field in ("minLength", "maxLength"):
        if field in constraints:
            length = constraints[field]
            if type(length) is not int:  # noqa: E721
                raise TypeError(
                    f'Job parameter "{parameter_name}" got {type(length).__name__} for {field_prefix}"{field}" but expected int'
                )
            if length < 0:
                raise ValueError(
                    f'Job parameter "{parameter_name}" got {length} for {field_prefix}"{field}" but the value must be non-negative'
                )

    # Reject constraints no value could satisfy, so the author learns at bundle load rather
    # than from a GUI that can never be submitted.
    min_length = constraints.get("minLength", 0)
    max_length = min(
        constraints.get("maxLength", _MAX_STRING_LIST_ITEM_LENGTH), _MAX_STRING_LIST_ITEM_LENGTH
    )
    if min_length > max_length:
        raise ValueError(
            f'Job parameter "{parameter_name}" has {field_prefix}"minLength" {min_length} greater than '
            f"the maximum length of {max_length}"
        )
    for i, allowed_value in enumerate(constraints.get("allowedValues", [])):
        if not min_length <= len(allowed_value) <= max_length:
            raise ValueError(
                f'Job parameter "{parameter_name}" has {field_prefix}"allowedValues" [{i}] of length '
                f"{len(allowed_value)}, outside the length range {min_length}-{max_length}"
            )


def _check_list_number_constraint(
    value: Any, *, list_type: str, parameter_name: str, field_path: str
) -> None:
    """Checks one number from the "item" object of a LIST[INT] or LIST[FLOAT] definition."""
    expected: Any = int if list_type == "LIST[INT]" else (int, float)
    if isinstance(value, bool) or not isinstance(value, expected):
        expected_name = "int" if list_type == "LIST[INT]" else "int or float"
        raise TypeError(
            f'Job parameter "{parameter_name}" got {type(value).__name__} for "item" -> {field_path} but expected {expected_name}'
        )
    if list_type == "LIST[INT]":
        if not _MIN_INT64 <= value <= _MAX_INT64:
            raise ValueError(
                f'Job parameter "{parameter_name}" got {value} for "item" -> {field_path} which is outside the 64-bit integer range'
            )
    elif not _is_finite_float(value):
        raise ValueError(
            f'Job parameter "{parameter_name}" got {value!r} for "item" -> {field_path} which is not a finite number'
        )


def _is_finite_float(value: int | float) -> bool:
    try:
        return math.isfinite(float(value))
    except OverflowError:
        return False


def _validate_list_number_item_constraints(
    item: Any, *, list_type: str, parameter_name: str
) -> None:
    """Validates the "item" object of a LIST[INT] or LIST[FLOAT] job parameter definition."""
    if not isinstance(item, dict):
        raise TypeError(
            f'Job parameter "{parameter_name}" got {type(item).__name__} for "item" but expected dict'
        )
    item_fields = _LIST_ITEM_FIELDS[list_type]
    for field in item:
        if field not in item_fields:
            quoted = ", ".join(f'"{f}"' for f in item_fields)
            raise ValueError(
                f'Job parameter "{parameter_name}" has "item" -> "{field}" but type "{list_type}" only supports ({quoted})'
            )
    if "allowedValues" in item:
        allowed_values = item["allowedValues"]
        if not isinstance(allowed_values, list):
            raise TypeError(
                f'Job parameter "{parameter_name}" got {type(allowed_values).__name__} for "item" -> "allowedValues" but expected list'
            )
        if not allowed_values:
            raise ValueError(
                f'Job parameter "{parameter_name}" has an empty "item" -> "allowedValues" list'
            )
        for i, allowed_value in enumerate(allowed_values):
            _check_list_number_constraint(
                allowed_value,
                list_type=list_type,
                parameter_name=parameter_name,
                field_path=f'"allowedValues" [{i}]',
            )
    for field in ("minValue", "maxValue"):
        if field in item:
            _check_list_number_constraint(
                item[field],
                list_type=list_type,
                parameter_name=parameter_name,
                field_path=f'"{field}"',
            )

    # Reject constraints no item could satisfy, so the author learns at bundle load rather
    # than from a GUI that can never be submitted.
    item_min, item_max = item.get("minValue"), item.get("maxValue")
    if item_min is not None and item_max is not None and item_min > item_max:
        raise ValueError(
            f'Job parameter "{parameter_name}" has "item" -> "minValue" {item_min} greater than '
            f'"item" -> "maxValue" {item_max}'
        )
    for i, allowed_value in enumerate(item.get("allowedValues", [])):
        if (item_min is not None and allowed_value < item_min) or (
            item_max is not None and allowed_value > item_max
        ):
            raise ValueError(
                f'Job parameter "{parameter_name}" has "item" -> "allowedValues" [{i}] {allowed_value} '
                f"outside the item value range {item_min}-{item_max}"
            )


def _validate_list_length_range(input: dict[str, Any], *, parameter_name: str) -> None:
    """Rejects a list parameter minLength that no list could satisfy."""
    max_items_cap = _MAX_LIST_ITEMS[input["type"]]
    min_items = input.get("minLength", 0)
    max_items = min(input.get("maxLength", max_items_cap), max_items_cap)
    if min_items > max_items:
        raise ValueError(
            f'Job parameter "{parameter_name}" has "minLength" {min_items} greater than the '
            f"maximum item count of {max_items}"
        )


def _to_bool(value: Any) -> bool | None:
    """Converts a value that OpenJD accepts for a boolean to bool, or returns None if it
    is not one: a bool, exactly 0 or 1, or a case-insensitive true/yes/on/1 or
    false/no/off/0 string."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        # Only exactly 0 or 1, not C-style truthiness where any non-zero is true.
        if value == 1:
            return True
        if value == 0:
            return False
        return None
    if isinstance(value, str):
        normalized = value.lower()
        if normalized in _TRUE_STRINGS:
            return True
        if normalized in _FALSE_STRINGS:
            return False
    return None


def _check_list_item_type(list_type: str, name: str, i: int, item: Any) -> None:
    """Raises TypeError if a list item is not of the list's item type. Items are not
    converted between types, e.g. "5" is not an item of a LIST[INT]."""
    if list_type in _STRING_ITEM_LIST_TYPES:
        is_item_type = isinstance(item, str)
    elif list_type == "LIST[INT]":
        is_item_type = isinstance(item, int) and not isinstance(item, bool)
    else:
        is_item_type = isinstance(item, (int, float)) and not isinstance(item, bool)
    if not is_item_type:
        raise TypeError(
            f"Job parameter {name!r} has type {list_type} but item {i} is {item!r} of type {type(item)}."
        )
    if list_type == "LIST[INT]" and not _MIN_INT64 <= item <= _MAX_INT64:
        raise ValueError(
            f"Job parameter {name!r} item {i} {item} is outside the 64-bit integer range."
        )
    if list_type == "LIST[FLOAT]" and not _is_finite_float(item):
        raise ValueError(f"Job parameter {name!r} item {i} {item!r} is not a finite number.")


def _check_string_list_item(name: str, i: int, item: str, item_constraints: dict) -> None:
    item_min_length = item_constraints.get("minLength")
    item_max_length = min(
        item_constraints.get("maxLength", _MAX_STRING_LIST_ITEM_LENGTH),
        _MAX_STRING_LIST_ITEM_LENGTH,
    )
    if item_min_length is not None and len(item) < item_min_length:
        raise ValueError(
            f"Job parameter {name!r} item {i} {item!r} is shorter than item minLength {item_min_length}."
        )
    if len(item) > item_max_length:
        shown = item if len(item) <= 40 else item[:40] + "..."
        raise ValueError(
            f"Job parameter {name!r} item {i} {shown!r} is longer than the maximum of {item_max_length} characters."
        )


def _check_number_list_item(name: str, i: int, item: int | float, item_constraints: dict) -> None:
    item_min_value = item_constraints.get("minValue")
    if item_min_value is not None and item < item_min_value:
        raise ValueError(
            f"Job parameter {name!r} item {i} {item!r} is less than item minValue {item_min_value}."
        )
    item_max_value = item_constraints.get("maxValue")
    if item_max_value is not None and item > item_max_value:
        raise ValueError(
            f"Job parameter {name!r} item {i} {item!r} is greater than item maxValue {item_max_value}."
        )


def _to_bool_list_item(name: str, i: int, item: Any) -> bool:
    converted = _to_bool(item)
    if converted is None:
        error_type = ValueError if isinstance(item, (int, float, str)) else TypeError
        raise error_type(
            f"Job parameter {name!r} has type LIST[BOOL] but item {i} is {item!r} which is not boolean."
        )
    return converted


def _to_list(job_parameter: JobParameter, value: Any) -> list[Any]:
    """Converts a LIST[STRING], LIST[PATH], LIST[INT], LIST[FLOAT] or LIST[BOOL] value, a list
    or a string holding a JSON array, to a list and checks it against the definition's list and
    item constraints. LIST[FLOAT] items are returned as float, and LIST[BOOL] items as bool."""
    name = job_parameter["name"]
    list_type = job_parameter["type"]
    item_kind, example = _LIST_ITEM_DESCRIPTIONS[list_type]
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            raise ValueError(
                f"Job parameter {name!r} has type {list_type} but got value {value!r} which is not a JSON array of {item_kind}, such as {example}."
            ) from None
        if not isinstance(value, list):
            raise ValueError(
                f"Job parameter {name!r} has type {list_type} but got value {json.dumps(value)} which is not a JSON array."
            )
    elif not isinstance(value, list):
        raise TypeError(
            f"Job parameter {name!r} has type {list_type} but got value {value!r} of type {type(value)}."
        )

    if list_type == "LIST[BOOL]":
        # Unlike the other list types, an item accepts every spelling of a BOOL value.
        value = [_to_bool_list_item(name, i, item) for i, item in enumerate(value)]
    else:
        for i, item in enumerate(value):
            _check_list_item_type(list_type, name, i, item)

    min_length = job_parameter.get("minLength")
    if min_length is not None and len(value) < min_length:
        raise ValueError(
            f"Job parameter {name!r} has {len(value)} items but minLength is {min_length}."
        )
    max_items_cap = _MAX_LIST_ITEMS[list_type]
    max_length = min(job_parameter.get("maxLength", max_items_cap), max_items_cap)
    if len(value) > max_length:
        raise ValueError(
            f"Job parameter {name!r} has {len(value)} items but at most {max_length} are allowed."
        )

    item_constraints: dict = dict(job_parameter.get("item", {}))
    item_allowed_values = item_constraints.get("allowedValues")
    for i, item in enumerate(value):
        if list_type in _STRING_ITEM_LIST_TYPES:
            _check_string_list_item(name, i, item, item_constraints)
        elif list_type in ("LIST[INT]", "LIST[FLOAT]"):
            _check_number_list_item(name, i, item, item_constraints)
        if item_allowed_values is not None and item not in item_allowed_values:
            raise ValueError(
                f"Job parameter {name!r} item {i} {item!r} is not an allowed value from {tuple(item_allowed_values)!r}."
            )

    if list_type == "LIST[FLOAT]":
        return [float(item) for item in value]
    return list(value)


def validate_job_parameter_value(
    job_parameter: JobParameter,
    value: str | int | float | bool | list[str] | list[int] | list[float] | list[bool],
) -> str | int | float | bool | list[str] | list[int] | list[float] | list[bool]:
    """
    Validates a value for the specified parameter definition, returning the value with the correct type,
    e.g. a string "19" for an INT parameter is returned as the integer 19, and a string
    '["a", "b"]' for a LIST[STRING] parameter is returned as the list ["a", "b"]. LIST[PATH],
    LIST[INT], LIST[FLOAT] and LIST[BOOL] values are also accepted as a JSON array string.
    LIST[PATH] items are not made absolute. LIST[FLOAT]
    items are returned as float, and LIST[BOOL] items, which accept the same values as a BOOL
    parameter such as "yes" or 0, are returned as bool.
    Raises a ValueError if validation fails.

    See https://github.com/OpenJobDescription/openjd-specifications/wiki/2023-09-Template-Schemas#2-jobparameterdefinition

    Args:
        job_parameter: The parameter definition.
        value: The value to validate.
    Returns:
        The value, converted to the correct type for the parameter definition.
    """
    name = job_parameter["name"]
    param_type = job_parameter["type"]

    # First ensure the value has the correct type
    if param_type in ("STRING", "PATH"):
        if not isinstance(value, str):
            raise TypeError(
                f"Job parameter {name!r} has type {param_type} but got value {value!r} of type {type(value)}."
            )
        # CreateJob's JobParameter.path holds at most this many characters, as each pathList item does.
        if param_type == "PATH" and len(value) > _MAX_STRING_LIST_ITEM_LENGTH:
            shown = value[:40] + "..."
            raise ValueError(
                f"Job parameter {name!r} value {shown!r} is longer than the maximum of {_MAX_STRING_LIST_ITEM_LENGTH} characters."
            )
    elif param_type == "BOOL":
        converted = _to_bool(value)
        if converted is None:
            raise ValueError(
                f"Job parameter {name!r} has type BOOL but got value {value!r} which is not boolean."
            )
        value = converted
    elif param_type == "RANGE_EXPR":
        if not isinstance(value, str):
            raise TypeError(
                f"Job parameter {name!r} has type RANGE_EXPR but got value {value!r} of type {type(value)}."
            )
        try:
            parse_int_range_expr(value)
        except ValueError as e:
            raise ValueError(
                f"Job parameter {name!r} has type RANGE_EXPR but got value {value!r} which is not a valid range expression: {e}"
            ) from e
    elif param_type in _LIST_TYPES:
        # The list and per-item constraints differ from the scalar ones applied below.
        return _to_list(job_parameter, value)
    elif param_type == "INT":
        if isinstance(value, list):
            raise TypeError(f"Job parameter {name!r} has type INT but got list value {value!r}.")
        original_value = value
        try:
            if isinstance(value, str):
                value = int(value)
            else:
                value = int(value)
                # In the case of converting a float to an integer, the value gets truncated.
                # Check if the value was modified, and raise if so.
                if not isinstance(original_value, str) and value != original_value:
                    value = original_value
                    raise ValueError()
        except ValueError:
            raise ValueError(
                f"Job parameter {name!r} has type INT but got value {value!r} which is not an integer."
            )
    elif param_type == "FLOAT":
        if isinstance(value, list):
            raise TypeError(f"Job parameter {name!r} has type FLOAT but got list value {value!r}.")
        try:
            value = float(value)
        except ValueError:
            raise ValueError(
                f"Job parameter {name!r} has type FLOAT but got value {value!r} which is not floating point."
            )
    else:
        raise TypeError(
            f"The definition for job parameter {name!r} has unsupported type {param_type!r}"
        )

    # Then ensure the value satisfies the constraints in the parameter definition.
    # Assumes the parameter definition is already validated, so the existence or not
    # of constraint fields is enough, don't need to gate by type.
    min_length = job_parameter.get("minLength")
    if min_length is not None:
        if len(value) < min_length:  # type: ignore
            raise ValueError(
                f"Job parameter {name!r} value {value!r} is shorter than minLength {min_length}."
            )

    max_length = job_parameter.get("maxLength")
    if max_length is not None:
        if len(value) > max_length:  # type: ignore
            raise ValueError(
                f"Job parameter {name!r} value {value!r} is longer than maxLength {max_length}."
            )

    min_value = job_parameter.get("minValue")
    if min_value is not None:
        if value < min_value:  # type: ignore
            raise ValueError(
                f"Job parameter {name!r} value {value!r} is less than minValue {min_value}."
            )

    max_value = job_parameter.get("maxValue")
    if max_value is not None:
        if value > max_value:  # type: ignore
            raise ValueError(
                f"Job parameter {name!r} value {value!r} is greater than maxValue {max_value}."
            )

    allowed_values = job_parameter.get("allowedValues")
    if allowed_values is not None:
        if value not in allowed_values:  # type: ignore
            raise ValueError(
                f"Job parameter {name!r} value {value!r} is not an allowed value from {tuple(allowed_values)!r}."
            )

    return value


def validate_user_interface_spec(input: Any, *, parameter_name: str) -> UserInterfaceSpec:
    """Validates a job parameter's "userInterface" field as defined by Open Job Description. The
    validation allows for the union of all possible parameter "type"s.

    Note that the validation does not currently handle per-type validation (e.g. minValue only
    allowed on parameters of type "INT" / "FLOAT")

    userInterface: # @optional
        control: enum("CHECK_BOX", "CHOOSE_DIRECTORY", "CHOOSE_INPUT_FILE", "CHOOSE_OUTPUT_FILE", "DROPDOWN_LIST", "LINE_EDIT", "MULTILINE_EDIT", "SPIN_BOX")
        label: <UserInterfaceLabelString> # @optional
        groupLabel: <UserInterfaceLabelStringValue> # @optional
        fileFilters: [
            {
                label: str,
                patterns: [str]
            },
            ...
        ] # @optional
        fileFilterDefault: {
            label: str,
            patterns: [str]
        } # @optional
        singleStepDelta: <positiveint> | <positivefloat> # @optional

    Parameters
    ----------
    input : Any
        The input to validate
    parameter_name : str
        The parameter name whose "userInterface" field is being validated. This is used for
        producing user-friendly error messages

    Raises
    ------
    ValueError
        The input contains a non-valid value
    TypeError
        The input or one of its fields are not of the expected type

    Returns
    -------
    UserInterfaceSpec
        A type-cast version of the input. This is the same object reference to the input, but the
        data has been validated.
    """
    if not isinstance(input, dict):
        raise TypeError(f"Expected a dict but got {type(input).__name__}")

    # Validate "control"
    if "control" in input:
        control = input["control"]
        if control not in _VALID_UI_CONTROLS:
            quoted = (f'"{valid_ui_control}"' for valid_ui_control in _VALID_UI_CONTROLS)
            raise ValueError(
                f'Job parameter "{parameter_name}" got but expected one of ({", ".join(quoted)}) for "userInterface" -> "control" but got {control}'
            )

    # Validate "label"
    if "label" in input:
        label = input["label"]
        if not isinstance(label, str):
            raise TypeError(
                f'Job parameter "{parameter_name}" got {type(label).__name__} for "userInterface" -> "label" but expected str'
            )

    # Validate "groupLabel"
    if "groupLabel" in input:
        group_label = input["groupLabel"]
        if not isinstance(group_label, str):
            raise TypeError(
                f'Job parameter "{parameter_name}" got {type(group_label).__name__} for "userInterface" -> "groupLabel" but expected str'
            )

    # Validate "decimals"
    if "decimals" in input:
        decimals = input["decimals"]
        if type(decimals) is not int:  # noqa: E721
            raise TypeError(
                f'Job parameter "{parameter_name}" got {type(decimals).__name__} for "userInterface" -> "decimals" but expected int'
            )
        if decimals < 0:
            raise ValueError(
                f'Job parameter "{parameter_name}" got {decimals} for "userInterface" -> "decimals" but expected a non-negative int'
            )

    # Validate "singleStepDelta"
    if "singleStepDelta" in input:
        single_step_delta = input["singleStepDelta"]
        if type(single_step_delta) not in (int, float):
            raise TypeError(
                f'Job parameter "{parameter_name}" got but expected float for "userInterface" -> "singleStepDelta", but got {type(single_step_delta).__name__}'
            )
        if single_step_delta <= 0:
            raise ValueError(
                f'Job parameter "{parameter_name}" got {single_step_delta} for "userInterface" -> "singleStepDelta" but expected a positive number'
            )

    if "fileFilters" in input:
        file_filters = input["fileFilters"]
        if not isinstance(file_filters, list):
            raise TypeError(
                f'Job parameter "{parameter_name}" got but expected list for "userInterface" -> "fileFilters", but got {type(file_filters).__name__}'
            )
        for i, file_filter in enumerate(file_filters):
            validate_user_interface_file_filter(
                file_filter,
                parameter_name=parameter_name,
                field_path=f'"userInterface" -> "fileFilters" -> [{i}]',
            )

    if "fileFilterDefault" in input:
        file_filter_default = input["fileFilterDefault"]
        validate_user_interface_file_filter(
            file_filter_default,
            parameter_name=parameter_name,
            field_path='"userInterface" -> "fileFilterDefault"',
        )

    return cast(UserInterfaceSpec, input)


def validate_user_interface_file_filter(
    input: Any,
    *,
    parameter_name: str,
    field_path: str,
) -> UserInterfaceFileFilter:
    """Validates values in a job parameter structure in the following object paths:

    1.  "userInterface" -> "fileFilters" -> []
    2.  "userInterface" -> "fileFilterDefault"

    The expected format is:

        label: str
        patterns: [str, ...]

    Parameters
    ----------
    input : Any
        The input to validate
    parameter_name : str
        The parameter name whose "userInterface" field is being validated. This is used for
        producing user-friendly error messages
    field_path : str
        The JSON path to the field within the job parameter being validated. This is used to produce
        user-friendly error messages. For example:

            "userInterface" -> "fileFilters" -> [1]

    Raises
    ------
    TypeError
        When a field contains an incorrect type
    ValueError
        When a field contains a non-valid value

    Returns
    -------
    UserInterfaceFileFilter
        A type-cast version of the input. This is the same object reference to the input, but the
        data has been validated.
    """

    if not isinstance(input, dict):
        raise TypeError(
            f'Job parameter "{parameter_name}" got {type(input).__name__} for {field_path} but expected a dict'
        )

    # Validation for "label"
    if "label" not in input:
        raise ValueError(
            f'Job parameter "{parameter_name}" is missing required key {field_path} -> "label"'
        )
    else:
        label = input["label"]
        if not isinstance(label, str):
            raise TypeError(
                f'Job parameter "{parameter_name}" got {type(label).__name__} for {field_path} -> "label" but expected str'
            )

    # Validation for "patterns"
    if "patterns" not in input:
        raise ValueError(
            f'Job parameter "{parameter_name}" is missing required key {field_path} -> "patterns"'
        )
    else:
        patterns = input["patterns"]
        if not isinstance(patterns, list):
            raise TypeError(
                f'Job parameter "{parameter_name}" got {type(patterns).__name__} for {field_path} -> "patterns" but expected list'
            )
        for i, pattern in enumerate(patterns):
            if not isinstance(pattern, str):
                raise TypeError(
                    f'Job parameter "{parameter_name}" got "{pattern!r}" for {field_path} -> "patterns" [{i}] but expected str'
                )
            elif not (0 < len(pattern) <= 20):
                raise ValueError(
                    f'Job parameter "{parameter_name}" got "{pattern}" for {field_path} -> "patterns" [{i}] but must be between 1 and 20 characters'
                )

    return cast(UserInterfaceFileFilter, input)


def merge_queue_job_parameters(
    *,
    job_parameters: list[JobParameter],
    queue_parameters: list[JobParameter],
    queue_id: str | None = None,
) -> list[JobParameter]:
    """This function merges the queue environment parameters and the job bundle parameters. This
    primarily functions as a set union operation with a few added semantics:

    1.  The merge validates that parameters with the same name agree on the parameter type,
        otherwise a DeadlineOperationError exception is raised
    2.  If both the queue and job bundle have a parameter with the same name that specify default
        values, then the job bundle's default will take priority

    Parameters
    ----------
    job_parameters : list[JobParameter]
        The parameters from the job bundle
    queue_parameters : list[JobParameter]
        The parameters from the target queue's environment

    Raises
    ------
    DeadlineOperationError
        Raised if the job bundle and queue share a parameter with the same name but different types

    Returns
    -------
    list[JobParameter]
        The merged parameters
    """

    # Make a dict structure of the queue parameters for easy lookup by name.
    # We later mutate the values, so the values are shallow copies of the queue's parameter dicts
    collected_parameters: dict[str, JobParameter] = {
        param["name"]: param.copy() for param in queue_parameters
    }

    ParameterTypeMismatch = namedtuple("ParameterTypeMismatch", ("param_name", "differences"))

    param_mismatches: list[ParameterTypeMismatch] = []

    for job_parameter in job_parameters:
        job_parameter_name = job_parameter["name"]
        if job_parameter_name in collected_parameters:
            # Check for type mismatch between queue and job bundle

            collected_parameter = collected_parameters[job_parameter_name]

            # If the job parameter includes a value, always copy it to the collected parameter
            if "value" in job_parameter:
                collected_parameter["value"] = job_parameter["value"]
                # If the job parameter doesn't include a definition, there's nothing more to merge
                if {"name", "value"} == set(job_parameter.keys()):
                    continue

            # If the job parameter includes a default, always copy it to the collected parameter
            if "default" in job_parameter:
                collected_parameter["default"] = job_parameter["default"]

            differences = parameter_definition_difference(collected_parameter, job_parameter)
            # Ignore any differences in the default value
            differences = [name for name in differences if name != "default"]

            if differences:
                param_mismatches.append(
                    ParameterTypeMismatch(param_name=job_parameter_name, differences=differences)
                )
        else:
            # app-specific parameters have implicit definitions based on their "name"
            if {"name", "value"} == job_parameter.keys() and ":" not in job_parameter_name:
                raise DeadlineOperationError(
                    f'Parameter value was provided for an undefined parameter "{job_parameter_name}"'
                )
            collected_parameters[job_parameter_name] = job_parameter.copy()

    if param_mismatches:
        param_strs = [
            f'\t{param_mismatch.param_name}: differences for fields "{param_mismatch.differences}"'
            for param_mismatch in param_mismatches
        ]
        queue_str = f"queue ({queue_id})" if queue_id else "queue"
        raise DeadlineOperationError(
            f"The target {queue_str} and job bundle have conflicting parameter definitions:\n\n"
            + "\n".join(param_strs)
        )

    return list(collected_parameters.values())


def _parse_path_list(value: Any) -> list[str] | None:
    """Returns the items of a LIST[PATH] value, a list of strings or a string holding a JSON
    array of them, or None if the value is neither."""
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return None
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return list(value)
    return None


def _absolute_path_value(path: str, *, allow_uri_path_values: bool) -> str:
    """Makes a PATH value, or a LIST[PATH] item, given on the command line absolute against
    the current working directory. An empty value stays empty, and with the EXPR extension
    a URI such as s3://bucket/key is kept as it is."""
    if not path or (allow_uri_path_values and is_uri(path)):
        return path
    return _lexical_join(os.getcwd(), path)


def _path_list_with_absolute_items(
    parameter: JobParameter, value: Any, *, allow_uri_path_values: bool = False
) -> Any:
    """Returns a LIST[PATH] value given on the command line as a list, with each relative item
    made absolute against the current working directory, as a PATH value is. Empty items stay
    empty, and with the EXPR extension URI items are kept. A value that is not a list of
    strings is returned unchanged, for submission to report."""
    items = _parse_path_list(value)
    if items is None:
        return value
    return [
        _absolute_path_value(item, allow_uri_path_values=allow_uri_path_values) for item in items
    ]


def _add_path_asset_reference(
    asset_references: AssetReferences,
    parameter: JobParameter,
    path: str,
    *,
    allow_uri_path_values: bool = False,
) -> None:
    """Adds a PATH value, or one item of a LIST[PATH] value, to the asset references its
    parameter's dataFlow and objectType select. With the EXPR extension, a URI is not a
    local path, so job attachments neither upload, download nor map it."""
    data_flow = parameter.get("dataFlow", "NONE")
    if path == "":
        # An empty value names no path, so there is nothing to reference or transfer.
        return
    if allow_uri_path_values and is_uri(path):
        if data_flow != "NONE":
            _logger.info(
                "Job parameter %r value %r is a URI, so job attachments do not transfer it.",
                parameter["name"],
                path,
            )
        return
    if data_flow == "NONE":
        # This path is referenced, but its contents are not necessarily
        # input or output.
        asset_references.referenced_paths.add(path)
    else:
        object_type = parameter.get("objectType")

        if "IN" in data_flow:
            if object_type == "FILE":
                asset_references.input_filenames.add(path)
            else:
                asset_references.input_directories.add(path)
        if "OUT" in data_flow:
            if object_type == "FILE":
                # TODO: When job attachments supports output files in addition to directories, change this to
                #       add the filename instead.
                asset_references.output_directories.add(os.path.dirname(path))
            else:
                asset_references.output_directories.add(path)


def apply_job_parameters(
    job_parameters: list[dict[str, Any]],
    job_bundle_dir: str,
    parameters: list[JobParameter],
    asset_references: AssetReferences,
    *,
    allow_uri_path_values: bool = False,
) -> None:
    """
    Modifies the provided parameters and asset_references to incorporate
    the job_parameters and to resolve any relative paths in PATH and LIST[PATH] parameters.

    The following actions are taken:
    - Any job_parameters provided set or replace the "value" key in the corresponding
      job_bundle_parameters entry.
    - Any job_parameters for a PATH, that is a relative path, is made absolute by joining
      with the current working directory. So is each relative item of a LIST[PATH].
    - Any job_bundle_parameters for a PATH, not set by job_parameters, that is a
      relative path, is made absolute by joining with the job bundle directory.
    - Any PATH parameters that have IN, OUT, or INOUT assetReferences metadata are
      added to the appropriate asset_references entries. Each non-empty item of a
      LIST[PATH] is added the same way, as if it were a PATH with the same definition.

    Pass allow_uri_path_values=True for a job template that uses OpenJD's EXPR extension,
    where a PATH value or LIST[PATH] item may be a URI such as s3://bucket/key. A URI is
    neither made absolute nor added to asset_references.
    """
    # Convert the job_parameters to a dict for efficient lookup
    param_dict: dict[str, Any] = {
        parameter["name"]: parameter["value"] for parameter in job_parameters
    }

    for parameter in parameters:
        # Get the definition from the job bundle
        parameter_type = parameter.get("type", None)
        if not parameter_type:
            continue

        parameter_name = parameter["name"]

        # Apply the job_parameters value if available
        parameter_value = param_dict.pop(parameter_name, None)
        if parameter_value is not None:
            # Make relative PATH parameter values absolute by joining with the current
            # working directory. As OpenJD specifies, this applies even when allowedValues
            # constrains the value, which is checked after the join.
            if parameter_type == "PATH":
                if parameter_value == "":
                    continue
                parameter_value = _absolute_path_value(
                    parameter_value, allow_uri_path_values=allow_uri_path_values
                )
            elif parameter_type == "LIST[PATH]":
                parameter_value = _path_list_with_absolute_items(
                    parameter, parameter_value, allow_uri_path_values=allow_uri_path_values
                )
            parameter["value"] = parameter_value
        else:
            parameter_value = parameter.get("value", parameter.get("default"))
            if parameter_value is None:
                raise DeadlineOperationError(
                    f"Job Template for job bundle {job_bundle_dir}:\nNo parameter value provided for Job Template parameter {parameter_name}, and it has no default value."
                )

        if parameter_type in ("PATH", "LIST[PATH]"):
            data_flow = parameter.get("dataFlow", "NONE")
            if data_flow not in ("NONE", "IN", "OUT", "INOUT"):
                raise DeadlineOperationError(
                    f"Job Template for job bundle {job_bundle_dir}:\nJob Template parameter {parameter_name} had an incorrect "
                    + f"value {data_flow} for 'dataFlow'. Valid values are "
                    + "['NONE', 'IN', 'OUT', 'INOUT']"
                )

        # If it's a PATH parameter with dataFlow, add it to asset_references
        if parameter_type == "PATH":
            _add_path_asset_reference(
                asset_references,
                parameter,
                parameter_value,
                allow_uri_path_values=allow_uri_path_values,
            )
        elif parameter_type == "LIST[PATH]":
            # A value that is not a list of paths is reported when the parameters are
            # formatted for CreateJob.
            for item in _parse_path_list(parameter_value) or []:
                _add_path_asset_reference(
                    asset_references,
                    parameter,
                    item,
                    allow_uri_path_values=allow_uri_path_values,
                )


def _resolve_bundle_path_default(
    bundle_dir: str, name: str, default: str, *, item_index: Optional[int] = None
) -> str:
    """Returns the absolute path of a PATH default, or of item ``item_index`` of a LIST[PATH]
    default, which must be relative and resolve within the job bundle directory."""
    what = "Default PATH" if item_index is None else f"Default LIST[PATH] item {item_index}"
    # Not os.path.isabs, which before Python 3.11 reads a UNC path naming a
    # share as relative -- such a default reached the containment check below
    # and failed there, reporting the wrong reason.
    if is_absolute_path(default, path_module=os.path):
        raise DeadlineOperationError(
            f"Job Template for job bundle {bundle_dir}:\n{what} '{default}' for parameter '{name}' is absolute.\nPATH values must be relative, and must resolve within the Job Bundle directory."
        )
    bundle_real_path = os.path.realpath(bundle_dir)
    default_real_path = os.path.realpath(os.path.join(bundle_real_path, default))
    if not is_path_contained(default_real_path, bundle_real_path, path_module=os.path):
        raise DeadlineOperationError(
            f"Job Template for job bundle {bundle_dir}:\n{what} '{default_real_path}' for parameter '{name}' specifies files outside of Job Bundle directory '{bundle_real_path}'.\nPATH values must be relative, and must resolve within the Job Bundle directory."
        )

    return _lexical_join(os.path.abspath(bundle_dir), default)


def _lexical_join(base_dir: str, path: str) -> str:
    """Joins a relative path with an absolute base directory and normalizes the result
    lexically, as OpenJD specifies for PATH values: "." components are removed and ".."
    components are applied. Unlike os.path.abspath on Windows, which calls
    GetFullPathName, this keeps trailing dots and spaces, so a component such as "..." is
    kept."""
    return os.path.normpath(os.path.join(base_dir, path))


def read_job_bundle_parameters(bundle_dir: str) -> list[JobParameter]:
    """
    Reads the parameter definitions and parameter values from the job bundle. For
    any relative PATH parameters with data flow where no parameter value is supplied,
    it sets the value to that path relative to the job bundle directory.

    Return format:
    ```
    [
        {
            "name": <parameter name>,
            <all fields from the the "parameters" value in template.json/yaml>
            "value": <if provided from parameter_values.json/yaml>
        },
        ...
    ]
    ```
    """

    template = read_yaml_or_json_object(bundle_dir=bundle_dir, filename="template", required=True)
    parameter_values = read_yaml_or_json_object(
        bundle_dir=bundle_dir, filename="parameter_values", required=False
    )

    if not isinstance(template, dict):
        raise DeadlineOperationError(
            f"Job Template for job bundle {bundle_dir}:\nThe document does not contain a top-level object."
        )

    # Get the spec version of the template
    if "specificationVersion" not in template:
        raise DeadlineOperationError(
            f"Job Template for job bundle {bundle_dir}:\nDocument does not contain a specificationVersion."
        )
    elif template.get("specificationVersion") not in ["jobtemplate-2023-09"]:
        raise DeadlineOperationError(
            f"Job Template for job bundle {bundle_dir}:\nDocument has an unsupported specificationVersion: {template.get('specificationVersion')}"
        )

    # Start with the template parameters, converting them from a list into a dictionary
    template_parameters: dict[str, dict[str, Any]] = {}
    if "parameterDefinitions" in template:
        # parameters are a list of objects. Convert it to a map
        # from name -> parameter
        if not isinstance(template["parameterDefinitions"], list):
            raise DeadlineOperationError(
                f"Job Template for job bundle {bundle_dir}:\nJob parameter definitions must be a list."
            )
        _normalize_parameter_type_case(template)
        template_parameters = {param["name"]: param for param in template["parameterDefinitions"]}

    # Add the parameter values where provided
    if parameter_values:
        for parameter_value in parameter_values.get("parameterValues", []):
            name = parameter_value["name"]
            if name in template_parameters:
                template_parameters[name]["value"] = parameter_value["value"]
            else:
                # Keep the other parameter values around, they may be
                # provide values for queue parameters or specific render farm
                # values such as "deadline:*"
                template_parameters[name] = parameter_value

    # Make valueless PATH parameters with a 'default' absolute by joining with the job bundle
    # directory, and each item of a LIST[PATH] default the same way. As OpenJD specifies, this
    # applies even when allowedValues constrains the default, so the joined value is checked
    # against the constraints here; the default as written is checked by
    # validate_job_parameter below. With the EXPR extension, a URI default is not a path in the
    # bundle and is kept as it is.
    allow_uri_path_values = _has_expr_extension(template)
    joined_defaults: set[str] = set()

    def resolve_default(name: str, default: str, item_index: Optional[int] = None) -> str:
        if not default or (allow_uri_path_values and is_uri(default)):
            return default
        return _resolve_bundle_path_default(bundle_dir, name, default, item_index=item_index)

    for name, parameter in template_parameters.items():
        if "value" in parameter:
            continue
        if parameter["type"] == "PATH":
            default = parameter.get("default")
            if isinstance(default, str) and default:
                parameter["value"] = resolve_default(name, default)
                joined_defaults.add(name)
        elif parameter["type"] == "LIST[PATH]":
            # A malformed default is reported by validate_job_parameter below.
            default = parameter.get("default")
            if isinstance(default, list) and all(isinstance(value, str) for value in default):
                parameter["value"] = [
                    resolve_default(name, value, index) for index, value in enumerate(default)
                ]
                joined_defaults.add(name)

    # Rearrange the dict from the template into a list
    parameters = [
        validate_job_parameter({"name": name, **values})
        for name, values in template_parameters.items()
    ]

    for param in parameters:
        if param["name"] in joined_defaults:
            try:
                validate_job_parameter_value(param, param["value"])
            except (ValueError, TypeError) as e:
                raise DeadlineOperationError(
                    f"Job Template for job bundle {bundle_dir}:\nThe default for parameter "
                    f"'{param['name']}', joined with the Job Bundle directory, is not a valid "
                    f"value: {e}\nThe constraints of a PATH parameter apply to the joined path, so "
                    "use allowedValues with absolute paths or URIs, not a relative default."
                ) from e

    # Validate hidden parameters have values
    invalid_params = []
    for param in parameters:
        if param.get("userInterface", {}).get("control") == "HIDDEN":
            if "value" not in param and "default" not in param:
                invalid_params.append(param["name"])

    if invalid_params:
        if len(invalid_params) == 1:
            message = f'Job bundle validation failed:\nHidden parameter "{invalid_params[0]}" is missing a value.'
        else:
            param_list = ", ".join(f'"{name}"' for name in invalid_params)
            message = (
                f"Job bundle validation failed:\nHidden parameters {param_list} are missing values."
            )

        message += " Hidden parameters must have either a default value in the template or a value in parameter_values.yaml."
        raise DeadlineOperationError(message)

    return parameters


_SUPPORTED_CONTROLS_FOR_TYPE = {
    "STRING": {"LINE_EDIT", "MULTILINE_EDIT", "DROPDOWN_LIST", "CHECK_BOX", "HIDDEN"},
    "PATH": {
        "CHOOSE_INPUT_FILE",
        "CHOOSE_OUTPUT_FILE",
        "CHOOSE_DIRECTORY",
        "DROPDOWN_LIST",
        "HIDDEN",
    },
    "INT": {"SPIN_BOX", "DROPDOWN_LIST", "HIDDEN"},
    "FLOAT": {"SPIN_BOX", "DROPDOWN_LIST", "HIDDEN"},
    "BOOL": {"CHECK_BOX", "HIDDEN"},
    "RANGE_EXPR": {"LINE_EDIT", "HIDDEN"},
    "LIST[STRING]": {"LINE_EDIT_LIST", "HIDDEN"},
    "LIST[PATH]": {
        "CHOOSE_INPUT_FILE_LIST",
        "CHOOSE_OUTPUT_FILE_LIST",
        "CHOOSE_DIRECTORY_LIST",
        "HIDDEN",
    },
    "LIST[INT]": {"SPIN_BOX_LIST", "HIDDEN"},
    "LIST[FLOAT]": {"SPIN_BOX_LIST", "HIDDEN"},
    "LIST[BOOL]": {"CHECK_BOX_LIST", "HIDDEN"},
}


def get_ui_control_for_parameter_definition(param_def: JobParameter) -> str:
    """Returns the UI control for the given parameter definition, determining
    the default if not explicitly set."""
    # If it's explicitly provided, return that
    control = param_def.get("userInterface", {}).get("control")
    param_type = param_def["type"]
    if not control:
        if "allowedValues" in param_def:
            control = "DROPDOWN_LIST"
        elif param_type == "STRING":
            return "LINE_EDIT"
        elif param_type == "RANGE_EXPR":
            return "LINE_EDIT"
        elif param_type == "LIST[STRING]":
            return "LINE_EDIT_LIST"
        elif param_type in ("LIST[INT]", "LIST[FLOAT]"):
            return "SPIN_BOX_LIST"
        elif param_type == "LIST[BOOL]":
            return "CHECK_BOX_LIST"
        elif param_type == "PATH":
            if param_def.get("objectType", "DIRECTORY") == "FILE":
                if param_def.get("dataFlow", "NONE") == "OUT":
                    return "CHOOSE_OUTPUT_FILE"
                else:
                    return "CHOOSE_INPUT_FILE"
            else:
                return "CHOOSE_DIRECTORY"
        elif param_type == "LIST[PATH]":
            if param_def.get("objectType", "DIRECTORY") == "FILE":
                if param_def.get("dataFlow", "NONE") == "OUT":
                    return "CHOOSE_OUTPUT_FILE_LIST"
                else:
                    return "CHOOSE_INPUT_FILE_LIST"
            else:
                return "CHOOSE_DIRECTORY_LIST"
        elif param_type in ("INT", "FLOAT"):
            return "SPIN_BOX"
        elif param_type == "BOOL":
            return "CHECK_BOX"
        else:
            raise DeadlineOperationError(
                f"The job template parameter '{param_def.get('name', '<unnamed>')}' "
                + f"specifies an unsupported type '{param_type}'."
            )

    if control not in _SUPPORTED_CONTROLS_FOR_TYPE[param_type]:
        raise DeadlineOperationError(
            f"The job template parameter '{param_def.get('name', '<unnamed>')}' "
            + f"specifies an unsupported control '{control}' for its type '{param_type}'."
        )

    if control == "DROPDOWN_LIST" and "allowedValues" not in param_def:
        raise DeadlineOperationError(
            f"The job template parameter '{param_def.get('name', '<unnamed>')}' "
            + "must supply 'allowedValues' if it uses a DROPDOWN_LIST control."
        )

    return control


def _parameter_definition_fields_equivalent(
    lhs: JobParameter,
    rhs: JobParameter,
    field_name: str,
    set_comparison: bool = False,
) -> bool:
    lhs_value = lhs.get(field_name)
    rhs_value = rhs.get(field_name)
    if set_comparison and lhs_value is not None and rhs_value is not None:
        # Used to type-narrow at type-check time
        assert isinstance(lhs_value, list) and isinstance(rhs_value, list)
        return set(lhs_value) == set(rhs_value)
    else:
        return lhs_value == rhs_value


def parameter_definition_difference(
    lhs: JobParameter, rhs: JobParameter, *, ignore_missing: bool = False
) -> list[str]:
    """Compares the two parameter definitions, returning a list of fields which differ.
    Does not compare the userInterface properties.

    Parameters
    ----------
    lhs : JobParameter
        The "left-hand-side" job parameter to compare
    rhs : JobParameter
        The "right-hand-side" job parameter to compare
    ignore_missing : bool
        Whether to ignore missing fields in the comparison. Defaults to False

    Returns
    -------
    list[str]
        The fields whose values differ between lhs and rhs
    """
    differences = []
    # Compare these properties as values
    for name in (
        "name",
        "type",
        "minValue",
        "maxValue",
        "minLength",
        "maxLength",
        "dataFlow",
        "objectType",
    ):
        if ignore_missing and (name not in lhs or name not in rhs):
            continue
        if not _parameter_definition_fields_equivalent(lhs, rhs, name):
            differences.append(name)
    # Compare these properties as sets
    for name in ("allowedValues",):
        if ignore_missing and (name not in lhs or name not in rhs):
            continue
        if not _parameter_definition_fields_equivalent(lhs, rhs, name, set_comparison=True):
            differences.append(name)
    if not (ignore_missing and ("item" not in lhs or "item" not in rhs)):
        if not _item_constraints_equivalent(lhs.get("item"), rhs.get("item")):
            differences.append("item")
    return differences


def _item_constraints_equivalent(lhs: Any, rhs: Any) -> bool:
    """Compares list parameter "item" constraints, with allowedValues compared as sets like
    the top-level allowedValues. An absent "item" is the same as one with no constraints."""
    lhs = {} if lhs is None else lhs
    rhs = {} if rhs is None else rhs
    if not isinstance(lhs, dict) or not isinstance(rhs, dict):
        return lhs == rhs
    for field in ("minLength", "maxLength", "minValue", "maxValue"):
        if lhs.get(field) != rhs.get(field):
            return False
    lhs_allowed, rhs_allowed = lhs.get("allowedValues"), rhs.get("allowedValues")
    if lhs_allowed is None or rhs_allowed is None:
        return lhs_allowed == rhs_allowed
    return set(lhs_allowed) == set(rhs_allowed)
