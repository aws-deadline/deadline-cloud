# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Test cases for deadline.client.job_bundle.parameters.validate_user_interface_file_filter"""

from __future__ import annotations
from typing import Any

import pytest

from deadline.client.job_bundle import parameters


@pytest.fixture
def parameter_name() -> str:
    return "param"


@pytest.fixture
def field_path() -> str:
    return '"userInterface" -> "fileFilterDefault"'


@pytest.fixture
def valid_label() -> str:
    return "mylabel"


@pytest.fixture(params=(1, 20))
def valid_pattern_length(request: pytest.FixtureRequest) -> int:
    return request.param


@pytest.fixture
def valid_pattern(valid_pattern_length: int) -> str:
    return "a" * valid_pattern_length


@pytest.fixture
def valid_file_filter(
    valid_label: str,
    valid_pattern: str,
) -> parameters.UserInterfaceFileFilter:
    return {
        "label": valid_label,
        "patterns": [valid_pattern],
    }


def test_validate_user_interface_file_filter_valid(
    valid_file_filter: parameters.UserInterfaceFileFilter,
    parameter_name: str,
    field_path: str,
) -> None:
    """Tests that valid input will not raise an exception when a valid file filter is passed to
    validate_user_interface_file_filter and that the return value is a reference to the input.
    """
    # WHEN
    result = parameters.validate_user_interface_file_filter(
        valid_file_filter,
        parameter_name=parameter_name,
        field_path=field_path,
    )

    # THEN
    assert result is valid_file_filter


@pytest.mark.parametrize(
    argnames="field",
    argvalues=("label", "patterns"),
)
def test_validate_user_interface_file_filter_nonvalid_missing(
    valid_file_filter: parameters.UserInterfaceFileFilter,
    parameter_name: str,
    field_path: str,
    field: str,
) -> None:
    """Tests that valid input will raise an exception when a required field is missing from the
    input passed to validate_user_interface_file_filter."""
    # GIVEN
    file_filter = valid_file_filter.copy()
    del file_filter[field]  # type: ignore

    # WHEN
    def when() -> None:
        parameters.validate_user_interface_file_filter(
            file_filter,
            parameter_name=parameter_name,
            field_path=field_path,
        )

    # THEN
    with pytest.raises(ValueError) as ctx:
        when()
    assert (
        str(ctx.value)
        == f'Job parameter "{parameter_name}" is missing required key {field_path} -> "{field}"'
    )


@pytest.mark.parametrize(
    argnames="label",
    argvalues=(
        pytest.param(1, id="int"),
        pytest.param(1.5, id="float"),
        pytest.param(True, id="bool"),
        pytest.param(None, id="none"),
        pytest.param([], id="list"),
        pytest.param({}, id="dict"),
    ),
)
def test_validate_user_interface_file_filter_nonvalid_label_type(
    label: Any,
    parameter_name: str,
    field_path: str,
) -> None:
    """Tests that a nonvalid type passed for the \"label\" field will raise a TypeError
    with a user-friendly error message"""
    # GIVEN
    file_filter = {
        "label": label,
        "patterns": [],
    }

    # WHEN
    def when() -> None:
        parameters.validate_user_interface_file_filter(
            file_filter,
            parameter_name=parameter_name,
            field_path=field_path,
        )

    # THEN
    with pytest.raises(TypeError) as ctx:
        when()
    assert (
        str(ctx.value)
        == f'Job parameter "{parameter_name}" got {type(label).__name__} for {field_path} -> "label" but expected str'
    )


@pytest.mark.parametrize(
    argnames="pattern",
    argvalues=(
        pytest.param(1, id="int"),
        pytest.param(1.5, id="float"),
        pytest.param(True, id="bool"),
        pytest.param(None, id="none"),
        pytest.param([], id="list"),
        pytest.param({}, id="dict"),
    ),
)
def test_validate_user_interface_file_filter_nonvalid_patterns_type(
    valid_label: str,
    pattern: str,
    parameter_name: str,
    field_path: str,
) -> None:
    """Tests that a nonvalid type passed for the \"patterns\" field will raise a TypeError
    with a user-friendly error message"""
    # GIVEN
    file_filter = {
        "label": valid_label,
        "patterns": [pattern],
    }

    # WHEN
    def when() -> None:
        parameters.validate_user_interface_file_filter(
            file_filter,
            parameter_name=parameter_name,
            field_path=field_path,
        )

    # THEN
    with pytest.raises(TypeError) as ctx:
        when()
    assert (
        str(ctx.value)
        == f'Job parameter "{parameter_name}" got "{repr(pattern)}" for {field_path} -> "patterns" [0] but expected str'
    )


@pytest.mark.parametrize(
    argnames="pattern_length",
    argvalues=(0, 21),
)
def test_validate_user_interface_file_filter_nonvalid_pattern_length(
    valid_label: str,
    pattern_length: int,
    parameter_name: str,
    field_path: str,
) -> None:
    """Tests that a nonvalid type passed for the \"label\" field will raise a TypeError
    with a user-friendly error message"""
    # GIVEN
    pattern = "a" * pattern_length
    file_filter = {
        "label": valid_label,
        "patterns": [pattern],
    }

    # WHEN
    def when() -> None:
        parameters.validate_user_interface_file_filter(
            file_filter,
            parameter_name=parameter_name,
            field_path=field_path,
        )

    # THEN
    with pytest.raises(ValueError) as ctx:
        when()
    assert (
        str(ctx.value)
        == f'Job parameter "{parameter_name}" got "{pattern}" for {field_path} -> "patterns" [0] but must be between 1 and 20 characters'
    )
