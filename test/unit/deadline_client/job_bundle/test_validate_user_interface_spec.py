# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Test cases for deadline.client.job_bundle.parameters.validate_user_interface_spec"""


from __future__ import annotations
import re
from typing import Any, Dict, cast
from unittest.mock import patch

import pytest

from deadline.client.job_bundle import parameters


@pytest.fixture(
    params=(
        "CHECK_BOX",
        "CHOOSE_DIRECTORY",
        "CHOOSE_INPUT_FILE",
        "CHOOSE_OUTPUT_FILE",
        "DROPDOWN_LIST",
        "LINE_EDIT",
        "MULTILINE_EDIT",
        "SPIN_BOX",
    ),
    ids=(
        "control-CHECK_BOX",
        "control-CHOOSE_DIRECTORY",
        "control-CHOOSE_INPUT_FILE",
        "control-CHOOSE_OUTPUT_FILE",
        "control-DROPDOWN_LIST",
        "control-LINE_EDIT",
        "control-MULTILINE_EDIT",
        "control-SPIN_BOX",
    ),
)
def valid_control(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture
def valid_label() -> str:
    return "alabel"


@pytest.fixture
def valid_group_label() -> str:
    return "agrouplabel"


@pytest.fixture
def valid_decimal() -> int:
    return 1


@pytest.fixture
def valid_single_step_delta() -> float:
    return 1.5


@pytest.fixture
def parameter_name() -> str:
    return "myparam"


@pytest.fixture
def valid_user_interface_spec(
    valid_control: str,
    valid_label: str,
    valid_group_label: str,
    valid_decimal: int,
    valid_single_step_delta: float,
) -> parameters.UserInterfaceSpec:
    return {
        "control": valid_control,
        "label": valid_label,
        "groupLabel": valid_group_label,
        "decimal": valid_decimal,
        "singleStepDelta": valid_single_step_delta,
    }


def test_validate_user_interface_spec_valid(
    valid_user_interface_spec: parameters.UserInterfaceSpec,
    parameter_name: str,
) -> None:
    """Tests that when calling validate_user_interface_spec with a valid user interface spec
    that no exception is raised"""
    # WHEN
    result = parameters.validate_user_interface_spec(
        valid_user_interface_spec,
        parameter_name=parameter_name,
    )

    # THEN
    assert result is valid_user_interface_spec


@pytest.mark.parametrize(
    argnames="field",
    argvalues=("control", "label", "groupLabel", "decimal", "singleStepDelta"),
)
# Fix the valid_control fixture to a single value so we don't parametrize on it more than
# once. It's value does not matter to the test
@pytest.mark.parametrize(
    argnames="valid_control",
    argvalues=("CHECK_BOX",),
)
def test_validate_user_interface_spec_valid_optional_missing(
    valid_user_interface_spec: parameters.UserInterfaceSpec,
    field: str,
    parameter_name: str,
) -> None:
    """Tests that optional fields for the user interface are still valid when missing:

    - control
    - label
    - groupLabel
    - decimal
    - singleStepDelta
    """
    # GIVEN
    user_interface_spec = cast(Dict[str, Any], valid_user_interface_spec.copy())
    del user_interface_spec[field]

    # WHEN
    parameters.validate_user_interface_spec(
        user_interface_spec,
        parameter_name=parameter_name,
    )


@pytest.mark.parametrize(
    argnames="control",
    argvalues=(
        pytest.param(1, id="int"),
        pytest.param(1.2, id="float"),
        pytest.param(True, id="bool"),
        pytest.param(None, id="none"),
        pytest.param([], id="list"),
        pytest.param({}, id="dict"),
    ),
)
def test_validate_user_interface_spec_nonvalid_control_type(
    control: Any,
    parameter_name: str,
) -> None:
    """Tests that passing a value with an nonvalid "control" value raises an exception"""
    # GIVEN
    user_interface_spec: dict = {"control": control}

    # WHEN
    def when() -> None:
        parameters.validate_user_interface_spec(
            user_interface_spec,
            parameter_name=parameter_name,
        )

    # THEN
    with pytest.raises(ValueError) as ctx:
        when()
    ctx.match(
        rf'Job parameter "{re.escape(parameter_name)}" got but expected one of \(.*\) for "userInterface" -> "control" but got {re.escape(str(control))}'
    )


@pytest.mark.parametrize(
    argnames="label",
    argvalues=(
        pytest.param(1, id="int"),
        pytest.param(1.2, id="float"),
        pytest.param(True, id="bool"),
        pytest.param(None, id="none"),
        pytest.param([], id="list"),
        pytest.param({}, id="dict"),
    ),
)
def test_validate_user_interface_spec_nonvalid_label_type(
    label: Any,
    parameter_name: str,
) -> None:
    """Tests that passing a value with an nonvalid "label" value raises an exception"""
    # GIVEN
    user_interface_spec: dict = {"label": label}

    # WHEN
    def when() -> None:
        parameters.validate_user_interface_spec(
            user_interface_spec,
            parameter_name=parameter_name,
        )

    # THEN
    with pytest.raises(TypeError) as ctx:
        when()
    assert (
        str(ctx.value)
        == f'Job parameter "{parameter_name}" got {type(label).__name__} for "userInterface" -> "label" but expected str'
    )


@pytest.mark.parametrize(
    argnames="group_label",
    argvalues=(
        pytest.param(1, id="int"),
        pytest.param(1.2, id="float"),
        pytest.param(True, id="bool"),
        pytest.param(None, id="none"),
        pytest.param([], id="list"),
        pytest.param({}, id="dict"),
    ),
)
def test_validate_user_interface_spec_nonvalid_group_label_type(
    group_label: Any,
    parameter_name: str,
) -> None:
    """Tests that passing a value with an nonvalid "groupLabel" value raises an exception"""
    # GIVEN
    user_interface_spec: dict = {"groupLabel": group_label}

    # WHEN
    def when() -> None:
        parameters.validate_user_interface_spec(
            user_interface_spec,
            parameter_name=parameter_name,
        )

    # THEN
    with pytest.raises(TypeError) as ctx:
        when()
    assert (
        str(ctx.value)
        == f'Job parameter "{parameter_name}" got {type(group_label).__name__} for "userInterface" -> "groupLabel" but expected str'
    )


@pytest.mark.parametrize(
    argnames="decimals",
    argvalues=(
        pytest.param(1.2, id="float"),
        pytest.param("a", id="str"),
        pytest.param(True, id="bool"),
        pytest.param(None, id="none"),
        pytest.param([], id="list"),
        pytest.param({}, id="dict"),
    ),
)
def test_validate_user_interface_spec_nonvalid_decimals_type(
    decimals: Any,
    parameter_name: str,
) -> None:
    """Tests that passing a value with an nonvalid "decimals" value raises an exception"""
    # GIVEN
    user_interface_spec: dict = {"decimals": decimals}

    # WHEN
    def when() -> None:
        parameters.validate_user_interface_spec(
            user_interface_spec,
            parameter_name=parameter_name,
        )

    # THEN
    with pytest.raises(TypeError) as ctx:
        when()
    assert (
        str(ctx.value)
        == f'Job parameter "{parameter_name}" got {type(decimals).__name__} for "userInterface" -> "decimals" but expected int'
    )


def test_validate_user_interface_spec_nonvalid_decimals_negative(
    parameter_name: str,
) -> None:
    """Tests that passing a negative value for "decimals" raises an exception"""
    # GIVEN
    decimals = -1
    user_interface_spec: dict = {"decimals": decimals}

    # WHEN
    def when() -> None:
        parameters.validate_user_interface_spec(
            user_interface_spec,
            parameter_name=parameter_name,
        )

    # THEN
    with pytest.raises(ValueError) as ctx:
        when()
    assert (
        str(ctx.value)
        == f'Job parameter "{parameter_name}" got {decimals} for "userInterface" -> "decimals" but expected a non-negative int'
    )


@pytest.mark.parametrize(
    argnames="single_step_delta",
    argvalues=(
        pytest.param("a", id="str"),
        pytest.param(True, id="bool"),
        pytest.param(None, id="none"),
        pytest.param([], id="list"),
        pytest.param({}, id="dict"),
    ),
)
def test_validate_user_interface_spec_nonvalid_single_step_delta_type(
    single_step_delta: Any,
    parameter_name: str,
) -> None:
    """Tests that passing a value with an nonvalid "singleStepDelta" value raises an exception"""
    # GIVEN
    user_interface_spec: dict = {"singleStepDelta": single_step_delta}

    # WHEN
    def when() -> None:
        parameters.validate_user_interface_spec(
            user_interface_spec,
            parameter_name=parameter_name,
        )

    # THEN
    with pytest.raises(TypeError) as ctx:
        when()
    assert (
        str(ctx.value)
        == f'Job parameter "{parameter_name}" got but expected float for "userInterface" -> "singleStepDelta", but got {type(single_step_delta).__name__}'
    )


@pytest.mark.parametrize(
    argnames="single_step_delta",
    argvalues=(
        pytest.param(0, id="zero"),
        pytest.param(-1, id="negative"),
    ),
)
def test_validate_user_interface_spec_nonvalid_single_step_delta_non_positive(
    single_step_delta: float,
    parameter_name: str,
) -> None:
    """Tests that passing a non-positive value for "singleStepDelta" value raises an exception"""
    # GIVEN
    user_interface_spec: dict = {"singleStepDelta": single_step_delta}

    # WHEN
    def when() -> None:
        parameters.validate_user_interface_spec(
            user_interface_spec,
            parameter_name=parameter_name,
        )

    # THEN
    with pytest.raises(ValueError) as ctx:
        when()
    assert (
        str(ctx.value)
        == f'Job parameter "{parameter_name}" got {single_step_delta} for "userInterface" -> "singleStepDelta" but expected a positive number'
    )


def test_validate_user_interface_spec_nonvalid_file_filters(
    parameter_name: str,
) -> None:
    """Tests that when the input contains a "fileFilters" field, that its elements are passed
    to validate_user_interface_file_filter and exceptions raised by that call are not caught."""
    # GIVEN
    file_filter = object()
    user_interface_spec = {
        # We need something here in order for validate_user_interface_spec to call
        # validate_user_interface_file_filter
        "fileFilters": [file_filter]
    }
    error = Exception()

    with patch.object(
        parameters, "validate_user_interface_file_filter", side_effect=error
    ) as mock_validate_user_interface_file_filter:
        # WHEN
        def when() -> None:
            parameters.validate_user_interface_spec(
                user_interface_spec,
                parameter_name=parameter_name,
            )

        # THEN
        with pytest.raises(Exception) as ctx:
            when()
        mock_validate_user_interface_file_filter.assert_called_once_with(
            file_filter,
            parameter_name=parameter_name,
            field_path='"userInterface" -> "fileFilters" -> [0]',
        )
        assert ctx.value is error


@pytest.mark.parametrize(
    argnames="file_filters",
    argvalues=(
        pytest.param("a", id="str"),
        pytest.param(1, id="int"),
        pytest.param(1.5, id="float"),
        pytest.param(True, id="bool"),
        pytest.param({}, id="dict"),
        pytest.param(None, id="None"),
    ),
)
def test_validate_user_interface_spec_nonvalid_file_filters_nonlist(
    parameter_name: str,
    file_filters: Any,
) -> None:
    """Tests that when the input contains a "fileFilters" field value that is not a list that an
    exception is raised"""
    # GIVEN
    user_interface_spec = {"fileFilters": file_filters}

    # WHEN
    def when() -> None:
        parameters.validate_user_interface_spec(
            user_interface_spec,
            parameter_name=parameter_name,
        )

    # THEN
    with pytest.raises(TypeError) as ctx:
        when()

    assert (
        str(ctx.value)
        == f'Job parameter "{parameter_name}" got but expected list for "userInterface" -> "fileFilters", but got {type(file_filters).__name__}'
    )


def test_validate_user_interface_spec_nonvalid_file_filter_default(
    parameter_name: str,
) -> None:
    """Tests that when the input contains a "fileFilterDefault" field, that its value is passed
    to validate_user_interface_file_filter and exceptions raised by that call are not caught."""
    # GIVEN
    file_filter = object()
    user_interface_spec = {
        # We need something here in order for validate_user_interface_spec to call
        # validate_user_interface_file_filter
        "fileFilterDefault": file_filter,
    }
    error = Exception()

    with patch.object(
        parameters, "validate_user_interface_file_filter", side_effect=error
    ) as mock_validate_user_interface_file_filter:
        # WHEN
        def when() -> None:
            parameters.validate_user_interface_spec(
                user_interface_spec,
                parameter_name=parameter_name,
            )

        # THEN
        with pytest.raises(Exception) as ctx:
            when()
        mock_validate_user_interface_file_filter.assert_called_once_with(
            file_filter,
            parameter_name=parameter_name,
            field_path='"userInterface" -> "fileFilterDefault"',
        )
        assert ctx.value is error
