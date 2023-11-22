# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Test cases for deadline.client.job_bundle.parameters.validate_job_parameter"""

from __future__ import annotations
from typing import Any
from unittest.mock import patch

import pytest

from deadline.client.job_bundle import parameters


VALID_DEFAULTS_BY_TYPE = {
    "STRING": "valid",
    "INT": 1,
    "FLOAT": 2.5,
    "PATH": "/a/path",
}


@pytest.fixture
def valid_name() -> str:
    return "paramname"


@pytest.fixture
def valid_description() -> str:
    return "description"


@pytest.fixture(
    params=("STRING", "INT", "FLOAT", "PATH"),
    ids=("type-STRING", "type-INT", "type-FLOAT", "t"),
)
def valid_type(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture
def valid_default(valid_type: str) -> Any:
    return VALID_DEFAULTS_BY_TYPE[valid_type]


@pytest.fixture
def valid_allowed_values(valid_default: Any) -> list[Any]:
    return [valid_default]


@pytest.fixture(
    params=("NONE", "IN", "OUT", "INOUT"),
    ids=(("dataFlow-NONE", "dataFlow-IN", "dataFlow-OUT", "dataFlow-INOUT")),
)
def valid_data_flow(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture
def valid_min_length() -> int:
    return 1


@pytest.fixture
def valid_max_length() -> int:
    return 2


@pytest.fixture(
    params=(1, 1.5, "2.7", "3"),
    ids=(("minValue-int", "minValue-float", "minValue-floatstring", "minValue-intstring")),
)
def valid_min_value(request: pytest.FixtureRequest) -> str | int | float:
    return request.param


@pytest.fixture(
    params=(4, 4.5, "5.7", "6"),
    ids=(("maxValue-int", "maxValue-float", "maxValue-floatstring", "maxValue-intstring")),
)
def valid_max_value(request: pytest.FixtureRequest) -> str | int | float:
    return request.param


@pytest.fixture(
    params=("FILE", "DIRECTORY"),
    ids=("objectType-FILE", "objectType-DIRECTORY"),
)
def valid_object_type(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture
def valid_user_interface() -> parameters.UserInterfaceSpec:
    return {
        "control": "CHECK_BOX",
        "label": "labela",
        "groupLabel": "groupa",
        "decimal": 1,
        "singleStepDelta": 1.5,
    }


@pytest.fixture
def valid_job_parameter(
    valid_name: str,
    valid_description: str,
    valid_type: str,
    valid_default: Any,
    valid_allowed_values: list[Any],
    valid_data_flow: str,
    valid_object_type: str,
    valid_min_length: int,
    valid_max_length: int,
    valid_min_value: str | int | float,
    valid_max_value: str | int | float,
    valid_user_interface: parameters.UserInterfaceSpec,
) -> parameters.JobParameter:
    return {
        "name": valid_name,
        "description": valid_description,
        "type": valid_type,
        "default": valid_default,
        "allowedValues": valid_allowed_values,
        "dataFlow": valid_data_flow,
        "minLength": valid_min_length,
        "maxLength": valid_max_length,
        "minValue": valid_min_value,
        "maxValue": valid_max_value,
        "objectType": valid_object_type,
        "userInterface": valid_user_interface,
    }


def test_validate_job_parameter_valid(
    valid_job_parameter: parameters.JobParameter,
) -> None:
    """Tests that passing a valid job parameter to validate_job_parameter does not raise an
    exception"""
    # WHEN
    result = parameters.validate_job_parameter(valid_job_parameter)

    # THEN
    assert result is valid_job_parameter


@pytest.mark.parametrize(
    argnames="input",
    argvalues=(
        pytest.param(1, id="int"),
        pytest.param(1.2, id="float"),
        pytest.param(True, id="bool"),
        pytest.param("not-valid", id="non-valid-str"),
        pytest.param(None, id="none"),
        pytest.param([], id="list"),
    ),
)
def test_validate_job_parameter_nonvalid_root_type(
    input: Any,
) -> None:
    """Tests that a non-dict input passed to validate_job_parameter raises an exception"""

    # WHEN
    def when() -> None:
        parameters.validate_job_parameter(input)

    # THEN
    with pytest.raises(TypeError) as ctx:
        when()
    assert str(ctx.value) == f"Expected a dict for job parameter, but got {type(input).__name__}"


@pytest.mark.parametrize(
    argnames="name",
    argvalues=(
        pytest.param(1, id="int"),
        pytest.param(1.2, id="float"),
        pytest.param(True, id="bool"),
        pytest.param(None, id="none"),
        pytest.param([], id="list"),
        pytest.param({}, id="dict"),
    ),
)
def test_validate_job_parameter_nonvalid_name(
    name: Any,
) -> None:
    """Tests that when calling validate_job_parameter with an nonvalid values for "name"
    that an exception is raised."""
    # GIVEN
    job_parameter = {"name": name}

    # WHEN
    def when() -> None:
        parameters.validate_job_parameter(job_parameter)

    # THEN
    with pytest.raises(TypeError) as ctx:
        when()
    assert str(ctx.value) == f'Job parameter had {type(name).__name__} for "name" but expected str'


def test_validate_job_parameter_missing_name() -> None:
    """Tests that when calling validate_job_parameter without a "name" raises an exception."""
    # GIVEN
    job_parameter: dict = {}

    # WHEN
    def when() -> None:
        parameters.validate_job_parameter(job_parameter)

    # THEN
    with pytest.raises(ValueError) as ctx:
        when()
    assert str(ctx.value) == f'No "name" field in job parameter. Got {job_parameter}'


def test_validate_job_parameter_empty_name() -> None:
    """Tests that when calling validate_job_parameter without a "name" raises an exception."""
    # GIVEN
    job_parameter: dict = {"name": ""}

    # WHEN
    def when() -> None:
        parameters.validate_job_parameter(job_parameter)

    # THEN
    with pytest.raises(ValueError) as ctx:
        when()
    assert str(ctx.value) == "Job parameter has an empty name"


def test_validate_job_parameter_valid_no_type() -> None:
    """Tests that passing a job parameter without a "type" does not raise an exception by
    default or if type_required is False"""
    # GIVEN
    job_parameter: parameters.JobParameter = {"name": "foo"}

    # WHEN
    result = parameters.validate_job_parameter(job_parameter)

    # THEN
    assert result is job_parameter

    # WHEN
    result = parameters.validate_job_parameter(job_parameter, type_required=False)

    # THEN
    assert result is job_parameter


@pytest.mark.parametrize(
    argnames="typ",
    argvalues=(
        pytest.param(1, id="int"),
        pytest.param(1.2, id="float"),
        pytest.param(True, id="bool"),
        pytest.param("not-valid", id="non-valid-str"),
        pytest.param(None, id="none"),
        pytest.param([], id="list"),
        pytest.param({}, id="dict"),
    ),
)
def test_validate_job_parameter_nonvalid_type(
    typ: Any,
) -> None:
    """Tests that when calling validate_job_parameter with an nonvalid values for "type"
    that an exception is raised."""
    # GIVEN
    job_parameter: parameters.JobParameter = {
        "name": "foo",
        "type": typ,
    }

    # WHEN
    def when() -> None:
        parameters.validate_job_parameter(job_parameter)

    # THEN
    with pytest.raises(ValueError) as ctx:
        when()
    assert (
        str(ctx.value)
        == f'Job parameter "foo" had "type" {typ} but expected one of ("STRING", "PATH", "INT", "FLOAT")'
    )


@pytest.mark.parametrize(
    argnames="description",
    argvalues=(
        pytest.param(1, id="int"),
        pytest.param(1.2, id="float"),
        pytest.param(True, id="bool"),
        pytest.param(None, id="none"),
        pytest.param([], id="list"),
        pytest.param({}, id="dict"),
    ),
)
def test_validate_job_parameter_nonvalid_description(
    description: Any,
) -> None:
    """Tests that when calling validate_job_parameter with an nonvalid values for "description"
    that an exception is raised."""
    # GIVEN
    job_parameter = {
        "name": "a",
        "description": description,
    }

    # WHEN
    def when() -> None:
        parameters.validate_job_parameter(job_parameter)

    # THEN
    with pytest.raises(TypeError) as ctx:
        when()
    assert (
        str(ctx.value)
        == f'Job parameter "a" had {type(description).__name__} for "description" but expected str'
    )


def test_validate_job_parameter_valid_no_default() -> None:
    """Tests that passing a job parameter to validate_job_parameter without a "default" key does
    not raise an exception by default or if default_required is False"""
    # GIVEN
    job_parameter: parameters.JobParameter = {
        "name": "foo",
        "type": "INT",
    }

    # WHEN
    # Default is to not require a default
    result = parameters.validate_job_parameter(job_parameter)

    # THEN
    assert result is job_parameter

    # WHEN
    # Explicitly not require a default
    result = parameters.validate_job_parameter(job_parameter, default_required=False)

    # THEN
    assert result is job_parameter


def test_validate_job_parameter_valid_no_allowed_values() -> None:
    """Tests that passing a job parameter to validate_job_parameter without a "allowedValues"
    key does not raise an exception by default or if default_required is False"""
    # GIVEN
    job_parameter: parameters.JobParameter = {"name": "foo"}

    # WHEN
    result = parameters.validate_job_parameter(job_parameter)

    # THEN
    assert result is job_parameter


@pytest.mark.parametrize(
    argnames="allowed_values",
    argvalues=(
        pytest.param("a", id="str"),
        pytest.param(1, id="int"),
        pytest.param(1.2, id="float"),
        pytest.param(True, id="bool"),
        pytest.param(None, id="none"),
        pytest.param({}, id="dict"),
    ),
)
def test_validate_job_parameter_nonvalid_allowed_values(
    allowed_values: Any,
) -> None:
    """Tests that passing a job parameter to validate_job_parameter without a "allowedValues"
    key does not raise an exception by default or if default_required is False"""
    # GIVEN
    job_parameter: parameters.JobParameter = {
        "name": "foo",
        "allowedValues": allowed_values,
    }

    # WHEN
    def when() -> None:
        parameters.validate_job_parameter(job_parameter)

    # THEN
    with pytest.raises(TypeError) as ctx:
        when()
    assert (
        str(ctx.value)
        == f'Job parameter "foo" got {type(allowed_values).__name__} for "allowedValues" but expected list'
    )


def test_validate_job_parameter_valid_no_data_flow(
    valid_type: str,
) -> None:
    """Tests that passing a job parameter to validate_job_parameter without a "dataFlow" key
    no exception is raised"""
    # GIVEN

    job_parameter: parameters.JobParameter = {
        "name": "foo",
        "type": valid_type,
    }

    # WHEN
    # Default is to not require a default
    result = parameters.validate_job_parameter(job_parameter)

    # THEN
    assert result is job_parameter


@pytest.mark.parametrize(
    argnames="data_flow",
    argvalues=(
        pytest.param(1, id="int"),
        pytest.param(1.2, id="float"),
        pytest.param(True, id="bool"),
        pytest.param("not-valid", id="non-valid-str"),
        pytest.param(None, id="none"),
        pytest.param([], id="list"),
        pytest.param({}, id="dict"),
    ),
)
def test_validate_job_parameter_nonvalid_data_flow(
    data_flow: Any,
) -> None:
    """Tests that when calling validate_job_parameter with an nonvalid values for "dataFlow"
    that an exception is raised."""
    # GIVEN
    job_parameter: parameters.JobParameter = {
        "name": "foo",
        "type": "PATH",
        "dataFlow": data_flow,
    }

    # WHEN
    def when() -> None:
        parameters.validate_job_parameter(job_parameter)

    # THEN
    with pytest.raises(ValueError) as ctx:
        when()
    assert (
        str(ctx.value)
        == f'Job parameter "foo" got "{data_flow}" for "dataFlow" but expected one of ("NONE", "IN", "OUT", "INOUT")'
    )


@pytest.mark.parametrize(
    argnames="min_length",
    argvalues=(
        pytest.param(1.2, id="float"),
        pytest.param("a", id="str"),
        pytest.param(True, id="bool"),
        pytest.param(None, id="none"),
        pytest.param([], id="list"),
        pytest.param({}, id="dict"),
    ),
)
def test_validate_job_parameter_nonvalid_min_length_type(
    min_length: Any,
) -> None:
    """Tests that passing a value with an nonvalid "minLength" value raises an exception"""
    # GIVEN
    job_parameter: dict = {
        "name": "a",
        "minLength": min_length,
    }

    # WHEN
    def when() -> None:
        parameters.validate_job_parameter(job_parameter)

    # THEN
    with pytest.raises(TypeError) as ctx:
        when()
    assert (
        str(ctx.value)
        == f'Job parameter "a" got {type(min_length).__name__} for "minLength" but expected int'
    )


def test_validate_job_parameter_nonvalid_min_length_negative() -> None:
    """Tests that passing a value with an nonvalid "minLength" value raises an exception"""
    # GIVEN
    min_length = -1
    job_parameter: dict = {
        "name": "a",
        "minLength": min_length,
    }

    # WHEN
    def when() -> None:
        parameters.validate_job_parameter(job_parameter)

    # THEN
    with pytest.raises(ValueError) as ctx:
        when()
    assert (
        str(ctx.value)
        == f'Job parameter "a" got {min_length} for "minLength" but the value must be non-negative'
    )


@pytest.mark.parametrize(
    argnames="max_length",
    argvalues=(
        pytest.param(1.2, id="float"),
        pytest.param("a", id="str"),
        pytest.param(True, id="bool"),
        pytest.param(None, id="none"),
        pytest.param([], id="list"),
        pytest.param({}, id="dict"),
    ),
)
def test_validate_job_parameter_nonvalid_max_length_type(
    max_length: Any,
) -> None:
    """Tests that passing a value with an nonvalid "maxLength" value raises an exception"""
    # GIVEN
    job_parameter: dict = {
        "name": "a",
        "maxLength": max_length,
    }

    # WHEN
    def when() -> None:
        parameters.validate_job_parameter(job_parameter)

    # THEN
    with pytest.raises(TypeError) as ctx:
        when()
    assert (
        str(ctx.value)
        == f'Job parameter "a" got "{type(max_length).__name__}" for "maxLength" but expected int'
    )


def test_validate_job_parameter_nonvalid_max_length_negative() -> None:
    """Tests that passing a value with an nonvalid "maxLength" value raises an exception"""
    # GIVEN
    max_length = -1
    job_parameter: dict = {
        "name": "a",
        "maxLength": max_length,
    }

    # WHEN
    def when() -> None:
        parameters.validate_job_parameter(job_parameter)

    # THEN
    with pytest.raises(ValueError) as ctx:
        when()
    assert (
        str(ctx.value)
        == f'Job parameter "a" got {max_length} for "maxLength" but the value must be non-negative'
    )


@pytest.mark.parametrize(
    argnames="min_value",
    argvalues=(
        pytest.param(True, id="bool"),
        pytest.param(None, id="none"),
        pytest.param([], id="list"),
        pytest.param({}, id="dict"),
    ),
)
def test_validate_job_parameter_nonvalid_min_value_type(
    min_value: Any,
) -> None:
    """Tests that passing a value with an nonvalid "minValue" value raises an exception"""
    # GIVEN
    job_parameter: dict = {
        "name": "a",
        "minValue": min_value,
    }

    # WHEN
    def when() -> None:
        parameters.validate_job_parameter(job_parameter)

    # THEN
    with pytest.raises(TypeError) as ctx:
        when()
    assert (
        str(ctx.value)
        == f'Job parameter "a" got {type(min_value).__name__} for "minValue" but expected int'
    )


@pytest.mark.parametrize(
    argnames="min_value",
    argvalues=("one", "two"),
)
def test_validate_job_parameter_nonvalid_min_value_str(min_value: str) -> None:
    """Tests that passing a non-numeric string value to "minValue" raises an exception"""
    # GIVEN
    job_parameter: dict = {
        "name": "a",
        "minValue": min_value,
    }

    # WHEN
    def when() -> None:
        parameters.validate_job_parameter(job_parameter)

    # THEN
    with pytest.raises(ValueError) as ctx:
        when()
    assert (
        str(ctx.value)
        == f'Job parameter "a" has a non-numeric string value for "minValue": {min_value}'
    )


@pytest.mark.parametrize(
    argnames="max_value",
    argvalues=(
        pytest.param(True, id="bool"),
        pytest.param(None, id="none"),
        pytest.param([], id="list"),
        pytest.param({}, id="dict"),
    ),
)
def test_validate_job_parameter_nonvalid_max_value_type(
    max_value: Any,
) -> None:
    """Tests that passing a value with an nonvalid "maxValue" value raises an exception"""
    # GIVEN
    job_parameter: dict = {
        "name": "a",
        "maxValue": max_value,
    }

    # WHEN
    def when() -> None:
        parameters.validate_job_parameter(job_parameter)

    # THEN
    with pytest.raises(TypeError) as ctx:
        when()
    assert (
        str(ctx.value)
        == f'Job parameter "a" got {type(max_value).__name__} for "maxValue" but expected int'
    )


@pytest.mark.parametrize(
    argnames="max_value",
    argvalues=("one", "two"),
)
def test_validate_job_parameter_nonvalid_max_value_str(max_value: str) -> None:
    """Tests that passing a non-numeric string value to "maxValue" raises an exception"""
    # GIVEN
    job_parameter: dict = {
        "name": "a",
        "maxValue": max_value,
    }

    # WHEN
    def when() -> None:
        parameters.validate_job_parameter(job_parameter)

    # THEN
    with pytest.raises(ValueError) as ctx:
        when()
    assert (
        str(ctx.value)
        == f'Job parameter "a" has a non-numeric string value for "maxValue": {max_value}'
    )


@pytest.mark.parametrize(
    argnames="object_type",
    argvalues=(
        pytest.param(1, id="int"),
        pytest.param(1.2, id="float"),
        pytest.param(True, id="bool"),
        pytest.param("not-valid", id="non-valid-str"),
        pytest.param(None, id="none"),
        pytest.param([], id="list"),
        pytest.param({}, id="dict"),
    ),
)
def test_validate_job_parameter_nonvalid_object_type(
    object_type: Any,
) -> None:
    """Tests that when calling validate_job_parameter with an nonvalid values for "objectType"
    that an exception is raised."""
    # GIVEN
    job_parameter: parameters.JobParameter = {
        "name": "foo",
        "type": "PATH",
        "objectType": object_type,
    }

    # WHEN
    def when() -> None:
        parameters.validate_job_parameter(job_parameter)

    # THEN
    with pytest.raises(ValueError) as ctx:
        when()
    assert (
        str(ctx.value)
        == f'Job parameter "foo" got {object_type} for "objectType" but expected one of ("FILE", "DIRECTORY")'
    )


def test_validate_job_parameter_valid_no_user_interface(
    valid_type: str,
) -> None:
    """Tests that passing a job parameter to validate_job_parameter without a "userInterface"
    key does not raise an exception"""
    # GIVEN

    job_parameter: parameters.JobParameter = {
        "name": "foo",
        "type": valid_type,
    }

    # WHEN
    # Default is to not require a default
    result = parameters.validate_job_parameter(job_parameter)

    # THEN
    assert result is job_parameter


def test_validate_job_parameter_nonvalid_userinterface(
    valid_type: str,
) -> None:
    """Tests that an nonvalid user interface raises an exception. This is simply testing that
    an exception raised by validate_user_interface_spec is not caught and ignored"""
    # GIVEN
    job_parameter: parameters.JobParameter = {
        "name": "foo",
        "type": valid_type,
        "userInterface": {},
    }
    error_msg = "error message"
    user_interface_validation_error = Exception(error_msg)
    with patch.object(
        parameters, "validate_user_interface_spec", side_effect=user_interface_validation_error
    ):
        # WHEN
        def when():
            parameters.validate_job_parameter(job_parameter)

        # THEN
        with pytest.raises(Exception) as ctx:
            when()

        assert ctx.value is user_interface_validation_error
