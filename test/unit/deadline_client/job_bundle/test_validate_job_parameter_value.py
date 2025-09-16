# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Test cases for deadline.client.job_bundle.parameters.validate_job_parameter_value"""

from __future__ import annotations
from typing import Any, Union

import pytest

from deadline.client.job_bundle import parameters


# Base parameter definitions for different types
BASE_STRING_PARAM: parameters.JobParameter = {"name": "test_string_param", "type": "STRING"}

BASE_PATH_PARAM: parameters.JobParameter = {"name": "test_path_param", "type": "PATH"}

BASE_INT_PARAM: parameters.JobParameter = {"name": "test_int_param", "type": "INT"}

BASE_FLOAT_PARAM: parameters.JobParameter = {"name": "test_float_param", "type": "FLOAT"}

# Constraint parameter definitions
STRING_PARAM_WITH_MIN_LENGTH: parameters.JobParameter = {
    "name": "string_with_min_length",
    "type": "STRING",
    "minLength": 3,
}

STRING_PARAM_WITH_MAX_LENGTH: parameters.JobParameter = {
    "name": "string_with_max_length",
    "type": "STRING",
    "maxLength": 10,
}

INT_PARAM_WITH_MIN_VALUE: parameters.JobParameter = {
    "name": "int_with_min_value",
    "type": "INT",
    "minValue": 5,
}

INT_PARAM_WITH_MAX_VALUE: parameters.JobParameter = {
    "name": "int_with_max_value",
    "type": "INT",
    "maxValue": 100,
}

FLOAT_PARAM_WITH_MIN_VALUE: parameters.JobParameter = {
    "name": "float_with_min_value",
    "type": "FLOAT",
    "minValue": 1.5,
}

FLOAT_PARAM_WITH_MAX_VALUE: parameters.JobParameter = {
    "name": "float_with_max_value",
    "type": "FLOAT",
    "maxValue": 99.9,
}

STRING_PARAM_WITH_ALLOWED_VALUES: parameters.JobParameter = {
    "name": "string_with_allowed_values",
    "type": "STRING",
    "allowedValues": ["option1", "option2", "option3"],
}

INT_PARAM_WITH_ALLOWED_VALUES: parameters.JobParameter = {
    "name": "int_with_allowed_values",
    "type": "INT",
    "allowedValues": [1, 2, 3, 5, 8],
}

PARAM_WITH_MULTIPLE_CONSTRAINTS: parameters.JobParameter = {
    "name": "param_with_multiple_constraints",
    "type": "STRING",
    "minLength": 2,
    "maxLength": 8,
    "allowedValues": ["short", "medium", "longer"],
}


class TestTypeValidationAndConversion:
    @pytest.mark.parametrize(
        "job_parameter,input_value,expected_output",
        [
            pytest.param(
                BASE_STRING_PARAM, "hello world", "hello world", id="valid_string_unchanged"
            ),
            pytest.param(BASE_STRING_PARAM, "", "", id="empty_string_unchanged"),
            pytest.param(
                BASE_STRING_PARAM,
                "test with spaces and symbols!@#",
                "test with spaces and symbols!@#",
                id="string_with_special_chars_unchanged",
            ),
            pytest.param(BASE_STRING_PARAM, "123", "123", id="numeric_string_unchanged"),
        ],
    )
    def test_string_parameter_validation_valid_values(
        self, job_parameter: parameters.JobParameter, input_value: str, expected_output: str
    ) -> None:
        """Test that valid STRING parameter values are returned unchanged."""
        result = parameters.validate_job_parameter_value(job_parameter, input_value)
        assert result == expected_output
        assert isinstance(result, str)

    def test_string_parameter_validation_type_mismatch_errors(self) -> None:
        """Test that STRING parameters raise ValueError for non-string inputs."""
        with pytest.raises(
            TypeError,
            match=r"Job parameter 'test_string_param' has type STRING but got value 123 of type <class 'int'>",
        ):
            parameters.validate_job_parameter_value(BASE_STRING_PARAM, 123)

    @pytest.mark.parametrize(
        "job_parameter,input_value,expected_output",
        [
            pytest.param(
                BASE_PATH_PARAM,
                "/absolute/path/to/file",
                "/absolute/path/to/file",
                id="absolute_path_unchanged",
            ),
            pytest.param(
                BASE_PATH_PARAM,
                "relative/path/to/file",
                "relative/path/to/file",
                id="relative_path_unchanged",
            ),
            pytest.param(BASE_PATH_PARAM, "", "", id="empty_path_unchanged"),
            pytest.param(
                BASE_PATH_PARAM,
                "C:\\Windows\\Path",
                "C:\\Windows\\Path",
                id="windows_path_unchanged",
            ),
        ],
    )
    def test_path_parameter_validation_valid_values(
        self, job_parameter: parameters.JobParameter, input_value: str, expected_output: str
    ) -> None:
        """Test that valid PATH parameter values are returned unchanged."""
        result = parameters.validate_job_parameter_value(job_parameter, input_value)
        assert result == expected_output
        assert isinstance(result, str)

    def test_path_parameter_validation_type_mismatch_errors(self) -> None:
        """Test that PATH parameter raise ValueError for non-string input."""
        with pytest.raises(
            TypeError,
            match=r"Job parameter 'test_path_param' has type PATH but got value 123 of type <class 'int'>",
        ):
            parameters.validate_job_parameter_value(BASE_PATH_PARAM, 123)

    @pytest.mark.parametrize(
        "job_parameter,input_value,expected_output",
        [
            pytest.param(BASE_INT_PARAM, 42, 42, id="int_value_unchanged"),
            pytest.param(BASE_INT_PARAM, 0, 0, id="zero_int_unchanged"),
            pytest.param(BASE_INT_PARAM, -15, -15, id="negative_int_unchanged"),
            pytest.param(BASE_INT_PARAM, "123", 123, id="string_to_int_conversion"),
            pytest.param(BASE_INT_PARAM, "0", 0, id="string_zero_to_int_conversion"),
            pytest.param(BASE_INT_PARAM, "-456", -456, id="negative_string_to_int_conversion"),
        ],
    )
    def test_int_parameter_validation_valid_values(
        self,
        job_parameter: parameters.JobParameter,
        input_value: Union[int, str],
        expected_output: int,
    ) -> None:
        """Test that valid INT parameter values and conversions work correctly."""
        result = parameters.validate_job_parameter_value(job_parameter, input_value)
        assert result == expected_output
        assert isinstance(result, int)

    @pytest.mark.parametrize(
        "job_parameter,input_value,expected_error_pattern",
        [
            pytest.param(
                BASE_INT_PARAM,
                "not_a_number",
                r"Job parameter 'test_int_param' has type INT but got value 'not_a_number' which is not an integer",
                id="non_numeric_string_conversion_error",
            ),
            pytest.param(
                BASE_INT_PARAM,
                "12.5",
                r"Job parameter 'test_int_param' has type INT but got value '12\.5' which is not an integer",
                id="float_string_conversion_error",
            ),
            pytest.param(
                BASE_INT_PARAM,
                "abc123",
                r"Job parameter 'test_int_param' has type INT but got value 'abc123' which is not an integer",
                id="mixed_string_conversion_error",
            ),
            pytest.param(
                BASE_INT_PARAM,
                "",
                r"Job parameter 'test_int_param' has type INT but got value '' which is not an integer",
                id="empty_string_conversion_error",
            ),
            pytest.param(
                BASE_INT_PARAM,
                12.5,
                r"Job parameter 'test_int_param' has type INT but got value 12\.5 which is not an integer",
                id="float_input_conversion_error",
            ),
            pytest.param(
                BASE_INT_PARAM,
                None,
                r"int.. argument must be a string, a bytes-like object or a .*number, not 'NoneType'",
                id="none_input_conversion_error",
            ),
        ],
    )
    def test_int_parameter_validation_conversion_errors(
        self, job_parameter: parameters.JobParameter, input_value: Any, expected_error_pattern: str
    ) -> None:
        """Test that INT parameters raise ValueError for non-convertible values."""
        with pytest.raises((ValueError, TypeError), match=expected_error_pattern):
            parameters.validate_job_parameter_value(job_parameter, input_value)

    @pytest.mark.parametrize(
        "job_parameter,input_value,expected_output",
        [
            pytest.param(BASE_FLOAT_PARAM, 3.14, 3.14, id="float_value_unchanged"),
            pytest.param(BASE_FLOAT_PARAM, 0.0, 0.0, id="zero_float_unchanged"),
            pytest.param(BASE_FLOAT_PARAM, -2.5, -2.5, id="negative_float_unchanged"),
            pytest.param(BASE_FLOAT_PARAM, 42, 42.0, id="int_to_float_conversion"),
            pytest.param(BASE_FLOAT_PARAM, "3.14159", 3.14159, id="string_to_float_conversion"),
            pytest.param(BASE_FLOAT_PARAM, "0.0", 0.0, id="string_zero_to_float_conversion"),
            pytest.param(BASE_FLOAT_PARAM, "-2.7", -2.7, id="negative_string_to_float_conversion"),
            pytest.param(BASE_FLOAT_PARAM, "42", 42.0, id="int_string_to_float_conversion"),
        ],
    )
    def test_float_parameter_validation_valid_values(
        self,
        job_parameter: parameters.JobParameter,
        input_value: Union[int, float, str],
        expected_output: float,
    ) -> None:
        """Test that valid FLOAT parameter values and conversions work correctly."""
        result = parameters.validate_job_parameter_value(job_parameter, input_value)
        assert result == expected_output
        assert isinstance(result, float)

    @pytest.mark.parametrize(
        "job_parameter,input_value,expected_error_pattern",
        [
            pytest.param(
                BASE_FLOAT_PARAM,
                "not_a_number",
                r"Job parameter 'test_float_param' has type FLOAT but got value 'not_a_number' which is not floating point",
                id="non_numeric_string_conversion_error",
            ),
            pytest.param(
                BASE_FLOAT_PARAM,
                "abc123",
                r"Job parameter 'test_float_param' has type FLOAT but got value 'abc123' which is not floating point",
                id="mixed_string_conversion_error",
            ),
            pytest.param(
                BASE_FLOAT_PARAM,
                "",
                r"Job parameter 'test_float_param' has type FLOAT but got value '' which is not floating point",
                id="empty_string_conversion_error",
            ),
            pytest.param(
                BASE_FLOAT_PARAM,
                "12.5.6",
                r"Job parameter 'test_float_param' has type FLOAT but got value '12\.5\.6' which is not floating point",
                id="invalid_float_format_conversion_error",
            ),
            pytest.param(
                BASE_FLOAT_PARAM,
                None,
                r"float.. argument must be a string or a .*number, not 'NoneType'",
                id="none_input_conversion_error",
            ),
        ],
    )
    def test_float_parameter_validation_conversion_errors(
        self, job_parameter: parameters.JobParameter, input_value: Any, expected_error_pattern: str
    ) -> None:
        """Test that FLOAT parameters raise ValueError for non-convertible values."""
        with pytest.raises((ValueError, TypeError), match=expected_error_pattern):
            parameters.validate_job_parameter_value(job_parameter, input_value)

    def test_unsupported_type_error(self) -> None:
        """Test that an incorrect type in the type definition raises a TypeError."""
        with pytest.raises(
            TypeError,
            match="The definition for job parameter 'test_param' has unsupported type 'UNSUPPORTED'",
        ):
            job_parameter: parameters.JobParameter = {
                "name": "test_param",
                "type": "UNSUPPORTED",
            }
            parameters.validate_job_parameter_value(job_parameter, "value")


class TestConstraintValidation:
    @pytest.mark.parametrize(
        "job_parameter,input_value,expected_output",
        [
            # minLength constraint tests - valid cases
            pytest.param(
                STRING_PARAM_WITH_MIN_LENGTH, "abc", "abc", id="string_exactly_at_min_length"
            ),
            pytest.param(
                STRING_PARAM_WITH_MIN_LENGTH, "abcd", "abcd", id="string_above_min_length"
            ),
            pytest.param(
                STRING_PARAM_WITH_MIN_LENGTH,
                "this is a longer string",
                "this is a longer string",
                id="string_well_above_min_length",
            ),
            # maxLength constraint tests - valid cases
            pytest.param(
                STRING_PARAM_WITH_MAX_LENGTH, "short", "short", id="string_below_max_length"
            ),
            pytest.param(
                STRING_PARAM_WITH_MAX_LENGTH,
                "exactly10c",
                "exactly10c",
                id="string_exactly_at_max_length",
            ),
            pytest.param(STRING_PARAM_WITH_MAX_LENGTH, "", "", id="empty_string_below_max_length"),
            # PATH parameter length constraints
            pytest.param(
                {"name": "path_with_min_length", "type": "PATH", "minLength": 5},
                "/path",
                "/path",
                id="path_exactly_at_min_length",
            ),
            pytest.param(
                {"name": "path_with_max_length", "type": "PATH", "maxLength": 15},
                "/short/path",
                "/short/path",
                id="path_below_max_length",
            ),
        ],
    )
    def test_length_constraints_valid_values(
        self, job_parameter: parameters.JobParameter, input_value: str, expected_output: str
    ) -> None:
        """Test that STRING/PATH parameters with length constraints accept valid values."""
        result = parameters.validate_job_parameter_value(job_parameter, input_value)
        assert result == expected_output
        assert isinstance(result, str)

    @pytest.mark.parametrize(
        "job_parameter,input_value,expected_error_pattern",
        [
            # minLength constraint violation tests
            pytest.param(
                STRING_PARAM_WITH_MIN_LENGTH,
                "ab",
                r"Job parameter 'string_with_min_length' value 'ab' is shorter than minLength 3\.",
                id="string_below_min_length",
            ),
            pytest.param(
                STRING_PARAM_WITH_MIN_LENGTH,
                "",
                r"Job parameter 'string_with_min_length' value '' is shorter than minLength 3\.",
                id="empty_string_below_min_length",
            ),
            pytest.param(
                STRING_PARAM_WITH_MIN_LENGTH,
                "x",
                r"Job parameter 'string_with_min_length' value 'x' is shorter than minLength 3\.",
                id="single_char_below_min_length",
            ),
            # maxLength constraint violation tests
            pytest.param(
                STRING_PARAM_WITH_MAX_LENGTH,
                "this string is too long",
                r"Job parameter 'string_with_max_length' value 'this string is too long' is longer than maxLength 10\.",
                id="string_above_max_length",
            ),
            pytest.param(
                STRING_PARAM_WITH_MAX_LENGTH,
                "exactly11ch",
                r"Job parameter 'string_with_max_length' value 'exactly11ch' is longer than maxLength 10\.",
                id="string_one_char_above_max_length",
            ),
            # PATH parameter length constraint violations
            pytest.param(
                {"name": "path_with_min_length", "type": "PATH", "minLength": 10},
                "/short",
                r"Job parameter 'path_with_min_length' value '/short' is shorter than minLength 10\.",
                id="path_below_min_length",
            ),
            pytest.param(
                {"name": "path_with_max_length", "type": "PATH", "maxLength": 5},
                "/very/long/path",
                r"Job parameter 'path_with_max_length' value '/very/long/path' is longer than maxLength 5\.",
                id="path_above_max_length",
            ),
        ],
    )
    def test_length_constraints_violation_errors(
        self, job_parameter: parameters.JobParameter, input_value: str, expected_error_pattern: str
    ) -> None:
        """Test that STRING/PATH parameters raise ValueError for length constraint violations."""
        with pytest.raises(ValueError, match=expected_error_pattern):
            parameters.validate_job_parameter_value(job_parameter, input_value)

    @pytest.mark.parametrize(
        "job_parameter,input_value,expected_output",
        [
            # minValue constraint tests - valid cases
            pytest.param(INT_PARAM_WITH_MIN_VALUE, 5, 5, id="int_exactly_at_min_value"),
            pytest.param(INT_PARAM_WITH_MIN_VALUE, 10, 10, id="int_above_min_value"),
            pytest.param(INT_PARAM_WITH_MIN_VALUE, "5", 5, id="int_string_exactly_at_min_value"),
            pytest.param(INT_PARAM_WITH_MIN_VALUE, "15", 15, id="int_string_above_min_value"),
            pytest.param(FLOAT_PARAM_WITH_MIN_VALUE, 1.5, 1.5, id="float_exactly_at_min_value"),
            pytest.param(FLOAT_PARAM_WITH_MIN_VALUE, 2.0, 2.0, id="float_above_min_value"),
            pytest.param(
                FLOAT_PARAM_WITH_MIN_VALUE, "1.5", 1.5, id="float_string_exactly_at_min_value"
            ),
            # maxValue constraint tests - valid cases
            pytest.param(INT_PARAM_WITH_MAX_VALUE, 100, 100, id="int_exactly_at_max_value"),
            pytest.param(INT_PARAM_WITH_MAX_VALUE, 50, 50, id="int_below_max_value"),
            pytest.param(
                INT_PARAM_WITH_MAX_VALUE, "100", 100, id="int_string_exactly_at_max_value"
            ),
            pytest.param(FLOAT_PARAM_WITH_MAX_VALUE, 99.9, 99.9, id="float_exactly_at_max_value"),
            pytest.param(FLOAT_PARAM_WITH_MAX_VALUE, 50.5, 50.5, id="float_below_max_value"),
            pytest.param(
                FLOAT_PARAM_WITH_MAX_VALUE, "99.9", 99.9, id="float_string_exactly_at_max_value"
            ),
        ],
    )
    def test_value_constraints_valid_values(
        self,
        job_parameter: parameters.JobParameter,
        input_value: Union[str, int, float],
        expected_output: Union[int, float],
    ) -> None:
        """Test that parameters with value constraints accept valid values."""
        result = parameters.validate_job_parameter_value(job_parameter, input_value)
        assert result == expected_output
        assert type(result) is type(expected_output)

    @pytest.mark.parametrize(
        "job_parameter,input_value,expected_error_pattern",
        [
            # minValue constraint violation tests
            pytest.param(
                INT_PARAM_WITH_MIN_VALUE,
                4,
                r"Job parameter 'int_with_min_value' value 4 is less than minValue 5\.",
                id="int_below_min_value",
            ),
            pytest.param(
                INT_PARAM_WITH_MIN_VALUE,
                "0",
                r"Job parameter 'int_with_min_value' value 0 is less than minValue 5\.",
                id="int_string_below_min_value",
            ),
            pytest.param(
                INT_PARAM_WITH_MIN_VALUE,
                -10,
                r"Job parameter 'int_with_min_value' value -10 is less than minValue 5\.",
                id="negative_int_below_min_value",
            ),
            pytest.param(
                FLOAT_PARAM_WITH_MIN_VALUE,
                1.4,
                r"Job parameter 'float_with_min_value' value 1\.4 is less than minValue 1\.5\.",
                id="float_below_min_value",
            ),
            pytest.param(
                FLOAT_PARAM_WITH_MIN_VALUE,
                "0.5",
                r"Job parameter 'float_with_min_value' value 0\.5 is less than minValue 1\.5\.",
                id="float_string_below_min_value",
            ),
            # maxValue constraint violation tests
            pytest.param(
                INT_PARAM_WITH_MAX_VALUE,
                101,
                r"Job parameter 'int_with_max_value' value 101 is greater than maxValue 100\.",
                id="int_above_max_value",
            ),
            pytest.param(
                INT_PARAM_WITH_MAX_VALUE,
                "150",
                r"Job parameter 'int_with_max_value' value 150 is greater than maxValue 100\.",
                id="int_string_above_max_value",
            ),
            pytest.param(
                FLOAT_PARAM_WITH_MAX_VALUE,
                100.0,
                r"Job parameter 'float_with_max_value' value 100\.0 is greater than maxValue 99\.9\.",
                id="float_above_max_value",
            ),
            pytest.param(
                FLOAT_PARAM_WITH_MAX_VALUE,
                "200.5",
                r"Job parameter 'float_with_max_value' value 200\.5 is greater than maxValue 99\.9\.",
                id="float_string_above_max_value",
            ),
        ],
    )
    def test_value_constraints_violation_errors(
        self,
        job_parameter: parameters.JobParameter,
        input_value: Union[str, int, float],
        expected_error_pattern: str,
    ) -> None:
        """Test that parameters raise ValueError for value constraint violations."""
        with pytest.raises(ValueError, match=expected_error_pattern):
            parameters.validate_job_parameter_value(job_parameter, input_value)

    @pytest.mark.parametrize(
        "job_parameter,input_value,expected_output",
        [
            # STRING allowedValues tests - valid cases
            pytest.param(
                STRING_PARAM_WITH_ALLOWED_VALUES,
                "option1",
                "option1",
                id="string_first_allowed_value",
            ),
            pytest.param(
                STRING_PARAM_WITH_ALLOWED_VALUES,
                "option2",
                "option2",
                id="string_middle_allowed_value",
            ),
            pytest.param(
                STRING_PARAM_WITH_ALLOWED_VALUES,
                "option3",
                "option3",
                id="string_last_allowed_value",
            ),
            # INT allowedValues tests - valid cases
            pytest.param(INT_PARAM_WITH_ALLOWED_VALUES, 1, 1, id="int_first_allowed_value"),
            pytest.param(INT_PARAM_WITH_ALLOWED_VALUES, 5, 5, id="int_middle_allowed_value"),
            pytest.param(INT_PARAM_WITH_ALLOWED_VALUES, 8, 8, id="int_last_allowed_value"),
            pytest.param(
                INT_PARAM_WITH_ALLOWED_VALUES, "3", 3, id="int_string_allowed_value_with_conversion"
            ),
            # FLOAT allowedValues tests - valid cases
            pytest.param(
                {
                    "name": "float_with_allowed_values",
                    "type": "FLOAT",
                    "allowedValues": [1.5, 2.7, 3.14],
                },
                1.5,
                1.5,
                id="float_first_allowed_value",
            ),
            pytest.param(
                {
                    "name": "float_with_allowed_values",
                    "type": "FLOAT",
                    "allowedValues": [1.5, 2.7, 3.14],
                },
                "2.7",
                2.7,
                id="float_string_allowed_value_with_conversion",
            ),
            # PATH allowedValues tests - valid cases
            pytest.param(
                {
                    "name": "path_with_allowed_values",
                    "type": "PATH",
                    "allowedValues": ["/path1", "/path2", "/path3"],
                },
                "/path1",
                "/path1",
                id="path_first_allowed_value",
            ),
            pytest.param(
                {
                    "name": "path_with_allowed_values",
                    "type": "PATH",
                    "allowedValues": ["/path1", "/path2", "/path3"],
                },
                "/path3",
                "/path3",
                id="path_last_allowed_value",
            ),
        ],
    )
    def test_allowed_values_constraint_valid_values(
        self,
        job_parameter: parameters.JobParameter,
        input_value: Union[str, int, float],
        expected_output: Union[str, int, float],
    ) -> None:
        """Test that parameters with allowedValues constraints accept valid values."""
        result = parameters.validate_job_parameter_value(job_parameter, input_value)
        assert result == expected_output
        assert type(result) is type(expected_output)

    @pytest.mark.parametrize(
        "job_parameter,input_value,expected_error_pattern",
        [
            # STRING allowedValues violation tests
            pytest.param(
                STRING_PARAM_WITH_ALLOWED_VALUES,
                "invalid_option",
                r"Job parameter 'string_with_allowed_values' value 'invalid_option' is not an allowed value from \('option1', 'option2', 'option3'\)\.",
                id="string_invalid_allowed_value",
            ),
            pytest.param(
                STRING_PARAM_WITH_ALLOWED_VALUES,
                "Option1",
                r"Job parameter 'string_with_allowed_values' value 'Option1' is not an allowed value from \('option1', 'option2', 'option3'\)\.",
                id="string_case_sensitive_invalid_value",
            ),
            pytest.param(
                STRING_PARAM_WITH_ALLOWED_VALUES,
                "",
                r"Job parameter 'string_with_allowed_values' value '' is not an allowed value from \('option1', 'option2', 'option3'\)\.",
                id="empty_string_not_in_allowed_values",
            ),
            # INT allowedValues violation tests
            pytest.param(
                INT_PARAM_WITH_ALLOWED_VALUES,
                4,
                r"Job parameter 'int_with_allowed_values' value 4 is not an allowed value from \(1, 2, 3, 5, 8\)\.",
                id="int_invalid_allowed_value",
            ),
            pytest.param(
                INT_PARAM_WITH_ALLOWED_VALUES,
                "4",
                r"Job parameter 'int_with_allowed_values' value 4 is not an allowed value from \(1, 2, 3, 5, 8\)\.",
                id="int_string_invalid_allowed_value",
            ),
            pytest.param(
                INT_PARAM_WITH_ALLOWED_VALUES,
                0,
                r"Job parameter 'int_with_allowed_values' value 0 is not an allowed value from \(1, 2, 3, 5, 8\)\.",
                id="zero_not_in_allowed_values",
            ),
            # FLOAT allowedValues violation tests
            pytest.param(
                {
                    "name": "float_with_allowed_values",
                    "type": "FLOAT",
                    "allowedValues": [1.5, 2.7, 3.14],
                },
                2.0,
                r"Job parameter 'float_with_allowed_values' value 2\.0 is not an allowed value from \(1\.5, 2\.7, 3\.14\)\.",
                id="float_invalid_allowed_value",
            ),
            pytest.param(
                {
                    "name": "float_with_allowed_values",
                    "type": "FLOAT",
                    "allowedValues": [1.5, 2.7, 3.14],
                },
                "2.0",
                r"Job parameter 'float_with_allowed_values' value 2\.0 is not an allowed value from \(1\.5, 2\.7, 3\.14\)\.",
                id="float_string_invalid_allowed_value",
            ),
            # PATH allowedValues violation tests
            pytest.param(
                {
                    "name": "path_with_allowed_values",
                    "type": "PATH",
                    "allowedValues": ["/path1", "/path2", "/path3"],
                },
                "/invalid/path",
                r"Job parameter 'path_with_allowed_values' value '/invalid/path' is not an allowed value from \('/path1', '/path2', '/path3'\)\.",
                id="path_invalid_allowed_value",
            ),
            pytest.param(
                {
                    "name": "path_with_allowed_values",
                    "type": "PATH",
                    "allowedValues": ["/path1", "/path2", "/path3"],
                },
                "/Path1",
                r"Job parameter 'path_with_allowed_values' value '/Path1' is not an allowed value from \('/path1', '/path2', '/path3'\)\.",
                id="path_case_sensitive_invalid_value",
            ),
        ],
    )
    def test_allowed_values_constraint_violation_errors(
        self,
        job_parameter: parameters.JobParameter,
        input_value: Union[str, int, float],
        expected_error_pattern: str,
    ) -> None:
        """Test that parameters raise ValueError for allowedValues constraint violations."""
        with pytest.raises(ValueError, match=expected_error_pattern):
            parameters.validate_job_parameter_value(job_parameter, input_value)

    @pytest.mark.parametrize(
        "job_parameter,input_value,expected_output",
        [
            # Multiple constraints - valid cases
            pytest.param(
                PARAM_WITH_MULTIPLE_CONSTRAINTS,
                "short",
                "short",
                id="multiple_constraints_first_allowed_value",
            ),
            pytest.param(
                PARAM_WITH_MULTIPLE_CONSTRAINTS,
                "medium",
                "medium",
                id="multiple_constraints_middle_allowed_value",
            ),
            pytest.param(
                PARAM_WITH_MULTIPLE_CONSTRAINTS,
                "longer",
                "longer",
                id="multiple_constraints_last_allowed_value",
            ),
            # INT with multiple constraints
            pytest.param(
                {
                    "name": "int_with_multiple_constraints",
                    "type": "INT",
                    "minValue": 10,
                    "maxValue": 50,
                    "allowedValues": [10, 20, 30, 40, 50],
                },
                10,
                10,
                id="int_multiple_constraints_min_boundary",
            ),
            pytest.param(
                {
                    "name": "int_with_multiple_constraints",
                    "type": "INT",
                    "minValue": 10,
                    "maxValue": 50,
                    "allowedValues": [10, 20, 30, 40, 50],
                },
                "30",
                30,
                id="int_multiple_constraints_middle_value_with_conversion",
            ),
            pytest.param(
                {
                    "name": "int_with_multiple_constraints",
                    "type": "INT",
                    "minValue": 10,
                    "maxValue": 50,
                    "allowedValues": [10, 20, 30, 40, 50],
                },
                50,
                50,
                id="int_multiple_constraints_max_boundary",
            ),
            # FLOAT with multiple constraints
            pytest.param(
                {
                    "name": "float_with_multiple_constraints",
                    "type": "FLOAT",
                    "minValue": 1.0,
                    "maxValue": 10.0,
                    "allowedValues": [1.5, 5.0, 9.5],
                },
                1.5,
                1.5,
                id="float_multiple_constraints_valid_value",
            ),
            pytest.param(
                {
                    "name": "float_with_multiple_constraints",
                    "type": "FLOAT",
                    "minValue": 1.0,
                    "maxValue": 10.0,
                    "allowedValues": [1.5, 5.0, 9.5],
                },
                "9.5",
                9.5,
                id="float_multiple_constraints_with_conversion",
            ),
            # PATH with multiple constraints
            pytest.param(
                {
                    "name": "path_with_multiple_constraints",
                    "type": "PATH",
                    "minLength": 5,
                    "maxLength": 20,
                    "allowedValues": ["/path1", "/longer/path", "/very/long/path"],
                },
                "/path1",
                "/path1",
                id="path_multiple_constraints_short_valid",
            ),
            pytest.param(
                {
                    "name": "path_with_multiple_constraints",
                    "type": "PATH",
                    "minLength": 5,
                    "maxLength": 20,
                    "allowedValues": ["/path1", "/longer/path", "/very/long/path"],
                },
                "/very/long/path",
                "/very/long/path",
                id="path_multiple_constraints_long_valid",
            ),
        ],
    )
    def test_multiple_constraints_valid_values(
        self,
        job_parameter: parameters.JobParameter,
        input_value: Union[str, int, float],
        expected_output: Union[str, int, float],
    ) -> None:
        """Test that parameters with multiple constraints accept values that satisfy all constraints."""
        result = parameters.validate_job_parameter_value(job_parameter, input_value)
        assert result == expected_output
        assert type(result) is type(expected_output)

    @pytest.mark.parametrize(
        "job_parameter,input_value,expected_error_pattern",
        [
            # Multiple constraints - length violation
            pytest.param(
                PARAM_WITH_MULTIPLE_CONSTRAINTS,
                "x",
                r"Job parameter 'param_with_multiple_constraints' value 'x' is shorter than minLength 2\.",
                id="multiple_constraints_min_length_violation",
            ),
            pytest.param(
                PARAM_WITH_MULTIPLE_CONSTRAINTS,
                "toolongstring",
                r"Job parameter 'param_with_multiple_constraints' value 'toolongstring' is longer than maxLength 8\.",
                id="multiple_constraints_max_length_violation",
            ),
            # Multiple constraints - allowedValues violation (but satisfies length)
            pytest.param(
                PARAM_WITH_MULTIPLE_CONSTRAINTS,
                "valid",
                r"Job parameter 'param_with_multiple_constraints' value 'valid' is not an allowed value from \('short', 'medium', 'longer'\)\.",
                id="multiple_constraints_allowed_values_violation",
            ),
            # INT with multiple constraints - minValue violation
            pytest.param(
                {
                    "name": "int_with_multiple_constraints",
                    "type": "INT",
                    "minValue": 10,
                    "maxValue": 50,
                    "allowedValues": [10, 20, 30, 40, 50],
                },
                5,
                r"Job parameter 'int_with_multiple_constraints' value 5 is less than minValue 10\.",
                id="int_multiple_constraints_min_value_violation",
            ),
            # INT with multiple constraints - maxValue violation
            pytest.param(
                {
                    "name": "int_with_multiple_constraints",
                    "type": "INT",
                    "minValue": 10,
                    "maxValue": 50,
                    "allowedValues": [10, 20, 30, 40, 50],
                },
                60,
                r"Job parameter 'int_with_multiple_constraints' value 60 is greater than maxValue 50\.",
                id="int_multiple_constraints_max_value_violation",
            ),
            # INT with multiple constraints - allowedValues violation (but satisfies min/max)
            pytest.param(
                {
                    "name": "int_with_multiple_constraints",
                    "type": "INT",
                    "minValue": 10,
                    "maxValue": 50,
                    "allowedValues": [10, 20, 30, 40, 50],
                },
                25,
                r"Job parameter 'int_with_multiple_constraints' value 25 is not an allowed value from \(10, 20, 30, 40, 50\)\.",
                id="int_multiple_constraints_allowed_values_violation",
            ),
            # FLOAT with multiple constraints - minValue violation
            pytest.param(
                {
                    "name": "float_with_multiple_constraints",
                    "type": "FLOAT",
                    "minValue": 1.0,
                    "maxValue": 10.0,
                    "allowedValues": [1.5, 5.0, 9.5],
                },
                0.5,
                r"Job parameter 'float_with_multiple_constraints' value 0\.5 is less than minValue 1\.0\.",
                id="float_multiple_constraints_min_value_violation",
            ),
            # FLOAT with multiple constraints - allowedValues violation (but satisfies min/max)
            pytest.param(
                {
                    "name": "float_with_multiple_constraints",
                    "type": "FLOAT",
                    "minValue": 1.0,
                    "maxValue": 10.0,
                    "allowedValues": [1.5, 5.0, 9.5],
                },
                7.5,
                r"Job parameter 'float_with_multiple_constraints' value 7\.5 is not an allowed value from \(1\.5, 5\.0, 9\.5\)\.",
                id="float_multiple_constraints_allowed_values_violation",
            ),
            # PATH with multiple constraints - length violation
            pytest.param(
                {
                    "name": "path_with_multiple_constraints",
                    "type": "PATH",
                    "minLength": 5,
                    "maxLength": 20,
                    "allowedValues": ["/path1", "/longer/path", "/very/long/path"],
                },
                "/x",
                r"Job parameter 'path_with_multiple_constraints' value '/x' is shorter than minLength 5\.",
                id="path_multiple_constraints_min_length_violation",
            ),
            # PATH with multiple constraints - allowedValues violation (but satisfies length)
            pytest.param(
                {
                    "name": "path_with_multiple_constraints",
                    "type": "PATH",
                    "minLength": 5,
                    "maxLength": 20,
                    "allowedValues": ["/path1", "/longer/path", "/very/long/path"],
                },
                "/other/path",
                r"Job parameter 'path_with_multiple_constraints' value '/other/path' is not an allowed value from \('/path1', '/longer/path', '/very/long/path'\)\.",
                id="path_multiple_constraints_allowed_values_violation",
            ),
        ],
    )
    def test_multiple_constraints_violation_errors(
        self,
        job_parameter: parameters.JobParameter,
        input_value: Union[str, int, float],
        expected_error_pattern: str,
    ) -> None:
        """Test that parameters with multiple constraints raise ValueError when any constraint is violated."""
        with pytest.raises(ValueError, match=expected_error_pattern):
            parameters.validate_job_parameter_value(job_parameter, input_value)
