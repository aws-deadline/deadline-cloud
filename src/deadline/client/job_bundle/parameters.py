# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

__all__ = [
    "apply_job_parameters",
    "read_job_bundle_parameters",
    "get_ui_control_for_parameter_definition",
    "parameter_definition_difference",
]

import os
from collections import namedtuple
from typing import Any, TYPE_CHECKING, cast

# typing_extensions is only needed for type-checking. It fails to import at run-time in Python 3.7
# so provide stubs at run-time.
if TYPE_CHECKING:
    from typing_extensions import NotRequired, TypedDict
    from ..job_bundle.submission import AssetReferences
else:
    NotRequired = object
    TypedDict = object

from ..exceptions import DeadlineOperationError
from .loader import read_yaml_or_json_object

_VALID_PARAMETER_TYPES = (
    "STRING",
    "PATH",
    "INT",
    "FLOAT",
)
_VALID_UI_CONTROLS = (
    "CHECK_BOX",
    "CHOOSE_DIRECTORY",
    "CHOOSE_INPUT_FILE",
    "CHOOSE_OUTPUT_FILE",
    "DROPDOWN_LIST",
    "LINE_EDIT",
    "MULTILINE_EDIT",
    "SPIN_BOX",
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
    maxValue: NotRequired[int | float | str]
    minValue: NotRequired[int | float | str]
    userInterface: NotRequired[UserInterfaceSpec]


def validate_job_parameter(
    input: Any,
    *,
    type_required: bool = False,
    default_required: bool = False,
) -> JobParameter:
    """Validates a job parameter as defined by Open Job Description. The validation allows for the
    union of all possible fields but does not do per-type validation (e.g. minValue only allowed
    on parameters of type "INT" / "FLOAT")

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

    return cast(JobParameter, input)


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
                    f'Job parameter "{parameter_name}" got "{repr(pattern)}" for {field_path} -> "patterns" [{i}] but expected str'
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

            # Job parameters will only provide a value and will not have a definition fields if the
            # parameter is defined on the queue
            if {"name", "value"} == job_parameter.keys():
                collected_parameters[job_parameter_name]["value"] = job_parameter["value"]
                continue

            queue_parameter = collected_parameters[job_parameter_name]
            differences = parameter_definition_difference(queue_parameter, job_parameter)

            # Ignore differing defaults
            try:
                differences.remove("default")
            except ValueError:
                # Job bundle's default value for a parameter takes priority over queue's parameter
                # default
                if "default" in job_parameter:
                    collected_parameters[job_parameter_name]["default"] = job_parameter["default"]

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


def apply_job_parameters(
    job_parameters: list[dict[str, Any]],
    job_bundle_dir: str,
    parameters: list[JobParameter],
    asset_references: AssetReferences,
) -> None:
    """
    Modifies the provided parameters and asset_references to incorporate
    the job_parameters and to resolve any relative paths in PATH parameters.

    The following actions are taken:
    - Any job_parameters provided set or replace the "value" key in the corresponding
      job_bundle_parameters entry.
    - Any job_parameters for a PATH, that is a relative path, is made absolute by joining
      with the current working directory.
    - Any job_bundle_parameters for a PATH, not set by job_parameters, that is a
      relative path, is made absolute by joining with the job bundle directory.
    - Any PATH parameters that have IN, OUT, or INOUT assetReferences metadata are
      added to the appropriate asset_references entries.
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
            # Make PATH parameter values that are not constrained by allowedValues
            # absolute by joining with the current working directory
            if parameter_type == "PATH" and "allowedValues" not in parameter:
                if parameter_value == "":
                    continue
                parameter_value = os.path.abspath(parameter_value)
            parameter["value"] = parameter_value
        else:
            parameter_value = parameter.get("value", parameter.get("default"))
            if parameter_value is None:
                raise DeadlineOperationError(
                    f"Job Template for job bundle {job_bundle_dir}:\nNo parameter value provided for Job Template parameter {parameter_name}, and it has no default value."
                )

        # If it's a PATH parameter with dataFlow, add it to asset_references
        if parameter_type == "PATH":
            data_flow = parameter.get("dataFlow", "NONE")
            if data_flow not in ("NONE", "IN", "OUT", "INOUT"):
                raise DeadlineOperationError(
                    f"Job Template for job bundle {job_bundle_dir}:\nJob Template parameter {parameter_name} had an incorrect "
                    + f"value {data_flow} for 'dataFlow'. Valid values are "
                    + "['NONE', 'IN', 'OUT', 'INOUT']"
                )
            if data_flow == "NONE":
                # This path is referenced, but its contents are not necessarily
                # input or output.
                asset_references.referenced_paths.add(parameter_value)
            elif parameter_value != "":
                # While empty parameters are allowed, we don't want to add them to asset references
                object_type = parameter.get("objectType")

                if "IN" in data_flow:
                    if object_type == "FILE":
                        asset_references.input_filenames.add(parameter_value)
                    else:
                        asset_references.input_directories.add(parameter_value)
                if "OUT" in data_flow:
                    if object_type == "FILE":
                        # TODO: When job attachments supports output files in addition to directories, change this to
                        #       add the filename instead.
                        asset_references.output_directories.add(os.path.dirname(parameter_value))
                    else:
                        asset_references.output_directories.add(parameter_value)


def read_job_bundle_parameters(bundle_dir: str) -> list[JobParameter]:
    """
    Reads the parameter definitions and parameter values from the job bundle. For
    any relative PATH parameters with data flow where no parameter value is supplied,
    it sets the value to that path relative to the job bundle directory.

    Return format:
    [
        {
            "name": <parameter name>,
            <all fields from the the "parameters" value in template.json/yaml>
            "value": <if provided from parameter_values.json/yaml>
        },
        ...
    ]
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

    # Make valueless PATH parameters with 'default' (but not constrained
    # by allowedValues) absolute by joining with the job bundle directory
    for name, parameter in template_parameters.items():
        if (
            "value" not in parameter
            and parameter["type"] == "PATH"
            and "allowedValues" not in parameter
        ):
            default = parameter.get("default")
            if default:
                if os.path.isabs(default):
                    raise DeadlineOperationError(
                        f"Job Template for job bundle {bundle_dir}:\nDefault PATH '{default}' for parameter '{name}' is absolute.\nPATH values must be relative, and must resolve within the Job Bundle directory."
                    )
                bundle_real_path = os.path.realpath(bundle_dir)
                default_real_path = os.path.realpath(os.path.join(bundle_real_path, default))
                common_path = os.path.commonpath([bundle_real_path, default_real_path])
                if common_path != bundle_real_path:
                    raise DeadlineOperationError(
                        f"Job Template for job bundle {bundle_dir}:\nDefault PATH '{default_real_path}' for parameter '{name}' specifies files outside of Job Bundle directory '{bundle_real_path}'.\nPATH values must be relative, and must resolve within the Job Bundle directory."
                    )

                default_absolute = os.path.normpath(
                    os.path.abspath(os.path.join(bundle_dir, default))
                )
                parameter["value"] = default_absolute

    # Rearrange the dict from the template into a list
    return [
        validate_job_parameter({"name": name, **values})
        for name, values in template_parameters.items()
    ]


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
        elif param_type == "PATH":
            if param_def.get("objectType", "DIRECTORY") == "FILE":
                if param_def.get("dataFlow", "NONE") == "OUT":
                    return "CHOOSE_OUTPUT_FILE"
                else:
                    return "CHOOSE_INPUT_FILE"
            else:
                return "CHOOSE_DIRECTORY"
        elif param_type in ("INT", "FLOAT"):
            return "SPIN_BOX"
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
    return differences
