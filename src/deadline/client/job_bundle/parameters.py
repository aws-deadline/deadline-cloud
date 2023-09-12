# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

__all__ = [
    "apply_job_parameters",
    "read_job_bundle_parameters",
    "get_ui_control_for_parameter_definition",
    "parameter_definition_difference",
]

import os
from typing import Any

from ..exceptions import DeadlineOperationError
from .loader import read_yaml_or_json_object
from ..job_bundle.submission import AssetReferences


def apply_job_parameters(
    job_parameters: list[dict[str, Any]],
    job_bundle_dir: str,
    job_bundle_parameters: list[dict[str, Any]],
    queue_parameter_definitions: list[dict[str, Any]],
    asset_references: AssetReferences,
) -> None:
    """
    Modifies the provided job_bundle_parameters and asset_references to incorporate
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
    param_dict = {parameter["name"]: parameter["value"] for parameter in job_parameters}
    modified_job_parameters = param_dict.copy()

    queue_param_dict = {parameter["name"]: parameter for parameter in queue_parameter_definitions}

    for parameter in job_bundle_parameters:
        parameter_name = parameter["name"]
        # Skip application-specific parameters like "deadline:priority"
        if ":" in parameter_name:
            continue
        if "type" not in parameter:
            if parameter_name in queue_param_dict:
                # Use the parameter definition from the queue if the job didn't supply one
                parameter.update(queue_param_dict[parameter_name])
            else:
                raise DeadlineOperationError(
                    f"Job Template for job bundle {job_bundle_dir}:\nJob Template parameter {parameter_name} is missing its type."
                )
        parameter_type = parameter["type"]

        # Apply the job_parameters value if available
        parameter_value = param_dict.pop(parameter_name, None)
        if parameter_value is not None:
            # Make PATH parameter values that  are not constrained by allowedValues
            # absolute by joining with the current working directory
            if parameter_type == "PATH" and "allowedValues" not in parameter:
                if parameter_value == "":
                    continue
                parameter_value = os.path.abspath(parameter_value)
                modified_job_parameters[parameter_name] = parameter_value
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
            else:
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

    job_parameters.clear()
    for param_name, param_value in modified_job_parameters.items():
        job_parameters.append({"name": param_name, "value": param_value})


def read_job_bundle_parameters(bundle_dir: str) -> list[dict[str, Any]]:
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

    # Make valueless PATH parameters with default but not constrained
    # by allowedValues, absolute by joining with the job bundle directory
    for parameter in template_parameters.values():
        if (
            "value" not in parameter
            and parameter["type"] == "PATH"
            and "allowedValues" not in parameter
        ):
            default = parameter.get("default")
            if default:
                default_absolute = os.path.normpath(
                    os.path.abspath(os.path.join(bundle_dir, default))
                )

                if default_absolute != default:
                    parameter["value"] = default_absolute

    # Rearrange the dict from the template into a list
    return [{"name": name, **values} for name, values in template_parameters.items()]


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


def get_ui_control_for_parameter_definition(param_def: dict[str, Any]) -> str:
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
    lhs: dict[str, Any],
    rhs: dict[str, Any],
    field_name: str,
    set_comparison: bool = False,
) -> bool:
    lhs_value = lhs.get(field_name)
    rhs_value = rhs.get(field_name)
    if set_comparison and lhs_value is not None and rhs_value is not None:
        return set(lhs_value) == set(rhs_value)
    else:
        return lhs_value == rhs_value


def parameter_definition_difference(lhs: dict[str, Any], rhs: dict[str, Any]) -> list[str]:
    """Compares the two parameter definitions, returning a list of fields which differ.
    Does not compare the userInterface properties.
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
        if not _parameter_definition_fields_equivalent(lhs, rhs, name):
            differences.append(name)
    # Compare these properties as sets
    for name in ("allowedValues",):
        if not _parameter_definition_fields_equivalent(lhs, rhs, name, set_comparison=True):
            differences.append(name)
    return differences
