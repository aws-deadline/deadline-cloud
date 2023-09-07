# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

__all__ = [
    "apply_job_parameters",
    "read_job_bundle_parameters",
]

import os
from typing import Any

from ..exceptions import DeadlineOperationError
from .loader import read_yaml_or_json_object
from ..job_bundle.submission import FlatAssetReferences


def apply_job_parameters(
    job_parameters: list[dict[str, Any]],
    job_bundle_dir: str,
    job_bundle_parameters: list[dict[str, Any]],
    asset_references: FlatAssetReferences,
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

    for parameter in job_bundle_parameters:
        parameter_name = parameter["name"]
        # Skip application-specific parameters like "deadline:priority"
        if ":" in parameter_name:
            continue
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
                    f"No parameter value provided for Job Template parameter {parameter_name}, and it has no default value."
                )

        # If it's a PATH parameter with dataFlow, add it to asset_references
        if parameter_type == "PATH":
            data_flow = parameter.get("dataFlow", "NONE")
            if data_flow not in ("NONE", "IN", "OUT", "INOUT"):
                raise DeadlineOperationError(
                    f"Job Template parameter {parameter_name} had an incorrect "
                    + f"value {data_flow} for 'dataFlow'. Valid values are "
                    + "['NONE', 'IN', 'OUT', 'INOUT']"
                )
            if data_flow != "NONE":
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

    # Get the spec version of the template
    schema_version: str = ""
    if isinstance(template, dict):
        version = template.get("specificationVersion")
        if not isinstance(version, str):
            raise DeadlineOperationError("Job Template's 'specificationVersion' must be a string.")
        schema_version = version
        if schema_version not in ["2022-09-01"]:
            raise DeadlineOperationError(
                f"The Job Bundle's Job Template has an unsupported specificationVersion: {schema_version}"
            )

    # Start with the template parameters
    template_parameters = {}
    if isinstance(template, dict):
        template_parameters = template.get("parameters", {})

    if template_parameters:
        # parameters are a list of objects. Convert it to a map
        # from name -> parameter
        if not isinstance(template_parameters, list):
            raise DeadlineOperationError(
                f"Job parameters must be a list in a '{schema_version}' Job Template."
            )
        template_parameters = {param["name"]: param for param in template_parameters}

    # Add the parameter values where provided
    if parameter_values:
        for parameter_value in parameter_values.get("parameterValues", []):
            name = parameter_value["name"]
            if name in template_parameters:
                template_parameters[name]["value"] = parameter_value["value"]
            else:
                if ":" in name:
                    # Names with a ':' are for the system using the job bundle, like Amazon Deadline Cloud
                    template_parameters[name] = parameter_value

    # Make valueless PATH parameters with default and are not constrained
    # by allowedValues, absolute by joining with the job bundle directory
    for name in template_parameters:
        parameter = template_parameters[name]

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
