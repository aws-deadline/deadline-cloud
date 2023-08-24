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
    empty_path_parameters: list[str] = []

    for parameter in job_bundle_parameters:
        parameter_name = parameter["name"]
        # Skip application-specific parameters like "deadline:priority"
        if ":" in parameter_name:
            continue
        parameter_type = parameter["type"]

        # TODO: uiHint is deprecated, remove this once all job bundles use "userInterface"
        ui_hint = parameter.get("uiHint", {})
        if parameter_type == "STRING" and ui_hint.get("ojioFutureType") == "PATH":
            parameter_type = "PATH"

        # Apply the job_parameters value if available
        parameter_value = param_dict.pop(parameter_name, None)
        if parameter_value is not None:
            # Make PATH parameter values that have data flow, and are not constrained
            # by allowedValues, absolute by joining with the current working directory
            if (
                parameter_type == "PATH"
                and parameter.get("dataFlow") != "NONE"
                and "allowedValues" not in parameter
            ):
                if parameter_value == "":
                    empty_path_parameters.append(parameter_name)
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
            data_flow = parameter.get("dataFlow", ui_hint.get("assetReference", "NONE"))
            if data_flow not in ("NONE", "IN", "OUT", "INOUT"):
                raise DeadlineOperationError(
                    f"Job Template parameter {parameter_name} had an incorrect "
                    + f"value {data_flow} for 'dataFlow'. Valid values are "
                    + "['NONE', 'IN', 'OUT', 'INOUT']"
                )
            if data_flow != "NONE":
                object_type = parameter.get("objectType")
                if ui_hint and not object_type:
                    # uiHint determined the object type based on the control type,
                    # not 'objectType' is it is now.
                    control_type = ui_hint.get("controlType")
                    if not control_type:
                        raise DeadlineOperationError(
                            f"Job Template parameter {parameter_name} has a PATH type "
                            + "but is missing a controlType value required to specify whether it "
                            + "is a DIRECTORY or FILE path."
                        )
                    if control_type == "CHOOSE_DIRECTORY":
                        object_type = "DIRECTORY"
                    elif control_type in ("CHOOSE_INPUT_FILE", "CHOOSE_OUTPUT_FILE"):
                        object_type = "FILE"
                    else:
                        raise RuntimeError(
                            f"Job Template parameter {parameter_name} had an incorrect "
                            + f"control type {control_type} for the 'assetReference' in 'uiHint'"
                        )

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

    if empty_path_parameters:
        raise DeadlineOperationError(
            f"The following parameter values are missing: {empty_path_parameters}"
        )


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
            "uiHint": <if provided from template_uihint.json/yaml>
            "value": <if provided from parameter_values.json/yaml>
        },
        ...
    ]
    """

    template = read_yaml_or_json_object(bundle_dir=bundle_dir, filename="template", required=True)
    parameter_values = read_yaml_or_json_object(
        bundle_dir=bundle_dir, filename="parameter_values", required=False
    )
    # This sidecar file is deprecated and will be removed. Use parameter "userInterface" properties instead.
    template_uihint = read_yaml_or_json_object(
        bundle_dir=bundle_dir, filename="template_uihint", required=False
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

    # Add the deprecated ui hints where provided
    if template_uihint:
        for parameter_uihint in template_uihint.get("parameters", []):
            name = parameter_uihint["name"]
            if name in template_parameters:
                template_parameters[name]["uiHint"] = parameter_uihint["uiHint"]
                # If the default value is a relative path, fix it to the bundle dir
                # This only performs the transform where the parameter's type is STRING
                # but the uiHint override type is PATH.
                if (
                    "value" not in template_parameters[name]
                    and template_parameters[name]["type"] == "STRING"
                    and template_parameters[name]["uiHint"]["ojioFutureType"] == "PATH"
                    and "allowedValues" not in template_parameters[name]
                ):
                    default = template_parameters[name].get("default")
                    if default:
                        default_absolute = os.path.normpath(
                            os.path.abspath(os.path.join(bundle_dir, default))
                        )

                        if default_absolute != default:
                            template_parameters[name]["value"] = default_absolute
            else:
                raise RuntimeError(
                    f"Job bundle's template_uihint contains a parameter named '{name}' not in the template."
                )

    # Make valueless PATH parameters with default, that have data flow, and are not constrained
    # by allowedValues, absolute by joining with the job bundle directory
    for name in template_parameters:
        parameter = template_parameters[name]

        data_flow = parameter.get("dataFlow", "NONE")
        if (
            "value" not in parameter
            and parameter["type"] == "PATH"
            and data_flow != "NONE"
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
