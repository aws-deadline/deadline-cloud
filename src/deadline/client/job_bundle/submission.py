# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Helper functions to enable submission of a Job Bundle to CreateJob
"""
from __future__ import annotations
import dataclasses
import logging
import os
from typing import Any, Tuple, Optional

from ..exceptions import DeadlineOperationError
from .parameters import JobParameter

logger = logging.getLogger(__name__)

DEFAULT_APP_NAME = "deadline"
DEFAULT_SUPPORTED_APP_PARAMETER_NAMES = [
    "targetTaskRunStatus",
    "priority",
    "maxFailedTasksCount",
    "maxRetriesPerTask",
]


@dataclasses.dataclass
class AssetReferences:
    """Holds the asset references for a job bundle."""

    input_filenames: set[str] = dataclasses.field(default_factory=set)
    """Filenames whose file contents are input to the job."""
    input_directories: set[str] = dataclasses.field(default_factory=set)
    """Directories whose contents are input to the job."""
    output_directories: set[str] = dataclasses.field(default_factory=set)
    """Directories whose contents are output from the job."""
    referenced_paths: set[str] = dataclasses.field(default_factory=set)
    """Paths that are referenced by the job, but not necessarily input or output."""

    def __init__(
        self,
        *,
        input_filenames: Optional[set[str]] = None,
        input_directories: Optional[set[str]] = None,
        output_directories: Optional[set[str]] = None,
        referenced_paths: Optional[set[str]] = None,
    ):
        self.input_filenames = input_filenames or set()
        self.input_directories = input_directories or set()
        self.output_directories = output_directories or set()
        self.referenced_paths = referenced_paths or set()

    def __bool__(self) -> bool:
        """Returns whether the object has any asset references."""
        return (
            bool(self.input_filenames)
            or bool(self.input_directories)
            or bool(self.output_directories)
            or bool(self.referenced_paths)
        )

    def union(self, other: AssetReferences):
        """Returns the union of the asset references."""
        return AssetReferences(
            input_filenames=self.input_filenames.union(other.input_filenames),
            input_directories=self.input_directories.union(other.input_directories),
            output_directories=self.output_directories.union(other.output_directories),
            referenced_paths=self.referenced_paths.union(other.referenced_paths),
        )

    @classmethod
    def from_dict(cls, obj: Optional[dict[str, Any]]) -> AssetReferences:
        if obj:
            input_filenames = obj["assetReferences"].get("inputs", {}).get("filenames", [])
            input_directories = obj["assetReferences"].get("inputs", {}).get("directories", [])
            output_directories = obj["assetReferences"].get("outputs", {}).get("directories", [])
            referenced_paths = obj["assetReferences"].get("referencedPaths", [])

            return cls(
                input_filenames=set(os.path.normpath(path) for path in input_filenames),
                input_directories=set(os.path.normpath(path) for path in input_directories),
                output_directories=set(os.path.normpath(path) for path in output_directories),
                referenced_paths=set(os.path.normpath(path) for path in referenced_paths),
            )
        else:
            return cls()

    def to_dict(self) -> dict[str, Any]:
        return {
            "assetReferences": {
                "inputs": {
                    "directories": sorted(self.input_directories),
                    "filenames": sorted(self.input_filenames),
                },
                "outputs": {"directories": sorted(self.output_directories)},
                "referencedPaths": sorted(self.referenced_paths),
            }
        }


def split_parameter_args(
    parameters: list[JobParameter],
    job_bundle_dir: str,
    app_name: Optional[str] = None,
    supported_app_parameter_names: Optional[list[str]] = None,
) -> Tuple[dict[str, Any], dict[str, Any]]:
    """
    Splits the input job_bundle_parameters into separate application paramters
    and job specific parameters.

    Args:
        parameters (list): The list of submission parameters.
        job_bundle_dir (str): The job bundle directory, used for error messages.
        app_name (str): The name of the application prefix to accept for application-specific
            parameters.
        supported_app_parameter_names (list[str]): The list of parameter names
            that can be provided with the `app_name` prefix.
    """

    if app_name is None:
        app_name = DEFAULT_APP_NAME
    if supported_app_parameter_names is None:
        supported_app_parameter_names = DEFAULT_SUPPORTED_APP_PARAMETER_NAMES

    job_parameters: dict[str, Any] = {}
    app_parameters: dict[str, Any] = {}

    if parameters:
        for parameter in parameters:
            if "value" in parameter:
                parameter_name = parameter["name"]
                parameter_value = parameter["value"]

                if parameter_name.startswith(f"{app_name}:"):
                    # Application-specific parameters
                    app_parameter_name = parameter_name.split(":", 1)[1]
                    if app_parameter_name in supported_app_parameter_names:
                        app_parameters[app_parameter_name] = parameter_value
                    else:
                        raise DeadlineOperationError(
                            f"Unrecognized parameter named {parameter_name!r} from job bundle:\n{job_bundle_dir}"
                        )
                elif ":" in parameter_name:
                    # Drop application-specific parameters from other applications
                    pass
                else:
                    parameter_type = parameter["type"].lower()
                    job_parameters[parameter_name] = {parameter_type: str(parameter_value)}

    return app_parameters, job_parameters
