# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Helper functions to enable submission of a Job Bundle to CreateJob
"""
from __future__ import annotations

import dataclasses
import logging
import os
from typing import Any, Callable, Dict, List, Tuple, Optional

from deadline.job_attachments.models import AssetRootManifest
from deadline.job_attachments.progress_tracker import SummaryStatistics
from deadline.job_attachments.upload import S3AssetManager

from ..exceptions import DeadlineOperationError

logger = logging.getLogger(__name__)

DEFAULT_APP_NAME = "deadline"
DEFAULT_SUPPORTED_APP_PARAMETER_NAMES = [
    "targetTaskRunStatus",
    "priority",
    "maxFailedTasksCount",
    "maxRetriesPerTask",
]


@dataclasses.dataclass
class FlatAssetReferences:
    """Flat representation of a job bundle's asset references."""

    input_filenames: set[str] = dataclasses.field(default_factory=set)
    input_directories: set[str] = dataclasses.field(default_factory=set)
    output_directories: set[str] = dataclasses.field(default_factory=set)

    def __init__(
        self,
        *,
        input_filenames: Optional[set[str]] = None,
        input_directories: Optional[set[str]] = None,
        output_directories: Optional[set[str]] = None,
    ):
        if input_filenames:
            self.input_filenames = input_filenames
        else:
            self.input_filenames = set()
        if input_directories:
            self.input_directories = input_directories
        else:
            self.input_directories = set()
        if output_directories:
            self.output_directories = output_directories
        else:
            self.output_directories = set()

    def __bool__(self) -> bool:
        """Returns whether the object has any asset references."""
        return (
            bool(self.input_filenames)
            or bool(self.input_directories)
            or bool(self.output_directories)
        )

    def union(self, other: FlatAssetReferences):
        """Returns the union of the asset references."""
        return FlatAssetReferences(
            input_filenames=self.input_filenames.union(other.input_filenames),
            input_directories=self.input_directories.union(other.input_directories),
            output_directories=self.output_directories.union(other.output_directories),
        )

    @classmethod
    def from_dict(cls, obj: Optional[dict[str, Any]]) -> FlatAssetReferences:
        if obj:
            input_filenames = obj["assetReferences"].get("inputs", {}).get("filenames", [])
            input_directories = obj["assetReferences"].get("inputs", {}).get("directories", [])
            output_directories = obj["assetReferences"].get("outputs", {}).get("directories", [])

            return cls(
                input_filenames=set(os.path.normpath(path) for path in input_filenames),
                input_directories=set(os.path.normpath(path) for path in input_directories),
                output_directories=set(os.path.normpath(path) for path in output_directories),
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
            }
        }


def upload_job_attachments(
    asset_manager: S3AssetManager,
    manifests: List[AssetRootManifest],
    upload_progress_callback: Callable,
) -> Tuple[SummaryStatistics, Dict[str, Any]]:
    (upload_summary, attachments) = asset_manager.upload_assets(
        manifests=manifests, on_uploading_assets=upload_progress_callback
    )

    # TODO: dataclasses.asdict doesn't respect the "metadata(exclude)" from dataclasses_json
    #       that DeadlineJobAttachments is using. Would like to consider removing
    #       DeadlineJobAttachments' dependency on dataclasses_json to reduce the dependency
    #       footprint we place inside of DCCs, and find a consistent way to handle this.
    uploaded_attachments = _remove_nones(dataclasses.asdict(attachments))
    for manifest_properties in uploaded_attachments["manifests"]:
        if (
            "outputRelativeDirectories" in manifest_properties
            and manifest_properties["outputRelativeDirectories"] == []
        ):
            del manifest_properties["outputRelativeDirectories"]

    return upload_summary, uploaded_attachments


def split_parameter_args(
    job_bundle_parameters: list[dict[str, Any]],
    job_bundle_dir: str,
    app_name: Optional[str] = None,
    supported_app_parameter_names: Optional[list[str]] = None,
) -> Tuple[dict[str, Any], dict[str, Any]]:
    """
    Splits the input job_bundle_parameters into separate application paramters
    and job specific parameters.

    Args:
        job_bundle_parameters (list): The list of parameter values from the job bundle.
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

    if job_bundle_parameters:
        for parameter in job_bundle_parameters:
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


def _remove_nones(obj: Any) -> Any:
    """
    Removes any fields from dicts contained
    within the object whose value is None. Recursively
    processes any dict or list within the object.

    Modifies obj in place, and returns it.
    """
    if isinstance(obj, dict):
        keys_to_remove = []
        for key, value in obj.items():
            if value is None:
                keys_to_remove.append(key)
            elif isinstance(value, (dict, list)):
                _remove_nones(value)
        for key in keys_to_remove:
            del obj[key]
    elif isinstance(obj, list):
        for i in range(len(obj)):
            _remove_nones(obj[i])
    return obj
