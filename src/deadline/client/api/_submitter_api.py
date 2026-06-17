# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Unified Submitter API for DCC integrations.

Provides an abstract base class that DCC submitters implement to expose
a consistent interface for pipeline tools. External consumers call the
same methods regardless of DCC.
"""

from __future__ import annotations

__all__ = [
    "SubmissionContext",
    "SubmitterAPI",
    "SubmitterSettings",
    "append_conda_packages",
    "append_rez_packages",
    "get_queue_parameters",
    "set_conda_packages",
    "set_rez_packages",
]

from abc import ABC as _ABC
from abc import abstractmethod as _abstractmethod
from dataclasses import dataclass as _dataclass
from dataclasses import field as _field
from typing import Any as _Any
from typing import Optional as _Optional
from typing import cast as _cast

from ..config import get_setting as _get_setting
from ..exceptions import DeadlineOperationError as _DeadlineOperationError
from ._queue_parameters import (
    get_queue_parameter_definitions as _get_queue_parameter_definitions,
)


@_dataclass
class SubmitterSettings:
    """Common submission settings across all DCCs.

    DCC submitters subclass this to add DCC-specific fields.
    """

    name: str = ""
    description: str = ""
    priority: int = 50
    initial_status: str = "READY"
    max_failed_tasks_count: int = 20
    max_retries_per_task: int = 5
    max_worker_count: int = -1

    override_frame_range: bool = False
    frame_list: str = ""
    project_path: str = ""
    output_path: str = ""

    input_filenames: list[str] = _field(default_factory=list)
    input_directories: list[str] = _field(default_factory=list)
    output_directories: list[str] = _field(default_factory=list)


@_dataclass(frozen=True)
class SubmissionContext:
    """Read-only snapshot of collected submission data."""

    settings: SubmitterSettings
    job_template: dict[str, _Any]
    parameter_values: list[dict[str, _Any]]
    asset_references: dict[str, _Any]


class SubmitterAPI(_ABC):
    """Abstract base class for DCC submitter integrations.

    Each DCC submitter provides a concrete subclass that implements
    the abstract methods with DCC-specific logic. External consumers
    (pipeline tools) call these methods through a uniform interface.
    """

    @_abstractmethod
    def get_settings(self) -> SubmitterSettings:
        """Create settings fully initialized from the live DCC scene.

        Implementations MUST populate frame_list, project_path, output_path,
        and other scene-derived values. Callers should never need to manually
        set these after calling get_settings().
        """
        ...

    @_abstractmethod
    def get_job_template(
        self,
        settings: SubmitterSettings,
        host_requirements: _Optional[dict[str, _Any]] = None,
    ) -> dict[str, _Any]:
        """Build the OpenJD job template dict for submission."""
        ...

    @_abstractmethod
    def get_parameter_values(
        self,
        settings: SubmitterSettings,
        queue_parameters: list[dict[str, _Any]],
    ) -> list[dict[str, _Any]]:
        """Build the parameter values list for submission."""
        ...

    @_abstractmethod
    def get_asset_references(
        self,
        settings: SubmitterSettings,
    ) -> dict[str, _Any]:
        """Collect asset references (inputs/outputs) from the scene."""
        ...

    def get_submission_context(
        self,
        settings: _Optional[SubmitterSettings] = None,
    ) -> SubmissionContext:
        """Collect full submission data in one call.

        Args:
            settings: Pre-built settings. If None, calls get_settings()
                to initialize from the scene.
        """
        if settings is None:
            settings = self.get_settings()
        queue_parameters = get_queue_parameters()
        return SubmissionContext(
            settings=settings,
            job_template=self.get_job_template(settings),
            parameter_values=self.get_parameter_values(settings, queue_parameters),
            asset_references=self.get_asset_references(settings),
        )


def get_queue_parameters(
    farm_id: _Optional[str] = None,
    queue_id: _Optional[str] = None,
    initial_values: _Optional[dict[str, str]] = None,
) -> list[dict[str, _Any]]:
    """Fetch queue parameter definitions from Deadline Cloud API.

    DCC-agnostic utility. Uses configured defaults if farm_id/queue_id
    not provided.

    Args:
        farm_id: The farm ID. If not provided, uses the default from settings.
        queue_id: The queue ID. If not provided, uses the default from settings.
        initial_values: Optional dict of {parameter_name: value} to override
            default parameter values.

    Returns:
        A list of parameter definition dicts with "name" and "value" keys.

    Raises:
        DeadlineOperationError: If farm_id or queue_id are not configured.
    """
    if farm_id is None:
        farm_id = _get_setting("defaults.farm_id")
    if queue_id is None:
        queue_id = _get_setting("defaults.queue_id")

    if not farm_id or not queue_id:
        raise _DeadlineOperationError(
            "Farm ID and Queue ID must be configured. "
            "Either provide them as arguments or configure them in "
            "Deadline Cloud settings."
        )

    queue_parameters: list[dict[str, _Any]] = _cast(
        list[dict[str, _Any]],
        _get_queue_parameter_definitions(farmId=farm_id, queueId=queue_id),
    )

    for parameter in queue_parameters:
        if "value" not in parameter:
            parameter["value"] = parameter.get("default", "")
        if initial_values and parameter["name"] in initial_values:
            parameter["value"] = initial_values[parameter["name"]]

    return queue_parameters


# NOTE: deadline-cloud intentionally provides no discovery registry or factory
# (no get_submitter_api / register_submitter_api). A consumer always runs inside
# a known DCC and imports that DCC's concrete SubmitterAPI directly. See the TDD
# "Discovery: consumer-side direct import" section for rationale.


# --- Conda/Rez helpers ---


def _find_parameter(
    parameter_values: list[dict[str, _Any]], name: str
) -> _Optional[dict[str, _Any]]:
    """Find a parameter by name in the parameter values list."""
    for param in parameter_values:
        if param.get("name") == name:
            return param
    return None


def set_conda_packages(parameter_values: list[dict[str, _Any]], packages: str) -> None:
    """Set CondaPackages in parameter values list.

    Args:
        parameter_values: The parameter values list to modify in-place.
        packages: Space-separated conda package specifications.
    """
    param = _find_parameter(parameter_values, "CondaPackages")
    if param is not None:
        param["value"] = packages
    else:
        parameter_values.append({"name": "CondaPackages", "value": packages})


def append_conda_packages(
    parameter_values: list[dict[str, _Any]], packages: str
) -> None:
    """Append to existing CondaPackages in parameter values list.

    Args:
        parameter_values: The parameter values list to modify in-place.
        packages: Space-separated conda package specifications to append.
    """
    param = _find_parameter(parameter_values, "CondaPackages")
    if param is not None:
        existing = param["value"].strip()
        param["value"] = f"{existing} {packages}".strip()
    else:
        parameter_values.append({"name": "CondaPackages", "value": packages})


def set_rez_packages(parameter_values: list[dict[str, _Any]], packages: str) -> None:
    """Set RezPackages in parameter values list.

    Args:
        parameter_values: The parameter values list to modify in-place.
        packages: Space-separated rez package specifications.
    """
    param = _find_parameter(parameter_values, "RezPackages")
    if param is not None:
        param["value"] = packages
    else:
        parameter_values.append({"name": "RezPackages", "value": packages})


def append_rez_packages(parameter_values: list[dict[str, _Any]], packages: str) -> None:
    """Append to existing RezPackages in parameter values list.

    Args:
        parameter_values: The parameter values list to modify in-place.
        packages: Space-separated rez package specifications to append.
    """
    param = _find_parameter(parameter_values, "RezPackages")
    if param is not None:
        existing = param["value"].strip()
        param["value"] = f"{existing} {packages}".strip()
    else:
        parameter_values.append({"name": "RezPackages", "value": packages})
