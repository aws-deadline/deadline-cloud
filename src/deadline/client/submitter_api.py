# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Optional, cast

from .config import config_file
from .exceptions import DeadlineOperationError


@dataclass
class SubmitterSettings:
    """Common submission settings across all DCCs.

    DCC submitters subclass this to add DCC-specific fields.
    """

    name: str = ""
    description: str = ""
    frame_list: str = ""
    project_path: str = ""
    output_path: str = ""
    priority: int = 50
    initial_status: str = "READY"
    max_failed_tasks_count: int = 20
    max_retries_per_task: int = 5
    max_worker_count: int = -1
    override_frame_range: bool = False
    input_filenames: list[str] = field(default_factory=list)
    input_directories: list[str] = field(default_factory=list)
    output_directories: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class SubmissionContext:
    """Read-only snapshot of collected submission data."""

    settings: SubmitterSettings
    job_template: dict[str, Any]
    parameter_values: list[dict[str, Any]]
    asset_references: dict[str, Any]


class SubmitterAPI(ABC):
    """Abstract base class defining the unified interface all DCC submitters implement."""

    @abstractmethod
    def get_settings(self) -> SubmitterSettings:
        """Create settings fully initialized from the live DCC scene.

        This MUST populate frame_list, project_path, output_path,
        and other scene-derived values. Callers should never need
        to manually set these after calling get_settings().
        """

    @abstractmethod
    def get_job_template(
        self,
        settings: SubmitterSettings,
        host_requirements: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Build the OpenJD job template dict for submission."""

    @abstractmethod
    def get_parameter_values(
        self,
        settings: SubmitterSettings,
        queue_parameters: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Build the parameter values list for submission."""

    @abstractmethod
    def get_asset_references(
        self,
        settings: SubmitterSettings,
    ) -> dict[str, Any]:
        """Collect asset references (inputs/outputs) from the scene."""

    def get_submission_context(
        self,
        settings: Optional[SubmitterSettings] = None,
        *,
        farm_id: Optional[str] = None,
        queue_id: Optional[str] = None,
        initial_values: Optional[dict[str, Any]] = None,
        host_requirements: Optional[dict[str, Any]] = None,
        queue_parameters: Optional[list[dict[str, Any]]] = None,
    ) -> SubmissionContext:
        """Collect full submission data in one call.

        Args:
            settings: Pre-built settings. If None, calls get_settings()
                to initialize from the scene.
            farm_id: Target farm ID. If None, uses the configured default.
                Ignored when ``queue_parameters`` is provided.
            queue_id: Target queue ID. If None, uses the configured default.
                Ignored when ``queue_parameters`` is provided.
            initial_values: Optional {parameter_name: value} overrides for the
                queue parameters. Ignored when ``queue_parameters`` is provided.
            host_requirements: Optional host requirements forwarded to
                get_job_template().
            queue_parameters: Pre-fetched queue parameters. When provided, it is
                used as-is and farm_id/queue_id/initial_values are not consulted.
        """
        if settings is None:
            settings = self.get_settings()
        if queue_parameters is None:
            queue_parameters = get_queue_parameters(
                farm_id=farm_id,
                queue_id=queue_id,
                initial_values=initial_values,
            )
        return SubmissionContext(
            settings=settings,
            job_template=self.get_job_template(settings, host_requirements),
            parameter_values=self.get_parameter_values(settings, queue_parameters),
            asset_references=self.get_asset_references(settings),
        )


def get_queue_parameters(
    farm_id: Optional[str] = None,
    queue_id: Optional[str] = None,
    initial_values: Optional[dict[str, Any]] = None,
) -> list[dict[str, Any]]:
    """Fetch queue parameter definitions from Deadline Cloud API.

    DCC-agnostic. Uses configured defaults if farm_id/queue_id not provided.

    Args:
        farm_id: The farm ID. If not provided, uses the default from settings.
        queue_id: The queue ID. If not provided, uses the default from settings.
        initial_values: Optional dict of {parameter_name: value} to override
            default parameter values.

    Returns:
        A list of full queue-parameter *definition* dicts (each carrying the
        ``JobParameter`` fields ``name``/``type``/``default``/``userInterface``/
        ``allowedValues``/``minValue``/… as returned by
        ``get_queue_parameter_definitions``), with each augmented with a
        ``value`` key resolved from ``default`` and any ``initial_values``
        override. These are DCC-submitter inputs (passed to
        ``get_parameter_values``), NOT the reduced name/value
        ``parameterValues`` that ``deadline:CreateJob`` accepts — do not pass
        them straight to the service, and note the ``set_*``/``append_*``
        helpers below mutate this definition list in place.

    Raises:
        DeadlineOperationError: If farm_id or queue_id are not configured.
    """
    # Imported lazily to avoid importing the ``deadline.client.api`` package at
    # module load time. ``deadline.client.api`` re-exports the symbols defined
    # here, so a top-level import would create a circular import.
    from .api._queue_parameters import get_queue_parameter_definitions

    if farm_id is None:
        farm_id = config_file.get_setting("defaults.farm_id")
    if queue_id is None:
        queue_id = config_file.get_setting("defaults.queue_id")

    if not farm_id or not queue_id:
        raise DeadlineOperationError(
            "Farm ID and Queue ID must be configured. "
            "Either provide them as arguments or configure them in "
            "Deadline Cloud settings."
        )

    params = cast(
        "list[dict[str, Any]]",
        get_queue_parameter_definitions(farmId=farm_id, queueId=queue_id),
    )

    for param in params:
        if "value" not in param:
            param["value"] = param.get("default", "")
        if initial_values and param["name"] in initial_values:
            param["value"] = initial_values[param["name"]]

    return params


# NOTE: deadline-cloud intentionally provides no discovery registry or factory
# (no register_submitter_api / get_submitter_api). A consumer always runs inside
# a known DCC and imports that DCC's concrete SubmitterAPI directly. See the TDD
# "Discovery: consumer-side direct import" section for rationale.


def set_conda_packages(
    parameter_values: list[dict[str, Any]],
    packages: str,
) -> None:
    """Set CondaPackages in parameter values list."""
    for param in parameter_values:
        if param.get("name") == "CondaPackages":
            param["value"] = packages
            return
    parameter_values.append({"name": "CondaPackages", "value": packages})


def append_conda_packages(
    parameter_values: list[dict[str, Any]],
    packages: str,
) -> None:
    """Append to existing CondaPackages in parameter values list."""
    for param in parameter_values:
        if param.get("name") == "CondaPackages":
            existing = param.get("value", "")
            param["value"] = f"{existing} {packages}".strip() if existing else packages
            return
    parameter_values.append({"name": "CondaPackages", "value": packages})


def set_rez_packages(
    parameter_values: list[dict[str, Any]],
    packages: str,
) -> None:
    """Set RezPackages in parameter values list."""
    for param in parameter_values:
        if param.get("name") == "RezPackages":
            param["value"] = packages
            return
    parameter_values.append({"name": "RezPackages", "value": packages})


def append_rez_packages(
    parameter_values: list[dict[str, Any]],
    packages: str,
) -> None:
    """Append to existing RezPackages in parameter values list."""
    for param in parameter_values:
        if param.get("name") == "RezPackages":
            existing = param.get("value", "")
            param["value"] = f"{existing} {packages}".strip() if existing else packages
            return
    parameter_values.append({"name": "RezPackages", "value": packages})
