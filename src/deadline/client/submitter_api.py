# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

__all__ = [
    "SubmissionContext",
    "SubmitterAPI",
    "SubmitterSettings",
    "append_conda_packages",
    "get_queue_parameters",
    "get_submitter_api",
    "register_submitter_api",
    "set_conda_packages",
    "set_rez_packages",
]

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, cast

from .api._queue_parameters import get_queue_parameter_definitions
from .config import config_file


@dataclass
class SubmitterSettings:
    """Common submission settings across all DCCs."""

    name: str = ""
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
        self, settings: Optional[SubmitterSettings] = None
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
    farm_id: Optional[str] = None,
    queue_id: Optional[str] = None,
    initial_values: Optional[dict[str, Any]] = None,
) -> list[dict[str, Any]]:
    """Fetch queue parameter definitions from Deadline Cloud API.

    DCC-agnostic. Uses configured defaults if farm_id/queue_id not provided.
    """
    if farm_id is None:
        farm_id = config_file.get_setting("defaults.farm_id")
    if queue_id is None:
        queue_id = config_file.get_setting("defaults.queue_id")

    if not farm_id or not queue_id:
        return []

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


# Populated at runtime by each DCC submitter package via register_submitter_api().
# The shared library intentionally does not import DCC-specific packages, nor
# does it enumerate the known DCC hosts -- consumers own that mapping.
_SUBMITTER_REGISTRY: dict[str, Callable[[], SubmitterAPI]] = {}


def register_submitter_api(host_name: str, factory: Callable[[], SubmitterAPI]) -> None:
    """Register a SubmitterAPI factory for a DCC host.

    Called by each DCC submitter package (or a consumer such as AYON) so that
    ``deadline-cloud`` does not need a hard dependency on every DCC submitter.

    Args:
        host_name: DCC identifier string (e.g. "maya", "nuke").
        factory: Zero-argument callable returning a SubmitterAPI instance.
    """
    _SUBMITTER_REGISTRY[host_name] = factory


def get_submitter_api(host_name: str) -> SubmitterAPI:
    """Return the SubmitterAPI implementation for the given DCC.

    Args:
        host_name: DCC identifier string (e.g. "maya", "nuke").

    Raises:
        ValueError: If no implementation is registered for host_name.
    """
    factory = _SUBMITTER_REGISTRY.get(host_name)
    if factory is None:
        raise ValueError(
            f"No SubmitterAPI registered for host '{host_name}'. "
            f"Registered: {sorted(_SUBMITTER_REGISTRY)}"
        )
    return factory()


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
