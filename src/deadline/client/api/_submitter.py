# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

__all__ = [
    "BaseSubmitter",
    "BaseSubmitterSettings",
    "SubmissionContext",
    "get_queue_parameters",
]

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Optional, cast

from ..config import config_file
from ..exceptions import DeadlineOperationError
from ..job_bundle.submission import AssetReferences


@dataclass
class BaseSubmitterSettings:
    """Common submission settings across all DCCs.

    DCC submitters subclass this to add DCC-specific fields.

    All fields are optional (sensible defaults). ``frame_list`` in particular is
    not required — it assumes a frame-based rendering workload, and some
    submitters never set it (e.g. Nuke). An empty ``frame_list`` means "use the
    frame range defined in the scene."
    """

    job_name: str = ""
    """Name shown for the job in Deadline Cloud. Submitters typically default it
    to the scene/file name."""
    description: str = ""
    """Optional free-form description of the job."""
    frame_list: str = ""
    """OpenJD frame-list string (e.g. "1-10", "1-100:2"). Optional: empty means
    "use the scene's frame range"; leave unset for non-frame-based workloads
    (e.g. Nuke does not populate it)."""
    project_path: str = ""
    """Root working/project directory of the scene (an INPUT path — e.g. the Maya
    workspace / project root), used to anchor relative asset paths and as an
    input directory. Not the render output location (see ``output_path``)."""
    output_path: str = ""
    """Directory the DCC writes rendered frames to (an OUTPUT path)."""
    priority: int = 50
    """Job priority (higher runs first); Deadline Cloud range is 0-100."""
    initial_status: str = "READY"
    """Lifecycle status the job starts in — ``"READY"`` to run immediately or
    ``"SUSPENDED"`` to create it paused."""
    max_failed_tasks_count: int = 20
    """Fail the whole job once this many tasks have failed."""
    max_retries_per_task: int = 5
    """How many times a single failed task is retried before it is marked failed."""
    max_worker_count: int = -1
    """Upper bound on workers assigned to this job; ``-1`` means unlimited (let
    the queue decide)."""
    override_frame_range: bool = False
    """When True, submit ``frame_list`` instead of the scene's own frame range."""
    input_filenames: list[str] = field(default_factory=list)
    """Explicit input files to attach (merged with scene-detected inputs in
    ``get_asset_references``)."""
    input_directories: list[str] = field(default_factory=list)
    """Explicit input directories whose contents are attached as job inputs."""
    output_directories: list[str] = field(default_factory=list)
    """Directories whose contents are treated as job outputs."""


@dataclass(frozen=True)
class SubmissionContext:
    """Read-only snapshot of collected submission data."""

    settings: BaseSubmitterSettings
    """The resolved submission settings used to build this context."""
    job_template: dict[str, Any]
    """The OpenJD job template dict for the submission."""
    parameter_values: list[dict[str, Any]]
    """Resolved queue/job parameter values as ``{"name": ..., "value": ...}`` dicts."""
    asset_references: AssetReferences
    """Collected input/output asset references (call ``.to_dict()`` to serialize)."""


class BaseSubmitter(ABC):
    """Abstract base class defining the unified interface all DCC submitters implement."""

    @abstractmethod
    def get_settings(self) -> BaseSubmitterSettings:
        """Create settings initialized from the live DCC scene.

        Populates the scene-derived values that apply to the DCC (e.g.
        project_path, output_path) so callers rarely need to set them by hand.
        frame_list is optional — populate it when the DCC exposes a frame range,
        or leave it empty to mean "use the scene's frame range" (e.g. Nuke does
        not populate it).
        """

    @abstractmethod
    def get_job_template(
        self,
        settings: BaseSubmitterSettings,
        host_requirements: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Build the OpenJD job template dict for submission."""

    @abstractmethod
    def get_parameter_values(
        self,
        settings: BaseSubmitterSettings,
        queue_parameters: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Build the parameter values list for submission."""

    @abstractmethod
    def get_asset_references(
        self,
        settings: BaseSubmitterSettings,
    ) -> AssetReferences:
        """Collect asset references (inputs/outputs) from the scene.

        Returns the typed :class:`AssetReferences` (not a plain dict); call
        ``.to_dict()`` at the serialization boundary if a job-bundle dict is
        needed.
        """

    def get_submission_context(
        self,
        settings: Optional[BaseSubmitterSettings] = None,
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
        them straight to the service. To override a parameter's resolved value,
        pass ``initial_values`` here (a ``{parameter_name: value}`` mapping,
        matching the submit dialog's ``initial_shared_parameter_values``).

    Raises:
        DeadlineOperationError: If farm_id or queue_id are not configured.
    """
    # Imported lazily to avoid a circular import: ``deadline.client.api``'s
    # ``__init__`` imports this module to re-export its symbols, so importing a
    # sibling ``api`` submodule at module load time would re-enter that package
    # while it is still initializing.
    from ._queue_parameters import get_queue_parameter_definitions

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
