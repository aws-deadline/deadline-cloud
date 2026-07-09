# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Central controller for Deadline Cloud UI operations.

This controller manages all async API operations, ensuring proper
ordering of dependent calls and preventing race conditions.
"""

from configparser import ConfigParser
from logging import getLogger
from typing import Dict, Iterator, List, Optional, Tuple
import sys

from qtpy.QtCore import QObject, Qt, Signal

from ... import api
from ...api._list_apis import _iter_farms_by_region
from ...api._session import _resolve_region
from ...config import config_file, set_setting
from ...exceptions import DeadlineOperationError
from ...job_bundle.parameters import JobParameter
from ._async_runner import AsyncTaskRunner


logger = getLogger(__name__)


# Type alias for resource lists: [(display_name, id), ...]
ResourceList = List[Tuple[str, str]]

# Type alias for farm options, which additionally carry the farm's region per the
# (region, farm_id) ordering convention: [(label, farm_id, region), ...].
FarmList = List[Tuple[str, str, str]]


def _farm_label(display_name: str, region: str) -> str:
    """
    Builds a farm combo-box label, region first per the (region, farm_id) convention.

    e.g. ``(us-west-2) My Farm``. When the region is unknown/empty (legacy
    single-region responses), fall back to just the display name.
    """
    if region:
        return f"({region}) {display_name}"
    return display_name


class DeadlineUIController(QObject):
    """
    Central controller for Deadline Cloud UI operations.

    All API calls should go through this controller to ensure:
    - Proper ordering of dependent operations (farm -> queue -> storage profile)
    - Automatic cancellation of superseded requests
    - Thread-safe signal delivery to UI components
    - Centralized error handling

    Widgets should connect to this controller's signals rather than
    making API calls directly.

    Example::

        from deadline.client.ui.controllers import DeadlineUIController
        from qtpy.QtCore import Qt

        controller = DeadlineUIController.getInstance()
        controller.farms_updated.connect(
            self._on_farms_updated, Qt.QueuedConnection
        )
        controller.refresh_farms()

    Signals:
        farms_updated: Emitted when the farm list is (re)set wholesale. Args:
            [(label, farm_id, region), ...]. For multi-region refreshes this is
            emitted once with an empty list at the start of a refresh (to clear the
            box); farms then arrive incrementally via farms_appended.
        farms_appended: Emitted per region as that region's ListFarms completes, so
            the combo box fills in incrementally and a slow region does not delay
            farms that already came back. Args: [(label, farm_id, region), ...]
        farm_region_warning: Emitted when a single region's ListFarms fails during a
            multi-region refresh. Non-blocking: surviving regions' farms still show.
            Args: (region, exception)
        queues_updated: Emitted when queue list is updated. Args: [(name, queue_id), ...]
        storage_profiles_updated: Emitted when storage profiles are updated.
        queue_parameters_updated: Emitted when queue parameters are loaded.
        farms_loading: Emitted when farm loading state changes. Args: bool
        queues_loading: Emitted when queue loading state changes. Args: bool
        storage_profiles_loading: Emitted when storage profile loading state changes.
        queue_parameters_loading: Emitted when queue parameter loading state changes.
        operation_failed: Emitted when any operation fails. Args: (operation_name, exception)
    """

    # ─────────────────────────────────────────────────────────────
    # Resource List Signals
    # ─────────────────────────────────────────────────────────────

    farms_updated = Signal(list)
    farms_appended = Signal(list)
    farm_region_warning = Signal(str, BaseException)
    queues_updated = Signal(list)
    storage_profiles_updated = Signal(list)
    queue_parameters_updated = Signal(list)

    # Emitted after a farm or queue selection has been persisted, so a host dialog
    # can reload queue parameters and refresh its Submit button state.
    selection_changed = Signal()

    # ─────────────────────────────────────────────────────────────
    # Loading State Signals
    # ─────────────────────────────────────────────────────────────

    farms_loading = Signal(bool)
    queues_loading = Signal(bool)
    storage_profiles_loading = Signal(bool)
    queue_parameters_loading = Signal(bool)

    # ─────────────────────────────────────────────────────────────
    # Error Signal
    # ─────────────────────────────────────────────────────────────

    operation_failed = Signal(str, BaseException)

    # ─────────────────────────────────────────────────────────────
    # Singleton
    # ─────────────────────────────────────────────────────────────

    _instance: Optional["DeadlineUIController"] = None

    @classmethod
    def getInstance(cls) -> "DeadlineUIController":
        """Get the singleton controller instance."""
        if cls._instance is None:
            cls._instance = DeadlineUIController()
        return cls._instance

    @classmethod
    def resetInstance(cls) -> None:
        """
        Reset the singleton instance.

        Shuts down the existing instance and clears it.
        A new instance will be created on next getInstance() call.
        Primarily useful for testing.
        """
        if cls._instance is not None:
            cls._instance.shutdown()
            cls._instance = None

    def __init__(self, parent: Optional[QObject] = None) -> None:
        super().__init__(parent)

        self._task_runner = AsyncTaskRunner(self)
        self._task_runner.task_error.connect(self.operation_failed.emit, Qt.QueuedConnection)

        # Configuration
        self._config: Optional[ConfigParser] = None

        # Current selection state (for cascading refreshes)
        self._current_farm_id: str = ""
        self._current_queue_id: str = ""

        # Maps farm_id -> region, populated as farms stream in from each region.
        # Used so downstream queue/storage-profile calls target the farm's region
        # rather than the session default. Reset at the start of each farm refresh.
        self._farm_regions: Dict[str, str] = {}
        # Multi-region refresh outcome tracking (to detect the all-fail case).
        self._farms_any_succeeded: bool = False
        self._farms_any_failed: bool = False

    # ─────────────────────────────────────────────────────────────
    # Configuration
    # ─────────────────────────────────────────────────────────────

    def set_config(self, config: Optional[ConfigParser]) -> None:
        """
        Update the configuration used for API calls.

        Args:
            config: ConfigParser instance, or None to use default config
        """
        if config is not None:
            self._config = ConfigParser()
            self._config.read_dict(config)
        else:
            self._config = None

    @property
    def config(self) -> Optional[ConfigParser]:
        """Current configuration."""
        return self._config

    @property
    def current_farm_id(self) -> str:
        """Currently selected farm ID."""
        return self._current_farm_id

    @property
    def current_queue_id(self) -> str:
        """Currently selected queue ID."""
        return self._current_queue_id

    def region_for_farm(self, farm_id: str) -> Optional[str]:
        """
        Returns the region a farm was discovered in during the last refresh, or None.

        Resolution order: the region observed while streaming farms in this session,
        then the per-farm persisted ``defaults.farm_region`` setting (so it works even
        before a refresh has run, e.g. right after dialog open). Returns None when
        unknown, which lets callers fall back to the session/profile region.

        Because ``defaults.farm_region`` is keyed per farm (it depends on
        ``defaults.farm_id``), the persisted lookup is scoped to *this* ``farm_id`` via
        ``_resolve_region`` — selecting a non-default farm must not leak the default
        farm's region.
        """
        region = self._farm_regions.get(farm_id)
        if region:
            return region
        return _resolve_region(config=self._config, farm_id=farm_id)

    # ─────────────────────────────────────────────────────────────
    # Farm Operations
    # ─────────────────────────────────────────────────────────────

    def refresh_farms(self) -> None:
        """
        Fetch the list of farms across all regions, incrementally.

        Consumes the streaming :func:`_iter_farms_by_region` generator so farm
        options are surfaced to the UI as each region's ListFarms completes (out of
        order). A slow region does not delay farms that have already returned.

        Emission sequence:
          - ``farms_updated([])`` once up front to clear the box (without losing the
            user's current selection — the combo box preserves selection on appends).
          - ``farms_appended([...])`` per successful region as it arrives.
          - ``farm_region_warning(region, exc)`` per region that fails (non-blocking).
          - ``operation_failed`` only if *every* region fails (zero successes).
        """
        # Reset per-refresh region tracking; clear the box for the new results.
        self._farm_regions = {}
        self._farms_any_succeeded = False
        self._farms_any_failed = False
        self.farms_loading.emit(True)
        self.farms_updated.emit([])
        self._task_runner.run_streaming(
            operation_key="list_farms",
            fn=self._iter_farms_by_region,
            on_progress=self._on_farms_region_result,
            on_finished=self._on_farms_stream_finished,
            on_error=self._on_farms_error,
        )

    def _iter_farms_by_region(
        self,
    ) -> Iterator[Tuple[str, Optional[List[dict]], Optional[BaseException]]]:
        """Yield each region's ListFarms result as it completes. Runs in a thread."""
        return _iter_farms_by_region(config=self._config)

    def _on_farms_region_result(
        self,
        region_result: Tuple[str, Optional[List[dict]], Optional[BaseException]],
    ) -> None:
        """
        Handle a single region's streamed ListFarms result (main thread).

        Per the (region, farm_id) convention each tuple leads with the region.
        Success appends that region's farms; failure emits a non-blocking warning.
        """
        region, farms, exc = region_result
        if exc is not None:
            self._farms_any_failed = True
            # AccessDeniedException is expected when a region isn't opted-in / lacks perms.
            error_name = type(exc).__name__
            if "AccessDeniedException" in error_name:
                logger.debug("Could not fetch farms in region %s: %s", region, exc)
            else:
                logger.warning("Failed to list farms in region %s: %s", region, exc)
            self.farm_region_warning.emit(region, exc)
            return

        self._farms_any_succeeded = True
        farm_options: FarmList = []
        for item in farms or []:
            farm_id = item["farmId"]
            self._farm_regions[farm_id] = region
            farm_options.append((_farm_label(item["displayName"], region), farm_id, region))

        farm_options.sort(key=lambda option: (option[0].casefold(), option[1]))
        # Append this region's farms; the combo box adds them without resetting selection.
        self.farms_appended.emit(farm_options)

    def _on_farms_stream_finished(self) -> None:
        """Handle completion of the multi-region farm stream (main thread)."""
        self.farms_loading.emit(False)
        # If no region succeeded but at least one failed, surface a hard error so the
        # combo box can show the failed state rather than an empty success.
        if not self._farms_any_succeeded and self._farms_any_failed:
            self.operation_failed.emit(
                "list_farms",
                DeadlineOperationError("Failed to list farms in all regions."),
            )

    def _on_farms_error(self, error: BaseException) -> None:
        """Handle a fatal farm stream error (e.g. all regions failed in the API layer)."""
        # AccessDeniedException is expected when credentials are invalid or expired.
        # Don't log this as an exception.
        error_name = type(error).__name__
        if "AccessDeniedException" in error_name:
            logger.debug(f"Could not fetch farms: {error}")
        else:
            logger.exception("Failed to fetch farms", exc_info=error)
        self.farms_loading.emit(False)
        self.farms_updated.emit([])

    # ─────────────────────────────────────────────────────────────
    # Queue Operations
    # ─────────────────────────────────────────────────────────────

    def refresh_queues(self, farm_id: Optional[str] = None) -> None:
        """
        Fetch the list of queues for a farm.

        Args:
            farm_id: Farm ID to fetch queues for. If None, uses current farm.
        """
        if farm_id is None:
            farm_id = self._current_farm_id

        if not farm_id:
            self.queues_updated.emit([])
            return

        region = self.region_for_farm(farm_id)
        self.queues_loading.emit(True)
        self._task_runner.run(
            operation_key="list_queues",
            fn=self._fetch_queues,
            on_success=self._on_queues_success,
            on_error=self._on_queues_error,
            farm_id=farm_id,
            region=region,
        )

    def _fetch_queues(self, farm_id: str, region: Optional[str] = None) -> ResourceList:
        """Fetch queues from API in the farm's region. Runs in background thread."""
        response = api.list_queues(config=self._config, region=region, farmId=farm_id)
        return sorted(
            [(item["displayName"], item["queueId"]) for item in response["queues"]],
            key=lambda item: (item[0].casefold(), item[1]),
        )

    def _on_queues_success(self, queues: ResourceList) -> None:
        """Handle successful queue list fetch."""
        self.queues_loading.emit(False)
        self.queues_updated.emit(queues)

    def _on_queues_error(self, error: BaseException) -> None:
        """Handle queue list fetch error."""
        # ResourceNotFoundException is expected when switching profiles - the old farm_id
        # may not exist in the new profile. Don't log this as an exception.
        error_name = type(error).__name__
        if "ResourceNotFoundException" in error_name or "AccessDeniedException" in error_name:
            logger.debug(f"Could not fetch queues: {error}")
        else:
            logger.exception("Failed to fetch queues", exc_info=error)
        self.queues_loading.emit(False)
        self.queues_updated.emit([])

    # ─────────────────────────────────────────────────────────────
    # Storage Profile Operations
    # ─────────────────────────────────────────────────────────────

    def refresh_storage_profiles(
        self,
        farm_id: Optional[str] = None,
        queue_id: Optional[str] = None,
    ) -> None:
        """
        Fetch storage profiles for a queue.

        Args:
            farm_id: Farm ID. If None, uses current farm.
            queue_id: Queue ID. If None, uses current queue.
        """
        if farm_id is None:
            farm_id = self._current_farm_id
        if queue_id is None:
            queue_id = self._current_queue_id

        if not farm_id or not queue_id:
            self.storage_profiles_updated.emit([])
            return

        region = self.region_for_farm(farm_id)
        self.storage_profiles_loading.emit(True)
        self._task_runner.run(
            operation_key="list_storage_profiles",
            fn=self._fetch_storage_profiles,
            on_success=self._on_storage_profiles_success,
            on_error=self._on_storage_profiles_error,
            farm_id=farm_id,
            queue_id=queue_id,
            region=region,
        )

    def _fetch_storage_profiles(
        self, farm_id: str, queue_id: str, region: Optional[str] = None
    ) -> ResourceList:
        """Fetch storage profiles from API. Runs in background thread."""
        # Determine current OS for filtering
        if sys.platform.startswith("linux"):
            current_os = "linux"
        elif sys.platform.startswith("darwin"):
            current_os = "macos"
        elif sys.platform.startswith("win"):
            current_os = "windows"
        else:
            current_os = "unknown"

        response = api.list_storage_profiles_for_queue(
            config=self._config, region=region, farmId=farm_id, queueId=queue_id
        )

        profiles: ResourceList = []
        for item in response.get("storageProfiles", []):
            if item.get("osFamily", "").lower() == current_os:
                profiles.append((item["displayName"], item["storageProfileId"]))

        # Add "none selected" option at the beginning
        profiles.insert(0, ("<none selected>", ""))

        return sorted(profiles, key=lambda item: (item[0].casefold(), item[1]))

    def _on_storage_profiles_success(self, profiles: ResourceList) -> None:
        """Handle successful storage profile fetch."""
        self.storage_profiles_loading.emit(False)
        self.storage_profiles_updated.emit(profiles)

    def _on_storage_profiles_error(self, error: BaseException) -> None:
        """Handle storage profile fetch error."""
        # ResourceNotFoundException is expected when switching profiles - the old farm_id/queue_id
        # may not exist in the new profile. Don't log this as an exception.
        error_name = type(error).__name__
        if "ResourceNotFoundException" in error_name or "AccessDeniedException" in error_name:
            logger.debug(f"Could not fetch storage profiles: {error}")
        else:
            logger.exception("Failed to fetch storage profiles", exc_info=error)
        self.storage_profiles_loading.emit(False)
        self.storage_profiles_updated.emit([])

    # ─────────────────────────────────────────────────────────────
    # Queue Parameters Operations
    # ─────────────────────────────────────────────────────────────

    def refresh_queue_parameters(
        self,
        farm_id: Optional[str] = None,
        queue_id: Optional[str] = None,
    ) -> None:
        """
        Fetch queue parameter definitions.

        Args:
            farm_id: Farm ID. If None, uses current farm.
            queue_id: Queue ID. If None, uses current queue.
        """
        if farm_id is None:
            farm_id = self._current_farm_id
        if queue_id is None:
            queue_id = self._current_queue_id

        if not farm_id or not queue_id:
            self.queue_parameters_updated.emit([])
            return

        self.queue_parameters_loading.emit(True)
        self._task_runner.run(
            operation_key="get_queue_parameters",
            fn=self._fetch_queue_parameters,
            on_success=self._on_queue_parameters_success,
            on_error=self._on_queue_parameters_error,
            farm_id=farm_id,
            queue_id=queue_id,
        )

    def _fetch_queue_parameters(self, farm_id: str, queue_id: str) -> List[JobParameter]:
        """Fetch queue parameters from API. Runs in background thread."""
        return api.get_queue_parameter_definitions(
            config=self._config, farmId=farm_id, queueId=queue_id
        )

    def _on_queue_parameters_success(self, parameters: List[JobParameter]) -> None:
        """Handle successful queue parameters fetch."""
        self.queue_parameters_loading.emit(False)
        self.queue_parameters_updated.emit(parameters)

    def _on_queue_parameters_error(self, error: BaseException) -> None:
        """Handle queue parameters fetch error."""
        # ResourceNotFoundException is expected when switching profiles - the old farm_id/queue_id
        # may not exist in the new profile. Don't log this as an exception.
        error_name = type(error).__name__
        if "ResourceNotFoundException" in error_name or "AccessDeniedException" in error_name:
            logger.debug(f"Could not fetch queue parameters: {error}")
        else:
            logger.exception("Failed to fetch queue parameters", exc_info=error)
        self.queue_parameters_loading.emit(False)
        self.queue_parameters_updated.emit([])

    # ─────────────────────────────────────────────────────────────
    # Selection Handlers (single source of truth)
    # ─────────────────────────────────────────────────────────────
    #
    # These are the ONLY place farm/queue/storage selections are persisted and
    # cascaded. A user picking a resource in a combo, or the combo auto-selecting
    # a lone resource, routes here. The controller:
    #   1. persists the selection (and clears now-invalid dependents) on the main
    #      thread via the module-level set_setting (global config + disk), and
    #   2. updates its own _current_* ids, which refresh_queues/refresh_storage_
    #      profiles read, so dependent list fetches always use the just-selected
    #      ids rather than a stale per-widget config snapshot.
    # Having a single writer on a single thread is what removes the family of
    # races the previous dual-cascade design had to patch piecemeal.

    def select_farm(self, farm_id: str) -> None:
        """Persist a farm selection and cascade to queues.

        Clears the previously selected queue/storage profile, since they belong to
        the old farm, then refreshes the queue list for the new farm. ``selection_
        changed`` is emitted so the submit dialog can reload queue parameters and
        refresh the Submit button state.

        Args:
            farm_id: The newly selected farm ID (may be "" to clear).
        """
        set_setting("defaults.farm_id", farm_id)
        # The previous queue/storage profile belong to the old farm.
        set_setting("defaults.queue_id", "")
        set_setting("settings.storage_profile_id", "")

        self._current_farm_id = farm_id
        self._current_queue_id = ""

        # Drop dependent data so nothing stale lingers while the queue list reloads.
        self.storage_profiles_updated.emit([])
        self.queue_parameters_updated.emit([])

        self.refresh_queues(farm_id)
        self.selection_changed.emit()

    def select_queue(self, queue_id: str) -> None:
        """Persist a queue selection and cascade to storage profiles + queue params.

        Clears the previously selected storage profile (it is queue-scoped), then
        refreshes the storage-profile list and queue parameters for the new queue.

        Args:
            queue_id: The newly selected queue ID (may be "" to clear).
        """
        set_setting("defaults.queue_id", queue_id)
        # The previous storage profile belongs to the old queue.
        set_setting("settings.storage_profile_id", "")

        self._current_queue_id = queue_id

        self.refresh_storage_profiles()
        self.refresh_queue_parameters()
        self.selection_changed.emit()

    def select_storage_profile(self, storage_profile_id: str) -> None:
        """Persist a storage-profile selection.

        A storage profile has no dependents and does not gate the Submit button or
        queue parameters, so this only persists; no cascade or notification.

        Args:
            storage_profile_id: The newly selected storage profile ID (may be "").
        """
        set_setting("settings.storage_profile_id", storage_profile_id or "")

    def sync_selection_state(self, config: Optional[ConfigParser] = None) -> None:
        """Align the controller's cascade ids with the persisted config.

        Called on a plain refresh (dialog open, profile switch, sign-in) where the
        selection wasn't made through select_*; keeps refresh_queues/refresh_storage_
        profiles reading the correct ids without a redundant per-widget snapshot.
        """
        self._current_farm_id = config_file.get_setting("defaults.farm_id", config=config)
        self._current_queue_id = config_file.get_setting("defaults.queue_id", config=config)

    # ─────────────────────────────────────────────────────────────
    # Lifecycle
    # ─────────────────────────────────────────────────────────────

    def shutdown(self) -> None:
        """
        Shutdown the controller and cancel pending operations.

        Call this when closing dialogs or shutting down the application.
        """
        self._task_runner.cancel_all()

    def cancel_all_operations(self) -> None:
        """Cancel all pending async operations."""
        self._task_runner.cancel_all()
