# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Central controller for Deadline Cloud UI operations.

This controller manages all async API operations, ensuring proper
ordering of dependent calls and preventing race conditions.
"""

from configparser import ConfigParser
from logging import getLogger
from typing import List, Optional, Tuple
import sys

from qtpy.QtCore import QObject, Qt, Signal

from ... import api
from ...job_bundle.parameters import JobParameter
from ._async_runner import AsyncTaskRunner


logger = getLogger(__name__)


# Type alias for resource lists: [(display_name, id), ...]
ResourceList = List[Tuple[str, str]]


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
        farms_updated: Emitted when farm list is updated. Args: [(name, farm_id), ...]
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
    queues_updated = Signal(list)
    storage_profiles_updated = Signal(list)
    queue_parameters_updated = Signal(list)

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

    # ─────────────────────────────────────────────────────────────
    # Farm Operations
    # ─────────────────────────────────────────────────────────────

    def refresh_farms(self) -> None:
        """Fetch the list of farms asynchronously."""
        self.farms_loading.emit(True)
        self._task_runner.run(
            operation_key="list_farms",
            fn=self._fetch_farms,
            on_success=self._on_farms_success,
            on_error=self._on_farms_error,
        )

    def _fetch_farms(self) -> ResourceList:
        """Fetch farms from API. Runs in background thread."""
        response = api.list_farms(config=self._config)
        return sorted(
            [(item["displayName"], item["farmId"]) for item in response["farms"]],
            key=lambda item: (item[0].casefold(), item[1]),
        )

    def _on_farms_success(self, farms: ResourceList) -> None:
        """Handle successful farm list fetch."""
        self.farms_loading.emit(False)
        self.farms_updated.emit(farms)

    def _on_farms_error(self, error: BaseException) -> None:
        """Handle farm list fetch error."""
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

        self.queues_loading.emit(True)
        self._task_runner.run(
            operation_key="list_queues",
            fn=self._fetch_queues,
            on_success=self._on_queues_success,
            on_error=self._on_queues_error,
            farm_id=farm_id,
        )

    def _fetch_queues(self, farm_id: str) -> ResourceList:
        """Fetch queues from API. Runs in background thread."""
        response = api.list_queues(config=self._config, farmId=farm_id)
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

        self.storage_profiles_loading.emit(True)
        self._task_runner.run(
            operation_key="list_storage_profiles",
            fn=self._fetch_storage_profiles,
            on_success=self._on_storage_profiles_success,
            on_error=self._on_storage_profiles_error,
            farm_id=farm_id,
            queue_id=queue_id,
        )

    def _fetch_storage_profiles(self, farm_id: str, queue_id: str) -> ResourceList:
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
            config=self._config, farmId=farm_id, queueId=queue_id
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
    # Cascading Selection Handlers
    # ─────────────────────────────────────────────────────────────

    def on_farm_selected(self, farm_id: str) -> None:
        """
        Handle farm selection change.

        Triggers cascading refresh of dependent resources:
        farm -> queues -> storage profiles, queue parameters

        Args:
            farm_id: The newly selected farm ID
        """
        if farm_id == self._current_farm_id:
            return

        self._current_farm_id = farm_id
        self._current_queue_id = ""

        # Clear dependent data
        self.storage_profiles_updated.emit([])
        self.queue_parameters_updated.emit([])

        # Refresh queues for new farm
        self.refresh_queues(farm_id)

    def on_queue_selected(self, queue_id: str) -> None:
        """
        Handle queue selection change.

        Triggers refresh of queue-dependent resources.

        Args:
            queue_id: The newly selected queue ID
        """
        if queue_id == self._current_queue_id:
            return

        self._current_queue_id = queue_id

        # Refresh dependent data
        self.refresh_storage_profiles()
        self.refresh_queue_parameters()

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
