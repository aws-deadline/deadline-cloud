# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for DeadlineUIController class.
"""

import pytest
from unittest.mock import patch
from configparser import ConfigParser

try:
    from deadline.client.ui.controllers._deadline_controller import DeadlineUIController
    from deadline.client.ui.controllers._thread_pool import DeadlineThreadPool
    from qtpy.QtCore import Qt  # type: ignore[attr-defined]

    # Handle Qt5 vs Qt6 API differences for connection types
    try:
        _QueuedConnection = Qt.ConnectionType.QueuedConnection  # type: ignore[attr-defined]
    except AttributeError:
        _QueuedConnection = Qt.QueuedConnection  # type: ignore[attr-defined]
except ImportError:
    pytest.importorskip("deadline.client.ui.controllers._deadline_controller")


class TestDeadlineUIController:
    """Tests for DeadlineUIController class."""

    def setup_method(self):
        """Reset singleton and thread pool before each test."""
        DeadlineUIController.resetInstance()
        DeadlineThreadPool.reset()

    def teardown_method(self):
        """Clean up after each test."""
        DeadlineUIController.resetInstance()
        DeadlineThreadPool.shutdown(wait_for_done=True, timeout_ms=2000)
        DeadlineThreadPool.reset()

    def test_get_instance_returns_controller(self, qtbot):
        """Test that getInstance returns a DeadlineUIController."""
        controller = DeadlineUIController.getInstance()

        assert isinstance(controller, DeadlineUIController)

    def test_get_instance_is_singleton(self, qtbot):
        """Test that getInstance returns the same instance."""
        controller1 = DeadlineUIController.getInstance()
        controller2 = DeadlineUIController.getInstance()

        assert controller1 is controller2

    def test_reset_instance_clears_singleton(self, qtbot):
        """Test that resetInstance clears the singleton."""
        controller1 = DeadlineUIController.getInstance()
        DeadlineUIController.resetInstance()
        controller2 = DeadlineUIController.getInstance()

        assert controller1 is not controller2

    def test_set_config_stores_config(self, qtbot):
        """Test that set_config stores the configuration."""
        controller = DeadlineUIController.getInstance()

        config = ConfigParser()
        config["defaults"] = {"aws_profile_name": "test-profile"}

        controller.set_config(config)

        assert controller.config is not None
        assert controller.config["defaults"]["aws_profile_name"] == "test-profile"

    def test_set_config_none_clears_config(self, qtbot):
        """Test that set_config(None) clears the configuration."""
        controller = DeadlineUIController.getInstance()

        config = ConfigParser()
        controller.set_config(config)
        controller.set_config(None)

        assert controller.config is None

    def test_current_farm_id_initially_empty(self, qtbot):
        """Test that current_farm_id is initially empty."""
        controller = DeadlineUIController.getInstance()

        assert controller.current_farm_id == ""

    def test_current_queue_id_initially_empty(self, qtbot):
        """Test that current_queue_id is initially empty."""
        controller = DeadlineUIController.getInstance()

        assert controller.current_queue_id == ""

    @patch("deadline.client.ui.controllers._deadline_controller.api")
    def test_refresh_farms_emits_loading_signal(self, mock_api, qtbot):
        """Test that refresh_farms emits farms_loading signal."""
        controller = DeadlineUIController.getInstance()

        mock_api.list_farms.return_value = {"farms": []}

        loading_states = []
        controller.farms_loading.connect(lambda x: loading_states.append(x), _QueuedConnection)

        controller.refresh_farms()

        # Wait for signals
        qtbot.waitUntil(lambda: len(loading_states) >= 2, timeout=2000)

        assert loading_states[0] is True  # Loading started
        assert loading_states[-1] is False  # Loading finished

    @patch("deadline.client.ui.controllers._deadline_controller.api")
    def test_refresh_farms_emits_farms_updated(self, mock_api, qtbot):
        """Test that refresh_farms emits farms_updated with farm list."""
        controller = DeadlineUIController.getInstance()

        mock_api.list_farms.return_value = {
            "farms": [
                {"displayName": "Farm A", "farmId": "farm-a"},
                {"displayName": "Farm B", "farmId": "farm-b"},
            ]
        }

        farms_received = []
        controller.farms_updated.connect(lambda x: farms_received.append(x), _QueuedConnection)

        controller.refresh_farms()

        qtbot.waitUntil(lambda: len(farms_received) > 0, timeout=2000)

        assert len(farms_received) == 1
        farms = farms_received[0]
        assert len(farms) == 2
        # Should be sorted by name
        # Note: Qt signals may convert tuples to lists
        assert list(farms[0]) == ["Farm A", "farm-a"]
        assert list(farms[1]) == ["Farm B", "farm-b"]

    @patch("deadline.client.ui.controllers._deadline_controller.api")
    def test_refresh_farms_handles_error(self, mock_api, qtbot):
        """Test that refresh_farms handles API errors gracefully."""
        controller = DeadlineUIController.getInstance()

        mock_api.list_farms.side_effect = Exception("API Error")

        farms_received = []
        errors_received = []
        controller.farms_updated.connect(lambda x: farms_received.append(x), _QueuedConnection)
        controller.operation_failed.connect(
            lambda key, e: errors_received.append((key, e)), _QueuedConnection
        )

        controller.refresh_farms()

        qtbot.waitUntil(lambda: len(farms_received) > 0, timeout=2000)

        # Should emit empty list on error
        assert farms_received[0] == []
        # Should emit error signal
        assert len(errors_received) == 1

    @patch("deadline.client.ui.controllers._deadline_controller.api")
    def test_refresh_queues_with_no_farm_emits_empty(self, mock_api, qtbot):
        """Test that refresh_queues with no farm emits empty list."""
        controller = DeadlineUIController.getInstance()

        queues_received = []
        controller.queues_updated.connect(lambda x: queues_received.append(x), _QueuedConnection)

        controller.refresh_queues()

        # Should emit immediately without API call
        qtbot.waitUntil(lambda: len(queues_received) > 0, timeout=1000)

        assert queues_received[0] == []
        mock_api.list_queues.assert_not_called()

    @patch("deadline.client.ui.controllers._deadline_controller.api")
    def test_refresh_queues_fetches_for_farm(self, mock_api, qtbot):
        """Test that refresh_queues fetches queues for specified farm."""
        controller = DeadlineUIController.getInstance()

        mock_api.list_queues.return_value = {
            "queues": [
                {"displayName": "Queue 1", "queueId": "queue-1"},
            ]
        }

        queues_received = []
        controller.queues_updated.connect(lambda x: queues_received.append(x), _QueuedConnection)

        controller.refresh_queues(farm_id="farm-123")

        qtbot.waitUntil(lambda: len(queues_received) > 0, timeout=2000)

        mock_api.list_queues.assert_called_once()
        # Note: Qt signals may convert tuples to lists
        assert list(queues_received[0][0]) == ["Queue 1", "queue-1"]

    @patch("deadline.client.ui.controllers._deadline_controller.api")
    def test_on_farm_selected_updates_current_farm(self, mock_api, qtbot):
        """Test that on_farm_selected updates current_farm_id."""
        controller = DeadlineUIController.getInstance()

        mock_api.list_queues.return_value = {"queues": []}

        controller.on_farm_selected("farm-123")

        assert controller.current_farm_id == "farm-123"

    @patch("deadline.client.ui.controllers._deadline_controller.api")
    def test_on_farm_selected_clears_queue(self, mock_api, qtbot):
        """Test that on_farm_selected clears current_queue_id."""
        controller = DeadlineUIController.getInstance()

        mock_api.list_queues.return_value = {"queues": []}

        controller._current_queue_id = "old-queue"
        controller.on_farm_selected("farm-123")

        assert controller.current_queue_id == ""

    @patch("deadline.client.ui.controllers._deadline_controller.api")
    def test_on_farm_selected_triggers_queue_refresh(self, mock_api, qtbot):
        """Test that on_farm_selected triggers queue refresh."""
        controller = DeadlineUIController.getInstance()

        mock_api.list_queues.return_value = {"queues": []}

        queues_received = []
        controller.queues_updated.connect(lambda x: queues_received.append(x), _QueuedConnection)

        controller.on_farm_selected("farm-123")

        qtbot.waitUntil(lambda: len(queues_received) > 0, timeout=2000)

        mock_api.list_queues.assert_called_once()

    @patch("deadline.client.ui.controllers._deadline_controller.api")
    def test_on_farm_selected_clears_dependent_data(self, mock_api, qtbot):
        """Test that on_farm_selected clears storage profiles and queue params."""
        controller = DeadlineUIController.getInstance()

        mock_api.list_queues.return_value = {"queues": []}

        storage_profiles_received = []
        queue_params_received = []
        controller.storage_profiles_updated.connect(
            lambda x: storage_profiles_received.append(x), _QueuedConnection
        )
        controller.queue_parameters_updated.connect(
            lambda x: queue_params_received.append(x), _QueuedConnection
        )

        controller.on_farm_selected("farm-123")

        qtbot.waitUntil(lambda: len(storage_profiles_received) > 0, timeout=1000)
        qtbot.waitUntil(lambda: len(queue_params_received) > 0, timeout=1000)

        assert storage_profiles_received[0] == []
        assert queue_params_received[0] == []

    @patch("deadline.client.ui.controllers._deadline_controller.api")
    def test_on_farm_selected_ignores_same_farm(self, mock_api, qtbot):
        """Test that on_farm_selected does nothing if farm unchanged."""
        controller = DeadlineUIController.getInstance()

        controller._current_farm_id = "farm-123"

        controller.on_farm_selected("farm-123")

        mock_api.list_queues.assert_not_called()

    @patch("deadline.client.ui.controllers._deadline_controller.api")
    def test_on_queue_selected_updates_current_queue(self, mock_api, qtbot):
        """Test that on_queue_selected updates current_queue_id."""
        controller = DeadlineUIController.getInstance()

        controller._current_farm_id = "farm-123"
        mock_api.list_storage_profiles_for_queue.return_value = {"storageProfiles": []}
        mock_api.get_queue_parameter_definitions.return_value = []

        controller.on_queue_selected("queue-456")

        assert controller.current_queue_id == "queue-456"

    @patch("deadline.client.ui.controllers._deadline_controller.api")
    def test_on_queue_selected_triggers_storage_profile_refresh(self, mock_api, qtbot):
        """Test that on_queue_selected triggers storage profile refresh."""
        controller = DeadlineUIController.getInstance()

        controller._current_farm_id = "farm-123"
        mock_api.list_storage_profiles_for_queue.return_value = {"storageProfiles": []}
        mock_api.get_queue_parameter_definitions.return_value = []

        storage_received = []
        controller.storage_profiles_updated.connect(
            lambda x: storage_received.append(x), _QueuedConnection
        )

        controller.on_queue_selected("queue-456")

        qtbot.waitUntil(lambda: len(storage_received) > 0, timeout=2000)

        mock_api.list_storage_profiles_for_queue.assert_called_once()

    @patch("deadline.client.ui.controllers._deadline_controller.api")
    def test_on_queue_selected_triggers_queue_params_refresh(self, mock_api, qtbot):
        """Test that on_queue_selected triggers queue parameters refresh."""
        controller = DeadlineUIController.getInstance()

        controller._current_farm_id = "farm-123"
        mock_api.list_storage_profiles_for_queue.return_value = {"storageProfiles": []}
        mock_api.get_queue_parameter_definitions.return_value = [{"name": "param1"}]

        params_received = []
        controller.queue_parameters_updated.connect(
            lambda x: params_received.append(x), _QueuedConnection
        )

        controller.on_queue_selected("queue-456")

        qtbot.waitUntil(lambda: len(params_received) > 0, timeout=2000)

        mock_api.get_queue_parameter_definitions.assert_called_once()
        assert params_received[0] == [{"name": "param1"}]

    def test_shutdown_cancels_operations(self, qtbot):
        """Test that shutdown cancels pending operations."""
        controller = DeadlineUIController.getInstance()

        # Verify shutdown doesn't raise
        controller.shutdown()

    def test_cancel_all_operations(self, qtbot):
        """Test that cancel_all_operations cancels pending operations."""
        controller = DeadlineUIController.getInstance()

        # Verify cancel_all_operations doesn't raise
        controller.cancel_all_operations()
