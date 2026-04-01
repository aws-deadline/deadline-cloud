# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the controller-based Deadline resource list combo boxes.
"""

import pytest
from unittest.mock import patch
from configparser import ConfigParser

pytest.importorskip("deadline.client.ui.widgets._deadline_list_combo_boxes")

from deadline.client.ui.widgets._deadline_list_combo_boxes import (  # noqa: E402
    DeadlineFarmListComboBoxController,
    DeadlineQueueListComboBoxController,
    DeadlineStorageProfileListComboBoxController,
)
from deadline.client.ui.controllers._deadline_controller import DeadlineUIController  # noqa: E402
from deadline.client.ui.controllers._thread_pool import DeadlineThreadPool  # noqa: E402


class TestDeadlineFarmListComboBoxController:
    """Tests for DeadlineFarmListComboBoxController."""

    def setup_method(self):
        """Reset singleton and thread pool before each test."""
        DeadlineUIController.resetInstance()
        DeadlineThreadPool.reset()

    def teardown_method(self):
        """Clean up after each test."""
        DeadlineUIController.resetInstance()
        DeadlineThreadPool.shutdown(wait_for_done=True, timeout_ms=2000)
        DeadlineThreadPool.reset()

    def test_init_creates_widget(self, qtbot):
        """Test that the widget can be instantiated."""
        widget = DeadlineFarmListComboBoxController()
        qtbot.addWidget(widget)

        assert widget.box is not None
        assert widget.refresh_button is not None

    def test_set_config_updates_controller(self, qtbot):
        """Test that set_config updates the controller."""
        widget = DeadlineFarmListComboBoxController()
        qtbot.addWidget(widget)

        config = ConfigParser()
        config["defaults"] = {"aws_profile_name": "test-profile"}

        widget.set_config(config)

        assert widget.config is config

    def test_clear_list_empties_combobox(self, qtbot):
        """Test that clear_list empties the combobox."""
        widget = DeadlineFarmListComboBoxController()
        qtbot.addWidget(widget)

        # Add some items first
        widget.box.addItem("Farm 1", userData="farm-1")
        widget.box.addItem("Farm 2", userData="farm-2")
        assert widget.count() == 2

        widget.clear_list()

        assert widget.count() == 0

    def test_handle_list_update_populates_combobox(self, qtbot):
        """Test that _handle_list_update populates the combobox."""
        widget = DeadlineFarmListComboBoxController()
        qtbot.addWidget(widget)

        config = ConfigParser()
        config["defaults"] = {"farm_id": "farm-a"}  # Match one of the items
        widget.set_config(config)

        # Simulate list update from controller
        items = [["Farm A", "farm-a"], ["Farm B", "farm-b"]]

        with patch("deadline.client.ui.widgets._deadline_list_combo_boxes.config_file") as mock_cf:
            mock_cf.get_setting.return_value = "farm-a"
            widget._handle_list_update(items)

        assert widget.count() == 2
        assert widget.box.itemText(0) == "Farm A"
        assert widget.box.itemData(0) == "farm-a"
        assert widget.box.itemText(1) == "Farm B"
        assert widget.box.itemData(1) == "farm-b"

    def test_handle_loading_state_shows_refreshing(self, qtbot):
        """Test that _handle_loading_state shows refreshing indicator."""
        widget = DeadlineFarmListComboBoxController()
        qtbot.addWidget(widget)

        config = ConfigParser()
        config["defaults"] = {"farm_id": "farm-123"}
        widget.set_config(config)

        with patch("deadline.client.ui.widgets._deadline_list_combo_boxes.config_file") as mock_cf:
            mock_cf.get_setting.return_value = "farm-123"
            widget._handle_loading_state(True)

        assert widget.count() == 1
        assert widget.box.itemText(0) == "<refreshing>"
        assert widget.box.itemData(0) == "farm-123"
        assert widget.refresh_button.isEnabled() is False

    def test_handle_loading_state_enables_button_when_done(self, qtbot):
        """Test that _handle_loading_state enables button when loading completes."""
        widget = DeadlineFarmListComboBoxController()
        qtbot.addWidget(widget)

        widget._handle_loading_state(True)
        assert not widget.refresh_button.isEnabled()

        widget._handle_loading_state(False)
        assert widget.refresh_button.isEnabled()

    @patch.object(DeadlineUIController, "refresh_farms")
    def test_refresh_list_calls_controller(self, mock_refresh, qtbot):
        """Test that refresh_list calls the controller."""
        widget = DeadlineFarmListComboBoxController()
        qtbot.addWidget(widget)

        widget.refresh_list()

        mock_refresh.assert_called_once()

    def test_refresh_selected_id_selects_configured_farm(self, qtbot):
        """Test that refresh_selected_id selects the configured farm."""
        widget = DeadlineFarmListComboBoxController()
        qtbot.addWidget(widget)

        config = ConfigParser()
        config["defaults"] = {"farm_id": "farm-b"}
        widget.set_config(config)

        # Add items
        widget.box.addItem("Farm A", userData="farm-a")
        widget.box.addItem("Farm B", userData="farm-b")

        with patch("deadline.client.ui.widgets._deadline_list_combo_boxes.config_file") as mock_cf:
            mock_cf.get_setting.return_value = "farm-b"
            widget.refresh_selected_id()

        assert widget.box.currentData() == "farm-b"

    def test_refresh_selected_id_adds_none_selected_if_not_found(self, qtbot):
        """Test that refresh_selected_id adds none selected if ID not found."""
        widget = DeadlineFarmListComboBoxController()
        qtbot.addWidget(widget)

        config = ConfigParser()
        config["defaults"] = {"farm_id": "unknown-farm"}
        widget.set_config(config)

        # Add items that don't include the configured ID
        widget.box.addItem("Farm A", userData="farm-a")

        widget.refresh_selected_id()

        # Should add "<none selected>" and select it
        assert widget.box.currentText() == "<none selected>"
        assert widget.box.currentData() == ""


class TestDeadlineQueueListComboBoxController:
    """Tests for DeadlineQueueListComboBoxController."""

    def setup_method(self):
        """Reset singleton and thread pool before each test."""
        DeadlineUIController.resetInstance()
        DeadlineThreadPool.reset()

    def teardown_method(self):
        """Clean up after each test."""
        DeadlineUIController.resetInstance()
        DeadlineThreadPool.shutdown(wait_for_done=True, timeout_ms=2000)
        DeadlineThreadPool.reset()

    def test_init_creates_widget(self, qtbot):
        """Test that the widget can be instantiated."""
        widget = DeadlineQueueListComboBoxController()
        qtbot.addWidget(widget)

        assert widget.box is not None
        assert widget.resource_name == "Queue"

    @patch("deadline.client.ui.widgets._deadline_list_combo_boxes.config_file")
    @patch.object(DeadlineUIController, "refresh_queues")
    def test_refresh_list_calls_controller_with_farm_id(self, mock_refresh, mock_cf, qtbot):
        """Test that refresh_list calls controller with farm_id."""
        widget = DeadlineQueueListComboBoxController()
        qtbot.addWidget(widget)

        config = ConfigParser()
        config["defaults"] = {"farm_id": "farm-123", "queue_id": ""}
        widget.set_config(config)

        mock_cf.get_setting.return_value = "farm-123"
        widget.refresh_list()

        mock_refresh.assert_called_once_with(farm_id="farm-123")

    def test_handle_list_update_populates_queues(self, qtbot):
        """Test that _handle_list_update populates queues."""
        widget = DeadlineQueueListComboBoxController()
        qtbot.addWidget(widget)

        config = ConfigParser()
        config["defaults"] = {"queue_id": "queue-1"}
        widget.set_config(config)

        items = [["Queue 1", "queue-1"], ["Queue 2", "queue-2"]]

        with patch("deadline.client.ui.widgets._deadline_list_combo_boxes.config_file") as mock_cf:
            mock_cf.get_setting.return_value = "queue-1"
            widget._handle_list_update(items)

        assert widget.count() == 2
        assert widget.box.itemText(0) == "Queue 1"


class TestDeadlineStorageProfileListComboBoxController:
    """Tests for DeadlineStorageProfileListComboBoxController."""

    def setup_method(self):
        """Reset singleton and thread pool before each test."""
        DeadlineUIController.resetInstance()
        DeadlineThreadPool.reset()

    def teardown_method(self):
        """Clean up after each test."""
        DeadlineUIController.resetInstance()
        DeadlineThreadPool.shutdown(wait_for_done=True, timeout_ms=2000)
        DeadlineThreadPool.reset()

    def test_init_creates_widget(self, qtbot):
        """Test that the widget can be instantiated."""
        widget = DeadlineStorageProfileListComboBoxController()
        qtbot.addWidget(widget)

        assert widget.box is not None
        assert widget.resource_name == "Storage profile"

    @patch("deadline.client.ui.widgets._deadline_list_combo_boxes.config_file")
    @patch.object(DeadlineUIController, "refresh_storage_profiles")
    def test_refresh_list_calls_controller_with_ids(self, mock_refresh, mock_cf, qtbot):
        """Test that refresh_list calls controller with farm and queue IDs."""
        widget = DeadlineStorageProfileListComboBoxController()
        qtbot.addWidget(widget)

        config = ConfigParser()
        config["defaults"] = {"farm_id": "farm-123", "queue_id": "queue-456"}
        config["settings"] = {"storage_profile_id": ""}
        widget.set_config(config)

        # Return different values for different setting names
        def get_setting_side_effect(setting_name, config=None):
            if setting_name == "defaults.farm_id":
                return "farm-123"
            elif setting_name == "defaults.queue_id":
                return "queue-456"
            return ""

        mock_cf.get_setting.side_effect = get_setting_side_effect
        widget.refresh_list()

        mock_refresh.assert_called_once_with(farm_id="farm-123", queue_id="queue-456")

    def test_handle_list_update_populates_profiles(self, qtbot):
        """Test that _handle_list_update populates storage profiles."""
        widget = DeadlineStorageProfileListComboBoxController()
        qtbot.addWidget(widget)

        config = ConfigParser()
        config["settings"] = {"storage_profile_id": "profile-1"}
        widget.set_config(config)

        items = [
            ["<none selected>", ""],
            ["Profile 1", "profile-1"],
            ["Profile 2", "profile-2"],
        ]

        with patch("deadline.client.ui.widgets._deadline_list_combo_boxes.config_file") as mock_cf:
            mock_cf.get_setting.return_value = "profile-1"
            widget._handle_list_update(items)

        assert widget.count() == 3
        assert widget.box.itemText(0) == "<none selected>"
        assert widget.box.itemText(1) == "Profile 1"
