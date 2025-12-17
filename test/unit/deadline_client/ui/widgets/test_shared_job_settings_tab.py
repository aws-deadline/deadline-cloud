# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
from unittest.mock import patch, MagicMock

try:
    from deadline.client.ui.widgets.shared_job_settings_tab import (
        SharedJobSettingsWidget,
        DeadlineCloudSettingsWidget,
    )
    from deadline.client.ui.dataclasses import JobBundleSettings
except ImportError:
    # The tests in this file should be skipped if Qt UI related modules cannot be loaded
    pytest.importorskip("deadline.client.ui.widgets.shared_job_settings_tab")


@pytest.fixture(scope="function")
def shared_job_settings_tab(qtbot, temp_job_bundle_dir) -> SharedJobSettingsWidget:
    initial_settings = JobBundleSettings(input_job_bundle_dir=temp_job_bundle_dir, name="test-name")
    widget = SharedJobSettingsWidget(
        initial_settings=initial_settings,
        initial_shared_parameter_values=dict(),
    )
    qtbot.addWidget(widget)
    return widget


def test_name_should_be_truncated_to_openjd_spec_128_chars(
    shared_job_settings_tab: SharedJobSettingsWidget,
):
    expected_max_job_name_length = 128
    invalid_str = "a" * (expected_max_job_name_length + 1)
    shared_job_settings_tab.shared_job_properties_box.sub_name_edit.setText(invalid_str)
    assert (
        shared_job_settings_tab.shared_job_properties_box.sub_name_edit.text()
        == invalid_str[:expected_max_job_name_length]
    )


def test_description_should_be_truncated_to_openjd_spec_2048_chars(
    shared_job_settings_tab: SharedJobSettingsWidget,
):
    expected_max_job_description_length = 2048
    invalid_str = "a" * (expected_max_job_description_length + 1)
    shared_job_settings_tab.shared_job_properties_box.desc_edit.setText(invalid_str)
    assert (
        shared_job_settings_tab.shared_job_properties_box.desc_edit.text()
        == invalid_str[:expected_max_job_description_length]
    )


def test_priority_should_be_integer_within_range(shared_job_settings_tab: SharedJobSettingsWidget):
    shared_job_settings_tab.shared_job_properties_box.priority_box.setValue(-1)
    assert shared_job_settings_tab.shared_job_properties_box.priority_box.value() == 0

    shared_job_settings_tab.shared_job_properties_box.priority_box.setValue(101)
    assert shared_job_settings_tab.shared_job_properties_box.priority_box.value() == 100


def test_initial_state_should_be_allowed_enums(shared_job_settings_tab: SharedJobSettingsWidget):
    shared_job_settings_tab.shared_job_properties_box.initial_status_box.setCurrentText("Invalid")
    assert (
        shared_job_settings_tab.shared_job_properties_box.initial_status_box.currentText()
        == "READY"
    )


def test_max_failed_tasks_count_should_be_integer_within_range(
    shared_job_settings_tab: SharedJobSettingsWidget,
):
    shared_job_settings_tab.shared_job_properties_box.max_failed_tasks_count_box.setValue(-1)
    assert shared_job_settings_tab.shared_job_properties_box.max_failed_tasks_count_box.value() == 0


def test_max_retries_per_task_should_be_integer_within_range(
    shared_job_settings_tab: SharedJobSettingsWidget,
):
    shared_job_settings_tab.shared_job_properties_box.max_retries_per_task_box.setValue(-1)
    assert shared_job_settings_tab.shared_job_properties_box.max_retries_per_task_box.value() == 0


def test_max_worker_count_should_be_integer_within_range(
    shared_job_settings_tab: SharedJobSettingsWidget,
):
    shared_job_settings_tab.shared_job_properties_box.max_worker_count_box.setValue(-1)
    assert shared_job_settings_tab.shared_job_properties_box.max_worker_count_box.value() == 1


# Tests for DeadlineCloudSettingsWidget farm/queue combo boxes


@pytest.fixture(scope="function")
def deadline_cloud_settings_widget(qtbot) -> DeadlineCloudSettingsWidget:
    """Fixture for DeadlineCloudSettingsWidget with mocked combo boxes."""
    with patch(
        "deadline.client.ui.widgets.shared_job_settings_tab.config_file.read_config"
    ) as mock_config:
        mock_config.return_value = MagicMock()
        widget = DeadlineCloudSettingsWidget()
        qtbot.addWidget(widget)
        return widget


def test_farm_selection_updates_config(deadline_cloud_settings_widget: DeadlineCloudSettingsWidget):
    """Test that selecting a farm updates the config setting."""
    widget = deadline_cloud_settings_widget
    test_farm_id = "farm-test123"

    # Add a test item to the farm combo box
    widget.farm_box.box.addItem("Test Farm", test_farm_id)

    with patch(
        "deadline.client.ui.widgets.shared_job_settings_tab.set_setting"
    ) as mock_set_setting:
        # Select the farm (triggers _on_farm_changed)
        widget.farm_box.box.setCurrentIndex(widget.farm_box.box.count() - 1)

        # Verify config was updated with the farm ID
        mock_set_setting.assert_called_with("defaults.farm_id", test_farm_id)


def test_queue_selection_updates_config(
    deadline_cloud_settings_widget: DeadlineCloudSettingsWidget,
):
    """Test that selecting a queue updates the config setting."""
    widget = deadline_cloud_settings_widget
    test_queue_id = "queue-test456"

    # Add a test item to the queue combo box
    widget.queue_box.box.addItem("Test Queue", test_queue_id)

    with patch(
        "deadline.client.ui.widgets.shared_job_settings_tab.set_setting"
    ) as mock_set_setting:
        # Select the queue (triggers _on_queue_changed)
        widget.queue_box.box.setCurrentIndex(widget.queue_box.box.count() - 1)

        # Verify config was updated with the queue ID
        mock_set_setting.assert_called_with("defaults.queue_id", test_queue_id)


def test_farm_change_refreshes_queue_list(
    deadline_cloud_settings_widget: DeadlineCloudSettingsWidget,
):
    """Test that changing farm triggers queue list refresh."""
    widget = deadline_cloud_settings_widget
    test_farm_id = "farm-test789"

    # Add a test item to the farm combo box
    widget.farm_box.box.addItem("Test Farm", test_farm_id)

    with patch.object(widget.queue_box, "refresh_list") as mock_refresh:
        with patch("deadline.client.ui.widgets.shared_job_settings_tab.set_setting"):
            # Select the farm
            widget.farm_box.box.setCurrentIndex(widget.farm_box.box.count() - 1)

            # Verify queue list was refreshed
            mock_refresh.assert_called_once()


def test_refresh_setting_controls_updates_combo_boxes(
    deadline_cloud_settings_widget: DeadlineCloudSettingsWidget,
):
    """Test that refresh_setting_controls updates both combo boxes."""
    widget = deadline_cloud_settings_widget

    with patch.object(widget.farm_box, "set_config") as mock_farm_config, patch.object(
        widget.queue_box, "set_config"
    ) as mock_queue_config, patch.object(
        widget.farm_box, "refresh_selected_id"
    ) as mock_farm_refresh, patch.object(
        widget.queue_box, "refresh_selected_id"
    ) as mock_queue_refresh, patch.object(
        widget.farm_box, "refresh_list"
    ) as mock_farm_list, patch.object(widget.queue_box, "refresh_list") as mock_queue_list, patch(
        "deadline.client.ui.widgets.shared_job_settings_tab.config_file.read_config"
    ):
        # Call refresh with authorized=True
        widget.refresh_setting_controls(deadline_authorized=True)

        # Verify all methods were called
        mock_farm_config.assert_called_once()
        mock_queue_config.assert_called_once()
        mock_farm_refresh.assert_called_once()
        mock_queue_refresh.assert_called_once()
        mock_farm_list.assert_called_once()
        mock_queue_list.assert_called_once()


def test_refresh_setting_controls_skips_list_refresh_when_unauthorized(
    deadline_cloud_settings_widget: DeadlineCloudSettingsWidget,
):
    """Test that refresh_setting_controls skips list refresh when not authorized."""
    widget = deadline_cloud_settings_widget

    with patch.object(widget.farm_box, "set_config"), patch.object(
        widget.queue_box, "set_config"
    ), patch.object(widget.farm_box, "refresh_selected_id"), patch.object(
        widget.queue_box, "refresh_selected_id"
    ), patch.object(widget.farm_box, "refresh_list") as mock_farm_list, patch.object(
        widget.queue_box, "refresh_list"
    ) as mock_queue_list, patch(
        "deadline.client.ui.widgets.shared_job_settings_tab.config_file.read_config"
    ):
        # Call refresh with authorized=False
        widget.refresh_setting_controls(deadline_authorized=False)

        # Verify list refresh was NOT called
        mock_farm_list.assert_not_called()
        mock_queue_list.assert_not_called()


def test_storage_profile_hidden_by_default(
    deadline_cloud_settings_widget: DeadlineCloudSettingsWidget,
):
    """Test that storage profile selector is hidden by default."""
    widget = deadline_cloud_settings_widget

    # Storage profile should be hidden initially
    assert not widget.storage_profile_box.isVisible()
    assert not widget.storage_profile_box_label.isVisible()


def test_storage_profile_shown_when_profiles_available(
    deadline_cloud_settings_widget: DeadlineCloudSettingsWidget,
):
    """Test that storage profile selector is shown when profiles are available."""
    widget = deadline_cloud_settings_widget

    # Add a real storage profile item
    widget.storage_profile_box.box.addItem("Test Profile", "sp-test123")

    # Storage profile should now be visible
    assert widget.storage_profile_box.isVisible()
    assert widget.storage_profile_box_label.isVisible()


def test_storage_profile_shown_when_none_selected_sorts_first(
    deadline_cloud_settings_widget: DeadlineCloudSettingsWidget,
):
    """Test that storage profile is visible even when <none selected> sorts to first position.

    This tests the _list_update signal path which is used during async refresh.
    The _list_update signal handler populates the combo box with block_signals,
    so model signals don't fire - we need to explicitly trigger visibility update.
    """
    widget = deadline_cloud_settings_widget

    # Simulate the async refresh completing with sorted profiles where
    # "<none selected>" sorts first because '<' comes before letters alphabetically
    items_list = [
        ("<none selected>", ""),
        ("Profile A", "sp-profile-a"),
        ("Profile B", "sp-profile-b"),
    ]

    # Emit the _list_update signal to simulate async refresh completing
    # This is the code path that was broken - the combo box is populated
    # inside block_signals so model signals don't fire
    widget.storage_profile_box._list_update.emit(1, items_list)

    # Storage profile should be visible because there are real profiles
    assert widget.storage_profile_box.isVisible()
    assert widget.storage_profile_box_label.isVisible()


def test_storage_profile_selection_updates_config(
    deadline_cloud_settings_widget: DeadlineCloudSettingsWidget,
):
    """Test that selecting a storage profile updates the config setting."""
    widget = deadline_cloud_settings_widget
    test_profile_id = "sp-test123"

    # Add a test item to the storage profile combo box
    widget.storage_profile_box.box.addItem("Test Profile", test_profile_id)

    with patch(
        "deadline.client.ui.widgets.shared_job_settings_tab.set_setting"
    ) as mock_set_setting:
        # Select the storage profile (triggers _on_storage_profile_changed)
        widget.storage_profile_box.box.setCurrentIndex(widget.storage_profile_box.box.count() - 1)

        # Verify config was updated with the storage profile ID
        mock_set_setting.assert_called_with("settings.storage_profile_id", test_profile_id)


def test_queue_change_refreshes_storage_profile_list(
    deadline_cloud_settings_widget: DeadlineCloudSettingsWidget,
):
    """Test that changing queue triggers storage profile list refresh."""
    widget = deadline_cloud_settings_widget
    test_queue_id = "queue-test789"

    # Add a test item to the queue combo box
    widget.queue_box.box.addItem("Test Queue", test_queue_id)

    with patch.object(widget.storage_profile_box, "refresh_list") as mock_refresh:
        with patch("deadline.client.ui.widgets.shared_job_settings_tab.set_setting"):
            # Select the queue
            widget.queue_box.box.setCurrentIndex(widget.queue_box.box.count() - 1)

            # Verify storage profile list was refreshed
            mock_refresh.assert_called_once()


# Tests for SharedJobSettingsWidget.refresh_queue_parameters


def test_refresh_queue_parameters_triggers_on_farm_change(
    shared_job_settings_tab: SharedJobSettingsWidget,
):
    widget = shared_job_settings_tab

    # Set initial farm and queue IDs
    widget.farm_id = "farm-initial"
    widget.queue_id = "queue-initial"

    with patch(
        "deadline.client.ui.widgets.shared_job_settings_tab.get_setting"
    ) as mock_get_setting:
        # Simulate farm change (different farm_id, same queue_id)
        mock_get_setting.side_effect = lambda key: {
            "defaults.farm_id": "farm-new",
            "defaults.queue_id": "queue-initial",
        }.get(key)

        with patch.object(widget.queue_parameters_box, "rebuild_ui") as mock_rebuild, patch.object(
            widget, "_start_load_queue_parameters_thread"
        ) as mock_start_thread:
            widget.refresh_queue_parameters()

            mock_rebuild.assert_called_once_with(
                async_loading_state="Reloading Queue Environments..."
            )
            mock_start_thread.assert_called_once()


def test_refresh_queue_parameters_triggers_on_queue_change(
    shared_job_settings_tab: SharedJobSettingsWidget,
):
    widget = shared_job_settings_tab

    # Set initial farm and queue IDs
    widget.farm_id = "farm-initial"
    widget.queue_id = "queue-initial"

    with patch(
        "deadline.client.ui.widgets.shared_job_settings_tab.get_setting"
    ) as mock_get_setting:
        # Simulate queue change (same farm_id, different queue_id)
        mock_get_setting.side_effect = lambda key: {
            "defaults.farm_id": "farm-initial",
            "defaults.queue_id": "queue-new",
        }.get(key)

        with patch.object(widget.queue_parameters_box, "rebuild_ui") as mock_rebuild, patch.object(
            widget, "_start_load_queue_parameters_thread"
        ) as mock_start_thread:
            widget.refresh_queue_parameters()

            mock_rebuild.assert_called_once_with(
                async_loading_state="Reloading Queue Environments..."
            )
            mock_start_thread.assert_called_once()


def test_refresh_queue_parameters_no_refresh_when_unchanged(
    shared_job_settings_tab: SharedJobSettingsWidget,
):
    widget = shared_job_settings_tab

    # Set initial farm and queue IDs
    widget.farm_id = "farm-same"
    widget.queue_id = "queue-same"

    # Clear the async loading state so it doesn't trigger refresh
    widget.queue_parameters_box.async_loading_state = ""

    with patch(
        "deadline.client.ui.widgets.shared_job_settings_tab.get_setting"
    ) as mock_get_setting:
        # Same farm and queue IDs
        mock_get_setting.side_effect = lambda key: {
            "defaults.farm_id": "farm-same",
            "defaults.queue_id": "queue-same",
        }.get(key)

        with patch.object(widget.queue_parameters_box, "rebuild_ui") as mock_rebuild, patch.object(
            widget, "_start_load_queue_parameters_thread"
        ) as mock_start_thread:
            widget.refresh_queue_parameters()

            mock_rebuild.assert_not_called()
            mock_start_thread.assert_not_called()
