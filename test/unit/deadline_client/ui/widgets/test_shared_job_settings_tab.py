# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
from typing import Generator
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


MOCK_CONFIG_PATH = "deadline.client.ui.widgets.shared_job_settings_tab.config_file.read_config"
MOCK_SET_SETTING_PATH = "deadline.client.ui.widgets.shared_job_settings_tab.set_setting"
MOCK_COMBO_BOX_GET_SETTING_PATH = (
    "deadline.client.ui.widgets.deadline_cloud_resource_combo_boxes.config_file.get_setting"
)


@pytest.fixture(scope="function")
def deadline_cloud_settings_widget(qtbot) -> Generator[DeadlineCloudSettingsWidget, None, None]:
    """Fixture for DeadlineCloudSettingsWidget with mocked config."""
    with patch(MOCK_CONFIG_PATH) as mock_config, patch(
        MOCK_COMBO_BOX_GET_SETTING_PATH
    ) as mock_get_setting:
        mock_config.return_value = MagicMock()
        mock_get_setting.return_value = ""  # Return empty string for all get_setting calls
        widget = DeadlineCloudSettingsWidget()
        qtbot.addWidget(widget)
        yield widget


def test_farm_selection_updates_config(deadline_cloud_settings_widget: DeadlineCloudSettingsWidget):
    """Test that selecting a farm updates the config setting."""
    widget = deadline_cloud_settings_widget
    test_farm_id = "farm-test123"

    # Add two items so selecting the second one triggers currentIndexChanged
    widget.farm_box.box.addItem("Default Farm", "farm-default")
    widget.farm_box.box.addItem("Test Farm", test_farm_id)

    with patch(MOCK_SET_SETTING_PATH) as mock_set_setting:
        widget.farm_box.box.setCurrentIndex(1)
        mock_set_setting.assert_called_with("defaults.farm_id", test_farm_id)


def test_queue_selection_updates_config(
    deadline_cloud_settings_widget: DeadlineCloudSettingsWidget,
):
    """Test that selecting a queue updates the config setting."""
    widget = deadline_cloud_settings_widget
    test_queue_id = "queue-test456"

    widget.queue_box.box.addItem("Default Queue", "queue-default")
    widget.queue_box.box.addItem("Test Queue", test_queue_id)

    with patch(MOCK_SET_SETTING_PATH) as mock_set_setting:
        widget.queue_box.box.setCurrentIndex(1)
        mock_set_setting.assert_called_with("defaults.queue_id", test_queue_id)


def test_farm_change_refreshes_queue_list(
    deadline_cloud_settings_widget: DeadlineCloudSettingsWidget,
):
    """Test that changing farm triggers queue and storage profile list refresh."""
    widget = deadline_cloud_settings_widget

    widget.farm_box.box.addItem("Default Farm", "farm-default")
    widget.farm_box.box.addItem("Test Farm", "farm-test789")

    with patch.object(widget.queue_box, "refresh_list") as mock_queue_refresh, patch.object(
        widget.storage_profile_box, "refresh_list"
    ) as mock_sp_refresh, patch(MOCK_SET_SETTING_PATH):
        widget.farm_box.box.setCurrentIndex(1)
        mock_queue_refresh.assert_called_once()
        mock_sp_refresh.assert_called_once()


def test_refresh_setting_controls_updates_combo_boxes(
    deadline_cloud_settings_widget: DeadlineCloudSettingsWidget,
):
    """Test that refresh_setting_controls updates all combo boxes."""
    widget = deadline_cloud_settings_widget

    with patch.object(widget.farm_box, "set_config") as mock_farm_config, patch.object(
        widget.queue_box, "set_config"
    ) as mock_queue_config, patch.object(
        widget.storage_profile_box, "set_config"
    ) as mock_sp_config, patch.object(
        widget.farm_box, "refresh_selected_id"
    ) as mock_farm_refresh, patch.object(
        widget.queue_box, "refresh_selected_id"
    ) as mock_queue_refresh, patch.object(
        widget.storage_profile_box, "refresh_selected_id"
    ) as mock_sp_refresh, patch.object(
        widget.farm_box, "refresh_list"
    ) as mock_farm_list, patch.object(
        widget.queue_box, "refresh_list"
    ) as mock_queue_list, patch.object(widget.storage_profile_box, "refresh_list") as mock_sp_list:
        widget.refresh_setting_controls(deadline_authorized=True)

        mock_farm_config.assert_called_once()
        mock_queue_config.assert_called_once()
        mock_sp_config.assert_called_once()
        mock_farm_refresh.assert_called_once()
        mock_queue_refresh.assert_called_once()
        mock_sp_refresh.assert_called_once()
        mock_farm_list.assert_called_once()
        mock_queue_list.assert_called_once()
        mock_sp_list.assert_called_once()


def test_refresh_setting_controls_skips_list_refresh_when_unauthorized(
    deadline_cloud_settings_widget: DeadlineCloudSettingsWidget,
):
    """Test that refresh_setting_controls skips list refresh when not authorized."""
    widget = deadline_cloud_settings_widget

    with patch.object(widget.farm_box, "set_config"), patch.object(
        widget.queue_box, "set_config"
    ), patch.object(widget.storage_profile_box, "set_config"), patch.object(
        widget.farm_box, "refresh_selected_id"
    ), patch.object(widget.queue_box, "refresh_selected_id"), patch.object(
        widget.storage_profile_box, "refresh_selected_id"
    ), patch.object(widget.farm_box, "refresh_list") as mock_farm_list, patch.object(
        widget.queue_box, "refresh_list"
    ) as mock_queue_list, patch.object(widget.storage_profile_box, "refresh_list") as mock_sp_list:
        widget.refresh_setting_controls(deadline_authorized=False)

        mock_farm_list.assert_not_called()
        mock_queue_list.assert_not_called()
        mock_sp_list.assert_not_called()


def test_storage_profile_hidden_by_default(
    deadline_cloud_settings_widget: DeadlineCloudSettingsWidget,
):
    """Test that storage profile selector is hidden by default."""
    widget = deadline_cloud_settings_widget

    assert not widget.storage_profile_box.isVisibleTo(widget)
    assert not widget.storage_profile_box_label.isVisibleTo(widget)


def test_storage_profile_shown_when_profiles_available(
    deadline_cloud_settings_widget: DeadlineCloudSettingsWidget,
):
    """Test that storage profile selector is shown when profiles are available."""
    widget = deadline_cloud_settings_widget

    widget.storage_profile_box.box.addItem("Placeholder", "")
    widget.storage_profile_box.box.addItem("Test Profile", "sp-test123")

    # Use isVisibleTo(parent) since the widget isn't shown in a window during tests
    assert widget.storage_profile_box.isVisibleTo(widget)
    assert widget.storage_profile_box_label.isVisibleTo(widget)


def test_storage_profile_shown_when_none_selected_sorts_first(
    deadline_cloud_settings_widget: DeadlineCloudSettingsWidget,
):
    """Test that storage profile is visible even when <none selected> sorts to first position.

    This tests the _list_update signal path which is used during async refresh.
    The _list_update signal handler populates the combo box with block_signals,
    so model signals don't fire - we need to explicitly trigger visibility update.
    """
    widget = deadline_cloud_settings_widget

    items_list = [
        ("<none selected>", ""),
        ("Profile A", "sp-profile-a"),
        ("Profile B", "sp-profile-b"),
    ]

    # Emit with refresh_id=0 to match the widget's initial __refresh_id
    widget.storage_profile_box._list_update.emit(0, items_list)

    assert widget.storage_profile_box.isVisibleTo(widget)
    assert widget.storage_profile_box_label.isVisibleTo(widget)


def test_storage_profile_selection_updates_config(
    deadline_cloud_settings_widget: DeadlineCloudSettingsWidget,
):
    """Test that selecting a storage profile updates the config setting."""
    widget = deadline_cloud_settings_widget
    test_profile_id = "sp-test123"

    widget.storage_profile_box.box.addItem("Default Profile", "sp-default")
    widget.storage_profile_box.box.addItem("Test Profile", test_profile_id)

    with patch(MOCK_SET_SETTING_PATH) as mock_set_setting:
        widget.storage_profile_box.box.setCurrentIndex(1)
        mock_set_setting.assert_called_with("settings.storage_profile_id", test_profile_id)


def test_queue_change_refreshes_storage_profile_list(
    deadline_cloud_settings_widget: DeadlineCloudSettingsWidget,
):
    """Test that changing queue triggers storage profile list refresh."""
    widget = deadline_cloud_settings_widget

    widget.queue_box.box.addItem("Default Queue", "queue-default")
    widget.queue_box.box.addItem("Test Queue", "queue-test789")

    with patch.object(widget.storage_profile_box, "refresh_list") as mock_refresh, patch(
        MOCK_SET_SETTING_PATH
    ):
        widget.queue_box.box.setCurrentIndex(1)
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


# Tests verifying refactored module locations and public API surface


def test_combo_boxes_importable_from_new_module():
    """Verify combo box classes are importable from their new dedicated module."""
    from deadline.client.ui.widgets.deadline_cloud_resource_combo_boxes import (
        DeadlineFarmListComboBox,
        DeadlineQueueListComboBox,
        DeadlineStorageProfileNameListComboBox,
    )

    assert DeadlineFarmListComboBox is not None
    assert DeadlineQueueListComboBox is not None
    assert DeadlineStorageProfileNameListComboBox is not None


def test_deadline_cloud_settings_widget_importable_from_widgets_package():
    """Verify DeadlineCloudSettingsWidget is importable from the public widgets package."""
    from deadline.client.ui.widgets import DeadlineCloudSettingsWidget as WidgetFromPackage
    from deadline.client.ui.widgets.shared_job_settings_tab import (
        DeadlineCloudSettingsWidget as WidgetFromModule,
    )

    assert WidgetFromPackage is WidgetFromModule


def test_farm_change_propagates_config_to_all_boxes(
    deadline_cloud_settings_widget: DeadlineCloudSettingsWidget,
):
    """Test that changing farm propagates updated config to farm, queue, and storage profile boxes."""
    widget = deadline_cloud_settings_widget

    widget.farm_box.box.addItem("Default Farm", "farm-default")
    widget.farm_box.box.addItem("Test Farm", "farm-test")

    with patch(MOCK_SET_SETTING_PATH), patch.object(
        widget.farm_box, "set_config"
    ) as mock_farm_cfg, patch.object(
        widget.queue_box, "set_config"
    ) as mock_queue_cfg, patch.object(
        widget.storage_profile_box, "set_config"
    ) as mock_sp_cfg, patch.object(widget.queue_box, "refresh_list"), patch.object(
        widget.storage_profile_box, "refresh_list"
    ):
        widget.farm_box.box.setCurrentIndex(1)

        mock_farm_cfg.assert_called_once()
        mock_queue_cfg.assert_called_once()
        mock_sp_cfg.assert_called_once()


def test_queue_change_propagates_config_to_all_boxes(
    deadline_cloud_settings_widget: DeadlineCloudSettingsWidget,
):
    """Test that changing queue propagates updated config to farm, queue, and storage profile boxes."""
    widget = deadline_cloud_settings_widget

    widget.queue_box.box.addItem("Default Queue", "queue-default")
    widget.queue_box.box.addItem("Test Queue", "queue-test")

    with patch(MOCK_SET_SETTING_PATH), patch.object(
        widget.farm_box, "set_config"
    ) as mock_farm_cfg, patch.object(
        widget.queue_box, "set_config"
    ) as mock_queue_cfg, patch.object(
        widget.storage_profile_box, "set_config"
    ) as mock_sp_cfg, patch.object(widget.storage_profile_box, "refresh_list"):
        widget.queue_box.box.setCurrentIndex(1)

        mock_farm_cfg.assert_called_once()
        mock_queue_cfg.assert_called_once()
        mock_sp_cfg.assert_called_once()
