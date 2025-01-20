# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest

try:
    from deadline.client.ui.widgets.shared_job_settings_tab import SharedJobSettingsWidget
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

    shared_job_settings_tab.shared_job_properties_box.priority_box.setValue(100)
    assert shared_job_settings_tab.shared_job_properties_box.priority_box.value() == 99


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
