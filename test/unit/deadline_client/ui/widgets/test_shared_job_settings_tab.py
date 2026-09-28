# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import logging
from unittest.mock import patch

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


@pytest.mark.parametrize(
    "override, expected_value, expected_default, expect_precedence_log, reason",
    [
        (
            None,
            "deadline-cloud-v2 deadline-cloud conda-forge",
            "deadline-cloud-v2 deadline-cloud",
            False,
            "no override: the migration prepends v2 to both the value and the default",
        ),
        (
            "my-channel deadline-cloud",
            "my-channel deadline-cloud",
            "deadline-cloud",
            True,
            "explicit override wins; the migrated default is restored so no stale v2 leaks, and "
            "the defeated v2 preference is logged",
        ),
        (
            "deadline-cloud-v2 my-channel",
            "deadline-cloud-v2 my-channel",
            "deadline-cloud",
            False,
            "override already lists v2: it still wins and the default is restored, but there is no "
            "defeated-preference log since v2 is present",
        ),
    ],
)
def test_v2_channel_precedence(
    qtbot,
    temp_job_bundle_dir,
    caplog,
    override,
    expected_value,
    expected_default,
    expect_precedence_log,
    reason,
):
    """With use_deadline_cloud_v2_channel True, the migration prepends v2 to the queue's
    CondaChannels unless an explicit override (bundle, --parameter, or pre-GUI hook, all delivered
    via initial_shared_parameter_values) is present, in which case the override wins and the
    migrated default is restored."""
    initial_settings = JobBundleSettings(input_job_bundle_dir=temp_job_bundle_dir, name="test-name")
    widget = SharedJobSettingsWidget(
        initial_settings=initial_settings,
        initial_shared_parameter_values=(
            {"CondaChannels": override} if override is not None else {}
        ),
        use_deadline_cloud_v2_channel=True,
    )
    qtbot.addWidget(widget)

    queue_parameters = [
        {
            "name": "CondaChannels",
            "default": "deadline-cloud",
            "value": "deadline-cloud conda-forge",
        }
    ]
    with (
        patch.object(widget.queue_parameters_box, "rebuild_ui") as mock_rebuild,
        caplog.at_level(logging.DEBUG, logger="deadline.client.ui.widgets.shared_job_settings_tab"),
    ):
        widget._handle_queue_parameters_update(queue_parameters)

    rebuilt = mock_rebuild.call_args.kwargs["parameter_definitions"]
    conda_channels = next(p for p in rebuilt if p["name"] == "CondaChannels")
    assert conda_channels["value"] == expected_value, reason
    assert conda_channels["default"] == expected_default, reason
    assert ("takes precedence" in caplog.text) is expect_precedence_log, reason


def test_v2_channel_migration_not_applied_when_deactivated(
    shared_job_settings_tab: SharedJobSettingsWidget,
):
    """By default (flag off), the queue's CondaChannels is passed through unchanged."""
    queue_parameters = [{"name": "CondaChannels", "value": "deadline-cloud conda-forge"}]
    with patch.object(shared_job_settings_tab.queue_parameters_box, "rebuild_ui") as mock_rebuild:
        shared_job_settings_tab._handle_queue_parameters_update(queue_parameters)

    rebuilt = mock_rebuild.call_args.kwargs["parameter_definitions"]
    conda_channels = next(p for p in rebuilt if p["name"] == "CondaChannels")
    assert conda_channels["value"] == "deadline-cloud conda-forge"


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
