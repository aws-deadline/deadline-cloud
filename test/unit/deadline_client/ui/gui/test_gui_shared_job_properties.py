# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""GUI tests for SharedJobPropertiesWidget using pytest-qt."""

from dataclasses import dataclass
from unittest.mock import patch

import pytest

from deadline.client.ui.widgets.shared_job_settings_tab import SharedJobPropertiesWidget


@dataclass
class MockSettings:
    submitter_name: str = "Test"
    name: str = "Test Job"
    description: str = "A test"
    initial_status: str = "READY"
    max_failed_tasks_count: int = 20
    max_retries_per_task: int = 5
    priority: int = 50
    max_worker_count: int = -1


@dataclass
class MinimalSettings:
    """Settings missing optional attributes to test fallback behavior."""

    name: str = "Minimal Job"
    description: str = "Minimal"


@pytest.fixture(autouse=True)
def mock_config():
    with patch("deadline.client.ui.widgets.shared_job_settings_tab.get_setting") as mock:
        mock.side_effect = lambda name: {
            "settings.max_failed_tasks_count": "20",
            "settings.max_retries_per_task": "5",
        }.get(name, "")
        yield mock


@pytest.fixture
def default_settings():
    return MockSettings()


@pytest.fixture
def widget(qtbot, default_settings):
    w = SharedJobPropertiesWidget(initial_settings=default_settings)
    qtbot.addWidget(w)
    return w


class TestWidgetCreation:
    def test_creates_with_default_settings(self, qtbot, default_settings):
        """Widget creates successfully and populates from initial settings."""
        w = SharedJobPropertiesWidget(initial_settings=default_settings)
        qtbot.addWidget(w)

        assert w.sub_name_edit.text() == "Test Job"
        assert w.desc_edit.text() == "A test"
        assert w.priority_box.value() == 50
        assert w.initial_status_box.currentText() == "READY"
        assert w.max_failed_tasks_count_box.value() == 20
        assert w.max_retries_per_task_box.value() == 5

    def test_name_max_length(self, widget):
        """Name field enforces 128 character max length."""
        assert widget.sub_name_edit.maxLength() == 128

    def test_description_max_length(self, widget):
        """Description field enforces 2048 character max length."""
        assert widget.desc_edit.maxLength() == 2048

    def test_priority_range(self, widget):
        """Priority spinbox has range 0-100."""
        assert widget.priority_box.minimum() == 0
        assert widget.priority_box.maximum() == 100

    def test_max_failed_tasks_range(self, widget):
        """Max failed tasks spinbox has range 0-2147483647."""
        assert widget.max_failed_tasks_count_box.minimum() == 0
        assert widget.max_failed_tasks_count_box.maximum() == 2147483647

    def test_max_retries_range(self, widget):
        """Max retries spinbox has range 0-2147483647."""
        assert widget.max_retries_per_task_box.minimum() == 0
        assert widget.max_retries_per_task_box.maximum() == 2147483647

    def test_max_worker_count_spinbox_range(self, widget):
        """Max worker count spinbox has range 1-2147483647."""
        assert widget.max_worker_count_box.minimum() == 1
        assert widget.max_worker_count_box.maximum() == 2147483647

    def test_initial_status_options(self, widget):
        """Initial status combo has READY and SUSPENDED."""
        items = [
            widget.initial_status_box.itemText(i) for i in range(widget.initial_status_box.count())
        ]
        assert items == ["READY", "SUSPENDED"]


class TestRefreshUI:
    def test_populates_all_fields(self, widget):
        """refresh_ui populates every field from a complete settings object."""
        settings = MockSettings(
            name="New Job",
            description="New description",
            initial_status="SUSPENDED",
            max_failed_tasks_count=10,
            max_retries_per_task=3,
            priority=75,
            max_worker_count=8,
        )
        widget.refresh_ui(settings)

        assert widget.sub_name_edit.text() == "New Job"
        assert widget.desc_edit.text() == "New description"
        assert widget.initial_status_box.currentText() == "SUSPENDED"
        assert widget.max_failed_tasks_count_box.value() == 10
        assert widget.max_retries_per_task_box.value() == 3
        assert widget.priority_box.value() == 75
        assert widget.limited_max_worker_count.isChecked()
        assert widget.max_worker_count_box.value() == 8
        assert not widget.max_worker_count_box.isHidden()

    def test_unlimited_max_worker_count(self, widget):
        """refresh_ui with max_worker_count=-1 selects unlimited radio."""
        settings = MockSettings(max_worker_count=-1)
        widget.refresh_ui(settings)

        assert widget.unlimited_max_worker_count.isChecked()
        assert not widget.limited_max_worker_count.isChecked()
        assert widget.max_worker_count_box.isHidden()

    def test_handles_missing_attributes(self, qtbot):
        """refresh_ui falls back to defaults when settings lack attributes."""
        settings = MinimalSettings()
        w = SharedJobPropertiesWidget(initial_settings=settings)
        qtbot.addWidget(w)

        assert w.sub_name_edit.text() == "Minimal Job"
        assert w.desc_edit.text() == "Minimal"
        # Falls back to defaults
        assert w.initial_status_box.currentText() == "READY"
        assert w.max_failed_tasks_count_box.value() == 20  # from mock get_setting
        assert w.max_retries_per_task_box.value() == 5  # from mock get_setting
        assert w.priority_box.value() == 50  # hardcoded default
        assert w.unlimited_max_worker_count.isChecked()

    def test_refresh_updates_existing_widget(self, widget):
        """refresh_ui updates an already-populated widget to new values."""
        new_settings = MockSettings(
            name="Updated",
            description="Updated desc",
            priority=99,
            initial_status="SUSPENDED",
            max_failed_tasks_count=100,
            max_retries_per_task=10,
            max_worker_count=4,
        )
        widget.refresh_ui(new_settings)

        assert widget.sub_name_edit.text() == "Updated"
        assert widget.desc_edit.text() == "Updated desc"
        assert widget.priority_box.value() == 99
        assert widget.initial_status_box.currentText() == "SUSPENDED"
        assert widget.max_failed_tasks_count_box.value() == 100
        assert widget.max_retries_per_task_box.value() == 10
        assert widget.limited_max_worker_count.isChecked()
        assert widget.max_worker_count_box.value() == 4


class TestGetParameters:
    def test_returns_correct_parameter_list(self, widget):
        """get_parameters returns parameter dicts with correct names and values."""
        params = widget.get_parameters()

        param_names = [p["name"] for p in params]
        assert "deadline:targetTaskRunStatus" in param_names
        assert "deadline:maxFailedTasksCount" in param_names
        assert "deadline:maxRetriesPerTask" in param_names
        assert "deadline:priority" in param_names

    def test_parameter_values_match_widget(self, widget):
        """get_parameters values match current widget state."""
        params = widget.get_parameters()
        param_map = {p["name"]: p["value"] for p in params}

        assert param_map["deadline:targetTaskRunStatus"] == "READY"
        assert param_map["deadline:maxFailedTasksCount"] == 20
        assert param_map["deadline:maxRetriesPerTask"] == 5
        assert param_map["deadline:priority"] == 50

    def test_parameter_types(self, widget):
        """get_parameters includes correct type fields."""
        params = widget.get_parameters()
        param_map = {p["name"]: p for p in params}

        assert param_map["deadline:targetTaskRunStatus"]["type"] == "STRING"
        assert param_map["deadline:maxFailedTasksCount"]["type"] == "INT"
        assert param_map["deadline:maxRetriesPerTask"]["type"] == "INT"
        assert param_map["deadline:priority"]["type"] == "INT"

    def test_unlimited_excludes_max_worker_count(self, widget):
        """When unlimited is selected, maxWorkerCount is NOT in parameters."""
        # Default settings have max_worker_count=-1, so unlimited is selected
        params = widget.get_parameters()
        param_names = [p["name"] for p in params]
        assert "deadline:maxWorkerCount" not in param_names

    def test_limited_includes_max_worker_count(self, widget):
        """When limited is selected, maxWorkerCount IS in parameters."""
        settings = MockSettings(max_worker_count=10)
        widget.refresh_ui(settings)

        params = widget.get_parameters()
        param_map = {p["name"]: p["value"] for p in params}
        assert "deadline:maxWorkerCount" in param_map
        assert param_map["deadline:maxWorkerCount"] == 10


class TestSetParameterValue:
    def test_set_target_task_run_status(self, widget):
        """set_parameter_value sets initial status via deadline:targetTaskRunStatus."""
        widget.set_parameter_value({"name": "deadline:targetTaskRunStatus", "value": "SUSPENDED"})
        assert widget.initial_status_box.currentText() == "SUSPENDED"

    def test_set_max_failed_tasks_count(self, widget):
        """set_parameter_value sets max failed tasks via deadline:maxFailedTasksCount."""
        widget.set_parameter_value({"name": "deadline:maxFailedTasksCount", "value": 42})
        assert widget.max_failed_tasks_count_box.value() == 42

    def test_set_max_retries_per_task(self, widget):
        """set_parameter_value sets max retries via deadline:maxRetriesPerTask."""
        widget.set_parameter_value({"name": "deadline:maxRetriesPerTask", "value": 7})
        assert widget.max_retries_per_task_box.value() == 7

    def test_set_priority(self, widget):
        """set_parameter_value sets priority via deadline:priority."""
        widget.set_parameter_value({"name": "deadline:priority", "value": 80})
        assert widget.priority_box.value() == 80

    def test_set_max_worker_count_unlimited(self, widget):
        """set_parameter_value with -1 selects unlimited worker count."""
        # First set to limited
        widget.set_parameter_value({"name": "deadline:maxWorkerCount", "value": 5})
        assert widget.limited_max_worker_count.isChecked()

        # Now set to unlimited
        widget.set_parameter_value({"name": "deadline:maxWorkerCount", "value": -1})
        assert widget.unlimited_max_worker_count.isChecked()
        assert not widget.limited_max_worker_count.isChecked()
        assert widget.max_worker_count_box.isHidden()

    def test_set_max_worker_count_positive(self, widget):
        """set_parameter_value with positive value selects limited and sets spinbox."""
        widget.set_parameter_value({"name": "deadline:maxWorkerCount", "value": 12})
        assert widget.limited_max_worker_count.isChecked()
        assert not widget.unlimited_max_worker_count.isChecked()
        assert not widget.max_worker_count_box.isHidden()
        assert widget.max_worker_count_box.value() == 12

    def test_unknown_parameter_raises_key_error(self, widget):
        """set_parameter_value raises KeyError for an unknown parameter name."""
        with pytest.raises(KeyError):
            widget.set_parameter_value({"name": "deadline:unknownParam", "value": "x"})


class TestUpdateSettings:
    def test_writes_values_back(self, widget):
        """update_settings writes all widget values into the settings object."""
        widget.sub_name_edit.setText("Written Name")
        widget.desc_edit.setText("Written Desc")
        widget.initial_status_box.setCurrentText("SUSPENDED")
        widget.max_failed_tasks_count_box.setValue(15)
        widget.max_retries_per_task_box.setValue(8)
        widget.priority_box.setValue(90)
        widget.limited_max_worker_count.setChecked(True)
        widget.max_worker_count_box.setValue(6)

        settings = MockSettings()
        widget.update_settings(settings)

        assert settings.name == "Written Name"
        assert settings.description == "Written Desc"
        assert settings.initial_status == "SUSPENDED"
        assert settings.max_failed_tasks_count == 15
        assert settings.max_retries_per_task == 8
        assert settings.priority == 90
        assert settings.max_worker_count == 6

    def test_unlimited_worker_count_writes_negative_one(self, widget):
        """update_settings writes -1 for max_worker_count when unlimited is selected."""
        widget.unlimited_max_worker_count.setChecked(True)

        settings = MockSettings(max_worker_count=10)
        widget.update_settings(settings)

        assert settings.max_worker_count == -1

    def test_skips_missing_attributes(self, widget):
        """update_settings does not crash on settings missing optional attributes."""
        widget.sub_name_edit.setText("Updated Name")
        widget.desc_edit.setText("Updated Desc")

        settings = MinimalSettings()
        widget.update_settings(settings)

        # name and description are always written
        assert settings.name == "Updated Name"
        assert settings.description == "Updated Desc"
        # MinimalSettings lacks optional attrs; verify no exception and no new attrs
        assert not hasattr(settings, "initial_status")
        assert not hasattr(settings, "max_failed_tasks_count")


class TestRadioButtonToggle:
    def test_limited_shows_spinbox(self, widget):
        """Selecting limited radio button shows the max worker count spinbox."""
        widget.limited_max_worker_count.setChecked(True)
        assert not widget.max_worker_count_box.isHidden()

    def test_unlimited_hides_spinbox(self, widget):
        """Selecting unlimited radio button hides the max worker count spinbox."""
        # First select limited to show the spinbox
        widget.limited_max_worker_count.setChecked(True)
        assert not widget.max_worker_count_box.isHidden()

        # Then switch to unlimited
        widget.unlimited_max_worker_count.setChecked(True)
        assert widget.max_worker_count_box.isHidden()

    def test_toggle_back_and_forth(self, widget):
        """Toggling between limited and unlimited correctly shows/hides spinbox."""
        widget.limited_max_worker_count.setChecked(True)
        assert not widget.max_worker_count_box.isHidden()

        widget.unlimited_max_worker_count.setChecked(True)
        assert widget.max_worker_count_box.isHidden()

        widget.limited_max_worker_count.setChecked(True)
        assert not widget.max_worker_count_box.isHidden()
