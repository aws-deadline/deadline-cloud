# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
A UI Widget containing the render setup tab
"""

from __future__ import annotations

import threading
from typing import Any, Dict, Optional

from qtpy.QtCore import Signal  # type: ignore
from qtpy.QtWidgets import (  # type: ignore
    QComboBox,
    QFormLayout,
    QGroupBox,
    QLabel,
    QLineEdit,
    QRadioButton,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

from ...config import get_setting, set_setting, config_file
from .._utils import CancelationFlag, tr
from .openjd_parameters_widget import OpenJDParametersWidget
from ...api import get_queue_parameter_definitions


class SharedJobSettingsWidget(QWidget):  # pylint: disable=too-few-public-methods
    """
    Widget that holds Job setup shared across all job types.


    Signals:
        parameter_changed: This is sent whenever a parameter value in the widget changes. The message
            is a copy of the parameter definition with the "value" key containing the new value.

    Args:
        initial_settings: dataclass containing the job-specific settings.
        initial_shared_parameter_values: (dict[str, Any]): A dict of parameter values {<name>, <value>, ...}
            to override default queue parameter values from the queue. For example,
            a Rez queue environment may have a default "" for the RezPackages parameter, but a Maya
            submitter would override that default with "maya-2023" or similar.
        parent: The parent Qt Widget.
    """

    parameter_changed = Signal(dict)

    # Emitted when the queue parameter validity state changes
    valid_parameters = Signal(bool)

    # Emitted when the background refresh thread catches an exception,
    # provides (operation_name, BaseException)
    _background_exception = Signal(str, BaseException)

    # Emitted when an async queue parameters loading thread completes,
    # provides (refresh_id, queue_parameters)
    _queue_parameters_update = Signal(int, list)

    def __init__(
        self,
        *,
        initial_settings: Any,
        initial_shared_parameter_values: dict[str, Any],
        parent: Optional[QWidget] = None,
    ):
        super().__init__(parent=parent)
        layout = QVBoxLayout(self)

        # This is a dictionary {<name>: <value>} containing values to
        # override the queue parameter defaults.
        self.initial_shared_parameter_values = initial_shared_parameter_values

        self.shared_job_properties_box = SharedJobPropertiesWidget(
            initial_settings=initial_settings, parent=self
        )
        layout.addWidget(self.shared_job_properties_box)

        self.deadline_cloud_settings_box = DeadlineCloudSettingsWidget(parent=self)
        layout.addWidget(self.deadline_cloud_settings_box)

        self.queue_parameters_box = OpenJDParametersWidget(
            async_loading_state="Loading Queue Environments...", parent=self
        )
        layout.addWidget(self.queue_parameters_box)
        self.queue_parameters_box.parameter_changed.connect(
            lambda message: self.parameter_changed.emit(message)
        )

        self.__refresh_queue_parameters_thread: Optional[threading.Thread] = None
        self.__refresh_queue_parameters_id = 0
        self.__valid_queue = False
        self.canceled = CancelationFlag()
        self.destroyed.connect(self.canceled.set_canceled)
        self._queue_parameters_update.connect(self._handle_queue_parameters_update)
        self._background_exception.connect(self._handle_background_queue_parameters_exception)
        self._start_load_queue_parameters_thread()

        # Set any "deadline:*" parameters, like deadline:priority.
        # The queue parameters will be set asynchronously by the background thread.
        for name, value in initial_shared_parameter_values.items():
            if name.startswith("deadline:"):
                self.set_parameter_value({"name": name, "value": value})

    def __del__(self):
        self.canceled.set_canceled()
        if (
            self.__refresh_queue_parameters_thread
            and self.__refresh_queue_parameters_thread.is_alive()
        ):
            self.__refresh_queue_parameters_thread.join()

    def refresh_ui(self, job_settings: Any, load_new_bundle: bool = False):
        # Refresh the job settings in the UI
        self.shared_job_properties_box.refresh_ui(job_settings)

        if load_new_bundle:
            # Update the initial shared parameter values corresponding to the new job bundle
            self.initial_shared_parameter_values = {}
            for parameter in job_settings.parameters:
                if "default" in parameter or "value" in parameter:
                    self.initial_shared_parameter_values[parameter["name"]] = parameter.get(
                        "value", parameter.get("default")
                    )
        self.refresh_queue_parameters(load_new_bundle)

    def refresh_queue_parameters(self, load_new_bundle: bool = False):
        """
        If the default farm id, queue id, or job bundle has changed, refresh the queue parameters.
        """
        farm_id = get_setting("defaults.farm_id")
        queue_id = get_setting("defaults.queue_id")
        if not farm_id or not queue_id:
            self.queue_parameters_box.rebuild_ui(async_loading_state="")
            return  # If the user has not selected a farm or queue ID, don't try to load
        farm_or_queue_changed = farm_id != self.farm_id or queue_id != self.queue_id
        if (
            self.queue_parameters_box.async_loading_state
            or farm_or_queue_changed
            or load_new_bundle
        ):
            self.queue_parameters_box.rebuild_ui(
                async_loading_state="Reloading Queue Environments..."
            )
            # Join the thread if the farm, queue id, or job bundle has changed and the thread is running
            if (
                (farm_or_queue_changed or load_new_bundle)
                and self.__refresh_queue_parameters_thread
                and self.__refresh_queue_parameters_thread.is_alive()
            ):
                self.__refresh_queue_parameters_thread.join()

            # Start the thread if it doesn't exist or is not alive
            if (
                not self.__refresh_queue_parameters_thread
                or not self.__refresh_queue_parameters_thread.is_alive()
            ):
                self._start_load_queue_parameters_thread()

    def _handle_background_queue_parameters_exception(self, title: str, error: BaseException):
        self.__valid_queue = False
        self.valid_parameters.emit(False)
        if self.__refresh_queue_parameters_thread:
            self.canceled.set_canceled()
            self.__refresh_queue_parameters_thread.join()
        self.queue_parameters_box.rebuild_ui(
            async_loading_state="Error loading queue environments: {}\n\nError traceback: {}".format(
                title, error
            )
        )

    def _start_load_queue_parameters_thread(self):
        """
        Starts a background thread to load the queue parameters.
        """
        self.farm_id = farm_id = get_setting("defaults.farm_id")
        self.queue_id = queue_id = get_setting("defaults.queue_id")
        if not self.farm_id or not self.queue_id:
            # If the user has not selected a farm or queue ID, don't bother starting
            # the thread.
            return
        self.__refresh_queue_parameters_id += 1
        self.canceled = CancelationFlag()
        self.__refresh_queue_parameters_thread = threading.Thread(
            target=self._load_queue_parameters_thread_function,
            name="AWS Deadline Cloud load queue parameters thread",
            args=(self.__refresh_queue_parameters_id, farm_id, queue_id),
        )
        self.__refresh_queue_parameters_thread.start()

    def is_queue_valid(self) -> bool:
        return self.__valid_queue

    def _handle_queue_parameters_update(self, refresh_id, queue_parameters):
        # Apply the refresh if it's still for the latest call
        if refresh_id == self.__refresh_queue_parameters_id:
            self.__valid_queue = True
            self.valid_parameters.emit(True)
            # Apply the initial queue parameter values
            for parameter in queue_parameters:
                if parameter["name"] in self.initial_shared_parameter_values:
                    parameter["value"] = self.initial_shared_parameter_values[parameter["name"]]
            self.queue_parameters_box.rebuild_ui(parameter_definitions=queue_parameters)

    def _load_queue_parameters_thread_function(self, refresh_id: int, farm_id: str, queue_id: str):
        """
        This function gets started in a background thread to refresh the list.
        """
        try:
            queue_parameters = get_queue_parameter_definitions(farmId=farm_id, queueId=queue_id)
            if not self.canceled:
                self._queue_parameters_update.emit(refresh_id, queue_parameters)
        except BaseException as e:
            if not self.canceled:
                self._background_exception.emit("Invalid queue parameters", e)

    def update_settings(self, settings):
        self.shared_job_properties_box.update_settings(settings)

    def get_parameters(self):
        """
        Returns a list of OpenJD parameter definition dicts with
        a "value" key filled from the widget.
        """
        queue_parameters = self.queue_parameters_box.get_parameters()
        deadline_shared_job_parameters = self.shared_job_properties_box.get_parameters()

        return queue_parameters + deadline_shared_job_parameters

    def set_parameter_value(self, parameter: dict[str, Any]):
        """
        Given an OpenJD parameter definition with a "value" key,
        set the parameter value in the widget.

        If the parameter value cannot be set, raises a KeyError.
        """
        if parameter["name"].startswith("deadline:"):
            self.shared_job_properties_box.set_parameter_value(parameter)
        else:
            self.queue_parameters_box.set_parameter_value(parameter)


class SharedJobPropertiesWidget(QGroupBox):  # pylint: disable=too-few-public-methods
    """
    UI element to hold top level description components of the submission

    The settings object should be a dataclass with:
      - `name: str`        The name of the Job to submit.
      - `description: str`  The description of the Job to submit.
    """

    def __init__(self, *, initial_settings, parent: Optional[QWidget] = None):
        super().__init__(tr("Job Properties"), parent=parent)

        self._build_ui()
        self.refresh_ui(initial_settings)

    def _build_ui(self):
        self.layout = QFormLayout(self)
        self.layout.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)

        self.sub_name_edit = QLineEdit()
        self.sub_name_edit.setMaxLength(128)
        self.layout.addRow("Name", self.sub_name_edit)

        self.desc_label = QLabel(tr("Description"))
        self.desc_edit = QLineEdit()
        self.desc_edit.setMaxLength(2048)
        self.layout.addRow(self.desc_label, self.desc_edit)

        self.priority_box_label = QLabel(tr("Priority"))
        self.priority_box = QSpinBox(parent=self)
        self.priority_box.setRange(0, 100)
        self.layout.addRow(self.priority_box_label, self.priority_box)

        self.initial_status_box_label = QLabel(tr("Initial state"))
        self.initial_status_box = QComboBox(parent=self)
        self.initial_status_box.addItems(["READY", "SUSPENDED"])
        self.layout.addRow(self.initial_status_box_label, self.initial_status_box)

        self.max_failed_tasks_count_box_label = QLabel(tr("Maximum failed tasks count"))
        self.max_failed_tasks_count_box_label.setToolTip(
            "Maximum number of tasks that can fail before the job will be marked as failed."
        )
        self.max_failed_tasks_count_box = QSpinBox(parent=self)
        self.max_failed_tasks_count_box.setRange(0, 2147483647)
        self.layout.addRow(self.max_failed_tasks_count_box_label, self.max_failed_tasks_count_box)

        self.max_retries_per_task_box_label = QLabel(tr("Maximum retries per task"))
        self.max_retries_per_task_box_label.setToolTip(
            "Maximum number of times that a task will retry before it's marked as failed."
        )
        self.max_retries_per_task_box = QSpinBox(parent=self)
        self.max_retries_per_task_box.setRange(0, 2147483647)
        self.layout.addRow(self.max_retries_per_task_box_label, self.max_retries_per_task_box)

        self.max_worker_count_box_label = QLabel(tr("Maximum worker count"))
        self.max_worker_count_box_label.setToolTip(tr("Maximum worker count of job."))
        self.max_worker_count_box = QSpinBox()
        self.max_worker_count_box.setRange(1, 2147483647)
        self.unlimited_max_worker_count = QRadioButton(tr("No max worker count"))
        self.limited_max_worker_count = QRadioButton(tr("Set max worker count"))
        self.limited_max_worker_count.toggled.connect(
            self.limited_max_worker_count_radio_button_toggled
        )
        self.max_worker_count_layout = QVBoxLayout()
        self.max_worker_count_layout.addWidget(self.unlimited_max_worker_count)
        self.max_worker_count_layout.addWidget(self.limited_max_worker_count)
        self.max_worker_count_layout.addWidget(self.max_worker_count_box)
        self.layout.addRow(self.max_worker_count_box_label, self.max_worker_count_layout)

    def limited_max_worker_count_radio_button_toggled(self, state):
        """
        Enable the max worker count text box when limited max worker count radio button is enabled.
        """
        self.max_worker_count_box.setHidden(not state)

    def _has_compatible_attr(self, obj, attr_name, expected_type):
        """
        Determine if attribute exists and if the type is correct.
        """
        # DCCs can have anything in the settings object since they define their own dataclass to pass in.
        # Changing what we look for below may cause breaking changes in usage of this library.
        return isinstance(getattr(obj, attr_name, None), expected_type)

    def refresh_ui(self, settings: Any):
        self.sub_name_edit.setText(settings.name)
        self.desc_edit.setText(settings.description)

        # Set all fields with type checking
        self.initial_status_box.setCurrentText(
            settings.initial_status
            if self._has_compatible_attr(settings, "initial_status", str)
            else "READY"
        )
        self.max_failed_tasks_count_box.setValue(
            settings.max_failed_tasks_count
            if self._has_compatible_attr(settings, "max_failed_tasks_count", int)
            else 20
        )
        self.max_retries_per_task_box.setValue(
            settings.max_retries_per_task
            if self._has_compatible_attr(settings, "max_retries_per_task", int)
            else 5
        )
        self.priority_box.setValue(
            settings.priority if self._has_compatible_attr(settings, "priority", int) else 50
        )

        has_limited_max_worker_count = (
            (settings.max_worker_count > 0)
            if self._has_compatible_attr(settings, "max_worker_count", int)
            else False
        )
        self.unlimited_max_worker_count.setChecked(not has_limited_max_worker_count)
        self.limited_max_worker_count.setChecked(has_limited_max_worker_count)
        self.max_worker_count_box.setHidden(not has_limited_max_worker_count)
        if has_limited_max_worker_count:
            self.max_worker_count_box.setValue(settings.max_worker_count)

    def set_parameter_value(self, parameter: dict[str, Any]):
        """
        Given an OpenJD parameter definition with a "value" key,
        set the parameter value in the widget.

        If the parameter value cannot be set, raises a KeyError.
        """
        parameter_name = parameter["name"]
        if parameter_name == "deadline:targetTaskRunStatus":
            self.initial_status_box.setCurrentText(parameter["value"])
        elif parameter_name == "deadline:maxFailedTasksCount":
            self.max_failed_tasks_count_box.setValue(parameter["value"])
        elif parameter_name == "deadline:maxRetriesPerTask":
            self.max_retries_per_task_box.setValue(parameter["value"])
        elif parameter_name == "deadline:priority":
            self.priority_box.setValue(parameter["value"])
        elif parameter_name == "deadline:maxWorkerCount":
            if parameter["value"] == -1:
                self.unlimited_max_worker_count.setChecked(True)
                self.limited_max_worker_count.setChecked(False)
                self.max_worker_count_box.setHidden(True)
            else:
                self.unlimited_max_worker_count.setChecked(False)
                self.limited_max_worker_count.setChecked(True)
                self.max_worker_count_box.setHidden(False)
                self.max_worker_count_box.setValue(parameter["value"])
        else:
            raise KeyError(parameter_name)

    def get_parameters(self):
        """
        Returns a list of OpenJD parameter definition dicts with
        a "value" key filled from the widget.
        """
        job_parameters = [
            {
                "name": "deadline:targetTaskRunStatus",
                "type": "STRING",
                "userInterface": {
                    "control": "DROPDOWN_LIST",
                    "label": "Initial state",
                },
                "allowedValues": ["READY", "SUSPENDED"],
                "value": self.initial_status_box.currentText(),
            },
            {
                "name": "deadline:maxFailedTasksCount",
                "description": "Maximum number of Tasks that can fail before the Job will be marked as failed.",
                "type": "INT",
                "userInterface": {
                    "control": "SPIN_BOX",
                    "label": "Maximum failed tasks count",
                },
                "minValue": 0,
                "value": self.max_failed_tasks_count_box.value(),
            },
            {
                "name": "deadline:maxRetriesPerTask",
                "description": "Maximum number of times that a task will retry before it's marked as failed.",
                "type": "INT",
                "userInterface": {
                    "control": "SPIN_BOX",
                    "label": "Maximum retries per task",
                },
                "minValue": 0,
                "value": self.max_retries_per_task_box.value(),
            },
            {"name": "deadline:priority", "type": "INT", "value": self.priority_box.value()},
        ]
        if not self.unlimited_max_worker_count.isChecked():
            job_parameters.append(
                {
                    "name": "deadline:maxWorkerCount",
                    "type": "INT",
                    "value": self.max_worker_count_box.value(),
                }
            )
        return job_parameters

    def update_settings(self, settings):
        """
        Update a given instance of scene settings with updated values.
        """
        # TODO: Extract sticky settings from per-DCC implementation to centralized.
        settings.name = self.sub_name_edit.text()
        settings.description = self.desc_edit.text()

        # Set all fields with type checking
        if self._has_compatible_attr(settings, "initial_status", str):
            settings.initial_status = self.initial_status_box.currentText()

        if self._has_compatible_attr(settings, "max_failed_tasks_count", int):
            settings.max_failed_tasks_count = self.max_failed_tasks_count_box.value()

        if self._has_compatible_attr(settings, "max_retries_per_task", int):
            settings.max_retries_per_task = self.max_retries_per_task_box.value()

        if self._has_compatible_attr(settings, "priority", int):
            settings.priority = self.priority_box.value()

        # Handle `max_worker_count` based on UI selection:
        # Preserve unlimited worker setting by using -1 instead of overriding with spin box value
        if self._has_compatible_attr(settings, "max_worker_count", int):
            if self.unlimited_max_worker_count.isChecked():
                settings.max_worker_count = -1  # -1 denotes no max worker count limits.
            else:
                settings.max_worker_count = self.max_worker_count_box.value()


class DeadlineCloudSettingsWidget(QGroupBox):
    """
    UI component for the Deadline Cloud settings.
    """

    def __init__(self, *, parent: Optional[QWidget] = None):
        super().__init__(tr("Deadline Cloud settings"), parent=parent)
        self.deadline_settings: Dict[str, Any] = {"counter": -1}
        self.layout = QFormLayout(self)
        self.layout.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)

        self._build_ui()

    def _set_enabled_with_label(self, prop_name: str, enabled: bool):
        """Sets the enabled status of a control and its label"""
        getattr(self, prop_name).setEnabled(enabled)
        getattr(self, prop_name + "_label").setEnabled(enabled)

    def _build_ui(self):
        """
        Build the UI for the Deadline settings
        """
        # Import here to avoid circular import
        from ..dialogs.deadline_config_dialog import (
            DeadlineFarmListComboBox,
            DeadlineQueueListComboBox,
            DeadlineStorageProfileNameListComboBox,
        )

        self.farm_box_label = QLabel(tr("Farm"))
        self.farm_box = DeadlineFarmListComboBox(parent=self)
        self.layout.addRow(self.farm_box_label, self.farm_box)

        self.queue_box_label = QLabel(tr("Queue"))
        self.queue_box = DeadlineQueueListComboBox(parent=self)
        self.layout.addRow(self.queue_box_label, self.queue_box)

        self.storage_profile_box_label = QLabel(tr("Storage profile"))
        self.storage_profile_box = DeadlineStorageProfileNameListComboBox(parent=self)
        self.layout.addRow(self.storage_profile_box_label, self.storage_profile_box)

        # Hide storage profile by default - only show when queue has storage profiles
        self._set_storage_profile_visible(False)

        # Connect signals for farm, queue, and storage profile changes
        self.farm_box.box.currentIndexChanged.connect(self._on_farm_changed)
        self.queue_box.box.currentIndexChanged.connect(self._on_queue_changed)
        self.storage_profile_box.box.currentIndexChanged.connect(self._on_storage_profile_changed)

        # Connect to storage profile combo box model to detect when list changes
        self.storage_profile_box.box.model().rowsInserted.connect(
            self._update_storage_profile_visibility
        )
        self.storage_profile_box.box.model().rowsRemoved.connect(
            self._update_storage_profile_visibility
        )
        self.storage_profile_box.box.model().modelReset.connect(
            self._update_storage_profile_visibility
        )
        # Also connect to the _list_update signal to handle updates after async refresh
        self.storage_profile_box._list_update.connect(
            lambda *args: self._update_storage_profile_visibility()
        )

        # Initialize with current config
        config = config_file.read_config()
        self.farm_box.set_config(config)
        self.queue_box.set_config(config)
        self.storage_profile_box.set_config(config)

    def _set_storage_profile_visible(self, visible: bool):
        """Show or hide the storage profile selector"""
        self.storage_profile_box_label.setVisible(visible)
        self.storage_profile_box.setVisible(visible)

    def _update_storage_profile_visibility(self):
        """Update storage profile visibility based on available profiles"""
        # Check if there are actual storage profiles (not just placeholder items)
        # Note: "<none selected>" sorts first alphabetically and has empty string as data,
        # so we need to check if there are items with non-empty data
        count = self.storage_profile_box.box.count()
        has_real_profiles = False
        for i in range(count):
            item_data = self.storage_profile_box.box.itemData(i)
            item_text = self.storage_profile_box.box.itemText(i)
            # Skip placeholder items
            if item_text in ("<refreshing>", "<none selected>") or item_data in (None, ""):
                continue
            has_real_profiles = True
            break
        self._set_storage_profile_visible(has_real_profiles)

    def _on_farm_changed(self, index: int):
        """Handle farm selection change in Submit Dialog"""
        if index < 0:
            return

        # Get the selected farm ID from the combo box
        farm_id = self.farm_box.box.itemData(index)
        if farm_id is None:
            return

        # Update config immediately (unlike Settings Dialog which defers to apply())
        set_setting("defaults.farm_id", farm_id)

        # Refresh queue list for the new farm (same as Settings Dialog)
        self.queue_box.refresh_list()

        # Notify parent to refresh (triggers submit button state update and queue parameters)
        self._notify_parent_refresh()

    def _on_queue_changed(self, index: int):
        """Handle queue selection change in Submit Dialog"""
        if index < 0:
            return

        # Get the selected queue ID from the combo box
        queue_id = self.queue_box.box.itemData(index)
        if queue_id is None:
            return

        # Update config immediately (unlike Settings Dialog which defers to apply())
        set_setting("defaults.queue_id", queue_id)

        # Refresh storage profile list for the new queue
        self.storage_profile_box.refresh_list()

        # Notify parent to refresh (triggers submit button state update and queue parameters)
        self._notify_parent_refresh()

    def _on_storage_profile_changed(self, index: int):
        """Handle storage profile selection change in Submit Dialog"""
        if index < 0:
            return

        # Get the selected storage profile ID from the combo box
        storage_profile_id = self.storage_profile_box.box.itemData(index)

        # Update config immediately
        set_setting("settings.storage_profile_id", storage_profile_id if storage_profile_id else "")

    def _notify_parent_refresh(self):
        """Helper to notify parent widgets to refresh after config changes"""
        # Find SharedJobSettingsWidget parent to refresh queue parameters
        parent_widget = self.parent()
        while parent_widget is not None:
            if hasattr(parent_widget, "refresh_queue_parameters"):
                parent_widget.refresh_queue_parameters()
            if hasattr(parent_widget, "parent") and callable(parent_widget.parent):
                parent_widget = parent_widget.parent()
            else:
                break

        # Find SubmitJobToDeadlineDialog to refresh submit button state
        parent_widget = self.parent()
        while parent_widget is not None:
            if hasattr(parent_widget, "refresh_deadline_settings"):
                parent_widget.refresh_deadline_settings()
                break
            if hasattr(parent_widget, "parent") and callable(parent_widget.parent):
                parent_widget = parent_widget.parent()
            else:
                break

    def refresh_setting_controls(self, deadline_authorized):
        """
        Refreshes the controls for UI items that depend on the AWS Deadline Cloud API
        for their values.

        Args:
            deadline_authorized (bool): Should be the result of a call to
                    api.check_deadline_available, for example from
                    an AWS Deadline Cloud Status Widget.
        """
        # Update config for combo boxes
        config = config_file.read_config()
        self.farm_box.set_config(config)
        self.queue_box.set_config(config)
        self.storage_profile_box.set_config(config)

        # Refresh selected items to reflect current config
        self.farm_box.refresh_selected_id()
        self.queue_box.refresh_selected_id()
        self.storage_profile_box.refresh_selected_id()

        # Refresh lists if authorized
        if deadline_authorized:
            self.farm_box.refresh_list()
            self.queue_box.refresh_list()
            self.storage_profile_box.refresh_list()
