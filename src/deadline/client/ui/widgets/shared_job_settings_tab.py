# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
A UI Widget containing the render setup tab
"""
from __future__ import annotations

import sys
import threading
from typing import Any, Dict, Optional

from qtpy.QtCore import Signal  # type: ignore
from qtpy.QtWidgets import (  # type: ignore
    QComboBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

from ... import api
from ...config import get_setting
from .. import CancelationFlag
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
        self, *, initial_settings, initial_shared_parameter_values: dict[str, Any], parent=None
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

    def refresh_ui(self, job_settings: Any):
        # Refresh the job settings in the UI
        self.shared_job_properties_box.refresh_ui(job_settings)
        self.refresh_queue_parameters()

    def refresh_queue_parameters(self):
        """
        If the default queue id has changed, refresh the queue parameters.
        """
        farm_id = get_setting("defaults.farm_id")
        queue_id = get_setting("defaults.queue_id")
        if not farm_id or not queue_id:
            self.queue_parameters_box.rebuild_ui(async_loading_state="")
            return  # If the user has not selected a farm or queue ID, don't try to load
        if self.queue_parameters_box.async_loading_state or queue_id != self.queue_id:
            self.queue_parameters_box.rebuild_ui(
                async_loading_state="Reloading Queue Environments..."
            )
            # Join the thread if the queue id has changed and the thread is running
            if (
                queue_id != self.queue_id
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

    def __init__(self, *, initial_settings, parent=None):
        super().__init__("Job Properties", parent=parent)

        self._build_ui()
        self.refresh_ui(initial_settings)

    def _build_ui(self):
        self.layout = QFormLayout(self)
        self.layout.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)

        self.sub_name_edit = QLineEdit()
        self.layout.addRow("Name", self.sub_name_edit)

        self.desc_label = QLabel("Description")
        self.desc_edit = QLineEdit()
        self.layout.addRow(self.desc_label, self.desc_edit)

        self.priority_box_label = QLabel("Priority")
        self.priority_box = QSpinBox(parent=self)
        self.layout.addRow(self.priority_box_label, self.priority_box)

        self.initial_status_box_label = QLabel("Initial state")
        self.initial_status_box = QComboBox(parent=self)
        self.initial_status_box.addItems(["READY", "SUSPENDED"])
        self.layout.addRow(self.initial_status_box_label, self.initial_status_box)

        self.max_failed_tasks_count_box_label = QLabel("Maximum failed tasks count")
        self.max_failed_tasks_count_box_label.setToolTip(
            "Maximum number of tasks that can fail before the job will be marked as failed."
        )
        self.max_failed_tasks_count_box = QSpinBox(parent=self)
        self.max_failed_tasks_count_box.setRange(0, 2147483647)
        self.layout.addRow(self.max_failed_tasks_count_box_label, self.max_failed_tasks_count_box)

        self.max_retries_per_task_box_label = QLabel("Maximum retries per task")
        self.max_retries_per_task_box_label.setToolTip(
            "Maximum number of times that a task will retry before it's marked as failed."
        )
        self.max_retries_per_task_box = QSpinBox(parent=self)
        self.max_retries_per_task_box.setRange(0, 2147483647)
        self.layout.addRow(self.max_retries_per_task_box_label, self.max_retries_per_task_box)

    def refresh_ui(self, settings: Any):
        self.sub_name_edit.setText(settings.name)
        self.desc_edit.setText(settings.description)
        self.initial_status_box.setCurrentText("READY")
        self.max_failed_tasks_count_box.setValue(20)
        self.max_retries_per_task_box.setValue(5)
        self.priority_box.setValue(50)

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
        else:
            raise KeyError(parameter_name)

    def get_parameters(self):
        """
        Returns a list of OpenJD parameter definition dicts with
        a "value" key filled from the widget.
        """
        return [
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

    def update_settings(self, settings):
        """
        Update a given instance of scene settings with updated values.
        """
        settings.name = self.sub_name_edit.text()
        settings.description = self.desc_edit.text()


class DeadlineCloudSettingsWidget(QGroupBox):
    """
    UI component for the Deadline Cloud settings.
    """

    def __init__(self, *, parent: Optional[QWidget] = None):
        super().__init__("Deadline Cloud settings", parent=parent)
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
        self.farm_box_label = QLabel("Farm")
        self.farm_box = DeadlineFarmDisplay()
        self.layout.addRow(self.farm_box_label, self.farm_box)

        self.queue_box_label = QLabel("Queue")
        self.queue_box = DeadlineQueueDisplay()
        self.layout.addRow(self.queue_box_label, self.queue_box)

    def refresh_setting_controls(self, deadline_authorized):
        """
        Refreshes the controls for UI items that depend on the AWS Deadline Cloud API
        for their values.

        Args:
            deadline_authorized (bool): Should be the result of a call to
                    api.check_deadline_available, for example from
                    an AWS Deadline Cloud Status Widget.
        """
        self.farm_box.refresh(deadline_authorized)
        self.queue_box.refresh(deadline_authorized)


class _DeadlineNamedResourceDisplay(QWidget):
    """
    A Label for displaying an AWS Deadline Cloud resource, that starts displaying
    it as the Id, but does an async call to AWS Deadline Cloud to convert it
    to the name.

    Args:
        resource_name (str): The resource name for the list, like "Farm",
                "Queue", "Fleet".
        setting_name (str): The setting name for the item.
    """

    # Emitted when the background refresh thread catches an exception,
    # provides (operation_name, BaseException)
    background_exception = Signal(str, BaseException)

    # Emitted when an async refresh_item thread completes,
    # provides (refresh_id, id, name, description)
    _item_update = Signal(int, str, str, str)

    def __init__(self, *, resource_name, setting_name, parent=None):
        super().__init__(parent=parent)

        self.__refresh_thread = None
        self.__refresh_id = 0
        self.canceled = CancelationFlag()
        self.destroyed.connect(self.canceled.set_canceled)

        self.resource_name = resource_name
        self.setting_name = setting_name
        self.item_id = get_setting(self.setting_name)
        self.item_name = ""
        self.item_description = ""

        self._build_ui()

        self.label.setText(self.item_display_name())

    def _build_ui(self):
        self.label = QLabel(parent=self)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self.label)
        self._item_update.connect(self.handle_item_update)
        self.background_exception.connect(self.handle_background_exception)

    def handle_background_exception(self, e):
        self.label.setText(self.item_id)
        self.label.setToolTip("")

    def item_display_name(self):
        """Returns the text to display the item name as"""
        return self.item_name or self.item_id or "<not configured>"

    def refresh(self, deadline_authorized):
        """
        Starts a background thread to refresh the item name.

        Args:
            deadline_authorized (bool): Should be the result of a call to
                    api.check_deadline_available, for example from
                    an AWS Deadline Cloud Status Widget.
        """
        resource_id = get_setting(self.setting_name)
        if resource_id != self.item_id or not self.item_name:
            self.item_id = resource_id
            self.item_name = ""
            self.item_description = ""
            display_name = self.item_display_name()
            # Only call the AWS Deadline Cloud API if we've confirmed access
            if deadline_authorized:
                display_name = "<refreshing> - " + display_name

                self.__refresh_id += 1
                self.__refresh_thread = threading.Thread(
                    target=self._refresh_thread_function,
                    name=f"AWS Deadline Cloud refresh {self.resource_name} item thread",
                    args=(self.__refresh_id,),
                )
                self.__refresh_thread.start()

            self.label.setText(display_name)
            self.label.setToolTip(self.item_description)
        else:
            self.label.setText(self.item_display_name())

    def handle_item_update(self, refresh_id, id, name, description):
        # Apply the refresh if it's still for the latest call
        if refresh_id == self.__refresh_id:
            self.item_id = id
            self.item_name = name
            self.item_description = description
            self.label.setText(self.item_display_name())
            self.label.setToolTip(self.item_description)

    def _refresh_thread_function(self, refresh_id: int):
        """
        This function gets started in a background thread to refresh the list.
        """
        try:
            item = self.get_item()
            if not self.canceled:
                self._item_update.emit(refresh_id, *item)
        except BaseException as e:
            if not self.canceled:
                self.background_exception.emit(f"Refresh {self.resource_name} item", e)


class DeadlineFarmDisplay(_DeadlineNamedResourceDisplay):
    def __init__(self, *, parent=None):
        super().__init__(resource_name="Farm", setting_name="defaults.farm_id", parent=parent)

    def get_item(self):
        farm_id = get_setting(self.setting_name)
        if farm_id:
            deadline = api.get_boto3_client("deadline")
            response = deadline.get_farm(farmId=farm_id)
            return (response["farmId"], response["displayName"], response["description"])
        else:
            return ("", "", "")


class DeadlineQueueDisplay(_DeadlineNamedResourceDisplay):
    def __init__(self, *, parent=None):
        super().__init__(resource_name="Queue", setting_name="defaults.queue_id", parent=parent)

    def get_item(self):
        farm_id = get_setting("defaults.farm_id")
        queue_id = get_setting(self.setting_name)
        if farm_id and queue_id:
            deadline = api.get_boto3_client("deadline")
            response = deadline.get_queue(farmId=farm_id, queueId=queue_id)
            return (response["queueId"], response["displayName"], response["description"])
        else:
            return ("", "", "")


class DeadlineStorageProfileNameDisplay(_DeadlineNamedResourceDisplay):
    WINDOWS_OS = "Windows"
    MAC_OS = "Macos"
    LINUX_OS = "Linux"

    def __init__(self, *, parent=None):
        super().__init__(
            resource_name="Storage profile name",
            setting_name="settings.storage_profile_id",
            parent=parent,
        )

    def get_item(self):
        farm_id = get_setting("defaults.farm_id")
        queue_id = get_setting("defaults.queue_id")
        storage_profile_id = get_setting(self.setting_name)

        if farm_id and queue_id and storage_profile_id:
            deadline = api.get_boto3_client("deadline")
            response = deadline.list_storage_profiles_for_queue(farmId=farm_id, queueId=queue_id)
            farm_storage_profiles = response.get("storageProfiles", {})

            if farm_storage_profiles:
                storage_profile = [
                    (item["storageProfileId"], item["displayName"], item["osFamily"])
                    for item in farm_storage_profiles
                    if storage_profile_id == item["storageProfileId"]
                ]
                return storage_profile[0]

        return ("", "", "")

    def _get_default_storage_profile_name(self) -> str:
        """
        Get a string specifying what the OS is, following the format the Deadline storage profile API expects.
        """
        if sys.platform.startswith("linux"):
            return self.LINUX_OS

        if sys.platform.startswith("darwin"):
            return self.MAC_OS

        if sys.platform.startswith("win"):
            return self.WINDOWS_OS

        return ""
