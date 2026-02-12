# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
Combo box widgets for selecting AWS Deadline Cloud resources (farms, queues, storage profiles).
These are used by both the Settings Dialog and the Submit Dialog.
"""

__all__ = [
    "DeadlineFarmListComboBox",
    "DeadlineQueueListComboBox",
    "DeadlineStorageProfileNameListComboBox",
]

import sys
import threading
from configparser import ConfigParser
from typing import Optional

from qtpy.QtCore import QSize, Signal
from qtpy.QtWidgets import (  # type: ignore
    QApplication,
    QComboBox,
    QHBoxLayout,
    QPushButton,
    QStyle,
    QWidget,
)

from ... import api
from ...config import config_file
from .._utils import CancelationFlag, block_signals


class _DeadlineResourceListComboBox(QWidget):
    """
    A ComboBox for selecting an AWS Deadline Cloud Id, with a refresh button.

    The caller should connect the `background_exception` signal, e.g.
    to show a message box, and should call `set_config` whenever there is
    a change to the AWS Deadline Cloud config object.

    Args:
        resource_name (str): The resource name for the list, like "Farm",
                "Queue", "Fleet".
    """

    # Emitted when the background refresh thread catches an exception,
    # provides (operation_name, BaseException)
    background_exception = Signal(str, BaseException)

    # Emitted when an async refresh_farms_list thread completes,
    # provides (refresh_id, [(farm_id, farm_name), ...])
    _list_update = Signal(int, list)

    def __init__(self, resource_name, setting_name, parent: Optional[QWidget] = None):
        super().__init__(parent)

        self.__refresh_thread = None
        self.__refresh_id = 0
        self.canceled = CancelationFlag()
        self.destroyed.connect(self.canceled.set_canceled)

        self.resource_name = resource_name
        self.setting_name = setting_name

        self._build_ui()

    def _build_ui(self):
        self.box = QComboBox(parent=self)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self.box, stretch=1)

        self.refresh_button = QPushButton("")
        layout.addWidget(self.refresh_button)
        self.refresh_button.setIcon(QApplication.style().standardIcon(QStyle.SP_BrowserReload))
        self.refresh_button.setFixedSize(QSize(22, 22))  # Make the button square
        self.refresh_button.clicked.connect(self.refresh_list)
        self._list_update.connect(self.handle_list_update)
        self.background_exception.connect(self.handle_background_exception)

    def handle_background_exception(self, e):
        with block_signals(self.box):
            self.box.clear()
        self.refresh_selected_id()

    def count(self) -> int:
        """Returns the number of items in the combobox"""
        return self.box.count()

    def set_config(self, config: ConfigParser) -> None:
        """Updates the AWS Deadline Cloud config object the control uses."""
        self.config = config

    def clear_list(self):
        """
        Fully clears the list. The caller needs to call either
        `refresh_list` or `refresh_selected_id` at a later point
        to finish it.
        """
        with block_signals(self.box):
            self.box.clear()

    def refresh_list(self):
        """
        Starts a background thread to refresh the resource list.
        """
        config = self.config
        selected_id = config_file.get_setting(self.setting_name, config=config)
        # Reset to a list of just the currently configured id during refresh
        with block_signals(self.box):
            self.box.clear()
            self.box.addItem("<refreshing>", userData=selected_id)

        self.__refresh_id += 1
        self.__refresh_thread = threading.Thread(
            target=self._refresh_thread_function,
            name=f"AWS Deadline Cloud refresh {self.resource_name} thread",
            args=(self.__refresh_id, config),
        )
        self.__refresh_thread.start()

    def handle_list_update(self, refresh_id, items_list):
        # Apply the refresh if it's still for the latest call
        if refresh_id == self.__refresh_id:
            with block_signals(self.box):
                self.box.clear()
                for name, id in items_list:
                    self.box.addItem(name, userData=id)

                self.refresh_selected_id()

    def refresh_selected_id(self):
        """Refreshes the selected id from the config object"""
        selected_id = config_file.get_setting(self.setting_name, config=self.config)
        # Restore the selected Id, inserting a new item if
        # it doesn't exist in the list.
        with block_signals(self.box):
            index = self.box.findData(selected_id)
            if index >= 0:
                self.box.setCurrentIndex(index)
            elif selected_id:
                # User has a configured ID but it's not in the list (e.g., lacks
                # permission to list resources). Show the raw ID so they can see
                # what's configured.
                self.box.insertItem(0, selected_id, userData=selected_id)
                self.box.setCurrentIndex(0)
            else:
                # No ID configured - show "<none selected>"
                index = self.box.findText("<none selected>")
                if index >= 0:
                    self.box.setCurrentIndex(index)
                else:
                    self.box.insertItem(0, "<none selected>", userData="")
                    self.box.setCurrentIndex(0)
                    self.box.setCurrentIndex(0)

    def _refresh_thread_function(self, refresh_id: int, config: Optional[ConfigParser] = None):
        """
        This function gets started in a background thread to refresh the list.
        """
        try:
            resources = self.list_resources(config=config)
            if not self.canceled:
                self._list_update.emit(refresh_id, resources)
        except BaseException as e:
            if not self.canceled and refresh_id == self.__refresh_id:
                self.background_exception.emit(f"Refresh {self.resource_name}s list", e)


class DeadlineFarmListComboBox(_DeadlineResourceListComboBox):
    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(resource_name="Farm", setting_name="defaults.farm_id", parent=parent)

    def list_resources(self, config: Optional[ConfigParser]):
        response = api.list_farms(config=config)
        return sorted(
            [(item["displayName"], item["farmId"]) for item in response["farms"]],
            key=lambda item: (item[0].casefold(), item[1]),
        )


class DeadlineQueueListComboBox(_DeadlineResourceListComboBox):
    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(resource_name="Queue", setting_name="defaults.queue_id", parent=parent)

    def list_resources(self, config: Optional[ConfigParser]):
        default_farm_id = config_file.get_setting("defaults.farm_id", config=config)
        if default_farm_id:
            response = api.list_queues(config=config, farmId=default_farm_id)
            return sorted(
                [(item["displayName"], item["queueId"]) for item in response["queues"]],
                key=lambda item: (item[0].casefold(), item[1]),
            )
        else:
            return []


class DeadlineStorageProfileNameListComboBox(_DeadlineResourceListComboBox):
    WINDOWS_OS = "windows"
    MAC_OS = "macos"
    LINUX_OS = "linux"

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(
            resource_name="Storage profile",
            setting_name="settings.storage_profile_id",
            parent=parent,
        )

    def list_resources(self, config: Optional[ConfigParser]):
        default_farm_id = config_file.get_setting("defaults.farm_id", config=config)
        default_queue_id = config_file.get_setting("defaults.queue_id", config=config)
        if default_farm_id and default_queue_id:
            response = api.list_storage_profiles_for_queue(
                config=config, farmId=default_farm_id, queueId=default_queue_id
            )
            storage_profiles = response.get("storageProfiles", [])
            # add a "<none selected>" option since its possible to select nothing for this type
            # of resource
            storage_profiles.append(
                {
                    "storageProfileId": "",
                    "displayName": "<none selected>",
                    "osFamily": self._get_current_os(),
                }
            )
            return sorted(
                [
                    (item["displayName"], item["storageProfileId"])
                    for item in storage_profiles
                    if self._get_current_os() == item["osFamily"].lower()
                ],
                key=lambda item: (item[0].casefold(), item[1]),
            )
        else:
            return []

    def _get_current_os(self) -> str:
        """
        Get a string specifying what the OS is, following the format the Deadline storage profile API expects.
        """
        if sys.platform.startswith("linux"):
            return self.LINUX_OS

        if sys.platform.startswith("darwin"):
            return self.MAC_OS

        if sys.platform.startswith("win"):
            return self.WINDOWS_OS

        return "Unknown"
