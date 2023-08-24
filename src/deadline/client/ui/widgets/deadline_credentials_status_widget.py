# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Provides a widget to place in Amazon Deadline Cloud submitter dialogs, that shows
the current status of Amazon Deadline Cloud credentials.

The widget emits the following Qt Signals:
   aws_creds_changed: The AWS credentials in ~/.aws changed.
   deadline_config_changed: The Amazon Deadline Cloud configuration in ~/.deadline changed.
   refresh_thread_update: A background thread confirming the credentials
                        status updated one of the status variables.

The status includes three parts:
  1. Are credentials configured and available for use?
     This is checked with an sts:GetCallerIdentity AWS API call.
  2. Do the credentials grant access to Amazon Deadline Cloud APIs?
     This is checked with a dry-run deadline:ListFarms AWS API call.
  3. Do the credentials use Cloud Companion?
     This is checked by looking for the relevant properties
     in the AWS profile configuration.
"""
import os
import threading
from configparser import ConfigParser
from logging import getLogger
from typing import Optional

from PySide2.QtCore import QFileSystemWatcher, Signal
from PySide2.QtWidgets import (  # pylint: disable=import-error; type: ignore
    QFrame,
    QHBoxLayout,
    QLabel,
    QSizePolicy,
    QWidget,
)

from ... import api
from ...config import config_file
from .. import CancelationFlag

logger = getLogger(__name__)


class DeadlineCredentialsStatusWidget(QWidget):
    """
    A Widget that holds and displays status information about Amazon Deadline Cloud
    credentials. Currently displayed status values are available as
    properties:

       widget.creds_type: result of api.get_credentials_type()
       widget.creds_status: result of api.check_credentials_status()
       widget.deadline_api_available: result of api.check_deadline_api_available()

    To display the status of a non-default Amazon Deadline Cloud configuration, pass in
    a Amazon Deadline Cloud configuration object to config, call set_config to change it.
    """

    # This signal is sent when an AWS credential changes (e.g. config file)
    aws_creds_changed = Signal()
    # This signal is sent when the Amazon Deadline Cloud configuration changes
    deadline_config_changed = Signal()

    # This signal is sent when the background status refresh thread has an update.
    refresh_thread_update = Signal(int)

    def __init__(self, parent=None, config: Optional[ConfigParser] = None) -> None:
        super().__init__(parent=parent)

        self.__refresh_thread: Optional[threading.Thread] = None
        self.__refresh_id = 0
        self.creds_status: Optional[api.AwsCredentialsStatus] = None
        self.deadline_authorized: Optional[bool] = None
        # Make a copy of the config object
        self.config: Optional[ConfigParser] = None
        if config:
            self.config = ConfigParser()
            self.config.read_dict(config)
        self.canceled = CancelationFlag()
        self.destroyed.connect(self.canceled.set_canceled)

        # Watch the ~/.aws path for any changes to config or credentials,
        # the ~/.aws/sso/cache to capture "aws sso login/logout", and
        # the ~/.deadline path for any changes to the Amazon Deadline Cloud config.
        self.aws_creds_file_watcher = QFileSystemWatcher()
        self.aws_creds_paths = [
            os.path.expanduser(os.path.join("~", ".aws")),
            os.path.expanduser(os.path.join("~", ".aws", "sso", "cache")),
        ]
        self.deadline_config_paths = [
            os.path.expanduser(os.path.join("~", ".deadline")),
        ]
        failed_paths = self.aws_creds_file_watcher.addPaths(
            self.aws_creds_paths + self.deadline_config_paths
        )
        if failed_paths:
            logger.error(
                "Failed to watch these AWS/Amazon Deadline Cloud configurations: %s", failed_paths
            )
        self.aws_creds_file_watcher.fileChanged.connect(self.files_changed)
        self.aws_creds_file_watcher.directoryChanged.connect(self.files_changed)

        layout = QHBoxLayout(self)

        self.creds_type_label = QLabel("")
        layout.addWidget(self.creds_type_label)
        self.creds_status_label = QLabel("")
        layout.addWidget(self.creds_status_label)
        self.deadline_authorized_label = QLabel("")
        layout.addWidget(self.deadline_authorized_label)

        # Make each label a sunken panel
        for label in (
            self.creds_status_label,
            self.deadline_authorized_label,
            self.creds_type_label,
        ):
            label.setFrameStyle(QFrame.Panel | QFrame.Sunken)
            label.setLineWidth(1)
            label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
            label.setMinimumSize(60, 22)

        self.refresh_thread_update.connect(self.handle_refresh_thread_update)

        self.refresh_status()

    def set_config(self, config: Optional[ConfigParser]) -> None:
        """
        Changes the Amazon Deadline Cloud configuration object used to display credentials
        status.

        Args:
            config (ConfigParser): The Amazon Deadline Cloud configuration to use.
        """

        # Refresh the status if any setting that impacts credentials was changed
        if self.config:
            creds_config_changed = False
            for setting_name in [
                "defaults.aws_profile_name",
                "settings.deadline_endpoint_url",
            ]:
                if config_file.get_setting(setting_name, self.config) != config_file.get_setting(
                    setting_name, config
                ):
                    creds_config_changed = True
        else:
            creds_config_changed = True

        # Make a copy of the config object
        self.config = None
        if config:
            self.config = ConfigParser()
            self.config.read_dict(config)

        if creds_config_changed:
            self.refresh_status()

    def files_changed(self, changed_path):
        logger.info(f"Path {changed_path} changed, refreshing credential status")

        # Force the cached boto3 session to refresh, since we don't check the creds
        # file
        if changed_path in self.aws_creds_paths:
            api.get_boto3_session(force_refresh=True)

        self.refresh_status()

        if changed_path in self.aws_creds_paths:
            self.aws_creds_changed.emit()
        elif changed_path in self.deadline_config_paths:
            self.deadline_config_changed.emit()

    def _set_status_refreshing(self) -> None:
        self.creds_type_label.setText("Creds: <refreshing>")
        self.creds_status_label.setText("Status: <refreshing>")
        self.deadline_authorized_label.setText("Amazon Deadline Cloud API: <refreshing>")

    def _refresh_status_background_thread(self, refresh_id: int):
        """
        This function is for running in a background thread, started
        by the `refresh_status` function. We run it in the background to avoid
        blocking the UI when there is API latency.
        """
        try:
            self.creds_status = api.check_credentials_status(config=self.config)
            if self.canceled:
                return
            self.refresh_thread_update.emit(refresh_id)

            self.deadline_authorized = api.check_deadline_api_available(config=self.config)
            if not self.canceled:
                self.refresh_thread_update.emit(refresh_id)
        except BaseException as e:
            logger.exception(e)
            self.creds_status = api.AwsCredentialsStatus.CONFIGURATION_ERROR
            self.deadline_authorized = False
            if not self.canceled:
                self.refresh_thread_update.emit(refresh_id)

    def refresh_status(self) -> None:
        """
        Initiates an asynchronous status refresh.
        """
        self._set_status_refreshing()

        self.creds_type = api.get_credentials_type(config=self.config)
        color = "red" if self.creds_type == api.AwsCredentialsType.NOT_VALID else "green"
        self.creds_type_label.setText(
            f"Creds: <b style='color:{color};'>{self.creds_type.name}</b>"
        )

        self.creds_status = None
        self.deadline_authorized = None
        self.__refresh_id += 1
        self.__refresh_thread = threading.Thread(
            target=self._refresh_status_background_thread,
            name="Amazon Deadline Cloud Status Refresh Thread",
            args=(self.__refresh_id,),
        )
        self.__refresh_thread.start()

    def handle_refresh_thread_update(self, refresh_id) -> None:
        """
        Handles the `refresh_thread_update` signal that the background status refresh
        thread sends.
        """
        if refresh_id == self.__refresh_id:
            if self.creds_status is not None:
                color = (
                    "green"
                    if self.creds_status == api.AwsCredentialsStatus.AUTHENTICATED
                    else "red"
                )
                self.creds_status_label.setText(
                    f"Status: <b style='color:{color};'>{self.creds_status.name}</b>"
                )

            if self.deadline_authorized is not None:
                color = "green" if self.deadline_authorized else "red"
                message = "AUTHORIZED" if self.deadline_authorized else "UNAVAILABLE"
                self.deadline_authorized_label.setText(
                    f"Amazon Deadline Cloud API:  <b style='color:{color};'>{message}</b>"
                )
