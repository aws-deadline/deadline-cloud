# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Provides an object that can be used to track current status of Amazon Deadline Cloud 
credentials.

The object emits the following Qt Signals:
   aws_creds_changed: The AWS credentials in ~/.aws changed.
   deadline_config_changed: The Amazon Deadline Cloud configuration in ~/.deadline changed.
   creds_type_changed: triggered when credential type changes
   creds_status_changed: triggered when credential status changes
   api_availability_changed: triggered when api availability changes

The status includes three parts:
  1. Are credentials configured and available for use?
     This is checked with an sts:GetCallerIdentity AWS API call.
  2. Do the credentials grant access to Amazon Deadline Cloud APIs?
     This is checked with a simplified deadline:ListFarms AWS API call.
  3. Do the credentials use Deadline Cloud Monitor?
     This is checked by looking for the relevant properties
     in the AWS profile configuration.
"""
import os
import threading
from configparser import ConfigParser
from logging import getLogger
from typing import Optional

from PySide2.QtCore import QObject, QFileSystemWatcher, Signal

from .. import api
from ..config import config_file

logger = getLogger(__name__)

_deadline_credential_status = None


class DeadlineCredentialsStatus(QObject):
    """
    Holds status information about Amazon Deadline Cloud credentials.
    Currently status values are available as properties:

       status.creds_type: result of api.get_credentials_type()
       status.creds_status: result of api.check_credentials_status()
       status.api_availability: result of api.check_deadline_api_available()

    To initialize the status of a non-default Amazon Deadline Cloud configuration, pass in
    an Amazon Deadline Cloud configuration object to config, call set_config to change it.
    """

    # This signal is sent when an AWS credential changes (e.g. config file)
    aws_creds_changed = Signal()
    # This signal is sent when the Amazon Deadline Cloud configuration changes
    deadline_config_changed = Signal()

    # This signal is sent when an AWS credential type changes
    creds_type_changed = Signal()
    # This signal is sent when an AWS credential status changes
    creds_status_changed = Signal()
    # This signal is sent when AWS Deadline Cloud API availability changes
    api_availability_changed = Signal()

    @staticmethod
    def getInstance():
        global _deadline_credential_status
        if _deadline_credential_status is None:
            _deadline_credential_status = DeadlineCredentialsStatus()
        return _deadline_credential_status

    def __init__(self, parent=None) -> None:
        super(DeadlineCredentialsStatus, self).__init__(parent)

        self.__creds_type: Optional[api.AwsCredentialsType] = None
        self.__creds_status: Optional[api.AwsCredentialsStatus] = None
        self.__api_availability: Optional[bool] = None

        # Load the default config
        self.config = ConfigParser()
        self.config.read_dict(config_file.read_config())

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
        self.config = ConfigParser()
        if config:
            self.config.read_dict(config)
        else:
            self.config.read_dict(config_file.read_config())

        if creds_config_changed:
            self.refresh_status()

    @property
    def creds_type(self) -> Optional[api.AwsCredentialsType]:
        return self.__creds_type

    @property
    def creds_status(self) -> Optional[api.AwsCredentialsStatus]:
        return self.__creds_status

    @property
    def api_availability(self) -> Optional[bool]:
        return self.__api_availability

    def files_changed(self, changed_path) -> None:
        # Force the cached boto3 session to refresh, since we don't check the creds
        # file
        if changed_path in self.aws_creds_paths:
            logger.info(f"Path {changed_path} changed, refreshing credential status")
            # Send it to another thread to avoid blocking the Qt event loop
            self.session_thread = threading.Thread(
                target=self._get_session, kwargs={"changed_path": changed_path}
            )
            self.session_thread.start()
        else:
            logger.info(f"Path {changed_path} changed, does not affect credential status")

    def _get_session(self, changed_path):
        api.get_boto3_session(force_refresh=True)
        self.refresh_status()

        if changed_path in self.aws_creds_paths:
            self.aws_creds_changed.emit()
        elif changed_path in self.deadline_config_paths:
            self.deadline_config_changed.emit()

    def _refresh_creds_type(self) -> None:
        self.__creds_type = None
        self.creds_type_changed.emit()
        self.__creds_type = api.get_credentials_type(config=self.config)
        self.creds_type_changed.emit()

    def _refresh_creds_status(self) -> None:
        self.__creds_status = None
        self.creds_status_changed.emit()
        try:
            self.__creds_status = api.check_credentials_status(config=self.config)
        except BaseException as e:
            logger.exception(e)
            self.__creds_status = api.AwsCredentialsStatus.CONFIGURATION_ERROR
        self.creds_status_changed.emit()

    def _refresh_api_availability(self) -> None:
        self.__api_availability = None
        self.api_availability_changed.emit()
        try:
            self.__api_availability = api.check_deadline_api_available(config=self.config)
        except BaseException as e:
            logger.exception(e)
            self.__api_availability = False
        self.api_availability_changed.emit()

    def refresh_status(self) -> None:
        """
        Initiates an asynchronous status refresh.
        """
        self.__creds_type_thread = threading.Thread(target=self._refresh_creds_type)
        self.__creds_type_thread.start()
        self.__creds_status_thread = threading.Thread(target=self._refresh_creds_status)
        self.__creds_status_thread.start()
        self.__api_availability_thread = threading.Thread(target=self._refresh_api_availability)
        self.__api_availability_thread.start()
