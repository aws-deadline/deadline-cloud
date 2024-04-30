# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Provides a widget to place in AWS Deadline Cloud submitter dialogs, that shows
the current status of AWS Deadline Cloud authentication and API.
The current status is handled by DeadlineAuthenticationStatus.
"""
from logging import getLogger
from typing import Optional

from qtpy.QtCore import Qt
from qtpy.QtWidgets import (  # pylint: disable=import-error; type: ignore
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QWidget,
)

from ... import api
from ..deadline_authentication_status import DeadlineAuthenticationStatus

logger = getLogger(__name__)


class DeadlineAuthenticationStatusWidget(QWidget):
    """
    A Widget that displays status information about AWS Deadline Cloud
    authentication from a DeadlineAuthenticationStatus object.
    """

    def __init__(self, parent=None) -> None:
        super().__init__(parent=parent)

        layout = QHBoxLayout(self)

        self.creds_source_group = AuthenticationStatusGroup(title="Credential source", parent=self)
        layout.addWidget(self.creds_source_group)
        self.auth_status_group = AuthenticationStatusGroup(
            title="Authentication status", parent=self
        )
        layout.addWidget(self.auth_status_group)
        self.deadline_authorized_group = AuthenticationStatusGroup(
            title="AWS Deadline Cloud API", parent=self
        )
        layout.addWidget(self.deadline_authorized_group)

        self._status = DeadlineAuthenticationStatus.getInstance()
        self._status.creds_source_changed.connect(self._creds_source_changed)
        self._status.auth_status_changed.connect(self._auth_status_changed)
        self._status.api_availability_changed.connect(self._api_availability_changed)

        # Update with current values
        self._creds_source_changed()
        self._auth_status_changed()
        self._api_availability_changed()

    def _creds_source_changed(self) -> None:
        if self._status.creds_source is None:
            color = "white"
            text = "&lt;Refreshing&gt;"
        elif self._status.creds_source == api.AwsCredentialsSource.NOT_VALID:
            color = "red"
            text = self._status.creds_source.name
        else:
            color = "green"
            text = self._status.creds_source.name
        self.creds_source_group.label.setText(f"<b style='color:{color};'>{text}</b>")

    def _auth_status_changed(self) -> None:
        if self._status.auth_status is None:
            color = "white"
            text = "&lt;Refreshing&gt;"
        elif self._status.auth_status == api.AwsAuthenticationStatus.AUTHENTICATED:
            color = "green"
            text = self._status.auth_status.name
        else:
            color = "red"
            text = self._status.auth_status.name
        self.auth_status_group.label.setText(f"<b style='color:{color};'>{text}</b>")

    def _api_availability_changed(self) -> None:
        if self._status.api_availability is None:
            color = "white"
            text = "&lt;Refreshing&gt;"
        elif self._status.api_availability:
            color = "green"
            text = "AUTHORIZED"
        else:
            color = "red"
            text = "UNAVAILABLE"
        self.deadline_authorized_group.label.setText(f"<b style='color:{color};'>{text}</b>")


class AuthenticationStatusGroup(QGroupBox):
    """
    UI element to group the status of authentication.
    """

    def __init__(self, *, title: str, parent: Optional[QWidget] = None):
        super().__init__(parent=parent, title=title)

        self._build_ui()

    def _build_ui(self):
        self.layout = QFormLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.layout.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)
        self.label = QLabel("")

        self.label.setAlignment(Qt.AlignCenter)
        self.layout.addWidget(self.label)
