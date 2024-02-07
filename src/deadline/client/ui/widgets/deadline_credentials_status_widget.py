# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Provides a widget to place in Amazon Deadline Cloud submitter dialogs, that shows
the current status of Amazon Deadline Cloud credentials and API.
The current stauts is handled by DeadlineCredentialstatus.
"""
from logging import getLogger
from typing import Optional

from PySide2.QtCore import Qt
from PySide2.QtWidgets import (  # pylint: disable=import-error; type: ignore
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QWidget,
)

from ... import api
from ..deadline_credentials_status import DeadlineCredentialsStatus

logger = getLogger(__name__)


class DeadlineCredentialsStatusWidget(QWidget):
    """
    A Widget that displays status information about Amazon Deadline Cloud
    credentials from a DeadlineCredentialStatus object.
    """

    def __init__(self, parent=None) -> None:
        super().__init__(parent=parent)

        layout = QHBoxLayout(self)

        self.creds_type_group = CredentialsStatusGroup(title="Credentials Type", parent=self)
        layout.addWidget(self.creds_type_group)
        self.creds_status_group = CredentialsStatusGroup(title="Credentials Status", parent=self)
        layout.addWidget(self.creds_status_group)
        self.deadline_authorized_group = CredentialsStatusGroup(
            title="Amazon Deadline Cloud API", parent=self
        )
        layout.addWidget(self.deadline_authorized_group)

        self._status = DeadlineCredentialsStatus.getInstance()
        self._status.creds_type_changed.connect(self._creds_type_changed)
        self._status.creds_status_changed.connect(self._creds_status_changed)
        self._status.api_availability_changed.connect(self._api_availability_changed)

        # Update with current values
        self._creds_type_changed()
        self._creds_status_changed()
        self._api_availability_changed()

    def _creds_type_changed(self) -> None:
        if self._status.creds_type is None:
            color = "white"
            text = "&lt;Refreshing&gt;"
        elif self._status.creds_type == api.AwsCredentialsType.NOT_VALID:
            color = "red"
            text = self._status.creds_type.name
        else:
            color = "green"
            text = self._status.creds_type.name
        self.creds_type_group.label.setText(f"<b style='color:{color};'>{text}</b>")

    def _creds_status_changed(self) -> None:
        if self._status.creds_status is None:
            color = "white"
            text = "&lt;Refreshing&gt;"
        elif self._status.creds_status == api.AwsCredentialsStatus.AUTHENTICATED:
            color = "green"
            text = self._status.creds_status.name
        else:
            color = "red"
            text = self._status.creds_status.name
        self.creds_status_group.label.setText(f"<b style='color:{color};'>{text}</b>")

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


class CredentialsStatusGroup(QGroupBox):
    """
    UI element to group the status of credentials.
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
