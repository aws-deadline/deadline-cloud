# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Provides a widget to place in Amazon Deadline Cloud submitter dialogs, that shows
the current status of Amazon Deadline Cloud credentials and API.
The current stauts is handled by DeadlineCredentialstatus.
"""
from logging import getLogger

from PySide2.QtWidgets import (  # pylint: disable=import-error; type: ignore
    QFrame,
    QHBoxLayout,
    QLabel,
    QSizePolicy,
    QWidget,
)

from ... import api
from ...cli.deadline_credentials_status import DeadlineCredentialsStatus

logger = getLogger(__name__)


class DeadlineCredentialsStatusWidget(QWidget):
    """
    A Widget that displays status information about Amazon Deadline Cloud
    credentials from a DeadlineCredentialStatus object.
    """

    def __init__(self, parent=None) -> None:
        super().__init__(parent=parent)

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
            text = "<refreshing>"
        elif self._status.creds_type == api.AwsCredentialsType.NOT_VALID:
            color = "red"
            text = self._status.creds_type.name
        else:
            color = "green"
            text = self._status.creds_type.name
        self.creds_type_label.setText(f"Creds: <b style='color:{color};'>{text}</b>")

    def _creds_status_changed(self) -> None:
        if self._status.creds_status is None:
            color = "white"
            text = "<refreshing>"
        elif self._status.creds_status == api.AwsCredentialsStatus.AUTHENTICATED:
            color = "green"
            text = self._status.creds_status.name
        else:
            color = "red"
            text = self._status.creds_status.name
        self.creds_status_label.setText(f"Status: <b style='color:{color};'>{text}</b>")

    def _api_availability_changed(self) -> None:
        if self._status.api_availability is None:
            color = "white"
            text = "<refreshing>"
        elif self._status.api_availability:
            color = "green"
            text = "AUTHORIZED"
        else:
            color = "red"
            text = "UNAVAILABLE"
        self.deadline_authorized_label.setText(
            f"Amazon Deadline Cloud API: <b style='color:{color};'>{text}</b>"
        )
