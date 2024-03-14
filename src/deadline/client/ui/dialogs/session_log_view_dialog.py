# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Provides a dialog for viewing Deadline Cloud session logs.
"""
from __future__ import annotations

import logging
import os
import sys
from typing import Any, Dict, Optional

from PySide2.QtCore import QSize, Qt  # pylint: disable=import-error
from PySide2.QtGui import QKeyEvent  # pylint: disable=import-error
from PySide2.QtWidgets import (  # pylint: disable=import-error; type: ignore
    QApplication,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from deadline.client.ui.dialogs.submit_job_progress_dialog import SubmitJobProgressDialog
from deadline.job_attachments.models import JobAttachmentS3Settings
from deadline.job_attachments.upload import S3AssetManager

from ... import api
from .. import block_signals
from ...config import get_setting
from ..widgets.cloud_watch_log_view_widget import CloudWatchLogViewWidget

logger = logging.getLogger(__name__)


class SessionLogViewDialog(QDialog):
    """
    A dialog containing a virtual scrolling view of a session log.
    """

    def __init__(
        self,
        *,
        boto3_deadline_client,
        boto3_logs_client,
        parent=None,
        f=Qt.WindowFlags(),
    ):
        # The Qt.Tool flag makes sure our widget stays in front of the main application window
        super().__init__(parent=parent, f=f)
        self.setWindowTitle("Session Log View")
        self.setMinimumSize(400, 400)

        self.boto3_deadline_client = boto3_deadline_client
        self.boto3_logs_client = boto3_logs_client

        self._build_ui()

    def sizeHint(self):
        return QSize(940, 600)

    def _build_ui(
        self,
    ):
        self.layout = QVBoxLayout(self)
        # self.layout.setContentsMargins(5, 5, 5, 5)

        self.logview = CloudWatchLogViewWidget(parent=self, boto3_deadline_client=self.boto3_deadline_client, boto3_logs_client=self.boto3_logs_client)
        self.layout.addWidget(self.logview)
