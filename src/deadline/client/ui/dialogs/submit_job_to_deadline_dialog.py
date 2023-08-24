# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""UI Components for the Render Submitter"""
from __future__ import annotations

import logging
import os
import sys
from typing import Any, Dict, Optional

from PySide2.QtCore import QSize, Qt  # pylint: disable=import-error
from PySide2.QtGui import QKeyEvent  # pylint: disable=import-error
from PySide2.QtWidgets import (  # pylint: disable=import-error; type: ignore
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
from ...config import get_setting
from ...config.config_file import str2bool
from ...job_bundle import create_job_history_bundle_dir
from ..widgets.deadline_credentials_status_widget import DeadlineCredentialsStatusWidget
from ..widgets.job_attachments_tab import JobAttachmentsWidget
from ..widgets.shared_job_settings_tab import SharedJobSettingsWidget
from . import DeadlineConfigDialog, DeadlineLoginDialog
from ...job_bundle.submission import FlatAssetReferences

logger = logging.getLogger(__name__)


class SubmitJobToDeadlineDialog(QDialog):
    """
    A widget containing all the standard tabs for submitting a Amazon Deadline Cloud job.

    If you're using this dialog within an application and want it to stay in front,
    pass f=Qt.Tool, a flag that tells it to do that.

    Args:
        job_setup_widget_type: QWidget - The type of the widget for the job-specific settings.
        initial_job_settings: dataclass - A dataclass containing the initial job settings
        auto_detected_attachments (FlatAssetReferences): The job attachments that were automatically detected
            from the input document/scene file or starting job bundle.
        attachments: (FlatAssetReferences): The job attachments that have been added to the job by the user.
        on_create_job_bundle_callback: A function to call when the dialog needs to create a Job Bundle. It
            is called with arguments (settings, job_bundle_dir, asset_references)
    """

    def __init__(
        self,
        job_setup_widget_type: QWidget,
        initial_job_settings,
        auto_detected_attachments: FlatAssetReferences,
        attachments: FlatAssetReferences,
        on_create_job_bundle_callback,
        parent=None,
        f=Qt.WindowFlags(),
    ):
        # The Qt.Tool flag makes sure our widget stays in front of the main application window
        super().__init__(parent=parent, f=f)
        self.setWindowTitle("Submit to Amazon Deadline Cloud")
        self.setMinimumSize(400, 400)

        self.job_settings_type = type(initial_job_settings)
        self.on_create_job_bundle_callback = on_create_job_bundle_callback
        self.create_job_response: Optional[Dict[str, Any]] = None

        self._build_ui(
            job_setup_widget_type, initial_job_settings, auto_detected_attachments, attachments
        )

        self.gui_update_counter: Any = None
        self.refresh_deadline_settings()

    def sizeHint(self):
        return QSize(540, 600)

    def refresh_deadline_settings(self):
        # Enable/disable the Login and Logout buttons based on whether
        # the configured profile is for Cloud Companion
        self.login_button.setEnabled(
            self.creds_status_box.creds_type == api.AwsCredentialsType.CLOUD_COMPANION_LOGIN
        )
        self.logout_button.setEnabled(
            self.creds_status_box.creds_type == api.AwsCredentialsType.CLOUD_COMPANION_LOGIN
        )
        # Enable/disable the Submit button based on whether the
        # Amazon Deadline Cloud API is accessible and the farm+queue are configured.
        self.submit_button.setEnabled(
            self.creds_status_box.deadline_authorized is True
            and get_setting("defaults.farm_id") != ""
            and get_setting("defaults.queue_id") != ""
        )

        self.shared_job_settings.deadline_settings_box.refresh_setting_controls(
            self.creds_status_box.deadline_authorized
        )

    def _build_ui(
        self,
        job_setup_widget_type,
        initial_job_settings,
        auto_detected_attachments: FlatAssetReferences,
        attachments: FlatAssetReferences,
    ):
        self.lyt = QVBoxLayout(self)
        self.lyt.setContentsMargins(5, 5, 5, 5)

        man_layout = QFormLayout()
        self.lyt.addLayout(man_layout)
        self.tabs = QTabWidget()
        self.lyt.addWidget(self.tabs)

        self._build_shared_job_settings_tab(initial_job_settings)
        self._build_job_settings_tab(job_setup_widget_type, initial_job_settings)
        self._build_job_attachments_tab(auto_detected_attachments, attachments)

        self.creds_status_box = DeadlineCredentialsStatusWidget()
        self.lyt.addWidget(self.creds_status_box)
        self.creds_status_box.refresh_thread_update.connect(self.refresh_deadline_settings)

        self.button_box = QDialogButtonBox(Qt.Horizontal)
        self.login_button = QPushButton("Login")
        self.login_button.clicked.connect(self.on_login)
        self.button_box.addButton(self.login_button, QDialogButtonBox.ResetRole)
        self.logout_button = QPushButton("Logout")
        self.logout_button.clicked.connect(self.on_logout)
        self.button_box.addButton(self.logout_button, QDialogButtonBox.ResetRole)
        self.settings_button = QPushButton("Settings...")
        self.settings_button.clicked.connect(self.on_settings)
        self.button_box.addButton(self.settings_button, QDialogButtonBox.ResetRole)
        self.submit_button = QPushButton("Submit")
        self.submit_button.clicked.connect(self.on_submit)
        self.button_box.addButton(self.submit_button, QDialogButtonBox.AcceptRole)
        self.save_bundle_button = QPushButton("Save Bundle")
        self.save_bundle_button.clicked.connect(self.on_save_bundle)
        self.button_box.addButton(self.save_bundle_button, QDialogButtonBox.AcceptRole)
        self.lyt.addWidget(self.button_box)

    def keyPressEvent(self, event: QKeyEvent) -> None:
        """
        Override to capture any enter/return key presses so that the Submit
        button isn't "pressed" when the enter/return key is.
        """
        if event.key() == Qt.Key_Return or event.key() == Qt.Key_Enter:
            return
        super().keyPressEvent(event)

    def _build_shared_job_settings_tab(self, initial_job_settings):
        self.shared_job_settings_tab = QScrollArea()
        self.tabs.addTab(self.shared_job_settings_tab, "Shared Job Settings")
        self.shared_job_settings = SharedJobSettingsWidget(
            initial_settings=initial_job_settings, parent=self
        )
        self.shared_job_settings_tab.setWidget(self.shared_job_settings)
        self.shared_job_settings_tab.setWidgetResizable(True)

    def _build_job_settings_tab(self, job_setup_widget_type, initial_job_settings):
        self.job_settings_tab = QScrollArea()
        self.tabs.addTab(self.job_settings_tab, "Job-Specific Settings")
        self.job_settings_tab.setWidgetResizable(True)

        self.job_settings = job_setup_widget_type(
            initial_settings=initial_job_settings, parent=self
        )
        self.job_settings_tab.setWidget(self.job_settings)

    def _build_job_attachments_tab(
        self, auto_detected_attachments: FlatAssetReferences, attachments: FlatAssetReferences
    ):
        self.job_attachments_tab = QScrollArea()
        self.tabs.addTab(self.job_attachments_tab, "Job Attachments")
        self.job_attachments = JobAttachmentsWidget(
            auto_detected_attachments, attachments, parent=self
        )
        self.job_attachments_tab.setWidget(self.job_attachments)
        self.job_attachments_tab.setWidgetResizable(True)

    def on_login(self):
        DeadlineLoginDialog.login(parent=self)
        self.refresh_deadline_settings()
        # This widget watches the creds files, but that does
        # not always catch a change so force a refresh here.
        self.creds_status_box.refresh_status()

    def on_logout(self):
        api.logout()
        self.refresh_deadline_settings()
        # This widget watches the creds files, but that does
        # not always catch a change so force a refresh here.
        self.creds_status_box.refresh_status()

    def on_settings(self):
        if DeadlineConfigDialog.configure_settings(parent=self):
            self.refresh_deadline_settings()

    def on_save_bundle(self):
        """
        Saves a Job Bundle, but does not submit the job.
        """
        # Retrieve all the settings into the dataclass
        settings = self.job_settings_type()
        self.shared_job_settings.deadline_settings_box.update_settings(settings)
        self.shared_job_settings.desc_box.update_settings(settings)
        self.job_settings.update_settings(settings)
        self.shared_job_settings.installation_requirements_box.update_settings(settings)

        asset_references = self.job_attachments.get_asset_references()

        # Save the bundle
        try:
            job_bundle_dir = create_job_history_bundle_dir(settings.submitter_name, settings.name)
            self.on_create_job_bundle_callback(self, settings, job_bundle_dir, asset_references)

            logger.info("Saved the submission as a job bundle:")
            logger.info(job_bundle_dir)
            if sys.platform == "win32":
                # Open the directory in the OS's file explorer
                os.startfile(job_bundle_dir)
            QMessageBox.information(
                self,
                f"{settings.submitter_name} Job Submission",
                f"Saved the submission as a job bundle:\n{job_bundle_dir}",
            )
            # Close the submitter window to signal the submission is done
            self.close()
        except Exception as exc:
            logger.exception("Error saving bundle")
            message = str(exc)
            QMessageBox.warning(self, f"{settings.submitter_name} Job Submission", message)

    def on_submit(self):
        """
        Perform a submission when the submit button is pressed
        """
        # Retrieve all the settings into the dataclass
        settings = self.job_settings_type()
        self.shared_job_settings.deadline_settings_box.update_settings(settings)
        self.shared_job_settings.desc_box.update_settings(settings)
        self.job_settings.update_settings(settings)
        self.shared_job_settings.installation_requirements_box.update_settings(settings)

        asset_references = self.job_attachments.get_asset_references()

        # Submit the job
        try:
            deadline = api.get_boto3_client("deadline")

            job_bundle_dir = create_job_history_bundle_dir(settings.submitter_name, settings.name)
            self.on_create_job_bundle_callback(self, settings, job_bundle_dir, asset_references)

            farm_id = get_setting("defaults.farm_id")
            queue_id = get_setting("defaults.queue_id")

            queue = deadline.get_queue(farmId=farm_id, queueId=queue_id)

            queue_role_session = api.get_queue_boto3_session(
                deadline=deadline,
                farm_id=farm_id,
                queue_id=queue_id,
                queue_display_name=queue["displayName"],
            )

            asset_manager = S3AssetManager(
                farm_id=farm_id,
                queue_id=queue_id,
                job_attachment_settings=JobAttachmentS3Settings(**queue["jobAttachmentSettings"]),
                session=queue_role_session,
            )

            self.create_job_response = SubmitJobProgressDialog.start_submission(
                farm_id,
                queue_id,
                job_bundle_dir,
                asset_manager,
                deadline,
                auto_accept=str2bool(get_setting("settings.auto_accept")),
                parent=self,
            )
        except Exception as exc:
            logger.exception("error submitting job")
            QMessageBox.warning(self, f"{settings.submitter_name} Job Submission", str(exc))

        if self.create_job_response:
            # Close the submitter window to signal the submission is done
            self.close()
