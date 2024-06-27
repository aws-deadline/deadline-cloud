# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""UI Components for the Render Submitter"""
from __future__ import annotations

import logging
import os
import sys
from typing import Any, Dict, Optional

from qtpy.QtCore import QSize, Qt  # pylint: disable=import-error
from qtpy.QtGui import QKeyEvent  # pylint: disable=import-error
from qtpy.QtWidgets import (  # pylint: disable=import-error; type: ignore
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
from ..deadline_authentication_status import DeadlineAuthenticationStatus
from .. import block_signals
from ...config import get_setting
from ...config.config_file import str2bool
from ...exceptions import UserInitiatedCancel
from ...job_bundle import create_job_history_bundle_dir
from ...job_bundle.submission import AssetReferences
from ..widgets.deadline_authentication_status_widget import DeadlineAuthenticationStatusWidget
from ..widgets.job_attachments_tab import JobAttachmentsWidget
from ..widgets.shared_job_settings_tab import SharedJobSettingsWidget
from ..widgets.host_requirements_tab import HostRequirementsWidget
from . import DeadlineConfigDialog, DeadlineLoginDialog
from ._types import JobBundlePurpose

logger = logging.getLogger(__name__)

# initialize early so once the UI opens, things are already initialized
DeadlineAuthenticationStatus.getInstance()


class SubmitJobToDeadlineDialog(QDialog):
    """
    A widget containing all the standard tabs for submitting an AWS Deadline Cloud job.

    If you're using this dialog within an application and want it to stay in front,
    pass f=Qt.Tool, a flag that tells it to do that.

    Args:
        job_setup_widget_type (QWidget): The type of the widget for the job-specific settings.
        initial_job_settings (dataclass): A dataclass containing the initial job settings
        initial_shared_parameter_values (dict[str, Any]): A dict of parameter values {<name>, <value>, ...}
            to override default queue parameter values from the queue. For example,
            a Rez queue environment may have a default "" for the RezPackages parameter, but a Maya
            submitter would override that default with "maya-2023" or similar.
        auto_detected_attachments (FlatAssetReferences): The job attachments that were automatically detected
            from the input document/scene file or starting job bundle.
        attachments: (FlatAssetReferences): The job attachments that have been added to the job by the user.
        on_create_job_bundle_callback: A function to call when the dialog needs to create a Job Bundle. It
            is called with arguments (widget, job_bundle_dir, settings, queue_parameters, asset_references)
        parent: parent of the widget
        f: Qt Window Flags
        show_host_requirements_tab: Display the host requirements tab in dialog if set to True. Default
            to False.
    """

    def __init__(
        self,
        *,
        job_setup_widget_type: type[QWidget],
        initial_job_settings,
        initial_shared_parameter_values: dict[str, Any],
        auto_detected_attachments: AssetReferences,
        attachments: AssetReferences,
        on_create_job_bundle_callback,
        parent=None,
        f=Qt.WindowFlags(),
        show_host_requirements_tab=False,
    ):
        # The Qt.Tool flag makes sure our widget stays in front of the main application window
        super().__init__(parent=parent, f=f)
        self.setWindowTitle("Submit to AWS Deadline Cloud")
        self.setMinimumSize(400, 400)

        self.job_settings_type = type(initial_job_settings)
        self.on_create_job_bundle_callback = on_create_job_bundle_callback
        self.create_job_response: Optional[Dict[str, Any]] = None
        self.job_history_bundle_dir: Optional[str] = None
        self.deadline_authentication_status = DeadlineAuthenticationStatus.getInstance()
        self.show_host_requirements_tab = show_host_requirements_tab

        self._build_ui(
            job_setup_widget_type,
            initial_job_settings,
            initial_shared_parameter_values,
            auto_detected_attachments,
            attachments,
        )

        self.gui_update_counter: Any = None
        self.refresh_deadline_settings()

    def sizeHint(self):
        return QSize(540, 600)

    def refresh(
        self,
        *,
        job_settings: Optional[Any] = None,
        auto_detected_attachments: Optional[AssetReferences] = None,
        attachments: Optional[AssetReferences] = None,
    ):
        # Refresh the UI components
        self.refresh_deadline_settings()
        if (auto_detected_attachments is not None) or (attachments is not None):
            self.job_attachments.refresh_ui(auto_detected_attachments, attachments)

        if job_settings is not None:
            self.job_settings_type = type(job_settings)
            # Refresh shared job settings
            self.shared_job_settings.refresh_ui(job_settings)
            # Refresh job specific settings
            if hasattr(self.job_settings, "refresh_ui"):
                self.job_settings.refresh_ui(job_settings)

    def _build_ui(
        self,
        job_setup_widget_type,
        initial_job_settings,
        initial_shared_parameter_values,
        auto_detected_attachments: AssetReferences,
        attachments: AssetReferences,
    ):
        self.lyt = QVBoxLayout(self)
        self.lyt.setContentsMargins(5, 5, 5, 5)

        man_layout = QFormLayout()
        self.lyt.addLayout(man_layout)
        self.tabs = QTabWidget()
        self.lyt.addWidget(self.tabs)

        self._build_shared_job_settings_tab(initial_job_settings, initial_shared_parameter_values)
        self._build_job_settings_tab(job_setup_widget_type, initial_job_settings)
        self._build_job_attachments_tab(auto_detected_attachments, attachments)

        # Show host requirements only if requested by the constructor
        if self.show_host_requirements_tab:
            self._build_host_requirements_tab()

        self.auth_status_box = DeadlineAuthenticationStatusWidget(self)
        self.lyt.addWidget(self.auth_status_box)
        self.deadline_authentication_status.api_availability_changed.connect(
            self.refresh_deadline_settings
        )

        # Refresh the submit button enable state once queue parameter status changes
        self.shared_job_settings.valid_parameters.connect(self._set_submit_button_state)

        self.button_box = QDialogButtonBox(Qt.Horizontal)
        self.login_button = QPushButton("Login")
        self.login_button.clicked.connect(self.on_login)
        self.button_box.addButton(self.login_button, QDialogButtonBox.ResetRole)
        self.logout_button = QPushButton("Logout")
        self.logout_button.clicked.connect(self.on_logout)
        self.button_box.addButton(self.logout_button, QDialogButtonBox.ResetRole)
        self.settings_button = QPushButton("Settings...")
        self.settings_button.clicked.connect(self.on_settings_button_clicked)
        self.button_box.addButton(self.settings_button, QDialogButtonBox.ResetRole)
        self.submit_button = QPushButton("Submit")
        self.submit_button.clicked.connect(self.on_submit)
        self.button_box.addButton(self.submit_button, QDialogButtonBox.AcceptRole)
        self.export_bundle_button = QPushButton("Export bundle")
        self.export_bundle_button.clicked.connect(self.on_export_bundle)
        self.button_box.addButton(self.export_bundle_button, QDialogButtonBox.AcceptRole)

        self.lyt.addWidget(self.button_box)

    def _set_submit_button_state(self):
        # Enable/disable the Submit button based on whether the
        # AWS Deadline Cloud API is accessible and the farm+queue are configured.
        enable = (
            self.deadline_authentication_status.api_availability is True
            and get_setting("defaults.farm_id") != ""
            and get_setting("defaults.queue_id") != ""
            and self.shared_job_settings.is_queue_valid()
        )

        self.submit_button.setEnabled(enable)

        if not enable:
            self.submit_button.setToolTip(
                "Cannot submit job to Deadline Cloud. Nonvalid credentials or queue parameters."
            )
        else:
            self.submit_button.setToolTip("")

    def refresh_deadline_settings(self):
        # Enable/disable the Login and Logout buttons based on whether
        # the configured profile is for Deadline Cloud monitor
        self.login_button.setEnabled(
            self.deadline_authentication_status.creds_source
            == api.AwsCredentialsSource.DEADLINE_CLOUD_MONITOR_LOGIN
        )
        self.logout_button.setEnabled(
            self.deadline_authentication_status.creds_source
            == api.AwsCredentialsSource.DEADLINE_CLOUD_MONITOR_LOGIN
        )

        self._set_submit_button_state()

        self.shared_job_settings.deadline_cloud_settings_box.refresh_setting_controls(
            self.deadline_authentication_status.api_availability is True
        )
        # If necessary, this reloads the queue parameters
        self.shared_job_settings.refresh_queue_parameters()

    def keyPressEvent(self, event: QKeyEvent) -> None:
        """
        Override to capture any enter/return key presses so that the Submit
        button isn't "pressed" when the enter/return key is.
        """
        if event.key() == Qt.Key_Return or event.key() == Qt.Key_Enter:
            return
        super().keyPressEvent(event)

    def _build_shared_job_settings_tab(self, initial_job_settings, initial_shared_parameter_values):
        self.shared_job_settings_tab = QScrollArea()
        self.tabs.addTab(self.shared_job_settings_tab, "Shared job settings")
        self.shared_job_settings = SharedJobSettingsWidget(
            initial_settings=initial_job_settings,
            initial_shared_parameter_values=initial_shared_parameter_values,
            parent=self,
        )
        self.shared_job_settings.parameter_changed.connect(self.on_shared_job_parameter_changed)
        self.shared_job_settings_tab.setWidget(self.shared_job_settings)
        self.shared_job_settings_tab.setWidgetResizable(True)
        self.shared_job_settings.parameter_changed.connect(self.on_shared_job_parameter_changed)

    def _build_job_settings_tab(self, job_setup_widget_type, initial_job_settings):
        self.job_settings_tab = QScrollArea()
        self.tabs.addTab(self.job_settings_tab, "Job-specific settings")
        self.job_settings_tab.setWidgetResizable(True)

        self.job_settings = job_setup_widget_type(
            initial_settings=initial_job_settings, parent=self
        )
        self.job_settings_tab.setWidget(self.job_settings)
        if hasattr(self.job_settings, "parameter_changed"):
            self.job_settings.parameter_changed.connect(self.on_job_template_parameter_changed)

    def _build_job_attachments_tab(
        self, auto_detected_attachments: AssetReferences, attachments: AssetReferences
    ):
        self.job_attachments_tab = QScrollArea()
        self.tabs.addTab(self.job_attachments_tab, "Job attachments")
        self.job_attachments = JobAttachmentsWidget(
            auto_detected_attachments, attachments, parent=self
        )
        self.job_attachments_tab.setWidget(self.job_attachments)
        self.job_attachments_tab.setWidgetResizable(True)

    def _build_host_requirements_tab(self):
        self.host_requirements = HostRequirementsWidget()
        self.host_requirements_tab = QScrollArea()
        self.tabs.addTab(self.host_requirements_tab, "Host requirements")
        self.host_requirements_tab.setWidget(self.host_requirements)
        self.host_requirements_tab.setWidgetResizable(True)

    def on_shared_job_parameter_changed(self, parameter: dict[str, Any]):
        """
        Handles an edit to a shared job parameter, for example one of the
        queue parameters.

        When a queue parameter and a job template parameter have
        the same name, we update between them to keep them consistent.
        """
        try:
            if hasattr(self.job_settings, "set_parameter_value"):
                with block_signals(self.job_settings):
                    self.job_settings.set_parameter_value(parameter)
        except KeyError:
            # If there is no corresponding job template parameter,
            # just ignore it.
            pass

    def on_job_template_parameter_changed(self, parameter: dict[str, Any]):
        """
        Handles an edit to a job template parameter.

        When a queue parameter and a job template parameter have
        the same name, we update between them to keep them consistent.
        """
        try:
            with block_signals(self.shared_job_settings):
                self.shared_job_settings.set_parameter_value(parameter)
        except KeyError:
            # If there is no corresponding queue parameter,
            # just ignore it.
            pass

    def on_login(self):
        DeadlineLoginDialog.login(parent=self)
        self.refresh_deadline_settings()
        # This widget watches the auth files, but that does
        # not always catch a change so force a refresh here.
        self.deadline_authentication_status.refresh_status()

    def on_logout(self):
        api.logout()
        self.refresh_deadline_settings()
        # This widget watches the auth files, but that does
        # not always catch a change so force a refresh here.
        self.deadline_authentication_status.refresh_status()

    def on_settings_button_clicked(self):
        if DeadlineConfigDialog.configure_settings(parent=self):
            self.refresh_deadline_settings()

    def on_export_bundle(self):
        """
        Exports a Job Bundle, but does not submit the job.
        """
        # Retrieve all the settings into the dataclass
        settings = self.job_settings_type()
        self.shared_job_settings.update_settings(settings)
        self.job_settings.update_settings(settings)

        queue_parameters = self.shared_job_settings.get_parameters()

        asset_references = self.job_attachments.get_asset_references()

        # Save the bundle
        try:
            self.job_history_bundle_dir = create_job_history_bundle_dir(
                settings.submitter_name, settings.name
            )

            if self.show_host_requirements_tab:
                requirements = self.host_requirements.get_requirements()
                self.on_create_job_bundle_callback(
                    self,
                    self.job_history_bundle_dir,
                    settings,
                    queue_parameters,
                    asset_references,
                    requirements,
                    purpose=JobBundlePurpose.EXPORT,
                )
            else:
                # Maintaining backward compatibility for submitters that do not support host_requirements yet
                self.on_create_job_bundle_callback(
                    self,
                    self.job_history_bundle_dir,
                    settings,
                    queue_parameters,
                    asset_references,
                    purpose=JobBundlePurpose.EXPORT,
                )

            logger.info(f"Saved the submission as a job bundle: {self.job_history_bundle_dir}")
            if sys.platform == "win32":
                # Open the directory in the OS's file explorer
                os.startfile(self.job_history_bundle_dir)
            QMessageBox.information(
                self,
                f"{settings.submitter_name} job submission",
                f"Saved the submission as a job bundle:\n{self.job_history_bundle_dir}",
            )
            # Close the submitter window to signal the submission is done
            self.close()
        except Exception as exc:
            logger.exception("Error saving bundle")
            message = str(exc)
            QMessageBox.warning(self, f"{settings.submitter_name} job submission", message)

    def on_submit(self):
        """
        Perform a submission when the submit button is pressed
        """
        # Unset any cached response
        self.create_job_response = None

        # Retrieve all the settings into the dataclass
        settings = self.job_settings_type()
        self.shared_job_settings.update_settings(settings)
        self.job_settings.update_settings(settings)

        queue_parameters = self.shared_job_settings.get_parameters()

        asset_references = self.job_attachments.get_asset_references()

        job_progress_dialog = SubmitJobProgressDialog(parent=self)
        job_progress_dialog.show()
        QApplication.instance().processEvents()  # type: ignore[union-attr]

        # Submit the job
        try:
            deadline = api.get_boto3_client("deadline")

            self.job_history_bundle_dir = create_job_history_bundle_dir(
                settings.submitter_name, settings.name
            )

            if self.show_host_requirements_tab:
                requirements = self.host_requirements.get_requirements()
                self.on_create_job_bundle_callback(
                    self,
                    self.job_history_bundle_dir,
                    settings,
                    queue_parameters,
                    asset_references,
                    requirements,
                    purpose=JobBundlePurpose.SUBMISSION,
                )
            else:
                # Maintaining backward compatibility for submitters that do not support host_requirements yet
                self.on_create_job_bundle_callback(
                    self,
                    self.job_history_bundle_dir,
                    settings,
                    queue_parameters,
                    asset_references,
                    purpose=JobBundlePurpose.SUBMISSION,
                )

            farm_id = get_setting("defaults.farm_id")
            queue_id = get_setting("defaults.queue_id")
            storage_profile_id = get_setting("settings.storage_profile_id")

            storage_profile = None
            if storage_profile_id:
                storage_profile = api.get_storage_profile_for_queue(
                    farm_id, queue_id, storage_profile_id, deadline
                )

            queue = deadline.get_queue(farmId=farm_id, queueId=queue_id)

            queue_role_session = api.get_queue_user_boto3_session(
                deadline=deadline,
                farm_id=farm_id,
                queue_id=queue_id,
                queue_display_name=queue["displayName"],
            )

            asset_manager: Optional[S3AssetManager] = None
            if "jobAttachmentSettings" in queue:
                asset_manager = S3AssetManager(
                    farm_id=farm_id,
                    queue_id=queue_id,
                    job_attachment_settings=JobAttachmentS3Settings(
                        **queue["jobAttachmentSettings"]
                    ),
                    session=queue_role_session,
                )

            api.get_deadline_cloud_library_telemetry_client().record_event(
                event_type="com.amazon.rum.deadline.submission",
                event_details={
                    "submitter_name": settings.submitter_name,
                },
                from_gui=True,
            )

            self.create_job_response = job_progress_dialog.start_submission(
                farm_id,
                queue_id,
                storage_profile,
                self.job_history_bundle_dir,
                queue_parameters,
                asset_manager,
                deadline,
                auto_accept=str2bool(get_setting("settings.auto_accept")),
                require_paths_exist=self.job_attachments.get_require_paths_exist(),
            )
        except UserInitiatedCancel as uic:
            logger.info("Canceling submission.")
            QMessageBox.information(self, f"{settings.submitter_name} job submission", str(uic))
            job_progress_dialog.close()
        except Exception as exc:
            logger.exception("error submitting job")
            api.get_deadline_cloud_library_telemetry_client().record_error(
                event_details={"exception_scope": "on_submit"},
                exception_type=str(type(exc)),
                from_gui=True,
            )
            QMessageBox.warning(self, f"{settings.submitter_name} job submission", str(exc))
            job_progress_dialog.close()

        if self.create_job_response:
            # Close the submitter window to signal the submission is done but
            # keep the standalone gui submitter open
            if settings.submitter_name != "JobBundle":
                self.close()
