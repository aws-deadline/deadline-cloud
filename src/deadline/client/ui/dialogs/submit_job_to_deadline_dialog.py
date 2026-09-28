# coding: utf-8
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""UI Components for the Render Submitter"""

from __future__ import annotations

import json
import logging
import os
import shutil
from typing import Any, Dict, Optional, Protocol
import yaml

from qtpy.QtCore import QSize, Qt, QThread, QTimer, Signal as _Signal  # pylint: disable=import-error
from qtpy.QtGui import QKeyEvent  # pylint: disable=import-error
from qtpy.QtWidgets import (  # pylint: disable=import-error; type: ignore
    QApplication,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QLabel,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QScrollArea,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from .submit_job_progress_dialog import SubmitJobProgressDialog

from ..dataclasses import HostRequirements
from ...dataclasses import SubmitterInfo
from ... import api
from ...api._session import session_context as _session_context
from ..deadline_authentication_status import DeadlineAuthenticationStatus
from .._utils import block_signals, tr
from ...config import get_setting, set_setting, config_file
from ...config.config_file import _SETTING_FARM_ID, _SETTING_QUEUE_ID
from ...exceptions import UserInitiatedCancel, NonValidInputError
from ...job_bundle import create_job_history_bundle_dir
from ...job_bundle.loader import is_job_bundle_dir as _is_job_bundle_dir
from ...job_bundle.parameters import JobParameter
from ...job_bundle.submission import AssetReferences
from ...job_bundle._repository import (
    S3BundleRepository as _S3BundleRepository,
    archive_bundle_dir as _archive_bundle_dir,
    get_bundle_dir_size as _get_bundle_dir_size,
    sanitize_bundle_name as _sanitize_bundle_name,
)
from ..widgets.deadline_authentication_status_widget import DeadlineAuthenticationStatusWidget
from ..widgets.job_attachments_tab import JobAttachmentsWidget
from ..widgets.shared_job_settings_tab import SharedJobSettingsWidget
from ..widgets.host_requirements_tab import HostRequirementsWidget
from . import DeadlineConfigDialog, DeadlineLoginDialog
from ._types import JobBundlePurpose
from ._help_dialog import _HelpDialog
from .export_bundle_dialog import ExportBundleDialog as _ExportBundleDialog

logger = logging.getLogger(__name__)


# initialize early so once the UI opens, things are already initialized
DeadlineAuthenticationStatus.getInstance()


class OnCreateJobBundleCallback(Protocol):
    """This protocol defines the callback for creating a job bundle in the SubmitJobToDeadlineDialog."""

    def __call__(
        self,
        widget: SubmitJobToDeadlineDialog,
        job_bundle_dir: str,
        settings: Any,
        queue_parameters: list[JobParameter],
        asset_references: AssetReferences,
        host_requirements: Optional[Dict[str, Any]] = None,
        *,
        purpose: JobBundlePurpose,
    ) -> Optional[dict[str, Any]]: ...


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
        auto_detected_attachments (AssetReferences): The job attachments that were automatically detected
            from the input document/scene file or starting job bundle.
        attachments (AssetReferences): The job attachments that have been added to the job by the user.
        on_create_job_bundle_callback (OnCreateJobBundleCallback): A function to call when the dialog
            needs to create a Job Bundle. It is called with arguments:
            (widget, job_bundle_dir, settings, queue_parameters, asset_references, host_requirements, purpose).
            It can return either None or a dict with parameters about the submission. Currently,
            the additional parameters supported are:
            {
                # See documentation for deadline.client.api.create_job_from_job_bundle about these parameters
                "job_parameters": [{"name": "ParameterName", "value": "Parameter Value", ...}],
                "known_asset_paths": ["/path/1", ...],
            }
        parent: parent of the widget
        f: Qt Window Flags
        show_host_requirements_tab: Display the host requirements tab in dialog if set to True. Default
            to False.
        submitter_info (SubmitterInfo): Information related to the submitter window and application it's running in
        use_deadline_cloud_v2_channel (bool): When True, prepend the "deadline-cloud-v2" Conda
            channel ahead of the default "deadline-cloud" channel in the CondaChannels queue
            parameter as it loads from the queue. The "deadline-cloud" channel is kept as a
            fallback and other channels are left untouched. Defaults to False (no change).
    """

    def __init__(
        self,
        *,
        job_setup_widget_type: type[QWidget],
        initial_job_settings: Any,
        initial_shared_parameter_values: dict[str, Any],
        auto_detected_attachments: AssetReferences,
        attachments: AssetReferences,
        on_create_job_bundle_callback: OnCreateJobBundleCallback,
        parent: Optional[QWidget] = None,
        f: Any = Qt.WindowFlags(),
        show_host_requirements_tab: bool = False,
        host_requirements: Optional[HostRequirements] = None,
        submitter_info: Optional[SubmitterInfo] = None,
        known_asset_paths: Optional[list[str]] = None,
        use_deadline_cloud_v2_channel: bool = False,
    ):
        # The Qt.Tool flag makes sure our widget stays in front of the main application window
        super().__init__(parent=parent, f=f)

        # Set window title with submitter package info if available
        window_title = tr("Submit to AWS Deadline Cloud")
        if submitter_info:
            # e.g. Deadline Cloud Blender Submitter x.y.z
            formatted_name = f"Deadline Cloud {submitter_info.submitter_name} {tr('Submitter')}"
            if submitter_info.submitter_package_version:
                window_title = f"{formatted_name} {submitter_info.submitter_package_version}"
            else:
                window_title = f"{formatted_name}"
        self.setWindowTitle(window_title)

        self.setMinimumSize(400, 400)

        self.job_settings_type = type(initial_job_settings)
        self.submitter_info = submitter_info or SubmitterInfo(
            submitter_name=self.job_settings_type().submitter_name
        )
        _session_context["submitter-name"] = self.submitter_info.submitter_name
        _session_context["submitter-version"] = self.submitter_info.submitter_package_version

        self.on_create_job_bundle_callback = on_create_job_bundle_callback
        self.job_id = None
        self.job_history_bundle_dir: Optional[str] = None
        self.deadline_authentication_status = DeadlineAuthenticationStatus.getInstance()
        self.show_host_requirements_tab = show_host_requirements_tab
        self.known_asset_paths = known_asset_paths or []
        self.use_deadline_cloud_v2_channel = use_deadline_cloud_v2_channel
        self.should_close = False

        self._build_ui(
            job_setup_widget_type,
            initial_job_settings,
            initial_shared_parameter_values,
            auto_detected_attachments,
            attachments,
            host_requirements,
        )

        self.gui_update_counter: Any = None
        self.refresh_deadline_settings()

    def _submission_succeeded_signal_receiver(self, job_id: str):
        self.job_id = job_id

        set_setting("defaults.job_id", job_id)

    def _close_event_receiver(self):
        if self.submitter_info.submitter_name != "JobBundle" and self.job_id:
            self.close()

    def sizeHint(self):
        return QSize(540, 700)

    def refresh(
        self,
        *,
        job_settings: Optional[Any] = None,
        auto_detected_attachments: Optional[AssetReferences] = None,
        attachments: Optional[AssetReferences] = None,
        load_new_bundle: bool = False,
    ):
        # Refresh the UI components
        self.refresh_deadline_settings()
        if (auto_detected_attachments is not None) or (attachments is not None):
            self.job_attachments.refresh_ui(auto_detected_attachments, attachments)

        if job_settings is not None:
            self.job_settings_type = type(job_settings)
            # Refresh shared job settings
            self.shared_job_settings.refresh_ui(job_settings, load_new_bundle)
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
        host_requirements: Optional[HostRequirements],
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
            self._build_host_requirements_tab(host_requirements)

        self.auth_status_box = DeadlineAuthenticationStatusWidget(self)
        self.auth_status_box.switch_profile_clicked.connect(self.on_switch_profile_clicked)
        self.auth_status_box.logout_clicked.connect(self.on_logout)
        self.auth_status_box.login_clicked.connect(self.on_login)
        self.lyt.addWidget(self.auth_status_box)
        self.deadline_authentication_status.api_availability_changed.connect(
            self.refresh_deadline_settings
        )

        # Refresh the submit button enable state once queue parameter status changes
        self.shared_job_settings.valid_parameters.connect(self._set_submit_button_state)

        self.button_box = QDialogButtonBox(Qt.Horizontal)
        self.settings_button = QPushButton(tr("Settings..."))
        self.settings_button.clicked.connect(self.on_settings_button_clicked)
        self.button_box.addButton(self.settings_button, QDialogButtonBox.ResetRole)
        self.help_button = QPushButton(tr("Help"))
        self.help_button.clicked.connect(self._on_help_button_clicked)
        self.button_box.addButton(self.help_button, QDialogButtonBox.HelpRole)
        self.submit_button = QPushButton(tr("Submit"))
        self.submit_button.clicked.connect(self.on_submit)
        self.button_box.addButton(self.submit_button, QDialogButtonBox.AcceptRole)
        if hasattr(initial_job_settings, "browse_enabled") and initial_job_settings.browse_enabled:
            self.load_bundle_button = QPushButton(tr("Load Bundle"))
            self.load_bundle_button.clicked.connect(self._on_load_bundle)
            self.button_box.addButton(self.load_bundle_button, QDialogButtonBox.AcceptRole)
        self.export_bundle_button = QPushButton(tr("Save bundle as"))
        self.export_bundle_button.clicked.connect(self.on_export_bundle)
        self.button_box.addButton(self.export_bundle_button, QDialogButtonBox.AcceptRole)

        self.lyt.addWidget(self.button_box)

    def _set_submit_button_state(self):
        # Enable/disable the Submit button based on whether the
        # AWS Deadline Cloud API is accessible and the farm+queue are configured.
        api_available = self.deadline_authentication_status.api_availability is True
        farm_configured = get_setting(_SETTING_FARM_ID) != ""
        queue_configured = get_setting(_SETTING_QUEUE_ID) != ""
        queue_valid = self.shared_job_settings.is_queue_valid()

        enable = api_available and farm_configured and queue_configured and queue_valid

        self.submit_button.setEnabled(enable)

        if not enable:
            issues = []
            if not api_available:
                issues.append(
                    tr(
                        "AWS Deadline Cloud API is not accessible. Check your authentication status."
                    )
                )
            if not farm_configured:
                issues.append(
                    tr("No farm is configured. Click Settings to select a farm for job submission.")
                )
            if not queue_configured:
                issues.append(
                    tr("No queue is configured. Click Settings to select a queue within your farm.")
                )
            if farm_configured and queue_configured and not queue_valid:
                issues.append(
                    tr("Queue parameters are not valid. Check Shared job settings tab for details.")
                )

            self.submit_button.setToolTip(
                tr("Cannot submit job:\n\n\u2022 {issues}").format(
                    issues="\n\n\u2022 ".join(issues)
                )
            )
        else:
            self.submit_button.setToolTip("")

    def refresh_deadline_settings(self):
        self._set_submit_button_state()

        # The tab's selectors refresh their lists; when a list resolves to a single
        # resource the combo auto-selects it, which the controller persists and
        # cascades (farm -> queue -> storage). That is the only auto-select path -
        # there is no separate background auto-select competing to write the same
        # settings.
        self.shared_job_settings.deadline_cloud_settings_box.refresh_setting_controls(
            self.deadline_authentication_status.api_availability is True
        )
        # If necessary, this reloads the queue parameters
        self.shared_job_settings.refresh_queue_parameters()

    def _on_deadline_cloud_selection_changed(self):
        """React to a farm/queue selection made on the Shared job settings tab.

        Only updates the Submit button state and reloads queue parameters; it does
        not re-list the resource combos (the controller has already cascaded their
        lists) so a queue change doesn't trigger a farm-list refresh.
        """
        self._set_submit_button_state()
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
        self.tabs.addTab(self.shared_job_settings_tab, tr("Shared job settings"))
        self.shared_job_settings = SharedJobSettingsWidget(
            initial_settings=initial_job_settings,
            initial_shared_parameter_values=initial_shared_parameter_values,
            use_deadline_cloud_v2_channel=self.use_deadline_cloud_v2_channel,
            parent=self,
        )
        self.shared_job_settings.parameter_changed.connect(self.on_shared_job_parameter_changed)
        self.shared_job_settings_tab.setWidget(self.shared_job_settings)
        self.shared_job_settings_tab.setWidgetResizable(True)
        self.shared_job_settings.parameter_changed.connect(self.on_shared_job_parameter_changed)
        # When the user edits the farm/queue selectors on the tab, reload queue
        # parameters and refresh the Submit button enable state. This does NOT go
        # through refresh_deadline_settings on purpose: the combos have already
        # updated their own lists, so re-listing them (and re-running auto-select)
        # would needlessly refresh the farm list when only the queue changed.
        self.shared_job_settings.deadline_cloud_settings_box.selection_changed.connect(
            self._on_deadline_cloud_selection_changed
        )

    def _build_job_settings_tab(self, job_setup_widget_type, initial_job_settings):
        self.job_settings_tab = QScrollArea()
        self.tabs.addTab(self.job_settings_tab, tr("Job-specific settings"))
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
        self.tabs.addTab(self.job_attachments_tab, tr("Job attachments"))
        self.job_attachments = JobAttachmentsWidget(
            auto_detected_attachments, attachments, parent=self
        )
        self.job_attachments_tab.setWidget(self.job_attachments)
        self.job_attachments_tab.setWidgetResizable(True)

    def _build_host_requirements_tab(self, host_requirements: Optional[HostRequirements]):
        self.host_requirements = HostRequirementsWidget()
        self.host_requirements_tab = QScrollArea()
        self.tabs.addTab(self.host_requirements_tab, tr("Host requirements"))
        self.host_requirements_tab.setWidget(self.host_requirements)
        self.host_requirements_tab.setWidgetResizable(True)
        if host_requirements:
            self.host_requirements.set_requirements(host_requirements)

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

    def on_switch_profile_clicked(self):
        if DeadlineConfigDialog.configure_settings(parent=self, set_profile_focus=True):
            self.refresh_deadline_settings()

    def on_settings_button_clicked(self):
        if DeadlineConfigDialog.configure_settings(parent=self):
            self.refresh_deadline_settings()

    def _on_help_button_clicked(self):
        """Show the Help dialog with submitter information."""
        try:
            dialog = _HelpDialog(self.submitter_info, parent=self)
            dialog.exec_()
        except Exception as e:
            logger.error(f"Failed to create HelpDialog: {e}")
            QMessageBox.critical(
                self,
                "Error",
                f"Failed to display Help dialog: {str(e)}",
            )

    def _on_load_bundle(self):
        """Delegates to the job_settings widget's on_load_bundle method."""
        if hasattr(self.job_settings, "on_load_bundle"):
            self.job_settings.on_load_bundle(s3_repo=getattr(self, "_s3_repo", None))

    def on_export_bundle(self):
        """Export a job bundle to Queue (S3) or a local directory."""
        # Gather settings
        settings = self.job_settings_type()
        self.shared_job_settings.update_settings(settings)
        self.job_settings.update_settings(settings)

        # Default export name is the bundle directory name on disk. Only
        # JobBundleSettings carries ``input_job_bundle_dir``; other submitters
        # (CLI, DCC) do not, so fall back to the job name for them.
        input_job_bundle_dir = getattr(settings, "input_job_bundle_dir", "")
        resolved_name = (
            os.path.basename(input_job_bundle_dir) if input_job_bundle_dir else settings.name
        )

        # Try to get queue repo for the dialog. ``from_config`` makes network
        # calls; show a wait cursor so the click doesn't look ignored.
        queue_repo = None
        queue_error = ""
        QApplication.setOverrideCursor(Qt.WaitCursor)
        try:
            queue_repo = _S3BundleRepository.from_config()
        except Exception as e:
            queue_error = str(e)
        finally:
            QApplication.restoreOverrideCursor()

        # Get default local directory
        local_dir = get_setting("settings.job_bundle_default_directory")
        if local_dir:
            local_dir = os.path.expanduser(local_dir)
        else:
            local_dir = os.path.expanduser("~")

        # Show export dialog
        dialog = _ExportBundleDialog(
            default_name=resolved_name,
            queue_repo=queue_repo,
            queue_error=queue_error,
            local_dir=local_dir,
            parent=self,
        )
        if dialog.exec_() != _ExportBundleDialog.Accepted or not dialog.bundle_name:
            return

        try:
            bundle_name = _sanitize_bundle_name(dialog.bundle_name)
        except ValueError:
            QMessageBox.warning(
                self,
                tr("Save bundle as"),
                f"The bundle name {dialog.bundle_name!r} is not valid. "
                "Choose a name that isn't empty and doesn't contain path separators "
                "or '..'.",
            )
            return

        # Generate the bundle with current edits applied
        import tempfile

        asset_references = self.job_attachments.get_asset_references()
        queue_parameters = self.shared_job_settings.get_parameters()
        requirements = (
            self.host_requirements.get_requirements() if self.show_host_requirements_tab else None
        )

        if dialog.export_to_queue:
            with tempfile.TemporaryDirectory() as export_dir:
                if not self._generate_export_bundle(
                    export_dir, settings, queue_parameters, asset_references, requirements
                ):
                    # Generation failed and the user was already notified. Abort
                    # rather than silently uploading the original, un-edited
                    # bundle (which would discard the user's in-dialog edits) or
                    # crashing in the upload worker with no bundle to archive.
                    return
                self._export_to_queue(queue_repo, bundle_name, export_dir)
        else:
            # The parent directory is free-form user input (the Location field is
            # editable in Local mode), so validate it before doing anything
            # destructive rather than silently creating a typo directory tree.
            if not os.path.isdir(dialog.local_directory):
                QMessageBox.warning(
                    self,
                    tr("Save bundle as"),
                    f"The location does not exist or is not a directory:\n{dialog.local_directory}",
                )
                return

            dest_path = os.path.join(dialog.local_directory, bundle_name)
            if os.path.exists(dest_path):
                # Only ever recursively replace something that is itself a job
                # bundle (the legitimate "overwrite a bundle of the same name"
                # case). Refuse to touch an arbitrary folder/file that merely
                # collides with the bundle name — otherwise a name matching an
                # existing project folder would be destroyed on a single "Yes".
                if not _is_job_bundle_dir(dest_path):
                    QMessageBox.warning(
                        self,
                        tr("Save bundle as"),
                        f"A file or folder that is not a job bundle already exists at:\n{dest_path}"
                        "\n\nChoose a different name or location.",
                    )
                    return
                reply = QMessageBox.question(
                    self,
                    tr("Save bundle as"),
                    f"Bundle '{bundle_name}' already exists at:\n{dest_path}\n\nOverwrite?",
                    QMessageBox.Yes | QMessageBox.No,
                    QMessageBox.No,
                )
                if reply != QMessageBox.Yes:
                    return

            # Generate into a staging directory on the same filesystem, then swap
            # it into place only once generation succeeds. Submitter callbacks can
            # fail (they're wrapped to return False), so generating directly into
            # dest_path would destroy an existing good bundle and leave an empty
            # directory behind. Staging alongside dest keeps the swap a fast rename.
            with tempfile.TemporaryDirectory(dir=dialog.local_directory) as staging:
                staged_bundle = os.path.join(staging, bundle_name)
                os.makedirs(staged_bundle, exist_ok=True)
                if not self._generate_export_bundle(
                    staged_bundle, settings, queue_parameters, asset_references, requirements
                ):
                    return
                if os.path.exists(dest_path):
                    shutil.rmtree(dest_path)
                shutil.move(staged_bundle, dest_path)
            QMessageBox.information(
                self,
                tr("Save bundle as"),
                f"Bundle saved to:\n{dest_path}",
            )

    def _generate_export_bundle(
        self,
        output_dir: str,
        settings,
        queue_parameters: list[JobParameter],
        asset_references: AssetReferences,
        requirements: Optional[Dict[str, Any]],
    ) -> bool:
        """Generate the job bundle (with the dialog's current edits) into ``output_dir``.

        Returns ``True`` on success. On failure, shows an error dialog and
        returns ``False`` so callers can abort instead of proceeding with a
        stale, un-edited, or missing bundle.
        """
        try:
            if self.show_host_requirements_tab:
                parameters_from_callback = self.on_create_job_bundle_callback(
                    self,
                    output_dir,
                    settings,
                    queue_parameters,
                    asset_references,
                    requirements,
                    purpose=JobBundlePurpose.EXPORT,
                )
            else:
                # Maintain backward compatibility for submitters that do not
                # support host_requirements yet (5-positional-arg callbacks).
                parameters_from_callback = self.on_create_job_bundle_callback(
                    self,
                    output_dir,
                    settings,
                    queue_parameters,
                    asset_references,
                    purpose=JobBundlePurpose.EXPORT,
                )
            # If the callback returned job parameters, persist them so the
            # exported bundle is equivalent to what submission would produce.
            job_parameters = (parameters_from_callback or {}).get("job_parameters", [])
            if job_parameters:
                self.save_job_parameters_to_job_bundle(output_dir, job_parameters)
            return True
        except Exception as exc:
            logger.warning("Failed to generate bundle for export: %s", exc)
            QMessageBox.critical(self, "Export failed", f"Failed to export bundle:\n{exc}")
            return False

    def _export_to_queue(
        self, queue_repo: Optional[_S3BundleRepository], bundle_name: str, source_dir: str
    ):
        """Archive and upload the bundle to the queue's S3 job-bundles folder."""
        from ...job_bundle._repository import build_bundle_metadata

        if not queue_repo:
            QMessageBox.critical(self, "Export failed", "Queue is not available.")
            return

        bundle_metadata = build_bundle_metadata(source_dir, bundle_name=bundle_name)

        # Archive and upload
        try:
            # Check if bundle already exists
            if queue_repo.bundle_exists(bundle_name):
                reply = QMessageBox.question(
                    self,
                    "Overwrite?",
                    f"Bundle '{bundle_name}' already exists on the queue. Overwrite?",
                    QMessageBox.Yes | QMessageBox.No,
                    QMessageBox.No,
                )
                if reply != QMessageBox.Yes:
                    return

            # Archive and upload on a background thread with progress
            class _UploadCancelled(Exception):
                """Raised inside the worker's callbacks to abort cooperatively."""

            class _UploadWorker(QThread):
                progress = _Signal(int, int)  # (current_bytes, total_bytes)
                status = _Signal(str)
                # NOTE: named ``done`` rather than ``finished`` to avoid shadowing
                # QThread's built-in ``finished`` signal.
                done = _Signal()
                error = _Signal(str)

                def __init__(self, repo, bundle_name, source_dir, metadata):
                    super().__init__()
                    self._repo = repo
                    self._bundle_name = bundle_name
                    self._source_dir = source_dir
                    self._metadata = metadata
                    self._cancelled = False

                def cancel(self):
                    # Cooperative cancel: the next archive/upload callback raises
                    # to abort, letting zipfile/boto3 unwind cleanly (boto's
                    # managed upload aborts the multipart transfer on exception,
                    # so no partial object is left on the queue).
                    self._cancelled = True

                def run(self):
                    try:
                        self.status.emit("Archiving bundle...")
                        total_size = _get_bundle_dir_size(self._source_dir)
                        self.progress.emit(0, max(1, total_size // 1024))

                        archived = [0]

                        def _on_archived(n):
                            if self._cancelled:
                                raise _UploadCancelled()
                            archived[0] += n
                            self.progress.emit(archived[0] // 1024, 0)

                        buf = _archive_bundle_dir(self._source_dir, progress_callback=_on_archived)

                        # archive_bundle_dir() returns the buffer already rewound
                        # to position 0, so buf.tell() would be 0 here. Use the
                        # buffer's byte length for the true archive size (matches
                        # the CLI upload path in bundle_group.py).
                        total = buf.getbuffer().nbytes
                        self.status.emit("Uploading bundle...")
                        self.progress.emit(0, max(1, total // 1024))

                        _sent = [0]

                        def _upload_cb(n):
                            if self._cancelled:
                                raise _UploadCancelled()
                            _sent[0] += n
                            self.progress.emit(_sent[0] // 1024, 0)

                        self._repo.upload_archive(
                            buf,
                            self._bundle_name,
                            metadata=self._metadata,
                            progress_callback=_upload_cb,
                        )
                        self.done.emit()
                    except _UploadCancelled:
                        # User cancelled — nothing to report; boto3 aborts the
                        # in-flight transfer when the callback raises.
                        pass
                    except Exception as e:
                        self.error.emit(str(e))

            progress_dialog = QDialog(self)
            progress_dialog.setWindowFlags(Qt.Dialog | Qt.CustomizeWindowHint | Qt.WindowTitleHint)
            progress_dialog.setWindowTitle("Save Bundle to Queue")
            progress_dialog.setWindowModality(Qt.ApplicationModal)
            progress_dialog.setMinimumWidth(350)
            _dlg_layout = QVBoxLayout(progress_dialog)
            _progress_label = QLabel("Archiving bundle...")
            _progress_label.setAlignment(Qt.AlignCenter)
            _progress_bar = QProgressBar()
            _progress_bar.setRange(0, 0)
            _cancel_btn = QPushButton("Cancel")
            _cancel_btn.clicked.connect(progress_dialog.reject)
            _dlg_layout.addWidget(_progress_label)
            _dlg_layout.addWidget(_progress_bar)
            _dlg_layout.addWidget(_cancel_btn, alignment=Qt.AlignRight)

            worker = _UploadWorker(
                queue_repo,
                bundle_name,
                source_dir,
                bundle_metadata if bundle_metadata else None,
            )

            upload_error = []
            _uploaded_bytes = [0]
            _total_bytes = [0]
            _phase = ["Archiving"]
            _finished = [False]

            def _format_size(b):
                if b >= 1024 * 1024 * 1024:
                    return f"{b / (1024**3):.1f} GB"
                elif b >= 1024 * 1024:
                    return f"{b / (1024**2):.1f} MB"
                elif b >= 1024:
                    return f"{b / 1024:.1f} KB"
                return f"{b} B"

            def _on_status(msg):
                _progress_label.setText(msg)
                if "Upload" in msg:
                    _phase[0] = "Uploading"

            def _on_progress(n, total):
                if _finished[0]:
                    return
                if total > 0:
                    _total_bytes[0] = total * 1024
                    _progress_bar.setMaximum(max(1, total))
                    _progress_bar.setValue(0)
                    _uploaded_bytes[0] = 0
                else:
                    _uploaded_bytes[0] = n * 1024
                    _progress_bar.setValue(n)
                    _progress_label.setText(
                        f"{_phase[0]} bundle... {_format_size(_uploaded_bytes[0])} / {_format_size(_total_bytes[0])}"
                    )

            def _on_finished():
                _finished[0] = True
                # Defer so queued progress signals are processed first
                QTimer.singleShot(0, _show_complete)

            def _show_complete():
                _progress_bar.setVisible(False)
                _progress_label.setText("Bundle saved to queue")
                _cancel_btn.setText("Close")
                _cancel_btn.clicked.disconnect()
                _cancel_btn.clicked.connect(progress_dialog.accept)

            def _on_error(msg):
                upload_error.append(msg)
                progress_dialog.close()

            worker.status.connect(_on_status, Qt.QueuedConnection)
            worker.progress.connect(_on_progress, Qt.QueuedConnection)
            worker.done.connect(_on_finished, Qt.QueuedConnection)
            worker.error.connect(_on_error, Qt.QueuedConnection)
            # Cancelling the dialog (Cancel button or window close) flags the
            # worker so its next archive/upload callback aborts cooperatively,
            # instead of blocking the UI until the whole transfer finishes.
            progress_dialog.rejected.connect(worker.cancel)
            worker.start()

            progress_dialog.exec_()
            worker.cancel()
            worker.wait()

            if upload_error:
                raise RuntimeError(upload_error[0])
        except Exception as exc:
            from botocore.exceptions import ClientError

            logger.error("Failed to save bundle: %s", exc, exc_info=True)
            if isinstance(exc, ClientError) and exc.response["Error"]["Code"] == "AccessDenied":
                msg = "You don't have permission to share bundles on this queue."
            else:
                msg = f"Failed to upload bundle:\n{exc}"
            QMessageBox.critical(self, "Export failed", msg)

    def save_job_parameters_to_job_bundle(
        self, job_bundle_dir: str, job_parameters: list[JobParameter]
    ):
        """
        Saves the job parameters to the job bundle. If the job bundle already has a parameter_values file,
        it updates it. Otherwise it creates it.
        """
        job_parameters_dict = {param["name"]: param for param in job_parameters}

        job_parameters_file = os.path.join(job_bundle_dir, "parameter_values.yaml")
        if os.path.exists(job_parameters_file):
            with open(job_parameters_file, "r", encoding="utf8") as f:
                existing_job_parameters = yaml.safe_load(f).get("parameterValues", [])
        else:
            job_parameters_file = os.path.join(job_bundle_dir, "parameter_values.json")
            if os.path.exists(job_parameters_file):
                with open(job_parameters_file, "r", encoding="utf8") as f:
                    existing_job_parameters = json.load(f).get("parameterValues", [])
            else:
                existing_job_parameters = []

        # Overwrite any existing values, and add new values at the end
        combined_job_parameters = []
        for param in existing_job_parameters:
            combined_job_parameters.append(job_parameters_dict.pop(param["name"], param))
        combined_job_parameters.extend(job_parameters_dict.values())

        with open(job_parameters_file, "w", encoding="utf8") as f:
            json.dump({"parameterValues": combined_job_parameters}, f, indent=1)

    def on_submit(self):
        """
        Perform a submission when the submit button is pressed
        """
        # Retrieve all the settings into the dataclass
        settings = self.job_settings_type()
        self.shared_job_settings.update_settings(settings)
        self.job_settings.update_settings(settings)

        queue_parameters = self.shared_job_settings.get_parameters()

        asset_references = self.job_attachments.get_asset_references()

        job_progress_dialog = SubmitJobProgressDialog(parent=self)
        job_progress_dialog.submission_thread_succeeded.connect(
            self._submission_succeeded_signal_receiver
        )
        job_progress_dialog.progress_window_closed.connect(self._close_event_receiver)
        job_progress_dialog.setModal(True)
        job_progress_dialog.show()
        QApplication.instance().processEvents()  # type: ignore[union-attr]

        # Submit the job
        try:
            self.job_history_bundle_dir = create_job_history_bundle_dir(
                self.submitter_info.submitter_name, settings.name
            )

            if self.show_host_requirements_tab:
                requirements = self.host_requirements.get_requirements()
                parameters_from_callback = self.on_create_job_bundle_callback(
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
                parameters_from_callback = self.on_create_job_bundle_callback(
                    self,
                    self.job_history_bundle_dir,
                    settings,
                    queue_parameters,
                    asset_references,
                    purpose=JobBundlePurpose.SUBMISSION,
                )
            if parameters_from_callback is None:
                parameters_from_callback = {}

            # If the callback returned job parameters, update them in the job bundle as well so that
            # submission from the job history dir is equivalent.
            job_parameters = parameters_from_callback.get("job_parameters", [])
            if job_parameters:
                self.save_job_parameters_to_job_bundle(self.job_history_bundle_dir, job_parameters)

            job_progress_dialog.start_job_submission(
                job_bundle_dir=self.job_history_bundle_dir,
                submitter_name=self.submitter_info.submitter_name,
                config=config_file.read_config(),
                require_paths_exist=self.job_attachments.get_require_paths_exist(),
                job_parameters=job_parameters,
                known_asset_paths=self.known_asset_paths
                + parameters_from_callback.get("known_asset_paths", []),
            )

        except UserInitiatedCancel as uic:
            logger.info("Canceling submission.")
            QMessageBox.information(
                self,
                tr("{submitter} job submission").format(
                    submitter=self.submitter_info.submitter_name
                ),
                str(uic),
            )
            job_progress_dialog.close()
        except NonValidInputError as nvie:
            QMessageBox.critical(self, tr("Non valid inputs detected"), str(nvie))
            job_progress_dialog.close()
        except Exception as exc:
            logger.exception("error submitting job")
            api.get_deadline_cloud_library_telemetry_client().record_error_with_trace(
                exc, "on_submit", from_gui=True
            )
            QMessageBox.critical(
                self,
                tr("{submitter} job submission").format(
                    submitter=self.submitter_info.submitter_name
                ),
                str(exc),
            )  # type: ignore[call-arg]
            job_progress_dialog.close()
