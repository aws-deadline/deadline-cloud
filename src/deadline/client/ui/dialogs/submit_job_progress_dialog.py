# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
Provides a modal dialog box for the submission progress when submitting to
AWS Deadline Cloud
"""
from __future__ import annotations

import json
import logging
import os
import threading
import textwrap
from typing import Any, Dict, List, Optional, cast

from botocore.client import BaseClient  # type: ignore[import]
from qtpy.QtCore import Qt, Signal
from qtpy.QtGui import QCloseEvent
from qtpy.QtWidgets import (  # pylint: disable=import-error; type: ignore
    QApplication,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QGroupBox,
    QLabel,
    QMessageBox,
    QPushButton,
    QProgressBar,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from deadline.client import api
from deadline.client.exceptions import (
    CreateJobWaiterCanceled,
    DeadlineOperationError,
    UserInitiatedCancel,
)
from deadline.client.config import set_setting, config_file
from deadline.client.job_bundle.loader import (
    read_yaml_or_json,
    read_yaml_or_json_object,
    validate_directory_symlink_containment,
)
from deadline.client.job_bundle.parameters import (
    JobParameter,
    apply_job_parameters,
    merge_queue_job_parameters,
    read_job_bundle_parameters,
)
from deadline.client.job_bundle.submission import (
    AssetReferences,
    split_parameter_args,
)
from deadline.job_attachments.exceptions import AssetSyncCancelledError, MisconfiguredInputsError
from deadline.job_attachments.models import (
    AssetRootGroup,
    AssetRootManifest,
    AssetUploadGroup,
    StorageProfile,
)
from deadline.job_attachments.progress_tracker import ProgressReportMetadata, SummaryStatistics
from deadline.job_attachments.upload import S3AssetManager
from deadline.job_attachments._utils import _human_readable_file_size

__all__ = ["SubmitJobProgressDialog"]

logger = logging.getLogger(__name__)


class SubmitJobProgressDialog(QDialog):
    """
    A modal dialog box for the submission progress while submitting a job bundle
    to AWS Deadline Cloud.
    """

    # These signals are sent when the background threads raise an exception.
    hashing_thread_exception = Signal(BaseException)
    upload_thread_exception = Signal(BaseException)
    create_job_thread_exception = Signal(BaseException)

    # These signals are sent when the background threads succeed.
    hashing_thread_succeeded = Signal(SummaryStatistics, list)
    upload_thread_succeeded = Signal(SummaryStatistics, dict)
    create_job_thread_succeeded = Signal(bool, str)

    # These signals are sent when the progress reporting callbacks are called
    # from job attachments during hashing/uploading.
    hashing_thread_progress_report = Signal(ProgressReportMetadata)
    upload_thread_progress_report = Signal(ProgressReportMetadata)

    def __init__(self, parent: QWidget) -> None:
        super().__init__(parent=parent)
        self._continue_submission = True
        self._submission_complete = False
        self._create_job_args: Dict[str, Any] = {}
        self._create_job_response: Dict[str, Any] = {}
        self.__hashing_thread: Optional[threading.Thread] = None
        self.__upload_thread: Optional[threading.Thread] = None
        self.__create_job_thread: Optional[threading.Thread] = None

        self._build_ui()

    def start_submission(
        self,
        farm_id: str,
        queue_id: str,
        storage_profile: Optional[StorageProfile],
        job_bundle_dir: str,
        queue_parameters: list[JobParameter],
        asset_manager: Optional[S3AssetManager],
        deadline_client: BaseClient,
        auto_accept: bool = False,
        require_paths_exist: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """
        Starts a submission. Returns the response from calling create job. If an error occurs
        or the submission is canceled then None is returned.

        Args:
            farm_id (str): Id of the farm to submit to
            queue_id (str): Id of the queue to submit to
            storage_profile (StorageProfile): the storage profile to associate
                with the job.
            job_bundle_dir (str): Path to the folder containing the job bundle to
                submit.
            asset_manager (S3AssetManager): A job attachments S3AssetManager
                configured for the farm/queue to submit to
            deadline_client (BaseClient): A boto client for AWS Deadline Cloud
            auto_accept (bool, default False): Flag for whether any confirmation
                prompts should automatically be accepted.
        """
        self._farm_id = farm_id
        self._queue_id = queue_id
        self._storage_profile = storage_profile
        self._job_bundle_dir = job_bundle_dir
        self._queue_parameters = queue_parameters
        self._asset_manager = asset_manager
        self._deadline_client = deadline_client
        self._auto_accept = auto_accept
        self._require_paths_exist = require_paths_exist

        self._start_submission()
        return self.exec_()

    def _build_ui(self):
        """Builds up the Dialog UI"""
        # Remove help button from title bar
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowContextHelpButtonHint)
        self.lyt = QVBoxLayout(self)
        self.lyt.setContentsMargins(5, 10, 5, 5)
        self.setMinimumWidth(450)

        self.status_label = QLabel("Preparing files...")
        self.status_label.setMargin(5)
        self.hashing_progress = JobAttachmentsProgressWidget(
            initial_message="Preparing for hashing...", title="Hashing progress", parent=self
        )
        self.upload_progress = JobAttachmentsProgressWidget(
            initial_message="Preparing for upload...", title="Upload progress", parent=self
        )
        self.summary_edit = QTextEdit()
        self.summary_edit.setVisible(False)
        self.summary_edit.setReadOnly(True)
        self.button_box = QDialogButtonBox(Qt.Horizontal)
        self.button_box.setStandardButtons(QDialogButtonBox.Cancel)

        self.lyt.setAlignment(Qt.AlignTop)
        self.lyt.addWidget(self.status_label)
        self.lyt.addWidget(self.hashing_progress)
        self.lyt.addWidget(self.upload_progress)
        self.lyt.addWidget(self.summary_edit)
        self.lyt.addWidget(self.button_box)

        self.setWindowTitle("AWS Deadline Cloud submission")

        self.hashing_thread_progress_report.connect(self.handle_hashing_thread_progress_report)
        self.hashing_thread_succeeded.connect(self.handle_hashing_thread_succeeded)
        self.hashing_thread_exception.connect(self.handle_thread_exception)

        self.upload_thread_progress_report.connect(self.handle_upload_thread_progress_report)
        self.upload_thread_succeeded.connect(self.handle_upload_thread_succeeded)
        self.upload_thread_exception.connect(self.handle_thread_exception)

        self.create_job_thread_succeeded.connect(self.handle_create_job_thread_succeeded)
        self.create_job_thread_exception.connect(self.handle_thread_exception)

        self.button_box.accepted.connect(self.accept)
        self.button_box.rejected.connect(self.close)

    def _start_submission(self):
        """
        Starts building up the arguments to pass to create job. If there are any
        job attachments the hashing process will be started. If there are no job
        attachments then create job will be called without calling job attachments.
        """

        # Ensure the job bundle doesn't contain files that resolve outside of the bundle directory
        validate_directory_symlink_containment(self._job_bundle_dir)

        # Read in the job template
        file_contents, file_type = read_yaml_or_json(
            self._job_bundle_dir, "template", required=True
        )

        self._create_job_args["farmId"] = self._farm_id
        self._create_job_args["queueId"] = self._queue_id
        self._create_job_args["template"] = file_contents
        self._create_job_args["templateType"] = file_type

        if self._storage_profile:
            self._create_job_args["storageProfileId"] = self._storage_profile.storageProfileId

        # The job parameters
        job_bundle_parameters = read_job_bundle_parameters(self._job_bundle_dir)

        asset_references_obj = read_yaml_or_json_object(
            self._job_bundle_dir, "asset_references", required=False
        )
        self.asset_references = AssetReferences.from_dict(asset_references_obj)

        parameter_definitions = merge_queue_job_parameters(
            queue_id=self._queue_id,
            job_parameters=job_bundle_parameters,
            queue_parameters=self._queue_parameters,
        )

        apply_job_parameters(
            [],
            self._job_bundle_dir,
            parameter_definitions,
            self.asset_references,
        )

        app_parameters_formatted, job_parameters_formatted = split_parameter_args(
            parameter_definitions, self._job_bundle_dir
        )

        self._create_job_args.update(app_parameters_formatted)

        if job_parameters_formatted:
            self._create_job_args["parameters"] = job_parameters_formatted

        if self._asset_manager and (
            self.asset_references.input_filenames
            or self.asset_references.input_directories
            or self.asset_references.output_directories
        ):
            # Extend input_filenames with all the files in the input_directories
            missing_directories: set[str] = set()
            for directory in self.asset_references.input_directories:
                if not os.path.isdir(directory):
                    if self._require_paths_exist:
                        missing_directories.add(directory)
                    else:
                        logging.warning(
                            f"Input directory '{directory}' does not exist. Adding to referenced paths."
                        )
                        self.asset_references.referenced_paths.add(directory)
                    continue

                is_dir_empty = True
                for root, _, files in os.walk(directory):
                    if not files:
                        continue
                    is_dir_empty = False
                    self.asset_references.input_filenames.update(
                        os.path.normpath(os.path.join(root, file)) for file in files
                    )
                # Empty directories just become references since there's nothing to upload
                if is_dir_empty:
                    logging.info(
                        f"Input directory '{directory}' is empty. Adding to referenced paths."
                    )
                    self.asset_references.referenced_paths.add(directory)
            self.asset_references.input_directories.clear()

            if missing_directories:
                sample_size = 3
                misconfigured_directories_msg = (
                    "Job submission contains misconfigured input directories and cannot be submitted."
                    " All input directories must exist."
                )

                missing_directory_list = sorted(list(missing_directories))
                sample_of_missing_directories = "\n\t".join(missing_directory_list[:sample_size])
                sample_of_misconfigured_inputs = (
                    f"\nNon-existent directories:\n\t{sample_of_missing_directories}\n"
                )
                all_missing_directories = "\n\t".join(missing_directory_list)
                all_misconfigured_inputs = (
                    f"\nNon-existent directories:\n\t{all_missing_directories}"
                )

                logging.error(misconfigured_directories_msg + all_misconfigured_inputs)
                if len(missing_directories) > sample_size:
                    misconfigured_directories_msg += (
                        " Check logs for all occurrences, here's a sample:\n"
                    )
                misconfigured_directories_msg += f"{sample_of_misconfigured_inputs}"

                raise MisconfiguredInputsError(misconfigured_directories_msg)

            upload_group = self._asset_manager.prepare_paths_for_upload(
                input_paths=sorted(self.asset_references.input_filenames),
                output_paths=sorted(self.asset_references.output_directories),
                referenced_paths=sorted(self.asset_references.referenced_paths),
                storage_profile=self._storage_profile,
                require_paths_exist=self._require_paths_exist,
            )
            # If we find any Job Attachments, start a background thread
            if upload_group.asset_groups:
                if not self._confirm_asset_references_outside_storage_profile(upload_group):
                    raise UserInitiatedCancel("Submission canceled.")

                self._start_hashing(
                    upload_group.asset_groups,
                    upload_group.total_input_files,
                    upload_group.total_input_bytes,
                )
                return

        self.hashing_progress.setVisible(False)
        self.upload_progress.setVisible(False)
        self._start_create_job()

    def _hashing_background_thread(
        self,
        asset_groups: list[AssetRootGroup],
        total_input_files: int,
        total_input_bytes: int,
    ) -> None:
        """
        This function gets started in a background thread to start the hashing
        of any job attachments.
        """
        try:

            def _update_hash_progress(hashing_metadata: ProgressReportMetadata) -> bool:
                self.hashing_thread_progress_report.emit(hashing_metadata)
                return self._continue_submission

            logger.info("Hashing job attachments files...")

            # This thread is only started if self._asset_manager is set.
            hashing_summary, manifests = cast(
                S3AssetManager, self._asset_manager
            ).hash_assets_and_create_manifest(
                asset_groups=asset_groups,
                total_input_files=total_input_files,
                total_input_bytes=total_input_bytes,
                hash_cache_dir=config_file.get_cache_directory(),
                on_preparing_to_submit=_update_hash_progress,
            )

            logger.info("Finished hashing job attachments files.")

            self.hashing_thread_succeeded.emit(hashing_summary, manifests)
        except AssetSyncCancelledError as e:
            # If it wasn't canceled, send the exception to the dialog
            if self._continue_submission:
                self.hashing_thread_exception.emit(e)
            else:
                logger.info("Job attachments hashing canceled.")
        except Exception as e:
            # Send the exception to the dialog
            self.hashing_thread_exception.emit(e)

    def _upload_background_thread(self, manifests: List[AssetRootManifest]) -> None:
        """
        This function gets started in a background thread to start the upload
        of any job attachments.
        """
        try:

            def _update_upload_progress(upload_metadata: ProgressReportMetadata) -> bool:
                self.upload_thread_progress_report.emit(upload_metadata)
                return self._continue_submission

            logger.info("Uploading job attachments files...")

            # This thread is only started if self._asset_manager is set.
            upload_summary, attachment_settings = cast(
                S3AssetManager, self._asset_manager
            ).upload_assets(
                manifests=manifests,
                on_uploading_assets=_update_upload_progress,
                s3_check_cache_dir=config_file.get_cache_directory(),
                manifest_write_dir=self._job_bundle_dir,
            )

            logger.info("Finished uploading job attachments files.")

            self.upload_thread_succeeded.emit(upload_summary, attachment_settings.to_dict())
        except AssetSyncCancelledError as e:
            # If it wasn't canceled, send the exception to the dialog
            if self._continue_submission:
                self.hashing_thread_exception.emit(e)
            else:
                logger.info("Job attachments upload canceled.")
        except Exception as e:
            # Send the exception to the dialog
            self.hashing_thread_exception.emit(e)

    def _create_job_background_thread(self) -> None:
        """
        This function gets started in a background thread to call CreateJob.
        """
        try:

            def _continue_create_job_wait() -> bool:
                return self._continue_submission

            logger.info("Waiting for job to be created...")

            success = False

            logger.debug(json.dumps(self._create_job_args, indent=1))
            self._create_job_response = self._deadline_client.create_job(**self._create_job_args)
            logger.debug(f"CreateJob Response {self._create_job_response}")

            if self._create_job_response and "jobId" in self._create_job_response:
                job_id = self._create_job_response["jobId"]

                # Set the default job id so it holds the most-recently submitted job.
                set_setting("defaults.job_id", job_id)

                success, message = api.wait_for_create_job_to_complete(
                    self._farm_id,
                    self._queue_id,
                    job_id,
                    self._deadline_client,
                    _continue_create_job_wait,
                )
                message += f"\n{job_id}\n"
            else:
                message = "CreateJob response was empty, or did not contain a job ID."
            if success:
                self.create_job_thread_succeeded.emit(success, message)
            else:
                self.create_job_thread_exception.emit(DeadlineOperationError(message))
        except CreateJobWaiterCanceled as e:
            # If it wasn't canceled, send the exception to the dialog
            if self._continue_submission:
                self.create_job_thread_exception.emit(e)
            else:
                logger.info("Wait for CreateJob result canceled.")
        except Exception as e:
            # Send the exception to the dialog
            self.create_job_thread_exception.emit(e)

    def _start_hashing(
        self,
        asset_groups: list[AssetRootGroup],
        total_input_files: int,
        total_input_bytes: int,
    ) -> None:
        """
        Starts the background hashing thread.
        """
        self.status_label.setText("Hashing job attachments...")
        self.__hashing_thread = threading.Thread(
            target=self._hashing_background_thread,
            name="AWS Deadline Cloud hashing background thread",
            args=(asset_groups, total_input_files, total_input_bytes),
        )
        self.__hashing_thread.start()

    def _start_upload(self, asset_manifests: List[AssetRootManifest]) -> None:
        """
        Starts the background upload thread.
        """
        self.status_label.setText("Uploading job attachments...")
        self.__upload_thread = threading.Thread(
            target=self._upload_background_thread,
            name="AWS Deadline Cloud upload background thread",
            args=(asset_manifests,),
        )
        self.__upload_thread.start()

    def _start_create_job(self) -> None:
        """
        Starts the background thread to call CreateJob.
        """
        self.status_label.setText("Waiting for job to be created...")
        self.__create_job_thread = threading.Thread(
            target=self._create_job_background_thread,
            name="AWS Deadline Cloud CreateJob background thread",
        )
        self.__create_job_thread.start()

    def handle_hashing_thread_progress_report(
        self, progress_metadata: ProgressReportMetadata
    ) -> None:
        """
        Handles the signal sent from the background threads when reporting
        hashing progress. Sets the progress bar in the dialog based on
        the callback progress data from job attachments.
        """
        self.hashing_progress.progress_bar.setValue(int(progress_metadata.progress))
        self.hashing_progress.progress_message.setText(progress_metadata.progressMessage)

    def handle_upload_thread_progress_report(
        self, progress_metadata: ProgressReportMetadata
    ) -> None:
        """
        Handles the signal sent from the background threads when reporting
        upload progress. Sets the progress bar in the dialog based on
        the callback progress data from job attachments.
        """
        self.upload_progress.progress_bar.setValue(int(progress_metadata.progress))
        self.upload_progress.progress_message.setText(progress_metadata.progressMessage)

    def handle_hashing_thread_succeeded(
        self,
        hashing_summary: SummaryStatistics,
        asset_manifests: List[AssetRootManifest],
    ) -> None:
        """
        Handles the signal sent from the background hashing thread when the
        hashing process has completed.
        """
        api.get_deadline_cloud_library_telemetry_client().record_hashing_summary(
            hashing_summary, from_gui=True
        )
        self.summary_edit.setText(
            f"\nHashing summary:\n{textwrap.indent(str(hashing_summary), '    ')}"
        )
        self._start_upload(asset_manifests)

    def handle_upload_thread_succeeded(
        self, upload_summary: SummaryStatistics, attachment_settings: Any
    ) -> None:
        """
        Handles the signal sent from the background upload thread when the upload
        has finished.
        """
        if attachment_settings:
            self._create_job_args["attachments"] = attachment_settings
            self._create_job_args["attachments"]["fileSystem"] = config_file.get_setting(
                "defaults.job_attachments_file_system"
            )

        api.get_deadline_cloud_library_telemetry_client().record_upload_summary(
            upload_summary, from_gui=True
        )
        self.summary_edit.setText(
            f"{self.summary_edit.toPlainText()}"
            + f"\nUpload summary:\n{textwrap.indent(str(upload_summary), '    ')}"
        )

        self._start_create_job()

    def handle_create_job_thread_succeeded(self, success: bool, status_message: str) -> None:
        """
        Handles the signal sent from the background CreateJob thread when the
        job creation has finished.
        """
        api.get_deadline_cloud_library_telemetry_client().record_event(
            event_type="com.amazon.rum.deadline.create_job",
            event_details={"is_success": success},
            from_gui=True,
        )

        if success:
            self._submission_complete = True
            self.status_label.setText("Submission complete")
            self.button_box.setStandardButtons(QDialogButtonBox.Ok)
            self.button_box.button(QDialogButtonBox.Ok).setDefault(True)
        else:
            self.status_label.setText("Submission error")
            self.button_box.setStandardButtons(QDialogButtonBox.Close)
            self.button_box.button(QDialogButtonBox.Close).setDefault(True)

        self.summary_edit.setText(f"{status_message} {self.summary_edit.toPlainText()}")
        self.summary_edit.setVisible(True)
        self.adjustSize()

    def handle_thread_exception(self, e: BaseException) -> None:
        """
        Handles the signal sent from the background threads when an exception is
        thrown.
        """
        self.hashing_progress.setVisible(False)
        self.upload_progress.setVisible(False)
        self.status_label.setVisible(False)
        self.button_box.setStandardButtons(QDialogButtonBox.Close)
        self.summary_edit.setText(f"Error occurred: {str(e)}")
        self.summary_edit.setVisible(True)
        self.adjustSize()
        logger.error(str(e))

    def _confirm_asset_references_outside_storage_profile(
        self, upload_group: AssetUploadGroup
    ) -> bool:
        """
        Creates a dialog to prompt the user to confirm that they want to proceed
        with uploading when files were found outside of the configured storage profile locations.
        """
        message_text = (
            f"Job submission contains {upload_group.total_input_files} input files totaling {_human_readable_file_size(upload_group.total_input_bytes)}. "
            " All input files will be uploaded to S3 if they are not already present in the job attachments bucket."
        )
        warning_message = ""
        for group in upload_group.asset_groups:
            if not group.file_system_location_name:
                warning_message += f"\n\nUnder the directory '{group.root_path}':"
                warning_message += (
                    f"\n\t{len(group.inputs)} input file{'' if len(group.inputs) == 1 else 's'}"
                    if len(group.inputs) > 0
                    else ""
                )
                warning_message += (
                    f"\n\t{len(group.outputs)} output director{'y' if len(group.outputs) == 1 else 'ies'}"
                    if len(group.outputs) > 0
                    else ""
                )
                warning_message += (
                    f"\n\t{len(group.references)} referenced file{'' if len(group.references) == 1 else 's'} and/or director{'y' if len(group.outputs) == 1 else 'ies'}"
                    if len(group.references) > 0
                    else ""
                )

        # Exit early if we've set auto accept and there are no warnings
        if not warning_message and self._auto_accept:
            return True

        # Build the UI
        message_box = QMessageBox(self)
        if warning_message:
            if self._storage_profile:
                fs_locations_text = "\n\t".join(
                    [fs_location.path for fs_location in self._storage_profile.fileSystemLocations]
                )
                message_text += f"\n\nFiles were specified outside of the configured storage profile location(s):\n{fs_locations_text}\n"
            else:
                message_text += "\n\nNo storage profile locations are configured for this queue."
            message_text += (
                "\nPlease confirm that you intend to submit a job that uses files from the following directories:"
                f"{warning_message}\n\n"
                "To permanently remove this warning you must only use files located within a storage profile location."
            )
            message_box.setIcon(QMessageBox.Warning)

        message_box.setText(message_text)
        message_box.setStandardButtons(QMessageBox.Ok | QMessageBox.Cancel)
        message_box.setDefaultButton(QMessageBox.Ok)

        if not warning_message:
            # If we don't have any warnings, add the "Do not ask again" button that acts like 'OK' but sets the config
            # setting to always auto-accept similar prompts in the future.
            dont_ask_button = QPushButton("Do not ask again", self)
            dont_ask_button.clicked.connect(lambda: set_setting("settings.auto_accept", "true"))
            message_box.addButton(dont_ask_button, QMessageBox.ActionRole)

        message_box.setWindowTitle("Job attachments valid files confirmation")
        selection = message_box.exec()

        return selection != QMessageBox.Cancel

    def closeEvent(self, event: QCloseEvent) -> None:
        """
        Overrides the closeEvent function to shutdown any running threads before
        closing the dialog. If the submission is complete then any button, even
        'X', should result in the dialog being accepted.
        """
        if self._submission_complete:
            self.accept()
        else:
            logger.info("Canceling submission...")
            self.status_label.setText("Canceling submission...")
            self.hashing_progress.setVisible(False)
            self.upload_progress.setVisible(False)
            self.adjustSize()
            self._continue_submission = False
            self._shutdown_threads()
            super().closeEvent(event)

    def _shutdown_threads(self) -> None:
        """Closes any threads. Used before canceling/closing"""

        threads = (self.__hashing_thread, self.__upload_thread, self.__create_job_thread)

        for thread in threads:
            if thread:
                while thread.is_alive():
                    QApplication.instance().processEvents()  # type: ignore[union-attr]

    def exec_(self) -> Optional[Dict[str, Any]]:  # type: ignore[override]
        """
        Runs the modal job progress dialog, returns the response from calling
        create job if the dialog was accepted. Otherwise returns None
        """
        if super().exec_() == QDialog.Accepted:
            return self._create_job_response
        return None


class JobAttachmentsProgressWidget(QGroupBox):
    """
    UI element to group job attachments progress bar with a status message.
    """

    def __init__(self, *, initial_message: str, title: str, parent: Optional[QWidget] = None):
        super().__init__(parent=parent, title=title)
        self.initial_message = initial_message

        self._build_ui()

    def _build_ui(self):
        self.layout = QFormLayout(self)
        self.layout.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)

        self.progress_bar = QProgressBar()
        self.progress_message = QLabel(self.initial_message)

        self.layout.addWidget(self.progress_bar)
        self.layout.addWidget(self.progress_message)
