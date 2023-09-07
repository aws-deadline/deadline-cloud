# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
Provides a modal dialog box for the submission progress when submitting to
Amazon Deadline Cloud
"""

__all__ = ["SubmitJobProgressDialog"]

import json
import logging
import os
import threading
from typing import Any, Dict, List, Optional, Set

from deadline.client.config import config_file

from botocore.client import BaseClient  # type: ignore[import]
from PySide2.QtCore import Qt, Signal
from PySide2.QtGui import QCloseEvent
from PySide2.QtWidgets import (  # pylint: disable=import-error; type: ignore
    QApplication,
    QDialog,
    QDialogButtonBox,
    QLabel,
    QMessageBox,
    QProgressBar,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from deadline.client import api
from deadline.client.exceptions import CreateJobWaiterCanceled
from deadline.client.config import set_setting
from deadline.client.job_bundle.loader import read_yaml_or_json, read_yaml_or_json_object
from deadline.client.job_bundle.parameters import apply_job_parameters, read_job_bundle_parameters
from deadline.client.job_bundle.submission import (
    FlatAssetReferences,
    split_parameter_args,
    upload_job_attachments,
)
from deadline.job_attachments.errors import AssetSyncCancelledError
from deadline.job_attachments.models import AssetRootManifest
from deadline.job_attachments.progress_tracker import ProgressReportMetadata, SummaryStatistics
from deadline.job_attachments.upload import S3AssetManager
from deadline.job_attachments.utils import human_readable_file_size

logger = logging.getLogger(__name__)


class SubmitJobProgressDialog(QDialog):
    """
    A modal dialog box for the submission progress while submitting a job bundle
    to Amazon Deadline Cloud.
    """

    # These signals are sent when the background threads raise an exception.
    hashing_thread_exception = Signal(BaseException)
    upload_thread_exception = Signal(BaseException)
    create_job_thread_exception = Signal(BaseException)

    # These signals are sent when the background threads succeed.
    hashing_thread_succeeded = Signal([SummaryStatistics, list])
    upload_thread_succeeded = Signal([SummaryStatistics, dict])
    create_job_thread_succeeded = Signal([bool, str])

    # These signals are sent when the progress reporting callbacks are called
    # from job attachments during hashing/uploading.
    hashing_thread_progress_report = Signal(ProgressReportMetadata)
    upload_thread_progress_report = Signal(ProgressReportMetadata)

    @staticmethod
    def start_submission(
        farm_id: str,
        queue_id: str,
        storage_profile_id: str,
        job_bundle_dir: str,
        asset_manager: S3AssetManager,
        deadline_client: BaseClient,
        parent: QWidget = None,
        auto_accept: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """
        Static method that runs the SubmitJobProgressDialog. Returns the response
        from calling create job. If an error occurs or the submission is canceled
        then None is returned.

        Args:
            farm_id (str): Id of the farm to submit to
            queue_id (str): Id of the queue to submit to
            storage_profile_id (str): Id of the storage profile to associate
                with the job.
            job_bundle_dir (str): Path to the folder containing the job bundle to
                submit.
            asset_manager (S3AssetManager): A job attachments S3AssetManager
                configured for the farm/queue to submit to
            deadline_client (BaseClient): A boto client for Amazon Deadline Cloud
            parent (QWidget): Parent widget of the dialog.
            auto_accept (bool, default False): Flag for whether any confirmation
                prompts should automatically be accepted.
            config (ConfigParser, optional): The Amazon Deadline Cloud configuration object to
                use instead of the config file.
        """
        job_progress_dialog = SubmitJobProgressDialog(
            farm_id,
            queue_id,
            storage_profile_id,
            job_bundle_dir,
            asset_manager,
            deadline_client,
            parent=parent,
            auto_accept=auto_accept,
        )
        return job_progress_dialog.exec()

    def __init__(
        self,
        farm_id: str,
        queue_id: str,
        storage_profile_id: str,
        job_bundle_dir: str,
        asset_manager: S3AssetManager,
        deadline_client: BaseClient,
        parent: QWidget = None,
        auto_accept: bool = False,
    ) -> None:
        super().__init__(parent=parent)

        self._farm_id = farm_id
        self._queue_id = queue_id
        self._storage_profile_id = storage_profile_id
        self._job_bundle_dir = job_bundle_dir
        self._asset_manager = asset_manager
        self._deadline_client = deadline_client
        self._auto_accept = auto_accept

        self._continue_submission = True
        self._submission_complete = False
        self._create_job_args: Dict[str, Any] = {}
        self._create_job_response: Dict[str, Any] = {}
        self.__hashing_thread: Optional[threading.Thread] = None
        self.__upload_thread: Optional[threading.Thread] = None
        self.__create_job_thread: Optional[threading.Thread] = None

        self._build_ui()
        self._start_submission()

    def _build_ui(self):
        """Builds up the Dialog UI"""
        # Remove help button from title bar
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowContextHelpButtonHint)
        self.lyt = QVBoxLayout(self)
        self.lyt.setContentsMargins(5, 5, 5, 5)
        self.setMinimumWidth(400)

        self.status_label = QLabel()
        self.hashing_progress_bar = QProgressBar()
        self.upload_progress_bar = QProgressBar()
        self.hashing_progress_message = QLabel("Preparing for hashing...")
        self.upload_progress_message = QLabel("Preparing for upload...")
        self.summary_edit = QTextEdit()
        self.summary_edit.setVisible(False)
        self.summary_edit.setReadOnly(True)
        self.button_box = QDialogButtonBox(Qt.Horizontal)
        self.button_box.setStandardButtons(QDialogButtonBox.Cancel)

        self.lyt.setAlignment(Qt.AlignTop)
        self.lyt.addWidget(self.status_label)
        self.lyt.addWidget(self.hashing_progress_bar)
        self.lyt.addWidget(self.hashing_progress_message)
        self.lyt.addWidget(self.upload_progress_bar)
        self.lyt.addWidget(self.upload_progress_message)
        self.lyt.addWidget(self.summary_edit)
        self.lyt.addWidget(self.button_box)

        self.setWindowTitle("Amazon Deadline Cloud Submission")

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
        # Read in the job template
        file_contents, file_type = read_yaml_or_json(
            self._job_bundle_dir, "template", required=True
        )

        self._create_job_args["farmId"] = self._farm_id
        self._create_job_args["queueId"] = self._queue_id
        self._create_job_args["template"] = file_contents
        self._create_job_args["templateType"] = file_type

        if self._storage_profile_id:
            self._create_job_args["storageProfileId"] = self._storage_profile_id

        # The job parameters
        job_bundle_parameters = read_job_bundle_parameters(self._job_bundle_dir)

        asset_references_obj = read_yaml_or_json_object(
            self._job_bundle_dir, "asset_references", required=False
        )
        self.asset_references = FlatAssetReferences.from_dict(asset_references_obj)

        apply_job_parameters([], self._job_bundle_dir, job_bundle_parameters, self.asset_references)

        app_parameters_formatted, job_parameters_formatted = split_parameter_args(
            job_bundle_parameters, self._job_bundle_dir
        )

        self._create_job_args.update(app_parameters_formatted)

        if job_parameters_formatted:
            self._create_job_args["parameters"] = job_parameters_formatted

        if (
            self.asset_references.input_filenames
            or self.asset_references.input_directories
            or self.asset_references.output_directories
        ):
            # Extend input_filenames with all the files in the input_directories
            for directory in self.asset_references.input_directories:
                for root, _, files in os.walk(directory):
                    self.asset_references.input_filenames.update(
                        os.path.normpath(os.path.join(root, file)) for file in files
                    )
            self.asset_references.input_directories.clear()

            self._start_hashing(
                self.asset_references.input_filenames,
                self.asset_references.output_directories,
            )
        else:
            self.hashing_progress_bar.setVisible(False)
            self.hashing_progress_message.setVisible(False)
            self.upload_progress_bar.setVisible(False)
            self.upload_progress_message.setVisible(False)
            self._start_create_job()

    def _hashing_background_thread(self, input_paths: Set[str], output_paths: Set[str]) -> None:
        """
        This function gets started in a background thread to start the hashing
        of any job attachments.
        """
        try:

            def _update_hash_progress(hashing_metadata: ProgressReportMetadata) -> bool:
                self.hashing_thread_progress_report.emit(hashing_metadata)
                return self._continue_submission

            logger.info("Hashing job attachments files...")

            hashing_summary, manifests = self._asset_manager.hash_assets_and_create_manifest(
                input_paths=sorted(input_paths),
                output_paths=sorted(output_paths),
                storage_profile_id=self._storage_profile_id,
                hash_cache_dir=os.path.expanduser(os.path.join("~", ".deadline", "cache")),
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

            upload_summary, attachment_settings = upload_job_attachments(
                self._asset_manager,
                manifests,
                _update_upload_progress,
            )

            logger.info("Finished uploading job attachments files.")

            self.upload_thread_succeeded.emit(upload_summary, attachment_settings)
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

            logger.info("Waiting for Job to be created...")

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
                message = "CreateJob response was empty, or did not contain a Job ID."
            self.create_job_thread_succeeded.emit(success, message)
        except CreateJobWaiterCanceled as e:
            # If it wasn't canceled, send the exception to the dialog
            if self._continue_submission:
                self.create_job_thread_exception.emit(e)
            else:
                logger.info("Wait for CreateJob result canceled.")
        except Exception as e:
            # Send the exception to the dialog
            self.create_job_thread_exception.emit(e)

    def _start_hashing(self, input_paths: Set[str], output_paths: Set[str]) -> None:
        """
        Starts the background hashing thread.
        """
        self.status_label.setText("Hashing job attachments...")
        self.__hashing_thread = threading.Thread(
            target=self._hashing_background_thread,
            name="Amazon Deadline Cloud Hashing Background Thread",
            args=(input_paths, output_paths),
        )
        self.__hashing_thread.start()

    def _start_upload(self, asset_manifests: List[AssetRootManifest]) -> None:
        """
        Starts the background upload thread.
        """
        self.status_label.setText("Uploading job attachments...")
        self.__upload_thread = threading.Thread(
            target=self._upload_background_thread,
            name="Amazon Deadline Cloud Upload Background Thread",
            args=(asset_manifests,),
        )
        self.__upload_thread.start()

    def _start_create_job(self) -> None:
        """
        Starts the background thread to call CreateJob.
        """
        self.status_label.setText("Waiting for Job to be created...")
        self.__create_job_thread = threading.Thread(
            target=self._create_job_background_thread,
            name="Amazon Deadline Cloud CreateJob Background Thread",
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
        self.hashing_progress_bar.setValue(int(progress_metadata.progress))
        self.hashing_progress_message.setText(progress_metadata.progressMessage)

    def handle_upload_thread_progress_report(
        self, progress_metadata: ProgressReportMetadata
    ) -> None:
        """
        Handles the signal sent from the background threads when reporting
        upload progress. Sets the progress bar in the dialog based on
        the callback progress data from job attachments.
        """
        self.upload_progress_bar.setValue(int(progress_metadata.progress))
        self.upload_progress_message.setText(progress_metadata.progressMessage)

    def handle_hashing_thread_succeeded(
        self,
        hashing_summary: SummaryStatistics,
        asset_manifests: List[AssetRootManifest],
    ) -> None:
        """
        Handles the signal sent from the background hashing thread when the
        hashing process has completed.
        """
        api.get_telemetry_client().record_hashing_summary(hashing_summary, from_gui=True)
        self.summary_edit.setText(
            f"\nHashing Summary:\n"
            f"    Hashed {hashing_summary.processed_files} files totaling"
            f" {human_readable_file_size(hashing_summary.processed_bytes)}.\n"
            f"    Skipped re-hashing {hashing_summary.skipped_files} files totaling"
            f" {human_readable_file_size(hashing_summary.skipped_bytes)}.\n"
            f"    Total hashing time of {round(hashing_summary.total_time, ndigits=5)} seconds"
            f" at {human_readable_file_size(int(hashing_summary.transfer_rate))}/s.\n"
        )

        if (
            not self._auto_accept
            and hashing_summary.total_files > 0
            and not self._confirm_job_attachments_upload(
                hashing_summary.total_files, hashing_summary.total_bytes
            )
        ):
            self.close()
        else:
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
            self._create_job_args["attachments"]["assetLoadingMethod"] = config_file.get_setting(
                "defaults.job_attachments_file_system"
            )

        api.get_telemetry_client().record_upload_summary(upload_summary, from_gui=True)
        self.summary_edit.setText(
            f"{self.summary_edit.toPlainText()}"
            f"\nUpload Summary:\n"
            f"    Uploaded {upload_summary.processed_files} files totaling"
            f" {human_readable_file_size(upload_summary.processed_bytes)}.\n"
            f"    Skipped re-uploading {upload_summary.skipped_files} files totaling"
            f" {human_readable_file_size(upload_summary.skipped_bytes)}.\n"
            f"    Total upload time of {round(upload_summary.total_time, ndigits=5)} seconds"
            f" at {human_readable_file_size(int(upload_summary.transfer_rate))}/s.\n"
        )

        self._start_create_job()

    def handle_create_job_thread_succeeded(self, success: bool, status_message: str) -> None:
        """
        Handles the signal sent from the background CreateJob thread when the
        job creation has finished.
        """
        if success:
            self._submission_complete = True
            self.status_label.setText("Submission Complete")
            self.button_box.setStandardButtons(QDialogButtonBox.Ok)
            self.button_box.button(QDialogButtonBox.Ok).setDefault(True)
        else:
            self.status_label.setText("Submission Error")
            self.button_box.setStandardButtons(QDialogButtonBox.Close)
            self.button_box.button(QDialogButtonBox.Close).setDefault(True)

        self.summary_edit.setText(f"{status_message} {self.summary_edit.toPlainText()}")
        self.summary_edit.setVisible(True)

    def handle_thread_exception(self, e: BaseException) -> None:
        """
        Handles the signal sent from the background threads when an exception is
        thrown.
        """
        self.hashing_progress_bar.setVisible(False)
        self.hashing_progress_message.setVisible(False)
        self.upload_progress_bar.setVisible(False)
        self.upload_progress_message.setVisible(False)
        self.status_label.setVisible(False)
        self.button_box.setStandardButtons(QDialogButtonBox.Close)
        self.summary_edit.setText(f"Error Occurred: {str(e)}")
        self.summary_edit.setVisible(True)
        logger.error(str(e))

    def _confirm_job_attachments_upload(self, num_files: int, upload_size: int) -> bool:
        """
        Creates a dialog to prompt the user to confirm that they want to proceed
        with uploding the specified number of files totaling a certain size.
        """
        message_box = QMessageBox(self)
        message_box.setText(
            f"Job submission contains {num_files} files totaling {human_readable_file_size(upload_size)}. "
            "All files will be uploaded to S3 if they are not already present in the job attachments bucket."
        )
        message_box.setStandardButtons(QMessageBox.Ok | QMessageBox.Cancel)
        message_box.setDefaultButton(QMessageBox.Ok)
        message_box.setWindowTitle("Job Attachments Upload Confirmation")
        selection = message_box.exec_()

        return selection == QMessageBox.Ok

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
            self.hashing_progress_bar.setVisible(False)
            self.hashing_progress_message.setVisible(False)
            self.upload_progress_bar.setVisible(False)
            self.upload_progress_message.setVisible(False)
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
                    QApplication.instance().processEvents()

    def exec(self) -> Optional[Dict[str, Any]]:
        """
        Runs the modal job progress dialog, returns the response from calling
        create job if the dialog was accepted. Otherwise returns None
        """
        if super().exec_() == QDialog.Accepted:
            return self._create_job_response
        return None

    def exec_(self) -> Optional[Dict[str, Any]]:
        return self.exec()
