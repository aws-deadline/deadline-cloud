# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
Provides a modal dialog box for the submission progress when submitting to
AWS Deadline Cloud
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Optional
from functools import partial
import time

from qtpy.QtCore import Qt, Signal, QSize
from qtpy.QtGui import QCloseEvent, QFontMetrics
from qtpy.QtWidgets import (  # pylint: disable=import-error; type: ignore
    QApplication,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QProgressBar,
    QStyle,
    QTextEdit,
    QVBoxLayout,
    QWidget,
    QSizePolicy,
)

from .. import CancelationFlag
from ... import api
from ...config import set_setting
from ....job_attachments.progress_tracker import ProgressReportMetadata

__all__ = ["SubmitJobProgressDialog"]

logger = logging.getLogger(__name__)


def _print_function_callback(
    cancelation_flag: CancelationFlag, dialog: SubmitJobProgressDialog, message: str
):
    """Callback for when api.create_job_from_job_bundle prints a message."""
    if not cancelation_flag:
        dialog.submission_thread_print.emit(message)


def _interactive_confirmation_callback(
    cancelation_flag: CancelationFlag,
    dialog: SubmitJobProgressDialog,
    message: str,
    default_response: bool,
) -> bool:
    """Callback for when api.create_job_from_job_bundle presents a warning to users."""
    if not cancelation_flag.canceled:
        # The handler for the submission_thread_request_warning_dialog signal will show
        # the warning dialog in the main GUI thread, then will set the _warning_dialog_completed
        # property to True when it is done.
        dialog._warning_dialog_completed = False
        dialog.submission_thread_request_warning_dialog.emit(message, default_response)
    while not cancelation_flag.canceled and not dialog._warning_dialog_completed:
        time.sleep(0.1)
    return not cancelation_flag.canceled and not dialog._warning_dialog_canceled


def _hashing_progress_callback(
    cancelation_flag: CancelationFlag,
    dialog: SubmitJobProgressDialog,
    progress_report_metadata: ProgressReportMetadata,
):
    """Callback for when api.create_job_from_job_bundle provides a hashing progress update."""
    if not cancelation_flag.canceled:
        dialog.submission_thread_hashing_progress.emit(progress_report_metadata)
    return not cancelation_flag.canceled


def _upload_progress_callback(
    cancelation_flag: CancelationFlag,
    dialog: SubmitJobProgressDialog,
    progress_report_metadata: ProgressReportMetadata,
):
    """Callback for when api.create_job_from_job_bundle provides an upload progress update."""
    if not cancelation_flag.canceled:
        dialog.submission_thread_upload_progress.emit(progress_report_metadata)
    return not cancelation_flag.canceled


def _create_job_result_callback(cancelation_flag: CancelationFlag):
    """Callback for when api.create_job_from_job_bundle checks whether to cancel."""
    return not cancelation_flag.canceled


def _submission_thread_runner(
    cancelation_flag: CancelationFlag, dialog: SubmitJobProgressDialog, kwargs
):
    """Function to run api.create_job_from_job_bundle in a background thread."""
    try:
        job_id = api.create_job_from_job_bundle(**kwargs)
        if not cancelation_flag.canceled:
            dialog.job_id = job_id
            dialog.submission_thread_succeeded.emit(job_id)
    except Exception as e:
        if not cancelation_flag.canceled:
            dialog.submission_thread_exception.emit(e)


class SubmitJobProgressDialog(QDialog):
    """
    A modal dialog box for the submission progress while submitting a job bundle
    to AWS Deadline Cloud.
    """

    cancelation_flag: CancelationFlag

    # This signal is sent when the background thread raises an exception.
    submission_thread_exception = Signal(BaseException)

    # These signals are sent from the background thread
    submission_thread_print = Signal(str)
    submission_thread_hashing_progress = Signal(ProgressReportMetadata)
    submission_thread_upload_progress = Signal(ProgressReportMetadata)
    submission_thread_request_warning_dialog = Signal(str, bool)

    # This signal is sent when the background thread succeeds.
    submission_thread_succeeded = Signal(str)
    progress_window_closed = Signal(None)

    job_id: Optional[str] = None

    def __init__(self, parent: QWidget) -> None:
        super().__init__(parent=parent)

        # Use the CancelationFlag object to decouple the cancelation value
        # from the window lifetime.
        self.cancelation_flag = CancelationFlag()
        self.destroyed.connect(self.cancelation_flag.set_canceled)
        self._warning_dialog_canceled = False

        self._submission_complete = False
        self.__submission_thread: Optional[threading.Thread] = None
        self.submission_thread_print.connect(self.handle_print)
        self.submission_thread_hashing_progress.connect(self.handle_hashing_thread_progress_report)
        self.submission_thread_upload_progress.connect(self.handle_upload_thread_progress_report)
        self.submission_thread_succeeded.connect(self.handle_create_job_thread_succeeded)
        self.submission_thread_request_warning_dialog.connect(self.handle_request_warning_dialog)
        self.submission_thread_exception.connect(self.handle_thread_exception)

        self._build_ui()

    def start_job_submission(
        self,
        job_bundle_dir: str,
        job_parameters: list[dict[str, Any]] = [],
        **kwargs,
    ) -> None:
        """
        Starts a job submission background thread and returns immediately. It wires up
        appropriate callbacks and then forwards all arguments.

        See the documentation for deadline.client.api.create_job_from_job_bundle for
        more details.
        """

        kwargs["job_bundle_dir"] = job_bundle_dir
        kwargs["job_parameters"] = job_parameters
        kwargs["from_gui"] = True
        kwargs["submitter_name"] = kwargs.get("submitter_name", "CustomGUI")

        # The CancelationFlag object has a lifetime decoupled from the dialog. Each callback
        # always checks for cancelation before calling a method or accessing a property of the dialog,
        # and the Qt object's destroyed event is connected to set its canceled flag.
        kwargs["print_function_callback"] = partial(
            _print_function_callback, self.cancelation_flag, self
        )
        kwargs["interactive_confirmation_callback"] = partial(
            _interactive_confirmation_callback, self.cancelation_flag, self
        )
        kwargs["hashing_progress_callback"] = partial(
            _hashing_progress_callback, self.cancelation_flag, self
        )
        kwargs["upload_progress_callback"] = partial(
            _upload_progress_callback, self.cancelation_flag, self
        )
        kwargs["create_job_result_callback"] = partial(
            _create_job_result_callback, self.cancelation_flag
        )

        self.__submission_thread = threading.Thread(
            target=partial(_submission_thread_runner, self.cancelation_flag, self, kwargs),
            name="AWS Deadline Cloud Job Submission",
        )
        self.__submission_thread.start()

    def _build_ui(self):
        """Builds job submission progress UI"""
        # Remove help button from title bar
        self.setWindowFlags(
            (self.windowFlags() & ~Qt.WindowContextHelpButtonHint) | Qt.WindowCloseButtonHint
        )
        self.lyt = QVBoxLayout(self)
        self.lyt.setContentsMargins(5, 10, 5, 5)
        self.setMinimumWidth(600)
        self.setMinimumHeight(700)

        self.status_label = QLabel("Preparing files...")
        self.status_label.setMargin(5)
        self.hashing_progress = JobAttachmentsProgressWidget(
            initial_message="Preparing for hashing...", title="Hashing progress", parent=self
        )
        self.upload_progress = JobAttachmentsProgressWidget(
            initial_message="Preparing for upload...", title="Upload progress", parent=self
        )
        self.submission_log = QTextEdit()
        self.submission_log.setReadOnly(True)
        self.button_box = QDialogButtonBox(Qt.Horizontal)
        self.button_box.setStandardButtons(QDialogButtonBox.Cancel)

        self.lyt.setAlignment(Qt.AlignTop)
        self.lyt.addWidget(self.status_label)
        self.lyt.addWidget(self.hashing_progress)
        self.lyt.addWidget(self.upload_progress)
        self.lyt.addWidget(self.submission_log)
        self.lyt.addWidget(self.button_box)

        self.setWindowTitle("AWS Deadline Cloud submission")

        self.button_box.accepted.connect(self.accept)
        self.button_box.rejected.connect(self.close)

    def handle_request_warning_dialog(self, message: str, default_response: bool):
        """Presents a dialog with the provided message.

        Args:
            message (str): The message to present to the user.
            default_response (bool):
                True if the default is to continue. This adds a "Do not ask again" button as well.
                False if the default is to Cancel.
        """
        # Build the UI for user confirmation
        dialog = _JobSumissionWarningDialog(message, default_response)

        selection = dialog.exec()

        if selection == QDialog.Rejected:
            self._warning_dialog_canceled = True

        self._warning_dialog_completed = True

    def handle_print(self, message: str) -> None:
        """
        Handles the signal sent from the background thread to print messages
        to the log.
        """
        self.submission_log.append(f"{message}\n")

    def handle_hashing_thread_progress_report(
        self, progress_metadata: ProgressReportMetadata
    ) -> None:
        """
        Handles the signal sent from the background thread when reporting
        hashing progress. Sets the progress bar in the dialog based on
        the callback progress data from job attachments.
        """
        self.hashing_progress.progress_bar.setValue(int(progress_metadata.progress))
        self.hashing_progress.progress_message.setText(progress_metadata.progressMessage)

    def handle_upload_thread_progress_report(
        self, progress_metadata: ProgressReportMetadata
    ) -> None:
        """
        Handles the signal sent from the background thread when reporting
        upload progress. Sets the progress bar in the dialog based on
        the callback progress data from job attachments.
        """
        self.upload_progress.progress_bar.setValue(int(progress_metadata.progress))
        self.upload_progress.progress_message.setText(progress_metadata.progressMessage)

    def handle_create_job_thread_succeeded(self, job_id: str) -> None:
        """
        Handles the signal sent from the background CreateJob thread when the
        job creation has finished.
        """
        if job_id:
            self._submission_complete = True
            self.status_label.setText("Submission complete")
            self.button_box.setStandardButtons(QDialogButtonBox.Ok)
            self.button_box.button(QDialogButtonBox.Ok).setDefault(True)
            self.button_box.button(QDialogButtonBox.Ok).clicked.connect(
                self.progress_window_closed.emit
            )
        else:
            if self.cancelation_flag or self._warning_dialog_canceled:
                self.status_label.setText("Submission canceled")
            else:
                self.status_label.setText("Submission error")
            self.button_box.setStandardButtons(QDialogButtonBox.Close)
            self.button_box.button(QDialogButtonBox.Close).setDefault(True)

    def handle_thread_exception(self, e: BaseException) -> None:
        """
        Handles the signal sent from the background threads when an exception is
        thrown.
        """
        self.button_box.setStandardButtons(QDialogButtonBox.Close)
        self.submission_log.append(f"Error occurred: {str(e)}\n")
        logger.exception(e, exc_info=(type(e), e, e.__traceback__))

    def closeEvent(self, event: QCloseEvent) -> None:
        """
        Overrides the closeEvent function to shutdown any running threads before
        closing the dialog. If the submission is complete then any button, even
        'X', should result in the dialog being accepted.
        """
        self.progress_window_closed.emit()
        if self._submission_complete:
            self.accept()
        else:
            self.cancelation_flag.set_canceled()
            logger.info("Canceling submission...")
            self.status_label.setText("Canceling submission...")
            if self.__submission_thread is not None:
                while self.__submission_thread.is_alive():
                    QApplication.instance().processEvents()  # type: ignore[union-attr]
            super().closeEvent(event)

    def exec_(self) -> Optional[str]:  # type: ignore[override]
        """
        Runs the modal job progress dialog, returns the submitted job ID if it
        was successful, otherwise None.
        """
        if super().exec_() == QDialog.Accepted:
            return self.job_id
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


class _JobSumissionWarningDialog(QDialog):
    """
    Simple Dialog which functions similar to a QMessageBox, but with a scrollable text area.
    """

    def __init__(self, message: str, default_response: bool = False):
        """
        Simple Dialog which functions similar to a QMessageBox, but with a scrollable text area.

        Args:
            message (str): The message to present to the user.
            default_response (bool):
                True if the default response should be to continue.
                    - This also adds a "Do not ask again" button which will set settings.auto_accept to True.
                False if the default response should be to Cancel.
        """
        super().__init__()
        self.setWindowTitle("Job Submission Confirmation")
        self.message = message
        self.default_response = default_response
        self.buttons = None
        layout = QVBoxLayout(self)

        # Top section with icon and title
        top_layout = QHBoxLayout()
        icon_label = QLabel()
        icon_label.setPixmap(
            self.style()
            .standardIcon(QStyle.SP_DirIcon if default_response else QStyle.SP_MessageBoxWarning)
            .pixmap(32, 32)
        )
        top_layout.addWidget(icon_label)
        title_label = QLabel("Job submission confirmation")
        title_label.setStyleSheet("font-weight: bold;")
        top_layout.addWidget(title_label)
        top_layout.addStretch()
        layout.addLayout(top_layout, 0)  # No stretch for title section

        # Scrollable Text Area
        self.text_edit = QTextEdit()
        self.text_edit.setPlainText(message)
        self.text_edit.setReadOnly(True)
        self.text_edit.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        layout.addWidget(self.text_edit, 1)  # Stretch factor 1 for text area

        # Set minimum width and calculate optimal height
        self.text_edit.setMinimumWidth(500)
        self._calculate_optimal_size()

        self.buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)

        if default_response:
            # If the default response is to continue, add the "Do not ask again" button
            dont_ask_button = QPushButton("Do not ask again", self)
            dont_ask_button.clicked.connect(lambda: set_setting("settings.auto_accept", "true"))
            dont_ask_button.clicked.connect(self.accept)
            self.buttons.addButton(dont_ask_button, QDialogButtonBox.ActionRole)

        layout.addWidget(self.buttons)

    def _calculate_optimal_size(self):
        """Calculate optimal height based on text content and font metrics."""
        # Get font metrics for the text edit
        font_metrics = QFontMetrics(self.text_edit.font())

        # Calculate text width accounting for margins and scrollbar
        text_width = self.text_edit.minimumWidth() - 40  # Account for margins and scrollbar

        # Calculate the height needed for the text
        text_rect = font_metrics.boundingRect(0, 0, text_width, 0, Qt.TextWordWrap, self.message)
        text_height = text_rect.height()

        # Add some padding for better appearance
        padding = 20
        optimal_height = text_height + padding

        # Set reasonable bounds
        min_height = 100
        max_height = 500

        # Clamp the height to reasonable bounds
        final_height = max(min_height, min(optimal_height, max_height))

        self.text_edit.setMinimumHeight(min_height)
        # Set dialog's initial size based on content
        dialog_height = final_height + 120  # Add space for title and buttons
        self.resize(500, dialog_height)

    def showEvent(self, event):
        """Override showEvent to set the default button after the dialog is shown."""
        super().showEvent(event)

        # Set the default button after the dialog is fully shown
        if self.buttons is not None:  # This type check is to make linting pass
            if self.default_response:
                ok_button = self.buttons.button(QDialogButtonBox.Ok)
                if ok_button:
                    ok_button.setDefault(True)
                    ok_button.setAutoDefault(True)
                    ok_button.setFocus()
            else:
                cancel_button = self.buttons.button(QDialogButtonBox.Cancel)
                if cancel_button:
                    cancel_button.setDefault(True)
                    cancel_button.setAutoDefault(True)
                    cancel_button.setFocus()

    def sizeHint(self):
        """Return size hint based on content."""
        # Calculate total dialog height including other widgets
        base_height = 50  # Height for title, buttons, and margins
        text_height = self.text_edit.minimumHeight()
        total_height = base_height + text_height

        return QSize(500, total_height)
