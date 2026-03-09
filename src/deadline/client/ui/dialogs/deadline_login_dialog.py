# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
Provides a modal dialog box for logging in to AWS Deadline Cloud.

Example code:
    from deadline.client.ui.dialogs import DeadlineLoginDialog
    if DeadlineLoginDialog.login(parent=self):
        print("Logged in successfully.")
    else:
        print("Failed to log in.")
"""

__all__ = ["DeadlineLoginDialog"]

import html
from configparser import ConfigParser
from typing import Optional

from qtpy.QtCore import Qt, Signal
from .._utils import tr
from ..controllers import AsyncTaskRunner
from qtpy.QtWidgets import (  # pylint: disable=import-error; type: ignore
    QApplication,
    QMessageBox,
    QWidget,
)

from ... import api
from ...api._session import AwsCredentialsSource


class DeadlineLoginDialog(QMessageBox):
    """
    A modal dialog box for logging in to AWS Deadline Cloud. The return value
    of the static DeadlineLoginDialog.login() and the modal exec()
    is True when the login is successful, False otherwise.

    Example code:
        if DeadlineLoginDialog.login(parent=self):
            print("Logged in successfully.")
        else:
            print("Failed to log in.")
    """

    # This signal is sent when the background login thread raises an exception.
    login_thread_exception = Signal(BaseException)
    # This signal is sent when the background login thread wants to change the
    # displayed message.
    login_thread_message = Signal(str)
    # This signal is sent when the background login thread succeeds.
    login_thread_succeeded = Signal(str)

    @staticmethod
    def login(
        parent: Optional[QWidget] = None,
        force_refresh=False,
        close_on_success=True,
        config: Optional[ConfigParser] = None,
    ) -> bool:
        """
        Static method that runs the Deadline Login Dialog. Returns True for
        a successful login, False otherwise.

        Args:
            force_refresh (bool, default False): Forces a re-login even when already authorized.
            close_on_success (bool, default True): Closes the dialog on successful login, instead
                   of showing a "successfully logged in" message.
            config (ConfigParser, optional): The AWS Deadline Cloud configuration
                    object to use instead of the config file.
        """
        deadline_login = DeadlineLoginDialog(
            parent=parent,
            close_on_success=close_on_success,
            config=config,
        )
        return deadline_login.exec_()

    def __init__(
        self,
        parent: Optional[QWidget] = None,
        close_on_success=True,
        config: Optional[ConfigParser] = None,
    ) -> None:
        super().__init__(parent=parent)

        self.close_on_success = close_on_success
        self.config = config
        self.canceled = False

        # Use AsyncTaskRunner for background login
        self._runner = AsyncTaskRunner(self)
        self._runner.task_error.connect(self._handle_task_error, Qt.QueuedConnection)

        self.login_thread_exception.connect(self.handle_login_thread_exception)
        self.login_thread_message.connect(self.handle_login_thread_message)
        self.login_thread_succeeded.connect(self.handle_login_thread_succeeded)
        self.buttonClicked.connect(self.on_button_clicked)

        self.setWindowTitle(tr("Log in to AWS Deadline Cloud"))
        self.setText(tr("Logging you in..."))
        self.setStandardButtons(QMessageBox.Cancel)

        self._start_login()

    def _handle_task_error(self, operation_name: str, error: BaseException) -> None:
        """Handle errors from the async runner."""
        self.login_thread_exception.emit(error)

    def _login_background_task(self) -> str:
        """
        This function runs in a background thread to perform the login handshake.
        It polls the `self.canceled` flag for cancellation.
        """

        def on_pending_authorization(**kwargs):
            if kwargs["credentials_source"] == AwsCredentialsSource.DEADLINE_CLOUD_MONITOR_LOGIN:
                self.login_thread_message.emit(
                    tr("Opening Deadline Cloud monitor. Please log in before returning here.")
                )

        def on_cancellation_check():
            return self.canceled

        return api.login(
            on_pending_authorization,
            on_cancellation_check,
            config=self.config,
        )

    def _on_login_success(self, success_message: str) -> None:
        """Handle successful login."""
        self.login_thread_succeeded.emit(success_message)

    def _on_login_error(self, error: BaseException) -> None:
        """Handle login error."""
        self.login_thread_exception.emit(error)

    def _start_login(self) -> None:
        """
        Starts the background login task.
        """
        self._runner.run(
            operation_key="login",
            fn=self._login_background_task,
            on_success=self._on_login_success,
            on_error=self._on_login_error,
        )

    def handle_login_thread_exception(self, e: BaseException) -> None:
        """
        Handles the signal sent from the background login thread when
        an exception is thrown.
        """
        self.setStandardButtons(QMessageBox.Close)
        self.setIcon(QMessageBox.Warning)
        self.setText(
            tr("Failed to log in to AWS Deadline Cloud:<br/><br/>{error}").format(
                error=html.escape(str(e))
            )
        )

    def handle_login_thread_message(self, message: str) -> None:
        """
        Handles the signal sent from the background login thread when
        the message needs to be set.
        """
        self.setText(message)

    def handle_login_thread_succeeded(self, success_message: str) -> None:
        """
        Handles the signal sent from the background login thread when
        the login has succeeded.
        """
        if self.close_on_success:
            # Effectively clicks on "OK"
            self.accept()
        else:
            self.setStandardButtons(QMessageBox.Ok)
            self.setIcon(QMessageBox.Information)
            self.setText(
                tr("Successfully logged into: <br/><br/>{profile}").format(
                    profile=html.escape(success_message)
                )
            )

    def on_button_clicked(self, button):
        if self.standardButton(button) == QMessageBox.Cancel:
            # Tell the login task to cancel
            self.canceled = True
            self._runner.cancel_all()
            # Process events to allow the task to complete
            while self._runner.is_running("login"):
                QApplication.instance().processEvents()  # type: ignore[union-attr]

    def exec_(self) -> bool:
        """
        Runs the modal login dialog, returning True if the login was
        successful, False otherwise.
        """
        return super().exec_() == QMessageBox.Ok
