# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for the DeadlineLoginDialog."""

import threading
from unittest.mock import patch

import pytest

try:
    from qtpy.QtCore import QTimer
    from qtpy.QtWidgets import QMessageBox
    from deadline.client.api._session import AwsCredentialsSource
    from deadline.client.ui._utils import tr
    from deadline.client.ui.dialogs.deadline_login_dialog import DeadlineLoginDialog
except ImportError:
    pytest.importorskip("deadline.client.ui.dialogs.deadline_login_dialog")


# Where DeadlineLoginDialog looks up the login backend. The dialog calls
# ``api.login(...)`` from its background thread, so patching this lets us drive
# the login to success or hold it open for cancellation without touching AWS.
_API_LOGIN = "deadline.client.ui.dialogs.deadline_login_dialog.api.login"


class TestDeadlineLoginDialogReturnValue:
    """
    Regression guard for the exec_()/login() return-value contract.

    A successful login calls ``self.accept()`` (QDialog.Accepted == 1); a
    cancel/reject leaves the dialog rejected. exec_() must translate those into
    True / False. The bug this guards against compared the result against
    QMessageBox.Ok (1024), so a real success-via-accept() wrongly returned False.
    """

    def test_exec_returns_true_on_successful_login(self, qtbot):
        """
        Construct the real dialog (running its full __init__, which starts the
        background login thread and connects its signals) with the login backend
        mocked to succeed. The success handler calls self.accept(), so exec_()
        must return True.
        """
        with patch(_API_LOGIN, return_value="my-profile"):
            dialog = DeadlineLoginDialog(parent=None, close_on_success=True)
            qtbot.addWidget(dialog)

            assert dialog.exec_() is True

    def test_exec_returns_true_on_success_when_not_closing_on_success(self, qtbot):
        """
        With close_on_success=False a successful login does not call accept();
        instead it swaps in an "Ok" button and waits for the user to click it.
        Clicking a QMessageBox standard button sets the result to QMessageBox.Ok
        (1024), not QDialog.Accepted (1). exec_() must still return True.
        """
        with patch(_API_LOGIN, return_value="my-profile"):
            dialog = DeadlineLoginDialog(parent=None, close_on_success=False)
            qtbot.addWidget(dialog)

            def click_ok():
                ok_button = dialog.button(QMessageBox.StandardButton.Ok)
                if ok_button is None:
                    # Success handler hasn't swapped in the Ok button yet; retry.
                    QTimer.singleShot(10, click_ok)
                    return
                ok_button.click()

            QTimer.singleShot(10, click_ok)

            assert dialog.exec_() is True

    def test_exec_returns_false_when_user_cancels(self, qtbot):
        """
        A realistic cancel: the login backend blocks until it observes the
        cancellation flag, and the user clicks the Cancel button while it is
        still running. The dialog is rejected, so exec_() must return False.
        """
        login_started = threading.Event()

        def blocking_login(on_pending_authorization, on_cancellation_check, config=None):
            # Emulate the real handshake: keep running until the dialog signals
            # cancellation (via the Cancel button setting dialog.canceled).
            login_started.set()
            while not on_cancellation_check():
                pass
            return "unused-because-canceled"

        with patch(_API_LOGIN, side_effect=blocking_login):
            dialog = DeadlineLoginDialog(parent=None, close_on_success=True)
            qtbot.addWidget(dialog)

            def click_cancel():
                # Wait until the background login is actually running, then cancel.
                if not login_started.is_set():
                    QTimer.singleShot(10, click_cancel)
                    return
                dialog.button(QMessageBox.StandardButton.Cancel).click()

            QTimer.singleShot(10, click_cancel)

            assert dialog.exec_() is False

    def test_exec_returns_false_on_login_error(self, qtbot):
        """
        When the login backend raises, the dialog shows an error and swaps in a
        Close button instead of accepting. The user closes it, so the dialog is
        rejected and exec_() must return False.
        """
        with patch(_API_LOGIN, side_effect=RuntimeError("boom")):
            dialog = DeadlineLoginDialog(parent=None, close_on_success=True)
            qtbot.addWidget(dialog)

            def close_dialog():
                close_button = dialog.button(QMessageBox.StandardButton.Close)
                if close_button is None:
                    # Error handler hasn't run yet; poll again shortly.
                    QTimer.singleShot(10, close_dialog)
                    return
                close_button.click()

            QTimer.singleShot(10, close_dialog)

            assert dialog.exec_() is False


class TestPendingAuthorizationMessage:
    """
    The dialog's text while login is in flight is the only instruction the user gets, so
    it has to name the credentials the profile actually wants. Both profile types are
    signed in by Deadline Cloud monitor, but a console profile signs in with AWS Console
    credentials, so the two messages must differ.
    """

    @staticmethod
    def _message_for(qtbot, credentials_source) -> str:
        """
        Runs a login that only fires `on_pending_authorization` with the given source,
        then blocks until cancelled, and returns the text the dialog settled on.
        """
        notified = threading.Event()

        def login(on_pending_authorization, on_cancellation_check, config=None):
            on_pending_authorization(credentials_source=credentials_source)
            notified.set()
            while not on_cancellation_check():
                pass
            return "unused-because-canceled"

        with patch(_API_LOGIN, side_effect=login):
            dialog = DeadlineLoginDialog(parent=None, close_on_success=True)
            qtbot.addWidget(dialog)

            def click_cancel():
                # The message is set via a queued signal, so wait for the callback to
                # have fired before tearing the dialog down.
                if not notified.is_set():
                    QTimer.singleShot(10, click_cancel)
                    return
                dialog.button(QMessageBox.StandardButton.Cancel).click()

            QTimer.singleShot(10, click_cancel)
            dialog.exec_()

            return dialog.text()

    def test_console_login_names_the_aws_console(self, qtbot):
        """
        A console profile's sign-in happens in Deadline Cloud monitor, but with AWS Console
        credentials -- saying only "log in" would leave the user guessing which to use.
        """
        message = self._message_for(qtbot, AwsCredentialsSource.AWS_CONSOLE_LOGIN)

        assert message == tr(
            "Opening Deadline Cloud monitor to sign in with the AWS Console. "
            "Please sign in before returning here."
        )

    def test_monitor_login_keeps_its_own_message(self, qtbot):
        """The monitor profile's wording must not pick up the console phrasing."""
        message = self._message_for(qtbot, AwsCredentialsSource.DEADLINE_CLOUD_MONITOR_LOGIN)

        assert message == tr("Opening Deadline Cloud monitor. Please log in before returning here.")
