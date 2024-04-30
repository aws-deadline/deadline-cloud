# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

# Only needed for access to command line arguments
import os
import signal
import sys
from logging import getLogger
from pathlib import Path

from qtpy.QtCore import Qt
from qtpy.QtGui import QColor, QIcon, QPalette
from qtpy.QtWidgets import QApplication, QFileDialog, QMainWindow, QStyleFactory

from .. import api
from .cli_job_submitter import show_cli_job_submitter
from .dialogs import DeadlineConfigDialog, DeadlineLoginDialog
from .job_bundle_submitter import show_job_bundle_submitter

logger = getLogger(__name__)


class DevMainWindow(QMainWindow):
    def __init__(self, parent=None):
        super().__init__(parent=parent)

        self.mainColor = QColor(64, 64, 64)
        self.selectionColor = QColor(37, 200, 25)
        self.setMinimumSize(400, 400)
        self.setup_ui()

        signal.signal(signal.SIGINT, self.signal_handler)
        if os.name != "nt":
            signal.signal(signal.SIGQUIT, self.signal_handler)  # type: ignore[attr-defined]
            signal.signal(signal.SIGTERM, self.signal_handler)  # type: ignore[attr-defined]

        # Remove the central widget. This leaves us with just dockable widgets, which provides
        # the most flexibility, since we don't really have a "main" widget.
        self.setCentralWidget(None)  # type: ignore[arg-type]

        self.setDockOptions(
            QMainWindow.AllowNestedDocks | QMainWindow.AllowTabbedDocks | QMainWindow.AnimatedDocks
        )

    def setup_ui(self):
        submit = self.menuBar().addMenu("&Submit job")
        submit.addAction("Submit job bundle...", self.submit_job_bundle)
        submit.addAction("Submit CLI job...", self.submit_cli_job)
        account = self.menuBar().addMenu("&Account")
        account.addAction("AWS Deadline Cloud workstation configuration...", self.configure)
        account.addAction("Log in to AWS Deadline Cloud...", self.login)
        account.addAction("Log out of AWS Deadline Cloud...", self.logout)

        # Set up status bar
        # self.statusBar().setStyleSheet("QStatusBar::item{ border: none; }")
        self.statusBar().showMessage("Testing!")

    def submit_job_bundle(self):
        input_job_bundle_dir = os.path.normpath(
            os.path.join(__file__, "../resources/cli_job_bundle")
        )
        input_job_bundle_dir = QFileDialog.getExistingDirectory(
            self, "Choose job bundle directory", input_job_bundle_dir
        )
        if input_job_bundle_dir:
            show_job_bundle_submitter(
                input_job_bundle_dir=os.path.normpath(input_job_bundle_dir),
                browse=True,
                parent=self,
                f=Qt.Tool,
            )

    def submit_cli_job(self):
        show_cli_job_submitter(self, f=Qt.Tool)

    def configure(self):
        DeadlineConfigDialog.configure_settings(parent=self)

    def login(self):
        if DeadlineLoginDialog.login(parent=self):
            logger.info("Logged in successfully")
        else:
            logger.info("Failed to log in")

    def logout(self):
        api.logout()

    def signal_handler(self, signal, frame):
        self.close()


def app() -> None:
    app = QApplication(sys.argv)

    # Set the style.
    app.setStyle(QStyleFactory.create("fusion"))

    # Set the application info.
    app.setApplicationName("AWS Deadline Cloud client test GUI")
    app.setOrganizationName("AWS")
    app.setOrganizationDomain("https://aws.amazon.com/")
    icon = QIcon(str(Path(__file__).parent / "resources" / "deadline_logo.svg"))
    app.setWindowIcon(icon)

    # Apply the stylesheet.
    pal = QPalette(QColor(64, 64, 64))
    pal.setColor(QPalette.Highlight, QColor(37, 200, 25))
    pal.setColor(QPalette.HighlightedText, QColor(0, 0, 0))
    pal.setColor(QPalette.Link, QColor(96, 185, 250))
    app.setPalette(pal)

    window = app.palette().color(QPalette.Window)
    selection = app.palette().color(QPalette.Highlight)
    app.setStyleSheet(
        """
        *{ selection-background-color: rgb("""
        + str(selection.red())
        + """, """
        + str(selection.green())
        + """, """
        + str(selection.blue())
        + """); }
        QMenu {background-color: rgb("""
        + str(window.red())
        + """, """
        + str(window.green())
        + """, """
        + str(window.blue())
        + """); menu-scrollable: 1;}
        QToolTip{ color: black;}
        QDockWidget {titlebar-close-icon: url(:/ThinkboxUI/Bitmaps/Close2_Dark.png);
                     titlebar-normal-icon: url(:/ThinkboxUI/Bitmaps/Undock2_Dark.png);}
        QDockWidget::close-button, QDockWidget::float-button {min-width: 18px; min-height: 18px; icon-size: 12px;}
        QDockWidget::float-button {position: relative; right: 20px; top: 2px;}
        QDockWidget::close-button {position: relative; right: 1px; top: 2px;}
        QDockWidget::title { text-align: center;}
        QDockWidget::title {background: rgb("""
        + str(max(0, (window.red() - 10)))
        + """, """
        + str(max(0, (window.green() - 10)))
        + """, """
        + str(max(0, (window.blue() - 10)))
        + """);}"""
    )

    main_window = DevMainWindow()
    main_window.show()

    main_window.submit_job_bundle()

    app.exec_()
