# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Provides a dialog to notify users about available updates for Deadline Cloud integrations.
"""

from __future__ import annotations

import logging
import webbrowser
from typing import Optional

from qtpy.QtWidgets import (  # pylint: disable=import-error; type: ignore
    QDialog,
    QDialogButtonBox,
    QLabel,
    QMessageBox,
    QPushButton,
    QVBoxLayout,
    QWidget,
)
from qtpy.QtCore import Qt  # pylint: disable=import-error

from .._utils import tr
from ...config import config_file

logger = logging.getLogger(__name__)

# AWS brand colors used for accent elements
_COLOR_ACCENT = "#ec7211"
_COLOR_ACCENT_HOVER = "#f59942"


class UpdateAvailableDialog(QDialog):
    """
    Dialog to notify users about available updates for Deadline Cloud integrations.

    Displays:
    - Integration name
    - Current version
    - Available version
    - Download button (if URL available)
    - Dismiss button

    After exec_(), check `user_downloaded` to see if the user clicked Download.
    If True, the caller should skip opening the submitter and let the user restart.
    """

    def __init__(
        self,
        integration_name: str,
        current_version: str,
        latest_version: str,
        download_url: str,
        release_notes_url: Optional[str] = None,
        parent: Optional[QWidget] = None,
    ):
        """
        Initialize the Update Available dialog.

        Args:
            integration_name: Human-readable name of the integration (e.g., "Cinema 4D")
            current_version: Currently installed version (e.g., "0.9.1")
            latest_version: Latest available version (e.g., "0.9.2")
            download_url: URL to download the update
            release_notes_url: Optional URL to the release notes page
            parent: Parent widget (optional)
        """
        super().__init__(
            parent=parent,
            f=Qt.WindowSystemMenuHint | Qt.WindowTitleHint | Qt.WindowCloseButtonHint,
        )

        self.integration_name = integration_name
        self.current_version = current_version
        self.latest_version = latest_version
        self.download_url = download_url
        self.release_notes_url = release_notes_url
        self.user_downloaded = False

        self.setWindowTitle(tr("New version available"))
        self.setMinimumWidth(400)

        self._build_ui()

    def _build_ui(self) -> None:
        """Build the dialog UI layout."""
        layout = QVBoxLayout()
        layout.setSpacing(8)
        layout.setContentsMargins(20, 20, 20, 20)

        # Main headline: "Version X.Y.Z of Deadline Cloud for {integration} submitter is now available."
        headline = QLabel(
            "<b>"
            + tr(
                "Version {latest_version} of Deadline Cloud for"
                " {integration_name} submitter is now available."
            ).format(
                latest_version=self.latest_version,
                integration_name=self.integration_name,
            )
            + "</b>"
        )
        headline.setAlignment(Qt.AlignLeft)
        headline.setWordWrap(True)
        layout.addWidget(headline)

        # Version comparison: "Current: X.Y.Z  ->  New: X.Y.Z"
        version_label = QLabel(
            tr("Current: {current_version}  ->  New: <b>{latest_version}</b>").format(
                current_version=self.current_version,
                latest_version=self.latest_version,
            )
        )
        version_label.setAlignment(Qt.AlignLeft)
        layout.addWidget(version_label)

        # Release notes link (if URL provided)
        if self.release_notes_url:
            self.release_notes_button = QPushButton(tr("View release notes"))
            self.release_notes_button.setFlat(True)
            self.release_notes_button.setCursor(Qt.PointingHandCursor)
            self.release_notes_button.setStyleSheet(
                f"QPushButton {{ color: {_COLOR_ACCENT}; text-decoration: underline;"
                f" border: none; background: transparent; padding: 0; text-align: left; }}"
                f"QPushButton:hover {{ color: {_COLOR_ACCENT_HOVER}; }}"
            )
            self.release_notes_button.clicked.connect(self._on_release_notes_clicked)
            layout.addWidget(self.release_notes_button, alignment=Qt.AlignLeft)

        # Add spacing before buttons
        layout.addSpacing(12)

        # Button box - consistent with other dialogs
        self.button_box = QDialogButtonBox(Qt.Horizontal)

        _button_base = "border: 1px solid #888; border-radius: 6px; padding: 3px 12px;"

        self.dismiss_button = self.button_box.addButton(QDialogButtonBox.Close)
        self.dismiss_button.setText(tr("Dismiss"))
        self.dismiss_button.setStyleSheet(f"QPushButton {{ {_button_base} }}")

        self.dont_remind_button = QPushButton(tr("Don't remind me again"))
        self.dont_remind_button.setStyleSheet(f"QPushButton {{ {_button_base} }}")
        self.button_box.addButton(self.dont_remind_button, QDialogButtonBox.DestructiveRole)
        self.dont_remind_button.clicked.connect(self._on_dont_remind_clicked)

        self.download_button = QPushButton(tr("Download installer"))
        self.download_button.setStyleSheet(
            f"QPushButton {{ {_button_base} background-color: {_COLOR_ACCENT}; color: white; }}"
        )
        self.button_box.addButton(self.download_button, QDialogButtonBox.AcceptRole)
        self.download_button.clicked.connect(self._on_download_clicked)

        self.button_box.rejected.connect(self.accept)

        layout.addWidget(self.button_box)
        self.setLayout(layout)

    def _on_download_clicked(self) -> None:
        """Handle the Download button click by opening the download URL in the default browser."""
        try:
            webbrowser.open(self.download_url)
            logger.info(f"Opened download URL: {self.download_url}")
            self.user_downloaded = True
        except Exception as e:
            logger.error(f"Failed to open download URL: {e}")
            return

        # Show restart reminder and close the dialog
        QMessageBox.information(
            self,
            tr("Application Restart Required"),
            tr(
                "Please run the installer and then restart {integration_name} to use the new version."
            ).format(integration_name=self.integration_name),
        )
        self.accept()

    def _on_release_notes_clicked(self) -> None:
        """Handle the release notes button click by opening the URL in the default browser."""
        if self.release_notes_url:
            try:
                webbrowser.open(self.release_notes_url)
                logger.info(f"Opened release notes URL: {self.release_notes_url}")
            except Exception as e:
                logger.error(f"Failed to open release notes URL: {e}")

    def _on_dont_remind_clicked(self) -> None:
        """Disable update notifications and close the dialog."""
        try:
            config_file.set_setting("settings.submitter_update_notification", "false")
            logger.info("Update notifications disabled by user")
        except Exception as e:
            logger.error(f"Failed to save notification preference: {e}")
        self.accept()
