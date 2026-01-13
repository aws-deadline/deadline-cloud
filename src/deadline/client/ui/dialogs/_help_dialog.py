# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Provides a modal dialog box for displaying submitter and environment information about the open submitter.
"""

from __future__ import annotations

import logging
import yaml

from dataclasses import asdict
from typing import List, Dict
from qtpy.QtWidgets import (  # pylint: disable=import-error; type: ignore
    QApplication,
    QDialog,
    QDialogButtonBox,
    QLabel,
    QMessageBox,
    QPushButton,
    QTextEdit,
    QVBoxLayout,
)
from qtpy.QtCore import Qt  # pylint: disable=import-error

from ...dataclasses import SubmitterInfo
from ..dataclasses._environment_info import _EnvironmentInfo
from .._utils import tr

logger = logging.getLogger(__name__)


# Hard-coded documentation links
_DOCUMENTATION_LINKS: List[Dict[str, str]] = [
    {"name": "User Guide", "url": "https://aws-deadline.github.io"},
]


class _HelpDialog(QDialog):
    """
    Modal dialog that displays submitter and environment information about the deadline-cloud
    library and the environment it's running in.

    The dialog displays all of the following if available:
    - Submitter name
    - Documentation link
    - Deadline Cloud library version
    - Submitter package name and version
    - Host application name and version
    - Additional application provided information
    - Python version
    - Operating system version
    - Qt version
    """

    def __init__(self, submitter_info: SubmitterInfo, parent=None):
        """
        Initialize the Help dialog.

        Args:
            submitter_info: SubmitterInfo object containing submitter details
            parent: Parent widget (optional)
        """
        super().__init__(parent=parent)

        self.submitter_info = submitter_info
        self.environment_info = _EnvironmentInfo.collect()

        self.setWindowTitle(
            f"{tr('About')} Deadline Cloud {submitter_info.submitter_name} {tr('Submitter')}"
        )
        self.setModal(True)
        self.setMinimumWidth(500)

        self._build_ui()

    def _build_ui(self):
        """Build the dialog UI layout."""
        layout = QVBoxLayout()

        # Add documentation links section (hard-coded)
        for link_info in _DOCUMENTATION_LINKS:
            name = link_info["name"]
            url = link_info["url"]
            doc_label = QLabel()
            doc_label.setText(f"<a href='{url}'>{name}</a>")
            doc_label.setToolTip(url)
            doc_label.setOpenExternalLinks(True)
            doc_label.setTextInteractionFlags(Qt.TextBrowserInteraction)
            doc_label.setWordWrap(True)
            layout.addWidget(doc_label)

        self.version_text = QTextEdit()
        self.version_text.setReadOnly(True)
        self.version_text.setPlainText(self._format_version_info())
        self.version_text.setMinimumHeight(200)
        layout.addWidget(self.version_text)

        button_box = QDialogButtonBox()

        self.copy_button = QPushButton(tr("Copy"))
        self.copy_button.clicked.connect(self._on_copy_clicked)
        button_box.addButton(self.copy_button, QDialogButtonBox.ActionRole)

        close_button = button_box.addButton(QDialogButtonBox.Close)
        close_button.setText(tr("Close"))
        close_button.clicked.connect(self.accept)

        layout.addWidget(button_box)

        self.setLayout(layout)

    @staticmethod
    def _make_keys_human_readable(data):
        """
        Recursively replace underscores with spaces in dictionary keys.

        Args:
            data: The data structure to process (dict, list, or scalar value)

        Returns:
            The data structure with all dictionary keys made human-readable
        """
        if isinstance(data, dict):
            return {
                k.replace("_", " "): _HelpDialog._make_keys_human_readable(v)
                for k, v in data.items()
            }
        elif isinstance(data, list):
            return [_HelpDialog._make_keys_human_readable(item) for item in data]
        else:
            return data

    def _format_version_info(self) -> str:
        """
        Combine submitter and version information for display.

        Returns:
            Formatted yaml string with submitter and version information
        """

        env_data = asdict(self.environment_info)
        submitter_data = asdict(self.submitter_info)

        # Filter out None values from submitter data
        submitter_dict = {k: v for k, v in submitter_data.items() if v is not None}

        combined_info = {**submitter_dict, **env_data}

        # Replace underscores with spaces in keys for user-friendly display (recursively)
        friendly_info = self._make_keys_human_readable(combined_info)

        yaml_str = yaml.dump(friendly_info, indent=4, sort_keys=False)
        return yaml_str

    def _format_for_copy(self) -> str:
        """
        Format all information for copy/pasting with header.

        Returns:
            Formatted string suitable for copying and pasting
        """
        header = "AWS Deadline Cloud Submitter Information"
        lines = [header, "=" * len(header)]
        lines.append(self._format_version_info())
        return "\n".join(lines)

    def _on_copy_clicked(self):
        """Handle the Copy button click."""
        try:
            clipboard = QApplication.clipboard()
            if clipboard:
                formatted_text = self._format_for_copy()
                clipboard.setText(formatted_text)
                logger.info("Submitter information copied to clipboard")
            else:
                logger.warning("Clipboard is not available")
                QMessageBox.warning(
                    self,
                    "Copy Failed",
                    "Unable to access the system clipboard.",
                )
        except Exception as e:
            logger.error(f"Failed to copy version info: {e}")
            QMessageBox.warning(
                self,
                "Copy Failed",
                f"Failed to copy version information to clipboard: {str(e)}",
            )
