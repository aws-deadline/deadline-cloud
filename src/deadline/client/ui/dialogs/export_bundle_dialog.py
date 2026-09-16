# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Dialog for exporting a job bundle to Queue (S3) or a local directory."""

from __future__ import annotations

import html
import os
from typing import Optional

from qtpy.QtCore import Qt  # type: ignore
from qtpy.QtWidgets import (  # type: ignore
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QRadioButton,
    QVBoxLayout,
)

from .._utils import tr, warning_banner_qss
from ...job_bundle._repository import S3BundleRepository as _S3BundleRepository


class ExportBundleDialog(QDialog):
    """Dialog to choose where to export a bundle (Queue or Local) with a name override."""

    def __init__(
        self,
        *,
        default_name: str = "",
        queue_repo: Optional[_S3BundleRepository] = None,
        queue_error: str = "",
        local_dir: str = "",
        parent=None,
    ):
        super().__init__(parent=parent)
        self.setWindowTitle(tr("Save bundle as"))
        self.setMinimumWidth(500)

        self._queue_repo = queue_repo
        self._queue_available = queue_repo is not None
        self._queue_error = queue_error
        self._local_dir = local_dir or os.path.expanduser("~")

        self._build_ui(default_name)

    @property
    def bundle_name(self) -> str:
        return self._name_edit.text().strip()

    @property
    def export_to_queue(self) -> bool:
        return self._radio_queue.isChecked()

    @property
    def local_directory(self) -> str:
        return self._location_edit.text()

    def _build_ui(self, default_name: str):
        layout = QVBoxLayout(self)

        # Name
        name_row = QHBoxLayout()
        name_row.addWidget(QLabel(f"{tr('Name')}:"))
        self._name_edit = QLineEdit(default_name)
        name_row.addWidget(self._name_edit)
        layout.addLayout(name_row)

        # Save to
        source_row = QHBoxLayout()
        source_row.addWidget(QLabel(f"{tr('Save to')}:"))
        self._radio_queue = QRadioButton(tr("Queue"))
        self._radio_queue.setEnabled(self._queue_available)
        self._radio_queue.setFocusPolicy(Qt.StrongFocus)
        self._radio_queue.toggled.connect(self._on_source_changed)
        source_row.addWidget(self._radio_queue)
        self._radio_local = QRadioButton(tr("Local"))
        self._radio_local.setFocusPolicy(Qt.StrongFocus)
        self._radio_local.toggled.connect(self._on_source_changed)
        source_row.addWidget(self._radio_local)
        source_row.addStretch()
        layout.addLayout(source_row)

        # Warning label for unavailable queue
        self._queue_warning = QLabel()
        self._queue_warning.setWordWrap(True)
        self._queue_warning.setTextFormat(Qt.RichText)
        self._queue_warning.setStyleSheet(warning_banner_qss(self))
        if not self._queue_available and self._queue_error:
            self._queue_warning.setText(
                f"\u26a0 <b>Queue unavailable:</b> {html.escape(self._queue_error)}"
            )
            self._queue_warning.setVisible(True)
        else:
            self._queue_warning.setVisible(False)
        layout.addWidget(self._queue_warning)

        # Location
        location_row = QHBoxLayout()
        location_row.addWidget(QLabel(f"{tr('Location')}:"))
        self._location_edit = QLineEdit()
        location_row.addWidget(self._location_edit)
        self._browse_button = QPushButton("...")
        self._browse_button.setFixedWidth(30)
        self._browse_button.clicked.connect(self._on_browse)
        location_row.addWidget(self._browse_button)
        layout.addLayout(location_row)

        # Buttons
        button_box = QDialogButtonBox(QDialogButtonBox.Cancel)
        self._export_button = QPushButton(tr("Save bundle as"))
        button_box.addButton(self._export_button, QDialogButtonBox.AcceptRole)
        self._export_button.clicked.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

        # Default selection
        if self._queue_available:
            self._radio_queue.setChecked(True)
        else:
            self._radio_local.setChecked(True)

    def _on_source_changed(self):
        if self._radio_queue.isChecked() and self._queue_repo:
            s3_path = self._queue_repo.root_path()
            self._location_edit.setText(s3_path)
            self._location_edit.setReadOnly(True)
            self._browse_button.setVisible(False)
        else:
            self._location_edit.setText(self._local_dir)
            self._location_edit.setReadOnly(False)
            self._browse_button.setVisible(True)

    def _on_browse(self):
        directory = QFileDialog.getExistingDirectory(
            self, tr("Select directory"), self._location_edit.text()
        )
        if directory:
            self._location_edit.setText(directory)
