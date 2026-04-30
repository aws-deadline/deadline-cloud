# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Two-phase progress dialog for bundle operations (archive/upload, download/extract).
Matches the visual style of the job attachments progress in SubmitJobProgressDialog.
"""

from __future__ import annotations

from qtpy.QtCore import Qt  # type: ignore
from qtpy.QtWidgets import (  # type: ignore
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QGroupBox,
    QLabel,
    QProgressBar,
    QVBoxLayout,
    QWidget,
)


class _PhaseWidget(QGroupBox):
    """A single phase with a progress bar and status message."""

    def __init__(self, title: str, parent=None):
        super().__init__(title=title, parent=parent)
        layout = QFormLayout(self)
        layout.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)
        self.progress_bar = QProgressBar()
        self.status_label = QLabel("")
        layout.addWidget(self.progress_bar)
        layout.addWidget(self.status_label)


class BundleProgressDialog(QDialog):
    """A modal dialog showing two-phase progress for bundle operations.

    Each phase has a titled group box with a progress bar and status label,
    matching the style of the job attachments submission progress.

    Example::

        dialog = BundleProgressDialog(
            "Saving bundle to queue",
            phase1_title="Archiving",
            phase2_title="Uploading",
            parent=self,
        )
        dialog.show()
        dialog.set_phase1_progress(50, 100, "50 MB / 100 MB")
        dialog.set_phase2_progress(0, 100, "Waiting...")
        dialog.set_complete("Bundle saved to queue.")
    """

    def __init__(
        self,
        window_title: str,
        *,
        phase1_title: str = "Phase 1",
        phase2_title: str = "Phase 2",
        parent: QWidget | None = None,
    ):
        super().__init__(parent=parent)
        self.setWindowTitle(window_title)
        self.setWindowModality(Qt.WindowModal)
        self.setMinimumWidth(450)
        self.setWindowFlags(
            (self.windowFlags() & ~Qt.WindowContextHelpButtonHint) | Qt.WindowCloseButtonHint
        )

        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)

        self._phase1 = _PhaseWidget(phase1_title)
        layout.addWidget(self._phase1)

        self._phase2 = _PhaseWidget(phase2_title)
        layout.addWidget(self._phase2)

        self._button_box = QDialogButtonBox(QDialogButtonBox.Cancel)
        self._button_box.rejected.connect(self.reject)
        layout.addWidget(self._button_box)

        self._layout = layout

    def set_phase1_progress(self, value: int, maximum: int, message: str = ""):
        self._phase1.progress_bar.setMaximum(maximum)
        self._phase1.progress_bar.setValue(value)
        if message:
            self._phase1.status_label.setText(message)

    def set_phase2_progress(self, value: int, maximum: int, message: str = ""):
        self._phase2.progress_bar.setMaximum(maximum)
        self._phase2.progress_bar.setValue(value)
        if message:
            self._phase2.status_label.setText(message)

    def set_complete(self, message: str = "Complete"):
        """Mark operation as complete — hide progress, show checkmark, change Cancel to Close."""
        self._phase1.setVisible(False)
        self._phase2.setVisible(False)

        if not hasattr(self, "_complete_label"):
            self._complete_label = QLabel()
            self._complete_label.setAlignment(Qt.AlignCenter)
            self._complete_label.setStyleSheet("font-size: 14px; padding: 20px;")
            self._layout.insertWidget(0, self._complete_label)

        self._complete_label.setText(f"\u2705 {message}")
        self._complete_label.setVisible(True)

        self._button_box.clear()
        self._button_box.addButton(QDialogButtonBox.Close)
        self._button_box.rejected.connect(self.accept)

    def set_error(self, message: str):
        """Show error state."""
        self._phase2.status_label.setText(f"\u26a0 {message}")
        self._button_box.clear()
        self._button_box.addButton(QDialogButtonBox.Close)
        self._button_box.rejected.connect(self.reject)
