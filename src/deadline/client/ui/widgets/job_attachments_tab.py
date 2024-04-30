# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
UI widgets for the job attachments tab.
"""
from __future__ import annotations
import os
from logging import getLogger
from typing import Optional

from qtpy.QtWidgets import (  # type: ignore
    QAbstractItemView,
    QFileDialog,
    QHBoxLayout,
    QCheckBox,
    QListWidget,
    QListWidgetItem,
    QPushButton,
    QSizePolicy,
    QLabel,
    QVBoxLayout,
    QWidget,
    QGroupBox,
    QMessageBox,
)

from ...job_bundle.submission import AssetReferences
from .. import block_signals

logger = getLogger(__name__)


class JobAttachmentsWidget(QWidget):
    """
    The Widget for showing which files and folders will be attached, and to let the user configure more.
    The files and folders in auto_detected_attachments cannot be removed from the list, but they can
    be hidden or shown.

    Args:
        auto_detected_attachments (FlatAssetReferences): The job attachments that were automatically detected
            from the input document/scene file or starting job bundle.
        attachments: (FlatAssetReferences): The job attachments that have been added to the job by the user.
    """

    def __init__(
        self,
        auto_detected_attachments: AssetReferences,
        attachments: AssetReferences,
        parent: QWidget = None,
    ) -> None:
        super().__init__(parent=parent)
        self.auto_detected_attachments = auto_detected_attachments
        self.attachments = attachments

        self._build_ui()
        self._populate_attachment_lists()

    def _build_ui(self) -> None:
        tab_layout = QVBoxLayout(self)

        # Create a group box for general settings
        size_policy = QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        size_policy.setVerticalStretch(10)
        self.general_group = QGroupBox("General submission settings", self)
        tab_layout.addWidget(self.general_group)
        self.general_group.setSizePolicy(size_policy)
        general_layout = QVBoxLayout(self.general_group)

        # Create a group box for each type of attachment
        self.input_files_group = QGroupBox("Attach input files", self)
        size_policy = QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        size_policy.setVerticalStretch(80)
        tab_layout.addWidget(self.input_files_group)
        self.input_files_group.setSizePolicy(size_policy)
        input_files_layout = QVBoxLayout(self.input_files_group)

        self.input_directories_group = QGroupBox("Attach input directories", self)
        size_policy = QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        size_policy.setVerticalStretch(10)
        self.input_directories_group.setSizePolicy(size_policy)
        tab_layout.addWidget(self.input_directories_group)
        input_directories_layout = QVBoxLayout(self.input_directories_group)

        self.output_directories_group = QGroupBox("Specify output directories", self)
        size_policy = QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        size_policy.setVerticalStretch(10)
        self.output_directories_group.setSizePolicy(size_policy)
        tab_layout.addWidget(self.output_directories_group)
        output_directories_layout = QVBoxLayout(self.output_directories_group)

        # General settings
        self.general_settings = JobAttachmentsGeneralWidget(self)
        general_layout.addWidget(self.general_settings)

        # The "Attach Input Files" attachments
        self.input_files_controls = JobAttachmentsControlsWidget(self)
        self.input_files_controls.show_auto_detected.stateChanged.connect(
            self.show_auto_detected_change
        )
        self.input_files_controls.add.clicked.connect(self._add_input_files)
        self.input_files_controls.remove_selected.clicked.connect(self._remove_selected_input_files)
        self.input_files = QListWidget(parent=self)
        self.input_files.itemSelectionChanged.connect(self._update_status_messages)
        self.input_files.setSortingEnabled(False)
        self.input_files.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.input_files.setAlternatingRowColors(True)

        input_files_layout.addWidget(self.input_files_controls)
        input_files_layout.addWidget(self.input_files)

        # The "Attach input directories" attachments
        self.input_directories_controls = JobAttachmentsControlsWidget(self)
        self.input_directories_controls.show_auto_detected.stateChanged.connect(
            self.show_auto_detected_change
        )
        self.input_directories_controls.add.clicked.connect(self._add_input_directory)
        self.input_directories_controls.remove_selected.clicked.connect(
            self._remove_selected_input_directories
        )
        self.input_directories = QListWidget(parent=self)
        self.input_directories.itemSelectionChanged.connect(self._update_status_messages)
        self.input_directories.setSortingEnabled(False)
        self.input_directories.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.input_directories.setAlternatingRowColors(True)

        input_directories_layout.addWidget(self.input_directories_controls)
        input_directories_layout.addWidget(self.input_directories)

        # The "Specify output directories" attachments
        self.output_directories_controls = JobAttachmentsControlsWidget(self)
        self.output_directories_controls.show_auto_detected.stateChanged.connect(
            self.show_auto_detected_change
        )
        self.output_directories_controls.add.clicked.connect(self._add_output_directory)
        self.output_directories_controls.remove_selected.clicked.connect(
            self._remove_selected_output_directories
        )
        self.output_directories = QListWidget(parent=self)
        self.output_directories.itemSelectionChanged.connect(self._update_status_messages)
        self.output_directories.setSortingEnabled(False)
        self.output_directories.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.output_directories.setAlternatingRowColors(True)

        output_directories_layout.addWidget(self.output_directories_controls)
        output_directories_layout.addWidget(self.output_directories)

        self.attachments_controls = self._build_attachment_controls()

    def _build_attachment_controls(
        self,
    ) -> list[tuple[set[str], set[str], QListWidget, JobAttachmentsControlsWidget]]:
        # Put all the controls in a list of tuples for structured processing
        return [
            (
                self.auto_detected_attachments.input_filenames,
                self.attachments.input_filenames,
                self.input_files,
                self.input_files_controls,
            ),
            (
                self.auto_detected_attachments.input_directories,
                self.attachments.input_directories,
                self.input_directories,
                self.input_directories_controls,
            ),
            (
                self.auto_detected_attachments.output_directories,
                self.attachments.output_directories,
                self.output_directories,
                self.output_directories_controls,
            ),
        ]

    def refresh_ui(
        self,
        auto_detected_attachments: Optional[AssetReferences],
        attachments: Optional[AssetReferences],
    ):
        """Refresh the job attachment lists if provided"""
        if auto_detected_attachments:
            self.auto_detected_attachments = auto_detected_attachments
        if attachments:
            self.attachments = attachments
        self.attachments_controls = self._build_attachment_controls()
        self._populate_attachment_lists()

    def _set_attachments_list(
        self, list_widget: QListWidget, auto_detected_paths: set[str], paths: set[str]
    ):
        with block_signals(list_widget):
            list_widget.clear()
            if auto_detected_paths:
                italic_font = list_widget.font()
                italic_font.setItalic(True)
                for path in auto_detected_paths:
                    item = QListWidgetItem()
                    item.setText(path)
                    item.setFont(italic_font)
                    list_widget.addItem(item)
            list_widget.addItems(list(paths))
            list_widget.sortItems()

    def _populate_attachment_lists(self) -> None:
        """Initializes the lists of job attachments. Removes any auto-detected attachments from the added attachments sets."""
        for auto_detected_set, added_set, list_widget, controls_widget in self.attachments_controls:
            added_set.difference_update(auto_detected_set)
            if controls_widget.show_auto_detected.isChecked():
                self._set_attachments_list(
                    list_widget,
                    auto_detected_set,
                    added_set,
                )
            else:
                self._set_attachments_list(
                    list_widget,
                    set(),
                    added_set,
                )
        self._update_status_messages()

    def show_auto_detected_change(self, _: int):
        # When one of the "Show Auto-detected" checkboxes changes state, re-populate the lists
        self._populate_attachment_lists()

    def _update_status_messages(self) -> None:
        """Updates the status messages for each list to have the counts."""
        for auto_detected_set, added_set, list_widget, controls_widget in self.attachments_controls:
            selected_count = len(list_widget.selectedItems())
            controls_widget.status_message.setText(
                f"{len(auto_detected_set)} auto, {len(added_set)} added, {selected_count} selected"
            )

    def _add_input_files(self) -> None:
        new_files, _ = QFileDialog.getOpenFileNames(
            self, "Select input files to attach to your job"
        )

        if new_files:
            # Normalize the paths
            paths = set(os.path.normpath(path) for path in new_files)
            # Remove any that are from the auto-detected set
            paths.difference_update(self.auto_detected_attachments.input_filenames)
            # Add them to the attachments
            self.attachments.input_filenames.update(paths)
            self._populate_attachment_lists()

    def _remove_selected_input_files(self) -> None:
        selected_files = [item.text() for item in self.input_files.selectedItems()]
        unremoved_files = self.auto_detected_attachments.input_filenames.intersection(
            selected_files
        )
        # Remove the selected items only from the added attachments, not the auto-detected ones
        self.attachments.input_filenames.difference_update(selected_files)
        self._populate_attachment_lists()
        if unremoved_files:
            QMessageBox.warning(
                self,
                "Some files were not removed",
                f"The selected files from the auto-detected list ({len(unremoved_files)} items) were not removed.",
            )

    def _add_input_directory(self) -> None:
        new_dir = QFileDialog.getExistingDirectory(self, "Select a directory to attach to your job")

        if new_dir:
            # Normalize the path
            new_dir = os.path.normpath(new_dir)
            # Check if it's in the auto-detected set
            if new_dir not in self.auto_detected_attachments.input_directories:
                # Add it to the attachments
                self.attachments.input_directories.add(new_dir)
                self._populate_attachment_lists()

    def _remove_selected_input_directories(self) -> None:
        selected_dirs = [item.text() for item in self.input_directories.selectedItems()]
        unremoved_dirs = self.auto_detected_attachments.input_directories.intersection(
            selected_dirs
        )
        # Remove the selected items only from the added attachments, not the auto-detected ones
        self.attachments.input_directories.difference_update(selected_dirs)
        self._populate_attachment_lists()
        if unremoved_dirs:
            QMessageBox.warning(
                self,
                "Some directories were not removed",
                f"The selected directories from the auto-detected list ({len(unremoved_dirs)} items) were not removed.",
            )

    def _add_output_directory(self) -> None:
        new_dir = QFileDialog.getExistingDirectory(self, "Select an output directory of your job")

        if new_dir:
            # Normalize the path
            new_dir = os.path.normpath(new_dir)
            # Check if it's in the auto-detected set
            if new_dir not in self.auto_detected_attachments.output_directories:
                # Add it to the attachments
                self.attachments.output_directories.add(new_dir)
                self._populate_attachment_lists()

    def _remove_selected_output_directories(self) -> None:
        selected_dirs = [item.text() for item in self.output_directories.selectedItems()]
        unremoved_dirs = self.auto_detected_attachments.output_directories.intersection(
            selected_dirs
        )
        # Remove the selected items only from the added attachments, not the auto-detected ones
        self.attachments.output_directories.difference_update(selected_dirs)
        self._populate_attachment_lists()
        if unremoved_dirs:
            QMessageBox.warning(
                self,
                "Some directories were not removed",
                f"The selected directories from the auto-detected list ({len(unremoved_dirs)} items) were not removed.",
            )

    def get_asset_references(self) -> AssetReferences:
        """
        Creates an asset_references object that can be saved as the
        asset_references.json|yaml file in a Job Bundle.
        """
        return self.auto_detected_attachments.union(self.attachments)

    def get_require_paths_exist(self) -> bool:
        """
        Returns the checkbox value of whether to allow empty paths or not.
        """
        return self.general_settings.require_paths_exist.isChecked()


class JobAttachmentsGeneralWidget(QWidget):
    """
    A Widget that contains general settings for a specific submission.
    """

    def __init__(self, parent=None) -> None:
        super().__init__(parent=parent)

        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        self.require_paths_exist = QCheckBox("Require all input paths exist", parent=self)
        layout.addWidget(self.require_paths_exist)


class JobAttachmentsControlsWidget(QWidget):
    """
    A Widget that contains buttons for the actions to add/remove files or directories
    in a job attachments list.
    """

    def __init__(self, parent=None) -> None:
        super().__init__(parent=parent)

        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        self.add = QPushButton("Add...", parent=self)
        layout.addWidget(self.add)

        self.remove_selected = QPushButton("Remove selected", parent=self)
        layout.addWidget(self.remove_selected)

        self.status_message = QLabel("0 total")
        self.status_message.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
        layout.addWidget(self.status_message)

        self.show_auto_detected = QCheckBox("Show auto-detected", parent=self)
        self.show_auto_detected.setChecked(True)
        layout.addWidget(self.show_auto_detected)
