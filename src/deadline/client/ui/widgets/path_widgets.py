# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

__all__ = ["InputFilePickerWidget", "OutputFilePickerWidget", "DirectoryPickerWidget"]

import os

from qtpy.QtCore import Signal
from qtpy.QtWidgets import (  # pylint: disable=import-error; type: ignore
    QFileDialog,
    QHBoxLayout,
    QLineEdit,
    QPushButton,
    QWidget,
)

from .. import block_signals


class _FileWidget(QWidget):
    # Emitted when the file changes
    path_changed = Signal(str)

    def __init__(
        self,
        initial_filename: str,
        file_label: str,
        filter: str,
        selected_filter: str,
        collapse_user_dir: bool,
        parent,
    ):
        super().__init__(parent)
        self._build_ui()
        with block_signals(self.filename_edit):
            self.filename_edit.setText(initial_filename)
        self.file_label = file_label
        self.filter = filter
        self.selected_filter = selected_filter
        self.collapse_user_dir = collapse_user_dir

    def _build_ui(self):
        self.filename_edit = QLineEdit(parent=self)
        layout = QHBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self.filename_edit)
        self.choose_file_button = QPushButton("...")
        self.choose_file_button.setFixedSize(30, 22)
        layout.addWidget(self.choose_file_button)
        self.filename_edit.editingFinished.connect(self.on_filename_edited)
        self.choose_file_button.clicked.connect(self.on_choose_file)
        self.setLayout(layout)

    def text(self) -> str:
        """
        Gets the current directory value.
        """
        return self.filename_edit.text()

    def setText(self, filename):
        """Sets the current directory value"""
        if filename:
            filename = os.path.normpath(filename)
            if self.collapse_user_dir:
                # If it's in the home directory, change to the ~ syntax
                home_dir = os.path.expanduser("~")
                if filename.startswith(home_dir):
                    filename = os.path.join("~", filename[len(home_dir) + 1 :])

        with block_signals(self.filename_edit):
            self.filename_edit.setText(filename)

        self.path_changed.emit(filename)

    def on_filename_edited(self):
        self.path_changed.emit(self.text())

    def on_choose_file(self):
        filename = os.path.expanduser(self.filename_edit.text()) or "."

        filename = self.file_dialog(self, f"Choose {self.file_label}", filename)

        if filename:
            self.setText(filename)


class InputFilePickerWidget(_FileWidget):
    """
    A LineEdit + File Picker button, for choosing an input file.

    If it is in the user's home directory, the value is shortened to
    `~/<subdir...>`.

    The caller can listen to the path_changed signal to be
    notified of modifications.

    Args:
        initial_filename (str): The filename to show initially.
        file_label (str): The name of the file for GUI messages.
                For example, "Input Scene File".
        filter (str): Selects file types. E.g. "Images (*.png *.xpm *.jpg)" for
                      a single filter, or for multiple filters,
                      "Images (*.png *.xpm *.jpg);;Text files (*.txt);;XML files (*.xml)".
        selected_filter (str): Chooses which filter to show by default.
        collapse_user_dir (bool): Whether to collapse the user home directory to "~" or not.
    """

    def __init__(
        self,
        *,
        initial_filename: str,
        file_label: str,
        filter: str,
        selected_filter: str,
        collapse_user_dir: bool = False,
        parent=None,
    ):
        super().__init__(
            initial_filename, file_label, filter, selected_filter, collapse_user_dir, parent
        )

    def file_dialog(self, parent, caption, dir):
        filename, selected_filter = QFileDialog.getOpenFileName(
            parent,
            caption,
            dir,
            self.filter,
            self.selected_filter,
        )
        self.selected_filter = selected_filter
        return filename


class OutputFilePickerWidget(_FileWidget):
    """
    A LineEdit + File Picker button, for choosing an output file.

    If it is in the user's home directory, the value is shortened to
    `~/<subdir...>`.

    The caller can listen to the path_changed signal to be
    notified of modifications.

    Args:
        initial_filename (str): The filename to show initially.
        file_label (str): The name of the file for GUI messages.
                For example, "Render Output Image".
        filter (str): Selects file types. E.g. "Images (*.png *.xpm *.jpg)" for
                      a single filter, or for multiple filters,
                      "Images (*.png *.xpm *.jpg);;Text files (*.txt);;XML files (*.xml)".
        selected_filter (str): Chooses which filter to show by default.
        collapse_user_dir (bool): Whether to collapse the user home directory to "~" or not.
    """

    def __init__(
        self,
        *,
        initial_filename: str,
        file_label: str,
        filter: str,
        selected_filter: str,
        collapse_user_dir: bool = False,
        parent=None,
    ):
        super().__init__(
            initial_filename, file_label, filter, selected_filter, collapse_user_dir, parent
        )

    def file_dialog(self, parent, caption, dir):
        filename, selected_filter = QFileDialog.getSaveFileName(
            parent,
            caption,
            dir,
            self.filter,
            self.selected_filter,
        )
        self.selected_filter = selected_filter
        return filename


class DirectoryPickerWidget(QWidget):
    """
    A LineEdit + Directory Picker button, for choosing a directory.

    If it is in the user's home directory, the value is shortened to
    `~/<subdir...>`.

    The caller can listen to the path_changed signal to be
    notified of modifications.

    Args:
        initial_directory (str): The directory to show initially.
        directory_label (str): The name of the directory for GUI messages.
                For example, "Job History Dir".
        collapse_user_dir (bool): Whether to collapse the user home directory to "~" or not.
    """

    # Emitted when the directory changes
    path_changed = Signal(str)

    def __init__(
        self,
        *,
        initial_directory: str,
        directory_label: str,
        collapse_user_dir: bool = False,
        parent=None,
    ):
        super().__init__(parent)
        self._build_ui()
        with block_signals(self.directory_edit):
            self.directory_edit.setText(initial_directory)
        self.directory_label = directory_label
        self.collapse_user_dir = collapse_user_dir

    def _build_ui(self):
        self.directory_edit = QLineEdit(parent=self)
        layout = QHBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self.directory_edit)
        self.choose_directory_button = QPushButton("...")
        self.choose_directory_button.setFixedSize(30, 22)
        layout.addWidget(self.choose_directory_button)
        self.directory_edit.editingFinished.connect(self.on_directory_edited)
        self.choose_directory_button.clicked.connect(self.on_choose_directory)
        self.setLayout(layout)

    def text(self) -> str:
        """
        Gets the current directory value.
        """
        return self.directory_edit.text()

    def setText(self, directory):
        """Sets the current directory value"""
        if directory:
            directory = os.path.normpath(directory)
            if self.collapse_user_dir:
                # If it's in the home directory, collapse to the ~ syntax
                home_dir = os.path.expanduser("~")
                if directory.startswith(home_dir):
                    directory = os.path.join("~", directory[len(home_dir) + 1 :])

        with block_signals(self.directory_edit):
            self.directory_edit.setText(directory)

        self.path_changed.emit(directory)

    def on_directory_edited(self):
        self.path_changed.emit(self.text())

    def on_choose_directory(self):
        directory = os.path.expanduser(self.directory_edit.text()) or "."

        # If the directory is missing, create it so the dir chooser starts there
        if not os.path.isdir(directory):
            os.makedirs(directory)

        directory = QFileDialog.getExistingDirectory(
            self,
            f"Choose {self.directory_label}",
            directory,
            QFileDialog.ShowDirsOnly | QFileDialog.DontResolveSymlinks,
        )

        if directory:
            self.setText(directory)
