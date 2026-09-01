# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

__all__ = ["DirectoryPickerWidget", "InputFilePickerWidget", "OutputFilePickerWidget"]

import os
from typing import Any, Optional

from qtpy.QtCore import Signal
from qtpy.QtWidgets import (  # pylint: disable=import-error; type: ignore
    QFileDialog,
    QHBoxLayout,
    QLineEdit,
    QPushButton,
    QWidget,
)

from ..._path_utils import is_path_contained
from .._utils import block_signals, tr


def _collapse_user_dir(path: str, *, path_module: Any = None) -> str:
    """Rewrite a path inside the user's home directory to the ``~`` spelling.

    Containment is by component, not by string prefix: with a home directory of
    ``C:\\Users\\bob``, ``C:\\Users\\bobby\\scene.ma`` is not inside it. Slicing by the home
    directory's length kept such a path and ate the first character of what followed, and
    where the remainder then started with a separator ``join("~", ...)`` discarded the
    ``~`` and returned an unrelated absolute path. The config dialog writes this text
    straight into ``job_history_dir`` and ``job_bundle_default_directory``, so a wrong
    answer here is persisted.
    """
    path_module = path_module or os.path
    home_dir = path_module.expanduser("~")
    if not is_path_contained(path, home_dir, path_module=path_module):
        return path
    relative = path_module.relpath(path, home_dir)
    if relative == path_module.curdir:
        return "~"
    return path_module.join("~", relative)


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
        self.choose_file_button = QPushButton(tr("..."))
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
                filename = _collapse_user_dir(filename, path_module=os.path)

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
        parent: Optional[QWidget] = None,
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
        parent: Optional[QWidget] = None,
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
        parent: Optional[QWidget] = None,
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
                directory = _collapse_user_dir(directory, path_module=os.path)

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
