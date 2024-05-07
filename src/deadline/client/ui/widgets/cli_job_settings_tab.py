# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
UI widgets for the Scene Settings tab.
"""
import os

from qtpy.QtCore import Qt  # type: ignore
from qtpy.QtWidgets import (  # type: ignore
    QCheckBox,
    QComboBox,
    QGridLayout,
    QLabel,
    QLineEdit,
    QSizePolicy,
    QTextEdit,
    QWidget,
)

from ..dataclasses import CliJobSettings
from .path_widgets import DirectoryPickerWidget


class CliJobSettingsWidget(QWidget):
    """
    Widget containing job setup specific to CLI jobs.

    Args:
        initial_settings (CliJobSettings): dataclass containing the job-specific settings.
        parent: The parent Qt Widget.
    """

    def __init__(self, initial_settings: CliJobSettings, parent=None):
        super().__init__(parent=parent)

        self._build_ui()
        self._load_initial_settings(initial_settings)

    def _set_enabled_with_label(self, prop_name: str, enabled: bool):
        """Set the enabled status of a control and its label"""
        getattr(self, prop_name).setEnabled(enabled)
        getattr(self, prop_name + "_label").setEnabled(enabled)

    def _build_ui(self):
        layout = QGridLayout(self)

        self.bash_script = QTextEdit()
        if os.name == "nt":
            font_family = "Consolas"
        elif os.name == "darwin":
            font_family = "Monaco"
        else:
            font_family = "Monospace"
        font = self.bash_script.currentFont()
        font.setFamily(font_family)
        font.setFixedPitch(True)
        font.setKerning(False)
        font.setPointSize(font.pointSize() + 1)
        self.bash_script.setCurrentFont(font)
        self.bash_script.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

        layout.addWidget(self.bash_script, 0, 0, 1, 2)

        self.use_array_parameter_chck = QCheckBox("Use array parameter", self)
        self.array_parameter_name = QLineEdit(self)
        layout.addWidget(self.use_array_parameter_chck, 1, 0)
        layout.addWidget(self.array_parameter_name, 1, 1)
        self.use_array_parameter_chck.stateChanged.connect(self.use_array_parameter_changed)

        self.array_parameter_values_label = QLabel("Array parameter values")
        layout.addWidget(self.array_parameter_values_label, 2, 0)
        self.array_parameter_values = QLineEdit(self)
        layout.addWidget(self.array_parameter_values, 2, 1)

        self.data_dir_label = QLabel("Data directory")
        self.data_dir_edit = DirectoryPickerWidget(
            initial_directory=os.path.expanduser(os.path.join("~", "CLIJobData")),
            directory_label="Data directory",
            parent=self,
        )
        layout.addWidget(self.data_dir_label, 3, 0)
        layout.addWidget(self.data_dir_edit, 3, 1)

        self.file_format_label = QLabel("Template file format")
        self.file_format_box = QComboBox(parent=self)
        self.file_format_box.addItems(["YAML", "JSON"])
        layout.addWidget(self.file_format_label, 4, 0)
        layout.addWidget(self.file_format_box, 4, 1)

    def _load_initial_settings(self, initial_settings: CliJobSettings):
        self.bash_script.setPlainText(initial_settings.bash_script_contents)
        self.use_array_parameter_chck.setChecked(initial_settings.use_array_parameter)
        self.array_parameter_name.setText(initial_settings.array_parameter_name)
        self.array_parameter_values.setText(initial_settings.array_parameter_values)
        self.file_format_box.setCurrentText(initial_settings.file_format)

    def update_settings(self, settings: CliJobSettings):
        """
        Update a settings object with the latest values.
        """
        settings.bash_script_contents = self.bash_script.toPlainText()
        settings.use_array_parameter = self.use_array_parameter_chck.isChecked()
        settings.array_parameter_name = self.array_parameter_name.text()
        settings.array_parameter_values = self.array_parameter_values.text()
        settings.data_dir = os.path.expanduser(self.data_dir_edit.text())
        settings.file_format = self.file_format_box.currentText()

    def use_array_parameter_changed(self, state: int):
        self.array_parameter_name.setEnabled(state == Qt.Checked)
        self._set_enabled_with_label("array_parameter_values", state == Qt.Checked)
