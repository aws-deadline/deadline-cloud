# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
UI widgets for the Scene Settings tab.
"""
from __future__ import annotations

from typing import Any

from PySide2.QtCore import Signal  # type: ignore
from PySide2.QtWidgets import QVBoxLayout, QWidget  # type: ignore

from ..dataclasses import JobBundleSettings
from .openjd_parameters_widget import OpenJDParametersWidget


class JobBundleSettingsWidget(QWidget):
    """
    Widget containing job setup specific to CLI jobs.

    Signals:
        parameter_changed: This is sent whenever a parameter value in the widget changes. The message
            is a copy of the parameter definition with the "value" key containing the new value.

    Args:
        initial_settings (CliJobSettings): dataclass containing the job-specific settings.
        parent: The parent Qt Widget.
    """

    parameter_changed = Signal(dict)

    def __init__(self, initial_settings: JobBundleSettings, parent=None):
        super().__init__(parent=parent)

        self.layout = QVBoxLayout(self)
        self._build_ui(initial_settings)

    def refresh_ui(self, settings: JobBundleSettings):
        # Clear the layout
        for i in reversed(range(self.layout.count())):
            item = self.layout.takeAt(i)
            item.widget().deleteLater()

        self._build_ui(settings)

    def _build_ui(self, initial_settings: JobBundleSettings):
        self.input_job_bundle_dir = initial_settings.input_job_bundle_dir

        self.parameters_widget = OpenJDParametersWidget(
            parameter_definitions=initial_settings.parameters, parent=self
        )
        self.layout.addWidget(self.parameters_widget)
        self.parameters_widget.parameter_changed.connect(
            lambda message: self.parameter_changed.emit(message)
        )

    def update_settings(self, settings: JobBundleSettings):
        """
        Update a settings object with the latest values.
        """
        settings.input_job_bundle_dir = self.input_job_bundle_dir
        settings.parameters = self.parameters_widget.get_parameters()

    def get_parameters(self):
        """
        Returns a list of OpenJD parameter definition dicts with
        a "value" key filled from the widget.
        """
        return self.parameters_widget.get_parameters()

    def set_parameter_value(self, parameter: dict[str, Any]):
        """
        Given an OpenJD parameter definition with a "value" key,
        set the parameter value in the widget.

        If the parameter value cannot be set, raises a KeyError.
        """
        self.parameters_widget.set_parameter_value(parameter)
