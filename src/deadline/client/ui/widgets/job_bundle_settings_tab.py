# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
UI widgets for the scene settings tab.
"""
from __future__ import annotations

import os
from logging import getLogger
from typing import Any

from qtpy.QtCore import Signal  # type: ignore
from qtpy.QtWidgets import (  # type: ignore
    QVBoxLayout,
    QHBoxLayout,
    QWidget,
    QFileDialog,
    QPushButton,
    QSpacerItem,
    QSizePolicy,
    QMessageBox,
)

from ..dataclasses import JobBundleSettings
from .openjd_parameters_widget import OpenJDParametersWidget
from ...job_bundle.submission import AssetReferences
from ...job_bundle.loader import read_yaml_or_json_object, validate_directory_symlink_containment
from ...job_bundle.parameters import read_job_bundle_parameters
from ...config import config_file

logger = getLogger(__name__)


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

        self.parent = parent

        self.param_layout = QVBoxLayout()

        self._build_ui(initial_settings)

    def _build_ui(self, initial_settings: JobBundleSettings):
        self.input_job_bundle_dir = initial_settings.input_job_bundle_dir

        layout = QVBoxLayout(self)

        if initial_settings.browse_enabled:
            btnBox = QHBoxLayout()
            self.load_bundle_button = QPushButton("Load a different job bundle")
            self.load_bundle_button.clicked.connect(self.on_load_bundle)
            btnBox.addWidget(self.load_bundle_button)
            btnBox.addItem(QSpacerItem(0, 0, QSizePolicy.Expanding, QSizePolicy.Minimum))
            layout.addLayout(btnBox)

        layout.addLayout(self.param_layout)
        self.refresh_ui(initial_settings)

    def refresh_ui(self, settings: JobBundleSettings):
        # Clear the layout
        for i in reversed(range(self.param_layout.count())):
            item = self.param_layout.takeAt(i)
            item.widget().deleteLater()

        self.parameters_widget = OpenJDParametersWidget(
            parameter_definitions=settings.parameters, parent=self
        )
        self.param_layout.addWidget(self.parameters_widget)
        self.parameters_widget.parameter_changed.connect(
            lambda message: self.parameter_changed.emit(message)
        )

    def on_load_bundle(self):
        """
        Browse and load the selected submission bundle
        """
        # Open the file picker dialog
        bundle_path = os.path.expanduser(config_file.get_setting("settings.job_history_dir"))
        input_job_bundle_dir = QFileDialog.getExistingDirectory(
            self, "Choose job bundle directory", bundle_path
        )
        if not input_job_bundle_dir:
            return

        # Warn the user if the Job Bundle could not be loaded
        try:
            validate_directory_symlink_containment(input_job_bundle_dir)

            asset_references_obj = (
                read_yaml_or_json_object(input_job_bundle_dir, "asset_references", False) or {}
            )
            asset_references = AssetReferences.from_dict(asset_references_obj)

            # Load the template to get the bundle name
            template = read_yaml_or_json_object(input_job_bundle_dir, "template", True)
            name = (
                template.get("name", "Job bundle submission")
                if template
                else "Job bundle submission"
            )
            job_settings = JobBundleSettings(input_job_bundle_dir=input_job_bundle_dir, name=name)
            job_settings.parameters = read_job_bundle_parameters(input_job_bundle_dir)

        except Exception as e:
            msg = str(e)
            QMessageBox.warning(self, "Could not load job bundle", msg)
            logger.warning(msg)
            return

        if hasattr(self.parent, "refresh"):
            self.parent.refresh(
                job_settings=job_settings,
                auto_detected_attachments=asset_references,
                attachments=None,
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
