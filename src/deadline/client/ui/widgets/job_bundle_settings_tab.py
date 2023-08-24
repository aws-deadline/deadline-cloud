# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
UI widgets for the Scene Settings tab.
"""
import os

from PySide2.QtWidgets import QVBoxLayout, QWidget  # type: ignore

from ...job_bundle import read_job_bundle_parameters
from ..dataclasses import JobBundleSettings
from .job_template_parameters_widget import JobTemplateParametersWidget


class JobBundleSettingsWidget(QWidget):
    """
    Widget containing job setup specific to CLI jobs.

    Args:
        initial_settings (CliJobSettings): dataclass containing the job-specific settings.
        parent: The parent Qt Widget.
    """

    def __init__(self, initial_settings: JobBundleSettings, parent=None):
        super().__init__(parent=parent)

        self._build_ui(initial_settings)

    def _build_ui(self, initial_settings: JobBundleSettings):
        layout = QVBoxLayout(self)

        if not os.path.isdir(initial_settings.input_job_bundle_dir):
            raise RuntimeError(
                f"Input Job Bundle Dir is not valid: {initial_settings.input_job_bundle_dir}"
            )

        self.input_job_bundle_dir = initial_settings.input_job_bundle_dir
        self.job_bundle_parameters = read_job_bundle_parameters(
            initial_settings.input_job_bundle_dir
        )

        self.job_template_parameters_widget = JobTemplateParametersWidget(
            self.job_bundle_parameters, parent=self
        )
        layout.addWidget(self.job_template_parameters_widget)

    def update_settings(self, settings: JobBundleSettings):
        """
        Update a settings object with the latest values.
        """
        settings.input_job_bundle_dir = self.input_job_bundle_dir
        settings.parameter_values = self.job_template_parameters_widget.get_parameter_values()
