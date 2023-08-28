# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import json
import os
from logging import getLogger
from typing import Any, Dict, List, Optional

from PySide2.QtCore import Qt  # pylint: disable=import-error
from PySide2.QtWidgets import (  # pylint: disable=import-error; type: ignore
    QApplication,
    QFileDialog,
    QMainWindow,
)

from ..job_bundle import deadline_yaml_dump
from ..job_bundle.loader import (
    parse_yaml_or_json_content,
    read_yaml_or_json,
    read_yaml_or_json_object,
)
from ..job_bundle.parameters import apply_job_parameters
from .dataclasses import JobBundleSettings
from .dialogs.submit_job_to_deadline_dialog import SubmitJobToDeadlineDialog
from .widgets.job_bundle_settings_tab import JobBundleSettingsWidget
from ..job_bundle.submission import FlatAssetReferences

logger = getLogger(__name__)


def show_job_bundle_submitter(
    input_job_bundle_dir: str = "", parent=None, f=Qt.WindowFlags()
) -> Optional[SubmitJobToDeadlineDialog]:
    """
    Opens an Amazon Deadline Cloud job submission dialog for the provided job bundle.

    Pass f=Qt.Tool if running it within an application context and want it
    to stay on top.
    """

    if parent is None:
        # Get the main application window so we can parent ours to it
        app = QApplication.instance()
        main_windows = [
            widget for widget in app.topLevelWidgets() if isinstance(widget, QMainWindow)
        ]
        if main_windows:
            parent = main_windows[0]

    if not input_job_bundle_dir:
        input_job_bundle_dir = QFileDialog.getExistingDirectory(
            parent, "Choose Job Bundle Directory", input_job_bundle_dir
        )
        if not input_job_bundle_dir:
            return None

    def on_create_job_bundle_callback(
        widget: SubmitJobToDeadlineDialog,
        settings: JobBundleSettings,
        job_bundle_dir: str,
        asset_references: FlatAssetReferences,
    ) -> None:
        """
        Perform a submission when the submit button is pressed

        Args:
            widget (SubmitJobToDeadlineDialog): The Deadline job submission dialog.
            settings (JobBundleSettings): A settings object that was populated from the job submission dialog.
            job_bundle_dir (str): The directory within which to create the job bundle.
            asset_references (FlatAssetReferences): The input from the attachments provided during
                construction and the user's input in the Job Attachments tab.
        """
        # Copy the template
        file_contents, file_type = read_yaml_or_json(
            settings.input_job_bundle_dir, "template", True
        )

        template = parse_yaml_or_json_content(
            file_contents, file_type, settings.input_job_bundle_dir, "template"
        )
        template["name"] = settings.name

        with open(
            os.path.join(job_bundle_dir, f"template.{file_type.lower()}"), "w", encoding="utf8"
        ) as f:
            if file_type == "YAML":
                deadline_yaml_dump(template, f)
            elif file_type == "JSON":
                json.dump(template, f, indent=1)

        parameters_values: List[Dict[str, Any]] = [
            {"name": "deadline:priority", "value": settings.priority},
            {"name": "deadline:targetTaskRunStatus", "value": settings.initial_status},
            {"name": "deadline:maxFailedTasksCount", "value": settings.max_failed_tasks_count},
            {"name": "deadline:maxRetriesPerTask", "value": settings.max_retries_per_task},
        ]

        if asset_references:
            with open(
                os.path.join(job_bundle_dir, "asset_references.yaml"), "w", encoding="utf8"
            ) as f:
                deadline_yaml_dump(asset_references.to_dict(), f)

        job_bundle_parameters = widget.job_settings.job_bundle_parameters

        parameters_values.extend(settings.parameter_values)

        apply_job_parameters(
            parameters_values, job_bundle_dir, job_bundle_parameters, FlatAssetReferences()
        )

        with open(os.path.join(job_bundle_dir, "parameter_values.yaml"), "w", encoding="utf8") as f:
            deadline_yaml_dump({"parameterValues": parameters_values}, f)

    # Load the template to get the starting name
    template = read_yaml_or_json_object(input_job_bundle_dir, "template", True)

    asset_references_obj = (
        read_yaml_or_json_object(input_job_bundle_dir, "asset_references", False) or {}
    )
    asset_references = FlatAssetReferences.from_dict(asset_references_obj)

    name = "Job Bundle Submission"
    if template:
        name = template.get("name", name)

    submitter_dialog = SubmitJobToDeadlineDialog(
        JobBundleSettingsWidget,
        JobBundleSettings(input_job_bundle_dir=input_job_bundle_dir, name=name),
        asset_references,
        FlatAssetReferences(),
        on_create_job_bundle_callback,
        parent=parent,
        f=f,
    )
    submitter_dialog.show()
    return submitter_dialog
