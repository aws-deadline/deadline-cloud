# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations
import copy
import os
from logging import getLogger
from typing import Any, Optional, Dict

from qtpy.QtCore import Qt  # pylint: disable=import-error
from qtpy.QtWidgets import (  # pylint: disable=import-error; type: ignore
    QApplication,
    QFileDialog,
    QMainWindow,
)

from ..exceptions import DeadlineOperationError
from ..job_bundle.loader import (
    parse_yaml_or_json_content,
    read_yaml_or_json,
    read_yaml_or_json_object,
    validate_directory_symlink_containment,
)
from ..job_bundle.saver import save_yaml_or_json_to_file
from ..job_bundle.parameters import (
    JobParameter,
    apply_job_parameters,
    merge_queue_job_parameters,
    read_job_bundle_parameters,
)
from .dataclasses import JobBundleSettings
from .dialogs.submit_job_to_deadline_dialog import (
    SubmitJobToDeadlineDialog,
    JobBundlePurpose,
)
from .widgets.job_bundle_settings_tab import JobBundleSettingsWidget
from ..job_bundle.submission import AssetReferences

logger = getLogger(__name__)


def show_job_bundle_submitter(
    *, input_job_bundle_dir: str = "", browse: bool = False, parent=None, f=Qt.WindowFlags()
) -> Optional[SubmitJobToDeadlineDialog]:
    """
    Opens an AWS Deadline Cloud job submission dialog for the provided job bundle.

    Pass f=Qt.Tool if running it within an application context and want it
    to stay on top.
    """

    if parent is None:
        # Get the main application window so we can parent ours to it
        app = QApplication.instance()
        main_windows = [
            widget for widget in app.topLevelWidgets() if isinstance(widget, QMainWindow)  # type: ignore[union-attr]
        ]
        if main_windows:
            parent = main_windows[0]

    if not input_job_bundle_dir:
        input_job_bundle_dir = QFileDialog.getExistingDirectory(
            parent, "Choose job bundle directory", input_job_bundle_dir
        )
        if not input_job_bundle_dir:
            return None

    def on_create_job_bundle_callback(
        widget: SubmitJobToDeadlineDialog,
        job_bundle_dir: str,
        settings: JobBundleSettings,
        queue_parameters: list[JobParameter],
        asset_references: AssetReferences,
        host_requirements: Optional[Dict[str, Any]] = None,
        purpose: JobBundlePurpose = JobBundlePurpose.SUBMISSION,
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

        # If "HostRequirements" is provided, inject it into each of the "Step"
        if host_requirements:
            # for each step in the template, append the same host requirements.
            for step in template["steps"]:
                step["hostRequirements"] = copy.deepcopy(host_requirements)

        # First filter the queue parameters to exclude any from the job template,
        # then extend it with the job template parameters.
        job_parameter_names = {param["name"] for param in settings.parameters}
        parameters_values: list[dict[str, Any]] = [
            {"name": param["name"], "value": param["value"]}
            for param in queue_parameters
            if param["name"] not in job_parameter_names
        ]
        parameters_values.extend(
            {"name": param["name"], "value": param["value"]} for param in settings.parameters
        )

        parameters = merge_queue_job_parameters(
            queue_parameters=queue_parameters,
            job_parameters=settings.parameters,
        )

        apply_job_parameters(
            parameters_values,
            job_bundle_dir,
            parameters,
            AssetReferences(),
        )

        save_yaml_or_json_to_file(
            bundle_dir=job_bundle_dir, filename="template", file_type=file_type, data=template
        )
        save_yaml_or_json_to_file(
            bundle_dir=job_bundle_dir,
            filename="asset_references",
            file_type=file_type,
            data=asset_references.to_dict(),
        )
        save_yaml_or_json_to_file(
            bundle_dir=job_bundle_dir,
            filename="parameter_values",
            file_type=file_type,
            data={"parameterValues": parameters_values},
        )

    # Ensure the job bundle doesn't contain files that resolve outside of the bundle directory
    validate_directory_symlink_containment(input_job_bundle_dir)

    # Load the template to get the starting name
    template = read_yaml_or_json_object(input_job_bundle_dir, "template", True)

    asset_references_obj = (
        read_yaml_or_json_object(input_job_bundle_dir, "asset_references", False) or {}
    )
    asset_references = AssetReferences.from_dict(asset_references_obj)

    name = "Job bundle submission"
    if template:
        name = template.get("name", name)

    if not os.path.isdir(input_job_bundle_dir):
        raise DeadlineOperationError(f"Input Job Bundle Dir is not valid: {input_job_bundle_dir}")
    initial_settings = JobBundleSettings(input_job_bundle_dir=input_job_bundle_dir, name=name)
    initial_settings.parameters = read_job_bundle_parameters(input_job_bundle_dir)
    initial_settings.browse_enabled = browse

    # Populate the initial queue parameter values based on the job template parameter values
    initial_shared_parameter_values = {}
    for parameter in initial_settings.parameters:
        if "default" in parameter or "value" in parameter:
            initial_shared_parameter_values[parameter["name"]] = parameter.get(
                "value", parameter.get("default")
            )

    submitter_dialog = SubmitJobToDeadlineDialog(
        job_setup_widget_type=JobBundleSettingsWidget,
        initial_job_settings=initial_settings,
        # show_host_requirements_tab=True,  // Enable when we want to show the host requirement tab
        initial_shared_parameter_values=initial_shared_parameter_values,
        auto_detected_attachments=asset_references,
        attachments=AssetReferences(),
        on_create_job_bundle_callback=on_create_job_bundle_callback,
        parent=parent,
        f=f,
    )
    submitter_dialog.show()
    return submitter_dialog
