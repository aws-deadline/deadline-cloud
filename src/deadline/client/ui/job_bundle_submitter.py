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
    QMessageBox,
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
    validate_job_parameter_value,
)
from .dataclasses import JobBundleSettings
from .dialogs.submit_job_to_deadline_dialog import (
    SubmitJobToDeadlineDialog,
    JobBundlePurpose,
)
from .widgets.job_bundle_settings_tab import JobBundleSettingsWidget
from ..job_bundle.submission import AssetReferences
from ..api._session import session_context

logger = getLogger(__name__)


def _validate_job_parameters_against_definitions(
    job_parameters: list[dict[str, Any]],
    job_template_parameters: list[JobParameter],
    queue_parameters: list[dict[str, Any]],
) -> list[str]:
    """
    Validate CLI parameters against available parameter definitions.

    Args:
        job_parameters: List of CLI parameters with 'name' and 'value' keys
        job_template_parameters: List of job template parameter definitions
        queue_parameters: List of queue parameter definitions

    Returns:
        A list of unrecognized parameter names.
    """
    # Create sets of recognized parameter names
    job_template_names = {param["name"] for param in job_template_parameters}
    queue_parameter_names = {param["name"] for param in queue_parameters}
    all_recognized_names = job_template_names | queue_parameter_names

    unrecognized_names = {param["name"] for param in job_parameters} - all_recognized_names

    return sorted(unrecognized_names)


def _validate_and_warn_about_parameters(
    job_parameters: list[dict[str, Any]],
    job_template_parameters: list[JobParameter],
    queue_parameters: list[dict[str, Any]],
    parent_widget,
) -> bool:
    """
    Validate CLI parameters against job template and queue parameters.
    Display warning dialog for unrecognized parameters.

    Args:
        job_parameters: List of CLI parameters with 'name' and 'value' keys
        job_template_parameters: List of job template parameter definitions
        queue_parameters: List of queue parameter definitions
        parent_widget: Parent widget for the warning dialog

    Returns:
        True if user wants to continue, False if user wants to cancel
    """
    unrecognized_names = _validate_job_parameters_against_definitions(
        job_parameters, job_template_parameters, queue_parameters
    )

    if not unrecognized_names:
        return True

    # Display warning dialog for unrecognized parameters
    unrecognized_list = "\n".join(f"  • {name}" for name in unrecognized_names)
    message = (
        f"The following parameters are not recognized by the job template or queue:\n\n"
        f"{unrecognized_list}\n\n"
        f"These parameters will be ignored during job submission.\n\n"
        f"Do you want to continue?"
    )

    reply = QMessageBox.question(
        parent_widget,
        "Unrecognized Parameters",
        message,
        QMessageBox.Yes | QMessageBox.No,
        QMessageBox.No,
    )

    return reply == QMessageBox.Yes


def show_job_bundle_submitter(
    *,
    input_job_bundle_dir: str = "",
    browse: bool = False,
    parent=None,
    f=Qt.WindowFlags(),
    submitter_name: Optional[str] = None,
    known_asset_paths: Optional[list[str]] = None,
    job_parameters: Optional[list[dict[str, Any]]] = None,
) -> Optional[SubmitJobToDeadlineDialog]:
    """
    Opens an AWS Deadline Cloud job submission dialog for the provided job bundle.

    Pass f=Qt.Tool if running it within an application context and want it
    to stay on top.
    """

    if not submitter_name:
        submitter_name = "JobBundle"

    session_context["submitter-name"] = submitter_name

    if parent is None:
        # Get the main application window so we can parent ours to it
        app = QApplication.instance()
        main_windows = [
            widget
            for widget in app.topLevelWidgets()
            if isinstance(widget, QMainWindow)  # type: ignore[union-attr]
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
    ) -> dict[str, Any]:
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
        if settings.description:
            template["description"] = settings.description
        else:
            # remove description field since it can't be empty
            # ignore if description is missing from template
            template.pop("description", None)

        # If "HostRequirements" is provided, inject it into each of the "Step"
        if host_requirements:
            # for each step in the template, append the same host requirements.
            for step in template["steps"]:
                step["hostRequirements"] = copy.deepcopy(host_requirements)

        # First filter the queue parameters to exclude any from the job template,
        # then extend it with the job template parameters.
        job_parameter_names = {param["name"] for param in settings.parameters}
        parameter_values: list[dict[str, Any]] = [
            {"name": param["name"], "value": param["value"]}
            for param in queue_parameters
            if param["name"] not in job_parameter_names
        ]
        parameter_values.extend(
            {"name": param["name"], "value": param["value"]} for param in settings.parameters
        )

        parameters = merge_queue_job_parameters(
            queue_parameters=queue_parameters,
            job_parameters=settings.parameters,
        )

        apply_job_parameters(
            parameter_values,
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

        return {
            "known_asset_paths": [os.path.abspath(settings.input_job_bundle_dir)],
            "job_parameters": parameter_values,
        }

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

    initial_shared_parameter_values = {}

    job_parameters_dict = {param["name"]: param for param in (job_parameters or [])}
    for parameter in initial_settings.parameters:
        # Overwrite the parameter values from the job bundle with values provided by job_parameters,
        # e.g. from the CLI when this is called by the 'deadline bundle gui-submit' command.
        if parameter["name"] in job_parameters_dict:
            value = job_parameters_dict.pop(parameter["name"])["value"]
            # Convert any path parameters to absolute
            if parameter["type"] == "PATH":
                value = os.path.abspath(value)
            # Validate the value against the parameter definition and ensure it has the correct type
            try:
                value = validate_job_parameter_value(parameter, value)
            except (ValueError, TypeError) as e:
                # Convert the exception to DeadlineOperationError to avoid showing a full stack trace.
                raise DeadlineOperationError(str(e))
            parameter["value"] = value

        # Populate the initial queue parameter values based on the job template parameter values
        if "default" in parameter or "value" in parameter:
            initial_shared_parameter_values[parameter["name"]] = parameter.get(
                "value", parameter.get("default")
            )
    # Put the job_parameter values that weren't for the template in the shared parameter values
    for parameter in job_parameters_dict.values():
        initial_shared_parameter_values[parameter["name"]] = parameter["value"]

    submitter_dialog = SubmitJobToDeadlineDialog(
        job_setup_widget_type=JobBundleSettingsWidget,
        initial_job_settings=initial_settings,
        show_host_requirements_tab=True,
        initial_shared_parameter_values=initial_shared_parameter_values,
        auto_detected_attachments=asset_references,
        attachments=AssetReferences(),
        on_create_job_bundle_callback=on_create_job_bundle_callback,
        parent=parent,
        f=f,
        submitter_name=submitter_name,
        known_asset_paths=known_asset_paths,
    )

    if job_parameters:
        # We want to validate the job parameters after the queue parameters are loaded.
        # Connect a parameter validation function to the queue parameter loading completion
        def validate_parameters_after_queue_load(refresh_id: int, queue_parameters: list):
            """Validate CLI parameters against loaded queue parameters and set parameter values"""
            if not _validate_and_warn_about_parameters(
                job_parameters, initial_settings.parameters, queue_parameters, submitter_dialog
            ):
                # User chose to cancel, close the dialog
                submitter_dialog.close()

        # Connect to the queue parameters update signal
        submitter_dialog.shared_job_settings._queue_parameters_update.connect(
            validate_parameters_after_queue_load
        )

    submitter_dialog.show()
    return submitter_dialog
