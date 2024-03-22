# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

import json
import os
from importlib import reload
from logging import getLogger
from typing import Any, Dict, Optional
import copy

from qtpy.QtCore import Qt  # pylint: disable=import-error
from qtpy.QtWidgets import (  # pylint: disable=import-error; type: ignore
    QApplication,
    QMainWindow,
)

from ..job_bundle import deadline_yaml_dump
from .dataclasses import CliJobSettings
from .dialogs.submit_job_to_deadline_dialog import (
    SubmitJobToDeadlineDialog,
    JobBundlePurpose,
)
from .widgets.cli_job_settings_tab import CliJobSettingsWidget
from ..job_bundle.submission import AssetReferences

logger = getLogger(__name__)

# This contains the dialog we create, so that we only open one submitter dialog
__submitter_dialog = None


def show_cli_job_submitter(parent=None, f=Qt.WindowFlags()) -> None:
    """
    Shows an example CLI Job Submitter.

    Pass f=Qt.Tool if running it within an application context and want it
    to stay on top.
    """
    global __submitter_dialog

    close_submitter()

    if parent is None:
        # Get the main application window so we can parent ours to it
        app = QApplication.instance()
        parent = [widget for widget in app.topLevelWidgets() if isinstance(widget, QMainWindow)][0]  # type: ignore[union-attr]

    def on_create_job_bundle_callback(
        widget: SubmitJobToDeadlineDialog,
        job_bundle_dir: str,
        settings: CliJobSettings,
        queue_parameters: list[dict[str, Any]],
        asset_references: AssetReferences,
        host_requirements: Optional[Dict[str, Any]] = None,
        purpose: JobBundlePurpose = JobBundlePurpose.SUBMISSION,
    ) -> None:
        """
        Perform a submission when the submit button is pressed

        Args:
            widget (SubmitJobToDeadlineDialog): The Deadline job submission dialog.
            settings (CliJobSettings): A settings object that was populated from the job submission dialog.
            job_bundle_dir (str): The directory within which to create the job bundle.
            asset_references (FlatAssetReferences): The input from the attachments provided during
                construction and the user's input in the Job Attachments tab.
        """
        if settings.file_format not in ("YAML", "JSON"):
            raise RuntimeError(
                f"The CLI Job Submitter only supports YAML and JSON output, not {settings.file_format!r}."
            )

        job_template: Dict[str, Any] = {
            "specificationVersion": "jobtemplate-2023-09",
            "name": settings.name,
            "description": settings.description,
            "parameterDefinitions": [
                {
                    "name": "DataDir",
                    "type": "PATH",
                    "objectType": "DIRECTORY",
                    "dataFlow": "INOUT",
                    "default": settings.data_dir,
                }
            ],
        }
        if not settings.description:
            del job_template["description"]
        step = {
            "name": "CliScript",
            "script": {
                "actions": {
                    "onRun": {
                        "command": "{{Task.File.runScript}}",
                    }
                },
                "embeddedFiles": [
                    {
                        "name": "runScript",
                        "type": "TEXT",
                        "runnable": True,
                        "data": settings.bash_script_contents,
                    }
                ],
            },
        }
        job_template["steps"] = [step]
        if settings.use_array_parameter:
            job_array_parameter_name = f"{settings.array_parameter_name}Values"
            job_template["parameterDefinitions"].append(
                {
                    "name": job_array_parameter_name,
                    "type": "STRING",
                    "default": settings.array_parameter_values,
                }
            )
            step["parameterSpace"] = {
                "taskParameterDefinitions": [
                    {
                        "name": settings.array_parameter_name,
                        "type": "INT",
                        "range": "{{Param." + job_array_parameter_name + "}}",
                    }
                ]
            }

        # If "HostRequirements" is provided, inject it into each of the "Step"
        if host_requirements:
            # for each step in the template, append the same host requirements.
            for step in job_template["steps"]:
                step["hostRequirements"] = copy.deepcopy(host_requirements)

        with open(
            os.path.join(job_bundle_dir, f"template.{settings.file_format.lower()}"),
            "w",
            encoding="utf8",
        ) as f:
            if settings.file_format == "YAML":
                deadline_yaml_dump(job_template, f)
            elif settings.file_format == "JSON":
                json.dump(job_template, f, sort_keys=False, indent=1)

        # Filter the provided queue parameters to just their values
        parameters_values = [
            {"name": param["name"], "value": param["value"]} for param in queue_parameters
        ]

        with open(
            os.path.join(job_bundle_dir, f"parameter_values.{settings.file_format.lower()}"),
            "w",
            encoding="utf8",
        ) as f:
            if settings.file_format == "YAML":
                deadline_yaml_dump({"parameterValues": parameters_values}, f)
            elif settings.file_format == "JSON":
                json.dump({"parameterValues": parameters_values}, f, indent=1)

        if asset_references:
            with open(
                os.path.join(job_bundle_dir, f"asset_references.{settings.file_format.lower()}"),
                "w",
                encoding="utf8",
            ) as f:
                if settings.file_format == "YAML":
                    deadline_yaml_dump(asset_references.to_dict(), f)
                elif settings.file_format == "JSON":
                    json.dump(asset_references.to_dict(), f, indent=1)

    __submitter_dialog = SubmitJobToDeadlineDialog(
        job_setup_widget_type=CliJobSettingsWidget,
        initial_job_settings=CliJobSettings(),
        initial_shared_parameter_values={},
        auto_detected_attachments=AssetReferences(),
        attachments=AssetReferences(),
        show_host_requirements_tab=True,
        on_create_job_bundle_callback=on_create_job_bundle_callback,
        parent=parent,
        f=f,
    )
    __submitter_dialog.show()


def close_submitter() -> None:
    global __submitter_dialog

    if __submitter_dialog:
        __submitter_dialog.close()
        __submitter_dialog = None


def _reload_modules(mod):
    # TODO: Put this code where it makes sense
    """
    Recursively reloads all modules in the specified package, in postfix order
    """
    import types

    child_mods = [
        m
        for m in mod.__dict__.values()
        if isinstance(m, types.ModuleType)
        and m.__package__
        and m.__package__.startswith(mod.__package__)
    ]

    for child in child_mods:
        _reload_modules(mod=child)

    reload(mod)


def reload_plugin() -> None:
    # TODO: Put this code where it makes sense
    """
    Designed for interative development without closing the DCC
    the submitter is embedded inside. Closes the submitter,
    reloads the Python modules, and starts the submitter again.
    """
    close_submitter()

    # Reload the AWS Deadline Cloud submitter code
    import deadline

    _reload_modules(deadline)

    # Re-open the submitter
    show_cli_job_submitter()
