# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
UI widgets for the scene settings tab.
"""

from __future__ import annotations

import os
from logging import getLogger
from typing import Any, Optional

from qtpy.QtCore import QThread, Signal  # type: ignore
from qtpy.QtWidgets import (  # type: ignore
    QVBoxLayout,
    QWidget,
    QMessageBox,
)

from ..dataclasses import JobBundleSettings
from ...config import get_setting
from ...job_bundle._repository import S3BundleRepository as _S3BundleRepository
from .openjd_parameters_widget import OpenJDParametersWidget
from ...job_bundle.submission import AssetReferences
from ...job_bundle.loader import read_yaml_or_json_object, validate_directory_symlink_containment
from ...job_bundle.parameters import read_job_bundle_parameters

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

    def __init__(self, initial_settings: JobBundleSettings, parent: Optional[QWidget] = None):
        super().__init__(parent=parent)

        self.param_layout = QVBoxLayout()

        self._build_ui(initial_settings)

    def _build_ui(self, initial_settings: JobBundleSettings):
        self.input_job_bundle_dir = initial_settings.input_job_bundle_dir

        layout = QVBoxLayout(self)

        layout.addLayout(self.param_layout)
        self.refresh_ui(initial_settings)

    def refresh_ui(self, settings: JobBundleSettings):
        # Clear the layout
        for i in reversed(range(self.param_layout.count())):
            item = self.param_layout.takeAt(i)
            if item is None:
                continue
            widget = item.widget()
            if widget:
                widget.deleteLater()

        self.parameters_widget = OpenJDParametersWidget(
            parameter_definitions=settings.parameters, parent=self
        )
        self.param_layout.addWidget(self.parameters_widget)
        self.parameters_widget.parameter_changed.connect(
            lambda message: self.parameter_changed.emit(message)
        )

    def on_load_bundle(self, s3_repo=None):
        """
        Browse and load the selected submission bundle
        """
        from ..dialogs.job_bundle_browser_dialog import JobBundleBrowserDialog

        # Determine the default local browse directory
        default_dir = get_setting("settings.job_bundle_default_directory")
        if default_dir:
            default_dir = os.path.expanduser(default_dir)

        # Get the job history directory for the current profile
        job_history_dir = os.path.expanduser(get_setting("settings.job_history_dir"))

        s3_worker = None
        if s3_repo:
            # Reuse existing repo — no background init needed
            browser = JobBundleBrowserDialog(
                queue_source=s3_repo,
                queue_error="",
                local_source=default_dir,
                history_source=job_history_dir,
                parent=self,
            )
        else:
            # Start S3 initialization in background
            class _S3InitWorker(QThread):
                # NOTE: named ``done`` rather than ``finished`` to avoid shadowing
                # QThread's built-in ``finished`` signal.
                done = Signal(object, str, list, set)

                def run(self):
                    try:
                        from concurrent.futures import ThreadPoolExecutor

                        repo = _S3BundleRepository.from_config()
                        with ThreadPoolExecutor(max_workers=2) as ex:
                            entries_f = ex.submit(repo.list_entries, repo.root_path())
                            hidden_f = ex.submit(repo.get_hidden_set)
                            entries = entries_f.result()
                            hidden = hidden_f.result()
                        self.done.emit(repo, "", entries, hidden)
                    except Exception as e:
                        self.done.emit(None, str(e), [], set())

            browser = JobBundleBrowserDialog(
                queue_source=None,
                queue_error="",
                queue_loading=True,
                local_source=default_dir,
                history_source=job_history_dir,
                parent=self,
            )
            # Connect the worker's result BEFORE starting it. A fast failure (e.g.
            # not logged in) can emit ``done`` almost immediately; a cross-thread
            # queued signal emitted before the connection exists is dropped, which
            # would leave the browser stuck on "Loading..." forever.
            s3_worker = _S3InitWorker()
            s3_worker.done.connect(browser.set_queue_source)
            s3_worker.start()

        # Everything from here on may return early; wrap it so the background
        # S3 worker is always joined. Dropping the last reference to a still-
        # running QThread makes Qt call std::terminate (SIGABRT), crashing the
        # host application (e.g. Maya/Nuke). This is reachable by picking a
        # Local/History bundle before the Queue tab finishes loading.
        try:
            if browser.exec_() != JobBundleBrowserDialog.Accepted or not browser.selected_path:
                return

            browser.hide()
            input_job_bundle_dir = browser.resolve_selection()
            while not input_job_bundle_dir:
                browser.show()
                if browser.exec_() != JobBundleBrowserDialog.Accepted or not browser.selected_path:
                    return
                browser.hide()
                input_job_bundle_dir = browser.resolve_selection()

            # Update job bundle directory path
            self.input_job_bundle_dir = input_job_bundle_dir

            # Warn the user if the Job Bundle could not be loaded
            try:
                validate_directory_symlink_containment(input_job_bundle_dir)

                asset_references_obj = (
                    read_yaml_or_json_object(input_job_bundle_dir, "asset_references", False) or {}
                )
                asset_references = AssetReferences.from_dict(asset_references_obj)

                # Load the template to get the bundle name
                template = read_yaml_or_json_object(input_job_bundle_dir, "template", True)
                name = template.get("name", "Job bundle submission")  # type: ignore[union-attr]
                job_settings = JobBundleSettings(
                    input_job_bundle_dir=input_job_bundle_dir, name=name
                )
                job_settings.parameters = read_job_bundle_parameters(input_job_bundle_dir)

            except Exception as e:
                msg = str(e)
                QMessageBox.warning(self, "Could not load job bundle", msg)  # type: ignore[call-arg]
                logger.warning(msg)
                return

            dialog = self.window()
            if dialog is not None and hasattr(dialog, "refresh"):
                dialog.refresh(  # type: ignore[union-attr]
                    job_settings=job_settings,
                    auto_detected_attachments=asset_references,
                    attachments=None,
                    load_new_bundle=True,
                )
        finally:
            if s3_worker:
                s3_worker.wait()

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
