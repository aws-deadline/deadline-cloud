# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Entry point for the Rust CLI to launch Qt GUI dialogs.

The Rust `deadline` binary spawns this module as a subprocess:
    python -m deadline.client.ui._gui_entry <command> --params-json '{...}'

This module does NOT use click or any CLI framework — the Rust binary
handles all argument parsing and validation before invoking this.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import sys
from pathlib import Path
from typing import Optional


def _check_pyside6(install_gui: bool) -> None:
    """Check for PySide6 and optionally install it."""
    if importlib.util.find_spec("PySide6"):
        return

    if not install_gui:
        print(
            "GUI dependencies (PySide6) are not installed.\n"
            "Install them with: pip install 'deadline[gui]'\n"
            "Or re-run with --install-gui to install automatically.",
            file=sys.stderr,
        )
        sys.exit(1)

    # Auto-install PySide6
    import subprocess

    pyside6_pypi = "PySide6-essentials>=6.6,<6.11"
    python = sys.executable
    print(f"Installing GUI dependencies ({pyside6_pypi})...", file=sys.stderr)
    subprocess.run([python, "-m", "pip", "install", pyside6_pypi], check=True)


def _create_app():
    """Create and configure a QApplication."""
    os.environ["QT_API"] = "pyside6"

    from qtpy.QtGui import QIcon
    from qtpy.QtWidgets import QApplication

    app = QApplication(sys.argv)
    app.setApplicationName("AWS Deadline Cloud")
    icon_path = Path(__file__).parent / "resources" / "deadline_logo.svg"
    if icon_path.exists():
        app.setWindowIcon(QIcon(str(icon_path)))
    return app


def run_gui_submit(
    job_bundle_dir: Optional[str] = None,
    browse: bool = False,
    output: str = "verbose",
    known_asset_paths: Optional[list] = None,
    submitter_info_dict: Optional[dict] = None,
    job_parameters: Optional[list] = None,
    name: Optional[str] = None,
    auto_close: bool = False,
) -> str:
    """Run the job bundle submission GUI and return the result string."""
    from ..dataclasses import SubmitterInfo
    from ..exceptions import DeadlineOperationError
    from ._utils import tr
    from .job_bundle_submitter import show_job_bundle_submitter

    submitter_info = None
    if submitter_info_dict:
        submitter_info = SubmitterInfo(**submitter_info_dict)

    if not job_bundle_dir and not browse:
        raise DeadlineOperationError(
            tr("Specify a job bundle directory or run the bundle command with the --browse flag")
        )

    submitter = show_job_bundle_submitter(
        input_job_bundle_dir=job_bundle_dir or "",
        browse=browse,
        submitter_info=submitter_info,
        known_asset_paths=known_asset_paths or [],
        job_parameters=job_parameters or [],
        name=name,
    )

    if not submitter or auto_close:
        return _format_response(output, None, job_bundle_dir, None)

    # In JSON/programmatic output mode, always close after the progress
    # dialog finishes (success or cancel) so exec() returns and we can
    # print the result. Without this, the dialog stays open indefinitely
    # for JobBundle submitters.
    if output == "json":
        submitter._close_event_receiver = submitter.close

    submitter.show()

    from qtpy.QtWidgets import QApplication

    QApplication.instance().exec()

    return _format_response(output, submitter.job_id, job_bundle_dir, submitter.job_history_bundle_dir)


def run_config_gui(auto_close: bool = False) -> Optional[str]:
    """Run the workstation configuration GUI."""
    from .dialogs.deadline_config_dialog import DeadlineConfigDialog

    if auto_close:
        # Test mode: just verify the dialog can be created
        return None

    DeadlineConfigDialog.configure_settings()
    return None


def _format_response(
    output: str,
    job_id: Optional[str],
    job_bundle_dir: Optional[str],
    job_history_bundle_dir: Optional[str],
) -> str:
    """Format the GUI result for stdout."""
    if output == "json":
        if job_id:
            return json.dumps(
                {
                    "status": "SUBMITTED",
                    "jobId": job_id,
                    "jobHistoryBundleDirectory": job_history_bundle_dir,
                }
            )
        else:
            return json.dumps({"status": "CANCELED"})
    else:
        if job_id:
            lines = ["Submitted job bundle:", f"   {job_bundle_dir}", f"Job ID: {job_id}"]
            return "\n".join(lines)
        else:
            return "Job submission canceled."


def main() -> None:
    parser = argparse.ArgumentParser(description="Deadline Cloud GUI entry point")
    parser.add_argument("command", choices=["gui-submit", "config-gui"])
    parser.add_argument("--params-json", default="{}")
    parser.add_argument("--install-gui", action="store_true")
    args = parser.parse_args()

    params = json.loads(args.params_json)

    _check_pyside6(args.install_gui)
    _create_app()

    if args.command == "gui-submit":
        result = run_gui_submit(
            job_bundle_dir=params.get("job_bundle_dir"),
            browse=params.get("browse", False),
            output=params.get("output", "verbose"),
            known_asset_paths=params.get("known_asset_paths"),
            submitter_info_dict=params.get("submitter_info"),
            job_parameters=params.get("job_parameters"),
            name=params.get("name"),
        )
        if result:
            print(result, flush=True)
    elif args.command == "config-gui":
        run_config_gui()


if __name__ == "__main__":
    main()
