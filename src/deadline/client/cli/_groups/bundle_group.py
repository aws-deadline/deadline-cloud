# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
All the `deadline bundle` commands.
"""

from __future__ import annotations

import json
import logging
import sys
import re
from typing import Any, Optional

import click
from botocore.exceptions import ClientError

from ... import api
from ...config import config_file
from ....job_attachments.exceptions import (
    AssetSyncError,
    AssetSyncCancelledError,
    MisconfiguredInputsError,
)
from ....job_attachments.models import JobAttachmentsFileSystem

from ...exceptions import DeadlineOperationError, CreateJobWaiterCanceled
from .._common import (
    _apply_cli_options_to_config,
    _handle_error,
    _ProgressBarCallbackManager,
)
from ._sigint_handler import SigIntHandler

logger = logging.getLogger(__name__)

# Set up the signal handler for handling Ctrl + C interruptions.
sigint_handler = SigIntHandler()


@click.group(name="bundle")
@_handle_error
def cli_bundle():
    """
    Commands to work with Open Job Description job bundles.
    """


# Latin alphanumeric, starting with a letter
_openjd_identifier_regex = r"(?-m:^[A-Za-z_][A-Za-z0-9_]*\Z)"


def validate_parameters(ctx, param, value):
    """
    Validate provided --parameter values, ensuring that they are in the format "ParamName=Value", and convert them to a dict with the
    following format:
        [{"name": "<name>", "value": "<value>"}, ...]
    """
    parameters_split = []
    for parameter in value:
        regex_match = re.match("([^=]+)=(.*)", parameter)
        if not regex_match:
            raise click.BadParameter(
                f'Parameters must be provided in the format "ParamName=Value". Invalid parameter: {parameter}'
            )

        if not re.match(_openjd_identifier_regex, regex_match[1]):
            raise click.BadParameter(
                f"Parameter names must be alphanumeric Open Job Description identifiers. Invalid parameter name: {regex_match[1]}"
            )

        parameters_split.append({"name": regex_match[1], "value": regex_match[2]})

    return parameters_split


def _interactive_confirmation_prompt(message: str, default_response: bool) -> bool:
    """
    Callback to decide if submission should continue or be canceled. Returns True to continue, False to cancel.

    Args:
        warning_message (str): The warning message to display.
        default_response (bool): The default to present as the response (True to continue, False to cancel).
    """
    return click.confirm(
        message,
        default=default_response,
    )


@cli_bundle.command(name="submit")
@click.option(
    "-p",
    "--parameter",
    multiple=True,
    callback=validate_parameters,
    help='The values for the job template\'s parameters. Can be provided as key-value pairs, inline JSON strings, or as paths to a JSON or YAML document. If provided more than once, the values are combined in the order that they appear. Examples: --parameter MyParam=5 -p file://parameter_file.json -p \'{"MyParam": "5"}\'',
)
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The farm to use.")
@click.option("--queue-id", help="The queue to use.")
@click.option("--storage-profile-id", help="The storage profile to use.")
@click.option("--name", help="The job name to use in place of the one in the job bundle.")
@click.option(
    "--priority",
    type=int,
    default=50,
    help="The priority of the job. Jobs with a higher priority run first.",
)
@click.option(
    "--max-failed-tasks-count",
    type=int,
    help="The maximum number of failed tasks before the job is marked as failed.",
)
@click.option(
    "--max-retries-per-task",
    type=int,
    help="The maximum number of times to retry a task before it is marked as failed.",
)
@click.option(
    "--max-worker-count",
    type=int,
    help="The max worker count of the job.",
)
@click.option(
    "--job-attachments-file-system",
    help="The method workers use to access job attachments. "
    "COPIED means to copy files to the worker and VIRTUAL means to load "
    "files as needed from a virtual file system. If VIRTUAL is selected "
    "but not supported by a worker, it will fallback to COPIED.",
    type=click.Choice([e.value for e in JobAttachmentsFileSystem]),
)
@click.option(
    "--yes",
    is_flag=True,
    help="Skip any confirmation prompts",
)
@click.option(
    "--require-paths-exist",
    is_flag=True,
    help="Return an error if any input files are missing.",
)
@click.option(
    "--submitter-name",
    type=click.STRING,
    help="Name of the application submitting the bundle.",
)
@click.option(
    "--known-asset-path",
    multiple=True,
    help="Path that should not generate warnings when outside storage profile locations. "
    "Can be specified multiple times for different paths.",
)
@click.argument("job_bundle_dir")
@_handle_error
def bundle_submit(
    job_bundle_dir,
    job_attachments_file_system,
    parameter,
    known_asset_path,
    name,
    priority,
    max_failed_tasks_count,
    max_retries_per_task,
    max_worker_count,
    require_paths_exist,
    submitter_name,
    **args,
):
    """
    Submits an Open Job Description job bundle.
    """
    # Apply the CLI args to the config
    config = _apply_cli_options_to_config(required_options={"farm_id", "queue_id"}, **args)

    hash_callback_manager = _ProgressBarCallbackManager(length=100, label="Hashing Attachments")
    upload_callback_manager = _ProgressBarCallbackManager(length=100, label="Uploading Attachments")

    def _check_create_job_wait_canceled() -> bool:
        return sigint_handler.continue_operation

    try:
        job_id = api.create_job_from_job_bundle(
            job_bundle_dir=job_bundle_dir,
            job_parameters=parameter,
            name=name,
            job_attachments_file_system=job_attachments_file_system,
            config=config,
            priority=priority,
            max_failed_tasks_count=max_failed_tasks_count,
            max_retries_per_task=max_retries_per_task,
            max_worker_count=max_worker_count,
            hashing_progress_callback=hash_callback_manager.callback,
            upload_progress_callback=upload_callback_manager.callback,
            create_job_result_callback=_check_create_job_wait_canceled,
            print_function_callback=click.echo,
            interactive_confirmation_callback=_interactive_confirmation_prompt,
            require_paths_exist=require_paths_exist,
            submitter_name=submitter_name or "CLI",
            known_asset_paths=known_asset_path,
        )

        # Check Whether the CLI options are modifying any of the default settings that affect
        # the job id. If not, we'll save the job id submitted as the default job id.
        # If the submission is canceled by the user job_id will be None, so ignore this case as well.
        if (
            args.get("profile") is None
            and args.get("farm_id") is None
            and args.get("queue_id") is None
            and args.get("storage_profile_id") is None
        ):
            config_file.set_setting("defaults.job_id", job_id)

    except AssetSyncCancelledError as exc:
        if sigint_handler.continue_operation:
            raise DeadlineOperationError(f"Job submission unexpectedly canceled:\n{exc}") from exc
        else:
            click.echo("Job submission canceled.")
            sys.exit(1)
    except AssetSyncError as exc:
        raise DeadlineOperationError(f"Failed to upload job attachments:\n{exc}") from exc
    except CreateJobWaiterCanceled as exc:
        if sigint_handler.continue_operation:
            raise DeadlineOperationError(
                f"Unexpectedly canceled during wait for final status of CreateJob:\n{exc}"
            ) from exc
        else:
            click.echo("Canceled waiting for final status of CreateJob.")
            sys.exit(1)
    except ClientError as exc:
        raise DeadlineOperationError(
            f"Failed to submit the job bundle to AWS Deadline Cloud:\n{exc}"
        ) from exc
    except MisconfiguredInputsError as exc:
        click.echo(str(exc))
        click.echo("Job submission canceled.")
        sys.exit(1)
    except Exception as exc:
        api.get_deadline_cloud_library_telemetry_client().record_error(
            event_details={"exception_scope": "on_submit"},
            exception_type=str(type(exc)),
        )
        raise


@cli_bundle.command(name="gui-submit")
@click.argument("job_bundle_dir", required=False)
@click.option(
    "--browse",
    is_flag=True,
    help="Opens a folder browser to select a bundle.",
)
@click.option(
    "--install-gui",
    is_flag=True,
    help="Installs GUI dependencies if they are not installed already",
)
@click.option(
    "--submitter-name",
    type=click.STRING,
    help="Name of the application submitting the bundle. If a name is specified, the GUI will automatically close after submitting the job.",
)
@click.option(
    "--output",
    type=click.Choice(
        ["verbose", "json"],
        case_sensitive=False,
    ),
    default="verbose",
    help="Specifies the output format of the messages printed to stdout.\n"
    "VERBOSE: Displays messages in a human-readable text format.\n"
    "JSON: Displays messages in JSON line format, so that the info can be easily "
    "parsed/consumed by custom scripts.",
)
@click.option(
    "--known-asset-path",
    multiple=True,
    help="Path that should not generate warnings when outside storage profile locations. "
    "Can be specified multiple times for different paths.",
)
@_handle_error
def bundle_gui_submit(
    job_bundle_dir, browse, output, install_gui, submitter_name, known_asset_path, **args
):
    """
    Opens a GUI to submit an Open Job Description job bundle.
    """
    from ...ui import gui_context_for_cli

    with gui_context_for_cli(automatically_install_dependencies=install_gui) as app:
        from ...ui.job_bundle_submitter import show_job_bundle_submitter

        if not job_bundle_dir and not browse:
            raise DeadlineOperationError(
                "Specify a job bundle directory or run the bundle command with the --browse flag"
            )
        output = output.lower()

        submitter = show_job_bundle_submitter(
            input_job_bundle_dir=job_bundle_dir,
            browse=browse,
            submitter_name=submitter_name,
            known_asset_paths=known_asset_path,
        )

        if not submitter:
            return

        submitter.show()

        app.exec()

        _print_response(
            output=output,
            job_bundle_dir=job_bundle_dir,
            job_history_bundle_dir=submitter.job_history_bundle_dir,
            job_id=submitter.job_id,
        )


def _print_response(
    output: str,
    job_bundle_dir: str,
    job_history_bundle_dir: Optional[str],
    job_id: Optional[str],
):
    if output == "json":
        if job_id:
            response: dict[str, Any] = {
                "status": "SUBMITTED",
                "jobId": job_id,
                "jobHistoryBundleDirectory": job_history_bundle_dir,
            }
            click.echo(json.dumps(response))
        else:
            click.echo(json.dumps({"status": "CANCELED"}))
    else:
        if job_id:
            click.echo("Submitted job bundle:")
            click.echo(f"   {job_bundle_dir}")
            click.echo(f"Job ID: {job_id}")
        else:
            click.echo("Job submission canceled.")
