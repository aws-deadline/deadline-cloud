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
import tempfile
import shutil
import os
from dataclasses import fields

import click
from botocore.exceptions import ClientError

from ... import api
from ...api._monitor_urls import _get_job_monitor_url
from ...config import config_file
from ...dataclasses import SubmitterInfo
from ...job_bundle.loader import is_job_bundle_dir
from ...job_bundle._repository import (
    BundleRepository,
    LocalBundleRepository,
    S3BundleRepository,
    S3_JOB_BUNDLES_PREFIX,
    _parse_template,
    archive_bundle_dir,
    build_bundle_metadata,
    extract_bundle_info,
    get_bundle_dir_size,
    read_template_from_archive,
    sanitize_bundle_name,
)
from ....job_attachments.exceptions import (
    AssetSyncError,
    AssetSyncCancelledError,
    MisconfiguredInputsError,
)
from ....job_attachments._aws.deadline import get_queue
from ....job_attachments.models import JobAttachmentsFileSystem

from ...exceptions import DeadlineOperationError, CreateJobWaiterCanceled
from .._common import (
    _OUTPUT_FORMAT_HELP,
    _apply_cli_options_to_config,
    _handle_error,
    _ProgressBarCallbackManager,
    _parse_multi_format_parameters,
    _resolve_output_format,
    _suggest_resources_on_client_error,
)
from .._main import deadline as main
from ._sigint_handler import SigIntHandler

logger = logging.getLogger(__name__)

# Set up the signal handler for handling Ctrl + C interruptions.
sigint_handler = SigIntHandler()


@main.group(name="bundle")
@_handle_error
def cli_bundle():
    """
    Submit Open Job Description job bundles to a Deadline Cloud queue.

    Use `submit` for headless/scripted submission, or `gui-submit` to
    review and edit parameters in a GUI before submitting.

    \b
    Learn more about [job bundles](https://docs.aws.amazon.com/deadline-cloud/latest/developerguide/build-job-bundle.html)
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


def _validate_submitter_info(ctx, param, values):
    """
    Validate provided --submitter-info value and convert to SubmitterInfo object.

    Supports three input formats that can be mixed:
    - Key=value pairs: --submitter-info submitter_name=MyApp --submitter-info host_application_name=Maya
    - Inline JSON strings: --submitter-info '{"submitter_name": "MyApp", "additional_info": {"custom": "data"}}'
    - File paths (JSON or YAML): --submitter-info file://path/to/submitter.json

    All keys must be valid SubmitterInfo fields. Unknown keys will raise an error.
    """
    if not values:
        return None

    # Get valid field names from SubmitterInfo dataclass
    valid_fields = {field.name for field in fields(SubmitterInfo)}

    info_dict = _parse_multi_format_parameters(list(values))

    # Validate all keys
    for key in info_dict.keys():
        if key not in valid_fields:
            raise click.BadParameter(
                f"Unknown field '{key}'. Valid fields are: {', '.join(sorted(valid_fields))}"
            )

    # Ensure submitter_name is provided as a required field
    if "submitter_name" not in info_dict:
        raise click.BadParameter(
            "submitter_name is required when using --submitter-info. "
            "Example: --submitter-info submitter_name=MyApp"
        )

    try:
        return SubmitterInfo(**info_dict)
    except TypeError as e:
        raise click.BadParameter(f"Failed to create SubmitterInfo: {e}") from e


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
    help=(
        "The values for the job template's parameters. Can be provided as key-value pairs, inline JSON strings, "
        "or as paths to a JSON or YAML document. Later values for repeated parameter names take precedence. "
        'Examples: --parameter MyParam=5 -p file://parameter_file.json -p \'{"OtherParam": "10"}\''
    ),
)
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The farm to use.")
@click.option("--region", help="The AWS region of the farm.")
@click.option("--queue-id", help="The queue to use.")
@click.option("--storage-profile-id", help="The storage profile to use.")
@click.option(
    "--name",
    help="Override the job name. Defaults to the `name` field in the bundle's job template.",
)
@click.option(
    "--priority",
    type=int,
    default=50,
    help="Job priority, 0-100 (default 50). Jobs with a higher priority run first.",
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
    "--target-task-run-status",
    type=click.Choice(["READY", "SUSPENDED"], case_sensitive=False),
    help="The target task run status for the job. READY means tasks will start immediately, "
    "SUSPENDED means tasks will be created but not start until manually resumed.",
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
    help="Skip the interactive confirmation prompt. Required for non-interactive/scripted use.",
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
@click.option(
    "--save-debug-snapshot",
    help="EXPERIMENTAL - Instead of submitting the job, generate a debug snapshot as a directory or a zip file if the extension is .zip."
    " It includes the job attachments and parameters for creating the job."
    " You can later run the bash script in the snapshot to submit the job using AWS CLI commands.",
)
@click.option(
    "--force-s3-check/--no-force-s3-check",
    default=None,
    help="Force verification that job attachments exist in S3 before skipping upload. "
    "Use when S3 bucket contents may be out of sync with local caches. "
    "Overrides the 'settings.force_s3_check' config setting.",
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
    target_task_run_status,
    require_paths_exist,
    submitter_name,
    save_debug_snapshot,
    force_s3_check,
    **args,
):
    """
    Submits an Open Job Description job bundle to a Deadline Cloud queue.
    You can provide options to set parameter values, the job name, priority,
    and more.

    JOB_BUNDLE_DIR is a DIRECTORY (not a file) that must contain a `template.yaml`
    (or `template.json`) -- the OpenJD job template. It may optionally contain
    `parameter_values` and `asset_references` files (also either .yaml or .json).

    \b
    Example:
      deadline bundle submit ./my_job --yes

    The command returns a job id (job-xxxx). Use `deadline job get --job-id <id>`
    to see its current taskRunStatus, or `deadline job wait --job-id <id>` to
    block until the job reaches a terminal state (SUCCEEDED / FAILED / CANCELED).

    \b
    Learn more about [job bundles](https://docs.aws.amazon.com/deadline-cloud/latest/developerguide/build-job-bundle.html)
    """
    # Apply the CLI args to the config
    config = _apply_cli_options_to_config(required_options={"farm_id", "queue_id"}, **args)

    # Resolve force_s3_check: CLI flag takes precedence, otherwise use config setting
    if force_s3_check is None:
        force_s3_check = config_file.str2bool(
            config_file.get_setting("settings.force_s3_check", config=config)
        )

    # Resolve max_retries_per_task and max_failed_tasks_count from config when not specified
    if max_retries_per_task is None:
        max_retries_per_task = int(
            config_file.get_setting("settings.max_retries_per_task", config=config)
        )
    if max_failed_tasks_count is None:
        max_failed_tasks_count = int(
            config_file.get_setting("settings.max_failed_tasks_count", config=config)
        )

    hash_callback_manager = _ProgressBarCallbackManager(length=100, label="Hashing Attachments")
    upload_callback_manager = _ProgressBarCallbackManager(length=100, label="Uploading Attachments")

    def _check_create_job_wait_canceled() -> bool:
        return sigint_handler.continue_operation

    try:
        snapshot_tmpdir = None
        if save_debug_snapshot:
            save_debug_snapshot = os.path.abspath(save_debug_snapshot)

            # If the debug snapshot is to a zip file, first put it in a temporary directory
            if save_debug_snapshot.endswith(".zip"):
                snapshot_tmpdir = tempfile.TemporaryDirectory()

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
            target_task_run_status=target_task_run_status,
            hashing_progress_callback=hash_callback_manager.callback,
            upload_progress_callback=upload_callback_manager.callback,
            create_job_result_callback=_check_create_job_wait_canceled,
            print_function_callback=click.echo,
            interactive_confirmation_callback=_interactive_confirmation_prompt,
            require_paths_exist=require_paths_exist,
            submitter_name=submitter_name or "CLI",
            known_asset_paths=known_asset_path,
            debug_snapshot_dir=(snapshot_tmpdir.name if snapshot_tmpdir else save_debug_snapshot),
            force_s3_check=force_s3_check,
        )

        if snapshot_tmpdir:
            # Put the snapshot in a zip file
            os.makedirs(os.path.dirname(save_debug_snapshot), exist_ok=True)
            shutil.make_archive(save_debug_snapshot, "zip", snapshot_tmpdir.name)

        if save_debug_snapshot:
            click.echo("Saved job debug snapshot:")
            click.echo(f"    {save_debug_snapshot}")

        # Check Whether the CLI options are modifying any of the default settings that affect
        # the job id. If not, we'll save the job id submitted as the default job id.
        # If a job snapshot directory was provided, the job_id will be None.
        if (
            args.get("profile") is None
            and args.get("farm_id") is None
            and args.get("region") is None
            and args.get("queue_id") is None
            and args.get("storage_profile_id") is None
            and job_id
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
        suggestion = _suggest_resources_on_client_error(
            exc,
            farm_id=config_file.get_setting("defaults.farm_id", config=config),
            queue_id=config_file.get_setting("defaults.queue_id", config=config),
            config=config,
        )
        raise DeadlineOperationError(
            f"Failed to submit the job bundle to AWS Deadline Cloud:\n{exc}{suggestion}"
        ) from exc
    except MisconfiguredInputsError as exc:
        click.echo(str(exc))
        click.echo("Job submission canceled.")
        sys.exit(1)
    except Exception as exc:
        api.get_deadline_cloud_library_telemetry_client().record_error_with_trace(exc, "on_submit")
        raise
    finally:
        if snapshot_tmpdir:
            snapshot_tmpdir.cleanup()


@cli_bundle.command(name="gui-submit")
@click.option(
    "-p",
    "--parameter",
    multiple=True,
    callback=validate_parameters,
    help=(
        "Initial values in the GUI for the job template's parameters. Can be provided as key-value pairs, inline JSON strings, "
        "or as paths to a JSON or YAML document. Later values for repeated parameter names take precedence. "
        'Examples: --parameter MyParam=5 -p file://parameter_file.json -p \'{"OtherParam": "10"}\''
    ),
)
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
    help="[DEPRECATED] Use --submitter-info submitter_name=<name> instead. Name of the application submitting the bundle. If a name is specified, the GUI will automatically close after submitting the job.",
)
@click.option(
    "--output",
    type=click.Choice(
        ["verbose", "json"],
        case_sensitive=False,
    ),
    default=None,
    help=_OUTPUT_FORMAT_HELP,
)
@click.option(
    "--known-asset-path",
    multiple=True,
    help="Path that should not generate warnings when outside storage profile locations. "
    "Can be specified multiple times for different paths.",
)
@click.option(
    "--submitter-info",
    multiple=True,
    callback=_validate_submitter_info,
    help="Submitter and environment information. Supports key=value pairs, inline JSON strings, "
    "and file paths (JSON or YAML). Later values for repeated fields take precedence. "
    "Examples: --submitter-info submitter_name=MyApp --submitter-info host_application_name=Maya "
    'OR --submitter-info \'{"submitter_name": "MyApp", "additional_info": {"render_engine": "Cycles"}}\' '
    "OR --submitter-info file://path/to/submitter.json",
)
@click.option("--name", help="The job name to use in place of the one in the job bundle.")
@_handle_error
def bundle_gui_submit(
    parameter,
    job_bundle_dir,
    browse,
    output,
    install_gui,
    known_asset_path,
    submitter_name,
    submitter_info,
    name,
    **args,
):
    """
    Opens a GUI to submit an Open Job Description job bundle to a Deadline
    Cloud queue. You can provide options to set the initial parameter values
    shown in the GUI.

    \b
    Learn more about [job bundles](https://docs.aws.amazon.com/deadline-cloud/latest/developerguide/build-job-bundle.html)
    """

    if submitter_name:
        click.echo(
            click.style(
                "DeprecationWarning: The option --submitter-name is deprecated. Use --submitter-info instead.",
                fg="red",
            ),
            err=True,
        )
        if submitter_info:
            # --submitter-name takes precedence if we already have submitter_info provided
            submitter_info.submitter_name = submitter_name
        else:
            submitter_info = SubmitterInfo(submitter_name=submitter_name)

    from ...ui import gui_context_for_cli
    from ...ui._utils import tr

    with gui_context_for_cli(automatically_install_dependencies=install_gui) as app:
        # Pre-warm boto3 session + Deadline client (lru_cached, reused by background thread)
        if browse:
            try:
                from ...api import get_boto3_session, get_boto3_client

                get_boto3_session()
                get_boto3_client("deadline")
            except Exception:
                pass  # Non-fatal — background thread will handle it

        from ...ui.job_bundle_submitter import show_job_bundle_submitter

        if not job_bundle_dir and not browse:
            raise DeadlineOperationError(
                tr(
                    "Specify a job bundle directory or run the bundle command with the --browse flag"
                )
            )
        output = _resolve_output_format(output)

        submitter = show_job_bundle_submitter(
            input_job_bundle_dir=job_bundle_dir,
            browse=browse,
            submitter_info=submitter_info,
            known_asset_paths=known_asset_path,
            job_parameters=parameter,
            name=name,
        )

        if not submitter:
            return

        submitter.show()

        app.exec()

        job_url = None
        if submitter.job_id:
            # Best-effort monitor URL (only when using Deadline Cloud monitor
            # credentials). The GUI submitter uses the default config.
            job_url = _get_job_monitor_url(
                farm_id=config_file.get_setting("defaults.farm_id"),
                queue_id=config_file.get_setting("defaults.queue_id"),
                job_id=submitter.job_id,
            )

        _print_response(
            output=output,
            job_bundle_dir=job_bundle_dir,
            job_history_bundle_dir=submitter.job_history_bundle_dir,
            job_id=submitter.job_id,
            job_url=job_url,
        )


def _print_response(
    output: str,
    job_bundle_dir: str,
    job_history_bundle_dir: Optional[str],
    job_id: Optional[str],
    job_url: Optional[str] = None,
):
    if output == "json":
        if job_id:
            response: dict[str, Any] = {
                "status": "SUBMITTED",
                "jobId": job_id,
                "jobHistoryBundleDirectory": job_history_bundle_dir,
            }
            if job_url:
                response["jobUrl"] = job_url
            click.echo(json.dumps(response))
        else:
            click.echo(json.dumps({"status": "CANCELED"}))
    else:
        if job_id:
            click.echo("Submitted job bundle:")
            click.echo(f"   {job_bundle_dir}")
            click.echo(f"Job ID: {job_id}")
            if job_url:
                click.echo(f"Job URL: {job_url}")
        else:
            click.echo("Job submission canceled.")


def _get_queue_s3_settings(config):
    """Get the queue's job attachment S3 settings from config."""
    farm_id = config_file.get_setting("defaults.farm_id", config=config)
    queue_id = config_file.get_setting("defaults.queue_id", config=config)
    if not farm_id or not queue_id:
        raise DeadlineOperationError(
            "A default farm and queue must be configured. Run 'deadline config set defaults.farm_id <id>' and 'deadline config set defaults.queue_id <id>'."
        )
    boto3_session = api.get_boto3_session(config=config)
    queue = get_queue(farm_id=farm_id, queue_id=queue_id, session=boto3_session)
    if not queue.jobAttachmentSettings:
        raise DeadlineOperationError(
            f"Queue {queue_id} does not have job attachment settings configured."
        )
    # Use queue role credentials for S3 access (required for DCM profiles)
    deadline_client = api.get_boto3_client("deadline", config=config)
    s3_session = api.get_queue_user_boto3_session(
        deadline=deadline_client, config=config, farm_id=farm_id, queue_id=queue_id
    )
    return queue.jobAttachmentSettings, s3_session


@cli_bundle.command(name="list")
@click.argument("path", required=False)
@click.option(
    "--queue",
    "use_queue",
    is_flag=True,
    help="List bundles shared on the queue.",
)
@click.option(
    "--show-hidden",
    is_flag=True,
    help="Include hidden bundles in the output (queue only).",
)
@click.option(
    "--no-archives",
    is_flag=True,
    help="Skip archive files when listing local bundles.",
)
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The farm to use.")
@click.option("--queue-id", help="The queue to use.")
@click.option(
    "--output",
    type=click.Choice(["verbose", "json"], case_sensitive=False),
    default=None,
    help=_OUTPUT_FORMAT_HELP,
)
@_handle_error
def bundle_list(path, use_queue, show_hidden, no_archives, output, **args):
    """
    List job bundles.

    \b
    With no arguments, lists bundles in the configured default local directory
    (settings.job_bundle_default_directory, or home if not set).
    With PATH, lists bundles in that local directory.
    With --queue, lists bundles shared on the queue.
    """
    output = _resolve_output_format(output)

    hidden_set: set[str] = set()
    if use_queue:
        config = _apply_cli_options_to_config(required_options={"farm_id", "queue_id"}, **args)
        repo: BundleRepository = S3BundleRepository.from_config(config)
        hidden_set = repo.get_hidden_set()  # type: ignore[attr-defined]
    else:
        if path:
            local_root = os.path.abspath(path)
        else:
            local_root = config_file.get_setting("settings.job_bundle_default_directory")
            if not local_root:
                local_root = os.path.expanduser("~")
            local_root = os.path.expanduser(local_root)
        repo = LocalBundleRepository(root=local_root, include_archives=not no_archives)

    entries = repo.list_entries(repo.root_path())
    bundles = [e for e in entries if e.is_bundle]

    # Filter hidden bundles unless --show-hidden
    if use_queue and not show_hidden:
        bundles = [e for e in bundles if e.name not in hidden_set]

    if output == "json":
        result = [
            {
                "name": e.name,
                "path": e.path,
                "format": "archive" if e.is_archive else "folder",
                **({"hidden": True} if e.name in hidden_set else {}),
            }
            for e in bundles
        ]
        click.echo(json.dumps(result, indent=2))
    else:
        for e in bundles:
            suffix = " (hidden)" if e.name in hidden_set else ""
            click.echo(f"{e.name}{suffix}")


@cli_bundle.command(name="upload")
@click.argument("job_bundle_dir")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The farm to use.")
@click.option("--queue-id", help="The queue to use.")
@click.option(
    "--name",
    help="Name for the shared archive on the queue. Defaults to the bundle directory name.",
)
@click.option(
    "--yes",
    is_flag=True,
    help="Overwrite an existing shared bundle without prompting (for non-interactive use).",
)
@_handle_error
def bundle_upload(job_bundle_dir, name, yes, **args):
    """
    Upload a job bundle to share on the queue as an .ojd archive.
    """
    config = _apply_cli_options_to_config(required_options={"farm_id", "queue_id"}, **args)
    s3_settings, boto3_session = _get_queue_s3_settings(config)

    job_bundle_dir = os.path.abspath(job_bundle_dir)

    # Determine if input is an .ojd archive or a directory
    is_archive_input = os.path.isfile(job_bundle_dir) and job_bundle_dir.endswith(".ojd")

    if not is_archive_input and not is_job_bundle_dir(job_bundle_dir):
        raise DeadlineOperationError(
            f"Directory does not appear to be a job bundle (no template.yaml or template.json): {job_bundle_dir}"
        )

    # Parse the template to extract metadata for S3 object metadata
    bundle_metadata = {}
    if is_archive_input:
        result = read_template_from_archive(job_bundle_dir)
        if result:
            raw, fname = result
            template = _parse_template(raw, fname)
            if template:
                info = extract_bundle_info(template, job_bundle_dir)
                bundle_metadata = build_bundle_metadata(bundle_info=info)
    else:
        bundle_metadata = build_bundle_metadata(job_bundle_dir)

    bundle_name = name or os.path.basename(job_bundle_dir)
    if is_archive_input and bundle_name.endswith(".ojd"):
        bundle_name = bundle_name[:-4]
    # Sanitize the name the same way the GUI export path does: replace characters
    # that are unsafe in a filename and reject empty or path-traversal names. This
    # keeps CLI and GUI behavior consistent and prevents a name like "../foo" or
    # "a/b" from being written to an arbitrary S3 sub-prefix (or with ".."
    # segments) that the browser and `bundle list` would not find consistently.
    try:
        bundle_name = sanitize_bundle_name(bundle_name)
    except ValueError:
        raise DeadlineOperationError(
            "Bundle name is empty or invalid. Use --name to specify a valid name."
        )
    prefix = f"{s3_settings.rootPrefix.rstrip('/')}/{S3_JOB_BUNDLES_PREFIX}"
    s3_key = f"{prefix}/{bundle_name}.ojd"
    if len(s3_key) > 1024:
        raise DeadlineOperationError(
            f"Bundle name is too long. S3 key would be {len(s3_key)} characters (max 1024)."
        )

    s3 = boto3_session.client("s3")

    # Check if bundle already exists
    try:
        s3.head_object(Bucket=s3_settings.s3BucketName, Key=s3_key)
        if not yes and not click.confirm(
            f"Bundle '{bundle_name}' already exists on the queue. Overwrite?"
        ):
            click.echo("Upload canceled.")
            return
    except ClientError as e:
        if e.response["Error"]["Code"] != "404":
            raise

    # Archive and upload
    # Advertise the archive's true type as a courtesy hint for other consumers.
    # The download/browse paths do NOT trust ContentType (it's set by the
    # uploader) and validate by parsing the zip; this just keeps the object
    # correctly typed. Kept consistent with S3BundleRepository.upload_archive.
    extra_args: dict = {"ContentType": "application/zip"}
    if bundle_metadata:
        extra_args["Metadata"] = bundle_metadata
    if is_archive_input:
        # Already an .ojd — upload directly
        file_size = os.path.getsize(job_bundle_dir)
        with (
            open(job_bundle_dir, "rb") as f,
            click.progressbar(length=file_size, label="Uploading") as bar,  # type: ignore[var-annotated]
        ):
            s3.upload_fileobj(
                f,
                s3_settings.s3BucketName,
                s3_key,
                ExtraArgs=extra_args,
                Callback=lambda bytes_sent: bar.update(bytes_sent),
            )
    else:
        total_size = get_bundle_dir_size(job_bundle_dir)
        with click.progressbar(length=total_size, label="Archiving") as bar:  # type: ignore[var-annotated]
            buf = archive_bundle_dir(job_bundle_dir, progress_callback=lambda n: bar.update(n))

        file_size = buf.getbuffer().nbytes
        with click.progressbar(length=file_size, label="Uploading") as bar:  # type: ignore[var-annotated]
            s3.upload_fileobj(
                buf,
                s3_settings.s3BucketName,
                s3_key,
                ExtraArgs=extra_args,
                Callback=lambda bytes_sent: bar.update(bytes_sent),
            )
    click.echo(f"Uploaded bundle to s3://{s3_settings.s3BucketName}/{s3_key}")


@cli_bundle.command(name="download")
@click.argument("bundle_name")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The farm to use.")
@click.option("--queue-id", help="The queue to use.")
@click.option(
    "-o",
    "--output-dir",
    default=None,
    help="Local directory to copy the bundle to. If not specified, uses the local cache.",
)
@click.option(
    "--yes",
    is_flag=True,
    help="Overwrite the destination directory if it already exists (for non-interactive use).",
)
@click.option(
    "--output",
    type=click.Choice(["verbose", "json"], case_sensitive=False),
    default=None,
    help=_OUTPUT_FORMAT_HELP,
)
@_handle_error
def bundle_download(bundle_name, output_dir, yes, output, **args):
    """
    Download a shared job bundle from the queue.

    BUNDLE_NAME is the name of the bundle (e.g. 'blender-render').
    """
    output = _resolve_output_format(output)
    # Validate the name up front so an unsafe name (e.g. "..") produces a clean
    # error rather than a raw traceback from the local dest-path derivation.
    try:
        safe_bundle_name = sanitize_bundle_name(bundle_name)
    except ValueError:
        raise DeadlineOperationError(f"Bundle name '{bundle_name}' is not a valid bundle name.")

    config = _apply_cli_options_to_config(required_options={"farm_id", "queue_id"}, **args)
    repo = S3BundleRepository.from_config(config)

    if output_dir:
        output_dir = os.path.abspath(output_dir)
        os.makedirs(output_dir, exist_ok=True)

    # List entries to find the bundle by name
    entries = repo.list_entries(repo.root_path())
    match = None
    for entry in entries:
        if entry.name == bundle_name and entry.is_bundle:
            match = entry
            break

    if not match:
        available = [e.name for e in entries if e.is_bundle]
        msg = f"Bundle '{bundle_name}' not found in {repo.root_path()}"
        if available:
            msg += f"\nAvailable bundles: {', '.join(available)}"
        raise DeadlineOperationError(msg)

    # Get file size for progress bar
    file_size = repo.get_bundle_size(match.path)

    # Download and extract are sequential inside download_full_bundle. Show
    # progress bars only in human (non-json) output — matching the convention in
    # `job download-output` (`if not is_json_format:`) so machine-readable JSON on
    # stdout is never interleaved with progress rendering.
    show_progress = output != "json"
    _bars: dict = {}

    def _dl_callback(n):
        if "dl" not in _bars:
            _bars["dl"] = click.progressbar(length=file_size, label="Downloading")
            _bars["dl_ctx"] = _bars["dl"].__enter__()
        _bars["dl_ctx"].update(n)

    def _ex_callback(n):
        if "dl" in _bars and "dl_closed" not in _bars:
            _bars["dl_closed"] = True
            _bars["dl"].__exit__(None, None, None)
        if "ex" not in _bars:
            _bars["ex"] = click.progressbar(
                length=_bars.get("ex_size", file_size), label="Extracting"
            )
            _bars["ex_ctx"] = _bars["ex"].__enter__()
        _bars["ex_ctx"].update(n)

    def _ex_size_callback(total):
        _bars["ex_size"] = total

    local_path = repo.download_full_bundle(
        match.path,
        progress_callback=_dl_callback if show_progress else None,
        extract_callback=_ex_callback if show_progress else None,
        extract_size_callback=_ex_size_callback if show_progress else None,
    )
    if "dl" in _bars and "dl_closed" not in _bars:
        _bars["dl"].__exit__(None, None, None)
    if "ex" in _bars:
        _bars["ex"].__exit__(None, None, None)
    # download_full_bundle resolves to cache; copy to user's output_dir if specified
    if output_dir:
        dest_path = os.path.join(output_dir, safe_bundle_name)
        if os.path.exists(dest_path):
            # Only ever recursively delete a path that is itself a job bundle
            # (i.e. a prior download of this bundle). Refuse to clobber an
            # arbitrary folder/file that merely collides with the bundle name,
            # so a name matching an existing project directory can't be
            # destroyed — even with --yes from a non-interactive caller.
            if not is_job_bundle_dir(dest_path):
                raise DeadlineOperationError(
                    f"'{dest_path}' already exists and is not a job bundle. "
                    "Choose a different name or output directory."
                )
            # Deleting the user's existing bundle is destructive, so require
            # confirmation (or --yes for non-interactive callers) rather than
            # clobbering it silently.
            if not yes and not click.confirm(
                f"'{dest_path}' already exists and will be overwritten. Continue?",
                default=False,
            ):
                raise DeadlineOperationError(
                    "Download canceled; pass --yes to overwrite the destination."
                )
        # Copy into a staging dir on the same filesystem, then swap into place
        # only once the copy succeeds, so a failed copy (permissions, disk full)
        # can't leave the user with neither the old nor the new bundle. The swap
        # is a fast rename because staging lives under output_dir.
        with tempfile.TemporaryDirectory(dir=output_dir) as staging:
            staged = os.path.join(staging, safe_bundle_name)
            shutil.copytree(local_path, staged)
            if os.path.exists(dest_path):
                shutil.rmtree(dest_path)
            shutil.move(staged, dest_path)
        result_path = dest_path
    else:
        result_path = local_path

    if output == "json":
        click.echo(json.dumps({"path": result_path}))
    else:
        click.echo(f"Downloaded bundle to: {result_path}")


@cli_bundle.command(name="hide")
@click.argument("bundle_name")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The farm to use.")
@click.option("--queue-id", help="The queue to use.")
@_handle_error
def bundle_hide(bundle_name, **args):
    """
    Hide a shared queue bundle from your own listings.

    Hiding is a private, per-user view preference stored in a local file — it
    never changes anything on S3 and does not affect what other users see. The
    bundle is simply no longer shown in the browser or `deadline bundle list`
    by default; use `deadline bundle list --show-hidden` to see it again.
    """
    config = _apply_cli_options_to_config(required_options={"farm_id", "queue_id"}, **args)
    repo = S3BundleRepository.from_config(config)

    # Validate the name against the listing (like download/info) so a typo isn't
    # silently persisted forever in the local visibility file.
    entries = repo.list_entries(repo.root_path())
    match = next((e for e in entries if e.name == bundle_name and e.is_bundle), None)
    if not match:
        available = [e.name for e in entries if e.is_bundle]
        msg = f"Bundle '{bundle_name}' not found on queue."
        if available:
            msg += f"\nAvailable bundles: {', '.join(available)}"
        raise DeadlineOperationError(msg)

    key = repo.visibility_key(match.path)
    hidden_set = repo.get_hidden_set()
    if key in hidden_set:
        click.echo(f"Bundle already hidden: {bundle_name}")
        return

    repo.set_bundle_visibility(key, hidden=True)
    click.echo(f"Hidden bundle: {bundle_name}")


@cli_bundle.command(name="unhide")
@click.argument("bundle_name")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The farm to use.")
@click.option("--queue-id", help="The queue to use.")
@_handle_error
def bundle_unhide(bundle_name, **args):
    """
    Unhide a previously hidden queue bundle in your own listings.

    Clears the local, per-user hidden preference so the bundle is shown again
    in the browser and `deadline bundle list`. No S3 changes are made.
    """
    config = _apply_cli_options_to_config(required_options={"farm_id", "queue_id"}, **args)
    repo = S3BundleRepository.from_config(config)

    hidden_set = repo.get_hidden_set()
    if bundle_name not in hidden_set:
        click.echo(f"Bundle is not hidden: {bundle_name}")
        return

    repo.set_bundle_visibility(bundle_name, hidden=False)
    click.echo(f"Unhidden bundle: {bundle_name}")


@cli_bundle.command(name="info")
@click.argument("bundle_name")
@click.option(
    "--queue",
    "use_queue",
    is_flag=True,
    help="Inspect a bundle shared on the queue.",
)
@click.option(
    "--output",
    type=click.Choice(["verbose", "json"], case_sensitive=False),
    default=None,
    help=_OUTPUT_FORMAT_HELP,
)
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The farm to use.")
@click.option("--queue-id", help="The queue to use.")
@_handle_error
def bundle_info(bundle_name, use_queue, output, **args):
    """
    Show details about a job bundle (name, description, steps, parameters).

    BUNDLE_NAME is either a local path to a job bundle directory, or the name
    of a shared bundle on the queue (when used with --queue). For local bundles,
    if the path doesn't exist, searches by name in the current directory and then
    the configured job bundle default directory.
    """
    output = _resolve_output_format(output)
    if use_queue:
        config = _apply_cli_options_to_config(required_options={"farm_id", "queue_id"}, **args)
        repo: BundleRepository = S3BundleRepository.from_config(config)
        # Find the bundle by name in the listing
        entries = repo.list_entries(repo.root_path())
        match = next((e for e in entries if e.name == bundle_name and e.is_bundle), None)
        if not match:
            available = [e.name for e in entries if e.is_bundle]
            msg = f"Bundle '{bundle_name}' not found on queue."
            if available:
                msg += f"\nAvailable bundles: {', '.join(available)}"
            raise DeadlineOperationError(msg)
        info = repo.get_bundle_info(match.path)
    else:
        bundle_path = os.path.abspath(bundle_name)
        if not os.path.isdir(bundle_path):
            # Search by name in cwd, then configured default directory
            for search_dir in [
                os.getcwd(),
                os.path.expanduser(
                    config_file.get_setting("settings.job_bundle_default_directory") or ""
                ),
            ]:
                if not search_dir:
                    continue
                candidate = os.path.join(search_dir, bundle_name)
                if os.path.isdir(candidate) and is_job_bundle_dir(candidate):
                    bundle_path = candidate
                    break
            else:
                raise DeadlineOperationError(
                    f"Bundle '{bundle_name}' not found as a path, in current directory, "
                    "or in the configured job bundle default directory."
                )
        repo = LocalBundleRepository(root=os.path.dirname(bundle_path))
        info = repo.get_bundle_info(bundle_path)

    if not info:
        raise DeadlineOperationError(
            f"Could not read bundle template for '{bundle_name}'. "
            "The template may be missing or malformed."
        )

    if output == "json":
        result = info.to_dict()
        result["path"] = info.path
        click.echo(json.dumps(result, indent=2))
    else:
        click.echo(f"Path: {info.path}")
        click.echo(info.format_text())
