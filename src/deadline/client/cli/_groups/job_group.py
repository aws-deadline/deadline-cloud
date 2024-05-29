# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
All the `deadline job` commands.
"""

from __future__ import annotations
import json
import logging
from configparser import ConfigParser
from pathlib import Path
import os
import sys
from typing import Optional, Union
import datetime
from typing import Any

import click
from botocore.exceptions import ClientError

from deadline.client.api._session import _modified_logging_level
from deadline.job_attachments.download import OutputDownloader
from deadline.job_attachments.models import (
    FileConflictResolution,
    JobAttachmentS3Settings,
    PathFormat,
)
from deadline.job_attachments.progress_tracker import (
    DownloadSummaryStatistics,
    ProgressReportMetadata,
)
from deadline.job_attachments._utils import _human_readable_file_size

from ... import api
from ...config import config_file
from ...exceptions import DeadlineOperationError
from .._common import _apply_cli_options_to_config, _cli_object_repr, _handle_error
from ._sigint_handler import SigIntHandler

JSON_MSG_TYPE_TITLE = "title"
JSON_MSG_TYPE_PRESUMMARY = "presummary"
JSON_MSG_TYPE_PATH = "path"
JSON_MSG_TYPE_PATHCONFIRM = "pathconfirm"
JSON_MSG_TYPE_PROGRESS = "progress"
JSON_MSG_TYPE_SUMMARY = "summary"
JSON_MSG_TYPE_ERROR = "error"


# Set up the signal handler for handling Ctrl + C interruptions.
sigint_handler = SigIntHandler()


@click.group(name="job")
@_handle_error
def cli_job():
    """
    Commands to work with AWS Deadline Cloud Jobs.
    """


@cli_job.command(name="list")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The AWS Deadline Cloud Farm to use.")
@click.option("--queue-id", help="The AWS Deadline Cloud Queue to use.")
@click.option("--page-size", default=5, help="The number of items shown in the page.")
@click.option("--item-offset", default=0, help="The starting offset of the items.")
@_handle_error
def job_list(page_size, item_offset, **args):
    """
    Lists the Jobs in an AWS Deadline Cloud Queue.
    """
    # Get a temporary config object with the standard options handled
    config = _apply_cli_options_to_config(required_options={"farm_id", "queue_id"}, **args)

    farm_id = config_file.get_setting("defaults.farm_id", config=config)
    queue_id = config_file.get_setting("defaults.queue_id", config=config)

    deadline = api.get_boto3_client("deadline", config=config)
    try:
        response = deadline.search_jobs(
            farmId=farm_id,
            queueIds=[queue_id],
            itemOffset=item_offset,
            pageSize=page_size,
            sortExpressions=[{"fieldSort": {"name": "CREATED_AT", "sortOrder": "DESCENDING"}}],
        )
    except ClientError as exc:
        raise DeadlineOperationError(f"Failed to get Jobs from Deadline:\n{exc}") from exc

    total_results = response["totalResults"]

    # Select which fields to print and in which order
    name_field = "displayName"
    if len(response["jobs"]) and "name" in response["jobs"][0]:
        name_field = "name"
    structured_job_list = [
        {
            field: job.get(field, "")
            for field in [
                name_field,
                "jobId",
                "taskRunStatus",
                "startedAt",
                "endedAt",
                "createdBy",
                "createdAt",
            ]
        }
        for job in response["jobs"]
    ]

    click.echo(
        f"Displaying {len(structured_job_list)} of {total_results} Jobs starting at {item_offset}"
    )
    click.echo()
    click.echo(_cli_object_repr(structured_job_list))


@cli_job.command(name="get")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The AWS Deadline Cloud Farm to use.")
@click.option("--queue-id", help="The AWS Deadline Cloud Queue to use.")
@click.option("--job-id", help="The AWS Deadline Cloud Job to get.")
@_handle_error
def job_get(**args):
    """
    Get the details of an AWS Deadline Cloud Job.
    """
    # Get a temporary config object with the standard options handled
    config = _apply_cli_options_to_config(
        required_options={"farm_id", "queue_id", "job_id"}, **args
    )

    farm_id = config_file.get_setting("defaults.farm_id", config=config)
    queue_id = config_file.get_setting("defaults.queue_id", config=config)
    job_id = config_file.get_setting("defaults.job_id", config=config)

    deadline = api.get_boto3_client("deadline", config=config)
    response = deadline.get_job(farmId=farm_id, queueId=queue_id, jobId=job_id)
    response.pop("ResponseMetadata", None)

    click.echo(_cli_object_repr(response))


@cli_job.command(name="cancel")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The AWS Deadline Cloud Farm to use.")
@click.option("--queue-id", help="The AWS Deadline Cloud Queue to use.")
@click.option("--job-id", help="The AWS Deadline Cloud Job to cancel.")
@click.option(
    "--mark-as",
    type=click.Choice(["CANCELED", "FAILED", "SUCCEEDED"], case_sensitive=False),
    default="CANCELED",
    help="The run status to mark the job as.",
)
@click.option(
    "--yes",
    is_flag=True,
    help="Skip any confirmation prompts",
)
@_handle_error
def job_cancel(mark_as: str, yes: bool, **args):
    """
    Cancel an AWS Deadline Cloud Job from running.
    """
    # Get a temporary config object with the standard options handled
    config = _apply_cli_options_to_config(
        required_options={"farm_id", "queue_id", "job_id"}, **args
    )

    farm_id = config_file.get_setting("defaults.farm_id", config=config)
    queue_id = config_file.get_setting("defaults.queue_id", config=config)
    job_id = config_file.get_setting("defaults.job_id", config=config)

    mark_as = mark_as.upper()

    deadline = api.get_boto3_client("deadline", config=config)

    # Print a summary of the job to cancel
    job = deadline.get_job(farmId=farm_id, queueId=queue_id, jobId=job_id)
    # Remove the zero-count status counts
    job["taskRunStatusCounts"] = {
        name: count for name, count in job["taskRunStatusCounts"].items() if count != 0
    }
    # Filter the fields to a summary
    filtered_job = {
        field: job.get(field, "")
        for field in [
            "name",
            "jobId",
            "taskRunStatus",
            "taskRunStatusCounts",
            "startedAt",
            "endedAt",
            "createdBy",
            "createdAt",
        ]
    }
    click.echo(_cli_object_repr(filtered_job))

    # Ask for confirmation about canceling this job.
    if not (
        yes or config_file.str2bool(config_file.get_setting("settings.auto_accept", config=config))
    ):
        if mark_as == "CANCELED":
            cancel_message = "Are you sure you want to cancel this job?"
        else:
            cancel_message = (
                f"Are you sure you want to cancel this job and mark its taskRunStatus as {mark_as}?"
            )
        # We explicitly require a yes/no response, as this is an operation that will interrupt the work in progress
        # on their job.
        if not click.confirm(
            cancel_message,
            default=None,
        ):
            click.echo("Job not canceled.")
            sys.exit(1)

    if mark_as == "CANCELED":
        click.echo("Canceling job...")
    else:
        click.echo(f"Canceling job and marking as {mark_as}...")
    deadline.update_job(farmId=farm_id, queueId=queue_id, jobId=job_id, targetTaskRunStatus=mark_as)


def _download_job_output(
    config: Optional[ConfigParser],
    farm_id: str,
    queue_id: str,
    job_id: str,
    step_id: Optional[str],
    task_id: Optional[str],
    is_json_format: bool = False,
):
    """
    Starts the download of job output and handles the progress reporting callback.
    """
    deadline = api.get_boto3_client("deadline", config=config)

    auto_accept = config_file.str2bool(
        config_file.get_setting("settings.auto_accept", config=config)
    )
    conflict_resolution = config_file.get_setting("settings.conflict_resolution", config=config)

    job = deadline.get_job(farmId=farm_id, queueId=queue_id, jobId=job_id)
    step = {}
    task = {}
    if step_id:
        step = deadline.get_step(farmId=farm_id, queueId=queue_id, jobId=job_id, stepId=step_id)
    if task_id:
        task = deadline.get_task(
            farmId=farm_id, queueId=queue_id, jobId=job_id, stepId=step_id, taskId=task_id
        )

    click.echo(
        _get_start_message(job["name"], step.get("name"), task.get("parameters"), is_json_format)
    )

    queue = deadline.get_queue(farmId=farm_id, queueId=queue_id)

    queue_role_session = api.get_queue_user_boto3_session(
        deadline=deadline,
        config=config,
        farm_id=farm_id,
        queue_id=queue_id,
        queue_display_name=queue["displayName"],
    )

    # Get a dictionary mapping rootPath to rootPathFormat (OS) from job's manifests
    root_path_format_mapping: dict[str, str] = {}
    job_attachments = job.get("attachments", None)
    if job_attachments:
        job_attachments_manifests = job_attachments["manifests"]
        for manifest in job_attachments_manifests:
            root_path_format_mapping[manifest["rootPath"]] = manifest["rootPathFormat"]

    job_output_downloader = OutputDownloader(
        s3_settings=JobAttachmentS3Settings(**queue["jobAttachmentSettings"]),
        farm_id=farm_id,
        queue_id=queue_id,
        job_id=job_id,
        step_id=step_id,
        task_id=task_id,
        session=queue_role_session,
    )

    output_paths_by_root = job_output_downloader.get_output_paths_by_root()

    # If no output paths were found, log a message and exit.
    if output_paths_by_root == {}:
        click.echo(_get_no_output_message(is_json_format))
        return

    # Check if the asset roots came from different OS. If so, prompt users to
    # select alternative root paths to download to, (regardless of the auto-accept.)
    asset_roots = list(output_paths_by_root.keys())
    for asset_root in asset_roots:
        root_path_format = root_path_format_mapping.get(asset_root, "")
        if root_path_format == "":
            # There must be a corresponding root path format for each root path, by design.
            raise DeadlineOperationError(f"No root path format found for {asset_root}.")
        if PathFormat.get_host_path_format_string() != root_path_format:
            click.echo(_get_mismatch_os_root_warning(asset_root, root_path_format, is_json_format))

            if not is_json_format:
                new_root = click.prompt(
                    "> Please enter a new root path",
                    type=click.Path(exists=False),
                )
            else:
                json_string = click.prompt("", prompt_suffix="", type=str)
                new_root = _get_value_from_json_line(
                    json_string, JSON_MSG_TYPE_PATHCONFIRM, expected_size=1
                )[0]
                _assert_valid_path(new_root)

            job_output_downloader.set_root_path(asset_root, new_root)

    output_paths_by_root = job_output_downloader.get_output_paths_by_root()

    # Prompt users to confirm local root paths where they will download outputs to,
    # and allow users to select different location to download files to if they want.
    # (If auto-accept is enabled, automatically download to the default root paths.)
    if not auto_accept:
        if not is_json_format:
            user_choice = ""
            while user_choice != ("y" or "n"):
                click.echo(
                    _get_summary_of_files_to_download_message(output_paths_by_root, is_json_format)
                )
                asset_roots = list(output_paths_by_root.keys())
                click.echo(_get_roots_list_message(asset_roots, is_json_format))
                user_choice = click.prompt(
                    "> Please enter the index of root directory to edit, y to proceed without changes, or n to cancel the download",
                    type=click.Choice(
                        [*[str(num) for num in list(range(0, len(asset_roots)))], "y", "n"]
                    ),
                    default="y",
                )
                if user_choice == "n":
                    click.echo("Output download canceled.")
                    return
                elif user_choice != "y":
                    # User selected an index to modify the root directory.
                    index_to_change = int(user_choice)
                    new_root = click.prompt(
                        "> Please enter the new root directory path, or press Enter to keep it unchanged",
                        type=click.Path(exists=False),
                        default=asset_roots[index_to_change],
                    )
                    job_output_downloader.set_root_path(
                        asset_roots[index_to_change], str(Path(new_root))
                    )
                    output_paths_by_root = job_output_downloader.get_output_paths_by_root()
        else:
            click.echo(
                _get_summary_of_files_to_download_message(output_paths_by_root, is_json_format)
            )
            asset_roots = list(output_paths_by_root.keys())
            click.echo(_get_roots_list_message(asset_roots, is_json_format))
            json_string = click.prompt("", prompt_suffix="", type=str)
            confirmed_asset_roots = _get_value_from_json_line(
                json_string, JSON_MSG_TYPE_PATHCONFIRM, expected_size=len(asset_roots)
            )
            for index, confirmed_root in enumerate(confirmed_asset_roots):
                _assert_valid_path(confirmed_root)
                job_output_downloader.set_root_path(asset_roots[index], str(Path(confirmed_root)))
            output_paths_by_root = job_output_downloader.get_output_paths_by_root()

    # If the conflict resolution option was not specified, auto-accept is false, and
    # if there are any conflicting files in local, prompt users to select a resolution method.
    # (skip, overwrite, or make a copy.)
    if conflict_resolution != FileConflictResolution.NOT_SELECTED.name:
        file_conflict_resolution = FileConflictResolution[conflict_resolution]
    elif auto_accept:
        file_conflict_resolution = FileConflictResolution.CREATE_COPY
    else:
        file_conflict_resolution = FileConflictResolution.CREATE_COPY
        conflicting_filenames = _get_conflicting_filenames(output_paths_by_root)
        if conflicting_filenames:
            click.echo(_get_conflict_resolution_selection_message(conflicting_filenames))
            user_choice = click.prompt(
                "> Please enter your choice (1, 2, 3, or n to cancel the download)",
                type=click.Choice(["1", "2", "3", "n"]),
                default="3",
            )
            if user_choice == "n":
                click.echo("Output download canceled.")
                return
            else:
                resolution_choice_int = int(user_choice)
                file_conflict_resolution = FileConflictResolution(resolution_choice_int)

    # TODO: remove logging level setting when the max number connections for boto3 client
    # in Job Attachments library can be increased (currently using default number, 10, which
    # makes it keep logging urllib3 warning messages when downloading large files)
    with _modified_logging_level(logging.getLogger("urllib3"), logging.ERROR):
        if not is_json_format:
            # Note: click doesn't export the return type of progressbar(), so we suppress mypy warnings for
            # not annotating the type of download_progress.
            click.echo()
            with click.progressbar(length=100, label="Downloading Outputs") as download_progress:  # type: ignore[var-annotated]

                def _update_download_progress(download_metadata: ProgressReportMetadata) -> bool:
                    new_progress = int(download_metadata.progress) - download_progress.pos
                    if new_progress > 0:
                        download_progress.update(new_progress)
                    return sigint_handler.continue_operation

                download_summary: DownloadSummaryStatistics = (
                    job_output_downloader.download_job_output(
                        file_conflict_resolution=file_conflict_resolution,
                        on_downloading_files=_update_download_progress,
                    )
                )
        else:

            def _update_download_progress(download_metadata: ProgressReportMetadata) -> bool:
                click.echo(
                    _get_json_line(JSON_MSG_TYPE_PROGRESS, str(int(download_metadata.progress)))
                )
                # TODO: enable download cancellation for JSON format
                return True

            download_summary = job_output_downloader.download_job_output(
                file_conflict_resolution=file_conflict_resolution,
                on_downloading_files=_update_download_progress,
            )

    click.echo(_get_download_summary_message(download_summary, is_json_format))
    click.echo()


def _get_start_message(
    job_name: str, step_name: Optional[str], task_parameters: Optional[dict], is_json_format: bool
) -> str:
    if is_json_format:
        return _get_json_line(JSON_MSG_TYPE_TITLE, job_name)
    else:
        if step_name is None:
            return f"Downloading output from Job {job_name!r}"
        elif task_parameters is None:
            return f"Downloading output from Job {job_name!r} Step {step_name!r}"
        else:
            task_parameters_summary = "{}"
            if task_parameters:
                task_parameters_summary = (
                    "{"
                    + ",".join(
                        f"{key}={list(value.values())[0]}" for key, value in task_parameters.items()
                    )
                    + "}"
                )
            return f"Downloading output from Job {job_name!r} Step {step_name!r} Task {task_parameters_summary}"


def _get_no_output_message(is_json_format: bool) -> str:
    msg = (
        "There are no output files available for download at this moment. Please verify that"
        " the Job/Step/Task you are trying to download output from has completed successfully."
    )
    if is_json_format:
        return _get_json_line(JSON_MSG_TYPE_SUMMARY, msg)
    else:
        return msg


def _get_mismatch_os_root_warning(root: str, root_path_format: str, is_json_format: bool) -> str:
    if is_json_format:
        return _get_json_line(JSON_MSG_TYPE_PATH, [root])
    else:
        path_format_capitalized_first_letter = root_path_format[0].upper() + root_path_format[1:]
        return (
            "This root path format does not match the operating system you're using. "
            "Where would you like to save the files?\n"
            f"The location was {root}, on {path_format_capitalized_first_letter}."
        )


def _get_summary_of_files_to_download_message(
    output_paths_by_root: dict[str, list[str]], is_json_format: bool
) -> str:
    # Print some information about what we will download
    if is_json_format:
        return _get_json_line(JSON_MSG_TYPE_PRESUMMARY, output_paths_by_root)
    else:
        paths_message_joined = "    " + "\n    ".join(
            f"{os.path.commonpath([os.path.join(directory, p) for p in output_paths])} ({len(output_paths)} file{'s' if len(output_paths) > 1 else ''})"
            for directory, output_paths in output_paths_by_root.items()
        )
        return "\n" "Summary of files to download:\n" f"{paths_message_joined}" "\n"


def _get_roots_list_message(asset_roots: list[str], is_json_format: bool) -> str:
    if is_json_format:
        return _get_json_line(JSON_MSG_TYPE_PATH, asset_roots)
    else:
        asset_roots_str = "\n".join([f"[{index}] {root}" for index, root in enumerate(asset_roots)])
        return (
            f"You are about to download files which may come from multiple root directories. Here are a list of the current root directories:\n"
            f"{asset_roots_str}"
        )


def _get_conflict_resolution_selection_message(conflicting_filenames: list[str]) -> str:
    conflicting_filenames_str = "\n        ".join(conflicting_filenames)
    return (
        f"The following files already exist in your local directory:\n        {conflicting_filenames_str}\n"
        f"You have three options to choose from:\n"
        f"[1] Skip: Do not download these files\n"
        f"[2] Overwrite: Download these files and overwrite existing files\n"
        f"[3] Create a copy: Download the file with a new name, appending '(1)' to the end"
    )


def _get_download_summary_message(
    download_summary: DownloadSummaryStatistics, is_json_format: bool
) -> str:
    if is_json_format:
        return _get_json_line(
            JSON_MSG_TYPE_SUMMARY, f"Downloaded {download_summary.processed_files} files"
        )
    else:
        paths_joined = "\n        ".join(
            f"{directory} ({count} file{'s' if count > 1 else ''})"
            for directory, count in download_summary.file_counts_by_root_directory.items()
        )
        return (
            "Download Summary:\n"
            f"    Downloaded {download_summary.processed_files} files totaling"
            f" {_human_readable_file_size(download_summary.processed_bytes)}.\n"
            f"    Total download time of {round(download_summary.total_time, ndigits=5)} seconds"
            f" at {_human_readable_file_size(int(download_summary.transfer_rate))}/s.\n"
            f"    Download locations (total file counts):\n        {paths_joined}"
        )


def _get_conflicting_filenames(filenames_by_root: dict[str, list[str]]) -> list[str]:
    conflicting_filenames: list[str] = []

    for root, filenames in filenames_by_root.items():
        for filename in filenames:
            abs_path = Path(root).joinpath(filename).resolve()
            if abs_path.is_file():
                conflicting_filenames.append(str(abs_path))

    return conflicting_filenames


def _get_json_line(messageType: str, value: Union[str, list[str], dict[str, Any]]) -> str:
    return json.dumps({"messageType": messageType, "value": value}, ensure_ascii=True)


def _get_value_from_json_line(
    json_line: str, message_type: str, expected_size: Optional[int] = None
) -> Union[str, list[str]]:
    try:
        parsed_json = json.loads(json_line)
        if parsed_json["messageType"] != message_type:
            raise ValueError(
                f"Expected message type '{message_type}' but received '{parsed_json['messageType']}'"
            )
        if expected_size and len(parsed_json["value"]) != expected_size:
            raise ValueError(
                f"Expected {expected_size} item{'' if expected_size == 1 else 's'} in value "
                f"but received {len(parsed_json['value'])}"
            )
        return parsed_json["value"]
    except Exception as e:
        raise ValueError(f"Invalid JSON line '{json_line}': {e}")


def _assert_valid_path(path: str) -> None:
    """
    Validates that the path has the format of the OS currently running.
    """
    path_obj = Path(path)
    if not path_obj.is_absolute():
        raise ValueError(f"Path {path} is not an absolute path.")


@cli_job.command(name="download-output")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The AWS Deadline Cloud Farm to use.")
@click.option("--queue-id", help="The AWS Deadline Cloud Queue to use.")
@click.option("--job-id", help="The AWS Deadline Cloud Job to use.")
@click.option("--step-id", help="The AWS Deadline Cloud Step to use.")
@click.option("--task-id", help="The AWS Deadline Cloud Task to use.")
@click.option(
    "--conflict-resolution",
    type=click.Choice(
        [
            FileConflictResolution.SKIP.name,
            FileConflictResolution.OVERWRITE.name,
            FileConflictResolution.CREATE_COPY.name,
        ],
        case_sensitive=False,
    ),
    help="The resolution method to use when a file already exists."
    "Please choose one from the following options. If it is not provided, it defaults to CREATE_COPY:\n"
    "[1] SKIP: Do not download these files\n"
    "[2] OVERWRITE: Download these files and overwrite existing files\n"
    "[3] CREATE_COPY: Download the file with a new name, appending '(1)' to the end",
)
@click.option(
    "--yes",
    is_flag=True,
    help="Skip any confirmation prompts",
)
@click.option(
    "--output",
    type=click.Choice(
        ["verbose", "json"],
        case_sensitive=False,
    ),
    help="Specifies the output format of the messages printed to stdout.\n"
    "VERBOSE: Displays messages in a human-readable text format.\n"
    "JSON: Displays messages in JSON line format, so that the info can be easily "
    "parsed/consumed by custom scripts.",
)
@_handle_error
def job_download_output(step_id, task_id, output, **args):
    """
    Download the output attached to an AWS Deadline Cloud Job.
    """
    if task_id and not step_id:
        raise click.UsageError("Missing option '--step-id' required with '--task-id'")
    # Get a temporary config object with the standard options handled
    config = _apply_cli_options_to_config(
        required_options={"farm_id", "queue_id", "job_id"}, **args
    )

    farm_id = config_file.get_setting("defaults.farm_id", config=config)
    queue_id = config_file.get_setting("defaults.queue_id", config=config)
    job_id = config_file.get_setting("defaults.job_id", config=config)
    is_json_format = True if output == "json" else False

    try:
        _download_job_output(config, farm_id, queue_id, job_id, step_id, task_id, is_json_format)
    except Exception as e:
        if is_json_format:
            error_one_liner = str(e).replace("\n", ". ")
            click.echo(_get_json_line(JSON_MSG_TYPE_ERROR, error_one_liner))
            sys.exit(1)
        else:
            raise DeadlineOperationError(f"Failed to download output:\n{e}") from e


@cli_job.command(name="trace-schedule")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The AWS Deadline Cloud Farm to use.")
@click.option("--queue-id", help="The AWS Deadline Cloud Queue to use.")
@click.option("--job-id", help="The AWS Deadline Cloud Job to trace.")
@click.option("-v", "--verbose", is_flag=True, help="Output verbose trace details.")
@click.option(
    "--trace-format",
    type=click.Choice(
        ["chrome"],
        case_sensitive=False,
    ),
    help="The tracing format to write.",
)
@click.option("--trace-file", help="The tracing file to write.")
@_handle_error
def job_trace_schedule(verbose, trace_format, trace_file, **args):
    """
    EXPERIMENTAL - Print statistics about how a job, and optionally
    write a trace file.

    To visualize the trace output file when providing the options
    "--trace-format chrome --trace-file <output>.json", use
    the https://ui.perfetto.dev Tracing UI and choose "Open trace file".
    """
    # Get a temporary config object with the standard options handled
    config = _apply_cli_options_to_config(
        required_options={"farm_id", "queue_id", "job_id"}, **args
    )

    farm_id = config_file.get_setting("defaults.farm_id", config=config)
    queue_id = config_file.get_setting("defaults.queue_id", config=config)
    job_id = config_file.get_setting("defaults.job_id", config=config)

    if trace_file and not trace_format:
        raise DeadlineOperationError("Error: Must provide --trace-format with --trace-file.")

    deadline = api.get_boto3_client("deadline", config=config)

    click.echo("Getting the job...")
    job = deadline.get_job(farmId=farm_id, queueId=queue_id, jobId=job_id)
    job.pop("ResponseMetadata", None)

    click.echo("Getting all the sessions for the job...")
    response = deadline.list_sessions(farmId=farm_id, queueId=queue_id, jobId=job_id)
    while "nextToken" in response:
        old_list = response["sessions"]
        response = deadline.list_sessions(
            farmId=farm_id, queueId=queue_id, jobId=job_id, nextToken=response["nextToken"]
        )
        response["sessions"] = old_list + response["sessions"]
    response.pop("ResponseMetadata", None)

    sessions = sorted(response["sessions"], key=lambda session: session["startedAt"])

    click.echo("Getting all the session actions for the job...")
    for session in sessions:
        response = deadline.list_session_actions(
            farmId=farm_id, queueId=queue_id, jobId=job_id, sessionId=session["sessionId"]
        )
        while "nextToken" in response:
            old_list = response["sessionActions"]
            response = deadline.list_session_actions(
                farmId=farm_id,
                queueId=queue_id,
                jobId=job_id,
                sessionId=session["sessionId"],
                nextToken=response["nextToken"],
            )
            response["sessionActions"] = old_list + response["sessionActions"]
        response.pop("ResponseMetadata", None)

        session["actions"] = response["sessionActions"]

    # Cache steps and tasks by their id, to only get each once
    steps: dict[str, Any] = {}
    tasks: dict[str, Any] = {}

    with click.progressbar(  # type: ignore[var-annotated]
        length=len(sessions), label="Getting all the steps and tasks for the job..."
    ) as progressbar:
        for index, session in enumerate(sessions):
            session["index"] = index
            for action in session["actions"]:
                step_id = action["definition"].get("taskRun", {}).get("stepId")
                task_id = action["definition"].get("taskRun", {}).get("taskId")
                if step_id and task_id:
                    if "step" not in session:
                        if step_id in steps:
                            step = steps[step_id]
                        else:
                            step = deadline.get_step(
                                farmId=farm_id, queueId=queue_id, jobId=job_id, stepId=step_id
                            )
                            step.pop("ResponseMetadata", None)
                            steps[step_id] = step
                        session["step"] = step
                    elif session["step"]["stepId"] != step_id:
                        # The session itself doesn't have a step id, but for now the scheduler always creates new
                        # sessions for new steps.
                        raise DeadlineOperationError(
                            f"Session {session['sessionId']} ran more than one step! When this code was"
                            " written that wasn't possible."
                        )

                    if task_id in tasks:
                        task = tasks[task_id]
                    else:
                        task = deadline.get_task(
                            farmId=farm_id,
                            queueId=queue_id,
                            jobId=job_id,
                            stepId=step_id,
                            taskId=task_id,
                        )
                        task.pop("ResponseMetadata", None)
                        tasks[task_id] = task
                    action["task"] = task
            progressbar.update(1)

    # Collect the worker IDs that ran the sessions, and give them indexes to act as PIDs in the tracing file
    worker_ids = {session["workerId"] for session in sessions}
    workers = {worker_id: index for index, worker_id in enumerate(worker_ids)}

    click.echo("Processing the trace data...")
    trace_events = []

    started_at = job["startedAt"]

    def time_int(timestamp: datetime.datetime):
        return int((timestamp - started_at) / datetime.timedelta(microseconds=1))

    def duration_of(resource):
        try:
            return time_int(resource["endedAt"]) - time_int(resource["startedAt"])
        except KeyError:
            return 0

    accumulators = {
        "sessionCount": 0,
        "sessionActionCount": 0,
        "taskRunCount": 0,
        "envActionCount": 0,
        "syncJobAttachmentsCount": 0,
        "sessionDuration": 0,
        "sessionActionDuration": 0,
        "taskRunDuration": 0,
        "envActionDuration": 0,
        "syncJobAttachmentsDuration": 0,
    }

    for session in sessions:
        accumulators["sessionCount"] += 1
        accumulators["sessionDuration"] += duration_of(session)

        pid = workers[session["workerId"]]
        session_event_name = f"{session['step']['name']} - {session['index']}"
        trace_events.append(
            {
                "name": session_event_name,
                "cat": "SESSION",
                "ph": "B",  # Begin Event
                "ts": time_int(session["startedAt"]),
                "pid": pid,
                "tid": 0,
                "args": {
                    "sessionId": session["sessionId"],
                    "workerId": session["workerId"],
                    "fleetId": session["fleetId"],
                    "lifecycleStatus": session["lifecycleStatus"],
                },
            }
        )

        for action in session["actions"]:
            accumulators["sessionActionCount"] += 1
            accumulators["sessionActionDuration"] += duration_of(action)

            name = action["sessionActionId"]
            action_type = list(action["definition"].keys())[0]
            if action_type == "taskRun":
                accumulators["taskRunCount"] += 1
                accumulators["taskRunDuration"] += duration_of(action)

                task = action["task"]
                parameters = task.get("parameters", {})
                name = ",".join(
                    f"{param}={list(parameters[param].values())[0]}" for param in parameters
                )
                if not name:
                    name = "<No Task Params>"
            elif action_type in ("envEnter", "envExit"):
                accumulators["envActionCount"] += 1
                accumulators["envActionDuration"] += duration_of(action)

                name = action["definition"][action_type]["environmentId"].split(":")[-1]
            elif action_type == "syncInputJobAttachments":
                accumulators["syncJobAttachmentsCount"] += 1
                accumulators["syncJobAttachmentsDuration"] += duration_of(action)

                if "stepId" in action["definition"][action_type]:
                    name = "Sync Job Attchmnt (Dependencies)"
                else:
                    name = "Sync Job Attchmnt (Submitted)"
            if "startedAt" in action:
                trace_events.append(
                    {
                        "name": name,
                        "cat": action_type,
                        "ph": "X",  # Complete Event
                        "ts": time_int(action["startedAt"]),
                        "dur": duration_of(action),
                        "pid": pid,
                        "tid": 0,
                        "args": {
                            "sessionActionId": action["sessionActionId"],
                            "status": action["status"],
                            "stepName": session["step"]["name"],
                        },
                    }
                )
        trace_events.append(
            {
                "name": session_event_name,
                "cat": "SESSION",
                "ph": "E",  # End Event
                "ts": time_int(session["endedAt"]),
                "pid": pid,
                "tid": 0,
            }
        )

    if verbose:
        click.echo(" ==== TRACE DATA ====")
        click.echo(_cli_object_repr(job))
        click.echo("")
        click.echo(_cli_object_repr(sessions))

    click.echo("")
    click.echo(" ==== SUMMARY ====")
    click.echo("")
    click.echo(f"Session Count: {accumulators['sessionCount']}")
    session_total_duration = accumulators["sessionDuration"]
    click.echo(f"Session Total Duration: {datetime.timedelta(microseconds=session_total_duration)}")
    click.echo(f"Session Action Count: {accumulators['sessionActionCount']}")
    click.echo(
        f"Session Action Total Duration: {datetime.timedelta(microseconds=accumulators['sessionActionDuration'])}"
    )
    click.echo(f"Task Run Count: {accumulators['taskRunCount']}")
    task_run_total_duration = accumulators["taskRunDuration"]
    click.echo(
        f"Task Run Total Duration: {datetime.timedelta(microseconds=task_run_total_duration)} ({100 * task_run_total_duration / session_total_duration:.1f}%)"
    )
    click.echo(
        f"Non-Task Run Count: {accumulators['sessionActionCount'] - accumulators['taskRunCount']}"
    )
    non_task_run_total_duration = (
        accumulators["sessionActionDuration"] - accumulators["taskRunDuration"]
    )
    click.echo(
        f"Non-Task Run Total Duration: {datetime.timedelta(microseconds=non_task_run_total_duration)} ({100 * non_task_run_total_duration / session_total_duration:.1f}%)"
    )
    click.echo(f"Sync Job Attachments Count: {accumulators['syncJobAttachmentsCount']}")
    sync_job_attachments_total_duration = accumulators["syncJobAttachmentsDuration"]
    click.echo(
        f"Sync Job Attachments Total Duration: {datetime.timedelta(microseconds=sync_job_attachments_total_duration)} ({100 * sync_job_attachments_total_duration / session_total_duration:.1f}%)"
    )
    click.echo(f"Env Action Count: {accumulators['envActionCount']}")
    env_action_total_duration = accumulators["envActionDuration"]
    click.echo(
        f"Env Action Total Duration: {datetime.timedelta(microseconds=env_action_total_duration)} ({100 * env_action_total_duration / session_total_duration:.1f}%)"
    )
    click.echo("")
    within_session_overhead_duration = (
        accumulators["sessionDuration"] - accumulators["sessionActionDuration"]
    )
    click.echo(
        f"Within-session Overhead Duration: {datetime.timedelta(microseconds=within_session_overhead_duration)} ({100 * within_session_overhead_duration / session_total_duration:.1f}%)"
    )
    click.echo(
        f"Within-session Overhead Duration Per Action: {datetime.timedelta(microseconds=(accumulators['sessionDuration'] - accumulators['sessionActionDuration']) / accumulators['sessionActionCount'])}"
    )

    tracing_data: dict[str, Any] = {
        "traceEvents": trace_events,
        # "displayTimeUnits": "s",
        "otherData": {
            "farmId": farm_id,
            "queueId": queue_id,
            "jobId": job_id,
            "jobName": job["name"],
            "startedAt": job["startedAt"].isoformat(sep="T"),
        },
    }
    if "endedAt" in job:
        tracing_data["otherData"]["endedAt"] = job["endedAt"].isoformat(sep="T")

    tracing_data["otherData"].update(accumulators)

    if trace_file:
        with open(trace_file, "w", encoding="utf8") as f:
            json.dump(tracing_data, f, indent=1)
