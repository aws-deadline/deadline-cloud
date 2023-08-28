# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
All the `deadline job` commands.
"""

from __future__ import annotations
import json
import logging
from configparser import ConfigParser
from pathlib import Path
import sys
from typing import Optional, Union

import click
from botocore.exceptions import ClientError  # type: ignore[import]

from deadline.client.api._session import _modified_logging_level
from deadline.job_attachments.download import OutputDownloader
from deadline.job_attachments.models import JobAttachmentS3Settings
from deadline.job_attachments.progress_tracker import (
    DownloadSummaryStatistics,
    ProgressReportMetadata,
)
from deadline.job_attachments.utils import FileConflictResolution, human_readable_file_size

from deadline.job_attachments.utils import OperatingSystemFamily, get_deadline_formatted_os

from ... import api
from ...config import config_file
from ...exceptions import DeadlineOperationError
from .._common import apply_cli_options_to_config, cli_object_repr, handle_error

JSON_MSG_TYPE_TITLE = "title"
JSON_MSG_TYPE_PATH = "path"
JSON_MSG_TYPE_PATHCONFIRM = "pathconfirm"
JSON_MSG_TYPE_PROGRESS = "progress"
JSON_MSG_TYPE_SUMMARY = "summary"
JSON_MSG_TYPE_ERROR = "error"


@click.group(name="job")
@handle_error
def cli_job():
    """
    Commands to work with Amazon Deadline Cloud Jobs.
    """


@cli_job.command(name="list")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The Amazon Deadline Cloud Farm to use.")
@click.option("--queue-id", help="The Amazon Deadline Cloud Queue to use.")
@click.option("--page-size", default=5, help="The number of items shown in the page.")
@click.option("--item-offset", default=0, help="The starting offset of the items.")
@handle_error
def job_list(page_size, item_offset, **args):
    """
    Lists the Jobs in an Amazon Deadline Cloud Queue.
    """
    # Get a temporary config object with the standard options handled
    config = apply_cli_options_to_config(required_options={"farm_id", "queue_id"}, **args)

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
                "lifecycleStatus",
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
    click.echo(cli_object_repr(structured_job_list))


@cli_job.command(name="get")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The Amazon Deadline Cloud Farm to use.")
@click.option("--queue-id", help="The Amazon Deadline Cloud Queue to use.")
@click.option("--job-id", help="The Amazon Deadline Cloud Job to get.")
@handle_error
def job_get(**args):
    """
    Get the details of an Amazon Deadline Cloud Job.
    """
    # Get a temporary config object with the standard options handled
    config = apply_cli_options_to_config(required_options={"farm_id", "queue_id", "job_id"}, **args)

    farm_id = config_file.get_setting("defaults.farm_id", config=config)
    queue_id = config_file.get_setting("defaults.queue_id", config=config)
    job_id = config_file.get_setting("defaults.job_id", config=config)

    deadline = api.get_boto3_client("deadline", config=config)
    response = deadline.get_job(farmId=farm_id, queueId=queue_id, jobId=job_id)
    response.pop("ResponseMetadata", None)

    click.echo(cli_object_repr(response))


def _download_job_output(
    config: Optional[ConfigParser],
    farm_id: str,
    queue_id: str,
    job_id: str,
    step_id: Optional[str],
    task_id: Optional[str],
    conflict_resolution: Optional[str] = None,
    is_json_format: bool = False,
):
    """
    Starts the download of job output and handles the progress reporting callback.
    """
    deadline = api.get_boto3_client("deadline", config=config)

    auto_accept = config_file.str2bool(
        config_file.get_setting("settings.auto_accept", config=config)
    )

    job = deadline.get_job(farmId=farm_id, queueId=queue_id, jobId=job_id)

    click.echo(_get_start_message(job["name"], step_id, task_id, is_json_format))

    queue = deadline.get_queue(farmId=farm_id, queueId=queue_id)

    queue_role_session = api.get_queue_boto3_session(
        deadline=deadline,
        config=config,
        farm_id=farm_id,
        queue_id=queue_id,
        queue_display_name=queue["displayName"],
    )

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

    # Check if the asset roots came from different OS. If so, prompt users to
    # select alternative root paths to download to, (regardless of the auto-accept.)
    asset_roots = list(output_paths_by_root.keys())
    for asset_root in asset_roots:
        if _is_current_os_windows() != _is_path_in_windows_format(asset_root):
            click.echo(_get_mismatch_os_root_warning(asset_root, is_json_format))

            if not is_json_format:
                new_root = click.prompt(
                    "> Please enter a new root path",
                    type=click.Path(exists=True, file_okay=False, dir_okay=True),
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
        asset_roots = list(output_paths_by_root.keys())
        asset_roots_resolved = [f"{str(Path(root).resolve())}" for root in asset_roots]
        click.echo(_get_roots_confirmation_message(asset_roots_resolved, is_json_format))

        if not is_json_format:
            user_choice = ""
            while user_choice != ("y" or "n"):
                user_choice = click.prompt(
                    "> Please enter a number of root path to edit, y to proceed, or n to cancel the download:",
                    type=click.Choice(
                        [*[str(num) for num in list(range(0, len(asset_roots)))], "y", "n"]
                    ),
                    default="y",
                )
                if user_choice == "n":
                    click.echo("Output download canceled.")
                    return
                elif user_choice != "y":  # The user entered a number.
                    index_to_change = int(user_choice)
                    new_root = click.prompt(
                        "> Please enter a new path for the root directory",
                        type=click.Path(exists=True, file_okay=False, dir_okay=True),
                        default=asset_roots_resolved[index_to_change],
                    )
                    job_output_downloader.set_root_path(
                        asset_roots[index_to_change], str(Path(new_root).resolve())
                    )
                    output_paths_by_root = job_output_downloader.get_output_paths_by_root()

                asset_roots = list(output_paths_by_root.keys())
                asset_roots_resolved = [f"{str(Path(root).resolve())}" for root in asset_roots]
                click.echo(_get_roots_confirmation_message(asset_roots_resolved, is_json_format))
        else:
            json_string = click.prompt("", prompt_suffix="", type=str)
            confirmed_asset_roots = _get_value_from_json_line(
                json_string, JSON_MSG_TYPE_PATHCONFIRM, expected_size=len(asset_roots)
            )
            for index, confirmed_root in enumerate(confirmed_asset_roots):
                _assert_valid_path(confirmed_root)
                job_output_downloader.set_root_path(
                    asset_roots[index], str(Path(confirmed_root).resolve())
                )
            output_paths_by_root = job_output_downloader.get_output_paths_by_root()

    # If the conflict resolution option was not provided as a command option, auto-accept is false,
    # and if there are any conflicting files in local, prompt users to select a resolution method.
    # (skip, overwrite, or make a copy.)
    if conflict_resolution:
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
                file_conflict_resolution = FileConflictResolution.from_index(resolution_choice_int)

    # TODO: remove logging level setting when the max number connections for boto3 client
    # in Job Attachments library can be increased (currently using default number, 10, which
    # makes it keep logging urllib3 warning messages when downloading large files)
    with _modified_logging_level(logging.getLogger("urllib3"), logging.ERROR):
        if not is_json_format:
            with click.progressbar(length=100, label="Downloading Outputs") as download_progress:

                def _update_download_progress(download_metadata: ProgressReportMetadata) -> bool:
                    new_progress = int(download_metadata.progress) - download_progress.pos
                    if new_progress > 0:
                        download_progress.update(new_progress)
                    return True

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
                return True

            download_summary = job_output_downloader.download_job_output(
                file_conflict_resolution=file_conflict_resolution,
                on_downloading_files=_update_download_progress,
            )

    click.echo(_get_download_summary_message(download_summary, is_json_format))


def _get_start_message(
    job_name: str, step_id: Optional[str], task_id: Optional[str], is_json_format: bool
) -> str:
    if is_json_format:
        return _get_json_line(JSON_MSG_TYPE_TITLE, job_name)
    else:
        if step_id is None and task_id is None:
            return f"Downloading output from Job {job_name!r}"
        elif task_id is None:
            return f"Downloading output from Job {job_name!r} Step {step_id}"
        else:
            return f"Downloading output from Job {job_name!r} Step {step_id} Task {task_id}"


def _get_mismatch_os_root_warning(root: str, is_json_format: bool) -> str:
    if is_json_format:
        return _get_json_line(JSON_MSG_TYPE_PATH, [root])
    else:
        return (
            "This root path format does not match the operating system you're using. "
            "Where would you like to save the files?\n"
            f"The location was {root}, on {'Windows' if _is_path_in_windows_format(root) else 'POSIX OS'}."
        )


def _get_roots_confirmation_message(asset_roots: list[str], is_json_format: bool) -> str:
    if is_json_format:
        return _get_json_line(JSON_MSG_TYPE_PATH, asset_roots)
    else:
        asset_roots_str = "\n".join([f"[{index}] {root}" for index, root in enumerate(asset_roots)])
        return f"Outputs will be downloaded to the following root paths:\n{asset_roots_str}"


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
            f" {human_readable_file_size(download_summary.processed_bytes)}.\n"
            f"    Total download time of {round(download_summary.total_time, ndigits=5)} seconds"
            f" at {human_readable_file_size(int(download_summary.transfer_rate))}/s.\n"
            f"    Download locations (file counts):\n        {paths_joined}"
        )


def _get_conflicting_filenames(filenames_by_root: dict[str, list[str]]) -> list[str]:
    conflicting_filenames: list[str] = []

    for root, filenames in filenames_by_root.items():
        for filename in filenames:
            abs_path = Path(root).joinpath(filename).resolve()
            if abs_path.is_file():
                conflicting_filenames.append(str(abs_path))

    return conflicting_filenames


def _get_json_line(messageType: str, value: Union[str, list[str]]) -> str:
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
    Validates that the path exists and has the format of the OS currently running.
    """
    if not Path(path).is_dir():
        raise ValueError(f"Path {path} does not exist.")
    if _is_current_os_windows() != _is_path_in_windows_format(path):
        raise ValueError(f"Path {path} is not in the format of the operating system you're using.")


def _is_current_os_windows() -> bool:
    """
    Checks whether the current OS is Windows.
    """
    return get_deadline_formatted_os() == OperatingSystemFamily.WINDOWS.value


def _is_path_in_windows_format(path_str: str) -> bool:
    """
    Checks the format of a path and determines whether it's in POSIX or Windows format.
    Returns True if the path is in Windows format, False if it's in POSIX format.

    Note:
        This function assumes that path_str is an absolute path.
        A path is considered to be in POSIX format if it starts with "/".
        For Windows format, it starts with a drive letter followed by ":" (e.g., C:).
    """
    if path_str.startswith("/"):
        return False
    elif path_str[0:1].isalpha() and path_str[1:2] == ":":
        return True
    else:
        raise ValueError(f"Path {path_str} is not an absolute path.")


@cli_job.command(name="download-output")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The Amazon Deadline Cloud Farm to use.")
@click.option("--queue-id", help="The Amazon Deadline Cloud Queue to use.")
@click.option("--job-id", help="The Amazon Deadline Cloud Job to use.")
@click.option("--step-id", help="The Amazon Deadline Cloud Step to use.")
@click.option("--task-id", help="The Amazon Deadline Cloud Task to use.")
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
@handle_error
def job_download_output(step_id, task_id, conflict_resolution, output, **args):
    """
    Download the output attached to an Amazon Deadline Cloud Job.
    """
    if task_id and not step_id:
        raise click.UsageError("Missing option '--step-id' required with '--task-id'")
    # Get a temporary config object with the standard options handled
    config = apply_cli_options_to_config(required_options={"farm_id", "queue_id", "job_id"}, **args)

    farm_id = config_file.get_setting("defaults.farm_id", config=config)
    queue_id = config_file.get_setting("defaults.queue_id", config=config)
    job_id = config_file.get_setting("defaults.job_id", config=config)
    is_json_format = True if output == "json" else False

    try:
        _download_job_output(
            config, farm_id, queue_id, job_id, step_id, task_id, conflict_resolution, is_json_format
        )
    except Exception as e:
        if is_json_format:
            error_one_liner = str(e).replace("\n", ". ")
            click.echo(_get_json_line("error", error_one_liner))
            sys.exit(1)
        else:
            raise DeadlineOperationError(f"Failed to download output:\n{e}") from e
