# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
All the `deadline queue` commands.
"""

import click
import json
import time
import os
import sys
from configparser import ConfigParser
from typing import Optional
import boto3
from botocore.exceptions import ClientError  # type: ignore[import]
from datetime import datetime, timedelta, timezone

from ... import api
from ...api._session import get_session_client
from ...config import config_file
from ...exceptions import DeadlineOperationError
from .._common import _apply_cli_options_to_config, _cli_object_repr, _handle_error
from ....job_attachments.models import (
    FileConflictResolution,
)
from .click_logger import ClickLogger
from .._incremental_download import _incremental_output_download
from .._pid_file_lock import PidFileLock
from ....job_attachments._incremental_downloads.incremental_download_state import (
    IncrementalDownloadState,
)

DOWNLOAD_CHECKPOINT_FILE_NAME = "download_checkpoint.json"


@click.group(name="queue")
@_handle_error
def cli_queue():
    """
    Commands for queues.
    """


@cli_queue.command(name="list")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The farm to use.")
@_handle_error
def queue_list(**args):
    """
    Lists the available queues.
    """
    # Get a temporary config object with the standard options handled
    config = _apply_cli_options_to_config(required_options={"farm_id"}, **args)

    farm_id = config_file.get_setting("defaults.farm_id", config=config)

    try:
        response = api.list_queues(farmId=farm_id, config=config)
    except ClientError as exc:
        raise DeadlineOperationError(f"Failed to get Queues from Deadline:\n{exc}") from exc

    # Select which fields to print and in which order
    structured_queue_list = [
        {field: queue[field] for field in ["queueId", "displayName"]}
        for queue in response["queues"]
    ]

    click.echo(_cli_object_repr(structured_queue_list))


@cli_queue.command(name="export-credentials")
@click.option("--queue-id", help="The queue ID to use.")
@click.option("--farm-id", help="The farm ID to use.")
@click.option(
    "--mode",
    type=click.Choice(["USER", "READ"], case_sensitive=False),
    default="USER",
    help="The type of queue role to assume (default: USER)",
)
@click.option(
    "--output-format",
    type=click.Choice(["credentials_process"], case_sensitive=False),
    default="credentials_process",
    help="Format of the output (default: credentials_process)",
)
@click.option("--profile", help="The AWS profile to use.")
@_handle_error
def queue_export_credentials(mode, output_format, **args):
    """
    Export queue credentials in a format compatible with AWS SDK credentials_process.
    """
    start_time = time.time()
    is_success = True
    error_type = None

    # Get a temporary config object with the standard options handled
    config = _apply_cli_options_to_config(required_options={"farm_id", "queue_id"}, **args)

    queue_id: str = config_file.get_setting("defaults.queue_id", config=config)
    farm_id: str = config_file.get_setting("defaults.farm_id", config=config)

    try:
        # Call the appropriate API based on mode
        if mode.upper() == "USER":
            response = api.assume_queue_role_for_user(
                farmId=farm_id, queueId=queue_id, config=config
            )
        elif mode.upper() == "READ":
            response = api.assume_queue_role_for_read(
                farmId=farm_id, queueId=queue_id, config=config
            )
        else:
            is_success = False
            error_type = "InvalidMode"
            raise DeadlineOperationError(f"Invalid mode: {mode}")

        # Format the response according to the AWS SDK credentials_process format
        if output_format.lower() == "credentials_process":
            credentials = response["credentials"]
            formatted_credentials = {
                "Version": 1,
                "AccessKeyId": credentials["accessKeyId"],
                "SecretAccessKey": credentials["secretAccessKey"],
                "SessionToken": credentials["sessionToken"],
                "Expiration": credentials["expiration"].isoformat(),
            }
            click.echo(json.dumps(formatted_credentials, indent=2))
        else:
            is_success = False
            error_type = "InvalidOutputFormat"
            raise DeadlineOperationError(f"Invalid output format: {output_format}")

    except ClientError as exc:
        is_success = False
        error_type = "ClientError"
        if "AccessDenied" in str(exc):
            raise DeadlineOperationError(
                f"Insufficient permissions to assume the requested queue role: {exc}"
            ) from exc
        elif "UnrecognizedClientException" in str(exc):
            raise DeadlineOperationError(
                f"Authentication failed. Please run 'deadline auth login' or check your AWS credentials: {exc}"
            ) from exc
        else:
            raise DeadlineOperationError(
                f"Failed to get credentials from AWS Deadline Cloud:\n{exc}"
            ) from exc
    finally:
        # Record telemetry
        duration_ms = int((time.time() - start_time) * 1000)
        api._telemetry.get_deadline_cloud_library_telemetry_client().record_event(
            "com.amazon.rum.deadline.queue_export_credentials",
            {
                "mode": mode,
                "queue_id": queue_id,
                "output_format": output_format,
                "is_success": is_success,
                "error_type": error_type if not is_success else None,
                "duration_ms": duration_ms,
            },
        )


@cli_queue.command(name="paramdefs")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The farm to use.")
@click.option("--queue-id", help="The queue to use.")
@_handle_error
def queue_paramdefs(**args):
    """
    Lists a Queue's Parameters Definitions.
    """
    # Get a temporary config object with the standard options handled
    config = _apply_cli_options_to_config(required_options={"farm_id", "queue_id"}, **args)

    farm_id = config_file.get_setting("defaults.farm_id", config=config)
    queue_id = config_file.get_setting("defaults.queue_id", config=config)

    try:
        response = api.get_queue_parameter_definitions(farmId=farm_id, queueId=queue_id)
    except ClientError as exc:
        raise DeadlineOperationError(
            f"Failed to get Queue Parameter Definitions from Deadline:\n{exc}"
        ) from exc

    click.echo(_cli_object_repr(response))


@cli_queue.command(name="get")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The farm to use.")
@click.option("--queue-id", help="The queue to use.")
@_handle_error
def queue_get(**args):
    """
    Get the details of a queue.

    If Queue ID is not provided, returns the configured default Queue.
    """
    # Get a temporary config object with the standard options handled
    config = _apply_cli_options_to_config(required_options={"farm_id", "queue_id"}, **args)

    farm_id = config_file.get_setting("defaults.farm_id", config=config)
    queue_id = config_file.get_setting("defaults.queue_id", config=config)

    deadline = api.get_boto3_client("deadline", config=config)
    response = deadline.get_queue(farmId=farm_id, queueId=queue_id)
    response.pop("ResponseMetadata", None)

    click.echo(_cli_object_repr(response))


@cli_queue.command(name="sync-output")
@click.option("--farm-id", help="The AWS Deadline Cloud Farm to use.")
@click.option("--queue-id", help="The AWS Deadline Cloud Queue to use.")
@click.option(
    "--storage-profile-id",
    help="The storage profile to use for mapping paths to local. Cannot be used together with --ignore-storage-profiles",
)
@click.option("--json", default=None, is_flag=True, help="Output is printed as JSON for scripting.")
@click.option(
    "--bootstrap-lookback-minutes",
    default=0,
    type=float,
    help="Downloads outputs for job-session-actions that have been completed since these many\n"
    "minutes at bootstrap. Default value is 0 minutes.",
)
@click.option(
    "--checkpoint-dir",
    default=config_file.DEFAULT_QUEUE_INCREMENTAL_DOWNLOAD_DIR,
    help="Proceed downloading from the previous progress file stored in this directory, if it exists.\n"
    "If the file does not exist, the download will initialize using the bootstrap lookback in minutes. \n",
)
@click.option(
    "--force-bootstrap",
    is_flag=True,
    help="Forces command to start from the bootstrap lookback period and overwrite any previous checkpoint.\n"
    "Default value is False.",
    default=False,
)
@click.option(
    "--ignore-storage-profiles",
    is_flag=True,
    help="Ignores the storage profile configuration. Only use if all jobs in the queue are submitted and downloaded from the same machine. Downloads all jobs to unmapped paths regardless of operating system.\n"
    "Default value is False.",
    default=False,
)
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
    default=FileConflictResolution.OVERWRITE.name,
    help="How to handle downloads if an output file already exists:\n"
    "CREATE_COPY: Download the file with a new name, appending '(1)' to the end\n"
    "SKIP: Do not download the file\n"
    "OVERWRITE (default): Download and replace the existing file.\n"
    "Default behaviour is to OVERWRITE.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Perform a dry run of the operation, don't actually download the output files.",
    default=False,
)
@_handle_error
@api.record_success_fail_telemetry_event(metric_name="queue_sync_output")
def sync_output(
    json: bool,
    bootstrap_lookback_minutes: float,
    checkpoint_dir: str,
    force_bootstrap: bool,
    ignore_storage_profiles: bool,
    conflict_resolution: str,
    dry_run: bool,
    **args,
):
    """
    Downloads any new job attachments output for all jobs in a queue since the last run of the same command.

    When run for the first time or with the --force-bootstrap option, it starts downloading from --bootstrap-lookback-minutes
    in the past. When run each subsequent time, it loads  the previous checkpoint and continues
    where it left off.

    With default options, the sync-output operation requires a storage profile defined in the deadline client configuration
    or provided with the --storage-profile-id option. Storage profiles are used to generate path mappings when a job was
    submitted from a machine with a different operating system or file system mount locations than the machine downloading outputs.

    If you only submit and download jobs from the same operating system and mount locations, you can use the --ignore-storage-profiles option.
    """
    api._session.session_context["cli-command-name"] = "deadline.queue.sync-output"

    if sys.version_info < (3, 9):
        raise DeadlineOperationError("The sync-output command requires Python version 3.9 or later")

    if ignore_storage_profiles and args.get("storage_profile_id") is not None:
        raise click.UsageError(
            "Options '--storage-profile-id' and '--ignore-storage-profiles' cannot be provided together"
        )

    logger: ClickLogger = ClickLogger(is_json=json)

    # Expand '~' to home directory and create the checkpoint directory if necessary
    checkpoint_dir = os.path.abspath(os.path.expanduser(checkpoint_dir))
    os.makedirs(checkpoint_dir, exist_ok=True)

    # Check that download progress location is writable
    if not os.access(checkpoint_dir, os.W_OK):
        raise DeadlineOperationError(
            f"Download progress checkpoint directory {checkpoint_dir} exists but is not writable, please provide write permissions"
        )

    # Get a temporary config object with the standard options handled
    config: Optional[ConfigParser] = _apply_cli_options_to_config(
        required_options={"farm_id", "queue_id"}, **args
    )

    # Get the default configs
    farm_id = config_file.get_setting("defaults.farm_id", config=config)
    queue_id = config_file.get_setting("defaults.queue_id", config=config)
    boto3_session: boto3.Session = api.get_boto3_session(config=config)
    deadline = get_session_client(boto3_session, "deadline")

    if ignore_storage_profiles:
        local_storage_profile_id = None
        logger.echo("Ignoring all storage profiles.")
    else:
        local_storage_profile_id = config_file.get_setting(
            "settings.storage_profile_id", config=config
        )
        if not local_storage_profile_id:
            raise DeadlineOperationError(
                "The sync-output operation requires a storage profile defined in the deadline client configuration or "
                "provided with the --storage-profile-id option.\n\n"
                "Storage profiles are used to generate path mappings when a job was submitted from a machine with a different "
                "operating system or file system mount locations than the machine downloading outputs. See "
                "https://docs.aws.amazon.com/deadline-cloud/latest/developerguide/modeling-your-shared-filesystem-locations-with-storage-profiles.html "
                "for more information.\n\n"
                "If you only submit and download jobs from the same operating system and mount locations, you can use the --ignore-storage-profiles option."
            )

        try:
            local_storage_profile = deadline.get_storage_profile_for_queue(
                farmId=farm_id,
                queueId=queue_id,
                storageProfileId=local_storage_profile_id,
            )
        except ClientError as e:
            id_source = (
                "configured locally"
                if args.get("storage_profile_id") is None
                else "provided with --storage-profile-id"
            )
            raise DeadlineOperationError(
                f"Could not retrieve the storage profile {local_storage_profile_id!r}, {id_source}, from Deadline Cloud:\n{e}"
            )

    # Get download progress file name appended by the queue id and storage profile id - a unique progress file exists per queue/storage profile
    download_checkpoint_file_name: str = f"{queue_id}_{local_storage_profile_id or 'ignore-storage-profiles'}_{DOWNLOAD_CHECKPOINT_FILE_NAME}"

    # Get saved progress file full path now that we've validated all file inputs are valid
    checkpoint_file_path: str = os.path.join(checkpoint_dir, download_checkpoint_file_name)

    queue = deadline.get_queue(farmId=farm_id, queueId=queue_id)
    if "jobAttachmentSettings" not in queue:
        raise DeadlineOperationError(
            f"Queue '{queue['displayName']}' does not have job attachments configured."
        )

    logger.echo(f"Started incremental download for queue: {queue['displayName']}")
    logger.echo(f"Checkpoint: {checkpoint_file_path}")
    logger.echo()

    if local_storage_profile_id:
        logger.echo(
            f"Mapping job output paths to the local storage profile {local_storage_profile['displayName']} ({local_storage_profile_id})"
        )
        logger.echo("  File system locations for the storage profile are:")
        for location in local_storage_profile["fileSystemLocations"]:
            logger.echo(f"    {location['name']}: {location['path']}")
        logger.echo()

    # Perform incremental download while holding a process id lock

    pid_lock_file_path: str = os.path.join(checkpoint_dir, f"{download_checkpoint_file_name}.pid")

    with PidFileLock(
        pid_lock_file_path,
        operation_name="incremental output download",
    ):
        checkpoint: IncrementalDownloadState

        if force_bootstrap or not os.path.exists(checkpoint_file_path):
            bootstrap_timestamp = datetime.now(timezone.utc) - timedelta(
                minutes=bootstrap_lookback_minutes
            )
            # Bootstrap with the specified lookback duration
            checkpoint = IncrementalDownloadState(
                local_storage_profile_id=local_storage_profile_id,
                downloads_started_timestamp=bootstrap_timestamp,
            )

            # Print the bootstrap time in local time
            if force_bootstrap:
                logger.echo(f"Bootstrap forced, lookback is {bootstrap_lookback_minutes} minutes")
            else:
                logger.echo(
                    f"Checkpoint not found, lookback is {bootstrap_lookback_minutes} minutes"
                )
            logger.echo(f"Initializing from: {bootstrap_timestamp.astimezone().isoformat()}")
        else:
            # Load the incremental download checkpoint file
            checkpoint = IncrementalDownloadState.from_file(checkpoint_file_path)

            # Print the previous download completed time in local time
            logger.echo("Checkpoint found")

            # The checkpoint's local storage profile id must match the CLI option
            if local_storage_profile_id != checkpoint.local_storage_profile_id:
                if checkpoint.local_storage_profile_id is None:
                    raise DeadlineOperationError(
                        "The checkpoint was created with the --ignore-storage-profiles, you must use the same option to continue from it."
                    )
                if local_storage_profile_id is None:
                    raise DeadlineOperationError(
                        "The checkpoint was created without the --ignore-storage-profiles, you must leave out the option to continue from it."
                    )
                raise DeadlineOperationError(
                    f"The checkpoint was created with local storage profile {checkpoint.local_storage_profile_id}, but the configured storage profile is {local_storage_profile_id}"
                )

            logger.echo(
                f"Continuing from: {checkpoint.downloads_completed_timestamp.astimezone().isoformat()}"
            )

        logger.echo()

        updated_download_state: IncrementalDownloadState = _incremental_output_download(
            boto3_session=boto3_session,
            farm_id=farm_id,
            queue=queue,
            checkpoint=checkpoint,
            file_conflict_resolution=FileConflictResolution[conflict_resolution],
            config=config,
            print_function_callback=logger.echo,
            dry_run=dry_run,
        )

        # Save the checkpoint file if it's not a dry run
        if not dry_run:
            updated_download_state.save_file(checkpoint_file_path)
            logger.echo("Checkpoint saved")
        else:
            logger.echo("This is a DRY RUN so the checkpoint was not saved")
