# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
All the `deadline bundle` commands.
"""

import json
import logging
import os
import re
import signal
from typing import Any, Dict, List, Optional, Set, Tuple

import click
from botocore.exceptions import ClientError  # type: ignore[import]

from deadline.client import api
from deadline.client.api import get_boto3_client, get_queue_boto3_session
from deadline.client.api._session import _modified_logging_level
from deadline.client.config import config_file, get_setting, set_setting
from deadline.client.job_bundle.loader import read_yaml_or_json, read_yaml_or_json_object
from deadline.client.job_bundle.parameters import apply_job_parameters, read_job_bundle_parameters
from deadline.client.job_bundle.submission import (
    FlatAssetReferences,
    split_parameter_args,
    upload_job_attachments,
)
from deadline.job_attachments.errors import AssetSyncError, AssetSyncCancelledError
from deadline.job_attachments.models import (
    AssetRootManifest,
    JobAttachmentS3Settings,
)
from deadline.job_attachments.progress_tracker import ProgressReportMetadata, SummaryStatistics
from deadline.job_attachments.upload import S3AssetManager
from deadline.job_attachments.utils import human_readable_file_size, AssetLoadingMethod

from ...exceptions import DeadlineOperationError, CreateJobWaiterCanceled
from .._common import apply_cli_options_to_config, handle_error

logger = logging.getLogger(__name__)


continue_submission = True


def _handle_sigint(signum, frame) -> None:
    global continue_submission
    continue_submission = False


signal.signal(signal.SIGINT, _handle_sigint)


@click.group(name="bundle")
@handle_error
def cli_bundle():
    """
    Commands to work with OpenJobIO job bundles.
    """


def validate_parameters(ctx, param, value):
    """
    Validate provided --parameter values, ensuring that they are in the format "Key=Value", and convert them to a dict with the
    following format:
        [{"name": "<name>", "value": "<value>"}, ...]
    """
    parameters_split = []
    for parameter in value:
        regex_match = re.match("(.+)=(.+)", parameter)
        if not regex_match:
            raise click.BadParameter(
                f'Parameters must be provided in the format "Key=Value". Invalid Parameter: {parameter}'
            )

        parameters_split.append({"name": regex_match[1], "value": regex_match[2]})

    return parameters_split


@cli_bundle.command(name="submit")
@click.option(
    "-p", "--parameter", multiple=True, callback=validate_parameters, help="Job template parameters"
)
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The Amazon Deadline Cloud Farm to use.")
@click.option("--queue-id", help="The Amazon Deadline Cloud Queue to use.")
@click.option(
    "--asset-loading-method",
    help="The method to use for loading assets on the server.  Options are PRELOAD (load assets onto server first then run the job) or ON_DEMAND (load assets as requested).",
    type=click.Choice([e.value for e in AssetLoadingMethod]),
    default=AssetLoadingMethod.PRELOAD.value,
)
@click.option(
    "--yes",
    is_flag=True,
    help="Skip any confirmation prompts",
)
@click.argument("job_bundle_dir")
@handle_error
def bundle_submit(job_bundle_dir, asset_loading_method, parameter, **args):
    """
    Submits an OpenJobIO job bundle to Amazon Deadline Cloud.
    """
    # Check Whether the CLI options are modifying any of the default settings that affect
    # the job id. If not, we'll save the job id submitted as the default job id.
    if args.get("profile") is None and args.get("farm_id") is None and args.get("queue_id") is None:
        should_save_job_id = True
    else:
        should_save_job_id = False
    # Get a temporary config object with the standard options handled
    config = apply_cli_options_to_config(required_options={"farm_id", "queue_id"}, **args)

    try:
        deadline = get_boto3_client("deadline", config=config)
        farm_id = get_setting("defaults.farm_id", config=config)
        queue_id = get_setting("defaults.queue_id", config=config)

        queue = deadline.get_queue(farmId=farm_id, queueId=queue_id)
        click.echo(f"Submitting to Queue: {queue['displayName']}")

        # Read in the job template
        file_contents, file_type = read_yaml_or_json(job_bundle_dir, "template", required=True)

        create_job_args: Dict[str, Any] = {
            "farmId": farm_id,
            "queueId": queue_id,
            "template": file_contents,
            "templateType": file_type,
        }

        storage_profile_id = get_setting("settings.storage_profile_id", config=config)
        if storage_profile_id:
            create_job_args["storageProfileId"] = storage_profile_id

        # The job parameters
        job_bundle_parameters = read_job_bundle_parameters(job_bundle_dir)

        asset_references_obj = read_yaml_or_json_object(
            job_bundle_dir, "asset_references", required=False
        )
        asset_references = FlatAssetReferences.from_dict(asset_references_obj)

        apply_job_parameters(parameter, job_bundle_dir, job_bundle_parameters, asset_references)
        app_parameters_formatted, job_parameters_formatted = split_parameter_args(
            job_bundle_parameters, job_bundle_dir
        )

        # Hash and upload job attachments if there are any
        if asset_references:
            # Extend input_filenames with all the files in the input_directories
            for directory in asset_references.input_directories:
                for root, _, files in os.walk(directory):
                    asset_references.input_filenames.update(
                        os.path.normpath(os.path.join(root, file)) for file in files
                    )
            asset_references.input_directories.clear()

            queue_role_session = get_queue_boto3_session(
                deadline=deadline,
                config=config,
                farm_id=farm_id,
                queue_id=queue_id,
                queue_display_name=queue["displayName"],
            )

            asset_manager = S3AssetManager(
                farm_id=create_job_args["farmId"],
                queue_id=create_job_args["queueId"],
                job_attachment_settings=JobAttachmentS3Settings(**queue["jobAttachmentSettings"]),
                session=queue_role_session,
            )

            hash_summary, asset_manifests = _hash_attachments(
                asset_manager,
                asset_references.input_filenames,
                asset_references.output_directories,
                storage_profile_id=storage_profile_id,
            )

            if (
                not config_file.str2bool(get_setting("settings.auto_accept", config=config))
                and len(asset_references.input_filenames) > 0
                and not click.confirm(
                    f"Job submission contains {hash_summary.total_files} files "
                    f"totaling {human_readable_file_size(hash_summary.total_bytes)}. "
                    "All files will be uploaded to S3 if they are not already present in the job attachments bucket. "
                    "Do you wish to proceed?",
                    default=True,
                )
            ):
                click.echo("Job submission canceled.")
                return

            attachment_settings = _upload_attachments(asset_manager, asset_manifests)

            attachment_settings["assetLoadingMethod"] = AssetLoadingMethod(asset_loading_method)
            create_job_args["attachments"] = attachment_settings

        create_job_args.update(app_parameters_formatted)

        if job_parameters_formatted:
            create_job_args["parameters"] = job_parameters_formatted

        if logging.DEBUG >= logger.getEffectiveLevel():
            logger.debug(json.dumps(create_job_args, indent=1))

        create_job_response = deadline.create_job(**create_job_args)
        logger.debug(f"CreateJob Response {create_job_response}")

        if create_job_response and "jobId" in create_job_response:
            job_id = create_job_response["jobId"]
            click.echo("Waiting for Job to be created...")

            # If using the default config, set the default job id so it holds the
            # most-recently submitted job.
            if should_save_job_id:
                set_setting("defaults.job_id", job_id)

            def _check_create_job_wait_canceled() -> bool:
                return continue_submission

            success, status_message = api.wait_for_create_job_to_complete(
                create_job_args["farmId"],
                create_job_args["queueId"],
                job_id,
                deadline,
                _check_create_job_wait_canceled,
            )
            status_message += f"\n{job_id}\n"
        else:
            raise DeadlineOperationError(
                "CreateJob response was empty, or did not contain a Job ID."
            )
    except AssetSyncCancelledError as exc:
        if continue_submission:
            raise DeadlineOperationError(f"Job submission unexpectedly canceled:\n{exc}") from exc
        else:
            click.echo("Job submission canceled.")
            return
    except AssetSyncError as exc:
        raise DeadlineOperationError(f"Failed to upload job attachments:\n{exc}") from exc
    except CreateJobWaiterCanceled as exc:
        if continue_submission:
            raise DeadlineOperationError(
                f"Unexpectedly canceled during wait for final status of CreateJob:\n{exc}"
            ) from exc
        else:
            click.echo("Canceled waiting for final status of CreateJob.")
            return
    except ClientError as exc:
        raise DeadlineOperationError(
            f"Failed to submit the job bundle to Amazon Deadline Cloud:\n{exc}"
        ) from exc

    click.echo("Submitted job bundle:")
    click.echo(f"   {job_bundle_dir}")
    click.echo(status_message)


@cli_bundle.command(name="gui-submit")
@click.argument("job_bundle_dir", required=False)
@handle_error
def bundle_gui_submit(job_bundle_dir, **args):
    """
    Opens GUI to submit an OpenJobIO job bundle to Amazon Deadline Cloud.
    """
    from ...ui import gui_context_for_cli

    with gui_context_for_cli() as app:
        from ...ui.job_bundle_submitter import show_job_bundle_submitter

        submitter = show_job_bundle_submitter(job_bundle_dir)

        if submitter:
            response = submitter.show()

        app.exec_()

        response = None
        if submitter:
            response = submitter.create_job_response
        if response:
            click.echo("Submitted job bundle:")
            click.echo(f"   {job_bundle_dir}")
            click.echo(f"Job ID: {response['jobId']}")
        else:
            click.echo("Job submission canceled.")


def _hash_attachments(
    asset_manager: S3AssetManager,
    input_paths: Set[str],
    output_paths: Set[str],
    storage_profile_id: Optional[str] = None,
) -> Tuple[SummaryStatistics, List[AssetRootManifest]]:
    """
    Starts the job attachments hashing and handles the progress reporting
    callback. Returns a list of the asset manifests of the hashed files.
    """

    with click.progressbar(length=100, label="Hashing Attachments") as hashing_progress:

        def _update_hash_progress(hashing_metadata: ProgressReportMetadata) -> bool:
            new_progress = int(hashing_metadata.progress) - hashing_progress.pos
            if new_progress > 0:
                hashing_progress.update(new_progress)
            return continue_submission

        hashing_summary, manifests = asset_manager.hash_assets_and_create_manifest(
            input_paths=sorted(input_paths),
            output_paths=sorted(output_paths),
            storage_profile_id=storage_profile_id,
            hash_cache_dir=os.path.expanduser(os.path.join("~", ".deadline", "cache")),
            on_preparing_to_submit=_update_hash_progress,
        )

    click.echo("Hashing Summary:")
    click.echo(
        f"    Hashed {hashing_summary.processed_files} files totaling"
        f" {human_readable_file_size(hashing_summary.processed_bytes)}."
    )
    click.echo(
        f"    Skipped re-hashing {hashing_summary.skipped_files} files totaling"
        f" {human_readable_file_size(hashing_summary.skipped_bytes)}."
    )
    click.echo(
        f"    Total hashing time of {round(hashing_summary.total_time, ndigits=5)} seconds"
        f" at {human_readable_file_size(int(hashing_summary.transfer_rate))}/s."
    )

    return hashing_summary, manifests


def _upload_attachments(
    asset_manager: S3AssetManager, manifests: List[AssetRootManifest]
) -> Dict[str, Any]:
    """
    Starts the job attachments upload and handles the progress reporting callback.
    Returns the attachment settings from the upload.
    """

    # TODO: remove logging level setting when the max number connections for boto3 client
    # in Job Attachments library can be increased (currently using default number, 10, which
    # makes it keep logging urllib3 warning messages when uploading large files)
    with click.progressbar(
        length=100, label="Uploading Attachments"
    ) as upload_progress, _modified_logging_level(logging.getLogger("urllib3"), logging.ERROR):

        def _update_upload_progress(upload_metadata: ProgressReportMetadata) -> bool:
            new_progress = int(upload_metadata.progress) - upload_progress.pos
            if new_progress > 0:
                upload_progress.update(new_progress)
            return continue_submission

        upload_summary, attachment_settings = upload_job_attachments(
            asset_manager, manifests, _update_upload_progress
        )

    click.echo("Upload Summary:")
    click.echo(
        f"    Uploaded {upload_summary.processed_files} files totaling"
        f" {human_readable_file_size(upload_summary.processed_bytes)}."
    )
    click.echo(
        f"    Skipped re-uploading {upload_summary.skipped_files} files totaling"
        f" {human_readable_file_size(upload_summary.skipped_bytes)}."
    )
    click.echo(
        f"    Total upload time of {round(upload_summary.total_time, ndigits=5)} seconds"
        f" at {human_readable_file_size(int(upload_summary.transfer_rate))}/s."
    )

    return attachment_settings
