# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Provides the function to submit a job bundle to Amazon Deadline Cloud.
"""
from __future__ import annotations

import json
import logging
import time
import os
import textwrap
from configparser import ConfigParser
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from deadline.client import api
from deadline.client.exceptions import DeadlineOperationError, CreateJobWaiterCanceled
from deadline.client.config import get_setting, set_setting
from deadline.client.job_bundle.loader import read_yaml_or_json, read_yaml_or_json_object
from deadline.client.job_bundle.parameters import (
    apply_job_parameters,
    merge_queue_job_parameters,
    read_job_bundle_parameters,
)
from deadline.client.job_bundle.submission import AssetReferences, split_parameter_args
from deadline.job_attachments.models import (
    JobAttachmentsFileSystem,
    AssetRootManifest,
    JobAttachmentS3Settings,
)
from deadline.job_attachments.progress_tracker import SummaryStatistics, ProgressReportMetadata
from deadline.job_attachments.upload import S3AssetManager
from botocore.client import BaseClient

from ..job_bundle.parameters import JobParameter

logger = logging.getLogger(__name__)


def create_job_from_job_bundle(
    job_bundle_dir: str,
    job_parameters: list[dict[str, Any]] = [],
    queue_parameter_definitions: Optional[list[JobParameter]] = None,
    job_attachments_file_system: str = "COPIED",
    should_save_job_id: Optional[bool] = None,
    config: Optional[ConfigParser] = None,
    priority: Optional[int] = None,
    max_failed_tasks_count: Optional[int] = None,
    max_retries_per_task: Optional[int] = None,
    handle_echo_messages_callback: Callable[[str], None] = lambda msg: None,
    decide_cancel_submission_callback: Callable[
        [AssetReferences, SummaryStatistics], bool
    ] = lambda ar, hs: False,
    hashing_progress_callback: Optional[Callable[[ProgressReportMetadata], bool]] = None,
    upload_progress_callback: Optional[Callable[[ProgressReportMetadata], bool]] = None,
    create_job_result_callback: Optional[Callable[[], bool]] = None,
) -> Union[str, None]:
    """
    Creates a job in the Amazon Deadline Cloud farm/queue configured as default for the
    workstation from the job bundle in the provided directory.

    A job bundle has the following directory structure:

    /template.json|yaml (required): An Open Job Description job template that specifies the work to be done. Job parameters
            are embedded here.
    /parameter_values.yson|yaml (optional): If provided, these are parameter values for the job template and for
            the render farm. Amazon Deadline Cloud-specific parameters are like "deadline:priority".
            Looks like:
            {
                "parameterValues": [
                    {"name": "<name>", "value": "<value>"},
                    ...
                ]
            }
    /asset_references.json|yaml (optional): If provided, these are references to the input and output assets
            of the job. Looks like:
            {
                "assetReferences": {
                    "inputs": {
                        "filenames": [
                            "/mnt/path/to/file.txt",
                            ...
                        ],
                        "directories": [
                            "/mnt/path/to/directory",
                            ...
                        ],
                    },
                    "outputs": {
                        "directories": [
                            "/mnt/path/to/output_directory",
                            ...
                        ],
                    }
                }
            }

    Args:
        job_bundle_dir (str): The directory containing the job bundle.
        job_parameters (List[Dict[str, Any]], optional): A list of job parameters in the following format:
            [{"name": "<name>", "value": "<value>"}, ...]
        config (ConfigParser, optional): The Amazon Deadline Cloud configuration
                object to use instead of the config file.
    """

    # Read in the job template
    file_contents, file_type = read_yaml_or_json(job_bundle_dir, "template", required=True)

    deadline = api.get_boto3_client("deadline", config=config)
    queue_id = get_setting("defaults.queue_id", config=config)
    farm_id = get_setting("defaults.farm_id", config=config)

    if job_attachments_file_system is None:
        job_attachments_file_system = get_setting(
            "defaults.job_attachments_file_system", config=config
        )

    queue = deadline.get_queue(
        farmId=farm_id,
        queueId=queue_id,
    )
    handle_echo_messages_callback(f"Submitting to Queue: {queue['displayName']}")

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
    asset_references = AssetReferences.from_dict(asset_references_obj)

    if queue_parameter_definitions is None:
        queue_parameter_definitions = api.get_queue_parameter_definitions(
            farmId=farm_id, queueId=queue_id
        )

    parameters = merge_queue_job_parameters(
        queue_id=queue_id,
        job_parameters=job_bundle_parameters,
        queue_parameters=queue_parameter_definitions,
    )

    apply_job_parameters(
        job_parameters,
        job_bundle_dir,
        parameters,
        asset_references,
    )
    app_parameters_formatted, job_parameters_formatted = split_parameter_args(
        parameters, job_bundle_dir
    )

    # Hash and upload job attachments if there are any
    if asset_references and "jobAttachmentSettings" in queue:
        # Extend input_filenames with all the files in the input_directories
        for directory in asset_references.input_directories:
            for root, _, files in os.walk(directory):
                asset_references.input_filenames.update(
                    os.path.normpath(os.path.join(root, file)) for file in files
                )
        asset_references.input_directories.clear()

        queue_role_session = api.get_queue_user_boto3_session(
            deadline=deadline,
            config=config,
            farm_id=farm_id,
            queue_id=queue_id,
            queue_display_name=queue["displayName"],
        )

        asset_manager = S3AssetManager(
            farm_id=farm_id,
            queue_id=queue_id,
            job_attachment_settings=JobAttachmentS3Settings(**queue["jobAttachmentSettings"]),
            session=queue_role_session,
        )

        hash_summary, asset_manifests = _hash_attachments(
            asset_manager=asset_manager,
            asset_references=asset_references,
            storage_profile_id=storage_profile_id,
            handle_echo_messages_callback=handle_echo_messages_callback,
            hashing_progress_callback=hashing_progress_callback,
        )

        if decide_cancel_submission_callback(asset_references, hash_summary):
            handle_echo_messages_callback("Job submission canceled.")
            return None

        attachment_settings = _upload_attachments(
            asset_manager, asset_manifests, handle_echo_messages_callback, upload_progress_callback
        )

        attachment_settings["fileSystem"] = JobAttachmentsFileSystem(job_attachments_file_system)
        create_job_args["attachments"] = attachment_settings

    create_job_args.update(app_parameters_formatted)

    if job_parameters_formatted:
        create_job_args["parameters"] = job_parameters_formatted

    if priority is not None:
        create_job_args["priority"] = priority
    if max_failed_tasks_count is not None:
        create_job_args["maxFailedTasksCount"] = max_failed_tasks_count
    if max_retries_per_task is not None:
        create_job_args["maxRetriesPerTask"] = max_retries_per_task

    if logging.DEBUG >= logger.getEffectiveLevel():
        logger.debug(json.dumps(create_job_args, indent=1))

    create_job_response = deadline.create_job(**create_job_args)
    logger.debug(f"CreateJob Response {create_job_response}")

    if create_job_response and "jobId" in create_job_response:
        job_id = create_job_response["jobId"]
        handle_echo_messages_callback("Waiting for Job to be created...")

        # If saving job id is not otherwise specified, then if using the default config,
        # set the default job id so it holds the most-recently submitted job.
        if should_save_job_id is None:
            should_save_job_id = config is None

        if should_save_job_id:
            set_setting("defaults.job_id", job_id)

        def _default_create_job_result_callback() -> bool:
            return True

        if not create_job_result_callback:
            create_job_result_callback = _default_create_job_result_callback

        success, status_message = wait_for_create_job_to_complete(
            farm_id,
            queue_id,
            job_id,
            deadline,
            create_job_result_callback,
        )

        if not success:
            raise DeadlineOperationError(status_message)

        handle_echo_messages_callback("Submitted job bundle:")
        handle_echo_messages_callback(f"   {job_bundle_dir}")
        handle_echo_messages_callback(status_message + f"\n{job_id}\n")

        return job_id
    else:
        raise DeadlineOperationError("CreateJob response was empty, or did not contain a Job ID.")


def wait_for_create_job_to_complete(
    farm_id: str,
    queue_id: str,
    job_id: str,
    deadline_client: BaseClient,
    continue_callback: Callable,
) -> Tuple[bool, str]:
    """
    Wait until a job exits the CREATE_IN_PROGRESS state.
    """

    delay_sec = 5  # Time to wait between checks in seconds.
    max_attempts = 60
    creating_statuses = {
        "CREATE_IN_PROGRESS",
    }
    failure_statuses = {"CREATE_FAILED"}

    for attempt in range(max_attempts):
        logger.debug(
            f"Waiting for creation of {job_id} to complete...attempt {attempt} of {max_attempts}"
        )

        if not continue_callback():
            raise CreateJobWaiterCanceled

        job = deadline_client.get_job(jobId=job_id, queueId=queue_id, farmId=farm_id)

        current_status = job["lifecycleStatus"] if "lifecycleStatus" in job else job["state"]
        if current_status in creating_statuses:
            time.sleep(delay_sec)
        elif current_status in failure_statuses:
            return False, job["lifecycleStatusMessage"]
        else:
            return True, job["lifecycleStatusMessage"]

    raise TimeoutError(
        f"Timed out after {delay_sec * max_attempts} seconds while waiting for Job to be created: {job_id}"
    )


def _hash_attachments(
    asset_manager: S3AssetManager,
    asset_references: AssetReferences,
    storage_profile_id: Optional[str] = None,
    handle_echo_messages_callback: Callable = lambda msg: None,
    hashing_progress_callback: Optional[Callable] = None,
    config: Optional[ConfigParser] = None,
) -> Tuple[SummaryStatistics, List[AssetRootManifest]]:
    """
    Starts the job attachments hashing and handles the progress reporting
    callback. Returns a list of the asset manifests of the hashed files.
    """

    def _default_update_hash_progress(hashing_metadata: Dict[str, str]) -> bool:
        return True

    if not hashing_progress_callback:
        hashing_progress_callback = _default_update_hash_progress

    hashing_summary, manifests = asset_manager.hash_assets_and_create_manifest(
        input_paths=sorted(asset_references.input_filenames),
        output_paths=sorted(asset_references.output_directories),
        referenced_paths=sorted(asset_references.referenced_paths),
        storage_profile_id=storage_profile_id,
        hash_cache_dir=os.path.expanduser(os.path.join("~", ".deadline", "cache")),
        on_preparing_to_submit=hashing_progress_callback,
    )
    api.get_deadline_cloud_library_telemetry_client(config=config).record_hashing_summary(
        hashing_summary
    )
    handle_echo_messages_callback("Hashing Summary:")
    handle_echo_messages_callback(textwrap.indent(str(hashing_summary), "    "))

    return hashing_summary, manifests


def _upload_attachments(
    asset_manager: S3AssetManager,
    manifests: List[AssetRootManifest],
    handle_echo_messages_callback: Callable = lambda msg: None,
    upload_progress_callback: Optional[Callable] = None,
    config: Optional[ConfigParser] = None,
) -> Dict[str, Any]:
    """
    Starts the job attachments upload and handles the progress reporting callback.
    Returns the attachment settings from the upload.
    """

    def _default_update_upload_progress(upload_metadata: Dict[str, str]) -> bool:
        return True

    if not upload_progress_callback:
        upload_progress_callback = _default_update_upload_progress

    upload_summary, attachment_settings = asset_manager.upload_assets(
        manifests, upload_progress_callback
    )
    api.get_deadline_cloud_library_telemetry_client(config=config).record_upload_summary(
        upload_summary
    )

    handle_echo_messages_callback("Upload Summary:")
    handle_echo_messages_callback(textwrap.indent(str(upload_summary), "    "))

    return attachment_settings.to_dict()
