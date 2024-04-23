# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Provides the function to submit a job bundle to AWS Deadline Cloud.
"""
from __future__ import annotations

import json
import logging
import time
import os
import textwrap
from configparser import ConfigParser
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from botocore.client import BaseClient  # type: ignore[import]

from .. import api
from ..exceptions import DeadlineOperationError, CreateJobWaiterCanceled
from ..config import get_setting, set_setting, config_file
from ..job_bundle import deadline_yaml_dump
from ..job_bundle.loader import (
    read_yaml_or_json,
    read_yaml_or_json_object,
    parse_yaml_or_json_content,
    validate_directory_symlink_containment,
)
from ..job_bundle.parameters import (
    apply_job_parameters,
    merge_queue_job_parameters,
    read_job_bundle_parameters,
    JobParameter,
)
from ..job_bundle.submission import AssetReferences, split_parameter_args
from ...job_attachments.exceptions import MisconfiguredInputsError
from ...job_attachments.models import (
    JobAttachmentsFileSystem,
    AssetRootGroup,
    AssetRootManifest,
    AssetUploadGroup,
    JobAttachmentS3Settings,
)
from ...job_attachments.progress_tracker import SummaryStatistics, ProgressReportMetadata
from ...job_attachments.upload import S3AssetManager

logger = logging.getLogger(__name__)


def create_job_from_job_bundle(
    job_bundle_dir: str,
    job_parameters: list[dict[str, Any]] = [],
    *,
    name: Optional[str] = None,
    queue_parameter_definitions: Optional[list[JobParameter]] = None,
    job_attachments_file_system: Optional[str] = None,
    config: Optional[ConfigParser] = None,
    priority: Optional[int] = None,
    max_failed_tasks_count: Optional[int] = None,
    max_retries_per_task: Optional[int] = None,
    print_function_callback: Callable[[str], None] = lambda msg: None,
    decide_cancel_submission_callback: Callable[
        [AssetUploadGroup], bool
    ] = lambda upload_group: False,
    hashing_progress_callback: Optional[Callable[[ProgressReportMetadata], bool]] = None,
    upload_progress_callback: Optional[Callable[[ProgressReportMetadata], bool]] = None,
    create_job_result_callback: Optional[Callable[[], bool]] = None,
    require_paths_exist: bool = False,
) -> Union[str, None]:
    """
    Creates a job in the AWS Deadline Cloud farm/queue configured as default for the
    workstation from the job bundle in the provided directory.

    A job bundle has the following directory structure:

    /template.json|yaml (required): An Open Job Description job template that specifies the work to be done. Job parameters
            are embedded here.
    /parameter_values.json|yaml (optional): If provided, these are parameter values for the job template and for
            the render farm. AWS Deadline Cloud-specific parameters are like "deadline:priority".
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
        name (str, optional): The name of the job to submit, replacing the name defined in the job bundle.
        queue_parameter_definitions (list[JobParameter], optional) A list of queue_parameters to use
                instead of retrieving queue_parameters from the queue with get_queue_parameter_definitions.
        job_attachments_file_system (str, optional): define which file system to use;
                (valid values: "COPIED", "VIRTUAL") instead of using the value in the config file.
        config (ConfigParser, optional): The AWS Deadline Cloud configuration
                object to use instead of the config file.
        priority (int, optional): explicit value for the priority of the job.
        max_failed_tasks_count (int, optional): explicit value for the maximum allowed failed tasks.
        max_retries_per_task (int, optional): explicit value for the maximum retries per task.
        print_function_callback (Callable str -> None, optional): Callback to print messages produced in this function.
                Used in the CLI to print to stdout using click.echo. By default ignores messages.
        decide_cancel_submission_callback (Callable dict[str, int], int, int -> bool): If the job has job
                attachments, decide whether or not to cancel the submission given what assets will
                or will not be uploaded. If returns true, the submission is canceled. If False,
                the submission continues. By default the submission always continues.
        hashing_progress_callback / upload_progress_callback / create_job_result_callback (Callable -> bool):
                Callbacks periodically called while hashing / uploading / waiting for job creation. If returns false,
                the operation will be cancelled. If return true, the operation continues. Default behavior for each
                is to not cancel the operation. hashing_progress_callback and upload_progress_callback both receive
                ProgressReport as a parameter, which can be used for projecting remaining time, as in done in the CLI.
    """

    # Ensure the job bundle doesn't contain files that resolve outside of the bundle directory
    validate_directory_symlink_containment(job_bundle_dir)

    # Read in the job template
    file_contents, file_type = read_yaml_or_json(job_bundle_dir, "template", required=True)

    # If requested, substitute the job name in the template
    if name is not None:
        template_obj = parse_yaml_or_json_content(
            file_contents, file_type, job_bundle_dir, "template"
        )
        template_obj["name"] = name
        if file_type == "YAML":
            file_contents = deadline_yaml_dump(template_obj)
        else:
            file_contents = json.dumps(template_obj)

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
    print_function_callback(f"Submitting to Queue: {queue['displayName']}")

    create_job_args: Dict[str, Any] = {
        "farmId": farm_id,
        "queueId": queue_id,
        "template": file_contents,
        "templateType": file_type,
        "priority": 50,
    }

    storage_profile_id = get_setting("settings.storage_profile_id", config=config)
    storage_profile = None
    if storage_profile_id:
        create_job_args["storageProfileId"] = storage_profile_id
        storage_profile = api.get_storage_profile_for_queue(
            farm_id, queue_id, storage_profile_id, deadline
        )

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
        missing_directories: set[str] = set()
        for directory in asset_references.input_directories:
            if not os.path.isdir(directory):
                if require_paths_exist:
                    missing_directories.add(directory)
                else:
                    logger.warning(
                        f"Input path '{directory}' does not exist. Adding to referenced paths."
                    )
                    asset_references.referenced_paths.add(directory)
                continue

            is_dir_empty = True
            for root, _, files in os.walk(directory):
                if not files:
                    continue
                is_dir_empty = False
                asset_references.input_filenames.update(
                    os.path.normpath(os.path.join(root, file)) for file in files
                )
            # Empty directories just become references since there's nothing to upload
            if is_dir_empty:
                logger.info(f"Input directory '{directory}' is empty. Adding to referenced paths.")
                asset_references.referenced_paths.add(directory)
        asset_references.input_directories.clear()

        if missing_directories:
            all_missing_directories = "\n\t".join(sorted(list(missing_directories)))
            misconfigured_directories_msg = (
                "Job submission contains misconfigured input directories and cannot be submitted."
                " All input directories must exist."
                f"\nNon-existent directories:\n\t{all_missing_directories}"
            )

            raise MisconfiguredInputsError(misconfigured_directories_msg)

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

        upload_group = asset_manager.prepare_paths_for_upload(
            input_paths=sorted(asset_references.input_filenames),
            output_paths=sorted(asset_references.output_directories),
            referenced_paths=sorted(asset_references.referenced_paths),
            storage_profile=storage_profile,
            require_paths_exist=require_paths_exist,
        )
        if upload_group.asset_groups:
            if decide_cancel_submission_callback(upload_group):
                print_function_callback("Job submission canceled.")
                return None

            _, asset_manifests = _hash_attachments(
                asset_manager=asset_manager,
                asset_groups=upload_group.asset_groups,
                total_input_files=upload_group.total_input_files,
                total_input_bytes=upload_group.total_input_bytes,
                print_function_callback=print_function_callback,
                hashing_progress_callback=hashing_progress_callback,
            )

            attachment_settings = _upload_attachments(
                asset_manager, asset_manifests, print_function_callback, upload_progress_callback
            )
            attachment_settings["fileSystem"] = JobAttachmentsFileSystem(
                job_attachments_file_system
            )
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

    api.get_deadline_cloud_library_telemetry_client().record_event(
        event_type="com.amazon.rum.deadline.submission",
        event_details={},
    )

    create_job_response = deadline.create_job(**create_job_args)
    logger.debug(f"CreateJob Response {create_job_response}")

    if create_job_response and "jobId" in create_job_response:
        job_id = create_job_response["jobId"]
        print_function_callback("Waiting for Job to be created...")

        # If using the default config, set the default job id so it holds the
        # most-recently submitted job.
        if config is None:
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

        api.get_deadline_cloud_library_telemetry_client().record_event(
            event_type="com.amazon.rum.deadline.create_job", event_details={"is_success": success}
        )

        if not success:
            raise DeadlineOperationError(status_message)

        print_function_callback("Submitted job bundle:")
        print_function_callback(f"   {job_bundle_dir}")
        print_function_callback(status_message + f"\n{job_id}\n")

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
    asset_groups: list[AssetRootGroup],
    total_input_files: int,
    total_input_bytes: int,
    print_function_callback: Callable = lambda msg: None,
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
        asset_groups=asset_groups,
        total_input_files=total_input_files,
        total_input_bytes=total_input_bytes,
        hash_cache_dir=config_file.get_cache_directory(),
        on_preparing_to_submit=hashing_progress_callback,
    )
    api.get_deadline_cloud_library_telemetry_client(config=config).record_hashing_summary(
        hashing_summary
    )
    print_function_callback("Hashing Summary:")
    print_function_callback(textwrap.indent(str(hashing_summary), "    "))

    return hashing_summary, manifests


def _upload_attachments(
    asset_manager: S3AssetManager,
    manifests: List[AssetRootManifest],
    print_function_callback: Callable = lambda msg: None,
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
        manifests=manifests,
        on_uploading_assets=upload_progress_callback,
        s3_check_cache_dir=config_file.get_cache_directory(),
    )
    api.get_deadline_cloud_library_telemetry_client(config=config).record_upload_summary(
        upload_summary
    )

    print_function_callback("Upload Summary:")
    print_function_callback(textwrap.indent(str(upload_summary), "    "))

    return attachment_settings.to_dict()
