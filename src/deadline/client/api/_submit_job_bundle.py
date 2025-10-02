# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Provides the function to submit a job bundle to AWS Deadline Cloud.
"""

from __future__ import annotations

import json
import logging
import time
import os
import re
import textwrap
from configparser import ConfigParser
from typing import Any, Callable, Dict, List, Optional, Tuple, Iterable
from collections.abc import Collection
from pathlib import Path
import shlex
from datetime import datetime

from botocore.client import BaseClient

from .. import api
from ..exceptions import (
    DeadlineOperationError,
    CreateJobWaiterCanceled,
    DeadlineOperationCanceled,
    UserInitiatedCancel,
)
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
    AssetRootManifest,
    AssetUploadGroup,
    JobAttachmentS3Settings,
    StorageProfile,
    FileSystemLocationType,
)
from ...job_attachments.progress_tracker import ProgressReportMetadata, ProgressStatus
from ...job_attachments.upload import S3AssetManager
from ._session import session_context
from ._job_attachment import _hash_attachments  # type: ignore[import]
from ...job_attachments._path_summarization import human_readable_file_size, summarize_path_list

logger = logging.getLogger(__name__)


def _summarize_asset_paths(
    input_paths: Collection[Path | str], output_paths: Collection[Path | str], name: str
) -> list[str]:
    result = []
    if input_paths:
        result.append(f"{name} for upload:\n")
        result.append(textwrap.indent(summarize_path_list(input_paths), "  "))
    if output_paths:
        result.append(f"{name} to collect job outputs for download:\n")
        # We expect the list of output paths to be small, but truncate it to an arbitrary limit just in case for the summary
        summary_entry_count = 4
        if len(output_paths) == summary_entry_count + 1:
            summary_entry_count += 1
        result.extend(f"  {path}\n" for path in sorted(output_paths)[:summary_entry_count])
        if len(output_paths) > summary_entry_count:
            result.append(f"\n  ... and {len(output_paths) - summary_entry_count} more\n")
    return result


def _generate_message_for_asset_paths(
    upload_group: AssetUploadGroup,
    storage_profile: Optional[StorageProfile],
    known_asset_paths: Iterable[str],
) -> tuple[str, bool]:
    """Generate a message about asset uploads and along with a flag indicating if there are warnings."""
    # Collect all the input and output paths
    all_input_paths: set[Path | str] = set()
    all_output_paths: set[Path | str] = set()
    for group in upload_group.asset_groups:
        all_input_paths.update(path for path in group.inputs)
        all_output_paths.update(path for path in group.outputs)

    # Filter to get the unknown paths
    if known_asset_paths:
        known_path_regex = re.compile(
            f"{'|'.join(re.escape(path) for path in known_asset_paths)}.*"
        )
        unknown_input_paths = {
            path for path in all_input_paths if not known_path_regex.match(str(path))
        }
        unknown_output_paths = {
            path for path in all_output_paths if not known_path_regex.match(str(path))
        }
    else:
        unknown_input_paths = all_input_paths
        unknown_output_paths = all_output_paths

    unknown_path_warnings = _summarize_asset_paths(
        unknown_input_paths, unknown_output_paths, "Unknown locations"
    )

    warning_messages = []
    default_prompt_response = not unknown_path_warnings
    if unknown_path_warnings:
        warning_messages.append("\nWARNING: Files were specified outside of known asset paths.\n\n")

    warning_messages.extend(
        [
            f"Job submission contains {upload_group.total_input_files} input files "
            f"totaling {human_readable_file_size(upload_group.total_input_bytes)}. "
            "All input files will be uploaded to S3 if they are not already present in the job attachments bucket.\n\n"
        ]
    )
    warning_messages.extend(_summarize_asset_paths(all_input_paths, all_output_paths, "Locations"))

    if unknown_path_warnings:
        warning_messages.append("\n---\n\n")
        warning_messages.append("The list of known asset prefixes for this submission are:\n")
        if known_asset_paths:
            warning_messages.extend(f"  {path}\n" for path in sorted(set(known_asset_paths)))
        else:
            warning_messages.append("  (empty list)\n")
        warning_messages.append("\n")
        warning_messages.extend(unknown_path_warnings)
        warning_messages.append(
            "\nTo enable submission without user input, add directory locations containing the unknown paths to either \n"
            + "1. The list of known asset paths in the local Deadline Cloud configuration. \n"
        )
        if storage_profile:
            warning_messages.append(
                f"2. The Storage Profile '{storage_profile.displayName}' as LOCAL file system locations, from the AWS Deadline Cloud management console.\n"
            )
        else:
            warning_messages.append(
                "2. In a Storage Profile as LOCAL file system locations created from the AWS Deadline Cloud management console, and then configured on your workstation.\n"
            )

    return "".join(warning_messages), default_prompt_response


@api.record_success_fail_telemetry_event(metric_name="asset_upload")
def _upload_attachments(
    asset_manager: S3AssetManager,
    manifests: List[AssetRootManifest],
    print_function_callback: Callable,
    upload_progress_callback: Optional[Callable],
    config: Optional[ConfigParser] = None,
    from_gui: bool = False,
) -> Dict[str, Any]:
    """
    Starts the job attachments upload and handles the progress reporting callback.
    Returns the attachment settings from the upload.
    """

    def _default_update_upload_progress(upload_metadata: ProgressReportMetadata) -> bool:
        return True

    if not upload_progress_callback:
        upload_progress_callback = _default_update_upload_progress

    upload_summary, attachment_settings = asset_manager.upload_assets(
        manifests=manifests,
        on_uploading_assets=upload_progress_callback,
        s3_check_cache_dir=config_file.get_cache_directory(),
    )
    api.get_deadline_cloud_library_telemetry_client(config=config).record_upload_summary(
        upload_summary,
        from_gui=from_gui,
    )

    if upload_summary.total_files > 0:
        print_function_callback("Upload Summary:")
        print_function_callback(textwrap.indent(str(upload_summary), "    "))
    else:
        # Ensure to call the callback once if no files were processed
        upload_progress_callback(
            ProgressReportMetadata(
                status=ProgressStatus.UPLOAD_IN_PROGRESS,
                progress=100,
                transferRate=0,
                progressMessage="No files to upload",
            )
        )

    return attachment_settings.to_dict()


@api.record_success_fail_telemetry_event(metric_name="asset_snapshot")
def _snapshot_attachments(
    snapshot_dir: str,
    asset_manager: S3AssetManager,
    manifests: List[AssetRootManifest],
    print_function_callback: Callable,
    snapshot_progress_callback: Optional[Callable],
    config: Optional[ConfigParser] = None,
    from_gui: bool = False,
) -> Dict[str, Any]:
    """
    Starts the job attachments upload and handles the progress reporting callback.
    Returns the attachment settings from the upload.
    """

    def _default_update_snapshot_progress(upload_metadata: ProgressReportMetadata) -> bool:
        return True

    if not snapshot_progress_callback:
        snapshot_progress_callback = _default_update_snapshot_progress

    upload_summary, attachment_settings = asset_manager.snapshot_assets(
        snapshot_dir=snapshot_dir,
        manifests=manifests,
        on_snapshotting_assets=snapshot_progress_callback,
    )

    if upload_summary.total_files > 0:
        print_function_callback("Snapshot Summary:")
        print_function_callback(textwrap.indent(str(upload_summary), "    "))
    else:
        # Ensure to call the callback once if no files were processed
        snapshot_progress_callback(
            ProgressReportMetadata(
                status=ProgressStatus.UPLOAD_IN_PROGRESS,
                progress=100,
                transferRate=0,
                progressMessage="No files to upload",
            )
        )

    return attachment_settings.to_dict()


def _filter_redundant_known_paths(known_asset_paths: Iterable[str]) -> list[str]:
    """
    Filters out redundant paths from the known asset paths list.

    This algorithm identifies any paths that have a different path as a prefix,
    and removes them from the list. Pseudo-code is:

        1. Sort the paths from shortest to longest, so any prefix of a path has
           to happen before that path.
        2. For each path, split it into parts (i.e. '/mnt/prod/project' becomes
           ['/', 'mnt', 'prod', 'project']), and then insert it part by part into
           a nested dict called dir_tree organized as a TRIE. The value True in the
           TRIE indicates that a path with that as its final part is in the list.
        3. While inserting a path into the TRIE, detect whether another path already
           had a prefix of the parts, and filter out the path when that occurs.
    """
    # This directory tree gets filled with the known asset paths, with
    # a True value as a marker for the last part of already seen paths.
    dir_tree: dict[str, Any] = {}
    filtered_paths: list[str] = []
    # Process the paths from shortest to longest, so that prefixes are always seen first
    for path in sorted(known_asset_paths, key=len):
        parts = Path(path).parts
        current: Optional[dict[str, Any]] = dir_tree
        for part in parts[:-1]:
            # If we see a True value, another path is a prefix so we can skip it.
            if current.get(part) is True:  # type: ignore
                current = None
                break
            current = current.setdefault(part, {})  # type: ignore
        # If we didn't find a prefix or equal path, add this one and mark it in dir_tree
        if current is not None and current.get(parts[-1]) is not True:
            filtered_paths.append(path)
            current[parts[-1]] = True
    return filtered_paths


def _save_debug_snapshot(
    debug_snapshot_dir: str,
    create_job_args: dict,
    asset_manager: S3AssetManager,
    queue: dict,
    storage_profile_id: str,
    storage_profile: Optional[StorageProfile],
):
    # Save the full set of arguments for passing to the deadline.create_job API
    with open(
        os.path.join(debug_snapshot_dir, "create_job_args.json"), "w", encoding="utf-8"
    ) as fh:
        json.dump(create_job_args, fh, indent=1)

    # Add all the parameters, saving JSON values and multi-line strings in files.
    cli_args: list[tuple] = []
    for param_name, param_value in create_job_args.items():
        words = re.findall("(?:^|[A-Z])[a-z]+", param_name)
        kebab_name = "-".join(word.lower() for word in words)
        if isinstance(param_value, (dict, list)):
            param_file = f"{kebab_name}_param.json"
            with open(os.path.join(debug_snapshot_dir, param_file), "w", encoding="utf-8") as fh:
                json.dump(param_value, fh, indent=1)
            cli_args.append((f"--{kebab_name}", f"file://{param_file}"))
        elif isinstance(param_value, str) and "\n" in param_value:
            param_file = f"{kebab_name}_param.data"
            with open(os.path.join(debug_snapshot_dir, param_file), "w", encoding="utf-8") as fh:
                fh.write(param_value)
            cli_args.append((f"--{kebab_name}", f"file://{param_file}"))
        else:
            cli_args.append((f"--{kebab_name}", str(param_value)))

    def write_commands(write_line: Callable, continuation: str):
        if "attachments" in create_job_args:
            for subdir in ("Data", "Manifests"):
                write_line(f"aws s3 cp {continuation}")
                write_line(f"    --recursive {continuation}")
                write_line(f"    ./{subdir} {continuation}")
                write_line(
                    f"    s3://{asset_manager.job_attachment_settings.s3BucketName}/{asset_manager.job_attachment_settings.rootPrefix}/{subdir}"  # type: ignore
                )
                write_line()
        write_line(f"aws deadline create-job {continuation}")
        for param_opts in cli_args[:-1]:
            write_line(f"    {shlex.join(param_opts)} {continuation}")
        write_line(f"    {shlex.join(cli_args[-1])}")

    # Write a shell script that submits the job using AWS CLI commands
    with open(os.path.join(debug_snapshot_dir, "submit_job.sh"), "wb") as sh_fh:

        def sh_line(val: str = ""):
            sh_fh.write(val.encode("utf-8"))  # type: ignore
            sh_fh.write(b"\n")  # type: ignore

        sh_line("#!/bin/sh")
        sh_line("# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.")
        sh_line("set -xeuo pipefail")
        sh_line('cd "$(dirname "$0")"')
        sh_line()
        write_commands(sh_line, "\\")

    # Write a batch file that submits the job using AWS CLI commands
    with open(os.path.join(debug_snapshot_dir, "submit_job.bat"), "wb") as bat_fh:

        def bat_line(val: str = ""):
            bat_fh.write(val.encode("utf-8"))  # type: ignore
            bat_fh.write(b"\r\n")  # type: ignore

        bat_line("REM Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.")
        bat_line('cd /d "%~dp0"')
        bat_line()
        write_commands(bat_line, "^")

    # Write the queue and storage profile resources
    with open(os.path.join(debug_snapshot_dir, "queue.json"), "w") as fh:
        queue_with_str = {
            key: (value.isoformat() if isinstance(value, datetime) else value)
            for key, value in queue.items()
            if key != "ResponseMetadata"
        }
        json.dump(queue_with_str, fh, indent=1)
    if storage_profile_id and storage_profile is not None:
        with open(os.path.join(debug_snapshot_dir, "storage_profile.json"), "w") as fh:
            json.dump(storage_profile.to_dict(), fh, indent=1)
    return None


@api.record_function_latency_telemetry_event()
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
    max_worker_count: Optional[int] = None,
    target_task_run_status: Optional[str] = None,
    require_paths_exist: bool = False,
    submitter_name: Optional[str] = None,
    known_asset_paths: Collection[str] = [],
    debug_snapshot_dir: Optional[str] = None,
    from_gui: bool = False,
    print_function_callback: Callable[[str], None] = print,
    interactive_confirmation_callback: Optional[Callable[[str, bool], bool]] = None,
    hashing_progress_callback: Optional[Callable[[ProgressReportMetadata], bool]] = None,
    upload_progress_callback: Optional[Callable[[ProgressReportMetadata], bool]] = None,
    create_job_result_callback: Optional[Callable[[], bool]] = None,
) -> Optional[str]:
    """
    Creates a job in the farm/queue configured as default for the workstation from the job bundle in the provided directory.

    The return value is the submitted job id except when debug_snapshot_dir is provided. When creating a debug snapshot,
    no job is submitted.

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
        max_worker_count (int, optional): explicit value for the max worker count of the job.
        target_task_run_status (str, optional): explicit value for the target task run status of the job.
                Valid values are "READY" or "SUSPENDED".
        require_paths_exist (bool, optional): Whether to require that all input paths exist.
        submitter_name (str, optional): Name of the application submitting the bundle.
        known_asset_paths (list[str], optional): A list of paths that should not generate
                warnings when outside storage profile locations. Defaults to an empty list.
        debug_snapshot_dir (str, optional): EXPERIMENTAL - A directory in which to save a debug snapshot of the data and commands
                needed to exactly replicate the deadline:CreateJob service API call.
        print_function_callback (Callable str -> None, optional): Callback to print messages produced in this function.
                By default calls print(), Can be replaced by click.echo or a logging function of choice.
        interactive_confirmation_callback (Callable [str, bool] -> bool): Callback arguments are (confirmation_message, default_response).
                This function should present the provided prompt, using default_response as the default value to respond with if the user
                does not make an explicit choice, and return True if the user wants to continue, False to cancel.
        hashing_progress_callback / upload_progress_callback / create_job_result_callback (Callable -> bool):
                Callbacks periodically called while hashing / uploading / waiting for job creation. If returns false,
                the operation will be cancelled. If return true, the operation continues. Default behavior for each
                is to not cancel the operation. hashing_progress_callback and upload_progress_callback both receive
                ProgressReport as a parameter, which can be used for projecting remaining time, as in done in the CLI.
    """

    if not submitter_name:
        submitter_name = "Custom"

    session_context["submitter-name"] = submitter_name

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
    if not debug_snapshot_dir:
        print_function_callback(f"Submitting to Queue: {queue['displayName']}\n")
    else:
        print_function_callback(f"Snapshotting submission to Queue: {queue['displayName']}\n")

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

    # Extend known_asset_paths with all paths that are treated as known. These are
    # paths provided explicitly by the call to submit the job bundle:
    #   * Paths in the known_asset_paths parameter to this function call
    #   * Paths contained inside the job bundle.
    #   * Paths configured in the locally configured storage profile as LOCAL (not SHARED).
    #   * Paths configured in the local config file settings.known_asset_paths
    #   * Paths provided within the job_parameters parameter to this function call
    # Paths that are treated as unknown (unless in one of the above categories). These can be
    # absolute paths referencing anywhere in the file system, not explicitly provided by the call,
    # so require that they be marked as known in the local configuration file or the associated
    # Storage Profile in the AWS account:
    #   * Paths provided in the job bundle via the parameter_values.json/.yaml file
    #   * Paths provided in the job bundle via the asset_references.json/.yaml files
    known_asset_paths = list(known_asset_paths) + [os.path.abspath(job_bundle_dir)]
    # Add the configured storage profile paths
    if storage_profile:
        known_asset_paths.extend(
            [
                fsl.path
                for fsl in storage_profile.fileSystemLocations
                if fsl.type == FileSystemLocationType.LOCAL
            ]
        )
    # Add the configured known asset paths
    configured_known_asset_paths = config_file.get_setting(
        "settings.known_asset_paths", config=config
    ).strip()
    if configured_known_asset_paths:
        known_asset_paths.extend(configured_known_asset_paths.split(os.pathsep))
    # Use the parameter names from job_parameters, but the values from parameters. If a value was provided
    # in job_parameters, it has been applied into parameters and normalized as necessary.
    known_parameter_names = {job_param.get("name") for job_param in job_parameters}
    for job_param in parameters:
        if job_param.get("type") == "PATH" and job_param.get("name") in known_parameter_names:
            job_param_value = job_param.get("value")
            if job_param_value:
                if job_param.get("objectType") == "FILE":
                    # If the job parameter is a file, use its directory as the known path. When collecting
                    # outputs for upload, only that directory is used, not the file path.
                    known_asset_paths.append(os.path.dirname(job_param_value))
                else:
                    known_asset_paths.append(job_param_value)

    # Filter known_asset_paths to remove any paths that have another one as a prefix. This can
    # reduce the amount of processing needed later, and produces a shorter warning message when presenting
    # to users.
    known_asset_paths = _filter_redundant_known_paths(known_asset_paths)

    # Hash and upload job attachments if there are any
    files_processed = False
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
            # Empty directories just become references since the current asset manifest spec
            # version cannot represent them.
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
            # Generate warning message if needed
            asset_path_message, default_prompt_response = _generate_message_for_asset_paths(
                upload_group, storage_profile, known_asset_paths
            )

            if interactive_confirmation_callback is None:
                # In this case, no user prompt can be presented. The result of the function must
                # be the default that would be presented to the interactive prompt.
                print_function_callback(asset_path_message)
                if not default_prompt_response:
                    print_function_callback("\nJob submission canceled (user input not enabled).")
                    raise DeadlineOperationCanceled()
            elif config_file.str2bool(get_setting("settings.auto_accept", config=config)):
                if not default_prompt_response:
                    if from_gui:
                        # In the from_gui case, we present a prompt even though settings.auto_accept is enabled.
                        if not interactive_confirmation_callback(
                            asset_path_message + "Do you wish to proceed?", default_prompt_response
                        ):
                            print_function_callback("Job submission canceled (user input).")
                            raise UserInitiatedCancel()
                    else:
                        # In this case, no user prompt should be presented. The result of the function must
                        # be the default that would be presented to the interactive prompt.
                        print_function_callback(
                            f"{asset_path_message}\nJob submission canceled (settings.auto_accept enabled and there were unknown paths)."
                        )
                        raise DeadlineOperationCanceled()
                else:
                    print_function_callback(asset_path_message)
            else:
                if not interactive_confirmation_callback(
                    asset_path_message + "\nDo you wish to proceed?", default_prompt_response
                ):
                    print_function_callback("Job submission canceled (user input).")
                    raise UserInitiatedCancel()

            _, asset_manifests = _hash_attachments(
                asset_manager=asset_manager,
                asset_groups=upload_group.asset_groups,
                total_input_files=upload_group.total_input_files,
                total_input_bytes=upload_group.total_input_bytes,
                print_function_callback=print_function_callback,
                hashing_progress_callback=hashing_progress_callback,
            )

            if not debug_snapshot_dir:
                attachment_settings = _upload_attachments(  # type: ignore
                    asset_manager,
                    asset_manifests,
                    print_function_callback,
                    upload_progress_callback,
                    from_gui=from_gui,
                )
            else:
                attachment_settings = _snapshot_attachments(  # type: ignore
                    debug_snapshot_dir,
                    asset_manager,
                    asset_manifests,
                    print_function_callback,
                    upload_progress_callback,
                    from_gui=from_gui,
                )

            attachment_settings["fileSystem"] = JobAttachmentsFileSystem(
                job_attachments_file_system
            )
            create_job_args["attachments"] = attachment_settings

            files_processed = True

    if not files_processed:
        # Call each callback once indicating nothing to do.
        if hashing_progress_callback is not None:
            hashing_progress_callback(
                ProgressReportMetadata(
                    status=ProgressStatus.PREPARING_IN_PROGRESS,
                    progress=0,
                    transferRate=0,
                    progressMessage="No files to hash",
                )
            )
        if upload_progress_callback is not None:
            upload_progress_callback(
                ProgressReportMetadata(
                    status=ProgressStatus.UPLOAD_IN_PROGRESS,
                    progress=0,
                    transferRate=0,
                    progressMessage="No files to upload",
                )
            )

    create_job_args.update(app_parameters_formatted)

    if job_parameters_formatted:
        create_job_args["parameters"] = job_parameters_formatted

    if priority is not None:
        create_job_args["priority"] = priority
    if max_worker_count is not None:
        create_job_args["maxWorkerCount"] = max_worker_count
    if max_failed_tasks_count is not None:
        create_job_args["maxFailedTasksCount"] = max_failed_tasks_count
    if max_retries_per_task is not None:
        create_job_args["maxRetriesPerTask"] = max_retries_per_task
    if target_task_run_status is not None:
        create_job_args["targetTaskRunStatus"] = target_task_run_status

    if logging.DEBUG >= logger.getEffectiveLevel():
        logger.debug(json.dumps(create_job_args, indent=1))

    api.get_deadline_cloud_library_telemetry_client().record_event(
        event_type="com.amazon.rum.deadline.submission",
        event_details={"submitter_name": submitter_name},
        from_gui=from_gui,
    )

    if debug_snapshot_dir:
        return _save_debug_snapshot(
            debug_snapshot_dir,
            create_job_args,
            asset_manager,
            queue,
            storage_profile_id,
            storage_profile,
        )

    create_job_response = deadline.create_job(**create_job_args)
    logger.debug("CreateJob Response %r", create_job_response)

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
            event_type="com.amazon.rum.deadline.create_job",
            event_details={"is_success": success},
            from_gui=from_gui,
        )

        if not success:
            raise DeadlineOperationError(status_message)

        print_function_callback("Submitted job bundle:")
        print_function_callback(f"   {job_bundle_dir}")
        print_function_callback(status_message + f"\n{job_id}")

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

    initial_delay = 0.3
    max_delay = 5.0
    timeout_seconds = 300
    creating_statuses = {
        "CREATE_IN_PROGRESS",
    }
    failure_statuses = {"CREATE_FAILED"}

    start_time = time.time()
    delay = initial_delay

    # Initial wait before first attempt
    time.sleep(initial_delay)

    while time.time() - start_time < timeout_seconds:
        if not continue_callback():
            raise CreateJobWaiterCanceled()

        job = deadline_client.get_job(jobId=job_id, queueId=queue_id, farmId=farm_id)

        current_status = job["lifecycleStatus"] if "lifecycleStatus" in job else job["state"]
        if current_status in creating_statuses:
            time.sleep(delay)
            delay = min(delay * 2, max_delay)
        elif current_status in failure_statuses:
            return False, job["lifecycleStatusMessage"]
        else:
            return True, job["lifecycleStatusMessage"]

    raise TimeoutError(
        f"Timed out after {timeout_seconds} seconds while waiting for Job to be created: {job_id}"
    )
