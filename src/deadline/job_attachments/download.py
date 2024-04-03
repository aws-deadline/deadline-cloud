# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Functions for downloading output from the Job Attachment CAS."""
from __future__ import annotations

import concurrent.futures
import io
import os
import re
import time
from collections import defaultdict
from datetime import datetime
from itertools import chain
from logging import Logger, LoggerAdapter, getLogger
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Callable, DefaultDict, List, Optional, Tuple, Union

import boto3
from boto3.s3.transfer import ProgressCallbackInvoker
from botocore.client import BaseClient
from botocore.exceptions import BotoCoreError, ClientError

from .asset_manifests.base_manifest import BaseAssetManifest, BaseManifestPath as RelativeFilePath
from .asset_manifests.hash_algorithms import HashAlgorithm
from .asset_manifests.decode import decode_manifest
from .exceptions import (
    COMMON_ERROR_GUIDANCE_FOR_S3,
    AssetSyncError,
    AssetSyncCancelledError,
    JobAttachmentS3BotoCoreError,
    JobAttachmentsS3ClientError,
    PathOutsideDirectoryError,
    JobAttachmentsError,
    MissingAssetRootError,
)
from .vfs import (
    VFSProcessManager,
    VFS_CACHE_REL_PATH_IN_SESSION,
    VFS_MANIFEST_FOLDER_IN_SESSION,
    VFS_LOGS_FOLDER_IN_SESSION,
    VFS_MANIFEST_FOLDER_PERMISSIONS,
)

from .models import (
    Attachments,
    FileConflictResolution,
    JobAttachmentS3Settings,
    ManifestPathGroup,
)
from .progress_tracker import (
    DownloadSummaryStatistics,
    ProgressReportMetadata,
    ProgressStatus,
    ProgressTracker,
)
from ._aws.aws_clients import (
    get_account_id,
    get_s3_client,
    get_s3_max_pool_connections,
    get_s3_transfer_manager,
)
from .os_file_permission import (
    FileSystemPermissionSettings,
    PosixFileSystemPermissionSettings,
    WindowsFileSystemPermissionSettings,
    _set_fs_group_for_posix,
    _set_fs_permission_for_windows,
)
from ._utils import _is_relative_to, _join_s3_paths

download_logger = getLogger("deadline.job_attachments.download")

S3_DOWNLOAD_MAX_CONCURRENCY = 10


def get_manifest_from_s3(
    manifest_key: str, s3_bucket: str, session: Optional[boto3.Session] = None
) -> BaseAssetManifest:
    s3_client = get_s3_client(session=session)
    try:
        file_buffer = io.BytesIO()
        s3_client.download_fileobj(
            s3_bucket,
            manifest_key,
            file_buffer,
            ExtraArgs={"ExpectedBucketOwner": get_account_id(session=session)},
        )
        byte_value = file_buffer.getvalue()
        string_value = byte_value.decode("utf-8")
        asset_manifest = decode_manifest(string_value)
        file_buffer.close()
        return asset_manifest
    except ClientError as exc:
        status_code = int(exc.response["ResponseMetadata"]["HTTPStatusCode"])
        status_code_guidance = {
            **COMMON_ERROR_GUIDANCE_FOR_S3,
            403: (
                (
                    "Forbidden or Access denied. Please check your AWS credentials, and ensure that "
                    "your AWS IAM Role or User has the 's3:GetObject' permission for this bucket. "
                )
                if "kms:" not in str(exc)
                else (
                    "Forbidden or Access denied. Please check your AWS credentials and Job Attachments S3 bucket "
                    "encryption settings. If a customer-managed KMS key is set, confirm that your AWS IAM Role or "
                    "User has the 'kms:Decrypt' and 'kms:DescribeKey' permissions for the key used to encrypt the bucket."
                )
            ),
            404: "Not found. Please check your bucket name and object key, and ensure that they exist in the AWS account.",
        }
        raise JobAttachmentsS3ClientError(
            action="downloading binary file",
            status_code=status_code,
            bucket_name=s3_bucket,
            key_or_prefix=manifest_key,
            message=f"{status_code_guidance.get(status_code, '')} {str(exc)}",
        ) from exc
    except BotoCoreError as bce:
        raise JobAttachmentS3BotoCoreError(
            action="downloading binary file",
            error_details=str(bce),
        ) from bce
    except Exception as e:
        raise AssetSyncError(e) from e


def _get_output_manifest_prefix(
    s3_settings: JobAttachmentS3Settings,
    farm_id: str,
    queue_id: str,
    job_id: str,
    step_id: Optional[str] = None,
    task_id: Optional[str] = None,
    session_action_id: Optional[str] = None,
) -> str:
    """
    Get full prefix for output manifest with given farm id, queue id, job id, step id and task id
    """
    manifest_prefix: str
    if session_action_id:
        if not task_id or not step_id:
            raise JobAttachmentsError(
                "Session Action ID specified, but no Task ID or Step ID. Job, Step, and Task ID are required to retrieve task outputs."
            )
        manifest_prefix = s3_settings.full_output_prefix(
            farm_id, queue_id, job_id, step_id, task_id, session_action_id
        )
    if task_id:
        if not step_id:
            raise JobAttachmentsError(
                "Task ID specified, but no Step ID. Job, Step, and Task ID are required to retrieve task outputs."
            )
        manifest_prefix = s3_settings.full_task_output_prefix(
            farm_id, queue_id, job_id, step_id, task_id
        )
    elif step_id:
        manifest_prefix = s3_settings.full_step_output_prefix(farm_id, queue_id, job_id, step_id)
    else:
        manifest_prefix = s3_settings.full_job_output_prefix(farm_id, queue_id, job_id)

    # Previous functions don't terminate the prefix with a '/'. So we'll do it here.
    return f"{manifest_prefix}/"


def _get_tasks_manifests_keys_from_s3(
    manifest_prefix: str, s3_bucket: str, session: Optional[boto3.Session] = None
) -> List[str]:
    """
    Returns the keys of all output manifests from the given s3 prefix.
    (Only the manifests that end with the prefix pattern task-*/*_output)
    """
    manifests_keys: List[str] = []
    s3_client = get_s3_client(session=session)
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(
            Bucket=s3_bucket,
            Prefix=manifest_prefix,
        )

        # 1. Find all files that match the pattern: task-{any}/{any}/{any}-output
        task_prefixes = defaultdict(list)
        for page in page_iterator:
            contents = page.get("Contents", None)
            if contents is None:
                raise JobAttachmentsError(
                    f"Unable to find asset manifest in s3://{s3_bucket}/{manifest_prefix}"
                )
            for content in contents:
                if re.search(r"task-.*/.*/.*_output", content["Key"]):
                    parts = content["Key"].split("/")
                    for i, part in enumerate(parts):
                        if "task-" in part:
                            task_folder = "/".join(parts[: i + 1])
                            task_prefixes[task_folder].append(content["Key"])

    except ClientError as exc:
        status_code = int(exc.response["ResponseMetadata"]["HTTPStatusCode"])
        status_code_guidance = {
            **COMMON_ERROR_GUIDANCE_FOR_S3,
            403: (
                "Forbidden or Access denied. Please check your AWS credentials, and ensure that "
                "your AWS IAM Role or User has the 's3:ListBucket' permission for this bucket."
            ),
            404: "Not found. Please ensure that the bucket and key/prefix exists.",
        }
        raise JobAttachmentsS3ClientError(
            action="listing bucket contents",
            status_code=status_code,
            bucket_name=s3_bucket,
            key_or_prefix=manifest_prefix,
            message=f"{status_code_guidance.get(status_code, '')} {str(exc)}",
        ) from exc
    except BotoCoreError as bce:
        raise JobAttachmentS3BotoCoreError(
            action="listing bucket contents",
            error_details=str(bce),
        ) from bce
    except JobAttachmentsError:
        raise  # pass along JobAttachmentsErrors if we get them
    except Exception as e:
        raise AssetSyncError(e) from e

    # 2. Select all files in the last subfolder (alphabetically) under each "task-{any}" folder.
    for task_folder, files in task_prefixes.items():
        last_subfolder = sorted(
            set(f.split("/")[len(task_folder.split("/"))] for f in files), reverse=True
        )[0]
        manifests_keys += [f for f in files if f.startswith(f"{task_folder}/{last_subfolder}/")]

    # Now `manifests_keys` is a list of the keys of files in the last folder (alphabetically) under each "task-" folder.
    return manifests_keys


def get_job_input_paths_by_asset_root(
    s3_settings: JobAttachmentS3Settings,
    attachments: Attachments,
    session: Optional[boto3.Session] = None,
) -> dict[str, ManifestPathGroup]:
    """
    Gets dict of grouped paths of all input files of a given job.
    The grouped paths are separated by asset root.
    Returns a dict of ManifestPathGroups, with the root path as the key.
    """
    inputs: dict[str, ManifestPathGroup] = {}

    for manifest_properties in attachments.manifests:
        if manifest_properties.inputManifestPath:
            key = _join_s3_paths(manifest_properties.inputManifestPath)
            asset_manifest = get_manifest_from_s3(
                manifest_key=key,
                s3_bucket=s3_settings.s3BucketName,
                session=session,
            )

            root_path = manifest_properties.rootPath
            if root_path not in inputs:
                inputs[root_path] = ManifestPathGroup()
            inputs[root_path].add_manifest_to_group(asset_manifest)

    return inputs


def get_job_input_output_paths_by_asset_root(
    s3_settings: JobAttachmentS3Settings,
    attachments: Attachments,
    farm_id: str,
    queue_id: str,
    job_id: str,
    step_id: Optional[str] = None,
    task_id: Optional[str] = None,
    session_action_id: Optional[str] = None,
    session: Optional[boto3.Session] = None,
) -> dict[str, ManifestPathGroup]:
    """
    With given IDs, gets the paths of all input and output files
    of this job. The grouped paths are separated by asset root.
    Returns a dict of ManifestPathGroups, with the root path as the key.
    """
    input_files = get_job_input_paths_by_asset_root(
        s3_settings=s3_settings,
        attachments=attachments,
        session=session,
    )
    output_files = get_job_output_paths_by_asset_root(
        s3_settings=s3_settings,
        farm_id=farm_id,
        queue_id=queue_id,
        job_id=job_id,
        step_id=step_id,
        task_id=task_id,
        session_action_id=session_action_id,
        session=session,
    )

    combined_path_groups: dict[str, ManifestPathGroup] = {}
    for asset_root, path_group in chain(input_files.items(), output_files.items()):
        if asset_root not in combined_path_groups:
            combined_path_groups[asset_root] = path_group
        else:
            combined_path_groups[asset_root].combine_with_group(path_group)

    return combined_path_groups


def download_files_in_directory(
    s3_settings: JobAttachmentS3Settings,
    attachments: Attachments,
    farm_id: str,
    queue_id: str,
    job_id: str,
    directory_path: str,
    local_download_dir: str,
    session: Optional[boto3.Session] = None,
    on_downloading_files: Optional[Callable[[ProgressReportMetadata], bool]] = None,
) -> DownloadSummaryStatistics:
    """
    From a given job's input and output files, downloads all files in
    the given directory path.
    (example of `directory_path`: "inputs/subdirectory1")
    (example of `local_download_dir`: "/home/username")
    """
    all_grouped_paths = get_job_input_output_paths_by_asset_root(
        s3_settings=s3_settings,
        attachments=attachments,
        farm_id=farm_id,
        queue_id=queue_id,
        job_id=job_id,
        session=session,
    )

    # Group by hash algorithm all the files that fall under the directory
    files_to_download: DefaultDict[HashAlgorithm, list[RelativeFilePath]] = DefaultDict(list)
    total_bytes = 0
    total_files = 0
    for path_group in all_grouped_paths.values():
        for hash_alg, path_list in path_group.files_by_hash_alg.items():
            files_list = [file for file in path_list if file.path.startswith(directory_path + "/")]
            files_size = sum([file.size for file in files_list])
            total_bytes += files_size
            total_files += len(files_list)
            files_to_download[hash_alg].extend(files_list)

    # Sets up progress tracker to report download progress back to the caller.
    progress_tracker = ProgressTracker(
        status=ProgressStatus.DOWNLOAD_IN_PROGRESS,
        total_files=total_files,
        total_bytes=total_bytes,
        on_progress_callback=on_downloading_files,
    )

    num_download_workers = _get_num_download_workers()

    start_time = time.perf_counter()

    for hash_alg, file_paths in files_to_download.items():
        downloaded_files_paths = _download_files_parallel(
            file_paths,
            hash_alg,
            num_download_workers,
            local_download_dir,
            s3_settings.s3BucketName,
            s3_settings.full_cas_prefix(),
            progress_tracker=progress_tracker,
        )

    progress_tracker.total_time = time.perf_counter() - start_time

    return progress_tracker.get_download_summary_statistics(
        {local_download_dir: downloaded_files_paths}
    )


def download_file(
    file: RelativeFilePath,
    hash_algorithm: HashAlgorithm,
    local_download_dir: str,
    s3_bucket: str,
    cas_prefix: Optional[str],
    s3_client: Optional[BaseClient] = None,
    session: Optional[boto3.Session] = None,
    modified_time_override: Optional[float] = None,
    progress_tracker: Optional[ProgressTracker] = None,
    file_conflict_resolution: Optional[FileConflictResolution] = FileConflictResolution.CREATE_COPY,
) -> Tuple[int, Optional[Path]]:
    """
    Downloads a file from the S3 bucket to the local directory. `modified_time_override` is ignored if the manifest
    version used supports timestamps.
    Returns a tuple of (size in bytes, filename) of the downloaded file.
    - The file size of 0 means that this file comes from a manifest version that does not provide file sizes.
    - The filename of None indicates that this file has been skipped or has not been downloaded.
    """
    if not s3_client:
        s3_client = get_s3_client(session=session)

    transfer_manager = get_s3_transfer_manager(s3_client=s3_client)

    # The modified time in the manifest is in microseconds, but utime requires the time be expressed in seconds.
    modified_time_override = file.mtime / 1000000  # type: ignore[attr-defined]

    file_bytes = file.size

    # Python will handle the path separator '/' correctly on every platform.
    local_file_name = Path(local_download_dir).joinpath(file.path)

    s3_key = (
        f"{cas_prefix}/{file.hash}.{hash_algorithm.value}"
        if cas_prefix
        else f"{file.hash}.{hash_algorithm.value}"
    )

    # If the file name already exists, resolve the conflict based on the file_conflict_resolution
    if local_file_name.is_file():
        if file_conflict_resolution == FileConflictResolution.SKIP:
            return (file_bytes, None)
        elif file_conflict_resolution == FileConflictResolution.OVERWRITE:
            pass
        elif file_conflict_resolution == FileConflictResolution.CREATE_COPY:
            # This loop resolves filename conflicts by appending " (1)"
            # to the stem of the filename until a unique name is found.
            while local_file_name.is_file():
                local_file_name = local_file_name.parent.joinpath(
                    local_file_name.stem + " (1)" + local_file_name.suffix
                )
        else:
            raise ValueError(
                f"Unknown choice for file conflict resolution: {file_conflict_resolution}"
            )

    local_file_name.parent.mkdir(parents=True, exist_ok=True)

    future: concurrent.futures.Future

    def handler(bytes_downloaded):
        nonlocal progress_tracker
        nonlocal future

        if progress_tracker:
            should_continue = progress_tracker.track_progress_callback(bytes_downloaded)
            if not should_continue:
                future.cancel()

    subscribers = [ProgressCallbackInvoker(handler)]

    future = transfer_manager.download(
        bucket=s3_bucket,
        key=s3_key,
        fileobj=str(local_file_name),
        extra_args={"ExpectedBucketOwner": get_account_id(session=session)},
        subscribers=subscribers,
    )

    try:
        future.result()
    except concurrent.futures.CancelledError as ce:
        if progress_tracker and progress_tracker.continue_reporting is False:
            raise AssetSyncCancelledError("File download cancelled.")
        else:
            raise AssetSyncError("File download failed.", ce) from ce
    except ClientError as exc:

        def process_client_error(exc: ClientError, status_code: int):
            status_code_guidance = {
                **COMMON_ERROR_GUIDANCE_FOR_S3,
                403: (
                    (
                        "Forbidden or Access denied. Please check your AWS credentials, and ensure that "
                        "your AWS IAM Role or User has the 's3:GetObject' permission for this bucket. "
                    )
                    if "kms:" not in str(exc)
                    else (
                        "Forbidden or Access denied. Please check your AWS credentials and Job Attachments S3 bucket "
                        "encryption settings. If a customer-managed KMS key is set, confirm that your AWS IAM Role or "
                        "User has the 'kms:Decrypt' and 'kms:DescribeKey' permissions for the key used to encrypt the bucket."
                    )
                ),
                404: (
                    "Not found. Please check your bucket name and object key, and ensure that they exist in the AWS account."
                ),
            }
            raise JobAttachmentsS3ClientError(
                action="downloading file",
                status_code=status_code,
                bucket_name=s3_bucket,
                key_or_prefix=s3_key,
                message=f"{status_code_guidance.get(status_code, '')} {str(exc)} (Failed to download the file to {str(local_file_name)})",
            ) from exc

        # TODO: Temporary to prevent breaking backwards-compatibility; if file not found, try again without hash alg postfix
        status_code = int(exc.response["ResponseMetadata"]["HTTPStatusCode"])
        if status_code == 404:
            s3_key = s3_key.rsplit(".", 1)[0]
            future = transfer_manager.download(
                bucket=s3_bucket,
                key=s3_key,
                fileobj=str(local_file_name),
                extra_args={"ExpectedBucketOwner": get_account_id(session=session)},
                subscribers=subscribers,
            )
            try:
                future.result()
            except concurrent.futures.CancelledError as ce:
                if progress_tracker and progress_tracker.continue_reporting is False:
                    raise AssetSyncCancelledError("File download cancelled.")
                else:
                    raise AssetSyncError("File download failed.", ce) from ce
            except ClientError as secondExc:
                status_code = int(exc.response["ResponseMetadata"]["HTTPStatusCode"])
                process_client_error(secondExc, status_code)
        else:
            process_client_error(exc, status_code)
    except BotoCoreError as bce:
        raise JobAttachmentS3BotoCoreError(
            action="downloading file",
            error_details=str(bce),
        ) from bce
    except Exception as e:
        raise AssetSyncError(e) from e

    download_logger.debug(f"Downloaded {file.path} to {str(local_file_name)}")
    os.utime(local_file_name, (modified_time_override, modified_time_override))  # type: ignore[arg-type]

    return (file_bytes, local_file_name)


def _download_files_parallel(
    files: List[RelativeFilePath],
    hash_algorithm: HashAlgorithm,
    num_download_workers: int,
    local_download_dir: str,
    s3_bucket: str,
    cas_prefix: Optional[str],
    s3_client: Optional[BaseClient] = None,
    session: Optional[boto3.Session] = None,
    file_mod_time: Optional[float] = None,
    progress_tracker: Optional[ProgressTracker] = None,
    file_conflict_resolution: Optional[FileConflictResolution] = FileConflictResolution.CREATE_COPY,
) -> list[str]:
    """
    Downloads files in parallel using thread pool.
    Returns a list of local paths of downloaded files.
    """
    downloaded_file_names: list[str] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_download_workers) as executor:
        futures = {
            executor.submit(
                download_file,
                file,
                hash_algorithm,
                local_download_dir,
                s3_bucket,
                cas_prefix,
                s3_client,
                session,
                file_mod_time,
                progress_tracker,
                file_conflict_resolution,
            ): file
            for file in files
        }
        # surfaces any exceptions in the thread
        for future in concurrent.futures.as_completed(futures):
            (file_bytes, local_file_name) = future.result()
            if local_file_name:
                downloaded_file_names.append(str(local_file_name.resolve()))
                if progress_tracker:
                    progress_tracker.increase_processed(1, 0)
                    progress_tracker.report_progress()
            else:
                if progress_tracker:
                    progress_tracker.increase_skipped(1, file_bytes)
                    progress_tracker.report_progress()

    # to report progress 100% at the end
    if progress_tracker:
        progress_tracker.report_progress()

    return downloaded_file_names


def download_files(
    files: list[RelativeFilePath],
    hash_algorithm: HashAlgorithm,
    local_download_dir: str,
    s3_settings: JobAttachmentS3Settings,
    session: Optional[boto3.Session] = None,
    progress_tracker: Optional[ProgressTracker] = None,
    file_conflict_resolution: Optional[FileConflictResolution] = FileConflictResolution.CREATE_COPY,
) -> list[str]:
    """
    Downloads all files from the S3 bucket in the Job Attachment settings to the specified directory.
    Returns a list of local paths of downloaded files.
    """
    s3_client = get_s3_client(session=session)
    num_download_workers = _get_num_download_workers()

    file_mod_time: float = datetime.now().timestamp()

    return _download_files_parallel(
        files,
        hash_algorithm,
        num_download_workers,
        local_download_dir,
        s3_settings.s3BucketName,
        s3_settings.full_cas_prefix(),
        s3_client,
        session,
        file_mod_time,
        progress_tracker,
        file_conflict_resolution,
    )


def _get_asset_root_from_s3(
    manifest_key: str, s3_bucket: str, session: Optional[boto3.Session] = None
) -> Optional[str]:
    """
    Gets asset root from metadata (of output manifest) stored in S3.
    If the key "asset-root" does not exist in the metadata, returns None.
    """
    s3_client = get_s3_client(session=session)
    try:
        head = s3_client.head_object(Bucket=s3_bucket, Key=manifest_key)
    except ClientError as exc:
        status_code = int(exc.response["ResponseMetadata"]["HTTPStatusCode"])
        status_code_guidance = {
            **COMMON_ERROR_GUIDANCE_FOR_S3,
            403: (
                "Access denied. Ensure that the bucket is in the AWS account, "
                "and your AWS IAM Role or User has the 's3:ListBucket' permission for this bucket."
            ),
            404: "Not found. Please check your bucket name and object key, and ensure that they exist in the AWS account.",
        }
        raise JobAttachmentsS3ClientError(
            action="checking if object exists",
            status_code=status_code,
            bucket_name=s3_bucket,
            key_or_prefix=manifest_key,
            message=f"{status_code_guidance.get(status_code, '')} {str(exc)}",
        ) from exc
    except BotoCoreError as bce:
        raise JobAttachmentS3BotoCoreError(
            action="checking for the existence of an object in the S3 bucket",
            error_details=str(bce),
        ) from bce
    except Exception as e:
        raise AssetSyncError(e) from e

    return head["Metadata"].get("asset-root", None)


def get_job_output_paths_by_asset_root(
    s3_settings: JobAttachmentS3Settings,
    farm_id: str,
    queue_id: str,
    job_id: str,
    step_id: Optional[str] = None,
    task_id: Optional[str] = None,
    session_action_id: Optional[str] = None,
    session: Optional[boto3.Session] = None,
) -> dict[str, ManifestPathGroup]:
    """
    Gets dict of grouped paths of all output files of a given job.
    The grouped paths are separated by asset root.
    Returns a dict of ManifestPathGroups, with the root path as the key.
    """
    output_manifests_by_root = get_output_manifests_by_asset_root(
        s3_settings, farm_id, queue_id, job_id, step_id, task_id, session_action_id, session=session
    )

    outputs: dict[str, ManifestPathGroup] = {}
    for root, manifests in output_manifests_by_root.items():
        for manifest in manifests:
            if root not in outputs:
                outputs[root] = ManifestPathGroup()
            outputs[root].add_manifest_to_group(manifest)

    return outputs


def get_output_manifests_by_asset_root(
    s3_settings: JobAttachmentS3Settings,
    farm_id: str,
    queue_id: str,
    job_id: str,
    step_id: Optional[str] = None,
    task_id: Optional[str] = None,
    session_action_id: Optional[str] = None,
    session: Optional[boto3.Session] = None,
) -> dict[str, list[BaseAssetManifest]]:
    """
    For a given job/step/task, gets a map from each root path to a corresponding list of
    output manifests.
    """
    outputs: DefaultDict[str, list[BaseAssetManifest]] = DefaultDict(list)
    manifest_prefix: str = _get_output_manifest_prefix(
        s3_settings, farm_id, queue_id, job_id, step_id, task_id, session_action_id
    )
    try:
        manifests_keys: list[str] = _get_tasks_manifests_keys_from_s3(
            manifest_prefix, s3_settings.s3BucketName, session=session
        )
    except JobAttachmentsError:
        return outputs

    for key in manifests_keys:
        asset_manifest = get_manifest_from_s3(
            manifest_key=key,
            s3_bucket=s3_settings.s3BucketName,
            session=session,
        )
        asset_root = _get_asset_root_from_s3(key, s3_settings.s3BucketName, session)
        if not asset_root:
            raise MissingAssetRootError(
                f"Failed to get asset root from metadata of output manifest: {key}"
            )
        outputs[asset_root].append(asset_manifest)

    return outputs


def download_files_from_manifests(
    s3_bucket: str,
    manifests_by_root: dict[str, BaseAssetManifest],
    cas_prefix: Optional[str] = None,
    fs_permission_settings: Optional[FileSystemPermissionSettings] = None,
    session: Optional[boto3.Session] = None,
    on_downloading_files: Optional[Callable[[ProgressReportMetadata], bool]] = None,
    logger: Optional[Union[Logger, LoggerAdapter]] = None,
) -> DownloadSummaryStatistics:
    """
    Given manifests, downloads all files from a CAS in each manifest.

    Args:
        s3_bucket: The name of the S3 bucket.
        manifests_by_root: a map from each local root path to a corresponding list of tuples of manifest contents and their path.
        cas_prefix: The CAS prefix of the files.
        session: The boto3 session to use.
        on_downloading_files: a callback to be called to periodically report progress to the caller.
            The callback returns True if the operation should continue as normal, or False to cancel.

    Returns:
        The download summary statistics.
    """
    s3_client = get_s3_client(session=session)
    num_download_workers = _get_num_download_workers()
    file_mod_time = datetime.now().timestamp()

    # Sets up progress tracker to report download progress back to the caller.
    total_size = 0
    total_files = 0
    for manifest in manifests_by_root.values():
        total_files += len(manifest.paths)
        total_size += manifest.totalSize  # type: ignore[attr-defined]
    progress_tracker = ProgressTracker(
        status=ProgressStatus.DOWNLOAD_IN_PROGRESS,
        total_files=total_files,
        total_bytes=total_size,
        on_progress_callback=on_downloading_files,
        logger=logger,
    )
    start_time = time.perf_counter()

    downloaded_files_paths_by_root: DefaultDict[str, list[str]] = DefaultDict(list)

    for local_download_dir, manifest in manifests_by_root.items():
        downloaded_files_paths = _download_files_parallel(
            manifest.paths,
            manifest.hashAlg,
            num_download_workers,
            local_download_dir,
            s3_bucket,
            cas_prefix,
            s3_client,
            session,
            file_mod_time,
            progress_tracker=progress_tracker,
        )

        if fs_permission_settings is not None:
            _set_fs_group(
                file_paths=downloaded_files_paths,
                local_root=local_download_dir,
                fs_permission_settings=fs_permission_settings,
            )

        downloaded_files_paths_by_root[local_download_dir].extend(downloaded_files_paths)

    progress_tracker.total_time = time.perf_counter() - start_time
    return progress_tracker.get_download_summary_statistics(downloaded_files_paths_by_root)


def _get_num_download_workers() -> int:
    """
    Determines the max number of thread workers for downloading multiple files in parallel,
    based on the allowed S3 max pool connections size. If the max worker count is calculated
    to be 0 due to a small pool connections size limit, it returns 1.
    """
    num_download_workers = int(get_s3_max_pool_connections() / S3_DOWNLOAD_MAX_CONCURRENCY)
    if num_download_workers <= 0:
        # This can result in triggering "Connection pool is full" warning messages during downloads.
        num_download_workers = 1
    return num_download_workers


def _set_fs_group(
    file_paths: list[str],
    local_root: str,
    fs_permission_settings: FileSystemPermissionSettings,
) -> None:
    """
    Sets file system group ownership and permissions for all files and directories
    in the given paths, starting from root. It is expected that all `file_paths`
    point to files, not directories.

    Raises:
        TypeError: If the `fs_permission_settings` are not specific to the underlying OS.
    """
    if os.name == "posix":
        if not isinstance(fs_permission_settings, PosixFileSystemPermissionSettings):
            raise TypeError(
                "The file system permission settings must be specific to Posix-based system."
            )
        _set_fs_group_for_posix(
            file_paths=file_paths,
            local_root=local_root,
            fs_permission_settings=fs_permission_settings,
        )
    else:  # if os.name is not "posix"
        if not isinstance(fs_permission_settings, WindowsFileSystemPermissionSettings):
            raise TypeError("The file system permission settings must be specific to Windows.")
        _set_fs_permission_for_windows(
            file_paths=file_paths,
            local_root=local_root,
            fs_permission_settings=fs_permission_settings,
        )


def merge_asset_manifests(manifests: list[BaseAssetManifest]) -> BaseAssetManifest | None:
    """Merge files from multiple manifests into a single list, ensuring that each filename
    is unique by keeping the one from the last encountered manifest. (Thus, the steps'
    outputs are downloaded over the input job attachments.)

    Args:
        manifests (list[AssetManifest]): A list of manifests to be merged.

    Raises:
        NotImplementedError: When two manifests have different hash algorithms.  All manifests must use the same hash algorithm.

    Returns:
        AssetManifest | None: A single manifest containing the merged paths of all provided manifests or None if no manifests were provided
    """
    if len(manifests) == 0:
        return None
    elif len(manifests) == 1:
        return manifests[0]

    first_manifest = manifests[0]

    hash_alg: HashAlgorithm = first_manifest.hashAlg
    merged_paths: dict[str, RelativeFilePath] = dict()
    total_size: int = 0

    # Loop each manifest
    for manifest in manifests:
        if manifest.hashAlg != hash_alg:
            raise NotImplementedError(
                f"Merging manifests with different hash algorithms is not supported.  {manifest.hashAlg.value} does not match {hash_alg.value}"
            )

        for path in manifest.paths:
            merged_paths[path.path] = path

    manifest_args: dict[str, Any] = {
        "hash_alg": hash_alg,
        "paths": list(merged_paths.values()),
    }

    total_size = sum([path.size for path in merged_paths.values()])  # type: ignore
    manifest_args["total_size"] = total_size

    output_manifest: BaseAssetManifest = first_manifest.__class__(**manifest_args)

    return output_manifest


def _write_manifest_to_temp_file(manifest: BaseAssetManifest, dir: Path) -> str:
    with NamedTemporaryFile(
        suffix=".json", prefix="deadline-merged-manifest-", delete=False, mode="w", dir=dir
    ) as file:
        file.write(manifest.encode())
        return file.name


def _read_manifest_file(input_manifest_path: Path):
    """
    Given a manifest path, open the file at that location and decode
    Args:
        input_manifest_path: Path to manifest
    Returns:
        BaseAssetManifest : Single decoded manifest
    """
    with open(input_manifest_path) as input_manifest_file:
        return decode_manifest(input_manifest_file.read())


def handle_existing_vfs(
    manifest: BaseAssetManifest, session_dir: Path, mount_point: str, os_user: str
) -> BaseAssetManifest:
    """
    Combines provided manifest with the input manifest of the running VFS at the
    given mount_point if it exists. Then kills the running process at that mount so
    it can be replaced

    Args:
        manifests (BaseAssetManifest): The manifest for the new inputs to be mounted
        mount_point (str): The local directory where the manifest is to be mounted
        os_user: the user running the job.
    Returns:
        BaseAssetManifest : A single manifest containing the merged paths or the original manifest
    """
    if not VFSProcessManager.is_mount(mount_point):
        return manifest

    input_manifest_path: Optional[Path] = VFSProcessManager.get_manifest_path_for_mount(
        session_dir=session_dir, mount_point=mount_point
    )
    if input_manifest_path is not None:
        input_manifest = _read_manifest_file(input_manifest_path)

        merged_input_manifest: Optional[BaseAssetManifest] = merge_asset_manifests(
            [input_manifest, manifest]
        )
        manifest = merged_input_manifest if merged_input_manifest is not None else manifest
    else:
        download_logger.error(f"input manifest not found for mount at {mount_point}")
        return manifest

    VFSProcessManager.kill_process_at_mount(
        session_dir=session_dir, mount_point=mount_point, os_user=os_user
    )

    return manifest


def mount_vfs_from_manifests(
    s3_bucket: str,
    manifests_by_root: dict[str, BaseAssetManifest],
    boto3_session: boto3.Session,
    session_dir: Path,
    os_env_vars: dict[str, str],
    fs_permission_settings: FileSystemPermissionSettings,
    cas_prefix: Optional[str] = None,
) -> None:
    """
    Given manifests, downloads all files from a CAS in those manifests.

    Args:
        s3_bucket: The name of the S3 bucket.
        manifests_by_root: a map from each local root path to a corresponding list of tuples of manifest contents and their path.
        boto3_session: The boto3 session to use.
        session_dir: the directory that the session is going to use.
        os_user: the user running the job.
        os_group: the group of the user running the job
        os_env_vars: environment variables to set for launched subprocesses
        cas_prefix: The CAS prefix of the files.

    Returns:
        None
    """
    if not isinstance(fs_permission_settings, PosixFileSystemPermissionSettings):
        raise TypeError("VFS can only be mounted from manifests on posix file systems.")
    vfs_cache_dir: Path = session_dir / VFS_CACHE_REL_PATH_IN_SESSION
    asset_cache_hash_path: Path = vfs_cache_dir
    if cas_prefix is not None:
        asset_cache_hash_path = vfs_cache_dir / cas_prefix
        _ensure_paths_within_directory(str(vfs_cache_dir), [str(asset_cache_hash_path)])

    asset_cache_hash_path.mkdir(parents=True, exist_ok=True)

    _set_fs_group([str(asset_cache_hash_path)], str(vfs_cache_dir), fs_permission_settings)

    manifest_dir: Path = session_dir / VFS_MANIFEST_FOLDER_IN_SESSION
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_dir_permissions = VFS_MANIFEST_FOLDER_PERMISSIONS
    manifest_dir_permissions.os_user = fs_permission_settings.os_user
    manifest_dir_permissions.os_group = fs_permission_settings.os_group

    _set_fs_group([str(manifest_dir)], str(manifest_dir), manifest_dir_permissions)

    vfs_logs_dir: Path = session_dir / VFS_LOGS_FOLDER_IN_SESSION
    vfs_logs_dir.mkdir(parents=True, exist_ok=True)

    _set_fs_group([str(vfs_logs_dir)], str(vfs_logs_dir), fs_permission_settings)

    for mount_point, manifest in manifests_by_root.items():
        # Validate the file paths to see if they are under the given download directory.
        _ensure_paths_within_directory(
            mount_point, [path.path for path in manifest.paths]  # type: ignore
        )
        final_manifest: BaseAssetManifest = handle_existing_vfs(
            manifest=manifest,
            session_dir=session_dir,
            mount_point=mount_point,
            os_user=fs_permission_settings.os_user,
        )

        # Write out a temporary file with the contents of the newly merged manifest
        manifest_path: str = _write_manifest_to_temp_file(final_manifest, dir=manifest_dir)

        vfs_manager: VFSProcessManager = VFSProcessManager(
            s3_bucket,
            boto3_session.region_name,
            manifest_path,
            mount_point,
            fs_permission_settings.os_user,
            os_env_vars,
            getattr(fs_permission_settings, "os_group", ""),
            cas_prefix,
            str(vfs_cache_dir),
        )
        vfs_manager.start(session_dir=session_dir)


def _ensure_paths_within_directory(root_path: str, paths_relative_to_root: list[str]) -> None:
    """
    Validates the given paths to ensure that they are within the given root path.
    If the root path is not an absolute path, raises a ValueError.
    If any path is not under the root directory, raises an PathOutsideDirectoryError.
    """
    if not Path(root_path).is_absolute():
        raise ValueError(f"The provided root path is not an absolute path: {root_path}")

    for path in paths_relative_to_root:
        resolved_path = Path(root_path, path).resolve()
        if not _is_relative_to(resolved_path, Path(root_path).resolve()):
            raise PathOutsideDirectoryError(
                f"The provided path is not under the root directory: {path}"
            )
    return


class OutputDownloader:
    """
    Handler for downloading all output files from the given job, with optional step and task-level granularity.
    If no session is provided the default credentials path will be used, see:
    https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#configuring-credentials

    TODO: The download location is OS-specific to the *submitting machine* matching
    the profile of job["attachments"]["submissionProfileName"]. The OS
    of the *downloading machine* might be different, so we need to check that
    and apply path mapping rules in that case.
    """

    def __init__(
        self,
        s3_settings: JobAttachmentS3Settings,
        farm_id: str,
        queue_id: str,
        job_id: str,
        step_id: Optional[str] = None,
        task_id: Optional[str] = None,
        session_action_id: Optional[str] = None,
        session: Optional[boto3.Session] = None,
    ) -> None:
        self.s3_settings = s3_settings
        self.session = session
        self.outputs_by_root = get_job_output_paths_by_asset_root(
            s3_settings=s3_settings,
            farm_id=farm_id,
            queue_id=queue_id,
            job_id=job_id,
            step_id=step_id,
            task_id=task_id,
            session_action_id=session_action_id,
            session=session,
        )

    def get_output_paths_by_root(self) -> dict[str, list[str]]:
        """
        Returns a dict of asset root paths to lists of output paths.
        """
        output_paths_by_root: dict[str, list[str]] = {}
        for root, path_group in self.outputs_by_root.items():
            output_paths_by_root[root] = path_group.get_all_paths()
        return output_paths_by_root

    def set_root_path(self, original_root: str, new_root: str) -> None:
        """
        Changes the root path for downloading output files, (which is the root path
        saved in the S3 metadata for the output manifest by default,) with a custom path.
        (It will store the new root path as an absolute path.)
        """
        # Need to use absolute to not resolve symlinks, but need normpath to get rid of relative paths, i.e. '..'
        new_root = str(os.path.normpath(Path(new_root).absolute()))

        if original_root not in self.outputs_by_root:
            raise ValueError(f"The root path {original_root} was not found in output manifests.")

        if new_root == original_root:
            return

        if new_root in self.outputs_by_root:
            # If the new_root already exists, and the file path in the original_root already exists
            # among the file paths of the new_root, then prefix the file path with the original_root path.
            # This is to avoid duplicate file paths in the new_root.
            paths_in_new_root = self.outputs_by_root[new_root].get_all_paths()
            for manifest_paths in self.outputs_by_root[original_root].files_by_hash_alg.values():
                for manifest_path in manifest_paths:
                    if manifest_path.path in paths_in_new_root:
                        new_name_prefix = (
                            original_root.replace("/", "_").replace("\\", "_").replace(":", "_")
                        )
                        manifest_path.path = str(
                            Path(manifest_path.path).with_name(
                                f"{new_name_prefix}_{manifest_path.path}"
                            )
                        )
            self.outputs_by_root[new_root].combine_with_group(self.outputs_by_root[original_root])
            del self.outputs_by_root[original_root]
        else:
            self.outputs_by_root = {
                key if key != original_root else new_root: value
                for key, value in self.outputs_by_root.items()
            }

    def download_job_output(
        self,
        file_conflict_resolution: Optional[
            FileConflictResolution
        ] = FileConflictResolution.CREATE_COPY,
        on_downloading_files: Optional[Callable[[ProgressReportMetadata], bool]] = None,
    ) -> DownloadSummaryStatistics:
        """
        Downloads outputs files from S3 bucket to the asset root(s).

        Args:
            file_conflict_resolution: resolution method for file conflicts.
            on_downloading_files: a callback to be called to periodically report progress to the caller.
                The callback returns True if the operation should continue as normal, or False to cancel.

        Returns:
            The download summary statistics
        """
        # Sets up progress tracker to report download progress back to the caller.
        total_bytes: int = 0
        total_files: int = 0
        for path_group in self.outputs_by_root.values():
            total_bytes += path_group.total_bytes
            total_files += len(path_group.get_all_paths())

        progress_tracker = ProgressTracker(
            status=ProgressStatus.DOWNLOAD_IN_PROGRESS,
            total_files=total_files,
            total_bytes=total_bytes,
            on_progress_callback=on_downloading_files,
        )

        start_time = time.perf_counter()
        downloaded_files_paths_by_root: DefaultDict[str, list[str]] = DefaultDict(list)

        try:
            for root, output_path_group in self.outputs_by_root.items():
                for hash_alg, path_list in output_path_group.files_by_hash_alg.items():
                    # Validate the file paths to see if they are under the given download directory.
                    _ensure_paths_within_directory(root, [file.path for file in path_list])

                    downloaded_files_paths = download_files(
                        files=path_list,
                        hash_algorithm=hash_alg,
                        local_download_dir=root,
                        s3_settings=self.s3_settings,
                        session=self.session,
                        progress_tracker=progress_tracker,
                        file_conflict_resolution=file_conflict_resolution,
                    )
                    downloaded_files_paths_by_root[root].extend(downloaded_files_paths)
        except AssetSyncCancelledError:
            downloaded_files = progress_tracker.processed_files
            raise AssetSyncCancelledError(
                "Download cancelled. "
                f"(Downloaded {downloaded_files} file{'' if downloaded_files == 1 else 's'} before cancellation.)"
            )

        progress_tracker.total_time = time.perf_counter() - start_time

        return progress_tracker.get_download_summary_statistics(downloaded_files_paths_by_root)
