# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Functions for downloading output from the Job Attachment CAS."""
from __future__ import annotations

import concurrent.futures
import os
import re
import shutil
import time
from collections import defaultdict
from datetime import datetime
from itertools import chain
from logging import Logger, LoggerAdapter, getLogger
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Callable, DefaultDict, List, Optional, Tuple, Union

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError

from deadline.job_attachments.progress_tracker import (
    DownloadSummaryStatistics,
    ProgressReportMetadata,
    ProgressStatus,
    ProgressTracker,
)

from .asset_manifests.base_manifest import BaseAssetManifest, BaseManifestPath as RelativeFilePath
from .asset_manifests.decode import decode_manifest
from ._aws.aws_clients import get_account_id, get_s3_client
from .exceptions import (
    COMMON_ERROR_GUIDANCE_FOR_S3,
    AssetSyncCancelledError,
    JobAttachmentsS3ClientError,
    PathOutsideDirectoryError,
    JobAttachmentsError,
    MissingAssetRootError,
)
from .fus3 import Fus3ProcessManager
from .models import JobAttachmentS3Settings, Attachments
from ._utils import (
    FileConflictResolution,
    FileSystemPermissionSettings,
    _is_relative_to,
    _join_s3_paths,
)

download_logger = getLogger("deadline.job_attachments.download")


def get_manifest_from_s3(
    manifest_key: str, s3_bucket: str, session: Optional[boto3.Session] = None
) -> str:
    s3_client = get_s3_client(session=session)
    try:
        with NamedTemporaryFile(suffix=".json", prefix="deadline-manifest-", delete=False) as file:
            s3_client.download_fileobj(
                s3_bucket,
                manifest_key,
                file,
                ExtraArgs={"ExpectedBucketOwner": get_account_id(session=session)},
            )
            return file.name
    except ClientError as exc:
        status_code = int(exc.response["ResponseMetadata"]["HTTPStatusCode"])
        status_code_guidance = {
            **COMMON_ERROR_GUIDANCE_FOR_S3,
            403: (
                "Forbidden or Access denied. Please check your AWS credentials, and ensure that "
                "your AWS IAM Role or User has the 's3:GetObject' permission for this bucket."
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
    (Only the manifests that end with the prefix pattern task-*/*_output.*)
    """
    manifests_keys: List[str] = []
    s3_client = get_s3_client(session=session)
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(
            Bucket=s3_bucket,
            Prefix=manifest_prefix,
        )

        # 1. Find all files that match the pattern: task-{any}/{any}/{any}-output.{any}
        task_prefixes = defaultdict(list)
        for page in page_iterator:
            contents = page.get("Contents", None)
            if contents is None:
                raise JobAttachmentsError(
                    f"Unable to find asset manifest in s3://{s3_bucket}/{manifest_prefix}"
                )
            for content in contents:
                if re.search(r"task-.*/.*/.*_output\..*", content["Key"]):
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
) -> Tuple[int, dict[str, list[RelativeFilePath]]]:
    """
    Gets lists of paths of all asset (input) files attached to a given job.
    The lists are separated by asset root.
    Returns a tuple of (total size of assets in bytes, lists of assets)
    """
    assets: DefaultDict[str, list[RelativeFilePath]] = DefaultDict(list)
    total_bytes = 0

    for manifest_properties in attachments.manifests:
        if manifest_properties.inputManifestPath:
            paths_list = assets[manifest_properties.rootPath]
            key = _join_s3_paths(manifest_properties.inputManifestPath)

            manifest = get_manifest_from_s3(
                manifest_key=key,
                s3_bucket=s3_settings.s3BucketName,
                session=session,
            )
            with open(manifest) as manifest_file:
                asset_manifest = decode_manifest(manifest_file.read())
                paths_list.extend(asset_manifest.paths)
                total_bytes += asset_manifest.totalSize  # type: ignore[attr-defined]

    return (total_bytes, assets)


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
) -> Tuple[int, dict[str, list[RelativeFilePath]]]:
    """
    With given IDs, gets the paths of all input and output files
    of this job. The lists are separated by asset root.
    Returns a tuple of (total size of files in bytes, lists of files)
    """
    (total_input_bytes, input_files) = get_job_input_paths_by_asset_root(
        s3_settings=s3_settings,
        attachments=attachments,
        session=session,
    )
    (total_output_bytes, output_files) = get_job_output_paths_by_asset_root(
        s3_settings=s3_settings,
        farm_id=farm_id,
        queue_id=queue_id,
        job_id=job_id,
        step_id=step_id,
        task_id=task_id,
        session_action_id=session_action_id,
        session=session,
    )

    paths_hashes: DefaultDict[str, list[RelativeFilePath]] = defaultdict(list)
    for asset_root, files in chain(input_files.items(), output_files.items()):
        paths_hashes[asset_root].extend(files)
    return (total_input_bytes + total_output_bytes, paths_hashes)


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
    (_, all_paths_hashes) = get_job_input_output_paths_by_asset_root(
        s3_settings=s3_settings,
        attachments=attachments,
        farm_id=farm_id,
        queue_id=queue_id,
        job_id=job_id,
        session=session,
    )

    files_to_download: list[RelativeFilePath] = []
    total_bytes = 0
    for files in all_paths_hashes.values():
        files_list = [file for file in files if file.path.startswith(directory_path + "/")]
        files_size = sum([file.size for file in files_list])
        total_bytes += files_size
        files_to_download.extend(files_list)

    # Sets up progress tracker to report download progress back to the caller.
    progress_tracker = ProgressTracker(
        status=ProgressStatus.DOWNLOAD_IN_PROGRESS,
        total_files=len(files_to_download),
        total_bytes=total_bytes,
        on_progress_callback=on_downloading_files,
    )

    start_time = time.perf_counter()

    downloaded_files_paths = _download_files_parallel(
        files_to_download,
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
    # If it's cancelled, raise an AssetSyncCancelledError.
    if progress_tracker and not progress_tracker.continue_reporting:
        raise AssetSyncCancelledError(
            "File download cancelled.", progress_tracker.get_summary_statistics()
        )

    if not s3_client:
        s3_client = get_s3_client(session=session)

    #  The modified time in the manifest is in microseconds, but utime requires the time be expressed in seconds.
    modified_time_override = file.mtime / 1000000  # type: ignore[attr-defined]

    file_bytes = file.size

    # Python will handle the path seperator '/' correctly on every platform.
    local_file_name = Path(local_download_dir).joinpath(file.path)

    s3_key = f"{cas_prefix}/{file.hash}" if cas_prefix else file.hash

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
    try:
        s3_client.download_file(
            s3_bucket,
            s3_key,
            str(local_file_name),
            ExtraArgs={"ExpectedBucketOwner": get_account_id(session=session)},
            Callback=_progress_logger(
                file_bytes,
                progress_tracker.track_progress_callback if progress_tracker else None,
            ),
        )
    except ClientError as exc:
        status_code = int(exc.response["ResponseMetadata"]["HTTPStatusCode"])
        status_code_guidance = {
            **COMMON_ERROR_GUIDANCE_FOR_S3,
            403: (
                "Forbidden or Access denied. Please check your AWS credentials, or ensure that "
                "your AWS IAM Role or User has the 's3:GetObject' permission for this bucket."
            ),
            404: "Not found. Please check your bucket name and object key, and ensure that they exist in the AWS account.",
        }
        raise JobAttachmentsS3ClientError(
            action="downloading file",
            status_code=status_code,
            bucket_name=s3_bucket,
            key_or_prefix=s3_key,
            message=f"{status_code_guidance.get(status_code, '')} {str(exc)}",
        ) from exc

    download_logger.debug(f"Downloaded {file.path} to {str(local_file_name)}")
    os.utime(local_file_name, (modified_time_override, modified_time_override))  # type: ignore[arg-type]

    return (file_bytes, local_file_name)


def _progress_logger(
    file_size_in_bytes: int, progress_tracker_callback: Optional[Callable] = None
) -> Callable[[int], None]:
    total_downloaded = 0

    def handler(bytes_downloaded):
        if progress_tracker_callback is None or file_size_in_bytes == 0:
            return

        nonlocal total_downloaded
        total_downloaded += bytes_downloaded
        progress_tracker_callback(bytes_downloaded, total_downloaded == file_size_in_bytes)

    return handler


def _download_files_parallel(
    files: List[RelativeFilePath],
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

    # TODO: tune this. max_worker defaults to 5 * number of processors. We can run into issues here
    # if we thread too aggressively on slower internet connections. So for now let's set it to 5,
    # which would the number of threads with one processor.
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(
                download_file,
                file,
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
                if file_bytes == 0 and progress_tracker:
                    # If the file size is 0, the download progress should be tracked by the number of files.
                    progress_tracker.increase_processed(1, 0)
                    progress_tracker.report_progress()
            else:
                if progress_tracker:
                    progress_tracker.increase_skipped(1, file_bytes)
                    progress_tracker.report_progress()

    # to report progress 100% at the end, and
    # to check if the download was canceled in the middle of processing the last batch of files.
    if progress_tracker:
        progress_tracker.report_progress()
        if not progress_tracker.continue_reporting:
            raise AssetSyncCancelledError(
                "File download cancelled.",
                progress_tracker.get_download_summary_statistics(
                    {local_download_dir: downloaded_file_names}
                ),
            )

    return downloaded_file_names


def download_files(
    files: list[RelativeFilePath],
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

    file_mod_time: float = datetime.now().timestamp()

    return _download_files_parallel(
        files,
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
) -> Tuple[int, dict[str, list[RelativeFilePath]]]:
    """
    Gets a list of paths of all output files of a given job.
    The lists are separated by asset root.
    Returns a tuple of (total size of files in bytes, lists of output files)
    """
    output_manifests_by_root = get_output_manifests_by_asset_root(
        s3_settings, farm_id, queue_id, job_id, step_id, task_id, session_action_id, session=session
    )

    outputs: DefaultDict[str, list[RelativeFilePath]] = DefaultDict(list)
    total_bytes = 0
    for root, manifests in output_manifests_by_root.items():
        # manifest path isn't needed here, so a variable isn't necessary
        for manifest, _ in manifests:
            outputs[root].extend(manifest.paths)
            total_bytes += manifest.totalSize  # type: ignore[attr-defined]

    return (total_bytes, outputs)


def get_output_manifests_by_asset_root(
    s3_settings: JobAttachmentS3Settings,
    farm_id: str,
    queue_id: str,
    job_id: str,
    step_id: Optional[str] = None,
    task_id: Optional[str] = None,
    session_action_id: Optional[str] = None,
    session: Optional[boto3.Session] = None,
) -> dict[str, list[tuple[BaseAssetManifest, str]]]:
    """
    For a given job/step/task, gets a map from each root path to a corresponding list of
    output manifests.
    """
    outputs: DefaultDict[str, list[tuple[BaseAssetManifest, str]]] = DefaultDict(list)
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
        manifest_path = get_manifest_from_s3(
            manifest_key=key,
            s3_bucket=s3_settings.s3BucketName,
            session=session,
        )
        asset_root = _get_asset_root_from_s3(key, s3_settings.s3BucketName, session)
        if not asset_root:
            raise MissingAssetRootError(
                f"Failed to get asset root from metadata of output manifest: {key}"
            )
        with open(manifest_path) as manifest_file:
            asset_manifest = decode_manifest(manifest_file.read())
        outputs[asset_root].append((asset_manifest, manifest_path))

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


def _set_fs_group(
    file_paths: list[str],
    local_root: str,
    fs_permission_settings: FileSystemPermissionSettings,
) -> None:
    """
    Sets file system group ownership and permissions for all files and directories
    in the given paths, starting from root. It is expected that all `file_paths`
    point to files, not directories.
    """
    os_group = fs_permission_settings.os_group
    dir_mode = fs_permission_settings.dir_mode
    file_mode = fs_permission_settings.file_mode

    # Initialize set to track changed path
    dir_paths_to_change_fs_group = set()

    # 1. Set group ownership and permissions for each file.
    for file_path in file_paths:
        # The file path must be relative to the root path (ie. local_root).
        if not _is_relative_to(file_path, local_root):
            raise PathOutsideDirectoryError(
                f"The provided path '{file_path}' is not under the root directory: {local_root}"
            )

        shutil.chown(Path(file_path), group=os_group)
        os.chmod(Path(file_path), Path(file_path).stat().st_mode | file_mode)

        # Accumulate unique parent directories for each file
        path_components = Path(file_path).relative_to(local_root).parents
        for path_component in path_components:
            path_to_change = Path(local_root).joinpath(path_component)
            dir_paths_to_change_fs_group.add(path_to_change)

    # 2. Set group ownership and permissions for the directories in the path starting from root.
    for path_to_change in dir_paths_to_change_fs_group:
        shutil.chown(path_to_change, group=os_group)
        os.chmod(path_to_change, path_to_change.stat().st_mode | dir_mode)


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

    hash_alg: str = first_manifest.hashAlg
    merged_paths: dict[str, RelativeFilePath] = dict()
    total_size: int = 0

    # Loop each manifest
    for manifest in manifests:
        if manifest.hashAlg != hash_alg:
            raise NotImplementedError(
                f"Merging manifests with different hash algorithms is not supported.  {manifest.hashAlg} does not match {hash_alg}"
            )

        for path in manifest.paths:
            merged_paths[path.path] = path

    manifest_args: dict[str, Any] = {"hash_alg": hash_alg, "paths": list(merged_paths.values())}

    total_size = sum([path.size for path in merged_paths.values()])  # type: ignore
    manifest_args["total_size"] = total_size

    output_manifest: BaseAssetManifest = first_manifest.__class__(**manifest_args)

    return output_manifest


def write_manifest_to_temp_file(manifest: BaseAssetManifest) -> str:
    with NamedTemporaryFile(
        suffix=".json", prefix="deadline-merged-manifest-", delete=False, mode="w"
    ) as file:
        file.write(manifest.encode())
        return file.name


def mount_vfs_from_manifests(
    s3_bucket: str,
    manifests_by_root: dict[str, BaseAssetManifest],
    boto3_session: boto3.Session,
    session_dir: Path,
    cas_prefix: Optional[str] = None,
) -> None:
    """
    Given manifests, downloads all files from a CAS in those manifests.

    Args:
        s3_bucket: The name of the S3 bucket.
        manifests_by_root: a map from each local root path to a corresponding list of tuples of manifest contents and their path.
        boto3_session: The boto3 session to use.
        session_dir: the directory that the session is going to use.z
        cas_prefix: The CAS prefix of the files.

    Returns:
        None
    """

    for local_download_dir, manifest in manifests_by_root.items():
        # Write out a temporary file with the contents of the newly merged manifest
        manifest_path: str = write_manifest_to_temp_file(manifest)

        # Validate the file paths to see if they are under the given download directory.
        _ensure_paths_within_directory(
            local_download_dir, [path.path for path in manifest.paths]  # type: ignore
        )
        vfs_manager: Fus3ProcessManager = Fus3ProcessManager(
            s3_bucket, boto3_session.region_name, manifest_path, local_download_dir, cas_prefix
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
        (self.total_bytes_to_download, self.outputs_by_root) = get_job_output_paths_by_asset_root(
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
        for root, output_files in self.outputs_by_root.items():
            output_paths_by_root[root] = [output_file.path for output_file in output_files]
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
            paths_in_new_root = {item.path for item in self.outputs_by_root[new_root]}
            for item in self.outputs_by_root[original_root]:
                if item.path in paths_in_new_root:
                    new_name_prefix = (
                        original_root.replace("/", "_").replace("\\", "_").replace(":", "_")
                    )
                    item.path = str(Path(item.path).with_name(f"{new_name_prefix}_{item.path}"))
                self.outputs_by_root[new_root].append(item)
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
        progress_tracker = ProgressTracker(
            status=ProgressStatus.DOWNLOAD_IN_PROGRESS,
            total_files=sum([len(files) for files in self.outputs_by_root.values()]),
            total_bytes=self.total_bytes_to_download,
            on_progress_callback=on_downloading_files,
        )

        start_time = time.perf_counter()
        downloaded_files_paths_by_root: DefaultDict[str, list[str]] = DefaultDict(list)

        for root, output_files in self.outputs_by_root.items():
            # Validate the file paths to see if they are under the given download directory.
            _ensure_paths_within_directory(root, [file.path for file in output_files])

            downloaded_files_paths = download_files(
                files=output_files,
                local_download_dir=root,
                s3_settings=self.s3_settings,
                session=self.session,
                progress_tracker=progress_tracker,
                file_conflict_resolution=file_conflict_resolution,
            )
            downloaded_files_paths_by_root[root].extend(downloaded_files_paths)

        progress_tracker.total_time = time.perf_counter() - start_time

        return progress_tracker.get_download_summary_statistics(downloaded_files_paths_by_root)
