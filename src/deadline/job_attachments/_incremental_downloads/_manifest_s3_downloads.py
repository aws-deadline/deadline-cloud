# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

__all__ = [
    "_add_output_manifests_from_s3",
    "_download_all_manifests_with_absolute_paths",
    "_merge_absolute_path_manifest_list",
    "_download_manifest_paths",
]

from typing import Any, Callable, DefaultDict, Optional
from datetime import datetime, timezone
import re
import os
import concurrent.futures
from threading import Lock
from pathlib import Path

import boto3
from boto3.s3.transfer import ProgressCallbackInvoker
from botocore.client import BaseClient
from botocore.exceptions import BotoCoreError, ClientError

from ..download import (
    _get_output_manifest_prefix,
    _get_tasks_manifests_keys_from_s3,
    _get_asset_root_and_manifest_from_s3_with_last_modified,
    _get_num_download_workers,
    _get_new_copy_file_path,
    S3_DOWNLOAD_MAX_CONCURRENCY,
)
from ..asset_manifests import (
    hash_data as ja_hash_data,
    BaseAssetManifest,
    BaseManifestPath,
    HashAlgorithm,
)
from ..asset_manifests.v2023_03_03.asset_manifest import DEFAULT_HASH_ALG
from ..models import (
    FileConflictResolution,
    JobAttachmentS3Settings,
    S3_MANIFEST_FOLDER_NAME,
)
from ..exceptions import (
    COMMON_ERROR_GUIDANCE_FOR_S3,
    AssetSyncError,
    AssetSyncCancelledError,
    JobAttachmentS3BotoCoreError,
    JobAttachmentsS3ClientError,
    JobAttachmentsError,
)
from .._aws.aws_clients import (
    get_account_id,
    get_s3_client,
    get_s3_transfer_manager,
)
from ..progress_tracker import (
    ProgressTracker,
    ProgressStatus,
    ProgressReportMetadata,
)
from .._utils import _get_long_path_compatible_path


"""
This file contains a forked copy of some functionality from deadline.job_attachments.download,
with its interface refactored to support the incremental download command.

It consists fully of internal-only functionality. We would like to iteratively refine these
interfaces over time, with the goal that we deprecate the current interfaces
in deadline.job_attachments.download and replace them with more general and flexible interfaces.
"""

SESSION_ACTION_ID_FROM_KEY_RE = re.compile(r"(sessionaction-[^/-]+-[^/-]+)/")


def _add_output_manifests_from_s3(
    farm_id: str,
    queue: dict[str, Any],
    job: dict[str, Any],
    boto3_session: boto3.Session,
    session_action_list: list[dict[str, Any]],
):
    """
    This function takes a list of session actions (as returned by boto3 deadline.list_session_actions),
    and for any that lack manifest fields, updates them with values retrieved from S3. While the response
    from Deadline Cloud will always return both outputManifestPath and outputManifestHash, this function
    only populates the outputManifestPath value. The order of the manifests in the list precisely correspond
    to the manifests returned by boto3 deadline.get_job, clients of these APIs can zip() the two
    manifests lists together to get the full set of fields needed for processing.

    * https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/deadline/client/list_session_actions.html
    * https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/deadline/client/get_job.html

    Args:
        farm_id: The farm id.
        queue: The queue as returned by boto3 deadline.get_queue().
        job: The job as returned by boto3 deadline.search_jobs().
        boto3_session: The boto3.Session for accessing AWS.
        session_action_list: A list of session actions to modify by adding the "manifests" field where necessary.
            Its shape is as returned by boto3 deadline.list_session_actions() or deadline.get_session_action().
    """
    # If the job has no job attachments, there's nothing to add
    if "attachments" not in job:
        return

    s3_settings = JobAttachmentS3Settings(**queue["jobAttachmentSettings"])

    # Filter the session action list to exclude any that already contain manifests,
    # then return early if the result is empty.
    session_action_list = [
        session_action
        for session_action in session_action_list
        if "manifests" not in session_action
    ]
    if not session_action_list:
        return

    # Process the job's attachments list to generate the hashes
    job_manifests_length = len(job["attachments"]["manifests"])
    job_indexed_root_path_hash = [
        (
            index,
            ja_hash_data(
                f"{manifest.get('fileSystemLocationName', '')}{manifest['rootPath']}".encode(),
                DEFAULT_HASH_ALG,
            ),
        )
        for index, manifest in enumerate(job["attachments"]["manifests"])
    ]
    # Initialize to empty "manifests" entries
    for session_action in session_action_list:
        session_action["manifests"] = [{}] * job_manifests_length

    # Get all the output manifest keys for all the steps/tasks of the job.
    # TODO: This is very inefficient if the job has lots of tasks, because
    #       the incremental download will generally only use a few of them at a time.
    manifest_prefix: str = _get_output_manifest_prefix(
        s3_settings, farm_id, queue["queueId"], job["jobId"]
    )
    try:
        manifests_keys: list[str] = _get_tasks_manifests_keys_from_s3(
            manifest_prefix,
            s3_settings.s3BucketName,
            session=boto3_session,
            select_latest_per_task=False,
        )
    except JobAttachmentsError as e:
        # If there are no manifests, treat as no data.
        if str(e).startswith("Unable to find asset manifest in"):
            return
        else:
            raise

    # Organize the session actions by session action id, so that we can quickly get
    # to the correct session action from the manifest object key.
    session_actions_by_session_action_id: dict[str, dict[str, Any]] = {
        session_action["sessionActionId"]: session_action for session_action in session_action_list
    }

    manifest_prefix = f"{queue['jobAttachmentSettings']['rootPrefix']}/{S3_MANIFEST_FOLDER_NAME}/"
    for key in manifests_keys:
        # Extract the session action id from the manifest key
        m = SESSION_ACTION_ID_FROM_KEY_RE.search(key)
        if m:
            manifest_session_action_id = m[1]
        else:
            raise RuntimeError(
                f"Job attachments manifest key for job {job['name']} ({job['jobId']}) lacks a session action id"
            )
        # Loop through all the manifests to see whether the hash of the rootPath is in the key,
        # in order to determine which position in the manifests list this key should get set in.
        manifests_index = None
        for index, root_path_hash in job_indexed_root_path_hash:
            if root_path_hash in key:
                manifests_index = index
                break
        if manifests_index is None:
            root_path_hashes = [hash for _, hash in job_indexed_root_path_hash]
            raise RuntimeError(
                f"Job attachments manifest key for job {job['name']} ({job['jobId']}) does not contain any of the rootPath hashes {', '.join(root_path_hashes)}: {key}"
            )
        # If this session action is in the list, add this key to it
        session_action_for_key = session_actions_by_session_action_id.get(
            manifest_session_action_id
        )
        if session_action_for_key is not None:
            # This is equivalent to "output_manifest_path = key.removeprefix(manifest_prefix)" to
            # retain support for Python 3.8 which does not support str.removeprefix.
            output_manifest_path = key
            if output_manifest_path.startswith(manifest_prefix):
                output_manifest_path = output_manifest_path[len(manifest_prefix) :]
            session_action_for_key["manifests"][manifests_index] = {
                "outputManifestPath": output_manifest_path,
            }


def _download_manifest_and_make_paths_absolute(
    index: int,
    queue: dict[str, Any],
    root_path: str,
    manifest_s3_key: str,
    boto3_session_for_s3: boto3.Session,
    output_manifests: list,
):
    """
    Downloads the specified manifest, makes all its paths absolute using root_path,
    and then places it in output_manifests[index].
    """
    # Download the manifest
    _, last_modified, manifest = _get_asset_root_and_manifest_from_s3_with_last_modified(
        manifest_s3_key, queue["jobAttachmentSettings"]["s3BucketName"], boto3_session_for_s3
    )
    # Convert all the manifest paths to have absolute normalized local paths
    for manifest_path in manifest.paths:
        manifest_path.path = os.path.normpath(os.path.join(root_path, manifest_path.path))
        # TODO: Apply path mapping rules to manifest_path.path right here
    output_manifests[index] = (last_modified, manifest)


def _get_manifests_to_download(
    job_attachments_root_prefix: str,
    download_candidate_jobs: dict[str, dict[str, Any]],
    job_sessions: dict[str, list],
) -> list[tuple[str, str]]:
    """
    Collect a list of (rootPath, manifest_s3_key) tuples for all the job attachments that need to be downloaded.

    Args:
        job_attachments_root_prefix: The queue.jobAttachmentSettings.rootPrefix field from the Deadline
            Cloud queue.
        download_candidate_jobs: A mapping from job id to jobs as returned by deadline.search_jobs.
        job_sessions: Contains each job's sessions and session actions, structured as job_sessions[job_id][session_index]["sessionActions"][session_action_index].
                      See the function _get_job_sessions for more details.

    Returns:
        A list of (rootPath, manifest_s3_key) tuples for the manifest objects that need to be downloaded.
    """
    manifests_to_download: list[tuple[str, str]] = []
    for job_id, session_list in job_sessions.items():
        job = download_candidate_jobs[job_id]
        for session in session_list:
            for session_action in session.get("sessionActions", []):
                # The manifests lists from the job and session action correspond, so we can zip them
                # together to attach the root path with the S3 manifest key
                for job_manifest, session_action_manifest in zip(
                    job["attachments"]["manifests"], session_action["manifests"]
                ):
                    if "outputManifestPath" in session_action_manifest:
                        manifests_to_download.append(
                            (
                                job_manifest["rootPath"],
                                "/".join(
                                    [
                                        job_attachments_root_prefix,
                                        S3_MANIFEST_FOLDER_NAME,
                                        session_action_manifest["outputManifestPath"],
                                    ]
                                ),
                            )
                        )
    return manifests_to_download


def _download_all_manifests_with_absolute_paths(
    queue: dict[str, Any],
    download_candidate_jobs: dict[str, dict[str, Any]],
    job_sessions: dict[str, list],
    boto3_session_for_s3: boto3.Session,
    print_function_callback: Callable[[str], None] = lambda msg: None,
) -> list[tuple[datetime, BaseAssetManifest]]:
    """
    Downloads all the manifest files that are in the session_actions of job_sessions, and uses the rootPath
    values taken from the job to make all the paths in the manifest absolute.

    Args:
        queue: The Deadline Cloud queue as returned by deadline.get_queue.
        download_candidate_jobs: A mapping from job id to jobs as returned by deadline.search_jobs.
        job_sessions: Contains each job's sessions and session actions, structured as job_sessions[job_id][session_index]["sessionActions"][session_action_index].
                      See the function _get_job_sessions for more details.
        boto3_session_for_s3: The boto3.Session to use for accessing S3.
        print_function_callback: Callback for printing output to the terminal or log.

    Returns:
        A list of BaseAssetManifest objects containing local absolute file paths sorted by the last_modified timestamp.
    """
    # Get the list of (rootPath, manifest_s3_key) tuples to download from S3.
    manifests_to_download: list[tuple[str, str]] = _get_manifests_to_download(
        queue["jobAttachmentSettings"]["rootPrefix"], download_candidate_jobs, job_sessions
    )

    print_function_callback(f"Downloading {len(manifests_to_download)} asset manifests from S3...")
    start_time = datetime.now(tz=timezone.utc)

    # Download all the manifest files from S3, and make the paths in the manifests absolute local paths
    # by joining with the root path and normalizing
    downloaded_manifests: list = [None] * len(manifests_to_download)

    max_workers = S3_DOWNLOAD_MAX_CONCURRENCY
    print_function_callback(f"Using {max_workers} threads")
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for index, (root_path, manifest_s3_key) in enumerate(manifests_to_download):
            futures.append(
                executor.submit(
                    _download_manifest_and_make_paths_absolute,
                    index,
                    queue,
                    root_path,
                    manifest_s3_key,
                    boto3_session_for_s3,
                    downloaded_manifests,
                )
            )
        # surfaces any exceptions in the thread
        for future in concurrent.futures.as_completed(futures):
            future.result()

    duration = datetime.now(tz=timezone.utc) - start_time
    print_function_callback(f"...downloaded manifests in {duration}")

    return downloaded_manifests


def _merge_absolute_path_manifest_list(
    downloaded_manifests: list[tuple[datetime, BaseAssetManifest]],
) -> list[BaseManifestPath]:
    """
    Given a list of manifests that contain absolute paths, uses the provided last modified timestamps
    to sort them, and merges them all into a single manifest. Returns the list of manifest paths
    for download.

    Args:
        downloaded_manifests: A list of (last_modified_timestamp, manifest) tuples, where each last
            modified timestamp is the LastModified datetime from the S3 object holding the manifest.

    Returns:
        A list of manifest paths to download, the result of merging the manifests.
    """
    # Because the paths in manifests are all absolute and normalized now, we can merge them
    # in order by inserting them into a dict in order, using the normcased path as the key.
    # Later files of the same name will overwrite earlier ones.

    # Sort the manifests by last modified, so that we can overlay them
    # with later manifests overwriting files from earlier ones.
    downloaded_manifests.sort(key=lambda item: item[0])

    merged_manifest_paths_dict = {}
    for _, manifest in downloaded_manifests:
        for manifest_path in manifest.paths:
            merged_manifest_paths_dict[os.path.normcase(manifest_path.path)] = manifest_path
    return list(merged_manifest_paths_dict.values())


def _download_file_with_transfer_manager(
    local_file_path: Path,
    s3_bucket: str,
    s3_key: str,
    boto3_session: boto3.Session,
    s3_client: BaseClient,
    progress_tracker: ProgressTracker,
):
    """
    Downloads a single file from S3 using S3 transfer manager. This is appropriate for a larger
    file that benefits from parallel multi-part download.
    """
    transfer_manager = get_s3_transfer_manager(s3_client=s3_client)

    future: concurrent.futures.Future

    def handler(bytes_downloaded):
        nonlocal progress_tracker
        nonlocal future

        should_continue = progress_tracker.track_progress_callback(bytes_downloaded)
        if not should_continue:
            future.cancel()

    subscribers = [ProgressCallbackInvoker(handler)]

    future = transfer_manager.download(
        bucket=s3_bucket,
        key=s3_key,
        fileobj=str(local_file_path),
        extra_args={"ExpectedBucketOwner": get_account_id(session=boto3_session)},
        subscribers=subscribers,
    )

    future.result()


def _download_file_with_get_object(
    local_file_path: Path,
    s3_bucket: str,
    s3_key: str,
    boto3_session: boto3.Session,
    s3_client: BaseClient,
    progress_tracker: ProgressTracker,
):
    """
    Downloads a single file from S3 using get_object. This is appropriate for a smaller
    file that benefits from reduced overhead.
    """
    res = s3_client.get_object(
        Bucket=s3_bucket,
        Key=s3_key,
        ExpectedBucketOwner=get_account_id(session=boto3_session),
    )
    body = res["Body"]
    # Copy the data this amount at a time
    buffer_size = 128 * 1024
    with open(local_file_path, "wb") as fh:
        while True:
            data = body.read(buffer_size)
            if not data:
                break
            should_continue = progress_tracker.track_progress_callback(len(data))
            if not should_continue:
                fh.close()
                os.remove(local_file_path)
                raise AssetSyncCancelledError("File download cancelled.")
            fh.write(data)


def _download_file(
    file: BaseManifestPath,
    hash_algorithm: HashAlgorithm,
    collision_lock: Lock,
    collision_file_dict: DefaultDict[str, int],
    s3_bucket: str,
    cas_prefix: str,
    s3_client: BaseClient,
    boto3_session_for_s3: boto3.Session,
    progress_tracker: ProgressTracker,
    file_conflict_resolution: FileConflictResolution,
) -> None:
    """
    Downloads a file from the S3 bucket to the local directory.

    Args:
        file: A BaseManifestPath whose path is a local absolute path.
        hash_algorithm: The hash algorithm used for the queue.
        collision_lock: A lock to ensure only one thread resolves a path name collision at a time.
        collision_file_dict: Dictionary for tracking path name collisions.
        s3_bucket: The job attachments S3 bucket.
        cas_prefix: The prefix for content-addressed data files in the S3 bucket.
        s3_client: A boto3 client for accessing S3.
        boto3_session_for_s3: The boto3.Session to use for accessing S3.
        progress_tracker: Object to update with download progress status.
        file_conflict_resolution: The strategy to use for file conflict resolution.
    """
    local_file_path = _get_long_path_compatible_path(file.path)

    s3_key = f"{cas_prefix}/{file.hash}.{hash_algorithm.value}"

    # If the file name already exists, resolve the conflict based on the file_conflict_resolution
    if local_file_path.is_file():
        if file_conflict_resolution == FileConflictResolution.SKIP:
            return
        elif file_conflict_resolution == FileConflictResolution.OVERWRITE:
            pass
        elif file_conflict_resolution == FileConflictResolution.CREATE_COPY:
            local_file_path = _get_new_copy_file_path(
                local_file_path, collision_lock, collision_file_dict
            )
        else:
            raise ValueError(
                f"Unknown choice for file conflict resolution: {file_conflict_resolution}"
            )

    local_file_path.parent.mkdir(parents=True, exist_ok=True)

    if file.size > 1024 * 1024:
        download_file = _download_file_with_transfer_manager
    else:
        download_file = _download_file_with_get_object

    try:
        download_file(
            local_file_path=local_file_path,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            boto3_session=boto3_session_for_s3,
            s3_client=s3_client,
            progress_tracker=progress_tracker,
        )
    except concurrent.futures.CancelledError as ce:
        if progress_tracker and progress_tracker.continue_reporting is False:
            raise AssetSyncCancelledError("File download cancelled.")
        else:
            raise AssetSyncError("File download failed.", ce) from ce
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
            404: (
                "Not found. Please check your bucket name and object key, and ensure that they exist in the AWS account."
            ),
        }
        raise JobAttachmentsS3ClientError(
            action="downloading file",
            status_code=status_code,
            bucket_name=s3_bucket,
            key_or_prefix=s3_key,
            message=f"{status_code_guidance.get(status_code, '')} {str(exc)} (Failed to download the file to {str(local_file_path)})",
        ) from exc
    except BotoCoreError as bce:
        raise JobAttachmentS3BotoCoreError(
            action="downloading file",
            error_details=str(bce),
        ) from bce
    except Exception as e:
        raise AssetSyncError(e) from e

    # The modified time in the manifest is in microseconds, but utime requires the time be expressed in seconds.
    modified_time_override = file.mtime / 1000000  # type: ignore[attr-defined]
    os.utime(local_file_path, (modified_time_override, modified_time_override))  # type: ignore[arg-type]

    # Verify that what we downloaded has the correct file size from the manifest.
    file_size_on_disk = os.path.getsize(local_file_path)
    if file_size_on_disk != file.size:
        # TODO: Improve this error message
        raise JobAttachmentsError(
            f"File from S3 for {file.path} had incorrect size {file_size_on_disk}. Required size: {file.size}"
        )


def _download_manifest_paths(
    manifest_paths_to_download: list[BaseManifestPath],
    hash_algorithm: HashAlgorithm,
    queue: dict[str, Any],
    boto3_session_for_s3: boto3.Session,
    file_conflict_resolution: FileConflictResolution,
    on_downloading_files: Optional[Callable[[ProgressReportMetadata], bool]],
    print_function_callback: Callable[[str], None] = lambda msg: None,
) -> None:
    """
    Downloads all files from the S3 bucket in the Job Attachment settings to the specified directory.
    Returns a list of local paths of downloaded files.

    Args:
        manifest_paths_to_download: A list of manifest path objects to download, whose path is an absolute file system path.
        hash_algorithm: The hash algorithm in use by the queue.
        queue: The queue as returned by boto3 deadline.get_queue().
        boto3_session_for_s3: The boto3.Session to use for accessing S3.
        file_conflict_resolution: The strategy to use for file conflict resolution.
        on_downloading_files: A callback to handle progress messages and cancelation.
        print_function_callback: Callback for printing output to the terminal or log.
    """
    s3_settings = JobAttachmentS3Settings(**queue["jobAttachmentSettings"])
    s3_client = get_s3_client(session=boto3_session_for_s3)
    max_workers = _get_num_download_workers()

    collision_lock: Lock = Lock()
    collision_file_dict: DefaultDict[str, int] = DefaultDict(int)
    full_cas_prefix: str = s3_settings.full_cas_prefix()

    progress_tracker = ProgressTracker(
        status=ProgressStatus.DOWNLOAD_IN_PROGRESS,
        total_files=len(manifest_paths_to_download),
        total_bytes=sum(manifest_path.size for manifest_path in manifest_paths_to_download),
        on_progress_callback=on_downloading_files,
    )

    print_function_callback(f"Using {max_workers} threads")
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                _download_file,
                manifest_path,
                hash_algorithm,
                collision_lock,
                collision_file_dict,
                s3_settings.s3BucketName,
                full_cas_prefix,
                s3_client,
                boto3_session_for_s3,
                progress_tracker,
                file_conflict_resolution,
            )
            for manifest_path in manifest_paths_to_download
        ]
        # surfaces any exceptions in the thread
        for future in concurrent.futures.as_completed(futures):
            future.result()
            if progress_tracker:
                progress_tracker.increase_processed(1, 0)
                progress_tracker.report_progress()

    # to report progress 100% at the end
    if progress_tracker:
        progress_tracker.report_progress()
