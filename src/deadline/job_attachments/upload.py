# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Classes for handling uploading of assets.
"""
from __future__ import annotations

import concurrent.futures
import threading
import functools
import logging
import math
import os
import time
from datetime import datetime
from io import BytesIO
from math import trunc
from pathlib import Path, PurePath
from typing import Any, Callable, Optional, Tuple, Type, Union

import boto3
from botocore.exceptions import ClientError
from s3transfer import ReadFileChunk
from s3transfer.utils import ChunksizeAdjuster
from deadline.client.config import config_file

from .asset_manifests import (
    BaseAssetManifest,
    BaseManifestModel,
    ManifestModelRegistry,
    ManifestVersion,
    base_manifest,
)
from ._aws.aws_clients import get_account_id, get_boto3_session, get_s3_client
from ._aws.deadline import get_storage_profile_for_queue
from .exceptions import (
    COMMON_ERROR_GUIDANCE_FOR_S3,
    AssetSyncCancelledError,
    AssetSyncError,
    JobAttachmentsS3ClientError,
    MissingS3BucketError,
    MissingS3RootPrefixError,
)
from .hash_cache import HashCache
from .models import (
    AssetRootGroup,
    AssetRootManifest,
    Attachments,
    FileSystemLocationType,
    HashCacheEntry,
    JobAttachmentS3Settings,
    ManifestProperties,
    PathFormat,
)
from .progress_tracker import (
    ProgressStatus,
    ProgressTracker,
    SummaryStatistics,
)
from ._utils import (
    _hash_data,
    _hash_file,
    _is_relative_to,
    _join_s3_paths,
)

logger = logging.getLogger("deadline.job_attachments.upload")


class S3AssetUploader:
    """
    Handler for uploading assets to S3 based off of an Asset Manifest. If no session is provided the default
    credentials path will be used, see
    https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#configuring-credentials
    """

    def __init__(
        self,
        session: Optional[boto3.Session] = None,
    ) -> None:
        if session is None:
            self._session = get_boto3_session()
        else:
            self._session = session

        self._s3 = get_s3_client(self._session)  # pylint: disable=invalid-name

        # TODO: full performance analysis to determine the ideal thresholds
        try:
            self.list_object_threshold = int(
                config_file.get_setting("settings.list_object_threshold")
            )
            self.multipart_upload_chunk_size = int(
                config_file.get_setting("settings.multipart_upload_chunk_size")
            )
            self.multipart_upload_max_workers = int(
                config_file.get_setting("settings.multipart_upload_max_workers")
            )
            # The small file threshold is the chunk size multiplied by the small file threshold multiplier.
            small_file_threshold_multiplier = int(
                config_file.get_setting("settings.small_file_threshold_multiplier")
            )
            self.small_file_threshold = (
                self.multipart_upload_chunk_size * small_file_threshold_multiplier
            )
        except ValueError as ve:
            raise AssetSyncError(
                "Failed to parse configuration settings. Please ensure that the following settings in the config file are integers: "
                "list_object_threshold, multipart_upload_chunk_size, multipart_upload_max_workers, small_file_threshold_multiplier"
            ) from ve

        # Confirm that the settings values are all positive.
        error_msg = ""
        if self.list_object_threshold <= 0:
            error_msg = (
                f"list_object_threshold ({self.list_object_threshold}) must be positive integer."
            )

        elif self.multipart_upload_chunk_size <= 0:
            error_msg = f"multipart_upload_chunk_size ({self.multipart_upload_chunk_size}) must be positive integer."

        elif self.multipart_upload_max_workers <= 0:
            error_msg = f"multipart_upload_max_workers ({self.multipart_upload_max_workers}) must be positive integer."

        elif small_file_threshold_multiplier <= 0:
            error_msg = f"small_file_threshold_multiplier ({small_file_threshold_multiplier}) must be positive integer."

        if error_msg:
            raise AssetSyncError("Invalid value for configuration setting: " + error_msg)

    def upload_assets(
        self,
        job_attachment_settings: JobAttachmentS3Settings,
        manifest: BaseAssetManifest,
        partial_manifest_prefix: str,
        source_root: Path,
        file_system_location_name: Optional[str] = None,
        progress_tracker: Optional[ProgressTracker] = None,
    ) -> tuple[str, str]:
        """
        Uploads assets based off of an asset manifest, uploads the asset manifest.

        Args:
            manifest: The asset manifest to upload.
            partial_manifest_prefix: The (partial) key prefix to use for uploading the manifest
                 to S3, excluding the initial section "<root-prefix>/Manifest/".
                e.g. "farm-1234/queue-1234/Inputs/<some-guid>"
            source_root: The local root path of the assets.
            job_attachment_settings: The settings for the job attachment configured in Queue.
            progress_tracker: Optional progress tracker to track progress.

        Returns:
            A tuple of (the partial key for the manifest on S3, the hash of input manifest).
        """
        self.upload_input_files(
            manifest,
            job_attachment_settings.s3BucketName,
            source_root,
            job_attachment_settings.full_cas_prefix(),
            progress_tracker,
        )
        manifest_bytes = manifest.encode().encode("utf-8")

        manifest_name_prefix = _hash_data(
            f"{file_system_location_name or ''}{str(source_root)}".encode()
        )
        manifest_name = f"{manifest_name_prefix}_input.{manifest.hashAlg}"

        if partial_manifest_prefix:
            partial_manifest_key = _join_s3_paths(partial_manifest_prefix, manifest_name)
        else:
            partial_manifest_key = manifest_name

        full_manifest_key = job_attachment_settings.add_root_and_manifest_folder_prefix(
            partial_manifest_key
        )

        self.upload_bytes_to_s3(
            bytes=BytesIO(manifest_bytes),
            bucket=job_attachment_settings.s3BucketName,
            key=full_manifest_key,
        )

        return (partial_manifest_key, _hash_data(manifest_bytes))

    def upload_input_files(
        self,
        manifest: BaseAssetManifest,
        s3_bucket: str,
        source_root: Path,
        s3_cas_prefix: str,
        progress_tracker: Optional[ProgressTracker] = None,
    ) -> None:
        """
        Uploads all of the files listed in the given manifest to S3 if they don't exist in the
        given S3 prefix already.

        Depending on the number of files to be uploaded, will either make a head-object or list-objects
        S3 API call to check if files have already been uploaded. Note that head-object is cheaper
        to call, but slows down significantly if needing to call many times, so the list-objects API
        is called for larger file lists.

        TODO: There is a known performance bottleneck if the bucket has a large number of files, but
        there isn't currently any way of knowing the size of the bucket without iterating through the
        contents of a prefix. For now, we'll just head-object when we have a small number of files.
        """
        files_to_upload: list[base_manifest.BaseManifestPath] = manifest.paths
        check_if_in_s3 = True

        if len(files_to_upload) >= self.list_object_threshold:
            # If different files have the same content (and thus the same hash), they are counted as skipped files.
            file_dict: dict[str, base_manifest.BaseManifestPath] = {}
            for file in files_to_upload:
                if file.hash in file_dict and progress_tracker:
                    progress_tracker.increase_skipped(
                        1, (source_root.joinpath(file.path)).stat().st_size
                    )
                else:
                    file_dict[file.hash] = file

            to_upload_set: set[str] = self.filter_objects_to_upload(
                s3_bucket, s3_cas_prefix, set(file_dict.keys())
            )
            files_to_upload = [file_dict[k] for k in to_upload_set]
            check_if_in_s3 = False  # Can skip the check since we just did it above
            # The input files that are already in s3 are counted as skipped files.
            if progress_tracker:
                skipped_set = set(file_dict.keys()) - to_upload_set
                files_to_skip = [file_dict[k] for k in skipped_set]
                progress_tracker.increase_skipped(
                    len(files_to_skip),
                    sum((source_root.joinpath(file.path)).stat().st_size for file in files_to_skip),
                )

        # Split into a separate 'large file' and 'small file' queues.
        # Separate 'large' files from 'small' files so that we can process 'large' files serially.
        # This wastes less bandwidth if uploads are cancelled, as it's better to use the multi-threaded
        # multi-part upload for a single large file than multiple large files at the same time.
        (small_file_queue, large_file_queue) = self._separate_files_by_size(
            files_to_upload, self.small_file_threshold
        )

        # First, process the whole 'small file' queue with parallel object uploads.
        # TODO: tune this. max_worker defaults to 5 * number of processors. We can run into issues here
        # if we thread too aggressively on slower internet connections. So for now let's set it to 5,
        # which would the number of threads with one processor.
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(
                    self.upload_object_to_cas,
                    file,
                    s3_bucket,
                    source_root,
                    s3_cas_prefix,
                    check_if_in_s3,
                    progress_tracker,
                ): file
                for file in small_file_queue
            }
            # surfaces any exceptions in the thread
            for future in concurrent.futures.as_completed(futures):
                (is_uploaded, file_size) = future.result()
                if progress_tracker and not is_uploaded:
                    progress_tracker.increase_skipped(1, file_size)

        # Now process the whole 'large file' queue with serial object uploads (but still parallel multi-part upload.)
        for file in large_file_queue:
            (is_uploaded, file_size) = self.upload_object_to_cas(
                file,
                s3_bucket,
                source_root,
                s3_cas_prefix,
                check_if_in_s3,
                progress_tracker,
            )
            if progress_tracker and not is_uploaded:
                progress_tracker.increase_skipped(1, file_size)

        # to report progress 100% at the end, and
        # to check if the job submission was canceled in the middle of processing the last batch of files.
        if progress_tracker:
            progress_tracker.report_progress()
            if not progress_tracker.continue_reporting:
                raise AssetSyncCancelledError(
                    "File upload cancelled.", progress_tracker.get_summary_statistics()
                )

    def _separate_files_by_size(
        self,
        files_to_upload: list[base_manifest.BaseManifestPath],
        size_threshold: int,
    ) -> Tuple[list[base_manifest.BaseManifestPath], list[base_manifest.BaseManifestPath]]:
        """
        Splits the given list of files into two queues: one for small files and one for large files.
        """
        small_file_queue: list[base_manifest.BaseManifestPath] = []
        large_file_queue: list[base_manifest.BaseManifestPath] = []
        for file in files_to_upload:
            if file.size <= size_threshold:
                small_file_queue.append(file)
            else:
                large_file_queue.append(file)
        return (small_file_queue, large_file_queue)

    def upload_object_to_cas(
        self,
        file: base_manifest.BaseManifestPath,
        s3_bucket: str,
        source_root: Path,
        s3_cas_prefix: str,
        check_if_in_s3: bool = True,
        progress_tracker: Optional[ProgressTracker] = None,
    ) -> Tuple[bool, int]:
        """
        Uploads an object to the S3 content-addressable storage (CAS) prefix. Optionally,
        does a head-object check and only uploads the file if it doesn't exist in S3 already.
        Returns a tuple (whether it has been uploaded, the file size).
        """
        local_path = source_root.joinpath(file.path)
        if s3_cas_prefix:
            s3_upload_key = _join_s3_paths(s3_cas_prefix, file.hash)
        else:
            s3_upload_key = file.hash
        is_uploaded = False
        file_size = local_path.stat().st_size

        if check_if_in_s3 and self.file_already_uploaded(s3_bucket, s3_upload_key):
            logger.debug(
                f"skipping {local_path} because it has already been uploaded to s3://{s3_bucket}/{s3_upload_key}"
            )
            return (is_uploaded, file_size)

        self.upload_file_to_s3(
            local_path=local_path,
            s3_bucket=s3_bucket,
            s3_upload_key=s3_upload_key,
            progress_tracker=progress_tracker,
        )
        is_uploaded = True
        return (is_uploaded, file_size)

    def upload_file_to_s3(
        self,
        local_path: Path,
        s3_bucket: str,
        s3_upload_key: str,
        progress_tracker: Optional[ProgressTracker] = None,
    ) -> None:
        """
        Uploads a single file to an S3 bucket using multipart upload, meaning that it
        splits the file into multiple parts and uploads them in parallel. It checks for
        cancellation at each part upload. If the callback in `progress_tracker` returns
        a cancel signal (False) from the caller, the upload is aborted immediately.

        Note: It uses boto3's low-level APIs for multipart upload to allow for granular
        control, specifically to enable cancellation of the upload mid-way.
        """
        file_size = local_path.stat().st_size
        upload_id = None
        expected_bucket_owner = get_account_id(session=self._session)
        try:
            # Initiate a multipart upload
            upload_id = self._s3.create_multipart_upload(
                Bucket=s3_bucket,
                Key=s3_upload_key,
                ExpectedBucketOwner=expected_bucket_owner,
            )["UploadId"]
            parts = []
            chunk_size_adjuster = ChunksizeAdjuster()
            chunk_size = chunk_size_adjuster.adjust_chunksize(
                current_chunksize=self.multipart_upload_chunk_size,
                file_size=file_size,
            )
            num_parts = int(math.ceil(file_size / float(chunk_size)))

            # Start uploading parts
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.multipart_upload_max_workers
            ) as executor:
                upload_partial = functools.partial(
                    self._upload_part,
                    s3_bucket,
                    s3_upload_key,
                    str(local_path),
                    upload_id,
                    chunk_size,
                    progress_tracker,
                )
                for result in executor.map(upload_partial, range(1, num_parts + 1)):
                    if result is False:
                        # If canceled, the upload is aborted immediately.
                        self._s3.abort_multipart_upload(
                            Bucket=s3_bucket,
                            Key=s3_upload_key,
                            UploadId=upload_id,
                            ExpectedBucketOwner=expected_bucket_owner,
                        )
                        if progress_tracker:
                            raise AssetSyncCancelledError(
                                "File upload cancelled.", progress_tracker.get_summary_statistics()
                            )
                    parts.append(result)

            # Complete the multipart upload
            self._s3.complete_multipart_upload(
                Bucket=s3_bucket,
                Key=s3_upload_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
                ExpectedBucketOwner=expected_bucket_owner,
            )
            is_uploaded = True
            if progress_tracker and is_uploaded:
                progress_tracker.increase_processed(1, 0)

        except ClientError as exc:
            if upload_id:
                # If any error occurs, the multipart upload must be aborted.
                self._s3.abort_multipart_upload(
                    Bucket=s3_bucket,
                    Key=s3_upload_key,
                    UploadId=upload_id,
                    ExpectedBucketOwner=expected_bucket_owner,
                )
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
                action="uploading file",
                status_code=status_code,
                bucket_name=s3_bucket,
                key_or_prefix=s3_upload_key,
                message=f"{status_code_guidance.get(status_code, '')} {str(exc)} (Failed to upload {str(local_path)})",
            ) from exc
        except Exception as exc:
            if upload_id:
                # If any error occurs, the multipart upload must be aborted.
                self._s3.abort_multipart_upload(
                    Bucket=s3_bucket,
                    Key=s3_upload_key,
                    UploadId=upload_id,
                    ExpectedBucketOwner=expected_bucket_owner,
                )
            raise exc

    def _upload_part(
        self,
        bucket: str,
        key: str,
        file_name: str,
        upload_id: str,
        part_size: int,
        progress_tracker: Optional[ProgressTracker],
        part_number: int,
    ):
        """
        As a helper function to be used within `upload_file_to_s3`, it uploads a single part
        of a file. Reads a chunk of the file, and uploads it using boto3's `upload_part` API.
        Returns a dictionary with the part number and ETag.
        """
        open_chunk_reader = ReadFileChunk.from_filename
        with open_chunk_reader(
            file_name,
            part_size * (part_number - 1),
            part_size,
            progress_tracker.track_progress_callback if progress_tracker else None,
        ) as body:
            response = self._s3.upload_part(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=body,
                ExpectedBucketOwner=get_account_id(session=self._session),
            )
            # Update the upload progress
            if progress_tracker:
                is_continue = progress_tracker.continue_reporting
                if not is_continue:
                    return False  # return false it if gets a signal to cancel
            etag = response["ETag"]
            return {"ETag": etag, "PartNumber": part_number}

    def filter_objects_to_upload(self, bucket: str, prefix: str, upload_set: set[str]) -> set[str]:
        """
        Makes a paginated list-objects request to S3 to get all objects in the given prefix.
        Given the set of files to be uploaded, returns which objects do not exist in S3.
        """
        try:
            paginator = self._s3.get_paginator("list_objects_v2")
            page_iterator = paginator.paginate(
                Bucket=bucket,
                Prefix=prefix,
            )

            for page in page_iterator:
                contents = page.get("Contents", None)
                if contents is None:
                    break
                for content in contents:
                    upload_set.discard(content["Key"].split("/")[-1])
                if len(upload_set) == 0:
                    break
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
                bucket_name=bucket,
                key_or_prefix=prefix,
                message=f"{status_code_guidance.get(status_code, '')} {str(exc)}",
            ) from exc

        return upload_set

    def file_already_uploaded(self, bucket: str, key: str) -> bool:
        """
        Check whether the file has already been uploaded by doing a head-object call.
        """
        try:
            self._s3.head_object(
                Bucket=bucket,
                Key=key,
            )
            return True
        except ClientError as exc:
            error_code = int(exc.response["ResponseMetadata"]["HTTPStatusCode"])
            if error_code == 403:
                message = (
                    f"Access denied. Ensure that the bucket is in the account {get_account_id(session=self._session)}, "
                    "and your AWS IAM Role or User has the 's3:ListBucket' permission for this bucket."
                )
                raise JobAttachmentsS3ClientError(
                    "checking if object exists", error_code, bucket, key, message
                ) from exc
            return False

    def upload_bytes_to_s3(
        self,
        bytes: BytesIO,
        bucket: str,
        key: str,
        progress_handler: Optional[Callable[[int], None]] = None,
        extra_args: dict[str, Any] = dict(),
    ) -> None:
        try:
            extra_args_merged: dict[str, Union[str, dict]] = {
                "ExpectedBucketOwner": get_account_id(session=self._session),
                **extra_args,
            }

            self._s3.upload_fileobj(
                bytes,
                bucket,
                key,
                ExtraArgs=extra_args_merged,
                Callback=progress_handler,
            )
        except ClientError as exc:
            status_code = int(exc.response["ResponseMetadata"]["HTTPStatusCode"])
            status_code_guidance = {
                **COMMON_ERROR_GUIDANCE_FOR_S3,
                403: (
                    "Forbidden or Access denied. Please check your AWS credentials, and ensure that "
                    "your AWS IAM Role or User has the 's3:PutObject' permission for this bucket."
                ),
                404: "Not found. Please check your bucket name, and ensure that it exists in the AWS account.",
            }
            raise JobAttachmentsS3ClientError(
                action="uploading binary file",
                status_code=status_code,
                bucket_name=bucket,
                key_or_prefix=key,
                message=f"{status_code_guidance.get(status_code, '')} {str(exc)}",
            ) from exc


class S3AssetManager:
    """
    Asset handler that creates an asset manifest and uploads assets. Based on an S3 file system.
    """

    _HASH_ALG = "xxh128"

    def __init__(
        self,
        farm_id: str,
        queue_id: str,
        job_attachment_settings: JobAttachmentS3Settings,
        asset_uploader: Optional[S3AssetUploader] = None,
        session: Optional[boto3.Session] = None,
        asset_manifest_version: ManifestVersion = ManifestVersion.v2023_03_03,
    ) -> None:
        self.farm_id = farm_id
        self.queue_id = queue_id
        self.job_attachment_settings: JobAttachmentS3Settings = job_attachment_settings

        if not self.job_attachment_settings.s3BucketName:
            raise MissingS3BucketError(
                "To use Job Attachments, the 's3BucketName' must be set in  your queue's JobAttachmentSettings"
            )
        if not self.job_attachment_settings.rootPrefix:
            raise MissingS3RootPrefixError(
                "To use Job Attachments, the 'rootPrefix' must be set in your queue's JobAttachmentSettings"
            )

        if asset_uploader is None:
            asset_uploader = S3AssetUploader(session=session)

        self.asset_uploader = asset_uploader
        self.session = session

        self.manifest_version: ManifestVersion = asset_manifest_version

    def _process_input_path(
        self,
        path: Path,
        root_path: str,
        hash_cache: HashCache,
        progress_tracker: Optional[ProgressTracker] = None,
    ) -> Tuple[bool, int, base_manifest.BaseManifestPath]:
        # If it's cancelled, raise an AssetSyncCancelledError exception
        if progress_tracker and not progress_tracker.continue_reporting:
            raise AssetSyncCancelledError(
                "File hashing cancelled.", progress_tracker.get_summary_statistics()
            )

        manifest_model: Type[BaseManifestModel] = ManifestModelRegistry.get_manifest_model(
            version=self.manifest_version
        )

        full_path = str(path.resolve())
        is_new_or_modified: bool = False
        actual_modified_time = str(datetime.fromtimestamp(path.stat().st_mtime))

        entry: Optional[HashCacheEntry] = hash_cache.get_entry(full_path)
        if entry is not None:
            # If the file was modified, we need to rehash it
            if actual_modified_time != entry.last_modified_time:
                entry.last_modified_time = actual_modified_time
                entry.file_hash = _hash_file(full_path)
                is_new_or_modified = True
        else:
            entry = HashCacheEntry(
                file_path=full_path,
                file_hash=_hash_file(full_path),
                last_modified_time=actual_modified_time,
            )
            is_new_or_modified = True

        if is_new_or_modified:
            hash_cache.put_entry(entry)

        file_size = path.resolve().stat().st_size
        path_args: dict[str, Any] = {
            "path": path.relative_to(root_path).as_posix(),
            "hash": entry.file_hash,
        }

        # stat().st_mtime_ns returns an int that represents the time in nanoseconds since the epoch.
        # The asset manifest spec requires the mtime to be represented as an integer in microseconds.
        path_args["mtime"] = trunc(path.stat().st_mtime_ns // 1000)
        path_args["size"] = file_size

        return (is_new_or_modified, file_size, manifest_model.Path(**path_args))

    def _create_manifest_file(
        self,
        input_paths: list[Path],
        root_path: str,
        hash_cache: HashCache,
        progress_tracker: Optional[ProgressTracker] = None,
    ) -> BaseAssetManifest:
        manifest_model: Type[BaseManifestModel] = ManifestModelRegistry.get_manifest_model(
            version=self.manifest_version
        )
        if manifest_model.manifest_version in {
            ManifestVersion.v2023_03_03,
        }:
            paths: list[base_manifest.BaseManifestPath] = []

            callback_lock = threading.Lock()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = {
                    executor.submit(
                        self._process_input_path, path, root_path, hash_cache, progress_tracker
                    ): path
                    for path in input_paths
                }
                for future in concurrent.futures.as_completed(futures):
                    (is_hashed, file_size, path_to_put_in_manifest) = future.result()
                    paths.append(path_to_put_in_manifest)
                    if progress_tracker:
                        if is_hashed:
                            progress_tracker.increase_processed(1, file_size)
                        else:
                            progress_tracker.increase_skipped(1, file_size)

                        with callback_lock:
                            progress_tracker.report_progress()

            # Need to sort the list to keep it canonical
            paths.sort(key=lambda x: x.path, reverse=True)

            manifest_args: dict[str, Any] = {"hash_alg": self._HASH_ALG, "paths": paths}

            manifest_args["total_size"] = sum([path.size for path in paths])

            return manifest_model.AssetManifest(**manifest_args)
        else:
            raise NotImplementedError(
                f"Creation of manifest version {manifest_model.manifest_version} is not supported."
            )

    def _get_asset_groups(
        self,
        input_paths: set[str],
        output_paths: set[str],
        referenced_paths: set[str],
        local_type_locations: dict[str, str] = {},
        shared_type_locations: dict[str, str] = {},
    ) -> list[AssetRootGroup]:
        """
        For the given input paths and output paths, a list of groups is returned, where paths sharing
        the same root path are grouped together. Note that paths can be files or directories.

        The returned list satisfies the following conditions:
        - If a path is relative to any of the paths in the given `shared_type_locations` paths, it is
          excluded from the list.
        - The given `local_type_locations` paths can each form a group based on its root path. In other
          words, if there are paths relative to any of the `local_type_locations` paths, they are grouped
          together as one.
        - The referenced paths may have no files or directories associated, but they always live
          relative to one of the AssetRootGroup objects returned.
        """
        groupings: dict[str, AssetRootGroup] = {}

        # Resolve full path, then cast to pure path to get top-level directory
        # Note for inputs, we only upload individual files so user doesn't unintentionally upload the entire hard drive
        for _path in input_paths:
            # Need to use absolute to not resolve symlinks, but need normpath to get rid of relative paths, i.e. '..'
            abs_path = Path(os.path.normpath(Path(_path).absolute()))
            if not abs_path.exists():
                logger.warning(f"Skipping uploading input as it doesn't exist: {abs_path}")
                continue
            if abs_path.is_dir():
                logger.warning(f"Skipping uploading input as it is a directory: {abs_path}")
                continue

            # Skips the upload if the path is relative to any of the File System Location
            # of SHARED type that was set in the Job.
            if any(_is_relative_to(abs_path, shared) for shared in shared_type_locations):
                continue

            # If the path is relative to any of the File System Location of LOCAL type,
            # groups the files into a single group using the path of that location.
            matched_root = self._find_matched_root_from_local_type_locations(
                groupings=groupings,
                abs_path=abs_path,
                local_type_locations=local_type_locations,
            )
            groupings[matched_root].inputs.add(abs_path)

        for _path in output_paths:
            abs_path = Path(os.path.normpath(Path(_path).absolute()))

            # Skips the upload if the path is relative to any of the File System Location
            # of SHARED type that was set in the Job.
            if any(_is_relative_to(abs_path, shared) for shared in shared_type_locations):
                continue

            # If the path is relative to any of the File System Location of LOCAL type,
            # groups the files into a single group using the path of that location.
            matched_root = self._find_matched_root_from_local_type_locations(
                groupings=groupings,
                abs_path=abs_path,
                local_type_locations=local_type_locations,
            )
            groupings[matched_root].outputs.add(abs_path)

        for _path in referenced_paths:
            abs_path = Path(os.path.normpath(Path(_path).absolute()))

            # Skips the reference if the path is relative to any of the File System Location
            # of SHARED type that was set in the Job.
            if any(_is_relative_to(abs_path, shared) for shared in shared_type_locations):
                continue
            # If the path is relative to any of the File System Location of LOCAL type,
            # groups the references into a single group using the path of that location.
            matched_root = self._find_matched_root_from_local_type_locations(
                groupings=groupings,
                abs_path=abs_path,
                local_type_locations=local_type_locations,
            )
            groupings[matched_root].references.add(abs_path)

        # Finally, build the list of asset root groups
        for asset_group in groupings.values():
            common_path: Path = Path(
                os.path.commonpath(
                    list(asset_group.inputs | asset_group.outputs | asset_group.references)
                )
            )
            if common_path.is_file():
                common_path = common_path.parent
            asset_group.root_path = str(common_path)

        return list(groupings.values())

    def _find_matched_root_from_local_type_locations(
        self,
        groupings: dict[str, AssetRootGroup],
        abs_path: Path,
        local_type_locations: dict[str, str] = {},
    ) -> str:
        """
        Checks if the given `abs_path` is relative to any of the File System Locations of LOCAL type.
        If it is, select the most specific File System Location, and add a new grouping keyed by that
        matched root path (if the key does not exist.) Then, returns the matched root path.
        If no match is found, returns the top directory of `abs_path` as the key used for grouping.
        """
        matched_root = None
        for root_path in local_type_locations.keys():
            if _is_relative_to(abs_path, root_path) and (
                matched_root is None or len(root_path) > len(matched_root)
            ):
                matched_root = root_path

        if matched_root is not None:
            if matched_root not in groupings:
                groupings[matched_root] = AssetRootGroup(
                    file_system_location_name=local_type_locations[matched_root],
                )
            return matched_root
        else:
            top_directory = PurePath(abs_path).parts[0]
            if top_directory not in groupings:
                groupings[top_directory] = AssetRootGroup()
            return top_directory

    def _get_total_size_of_files(self, paths: list[str]) -> int:
        total_bytes = 0
        try:
            for path in paths:
                total_bytes += Path(path).resolve().stat().st_size
        except FileNotFoundError:
            logger.warning(
                f"Skipping the input from total size calculation as it doesn't exist: {path}"
            )
        return total_bytes

    def _get_total_input_size_from_manifests(
        self, manifests: list[AssetRootManifest]
    ) -> tuple[int, int]:
        total_files = 0
        total_bytes = 0
        for asset_root_manifest in manifests:
            if asset_root_manifest.asset_manifest:
                input_paths = asset_root_manifest.asset_manifest.paths
                input_paths_str = [
                    str(Path(asset_root_manifest.root_path).joinpath(path.path))
                    for path in input_paths
                ]
                total_files += len(input_paths)
                total_bytes += self._get_total_size_of_files(input_paths_str)

        return (total_files, total_bytes)

    def _get_total_input_size_from_asset_group(
        self, groups: list[AssetRootGroup]
    ) -> tuple[int, int]:
        total_files = 0
        total_bytes = 0
        for group in groups:
            input_paths = [str(input) for input in group.inputs]
            total_bytes += self._get_total_size_of_files(input_paths)
            total_files += len(input_paths)
        return (total_files, total_bytes)

    def _get_file_system_locations_by_type(
        self,
        storage_profile_id: str,
        session: Optional[boto3.Session] = None,
    ) -> Tuple[dict, dict]:
        """
        Given the Storage Profile ID, fetches Storage Profile for Queue object, and
        extracts and groups path and name pairs from the File System Locations into
        two dicts - LOCAL and SHARED type, respectively. Returns a tuple of two dicts.
        """
        storage_profile_for_queue = get_storage_profile_for_queue(
            farm_id=self.farm_id,
            queue_id=self.queue_id,
            storage_profile_id=storage_profile_id,
            session=session,
        )

        local_type_locations: dict[str, str] = {}
        shared_type_locations: dict[str, str] = {}
        for fs_loc in storage_profile_for_queue.fileSystemLocations:
            if fs_loc.type == FileSystemLocationType.LOCAL:
                local_type_locations[fs_loc.path] = fs_loc.name
            elif fs_loc.type == FileSystemLocationType.SHARED:
                shared_type_locations[fs_loc.path] = fs_loc.name
        return local_type_locations, shared_type_locations

    def hash_assets_and_create_manifest(
        self,
        input_paths: list[str],
        output_paths: list[str],
        referenced_paths: list[str],
        storage_profile_id: Optional[str] = None,
        hash_cache_dir: Optional[str] = None,
        on_preparing_to_submit: Optional[Callable[[Any], bool]] = None,
    ) -> tuple[SummaryStatistics, list[AssetRootManifest]]:
        """
        Groups the input/output paths by asset root, computes the hashes for input files,
        and creates manifests using local hash cache.

        Args:
            input_paths: a list of input paths.
            output_paths: a list of output paths.
            hash_cache_dir: a path to local hash cache directory. If it's None, use default path.
            on_preparing_to_submit: a callback to be called to periodically report progress to the caller.
            The callback returns True if the operation should continue as normal, or False to cancel.

        Returns:
            a tuple with (1) the summary statistics of the hash operation, and
            (2) a list of AssetRootManifest (a manifest and output paths for each asset root).
        """
        start_time = time.perf_counter()

        local_type_locations: dict[str, str] = {}
        shared_type_locations: dict[str, str] = {}
        if storage_profile_id:
            (
                local_type_locations,
                shared_type_locations,
            ) = self._get_file_system_locations_by_type(storage_profile_id)

        # Group the paths by asset root, removing duplicates and empty strings
        asset_groups: list[AssetRootGroup] = self._get_asset_groups(
            {ip_path for ip_path in input_paths if ip_path},
            {op_path for op_path in output_paths if op_path},
            {rf_path for rf_path in referenced_paths if rf_path},
            local_type_locations,
            shared_type_locations,
        )

        # Sets up progress tracker to report upload progress back to the caller.
        (input_files, input_bytes) = self._get_total_input_size_from_asset_group(asset_groups)
        progress_tracker = ProgressTracker(
            status=ProgressStatus.PREPARING_IN_PROGRESS,
            total_files=input_files,
            total_bytes=input_bytes,
            on_progress_callback=on_preparing_to_submit,
        )

        asset_root_manifests: list[AssetRootManifest] = []
        for group in asset_groups:
            # Might have output directories, but no inputs for this group
            asset_manifest: Optional[BaseAssetManifest] = None
            if group.inputs:
                # Create manifest, using local hash cache
                with HashCache(hash_cache_dir) as hash_cache:
                    asset_manifest = self._create_manifest_file(
                        sorted(list(group.inputs)), group.root_path, hash_cache, progress_tracker
                    )

            asset_root_manifests.append(
                AssetRootManifest(
                    file_system_location_name=group.file_system_location_name,
                    root_path=group.root_path,
                    asset_manifest=asset_manifest,
                    outputs=sorted(list(group.outputs)),
                )
            )

        progress_tracker.total_time = time.perf_counter() - start_time

        return (progress_tracker.get_summary_statistics(), asset_root_manifests)

    def upload_assets(
        self,
        manifests: list[AssetRootManifest],
        on_uploading_assets: Optional[Callable[[Any], bool]] = None,
    ) -> tuple[SummaryStatistics, Attachments]:
        """
        Uploads all the files for provided manifests and manifests themselves to S3.

        Args:
            manifests: a list of manifests that contain assets to be uploaded
            on_uploading_assets: a callback to be called to periodically report progress to the caller.
            The callback returns True if the operation should continue as normal, or False to cancel.

        Returns:
            a tuple with (1) the summary statistics of the upload operation, and
            (2) the S3 path to the asset manifest file.
        """
        # Sets up progress tracker to report upload progress back to the caller.
        (input_files, input_bytes) = self._get_total_input_size_from_manifests(manifests)
        progress_tracker = ProgressTracker(
            status=ProgressStatus.UPLOAD_IN_PROGRESS,
            total_files=input_files,
            total_bytes=input_bytes,
            on_progress_callback=on_uploading_assets,
        )

        start_time = time.perf_counter()

        manifest_properties_list: list[ManifestProperties] = []

        for asset_root_manifest in manifests:
            output_rel_paths: list[str] = [
                str(path.relative_to(asset_root_manifest.root_path))
                for path in asset_root_manifest.outputs
            ]

            manifest_properties = ManifestProperties(
                fileSystemLocationName=asset_root_manifest.file_system_location_name,
                rootPath=asset_root_manifest.root_path,
                rootPathFormat=PathFormat.get_host_path_format(),
                outputRelativeDirectories=output_rel_paths,
            )

            if asset_root_manifest.asset_manifest:
                (partial_manifest_key, asset_manifest_hash) = self.asset_uploader.upload_assets(
                    job_attachment_settings=self.job_attachment_settings,
                    manifest=asset_root_manifest.asset_manifest,
                    partial_manifest_prefix=self.job_attachment_settings.partial_manifest_prefix(
                        self.farm_id, self.queue_id
                    ),
                    source_root=Path(asset_root_manifest.root_path),
                    file_system_location_name=asset_root_manifest.file_system_location_name,
                    progress_tracker=progress_tracker,
                )
                manifest_properties.inputManifestPath = partial_manifest_key
                manifest_properties.inputManifestHash = asset_manifest_hash

            manifest_properties_list.append(manifest_properties)

            logger.debug("Asset manifests - locations in S3:")
            logger.debug(
                "\n".join(
                    filter(
                        None,
                        (
                            manifest_properties.inputManifestPath
                            for manifest_properties in manifest_properties_list
                        ),
                    )
                )
            )

        progress_tracker.total_time = time.perf_counter() - start_time

        return (
            progress_tracker.get_summary_statistics(),
            Attachments(manifests=manifest_properties_list),
        )
