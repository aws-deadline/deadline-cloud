# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Classes for handling uploading of assets.
"""
from __future__ import annotations

import concurrent.futures
from contextlib import contextmanager
import errno
import logging
import os
import sys
import time
from datetime import datetime
from io import BufferedReader, BytesIO
from math import trunc
from pathlib import Path, PurePath
from typing import Any, Callable, Generator, Optional, Tuple, Type, Union

import boto3
from boto3.s3.transfer import ProgressCallbackInvoker
from botocore.exceptions import BotoCoreError, ClientError

from deadline.client.config import config_file

from .asset_manifests import (
    BaseAssetManifest,
    BaseManifestModel,
    HashAlgorithm,
    hash_data,
    hash_file,
    ManifestModelRegistry,
    ManifestVersion,
    base_manifest,
)
from ._aws.aws_clients import (
    get_account_id,
    get_boto3_session,
    get_s3_client,
    get_s3_transfer_manager,
)
from .exceptions import (
    COMMON_ERROR_GUIDANCE_FOR_S3,
    AssetSyncCancelledError,
    AssetSyncError,
    JobAttachmentS3BotoCoreError,
    JobAttachmentsS3ClientError,
    MisconfiguredInputsError,
    MissingS3BucketError,
    MissingS3RootPrefixError,
)
from .caches import HashCache, HashCacheEntry, S3CheckCache, S3CheckCacheEntry
from .models import (
    AssetRootGroup,
    AssetRootManifest,
    AssetUploadGroup,
    Attachments,
    FileSystemLocationType,
    JobAttachmentS3Settings,
    ManifestProperties,
    PathFormat,
    StorageProfile,
)
from .progress_tracker import (
    ProgressStatus,
    ProgressTracker,
    SummaryStatistics,
)
from ._utils import (
    _is_relative_to,
    _join_s3_paths,
)

logger = logging.getLogger("deadline.job_attachments.upload")

# The default multipart upload chunk size is 8 MB. We used this to determine the small file threshold,
# which is the chunk size multiplied by the small file threshold multiplier.
S3_MULTIPART_UPLOAD_CHUNK_SIZE: int = 8388608  # 8 MB
# The maximum number of concurrency for multipart uploads. This is used to determine the max number
# of thread workers for uploading multiple small files in parallel.
S3_UPLOAD_MAX_CONCURRENCY: int = 10


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

        try:
            # The small file threshold is the chunk size multiplied by the small file threshold multiplier.
            small_file_threshold_multiplier = int(
                config_file.get_setting("settings.small_file_threshold_multiplier")
            )
            self.small_file_threshold = (
                S3_MULTIPART_UPLOAD_CHUNK_SIZE * small_file_threshold_multiplier
            )

            s3_max_pool_connections = int(
                config_file.get_setting("settings.s3_max_pool_connections")
            )
            self.num_upload_workers = int(
                s3_max_pool_connections
                / min(small_file_threshold_multiplier, S3_UPLOAD_MAX_CONCURRENCY)
            )
            if self.num_upload_workers <= 0:
                # This can result in triggering "Connection pool is full" warning messages during uploads.
                self.num_upload_workers = 1
        except ValueError as ve:
            raise AssetSyncError(
                "Failed to parse configuration settings. Please ensure that the following settings in the config file are integers: "
                "'s3_max_pool_connections', 'small_file_threshold_multiplier'"
            ) from ve

        self._s3 = get_s3_client(self._session)  # pylint: disable=invalid-name

        # Confirm that the settings values are all positive.
        error_msg = ""
        if small_file_threshold_multiplier <= 0:
            error_msg = f"'small_file_threshold_multiplier' ({small_file_threshold_multiplier}) must be positive integer."
        elif s3_max_pool_connections <= 0:
            error_msg = (
                f"'s3_max_pool_connections' ({s3_max_pool_connections}) must be positive integer."
            )
        if error_msg:
            raise AssetSyncError("Nonvalid value for configuration setting: " + error_msg)

    def upload_assets(
        self,
        job_attachment_settings: JobAttachmentS3Settings,
        manifest: BaseAssetManifest,
        partial_manifest_prefix: str,
        source_root: Path,
        file_system_location_name: Optional[str] = None,
        progress_tracker: Optional[ProgressTracker] = None,
        s3_check_cache_dir: Optional[str] = None,
        manifest_write_dir: Optional[str] = None,
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

        # Upload asset manifest
        hash_alg = manifest.get_default_hash_alg()
        manifest_bytes = manifest.encode().encode("utf-8")
        manifest_name_prefix = hash_data(
            f"{file_system_location_name or ''}{str(source_root)}".encode(), hash_alg
        )
        manifest_name = f"{manifest_name_prefix}_input"

        if partial_manifest_prefix:
            partial_manifest_key = _join_s3_paths(partial_manifest_prefix, manifest_name)
        else:
            partial_manifest_key = manifest_name

        full_manifest_key = job_attachment_settings.add_root_and_manifest_folder_prefix(
            partial_manifest_key
        )

        if manifest_write_dir:
            self._write_local_manifest(
                manifest_write_dir, manifest_name, full_manifest_key, manifest
            )

        self.upload_bytes_to_s3(
            bytes=BytesIO(manifest_bytes),
            bucket=job_attachment_settings.s3BucketName,
            key=full_manifest_key,
        )

        # Upload assets
        self.upload_input_files(
            manifest,
            job_attachment_settings.s3BucketName,
            source_root,
            job_attachment_settings.full_cas_prefix(),
            progress_tracker,
            s3_check_cache_dir,
        )

        return (partial_manifest_key, hash_data(manifest_bytes, hash_alg))

    def _write_local_manifest(
        self,
        manifest_write_dir: str,
        manifest_name: str,
        full_manifest_key: str,
        manifest: BaseAssetManifest,
    ) -> None:
        """
        Writes a manifest file locally in a 'manifests' sub-directory.
        Also creates/appends to a file mapping the local manifest name to the full S3 key in the same directory.
        """
        local_manifest_file = Path(manifest_write_dir, "manifests", manifest_name)
        logger.info(f"Creating local manifest file: {local_manifest_file}\n")
        local_manifest_file.parent.mkdir(parents=True, exist_ok=True)
        with open(local_manifest_file, "w") as file:
            file.write(manifest.encode())

        # Create or append to an existing mapping file. We use this since path lengths can go beyond the
        # file name length limit on Windows if we were to create the full S3 key path locally.
        manifest_map_file = Path(manifest_write_dir, "manifests", "manifest_s3_mapping")
        mapping = {"local_file": manifest_name, "s3_key": full_manifest_key}
        with open(manifest_map_file, "a") as mapping_file:
            mapping_file.write(f"{mapping}\n")

    def upload_input_files(
        self,
        manifest: BaseAssetManifest,
        s3_bucket: str,
        source_root: Path,
        s3_cas_prefix: str,
        progress_tracker: Optional[ProgressTracker] = None,
        s3_check_cache_dir: Optional[str] = None,
    ) -> None:
        """
        Uploads all of the files listed in the given manifest to S3 if they don't exist in the
        given S3 prefix already.

        The local 'S3 check cache' is used to note if we've seen an object in S3 before so we
        can save the S3 API calls.
        """

        # Split into a separate 'large file' and 'small file' queues.
        # Separate 'large' files from 'small' files so that we can process 'large' files serially.
        # This wastes less bandwidth if uploads are cancelled, as it's better to use the multi-threaded
        # multi-part upload for a single large file than multiple large files at the same time.
        (small_file_queue, large_file_queue) = self._separate_files_by_size(
            manifest.paths, self.small_file_threshold
        )

        with S3CheckCache(s3_check_cache_dir) as s3_cache:
            # First, process the whole 'small file' queue with parallel object uploads.
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.num_upload_workers
            ) as executor:
                futures = {
                    executor.submit(
                        self.upload_object_to_cas,
                        file,
                        manifest.hashAlg,
                        s3_bucket,
                        source_root,
                        s3_cas_prefix,
                        s3_cache,
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
                    manifest.hashAlg,
                    s3_bucket,
                    source_root,
                    s3_cas_prefix,
                    s3_cache,
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

    def _get_current_timestamp(self) -> str:
        return str(datetime.now().timestamp())

    def upload_object_to_cas(
        self,
        file: base_manifest.BaseManifestPath,
        hash_algorithm: HashAlgorithm,
        s3_bucket: str,
        source_root: Path,
        s3_cas_prefix: str,
        s3_check_cache: S3CheckCache,
        progress_tracker: Optional[ProgressTracker] = None,
    ) -> Tuple[bool, int]:
        """
        Uploads an object to the S3 content-addressable storage (CAS) prefix. Optionally,
        does a head-object check and only uploads the file if it doesn't exist in S3 already.
        Returns a tuple (whether it has been uploaded, the file size).
        """
        local_path = source_root.joinpath(file.path)
        s3_upload_key = f"{file.hash}.{hash_algorithm.value}"
        if s3_cas_prefix:
            s3_upload_key = _join_s3_paths(s3_cas_prefix, s3_upload_key)
        is_uploaded = False
        file_size = local_path.resolve().stat().st_size

        if s3_check_cache.get_entry(s3_key=f"{s3_bucket}/{s3_upload_key}"):
            logger.debug(
                f"skipping {local_path} because {s3_bucket}/{s3_upload_key} exists in the cache"
            )
            return (is_uploaded, file_size)

        if self.file_already_uploaded(s3_bucket, s3_upload_key):
            logger.debug(
                f"skipping {local_path} because it has already been uploaded to s3://{s3_bucket}/{s3_upload_key}"
            )
        else:
            self.upload_file_to_s3(
                local_path=local_path,
                s3_bucket=s3_bucket,
                s3_upload_key=s3_upload_key,
                progress_tracker=progress_tracker,
            )
            is_uploaded = True

        s3_check_cache.put_entry(
            S3CheckCacheEntry(
                s3_key=f"{s3_bucket}/{s3_upload_key}",
                last_seen_time=self._get_current_timestamp(),
            )
        )

        return (is_uploaded, file_size)

    def upload_file_to_s3(
        self,
        local_path: Path,
        s3_bucket: str,
        s3_upload_key: str,
        progress_tracker: Optional[ProgressTracker] = None,
        base_dir_path: Optional[Path] = None,
    ) -> None:
        """
        Uploads a single file to an S3 bucket using TransferManager, allowing mid-way
        cancellation. It monitors for upload progress through a callback, `handler`,
        which also checks if the upload should continue or not. If the `progress_tracker`
        signals to stop, the ongoing upload is cancelled.
        """
        transfer_manager = get_s3_transfer_manager(s3_client=self._s3)

        future: concurrent.futures.Future

        def handler(bytes_uploaded):
            nonlocal progress_tracker
            nonlocal future

            if progress_tracker:
                should_continue = progress_tracker.track_progress_callback(bytes_uploaded)
                if not should_continue and future is not None:
                    future.cancel()

        subscribers = [ProgressCallbackInvoker(handler)]
        real_path = local_path.resolve()

        if base_dir_path:
            # If base_dir_path is given, check if the file is actually within the base directory
            is_file_within_base_dir = self._is_file_within_directory(real_path, base_dir_path)
        else:
            # If base_dir_path is not set, assume the file is within the base directory.
            is_file_within_base_dir = True

        # Skip the file if it's (1) a directory, 2. not existing, or 3. not within the base directory.
        if real_path.is_dir() or not real_path.exists() or not is_file_within_base_dir:
            return

        with self._open_non_symlink_file_binary(str(real_path)) as file_obj:
            if file_obj is None:
                return

            future = transfer_manager.upload(
                fileobj=file_obj,
                bucket=s3_bucket,
                key=s3_upload_key,
                subscribers=subscribers,
            )

            try:
                future.result()
                is_uploaded = True
                if progress_tracker and is_uploaded:
                    progress_tracker.increase_processed(1, 0)
            except concurrent.futures.CancelledError as ce:
                if progress_tracker and progress_tracker.continue_reporting is False:
                    raise AssetSyncCancelledError(
                        "File upload cancelled.", progress_tracker.get_summary_statistics()
                    )
                else:
                    raise AssetSyncError("File upload failed.", ce) from ce
            except ClientError as exc:
                status_code = int(exc.response["ResponseMetadata"]["HTTPStatusCode"])
                status_code_guidance = {
                    **COMMON_ERROR_GUIDANCE_FOR_S3,
                    403: (
                        (
                            "Forbidden or Access denied. Please check your AWS credentials, and ensure that "
                            "your AWS IAM Role or User has the 's3:PutObject' permission for this bucket. "
                        )
                        if "kms:" not in str(exc)
                        else (
                            "Forbidden or Access denied. Please check your AWS credentials and Job Attachments S3 bucket "
                            "encryption settings. If a customer-managed KMS key is set, confirm that your AWS IAM Role or "
                            "User has the 'kms:GenerateDataKey' and 'kms:DescribeKey' permissions for the key used to encrypt the bucket."
                        )
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
            except BotoCoreError as bce:
                raise JobAttachmentS3BotoCoreError(
                    action="uploading file",
                    error_details=str(bce),
                ) from bce
            except Exception as e:
                raise AssetSyncError(e) from e

    @contextmanager
    def _open_non_symlink_file_binary(
        self, path: str
    ) -> Generator[Optional[BufferedReader], None, None]:
        """
        Open a file in binary mode after verifying that it is not a symbolic link.
        Raises:
            OSError: If the given path is a symbolic link or doesn't match the actual file.
        """
        fd = None
        file_obj = None
        try:
            open_flags = os.O_RDONLY
            # Make sure the file isn’t following a symlink to a different path.
            if hasattr(os, "O_NOFOLLOW"):
                open_flags |= os.O_NOFOLLOW
            elif sys.platform != "win32" and not os.path.islink(path):
                # We are on a non-Windows system that does not support O_NOFOLLOW. When we encounter
                # symbolic link, we cannot guarantee security here, so log a warning and reject the file.
                logger.warning(
                    f"Job Attachments does not support files referenced by symbolic links on this system ({sys.platform}). "
                    "Please refrain from using symbolic links in Job Attachment asset roots and use real files instead. "
                    f"The following file will be skipped: {path}."
                )
                yield None

            fd = os.open(path, open_flags)
            if sys.platform == "win32":
                # Windows does not support O_NOFOLLOW. So, check the file handle with GetFinalPathNameByHandle
                # to verify it is actually pointing to the path that we verified to be safe to open.
                if not self._is_path_win32_final_path_of_file_descriptor(path, fd):
                    # ELOOP is the error code that open with NOFOLLOW will return
                    # if the path is a symlink.  We raise the same error here for
                    # the sake of consistency.
                    raise OSError(errno.ELOOP, "Mismatch between path and its final path", path)

            if str(Path(path).resolve()) != path:
                raise OSError(errno.ELOOP, "Mismatch between path and its final path", path)

            with os.fdopen(fd, "rb", closefd=False) as file_obj:
                yield file_obj
        except OSError as e:
            logger.warning(f"Failed to open file. The following file will be skipped: {path}: {e}")
            yield None
        finally:
            if fd is not None:
                os.close(fd)
            if file_obj is not None:
                file_obj.close()

    def _is_path_win32_final_path_of_file_descriptor(self, path: str, fd: int):
        """
        Check if the normalized path from the file descriptor matches the specified path.
        """
        if sys.platform != "win32":
            raise EnvironmentError("This function can only be executed on Windows systems.")

        import ctypes
        import msvcrt
        from ._windows import file as win_file

        # Get the handle from the file descriptor
        try:
            h = msvcrt.get_osfhandle(fd)
        except OSError as e:
            logger.warning(f"Error resolving file descriptor ({fd}) to '{path}': {e}")
            return False

        # Get the final path name using Win32 API GetFinalPathNameByHandleW
        buffer_len = 4096
        buffer = ctypes.create_unicode_buffer(buffer_len)
        path_len = win_file.GetFinalPathNameByHandleW(
            h,
            buffer,
            buffer_len,
            win_file.VOLUME_NAME_DOS,
        )
        if path_len == 0:
            ctypes.WinError()
        elif path_len > buffer_len:
            # path_len has the required buffer length (returned by GetFinalPathNameByHandleW)
            # Create a buffer of this size and call the API again
            buffer_len = path_len
            buffer = ctypes.create_unicode_buffer(buffer_len)
            path_len = win_file.GetFinalPathNameByHandleW(
                h,
                buffer,
                buffer_len,
                win_file.VOLUME_NAME_DOS,
            )

            if path_len != buffer_len or path_len == 0:
                # MS documentation states that if GetFinalPathNameByHandleW returns a positive value
                # greater than the initial buffer length, it is the required buffer length to fit the
                # path name. This branch uses the that value to create a new buffer, so this should
                # never fail unless GetFinalPathNameByHandleW behavior has changed.
                logger.error(
                    "GetFinalPathNameByHandleW reported incorrect required buffer length. "
                    f"Rejecting file at '{path}'"
                )
                return False

        final_path = ctypes.wstring_at(buffer)

        # GetFinalPathNameByHandleW() returns a path that starts with the \\?\
        # prefix, which pathlib.Path.resolve() removes.  The following is intended
        # to match the behavior of resolve().
        prefix = r"\\?" "\\"
        unc_prefix = r"\\?\UNC" "\\"

        if final_path.startswith(prefix) and not path.startswith(prefix):
            if final_path.startswith(unc_prefix):
                simplified_path = "\\\\" + final_path[len(unc_prefix) :]
            else:
                simplified_path = final_path[len(prefix) :]

            final_path = simplified_path

        return path == final_path

    def _is_file_within_directory(self, file_path: Path, directory_path: Path) -> bool:
        """
        Checks if the given file path is within the given directory path.
        """
        real_file_path = file_path.resolve()
        real_directory_path = directory_path.resolve()
        common_path = os.path.commonpath([real_file_path, real_directory_path])
        return common_path.startswith(str(real_directory_path))

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
        except BotoCoreError as bce:
            raise JobAttachmentS3BotoCoreError(
                action="checking for the existence of an object in the S3 bucket",
                error_details=str(bce),
            ) from bce
        except Exception as e:
            raise AssetSyncError(e) from e

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
                    (
                        "Forbidden or Access denied. Please check your AWS credentials, and ensure that "
                        "your AWS IAM Role or User has the 's3:PutObject' permission for this bucket. "
                    )
                    if "kms:" not in str(exc)
                    else (
                        "Forbidden or Access denied. Please check your AWS credentials and Job Attachments S3 bucket "
                        "encryption settings. If a customer-managed KMS key is set, confirm that your AWS IAM Role or "
                        "User has the 'kms:GenerateDataKey' and 'kms:DescribeKey' permissions for the key used to encrypt the bucket."
                    )
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
        except BotoCoreError as bce:
            raise JobAttachmentS3BotoCoreError(
                action="uploading binary file",
                error_details=str(bce),
            ) from bce
        except Exception as e:
            raise AssetSyncError(e) from e


class S3AssetManager:
    """
    Asset handler that creates an asset manifest and uploads assets. Based on an S3 file system.
    """

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
                "To use Job Attachments, the 's3BucketName' must be set in your queue's JobAttachmentSettings"
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
        hash_alg: HashAlgorithm = manifest_model.AssetManifest.get_default_hash_alg()

        full_path = str(path.resolve())
        is_new_or_modified: bool = False
        actual_modified_time = str(datetime.fromtimestamp(path.stat().st_mtime))

        entry: Optional[HashCacheEntry] = hash_cache.get_entry(full_path, hash_alg)
        if entry is not None:
            # If the file was modified, we need to rehash it
            if actual_modified_time != entry.last_modified_time:
                entry.last_modified_time = actual_modified_time
                entry.file_hash = hash_file(full_path, hash_alg)
                entry.hash_algorithm = hash_alg
                is_new_or_modified = True
        else:
            entry = HashCacheEntry(
                file_path=full_path,
                hash_algorithm=hash_alg,
                file_hash=hash_file(full_path, hash_alg),
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
                        progress_tracker.report_progress()

            # Need to sort the list to keep it canonical
            paths.sort(key=lambda x: x.path, reverse=True)

            manifest_args: dict[str, Any] = {
                "hash_alg": manifest_model.AssetManifest.get_default_hash_alg(),
                "paths": paths,
            }

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
        require_paths_exist: bool = False,
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
        missing_input_paths = set()
        misconfigured_directories = set()

        # Resolve full path, then cast to pure path to get top-level directory
        for _path in input_paths:
            # Need to use absolute to not resolve symlinks, but need normpath to get rid of relative paths, i.e. '..'
            abs_path = Path(os.path.normpath(Path(_path).absolute()))
            if not abs_path.exists():
                if require_paths_exist:
                    missing_input_paths.add(abs_path)
                else:
                    logger.warning(
                        f"Input path '{_path}' resolving to '{abs_path}' does not exist. Adding to referenced paths."
                    )
                    referenced_paths.add(_path)
                continue
            if abs_path.is_dir():
                misconfigured_directories.add(abs_path)
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
            matched_group = self._get_matched_group(matched_root, groupings)
            matched_group.inputs.add(abs_path)

        if missing_input_paths or misconfigured_directories:
            all_misconfigured_inputs = ""
            misconfigured_inputs_msg = (
                "Job submission contains missing input files or directories specified as files."
                " All inputs must exist and be classified properly."
            )
            if missing_input_paths:
                missing_inputs_list: list[str] = sorted([str(i) for i in missing_input_paths])
                all_missing_inputs = "\n\t".join(missing_inputs_list)
                all_misconfigured_inputs += f"\nMissing input files:\n\t{all_missing_inputs}"
            if misconfigured_directories:
                misconfigured_directories_list: list[str] = sorted(
                    [str(d) for d in misconfigured_directories]
                )
                all_misconfigured_directories = "\n\t".join(misconfigured_directories_list)
                all_misconfigured_inputs += (
                    f"\nDirectories classified as files:\n\t{all_misconfigured_directories}"
                )
            raise MisconfiguredInputsError(misconfigured_inputs_msg + all_misconfigured_inputs)

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
            matched_group = self._get_matched_group(matched_root, groupings)
            matched_group.outputs.add(abs_path)

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
            matched_group = self._get_matched_group(matched_root, groupings)
            matched_group.references.add(abs_path)

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

    def _get_matched_group(
        self, root_path: str, groupings: dict[str, AssetRootGroup]
    ) -> AssetRootGroup:
        root_normcase = os.path.normcase(root_path)
        matched_group = next(
            (group for key, group in groupings.items() if os.path.normcase(key) == root_normcase),
            None,
        )
        if matched_group is None:
            raise ValueError(
                f"No group found for the root path '{root_path}' in the groupings dictionary: {groupings}"
            )
        return matched_group

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
            keys_normcase = [os.path.normcase(key) for key in groupings.keys()]
            top_directory = PurePath(abs_path).parts[0]
            top_directory_normcase = os.path.normcase(top_directory)
            if top_directory_normcase not in keys_normcase:
                groupings[top_directory] = AssetRootGroup()
            else:
                return top_directory_normcase
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
        storage_profile_for_queue: StorageProfile,
    ) -> Tuple[dict, dict]:
        """
        Given the Storage Profile for Queue object, extracts and groups
        path and name pairs from the File System Locations into two dicts,
        LOCAL and SHARED type, respectively. Returns a tuple of two dicts.
        """
        local_type_locations: dict[str, str] = {}
        shared_type_locations: dict[str, str] = {}
        for fs_loc in storage_profile_for_queue.fileSystemLocations:
            if fs_loc.type == FileSystemLocationType.LOCAL:
                local_type_locations[fs_loc.path] = fs_loc.name
            elif fs_loc.type == FileSystemLocationType.SHARED:
                shared_type_locations[fs_loc.path] = fs_loc.name
        return local_type_locations, shared_type_locations

    def _group_asset_paths(
        self,
        input_paths: list[str],
        output_paths: list[str],
        referenced_paths: list[str],
        storage_profile: Optional[StorageProfile] = None,
        require_paths_exist: bool = False,
    ) -> list[AssetRootGroup]:
        """
        Resolves all of the paths that will be uploaded, sorting by storage profile location.
        """
        local_type_locations: dict[str, str] = {}
        shared_type_locations: dict[str, str] = {}
        if storage_profile:
            (
                local_type_locations,
                shared_type_locations,
            ) = self._get_file_system_locations_by_type(storage_profile)

        # Group the paths by asset root, removing duplicates and empty strings
        asset_groups: list[AssetRootGroup] = self._get_asset_groups(
            {ip_path for ip_path in input_paths if ip_path},
            {op_path for op_path in output_paths if op_path},
            {rf_path for rf_path in referenced_paths if rf_path},
            local_type_locations,
            shared_type_locations,
            require_paths_exist,
        )

        return asset_groups

    def prepare_paths_for_upload(
        self,
        input_paths: list[str],
        output_paths: list[str],
        referenced_paths: list[str],
        storage_profile: Optional[StorageProfile] = None,
        require_paths_exist: bool = False,
    ) -> AssetUploadGroup:
        """
        Processes all of the paths required for upload, grouping them by asset root and local storage profile locations.
        Returns an object containing the grouped paths, which also includes a dictionary of input directories and file counts
        for files that were not under the root path or any local storage profile locations.
        """
        asset_groups = self._group_asset_paths(
            input_paths,
            output_paths,
            referenced_paths,
            storage_profile,
            require_paths_exist,
        )
        (input_file_count, input_bytes) = self._get_total_input_size_from_asset_group(asset_groups)
        return AssetUploadGroup(
            asset_groups=asset_groups,
            total_input_files=input_file_count,
            total_input_bytes=input_bytes,
        )

    def hash_assets_and_create_manifest(
        self,
        asset_groups: list[AssetRootGroup],
        total_input_files: int,
        total_input_bytes: int,
        hash_cache_dir: Optional[str] = None,
        on_preparing_to_submit: Optional[Callable[[Any], bool]] = None,
    ) -> tuple[SummaryStatistics, list[AssetRootManifest]]:
        """
        Computes the hashes for input files, and creates manifests using the local hash cache.

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

        # Sets up progress tracker to report upload progress back to the caller.
        progress_tracker = ProgressTracker(
            status=ProgressStatus.PREPARING_IN_PROGRESS,
            total_files=total_input_files,
            total_bytes=total_input_bytes,
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
        s3_check_cache_dir: Optional[str] = None,
        manifest_write_dir: Optional[str] = None,
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
                    s3_check_cache_dir=s3_check_cache_dir,
                    manifest_write_dir=manifest_write_dir,
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
