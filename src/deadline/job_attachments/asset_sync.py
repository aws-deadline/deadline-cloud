# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

""" Module for File Attachment synching """
from __future__ import annotations
import os
import shutil
import sys
import time
from io import BytesIO
from logging import Logger, LoggerAdapter, getLogger
from math import trunc
from pathlib import Path, PurePosixPath
from typing import Any, Callable, DefaultDict, Dict, List, Optional, Tuple, Type, Union

import boto3

from .progress_tracker import (
    ProgressReportMetadata,
    ProgressStatus,
    ProgressTracker,
    SummaryStatistics,
)

from .asset_manifests import (
    BaseAssetManifest,
    BaseManifestModel,
    HashAlgorithm,
    hash_data,
    hash_file,
    ManifestModelRegistry,
    ManifestVersion,
)
from .asset_manifests import BaseManifestPath as RelativeFilePath
from ._aws.aws_clients import get_boto3_session
from ._aws.deadline import get_job, get_queue
from .download import (
    merge_asset_manifests,
    download_files_from_manifests,
    get_manifest_from_s3,
    get_output_manifests_by_asset_root,
    mount_vfs_from_manifests,
)

from .exceptions import (
    AssetSyncError,
    VFSExecutableMissingError,
    JobAttachmentsS3ClientError,
    VFSOSUserNotSetError,
)
from .vfs import VFSProcessManager
from .models import (
    Attachments,
    JobAttachmentsFileSystem,
    JobAttachmentS3Settings,
    ManifestProperties,
    OutputFile,
    PathFormat,
)
from .upload import S3AssetUploader
from .os_file_permission import FileSystemPermissionSettings, PosixFileSystemPermissionSettings
from ._utils import (
    _float_to_iso_datetime_string,
    _get_unique_dest_dir_name,
    _human_readable_file_size,
    _join_s3_paths,
)

logger = getLogger("deadline.job_attachments")


class AssetSync:
    """Class for managing AWS Deadline Cloud job-level attachments."""

    _ENDING_PROGRESS = 100.0

    def __init__(
        self,
        farm_id: str,
        boto3_session: Optional[boto3.Session] = None,
        manifest_version: ManifestVersion = ManifestVersion.v2023_03_03,
        deadline_endpoint_url: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> None:
        self.farm_id = farm_id

        self.logger: Union[Logger, LoggerAdapter] = logger
        if session_id:
            self.logger = LoggerAdapter(logger, {"session_id": session_id})

        self.session: boto3.Session
        if boto3_session is None:
            self.session = get_boto3_session()
        else:
            self.session = boto3_session

        self.deadline_endpoint_url = deadline_endpoint_url
        self.s3_uploader: S3AssetUploader = S3AssetUploader(session=boto3_session)
        self.manifest_model: Type[BaseManifestModel] = ManifestModelRegistry.get_manifest_model(
            version=manifest_version
        )

        # A dictionary mapping absolute file paths to their last modification times in microseconds.
        # This is used to determine if an asset has been modified since it was last synced.
        self.synced_assets_mtime: dict[str, int] = dict()

        self.hash_alg: HashAlgorithm = self.manifest_model.AssetManifest.get_default_hash_alg()

    def _upload_output_files_to_s3(
        self,
        s3_settings: JobAttachmentS3Settings,
        output_files: List[OutputFile],
        on_uploading_files: Optional[Callable[[ProgressReportMetadata], bool]],
    ) -> SummaryStatistics:
        """
        Uploads the given output files to the given S3 bucket.
        Sets up `progress_tracker` to report upload progress back to the caller (i.e. worker.)
        """
        # Sets up progress tracker to report upload progress back to the caller.
        total_file_size = sum([file.file_size for file in output_files])
        progress_tracker = ProgressTracker(
            status=ProgressStatus.UPLOAD_IN_PROGRESS,
            total_files=len(output_files),
            total_bytes=total_file_size,
            on_progress_callback=on_uploading_files,
            logger=self.logger,
        )

        start_time = time.perf_counter()

        for file in output_files:
            if file.in_s3:
                progress_tracker.increase_skipped(1, file.file_size)
                continue

            self.s3_uploader.upload_file_to_s3(
                local_path=Path(file.full_path),
                s3_bucket=s3_settings.s3BucketName,
                s3_upload_key=file.s3_key,
                progress_tracker=progress_tracker,
                base_dir_path=Path(file.base_dir) if file.base_dir else None,
            )

        progress_tracker.total_time = time.perf_counter() - start_time
        return progress_tracker.get_summary_statistics()

    def _upload_output_manifest_to_s3(
        self,
        s3_settings: JobAttachmentS3Settings,
        output_manifest: BaseAssetManifest,
        full_output_prefix: str,
        root_path: str,
        file_system_location_name: Optional[str] = None,
    ) -> None:
        """Uploads the given output manifest to the given S3 bucket."""
        hash_alg = output_manifest.get_default_hash_alg()
        manifest_bytes = output_manifest.encode().encode("utf-8")
        manifest_name_prefix = hash_data(
            f"{file_system_location_name or ''}{root_path}".encode(), hash_alg
        )
        manifest_path = _join_s3_paths(
            full_output_prefix,
            f"{manifest_name_prefix}_output",
        )
        metadata = {"Metadata": {"asset-root": root_path}}
        if file_system_location_name:
            metadata["Metadata"]["file-system-location-name"] = file_system_location_name

        self.logger.info(f"Uploading output manifest to {manifest_path}")

        self.s3_uploader.upload_bytes_to_s3(
            BytesIO(manifest_bytes),
            s3_settings.s3BucketName,
            manifest_path,
            extra_args=metadata,
        )

    def _generate_output_manifest(self, outputs: List[OutputFile]) -> BaseAssetManifest:
        paths: list[RelativeFilePath] = []
        for output in outputs:
            path_args: dict[str, Any] = {
                "hash": output.file_hash,
                "path": output.rel_path,
            }
            path_args["size"] = output.file_size
            # stat().st_mtime_ns returns an int that represents the time in nanoseconds since the epoch.
            # The asset manifest spec requires the mtime to be represented as an integer in microseconds.
            path_args["mtime"] = trunc(Path(output.full_path).stat().st_mtime_ns // 1000)
            paths.append(self.manifest_model.Path(**path_args))

        asset_manifest_args: dict[str, Any] = {
            "paths": paths,
            "hash_alg": self.hash_alg,
        }
        asset_manifest_args["total_size"] = sum([output.file_size for output in outputs])

        return self.manifest_model.AssetManifest(**asset_manifest_args)  # type: ignore[call-arg]

    def _get_output_files(
        self,
        manifest_properties: ManifestProperties,
        s3_settings: JobAttachmentS3Settings,
        local_root: Path,
        session_dir: Path,
    ) -> List[OutputFile]:
        """
        Walks the output directories for this asset root for any output files that have been created or modified
        since the start time provided. Hashes and checks if the output files already exist in the CAS.
        """
        output_files: List[OutputFile] = []

        source_path_format = manifest_properties.rootPathFormat
        current_path_format = PathFormat.get_host_path_format()

        for output_dir in manifest_properties.outputRelativeDirectories or []:
            if source_path_format != current_path_format:
                if source_path_format == PathFormat.WINDOWS:
                    output_dir = output_dir.replace("\\", "/")
                elif source_path_format == PathFormat.POSIX:
                    output_dir = output_dir.replace("/", "\\")
            output_root: Path = local_root / output_dir

            total_file_count = 0
            total_file_size = 0

            # Don't fail if output dir hasn't been created yet; another task might be working on it
            if not output_root.is_dir():
                self.logger.info(f"Found 0 files (Output directory {output_root} does not exist.)")
                continue

            # Get all files in this directory (includes sub-directories)
            for file_path in output_root.glob("**/*"):
                # Files that are new or have been modified since the last sync will be added to the output list.
                mtime_when_synced = self.synced_assets_mtime.get(str(file_path), None)
                file_mtime = file_path.stat().st_mtime_ns
                is_modified = False
                if mtime_when_synced:
                    if file_mtime > int(mtime_when_synced):
                        # This file has been modified during this session action.
                        is_modified = True
                else:
                    # This is a new file created during this session action.
                    self.synced_assets_mtime[str(file_path)] = int(file_mtime)
                    is_modified = True

                # Resolve the real path to prevent time-of-check/time-of-use vulnerability
                file_real_path = file_path.resolve()

                # validate that the file resolves inside of the session working directory.
                is_file_path_under_session_dir = self._is_file_within_directory(
                    file_real_path, session_dir
                )
                if is_file_path_under_session_dir is False:
                    self.logger.info(
                        f"Skipping file '{file_path}' as its resolved path '{file_real_path}' is"
                        f" outside the session directory '{session_dir}'"
                    )
                    continue

                if (
                    not file_real_path.is_dir()
                    and file_real_path.exists()
                    and is_modified
                    and is_file_path_under_session_dir
                ):
                    file_size = file_real_path.resolve().lstat().st_size
                    file_hash = hash_file(str(file_real_path), self.hash_alg)
                    s3_key = f"{file_hash}.{self.hash_alg.value}"

                    if s3_settings.full_cas_prefix():
                        s3_key = _join_s3_paths(s3_settings.full_cas_prefix(), s3_key)
                    in_s3 = self.s3_uploader.file_already_uploaded(s3_settings.s3BucketName, s3_key)

                    total_file_count += 1
                    total_file_size += file_size

                    output_files.append(
                        OutputFile(
                            file_size=file_size,
                            file_hash=file_hash,
                            rel_path=str(PurePosixPath(*file_path.relative_to(local_root).parts)),
                            full_path=str(file_real_path),
                            s3_key=s3_key,
                            in_s3=in_s3,
                            base_dir=str(session_dir),
                        )
                    )

            self.logger.info(
                f"Found {total_file_count} file{'' if total_file_count == 1 else 's'}"
                f" totaling {_human_readable_file_size(total_file_size)}"
                f" in output directory: {str(output_root)}"
            )

        return output_files

    def _is_file_within_directory(self, file_path: Path, directory_path: Path) -> bool:
        """
        Checks if the given file path is within the given directory path.
        """
        real_file_path = file_path.resolve()
        real_directory_path = directory_path.resolve()
        common_path = os.path.commonpath([real_file_path, real_directory_path])
        return common_path.startswith(str(real_directory_path))

    def get_s3_settings(self, farm_id: str, queue_id: str) -> Optional[JobAttachmentS3Settings]:
        """
        Gets Job Attachment S3 settings by calling the Deadline GetQueue API.
        """
        queue = get_queue(
            farm_id=farm_id,
            queue_id=queue_id,
            session=self.session,
            deadline_endpoint_url=self.deadline_endpoint_url,
        )
        return queue.jobAttachmentSettings if queue and queue.jobAttachmentSettings else None

    def get_attachments(self, farm_id: str, queue_id: str, job_id: str) -> Optional[Attachments]:
        """
        Gets Job Attachment settings by calling the Deadline GetJob API.
        """
        job = get_job(
            farm_id=farm_id,
            queue_id=queue_id,
            job_id=job_id,
            session=self.session,
            deadline_endpoint_url=self.deadline_endpoint_url,
        )
        return job.attachments if job and job.attachments else None

    def _record_attachment_mtimes(
        self, merged_manifests_by_root: dict[str, BaseAssetManifest]
    ) -> None:
        # Record the mapping of downloaded files' absolute paths to their last modification time
        # (in microseconds). This is used to later determine which files have been modified or
        # newly created during the session and need to be uploaded as output.
        for local_root, merged_manifest in merged_manifests_by_root.items():
            for manifest_path in merged_manifest.paths:
                abs_path = str(Path(local_root) / manifest_path.path)
                self.synced_assets_mtime[abs_path] = Path(abs_path).stat().st_mtime_ns

    def _ensure_disk_capacity(self, session_dir: Path, total_input_bytes: int) -> None:
        """
        Raises an AssetSyncError if the given input bytes is larger than the available disk space.
        """
        disk_free: int = shutil.disk_usage(session_dir).free
        if total_input_bytes > disk_free:
            input_size_readable = _human_readable_file_size(total_input_bytes)
            disk_free_readable = _human_readable_file_size(disk_free)
            raise AssetSyncError(
                "Error occurred while attempting to sync input files: "
                f"Total file size required for download ({input_size_readable}) is larger than available disk space ({disk_free_readable})"
            )

    def sync_inputs(
        self,
        s3_settings: Optional[JobAttachmentS3Settings],
        attachments: Optional[Attachments],
        queue_id: str,
        job_id: str,
        session_dir: Path,
        fs_permission_settings: Optional[FileSystemPermissionSettings] = None,
        storage_profiles_path_mapping_rules: dict[str, str] = {},
        step_dependencies: Optional[list[str]] = None,
        on_downloading_files: Optional[Callable[[ProgressReportMetadata], bool]] = None,
        os_env_vars: Dict[str, str] | None = None,
    ) -> Tuple[SummaryStatistics, List[Dict[str, str]]]:
        """
        Depending on the fileSystem in the Attachments this will perform two
        different behaviors:
            COPIED / None : downloads a manifest file and corresponding input files, if found.
            VIRTUAL: downloads a manifest file and mounts a Virtual File System at the
                       specified asset root corresponding to the manifest contents

        Args:
            s3_settings: S3-specific Job Attachment settings.
            attachments: an object that holds all input assets for the job.
            queue_id: the ID of the queue.
            job_id: the ID of the job.
            session_dir: the directory that the session is going to use.
            fs_permission_settings: An instance defining group ownership and permission modes
                to be set on the downloaded (synchronized) input files and directories.
            storage_profiles_path_mapping_rules: A dict of source path -> destination path mappings.
                If this dict is not empty, it means that the Storage Profile set in the job is
                different from the one configured in the Fleet performing the input-syncing.
            step_dependencies: the list of Step IDs whose output should be downloaded over the input
                job attachments.
            on_downloading_files: a function that will be called with a ProgressReportMetadata object
                for each file being downloaded. If the function returns False, the download will be
                cancelled. If it returns True, the download will continue.
            os_env_vars: environment variables to set for launched subprocesses

        Returns:
            COPIED / None : a tuple of (1) final summary statistics for file downloads,
                             and (2) a list of local roots for each asset root, used for
                             path mapping.
            VIRTUAL: same as COPIED, but the summary statistics will be empty since the
                       download hasn't started yet.
        """
        if not s3_settings:
            self.logger.info(
                f"No Job Attachment settings configured for Queue {queue_id}, no inputs to sync."
            )
            return (SummaryStatistics(), [])
        if not attachments:
            self.logger.info(f"No attachments configured for Job {job_id}, no inputs to sync.")
            return (SummaryStatistics(), [])

        grouped_manifests_by_root: DefaultDict[str, list[BaseAssetManifest]] = DefaultDict(list)
        pathmapping_rules: Dict[str, Dict[str, str]] = {}

        storage_profiles_source_paths = list(storage_profiles_path_mapping_rules.keys())

        for manifest_properties in attachments.manifests:
            local_root: str = ""
            if (
                len(storage_profiles_path_mapping_rules) > 0
                and manifest_properties.fileSystemLocationName
            ):
                if manifest_properties.rootPath in storage_profiles_source_paths:
                    local_root = storage_profiles_path_mapping_rules[manifest_properties.rootPath]
                else:
                    raise AssetSyncError(
                        "Error occurred while attempting to sync input files: "
                        f"No path mapping rule found for the source path {manifest_properties.rootPath}"
                    )
            else:
                dir_name: str = _get_unique_dest_dir_name(manifest_properties.rootPath)
                local_root = str(session_dir.joinpath(dir_name))
                pathmapping_rules[dir_name] = {
                    "source_path_format": manifest_properties.rootPathFormat.value,
                    "source_path": manifest_properties.rootPath,
                    "destination_path": local_root,
                }

            if manifest_properties.inputManifestPath:
                manifest_s3_key = s3_settings.add_root_and_manifest_folder_prefix(
                    manifest_properties.inputManifestPath
                )
                manifest = get_manifest_from_s3(
                    manifest_key=manifest_s3_key,
                    s3_bucket=s3_settings.s3BucketName,
                    session=self.session,
                )
                grouped_manifests_by_root[local_root].append(manifest)

        # Handle step-step dependencies.
        if step_dependencies:
            for step_id in step_dependencies:
                manifests_by_root = get_output_manifests_by_asset_root(
                    s3_settings,
                    self.farm_id,
                    queue_id,
                    job_id,
                    step_id=step_id,
                    session=self.session,
                )
                for root, manifests in manifests_by_root.items():
                    dir_name = _get_unique_dest_dir_name(root)
                    local_root = str(session_dir.joinpath(dir_name))
                    grouped_manifests_by_root[local_root].extend(manifests)

        # Merge the manifests in each root into a single manifest
        merged_manifests_by_root: dict[str, BaseAssetManifest] = dict()
        total_input_size: int = 0
        for root, manifests in grouped_manifests_by_root.items():
            merged_manifest = merge_asset_manifests(manifests)

            if merged_manifest:
                merged_manifests_by_root[root] = merged_manifest
                total_input_size += merged_manifest.totalSize  # type: ignore[attr-defined]

        # Download
        # Virtual Download Flow
        if (
            attachments.fileSystem == JobAttachmentsFileSystem.VIRTUAL.value
            and sys.platform != "win32"
            and fs_permission_settings is not None
            and os_env_vars is not None
            and "AWS_PROFILE" in os_env_vars
            and isinstance(fs_permission_settings, PosixFileSystemPermissionSettings)
        ):
            try:
                VFSProcessManager.find_vfs()
                mount_vfs_from_manifests(
                    s3_bucket=s3_settings.s3BucketName,
                    manifests_by_root=merged_manifests_by_root,
                    boto3_session=self.session,
                    session_dir=session_dir,
                    fs_permission_settings=fs_permission_settings,  # type: ignore[arg-type]
                    os_env_vars=os_env_vars,  # type: ignore[arg-type]
                    cas_prefix=s3_settings.full_cas_prefix(),
                )
                summary_statistics = SummaryStatistics()
                self._record_attachment_mtimes(merged_manifests_by_root)
                return (summary_statistics, list(pathmapping_rules.values()))
            except VFSExecutableMissingError:
                logger.error(
                    f"Virtual File System not found, falling back to {JobAttachmentsFileSystem.COPIED} for JobAttachmentsFileSystem."
                )

        # Copied Download flow
        self._ensure_disk_capacity(session_dir, total_input_size)
        try:
            download_summary_statistics = download_files_from_manifests(
                s3_bucket=s3_settings.s3BucketName,
                manifests_by_root=merged_manifests_by_root,
                cas_prefix=s3_settings.full_cas_prefix(),
                fs_permission_settings=fs_permission_settings,
                session=self.session,
                on_downloading_files=on_downloading_files,
                logger=self.logger,
            )
        except JobAttachmentsS3ClientError as exc:
            if exc.status_code == 404:
                raise JobAttachmentsS3ClientError(
                    action=exc.action,
                    status_code=exc.status_code,
                    bucket_name=exc.bucket_name,
                    key_or_prefix=exc.key_or_prefix,
                    message=(
                        "This can happen if the S3 check cache on the submitting machine is out of date. "
                        "Please delete the cache file from the submitting machine, usually located in the "
                        "home directory (~/.deadline/cache/s3_check_cache.db) and try submitting again."
                    ),
                ) from exc
            else:
                raise

        self._record_attachment_mtimes(merged_manifests_by_root)

        return (
            download_summary_statistics.convert_to_summary_statistics(),
            list(pathmapping_rules.values()),
        )

    def sync_outputs(
        self,
        s3_settings: Optional[JobAttachmentS3Settings],
        attachments: Optional[Attachments],
        queue_id: str,
        job_id: str,
        step_id: str,
        task_id: str,
        session_action_id: str,
        start_time: float,
        session_dir: Path,
        storage_profiles_path_mapping_rules: dict[str, str] = {},
        on_uploading_files: Optional[Callable[[ProgressReportMetadata], bool]] = None,
    ) -> SummaryStatistics:
        """Uploads any output files specified in the manifest, if found."""
        if not s3_settings:
            self.logger.info(
                f"No Job Attachment settings configured for Queue {queue_id}, no outputs to sync."
            )
            return SummaryStatistics()
        if not attachments:
            self.logger.info(f"No attachments configured for Job {job_id}, no outputs to sync.")
            return SummaryStatistics()

        all_output_files: List[OutputFile] = []

        storage_profiles_source_paths = list(storage_profiles_path_mapping_rules.keys())

        for manifest_properties in attachments.manifests:
            session_root = session_dir
            local_root: Path = Path()
            if (
                len(storage_profiles_path_mapping_rules) > 0
                and manifest_properties.fileSystemLocationName
            ):
                if manifest_properties.rootPath in storage_profiles_source_paths:
                    local_root = Path(
                        storage_profiles_path_mapping_rules[manifest_properties.rootPath]
                    )
                    # We use session_root to filter out any files resolved to a location outside
                    # of that directory. If storage profile's path mapping rules are available,
                    # we can consider the session_root to be the mapped-storage profile path.
                    session_root = local_root
                else:
                    raise AssetSyncError(
                        "Error occurred while attempting to sync output files: "
                        f"No path mapping rule found for the source path {manifest_properties.rootPath}"
                    )
            else:
                dir_name: str = _get_unique_dest_dir_name(manifest_properties.rootPath)
                local_root = session_dir.joinpath(dir_name)

            output_files: List[OutputFile] = self._get_output_files(
                manifest_properties,
                s3_settings,
                local_root,
                session_root,
            )
            if output_files:
                output_manifest = self._generate_output_manifest(output_files)
                session_action_id_with_time_stamp = (
                    f"{_float_to_iso_datetime_string(start_time)}_{session_action_id}"
                )
                full_output_prefix = s3_settings.full_output_prefix(
                    farm_id=self.farm_id,
                    queue_id=queue_id,
                    job_id=job_id,
                    step_id=step_id,
                    task_id=task_id,
                    session_action_id=session_action_id_with_time_stamp,
                )
                self._upload_output_manifest_to_s3(
                    s3_settings=s3_settings,
                    output_manifest=output_manifest,
                    full_output_prefix=full_output_prefix,
                    root_path=manifest_properties.rootPath,
                    file_system_location_name=manifest_properties.fileSystemLocationName,
                )
                all_output_files.extend(output_files)

        if all_output_files:
            num_output_files = len(all_output_files)
            self.logger.info(
                f"Uploading {num_output_files} output file{'' if num_output_files == 1 else 's'}"
                f" to S3: {s3_settings.s3BucketName}/{s3_settings.full_cas_prefix()}"
            )
            summary_stats: SummaryStatistics = self._upload_output_files_to_s3(
                s3_settings, all_output_files, on_uploading_files
            )
        else:
            summary_stats = SummaryStatistics()
        return summary_stats

    def cleanup_session(
        self,
        session_dir: Path,
        file_system: JobAttachmentsFileSystem,
        os_user: Optional[str] = None,
    ):
        if file_system == JobAttachmentsFileSystem.COPIED.value:
            return
        if not os_user:
            raise VFSOSUserNotSetError("No os user set - can't clean up vfs session")
        try:
            VFSProcessManager.find_vfs()
            # Shutdown all running Deadline VFS processes since session is complete
            VFSProcessManager.kill_all_processes(session_dir=session_dir, os_user=os_user)
        except VFSExecutableMissingError:
            logger.error("Virtual File System not found, no processes to kill.")
