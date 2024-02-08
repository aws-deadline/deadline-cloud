# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

""" Module for File Attachment synching """
from __future__ import annotations
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

from .asset_manifests.decode import decode_manifest
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

from .fus3 import Fus3ProcessManager
from .exceptions import AssetSyncError, Fus3ExecutableMissingError, JobAttachmentsS3ClientError
from .models import (
    Attachments,
    JobAttachmentsFileSystem,
    JobAttachmentS3Settings,
    ManifestProperties,
    OutputFile,
    PathFormat,
)
from .upload import S3AssetUploader
from .os_file_permission import FileSystemPermissionSettings
from ._utils import (
    _float_to_iso_datetime_string,
    _get_unique_dest_dir_name,
    _human_readable_file_size,
    _join_s3_paths,
)

logger = getLogger("deadline.job_attachments")


class AssetSync:
    """Class for managing Amazon Deadline Cloud job-level attachments."""

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
                Path(file.full_path),
                s3_settings.s3BucketName,
                file.s3_key,
                progress_tracker,
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
        # TODO: Remove hash algorithm file extension after sufficient time after the next release
        manifest_path = _join_s3_paths(
            full_output_prefix,
            f"{manifest_name_prefix}_output.{output_manifest.hashAlg.value}",
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
        start_time: float,
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
                if (
                    not file_path.is_dir()
                    and file_path.exists()
                    and file_path.lstat().st_mtime >= start_time
                ):
                    file_size = file_path.lstat().st_size
                    file_hash = hash_file(str(file_path), self.hash_alg)
                    # TODO: replace with uncommented line below after sufficient time after the next release
                    s3_key = file_hash  # f"{file_hash}.{self.hash_alg.value}"

                    if s3_settings.full_cas_prefix():
                        s3_key = _join_s3_paths(s3_settings.full_cas_prefix(), s3_key)
                    in_s3 = self.s3_uploader.file_already_uploaded(s3_settings.s3BucketName, s3_key)

                    total_file_count += 1
                    total_file_size += file_path.lstat().st_size

                    output_files.append(
                        OutputFile(
                            file_size=file_size,
                            file_hash=file_hash,
                            rel_path=str(PurePosixPath(*file_path.relative_to(local_root).parts)),
                            full_path=str(file_path.resolve()),
                            s3_key=s3_key,
                            in_s3=in_s3,
                        )
                    )

            self.logger.info(
                f"Found {total_file_count} file{'' if total_file_count == 1 else 's'}"
                f" totaling {_human_readable_file_size(total_file_size)}"
                f" in output directory: {str(output_root)}"
            )

        return output_files

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

        grouped_manifests_by_root: DefaultDict[
            str, list[tuple[BaseAssetManifest, str]]
        ] = DefaultDict(list)
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
                manifest_path = get_manifest_from_s3(
                    manifest_key=manifest_s3_key,
                    s3_bucket=s3_settings.s3BucketName,
                    session=self.session,
                )
                with open(manifest_path) as manifest_file:
                    manifest = decode_manifest(manifest_file.read())
                grouped_manifests_by_root[local_root].append((manifest, manifest_path))

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
        for root, manifests in grouped_manifests_by_root.items():
            merged_manifest = merge_asset_manifests([manifest[0] for manifest in manifests])

            if merged_manifest:
                merged_manifests_by_root[root] = merged_manifest

        # Download

        if (
            attachments.fileSystem == JobAttachmentsFileSystem.VIRTUAL.value
            and sys.platform != "win32"
            and fs_permission_settings is not None
            and os_env_vars is not None
            and "AWS_PROFILE" in os_env_vars
        ):
            try:
                Fus3ProcessManager.find_fus3()
                mount_vfs_from_manifests(
                    s3_bucket=s3_settings.s3BucketName,
                    manifests_by_root=merged_manifests_by_root,
                    boto3_session=self.session,
                    session_dir=session_dir,
                    os_user=fs_permission_settings.os_user,  # type: ignore[union-attr]
                    os_env_vars=os_env_vars,  # type: ignore[arg-type]
                    cas_prefix=s3_settings.full_cas_prefix(),
                )
                summary_statistics = SummaryStatistics()
                return (summary_statistics, list(pathmapping_rules.values()))
            except Fus3ExecutableMissingError:
                logger.error(
                    f"Virtual File System not found, falling back to {JobAttachmentsFileSystem.COPIED} for JobAttachmentsFileSystem."
                )

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

        try:
            for manifest_properties in attachments.manifests:
                local_root: Path = Path()
                if (
                    len(storage_profiles_path_mapping_rules) > 0
                    and manifest_properties.fileSystemLocationName
                ):
                    if manifest_properties.rootPath in storage_profiles_source_paths:
                        local_root = Path(
                            storage_profiles_path_mapping_rules[manifest_properties.rootPath]
                        )
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
                    start_time,
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
        finally:
            if attachments.fileSystem == JobAttachmentsFileSystem.VIRTUAL.value:
                try:
                    Fus3ProcessManager.find_fus3()
                    # Shutdown all running Fus3 processes since task is completed
                    Fus3ProcessManager.kill_all_processes(session_dir=session_dir)
                except Fus3ExecutableMissingError:
                    logger.error("Virtual File System not found, no processes to kill.")
        return summary_stats
