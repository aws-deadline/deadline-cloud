# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import datetime
from io import BytesIO
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.client import BaseClient

from deadline.client.api._session import _get_queue_user_boto3_session, get_default_client_config
from deadline.client.cli._groups.click_logger import ClickLogger
from deadline.job_attachments._aws.aws_clients import get_s3_client, get_s3_transfer_manager
from deadline.job_attachments._diff import _fast_file_list_to_manifest_diff, compare_manifest
from deadline.job_attachments._glob import _process_glob_inputs, _glob_paths
from deadline.job_attachments.asset_manifests._create_manifest import (
    _create_manifest_for_single_root,
)
from deadline.job_attachments.asset_manifests.base_manifest import (
    BaseManifestPath,
)
from deadline.job_attachments.asset_manifests.decode import decode_manifest
from deadline.job_attachments.asset_manifests.hash_algorithms import hash_data
from deadline.job_attachments.download import download_file_with_s3_key
from deadline.job_attachments.models import (
    S3_MANIFEST_FOLDER_NAME,
    FileStatus,
    GlobConfig,
    ManifestDownload,
    ManifestDownloadResponse,
    ManifestSnapshot,
    default_glob_all,
)
from deadline.job_attachments.upload import S3AssetUploader

"""
APIs here should be business logic only. It should perform one thing, and one thing well. 
It should use basic primitives like S3 upload, download, boto3 APIs.
These APIs should be boto3 session agnostic and a specific Boto3 Credential to use.
"""


def _manifest_snapshot(
    root: str,
    destination: str,
    name: str,
    include: Optional[List[str]] = None,
    exclude: Optional[List[str]] = None,
    include_exclude_config: Optional[str] = None,
    diff: Optional[str] = None,
    force_rehash: bool = False,
    logger: ClickLogger = ClickLogger(False),
) -> Optional[ManifestSnapshot]:

    # Get all files in the root.
    glob_config: GlobConfig
    if include or exclude:
        include = include if include is not None else default_glob_all()
        exclude = exclude if exclude is not None else []
        glob_config = GlobConfig(include_glob=include, exclude_glob=exclude)
    elif include_exclude_config:
        glob_config = _process_glob_inputs(include_exclude_config)
    else:
        # Default, include all.
        glob_config = GlobConfig()

    current_files = _glob_paths(
        root, include=glob_config.include_glob, exclude=glob_config.exclude_glob
    )

    # Compute the output manifest immediately and hash.
    if not diff:
        output_manifest = _create_manifest_for_single_root(
            files=current_files, root=root, logger=logger
        )
        if not output_manifest:
            return None

    # If this is a diff manifest, load the supplied manifest file.
    else:
        # Parse local manifest
        with open(diff) as source_diff:
            source_manifest_str = source_diff.read()
            source_manifest = decode_manifest(source_manifest_str)

        # Get the differences
        changed_paths: List[str] = []

        # Fast comparison using time stamps and sizes.
        if not force_rehash:
            changed_paths = _fast_file_list_to_manifest_diff(
                root, current_files, source_manifest, logger
            )
        else:
            # In "slow / thorough" mode, we check by hash, which is definitive.
            output_manifest = _create_manifest_for_single_root(
                files=current_files, root=root, logger=logger
            )
            if not output_manifest:
                return None
            differences: List[Tuple[FileStatus, BaseManifestPath]] = compare_manifest(
                source_manifest, output_manifest
            )
            for diff_item in differences:
                if diff_item[0] == FileStatus.MODIFIED or diff_item[0] == FileStatus.NEW:
                    full_diff_path = f"{root}/{diff_item[1].path}"
                    changed_paths.append(full_diff_path)
                    logger.echo(f"Found difference at: {full_diff_path}, Status: {diff_item[0]}")

        # If there were no files diffed, return None, there was nothing to snapshot.
        if len(changed_paths) == 0:
            return None

        # Since the files are already hashed, we can easily re-use has_attachments to remake a diff manifest.
        output_manifest = _create_manifest_for_single_root(
            files=changed_paths, root=root, logger=logger
        )
        if not output_manifest:
            return None

    # Write created manifest into local file, at the specified location at destination
    if output_manifest is not None:
        # Encode the root path as
        root_hash: str = hash_data(root.encode("utf-8"), output_manifest.get_default_hash_alg())
        timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
        manifest_name = name if name else root.replace("/", "_")
        manifest_name = manifest_name[1:] if manifest_name[0] == "_" else manifest_name
        manifest_name = f"{manifest_name}-{root_hash}-{timestamp}.manifest"

        local_manifest_file = os.path.join(destination, manifest_name)
        os.makedirs(os.path.dirname(local_manifest_file), exist_ok=True)
        with open(local_manifest_file, "w") as file:
            file.write(output_manifest.encode())

        # Output results.
        logger.echo(f"Manifest Generated at {local_manifest_file}\n")
        return ManifestSnapshot(manifest=local_manifest_file)
    else:
        # No manifest generated.
        logger.echo("No manifest generated")
        return None


def _manifest_upload(
    manifest_file: str,
    s3_bucket_name: str,
    manifest_path: str,
    s3_metadata: Dict[str, Any],
    boto_session: boto3.Session,
    logger: ClickLogger = ClickLogger(False),
):
    """
    BETA API - This API is still evolving but will be made public in the near future.
    API to upload a job attachment manifest to the Content Address Storage. Manifests will be
    uploaded to s3://{s3_bucket_name}/Manifest/{manifest_path} as per the Deadline CAS folder structure.
    manifest_file: File Path to the manifest file for upload.
    s3_bucket_name: S3 bucket name.
    manifest_path: S3 path for the manifest.
    s3_metadata: Additional S3 file metadata tagged on upload.
    boto_session: Boto3 session.
    logger: Click Logger instance to print to CLI as test or JSON.
    """
    # Always upload the manifest file to case root /Manifest with the original file name.
    manifest_path = manifest_path + "/Manifests/" + Path(manifest_file).name

    # S3 uploader.
    upload = S3AssetUploader(session=boto_session)
    with open(manifest_file) as manifest:
        upload.upload_bytes_to_s3(
            bytes=BytesIO(manifest.read().encode("utf-8")),
            bucket=s3_bucket_name,
            key=manifest_path,
            progress_handler=logger.echo,
            extra_args=s3_metadata,
        )


def _manifest_download(
    download_dir: str,
    farm_id: str,
    queue_id: str,
    job_id: str,
    boto3_session: boto3.Session,
    step_id: Optional[str] = None,
    logger: ClickLogger = ClickLogger(False),
) -> ManifestDownloadResponse:
    """
    BETA API - This API is still evolving but will be made public in the near future.
    API to download the Job Attachment manifest for a Job, and optionally dependencies for Step.
    download_dir: Download directory.
    farm_id: The Deadline Farm to download from.
    queue_id: The Deadline Queue to download from.
    job_id: Job Id to download.
    boto_session: Boto3 session.
    step_id: Optional[str]: Optional, download manifest for a step
    logger: Click Logger instance to print to CLI as test or JSON.
    return ManifestDownloadResponse Downloaded Manifest data. Contains source S3 key and local download path.
    """

    # Deadline Client and get the Queue to download.
    deadline = boto3_session.client("deadline", config=get_default_client_config())

    queue: dict = deadline.get_queue(
        farmId=farm_id,
        queueId=queue_id,
    )

    # assume queue role - session permissions
    queue_role_session: boto3.Session = _get_queue_user_boto3_session(
        deadline=deadline,
        base_session=boto3_session,
        farm_id=farm_id,
        queue_id=queue_id,
        queue_display_name=queue["displayName"],
    )

    # get input_manifest_paths from Deadline GetJob API
    job: dict = deadline.get_job(farmId=farm_id, queueId=queue_id, jobId=job_id)
    attachments: dict = job["attachments"]
    input_manifest_paths: List[Tuple[str, str]] = [
        (manifest["inputManifestPath"], manifest["rootPath"])
        for manifest in attachments["manifests"]
    ]

    # get s3BucketName from Deadline GetQueue API
    bucket_name: str = queue["jobAttachmentSettings"]["s3BucketName"]

    # get S3 prefix
    s3_prefix: Path = Path(queue["jobAttachmentSettings"]["rootPrefix"], S3_MANIFEST_FOLDER_NAME)

    s3_client: BaseClient = get_s3_client(session=queue_role_session)
    transfer_manager = get_s3_transfer_manager(s3_client=s3_client)

    # Capture a list of success and failed to download files for JSON output.
    successful_downloads: List[ManifestDownload] = []
    failed_downloads: List[str] = []

    # download each input_manifest_path
    for input_manifest in input_manifest_paths:
        local_file_name = Path(download_dir, input_manifest[1].replace("/", "-") + ".manifest")

        result = download_file_with_s3_key(
            s3_bucket=bucket_name,
            s3_key=(s3_prefix / input_manifest[0]).as_posix(),
            local_file_name=local_file_name,
            session=queue_role_session,
            transfer_manager=transfer_manager,
        )

        if result is not None:
            # TODO: do we want to add file size.
            # transfer_path = result.meta.call_args.fileobj  # type: ignore[attr-defined]
            # file_size = result.meta.size  # type: ignore[attr-defined]

            logger.echo(f"Downloaded manifest file to {local_file_name}.")
            # I don't like this output structure, how can we make it better?
            download_info = ManifestDownload(
                s3_key=input_manifest[0], local=local_file_name.absolute().as_posix()
            )
            successful_downloads.append(download_info)
        else:
            logger.echo(
                f"Failed to download file with S3 key '{input_manifest[0]}' from bucket '{bucket_name}'"
            )
            failed_downloads.append(input_manifest[0])

    # Now also handle step-step dependencies
    # TODO: Merge manifests by root.
    # TODO: Filter outputs by path
    # TODO: Merge all manifests by root.
    # SHARE this code with manifest sync_inputs work from Job run as user project!
    if step_id is not None:
        nextToken = ""
        step_dep_response = deadline.list_step_dependencies(
            farmId=farm_id,
            queueId=queue_id,
            jobId=job_id,
            stepId=step_id,
            nextToken=nextToken,
        )

        for step in step_dep_response["dependencies"]:
            logger.echo(f"Found Step-Step dependency. {step['stepId']}")

    # JSON output at the end.
    output = ManifestDownloadResponse(downloaded=successful_downloads, failed=failed_downloads)
    return output
