# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from configparser import ConfigParser
from io import BytesIO
from pathlib import Path
from typing import Any, List, Optional
import boto3
from botocore.client import BaseClient

from deadline.client import api
from deadline.client.cli._groups.click_logger import ClickLogger
from deadline.client.config import config_file
from deadline.job_attachments._aws.aws_clients import get_s3_client, get_s3_transfer_manager
from deadline.job_attachments._diff import compare_manifest, pretty_print_cli
from deadline.job_attachments._glob import _process_glob_inputs
from deadline.job_attachments._utils import _glob_paths
from deadline.job_attachments.asset_manifests.base_manifest import (
    BaseAssetManifest,
    BaseManifestPath,
)
from deadline.job_attachments.asset_manifests.decode import decode_manifest
from deadline.job_attachments.caches.hash_cache import HashCache
from deadline.job_attachments.download import download_file_with_s3_key
from deadline.job_attachments.models import (
    S3_MANIFEST_FOLDER_NAME,
    GlobConfig,
    JobAttachmentS3Settings,
    ManifestDiff,
    ManifestDownload,
    ManifestDownloadResponse,
)
from deadline.job_attachments.upload import FileStatus, S3AssetManager, S3AssetUploader

"""
APIs here should be business logic only. It should perform one thing, and one thing well. 
It should use basic primitives like S3 upload, download, boto3. 
These APIs should be boto3 session agnostic and given which underlying credentials are used.
"""


def _manifest_upload(
    manifest_file: str,
    bucket_name: str,
    manifest_path: str,
    s3_metadata: dict[str, Any],
    boto_session: boto3.Session,
    logger: ClickLogger = ClickLogger(False),
):
    """
    ALPHA API - This API is still evolving but will be made public in the near future.
    API to upload a job attachment manifest to the Content Address Storage.
    manifest_file: File Path to the manifest file for upload.
    bucket_name: S3 bucket name.
    manifest_path: S3 path.
    s3_metadata: Additional S3 file metadata.
    boto_session: Boto3 session.
    logger: Click Logger instance to print to CLI as test or JSON.
    """
    # Always upload the manifest file to case root /Manifest with the original file name.
    manifest_path = manifest_path + "/Manifests/" + Path(manifest_file).name

    # S3 uploader
    upload = S3AssetUploader(session=boto_session)
    with open(manifest_file) as manifest:
        upload.upload_bytes_to_s3(
            bytes=BytesIO(manifest.read().encode("utf-8")),
            bucket=bucket_name,
            key=manifest_path,
            progress_handler=logger.echo,
            extra_args=s3_metadata,
        )


def _manifest_download(
    download_dir: str,
    farm_id: str,
    queue_id: str,
    job_id: str,
    deadline: BaseClient,
    config: Optional[ConfigParser] = None,  # This needs refactoring to be removed
    step_id: Optional[str] = None,
    logger: ClickLogger = ClickLogger(False),
) -> ManifestDownloadResponse:
    """
    ALPHA API - This API is still evolving but will be made public in the near future.
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

    queue: dict = deadline.get_queue(
        farmId=farm_id,
        queueId=queue_id,
    )

    # assume queue role - session permissions
    queue_role_session: boto3.Session = api.get_queue_user_boto3_session(
        deadline=deadline,
        config=config,
        farm_id=farm_id,
        queue_id=queue_id,
        queue_display_name=queue["displayName"],
    )

    # get input_manifest_paths from Deadline GetJob API
    job: dict = deadline.get_job(farmId=farm_id, queueId=queue_id, jobId=job_id)
    attachments: dict = job["attachments"]
    input_manifest_paths: list[tuple[str, str]] = [
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
    successful_downloads: list[ManifestDownload] = []
    failed_downloads: list[str] = []

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
            # Todo: do we want to add file size.
            # transfer_path = result.meta.call_args.fileobj  # type: ignore[attr-defined]
            # file_size = result.meta.size  # type: ignore[attr-defined]

            logger.echo(f"\nDownloaded manifest file to {local_file_name}.")
            # I don't like this output structure, how can we make it better?
            download_info = ManifestDownload(
                s3=input_manifest[0], local=local_file_name.absolute().as_posix()
            )
            successful_downloads.append(download_info)
        else:
            logger.echo(
                f"\nFailed to download file with S3 key '{input_manifest[0]}' from bucket '{bucket_name}'"
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


def _manifest_diff(
    manifest: str,
    root: str,
    glob: str,
    logger: ClickLogger = ClickLogger(False),
    pretty_print: bool = False,
) -> ManifestDiff:
    """
    ALPHA API - This API is still evolving but will be made public in the near future.
    API to diff a manifest root with a previously snapshotted manifest.
    manifest: Manifest file path to compare against.
    root: Root directory to generate the manifest fileset.
    glob: Glob include and exclude of directory and file regex to include in the manifest.
    logger: Click Logger instance to print to CLI as test or JSON.
    """

    asset_manager = S3AssetManager(
        farm_id=" ", queue_id=" ", job_attachment_settings=JobAttachmentS3Settings(" ", " ")
    )

    # get inputs of directory
    glob_config: GlobConfig = _process_glob_inputs(glob)
    input_files = _glob_paths(
        root, include=glob_config.include_glob, exclude=glob_config.exclude_glob
    )
    input_paths = [Path(p) for p in input_files]

    # hash and create manifest of local directory
    cache_config = config_file.get_cache_directory()
    with HashCache(cache_config) as hash_cache:
        directory_manifest_object = asset_manager._create_manifest_file(
            input_paths=input_paths, root_path=root, hash_cache=hash_cache
        )

    # parse local manifest
    local_manifest_object: BaseAssetManifest
    with open(manifest) as input_file:
        manifest_data_str = input_file.read()
        local_manifest_object = decode_manifest(manifest_data_str)

    # compare manifests
    differences: List[tuple[FileStatus, BaseManifestPath]] = compare_manifest(
        reference_manifest=local_manifest_object, compare_manifest=directory_manifest_object
    )

    if pretty_print:
        logger.echo(f"\n{root}")
        pretty_print_cli(file_status_list=differences)

    output: ManifestDiff = ManifestDiff()

    for item in differences:
        if item[0] == FileStatus.MODIFIED:
            output.modified.append(item[1].path)
        elif item[0] == FileStatus.NEW:
            output.new.append(item[1].path)
        elif item[0] == FileStatus.DELETED:
            output.deleted.append(item[1].path)

    return output
