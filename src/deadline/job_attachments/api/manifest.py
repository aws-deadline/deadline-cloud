# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, List, Optional

import boto3
from botocore.client import BaseClient

from deadline.client import api
from deadline.client.api._session import _get_queue_user_boto3_session, get_default_client_config
from deadline.client.cli._common import _ProgressBarCallbackManager
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
    ManifestSnapshot,
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
    boto3_session: boto3.Session,
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
            # TODO: do we want to add file size.
            # transfer_path = result.meta.call_args.fileobj  # type: ignore[attr-defined]
            # file_size = result.meta.size  # type: ignore[attr-defined]

            logger.echo(f"\nDownloaded manifest file to {local_file_name}.")
            # I don't like this output structure, how can we make it better?
            download_info = ManifestDownload(
                s3_key=input_manifest[0], local=local_file_name.absolute().as_posix()
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


def _manifest_snapshot(
    root: str,
    destination: str,
    name: str,
    glob: str,
    diff: Optional[str] = None,
    logger: ClickLogger = ClickLogger(False),
) -> Optional[ManifestSnapshot]:

    # Get all files in the root.
    glob_config: GlobConfig = _process_glob_inputs(glob)
    inputs = _glob_paths(root, include=glob_config.include_glob, exclude=glob_config.exclude_glob)

    # Placeholder Asset Manager
    asset_manager = S3AssetManager(
        farm_id=" ", queue_id=" ", job_attachment_settings=JobAttachmentS3Settings(" ", " ")
    )

    hash_callback_manager = _ProgressBarCallbackManager(length=100, label="Hashing Attachments")

    upload_group = asset_manager.prepare_paths_for_upload(
        input_paths=inputs, output_paths=[root], referenced_paths=[]
    )
    assert len(upload_group.asset_groups) == 1

    if upload_group.asset_groups:
        _, manifests = api.hash_attachments(
            asset_manager=asset_manager,
            asset_groups=upload_group.asset_groups,
            total_input_files=upload_group.total_input_files,
            total_input_bytes=upload_group.total_input_bytes,
            print_function_callback=logger.echo,
            hashing_progress_callback=hash_callback_manager.callback,
        )

    if not manifests or len(manifests) == 0:
        logger.echo("No manifest generated")
        return None

    # This is a hard failure, we are snapshotting 1 directory.
    assert len(manifests) == 1
    output_manifest = manifests[0].asset_manifest
    assert output_manifest

    # If this is a diff manifest, load the supplied manifest file.
    if diff:
        # Parse local manifest
        with open(diff) as source_diff:
            source_manifest_str = source_diff.read()
            source_manifest = decode_manifest(source_manifest_str)

        # Get the differences
        changed_paths: list[str] = []
        differences: list[tuple[FileStatus, BaseManifestPath]] = compare_manifest(
            source_manifest, output_manifest
        )
        for diff_item in differences:
            if diff_item[0] == FileStatus.MODIFIED or diff_item[0] == FileStatus.NEW:
                full_diff_path = f"{root}/{diff_item[1].path}"
                changed_paths.append(full_diff_path)
                logger.echo(f"Found difference at: {full_diff_path}, Status: {diff_item[0]}")

        # Since the files are already hashed, we can easily re-use has_attachments to remake a diff manifest.
        diff_group = asset_manager.prepare_paths_for_upload(
            input_paths=changed_paths, output_paths=[root], referenced_paths=[]
        )
        _, diff_manifests = api.hash_attachments(
            asset_manager=asset_manager,
            asset_groups=diff_group.asset_groups,
            total_input_files=diff_group.total_input_files,
            total_input_bytes=diff_group.total_input_bytes,
            print_function_callback=logger.echo,
            hashing_progress_callback=hash_callback_manager.callback,
        )
        output_manifest = diff_manifests[0].asset_manifest

    # Write created manifest into local file, at the specified location at destination
    if output_manifest is not None:

        timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
        manifest_name = name if name else root.replace("/", "_")
        manifest_name = manifest_name[1:] if manifest_name[0] == "_" else manifest_name
        manifest_name = f"{manifest_name}-{timestamp}.manifest"

        local_manifest_file = Path(destination, manifest_name)
        local_manifest_file.parent.mkdir(parents=True, exist_ok=True)
        with open(local_manifest_file, "w") as file:
            file.write(output_manifest.encode())

        # Output results.
        logger.echo(f"Manifest Generated at {destination}{manifest_name}\n")
        return ManifestSnapshot(manifest=f"{destination}{manifest_name}")
    else:
        # No manifest generated.
        logger.echo("No manifest generated")
        return None


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
