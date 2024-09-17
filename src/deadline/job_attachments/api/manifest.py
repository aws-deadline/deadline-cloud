# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from io import BytesIO
from pathlib import Path
from typing import Any
import boto3

from deadline.client.cli._groups.click_logger import ClickLogger
from deadline.job_attachments.upload import S3AssetUploader

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
    ALPHA API - This API is still evolving but will be made public in the neear future.
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
