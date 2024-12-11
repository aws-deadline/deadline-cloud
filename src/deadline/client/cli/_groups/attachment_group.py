# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
All the `deadline attachment` commands:
    * upload
    * download
"""
from __future__ import annotations

import click
import boto3

from typing import Optional

from .click_logger import ClickLogger
from .._common import _apply_cli_options_to_config, _handle_error
from ...config import config_file

from deadline.client import api
from deadline.job_attachments import api as attachment_api
from deadline.job_attachments._aws.deadline import get_queue
from deadline.job_attachments.exceptions import MissingJobAttachmentSettingsError
from deadline.job_attachments.models import JobAttachmentS3Settings


@click.group(name="attachment")
@_handle_error
def cli_attachment():
    """
    Commands to work with Deadline Cloud Job Attachments.
    """


@cli_attachment.command(
    name="download",
    help="BETA - Download Job Attachment data files for given manifest(s).",
)
@click.option(
    "-m",
    "--manifests",
    multiple=True,
    required=True,
    help="File path(s) to manifest formatted file(s). File name has to contain the hash of corresponding source path.",
)
@click.option(
    "--s3-root-uri", help="Job Attachments S3 root uri including bucket name and root prefix."
)
@click.option("--path-mapping-rules", help="Path to a file with the path mapping rules to use.")
@click.option("--farm-id", help="The AWS Deadline Cloud Farm to use. ")
@click.option("--queue-id", help="The AWS Deadline Cloud Queue to use. ")
@click.option(
    "--profile", help="The AWS profile to use for interacting with Job Attachments S3 bucket."
)
@click.option("--json", default=None, is_flag=True, help="Output is printed as JSON for scripting.")
@_handle_error
def attachment_download(
    manifests: list[str],
    s3_root_uri: str,
    path_mapping_rules: str,
    json: bool,
    **args,
):
    """
    Download data files of manifest root(s) to a machine for given manifest(s) from S3.
    """
    logger: ClickLogger = ClickLogger(is_json=json)

    # Setup config
    config = _apply_cli_options_to_config(**args)

    # Assuming when passing with config, session constructs from the profile id for S3 calls
    # TODO - add type for profile, if queue type, get queue sesson directly
    boto3_session: boto3.session = api.get_boto3_session(config=config)

    # If profile is not provided via args, default to use local config file
    if not args.pop("profile", None):
        queue_id: str = config_file.get_setting("defaults.queue_id", config=config)
        farm_id: str = config_file.get_setting("defaults.farm_id", config=config)

        s3_settings: Optional[JobAttachmentS3Settings] = get_queue(
            farm_id=farm_id,
            queue_id=queue_id,
            session=boto3_session,
        ).jobAttachmentSettings
        if not s3_settings:
            raise MissingJobAttachmentSettingsError(f"Queue {queue_id} has no attachment settings")

        s3_root_uri = s3_settings.to_s3_root_uri()

        deadline_client = boto3_session.client("deadline")
        boto3_session = api.get_queue_user_boto3_session(deadline=deadline_client, config=config)

    if not s3_root_uri:
        raise MissingJobAttachmentSettingsError("No valid s3 root path available")

    attachment_api.attachment_download(
        manifests=manifests,
        s3_root_uri=s3_root_uri,
        boto3_session=boto3_session,
        path_mapping_rules=path_mapping_rules,
        logger=logger,
    )


@cli_attachment.command(
    name="upload",
    help="BETA - Upload Job Attachment data files for given manifest(s).",
)
@click.option(
    "-m",
    "--manifests",
    multiple=True,
    required=True,
    help="File path(s) to manifest formatted file(s). File name has to contain the hash of corresponding source path.",
)
@click.option(
    "-r",
    "--root-dirs",
    multiple=True,
    help="The root directory of assets to upload.",
)
@click.option("--path-mapping-rules", help="Path to a file with the path mapping rules to use.")
@click.option(
    "--s3-root-uri", help="Job Attachments S3 root uri including bucket name and root prefix."
)
@click.option(
    "--upload-manifest-path", default=None, help="File path for uploading the manifests to CAS."
)
@click.option("--farm-id", help="The AWS Deadline Cloud Farm to use. ")
@click.option("--queue-id", help="The AWS Deadline Cloud Queue to use. ")
@click.option(
    "--profile", help="The AWS profile to use for interacting with Job Attachments S3 bucket."
)
@click.option("--json", default=None, is_flag=True, help="Output is printed as JSON for scripting")
@_handle_error
def attachment_upload(
    manifests: list[str],
    root_dirs: list[str],
    path_mapping_rules: str,
    s3_root_uri: str,
    upload_manifest_path: str,
    json: bool,
    **args,
):
    """
    Upload output files to s3. The files always include data files, optionally upload manifests prefixed by given path.
    """
    logger: ClickLogger = ClickLogger(is_json=json)

    # Setup config
    config = _apply_cli_options_to_config(**args)

    # Assuming when passing with config, session constructs from the profile id for S3 calls
    # TODO - add type for profile, if queue type, get queue sesson directly
    boto3_session: boto3.session = api.get_boto3_session(config=config)

    # If profile is not provided via args, default to use local config file
    if not args.pop("profile", None):
        queue_id: str = config_file.get_setting("defaults.queue_id", config=config)
        farm_id: str = config_file.get_setting("defaults.farm_id", config=config)

        s3_settings: Optional[JobAttachmentS3Settings] = get_queue(
            farm_id=farm_id,
            queue_id=queue_id,
            session=boto3_session,
        ).jobAttachmentSettings
        if not s3_settings:
            raise MissingJobAttachmentSettingsError(f"Queue {queue_id} has no attachment settings")

        s3_root_uri = s3_settings.to_s3_root_uri()

        deadline_client = boto3_session.client("deadline")
        boto3_session = api.get_queue_user_boto3_session(deadline=deadline_client, config=config)

    if not s3_root_uri:
        raise MissingJobAttachmentSettingsError("No valid s3 root path available")

    attachment_api.attachment_upload(
        root_dirs=root_dirs,
        manifests=manifests,
        s3_root_uri=s3_root_uri,
        boto3_session=boto3_session,
        path_mapping_rules=path_mapping_rules,
        upload_manifest_path=upload_manifest_path,
        logger=logger,
    )
