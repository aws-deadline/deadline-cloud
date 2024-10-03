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
    Commands to work with AWS Deadline Cloud Job Attachments.
    """


@cli_attachment.command(
    name="download",
    help="BETA",
)
@click.option(
    "--manifests",
    multiple=True,
    required=True,
    help="Comma separated file paths to manifest formatted files.",
)
@click.option(
    "--s3-root-path", help="Job Attachments S3 root path including bucket name and root prefix."
)
@click.option("--path-mapping-rules", help="Path to a file with the path mapping rules to use")
@click.option("--farm-id", help="The AWS Deadline Cloud Farm to use. ")
@click.option("--queue-id", help="The AWS Deadline Cloud Queue to use. ")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--json", default=None, is_flag=True, help="Output is printed as JSON for scripting")
@_handle_error
def attachment_download(
    manifests: list[str],
    s3_root_path: str,
    path_mapping_rules: str,
    json: bool,
    **args,
):
    """
    Synchronize files of manifest root(s) to a machine.
    The input of the CLI is a path to a Job Attachments manifest file to download assets.
    """

    # Setup config
    config = _apply_cli_options_to_config(**args)

    # Assuming when passing with config, session constructs from the profile id
    # TODO - add type for profile, if queue type, get queue sesson directly
    boto3_session: boto3.session = api.get_boto3_session(config=config)

    if not args.pop("profile", None):
        queue_id: str = config_file.get_setting("defaults.queue_id", config=config)
        farm_id: str = config_file.get_setting("defaults.farm_id", config=config)

        deadline_client = boto3_session.client("deadline")
        boto3_session = api.get_queue_user_boto3_session(
            deadline=deadline_client,
            config=None,
            farm_id=farm_id,
            queue_id=queue_id,
        )
        s3_settings: Optional[JobAttachmentS3Settings] = get_queue(
            farm_id=farm_id, queue_id=queue_id
        ).jobAttachmentSettings
        if not s3_settings:
            raise MissingJobAttachmentSettingsError(f"Queue {queue_id} has no attachment settings")

        s3_root_path = s3_settings.to_root_path()

    if not s3_root_path:
        raise MissingJobAttachmentSettingsError("No valid s3 root path available")

    attachment_api.attachment_download(
        manifests=manifests,
        s3_root_path=s3_root_path,
        boto3_session=boto3_session,
        path_mapping_rules=path_mapping_rules,
    )
