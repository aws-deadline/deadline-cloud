# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
All the `deadline manifest` commands:
    * snapshot
    * upload
    * diff
    * download
"""
from __future__ import annotations

from configparser import ConfigParser
import dataclasses
import os
from typing import List, Optional
import boto3
import click

from deadline.client import api
from deadline.client.config import config_file
from deadline.job_attachments._diff import pretty_print_cli
from deadline.job_attachments.api.manifest import (
    _glob_files,
    _manifest_diff,
    _manifest_download,
    _manifest_snapshot,
    _manifest_upload,
)
from deadline.job_attachments.models import (
    S3_MANIFEST_FOLDER_NAME,
    JobAttachmentS3Settings,
    ManifestDiff,
)

from ...exceptions import NonValidInputError
from .._common import _apply_cli_options_to_config, _handle_error
from .click_logger import ClickLogger


@click.group(name="manifest")
@_handle_error
def cli_manifest():
    """
    Commands to work with AWS Deadline Cloud Job Attachments.
    """


@cli_manifest.command(
    name="snapshot",
    help="BETA - Generates a snapshot of files in a directory root as a Job Attachment Manifest.",
)
@click.option("--root", required=True, help="The root directory to snapshot. ")
@click.option(
    "-d",
    "--destination",
    default=None,
    help="Destination directory where manifest is created. Defaults to the manifest root directory.",
)
@click.option(
    "-n",
    "--name",
    default=None,
    help="Name of the manifest. A timestamp is added YYYY-MM-DD-HH-MM-SS for versioning.",
)
@click.option(
    "-i",
    "--include",
    default=None,
    help="Glob syntax of files and directories to include in the manifest. Can be provided multiple times.",
)
@click.option(
    "-e",
    "--exclude",
    default=None,
    help="Glob syntax of files and directories to exclude in the manifest. Can be provided multiple times.",
    multiple=True,
)
@click.option(
    "-ie",
    "--include-exclude-config",
    default=None,
    help="Include and exclude config of files and directories to include and exclude. Can be a json file or json string.",
    multiple=True,
)
@click.option(
    "--force-rehash",
    default=False,
    is_flag=True,
    help="Rehash all files to compare using file hashes.",
)
@click.option("--diff", default=None, help="File Path to Asset Manifest to diff against.")
@click.option("--json", default=None, is_flag=True, help="Output is printed as JSON for scripting.")
@_handle_error
def manifest_snapshot(
    root: str,
    destination: str,
    name: str,
    include: List[str],
    exclude: List[str],
    include_exclude_config: str,
    diff: str,
    force_rehash: bool,
    json: bool,
    **args,
):
    """
    Creates manifest of files specified by root directory.
    """
    logger: ClickLogger = ClickLogger(is_json=json)
    if not os.path.isdir(root):
        raise NonValidInputError(f"Specified root directory {root} does not exist.")

    if destination and not os.path.isdir(destination):
        raise NonValidInputError(f"Specified destination directory {destination} does not exist.")
    elif destination is None:
        destination = root
        logger.echo(f"Manifest creation path defaulted to {root} \n")

    manifest_out = _manifest_snapshot(
        root=root,
        destination=destination,
        name=name,
        include=include,
        exclude=exclude,
        include_exclude_config=include_exclude_config,
        diff=diff,
        force_rehash=force_rehash,
        logger=logger,
    )
    if manifest_out:
        logger.json(dataclasses.asdict(manifest_out))


@cli_manifest.command(
    name="diff",
    help="BETA - Compute the file difference of a root directory against an existing manifest for new, modified or deleted files.",
)
@click.option("--root", help="The root directory to compare changes to.")
@click.option(
    "--manifest",
    required=True,
    help="The path to manifest file to diff against.",
)
@click.option(
    "-i",
    "--include",
    default=None,
    help="Glob syntax of files and directories to include in the manifest. Can be provided multiple times.",
    multiple=True,
)
@click.option(
    "-e",
    "--exclude",
    default=None,
    help="Glob syntax of files and directories to exclude in the manifest. Can be provided multiple times.",
    multiple=True,
)
@click.option(
    "-ie",
    "--include-exclude-config",
    default=None,
    help="Include and exclude config of files and directories to include and exclude. Can be a json file or json string.",
)
@click.option(
    "--force-rehash",
    default=False,
    is_flag=True,
    help="Rehash all files to compare using file hashes.",
)
@click.option("--json", default=None, is_flag=True, help="Output is printed as JSON for scripting.")
@_handle_error
def manifest_diff(
    root: str,
    manifest: str,
    include: List[str],
    exclude: List[str],
    include_exclude_config: str,
    force_rehash: bool,
    json: bool,
    **args,
):
    """
    Check file differences between a directory and specified manifest.
    """
    logger: ClickLogger = ClickLogger(is_json=json)
    if not os.path.isfile(manifest):
        raise NonValidInputError(f"Specified manifest file {manifest} does not exist. ")

    if not os.path.isdir(root):
        raise NonValidInputError(f"Specified root directory {root} does not exist. ")

    # Perform the diff.
    differences: ManifestDiff = _manifest_diff(
        manifest=manifest,
        root=root,
        include=include,
        exclude=exclude,
        include_exclude_config=include_exclude_config,
        force_rehash=force_rehash,
        logger=logger,
    )

    # Print results to console.
    if json:
        logger.json(dataclasses.asdict(differences), indent=4)
    else:
        logger.echo(f"Manifest Diff of root directory: {root}")
        all_files = _glob_files(
            root=root,
            include=include,
            exclude=exclude,
            include_exclude_config=include_exclude_config,
        )
        pretty_print_cli(root=root, all_files=all_files, manifest_diff=differences)


@cli_manifest.command(
    name="download",
    help="BETA - Download Job Attachment Manifests for a Job, or Step including dependencies.",
)
@click.argument("download_dir")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--job-id", required=True, help="The AWS Deadline Cloud Job to get. ")
@click.option("--step-id", help="The AWS Deadline Cloud Step to get. ")
@click.option("--farm-id", help="The AWS Deadline Cloud Farm to use. ")
@click.option("--queue-id", help="The AWS Deadline Cloud Queue to use. ")
@click.option(
    "--json", default=None, is_flag=True, help="Output is printed as JSON for scripting. "
)
@_handle_error
def manifest_download(
    download_dir: str,
    job_id: str,
    step_id: str,
    json: bool,
    **args,
):
    """
    Downloads input manifest of previously submitted job.
    """
    logger: ClickLogger = ClickLogger(is_json=json)
    if not os.path.isdir(download_dir):
        raise NonValidInputError(f"Specified destination directory {download_dir} does not exist. ")

    # setup config
    config: Optional[ConfigParser] = _apply_cli_options_to_config(
        required_options={"farm_id", "queue_id"}, **args
    )
    queue_id: str = config_file.get_setting("defaults.queue_id", config=config)
    farm_id: str = config_file.get_setting("defaults.farm_id", config=config)

    boto3_session: boto3.Session = api.get_boto3_session(config=config)

    output = _manifest_download(
        download_dir=download_dir,
        farm_id=farm_id,
        queue_id=queue_id,
        job_id=job_id,
        step_id=step_id,
        boto3_session=boto3_session,
        logger=logger,
    )
    logger.json(dataclasses.asdict(output))


@cli_manifest.command(
    name="upload",
    help="BETA - Uploads a job attachment manifest file to a Content Addressable Storage's Manifest store. If calling via --s3-cas-path, it is recommended to use with --profile for a specific AWS profile with CAS S3 bucket access. Check exit code for success or failure.",
)
@click.argument("manifest_file")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--s3-cas-uri", help="The URI to the Content Addressable Storage S3 bucket and root.")
@click.option(
    "--s3-manifest-prefix", help="Prefix subpath in the manifest folder to upload the manifest."
)
@click.option(
    "--farm-id", help="The AWS Deadline Cloud Farm to use. Alternative to using --s3-cas-uri."
)
@click.option(
    "--queue-id", help="The AWS Deadline Cloud Queue to use. Alternative to using --s3-cas-uri."
)
@click.option("--json", default=None, is_flag=True, help="Output is printed as JSON for scripting.")
@_handle_error
def manifest_upload(
    manifest_file: str,
    s3_cas_uri: str,
    s3_manifest_prefix: str,
    json: bool,
    **args,
):
    # Input checking.
    if not manifest_file or not os.path.isfile(manifest_file):
        raise NonValidInputError(f"Specified manifest {manifest_file} does not exist. ")

    # Where will we upload the manifest to?
    required: set[str] = set()
    if not s3_cas_uri:
        required = {"farm_id", "queue_id"}

    config: Optional[ConfigParser] = _apply_cli_options_to_config(required_options=required, **args)

    # Logger
    logger: ClickLogger = ClickLogger(is_json=json)

    bucket_name: str = ""
    session: boto3.Session = api.get_boto3_session(config=config)
    if not s3_cas_uri:
        farm_id = config_file.get_setting("defaults.farm_id", config=config)
        queue_id = config_file.get_setting("defaults.queue_id", config=config)

        deadline = api.get_boto3_client("deadline", config=config)
        queue = deadline.get_queue(
            farmId=farm_id,
            queueId=queue_id,
        )
        queue_ja_settings: JobAttachmentS3Settings = JobAttachmentS3Settings(
            **queue["jobAttachmentSettings"]
        )
        bucket_name = queue_ja_settings.s3BucketName
        cas_path = queue_ja_settings.rootPrefix

        # IF we supplied a farm and queue, use the queue credentials.
        session = api.get_queue_user_boto3_session(
            deadline=deadline,
            config=config,
            farm_id=farm_id,
            queue_id=queue_id,
            queue_display_name=queue["displayName"],
        )

    else:
        # Self supplied cas path.
        uri_ja_settings: JobAttachmentS3Settings = JobAttachmentS3Settings.from_s3_root_uri(
            s3_cas_uri
        )
        bucket_name = uri_ja_settings.s3BucketName
        cas_path = uri_ja_settings.rootPrefix

    logger.echo(
        f"Uploading Manifest to {bucket_name} {cas_path} {S3_MANIFEST_FOLDER_NAME}, prefix: {s3_manifest_prefix}"
    )
    _manifest_upload(
        manifest_file=manifest_file,
        s3_bucket_name=bucket_name,
        s3_cas_prefix=cas_path,
        s3_key_prefix=s3_manifest_prefix,
        boto_session=session,
        logger=logger,
    )
    logger.echo("Uploading successful!")
