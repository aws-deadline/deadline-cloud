# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Functions for interfacing with Deadline API calls."""
from typing import Optional

import boto3
from botocore.exceptions import ClientError

from ..errors import JobAttachmentsError
from ..models import ManifestProperties, Attachments, Job, JobAttachmentS3Settings, Queue
from ..utils import AssetLoadingMethod, OperatingSystemFamily
from .aws_clients import get_deadline_client


def get_queue(
    farm_id: str,
    queue_id: str,
    session: Optional[boto3.Session] = None,
    deadline_endpoint_url: Optional[str] = None,
) -> Queue:
    """
    Retrieves a specific queue from Amazon Deadline Cloud.
    """
    try:
        response = get_deadline_client(
            session=session, endpoint_url=deadline_endpoint_url
        ).get_queue(farmId=farm_id, queueId=queue_id)
    except ClientError as exc:
        raise JobAttachmentsError(f'Failed to get queue "{queue_id}" from Deadline: {exc}') from exc

    # The API returns empty fields instead of an empty dict if there are no job attachment settings. So we need to
    # double check if the s3BucketName is set.
    if response["jobAttachmentSettings"] and response["jobAttachmentSettings"].get("s3BucketName"):
        job_attachment_settings = JobAttachmentS3Settings(
            s3BucketName=response["jobAttachmentSettings"].get("s3BucketName", ""),
            rootPrefix=response["jobAttachmentSettings"].get("rootPrefix", ""),
        )
    else:
        job_attachment_settings = None

    display_name_key = "displayName"
    status_key = "status"
    if "name" in response:
        display_name_key = "name"
    if "state" in response:
        status_key = "state"

    return Queue(
        displayName=response[display_name_key],
        queueId=response["queueId"],
        farmId=response["farmId"],
        status=response[status_key],
        jobAttachmentSettings=job_attachment_settings,
    )


def get_job(
    farm_id: str,
    queue_id: str,
    job_id: str,
    session: Optional[boto3.Session] = None,
    deadline_endpoint_url: Optional[str] = None,
) -> Job:
    """
    Retrieves a specific job from Deadline.
    """
    try:
        response = get_deadline_client(session=session, endpoint_url=deadline_endpoint_url).get_job(
            farmId=farm_id, queueId=queue_id, jobId=job_id
        )
    except ClientError as exc:
        raise JobAttachmentsError(f'Failed to get job "{job_id}" from Deadline') from exc
    return Job(
        jobId=response["jobId"],
        attachments=Attachments(
            manifests=[
                ManifestProperties(
                    fileSystemLocationName=manifest_properties.get("fileSystemLocationName", None),
                    rootPath=manifest_properties["rootPath"],
                    osType=OperatingSystemFamily.get_os_family(manifest_properties["osType"]),
                    outputRelativeDirectories=manifest_properties["outputRelativeDirectories"],
                    inputManifestPath=manifest_properties.get("inputManifestPath", None),
                )
                for manifest_properties in response["attachments"]["manifests"]
            ],
            assetLoadingMethod=AssetLoadingMethod(
                response["attachments"].get("assetLoadingMethod", AssetLoadingMethod.PRELOAD.value)
            ),
        )
        if "attachments" in response and response["attachments"]
        else None,
    )
