# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

__all__ = ["get_storage_profile_for_queue"]

from configparser import ConfigParser
from typing import Optional
from botocore.client import BaseClient  # type: ignore[import]

from ._session import get_boto3_client
from ...job_attachments.models import (
    FileSystemLocation,
    FileSystemLocationType,
    StorageProfile,
    StorageProfileOperatingSystemFamily,
)


def get_storage_profile_for_queue(
    farm_id: str,
    queue_id: str,
    storage_profile_id: str,
    deadline: Optional[BaseClient] = None,
    config: Optional[ConfigParser] = None,
) -> StorageProfile:
    if deadline is None:
        deadline = get_boto3_client("deadline", config=config)

    storage_profile_response = deadline.get_storage_profile_for_queue(
        farmId=farm_id, queueId=queue_id, storageProfileId=storage_profile_id
    )
    return StorageProfile(
        storageProfileId=storage_profile_response["storageProfileId"],
        displayName=storage_profile_response["displayName"],
        osFamily=StorageProfileOperatingSystemFamily(storage_profile_response["osFamily"]),
        fileSystemLocations=[
            FileSystemLocation(
                name=file_system_location["name"],
                path=file_system_location["path"],
                type=FileSystemLocationType(file_system_location["type"]),
            )
            for file_system_location in storage_profile_response.get("fileSystemLocations", [])
        ],
    )
