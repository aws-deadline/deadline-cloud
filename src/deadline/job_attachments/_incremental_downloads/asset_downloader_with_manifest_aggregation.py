# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from datetime import datetime
from deadline.client.api._session import _get_queue_user_boto3_session
from typing import Optional, Callable, Tuple, DefaultDict, List, Dict
import boto3
from ...job_attachments.download import (
    _get_output_manifest_files_by_asset_root_with_last_modified,
    _merge_asset_manifests_sorted_asc_by_last_modified,
)
from ...job_attachments.api.attachment import _attachment_download_with_root_manifests
from ...job_attachments.models import JobAttachmentS3Settings, FileConflictResolution
from ..asset_manifests.base_manifest import BaseAssetManifest


class SessionAction:
    """
    Model class representing a session action with its associated job, step, and task IDs.
    """

    def __init__(
        self,
        session_action_id: str,
        job_id: str,
        step_id: Optional[str] = None,
        task_id: Optional[str] = None,
        status: Optional[str] = None,
        manifests: Optional[List[Dict[str, str]]] = None,
    ):
        self.session_action_id = session_action_id
        self.job_id = job_id
        self.step_id = step_id
        self.task_id = task_id
        self.status = status
        self.manifests = manifests

    def to_dict(self) -> dict:
        """
        Convert the SessionAction object to a dictionary.

        Returns:
            dict: A dictionary representation of the SessionAction object.
        """
        return {
            "session_action_id": self.session_action_id,
            "job_id": self.job_id,
            "step_id": self.step_id,
            "task_id": self.task_id,
            "status": self.status,
            "manifests": self.manifests if self.manifests is not None else [],
        }


class AssetDownloadFailedException(Exception):
    """
    Asset Download Failed Exception to throw to caller if any of the aggregations & download failed
    """


def aggregate_manifest_and_download_outputs(
    boto3_session: boto3.Session,
    output_manifest_paths: List[str],
    farm_id: str,
    queue_id: str,
    file_conflict_resolution: FileConflictResolution,
    print_function_callback: Callable[[str], None] = lambda msg: None,
) -> None:
    """
    Aggregate manifests and download outputs for the given output manifest paths.
    Manifests are retrieved with their S3 last modified timestamps and then sorted by these timestamps (oldest first),
    ensuring that when merging and downloading, files are processed in the correct temporal order.

    :param file_conflict_resolution: Method of file conflict resolution
    :param boto3_session: The boto3 session to use for API calls
    :param output_manifest_paths: List of output manifest paths to merge and download outputs
    :param print_function_callback: Function for logging messages
    :param farm_id: The farm ID
    :param queue_id: The queue ID
    :return: Returns None
    Throws: AssetDownloadFailedException, when there is an exception during aggregation & download for any of the input
    session action ids
    """

    # Get queue for assuming queue role session to access JA bucket
    deadline = boto3_session.client("deadline")
    queue = deadline.get_queue(farmId=farm_id, queueId=queue_id)
    queue_role_session: boto3.Session = _get_queue_user_boto3_session(
        deadline=deadline,
        base_session=boto3_session,
        farm_id=farm_id,
        queue_id=queue_id,
        queue_display_name=queue["displayName"],
    )

    # Get job attachment settings once
    ja_settings = JobAttachmentS3Settings(**queue["jobAttachmentSettings"])
    s3_root_uri = ja_settings.to_s3_root_uri()

    try:
        # Get all manifests with their last modified timestamps, grouped by asset root
        output_manifests_list = _get_output_manifest_files_by_asset_root_with_last_modified(
            s3_settings=ja_settings,
            output_manifest_paths=output_manifest_paths,
            session=queue_role_session,
        )

        # Group manifests by asset root
        output_manifest_with_last_modified_per_asset_root: DefaultDict[
            str, List[Tuple[datetime, BaseAssetManifest]]
        ] = DefaultDict(list)

        for asset_root, last_modified, asset_manifest in output_manifests_list:
            if asset_root not in output_manifest_with_last_modified_per_asset_root:
                output_manifest_with_last_modified_per_asset_root[asset_root] = []
            output_manifest_with_last_modified_per_asset_root[asset_root].append(
                (last_modified, asset_manifest)
            )

        # Process each root directory separately
        merged_output_manifests = {}

        for (
            root,
            manifests_with_last_modified_timestamps,
        ) in output_manifest_with_last_modified_per_asset_root.items():
            # Merge the asset manifests sorted ascending by last modified timestamp
            merged_manifest = _merge_asset_manifests_sorted_asc_by_last_modified(
                manifests_with_last_modified_timestamps
            )

            if merged_manifest:
                merged_output_manifests[root] = merged_manifest
                print_function_callback(
                    f"Created merged manifest for root '{root}' with {len(merged_manifest.paths)} files"
                )

        # Log merged_output_manifests for every root
        for root, merged_manifest in merged_output_manifests.items():
            print_function_callback(
                f"Merged output manifest for root '{root}': "
                f"paths={[str(path) for path in merged_manifest.paths]}, "
            )

        # If we have merged manifests, download them
        if merged_output_manifests:
            # Download attachments
            _attachment_download_with_root_manifests(
                boto3_session=queue_role_session,
                file_name_manifest_dict=merged_output_manifests,
                s3_root_uri=s3_root_uri,
                conflict_resolution=file_conflict_resolution,
            )

    except Exception as e:
        print_function_callback(f"Error processing or downloading manifests: {str(e)}")
        raise AssetDownloadFailedException("Aggregation & Download Failed")
