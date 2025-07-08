# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the _add_output_manifests_from_s3 function in the incremental downloads module.

This module contains comprehensive unit tests for the _add_output_manifests_from_s3 function,
which is responsible for populating missing manifest information in session actions by
retrieving output manifest paths from S3.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import patch

import boto3
from moto import mock_aws

from deadline.job_attachments._incremental_downloads._manifest_s3_downloads import (
    _add_output_manifests_from_s3,
)
from deadline.job_attachments.asset_manifests import hash_data as ja_hash_data
from deadline.job_attachments.asset_manifests.v2023_03_03.asset_manifest import DEFAULT_HASH_ALG


# Test data generation utilities


def generate_test_job(job_id: str, root_paths: list[str]) -> dict[str, Any]:
    """Generate a test job with specified root paths for attachment manifests."""
    return {
        "jobId": job_id,
        "name": f"test-job-{job_id}",
        "attachments": {
            "manifests": [
                {
                    "rootPath": root_path,
                    "rootPathFormat": "POSIX",
                    "inputManifestPath": f"input_manifest_{i}",
                    "inputManifestHash": f"input_hash_{i}",
                    "outputRelativeDirectories": [f"output_{i}"],
                }
                for i, root_path in enumerate(root_paths)
            ]
        },
    }


def generate_session_actions(
    session_action_ids: list[str],
    farm_id: str = "farm-01234567890123456789012345678901",
    queue_id: str = "queue-01234567890123456789012345678901",
    job_id: str = "job-01234567890123456789012345678901",
    root_path_hashes: list[str] | None = None,
    *,
    with_manifests: bool = False,
) -> list[dict[str, Any]]:
    """Generate session actions with or without existing manifests following the correct S3 path pattern."""
    session_actions = []

    # Default root path hashes if not provided
    if root_path_hashes is None:
        root_path_hashes = ["hash1", "hash2"]

    for session_action_id in session_action_ids:
        session_action: dict[str, Any] = {"sessionActionId": session_action_id}
        if with_manifests:
            # Generate manifests following the pattern:
            # <farm_id>/<queue_id>/<job_id>/<step_id>/<task_id>/<timestamp>_<session_action_id>/<manifest_hash>_output
            step_id = "step-123"
            task_id = "task-456"
            timestamp = "2023-01-01T12:00:00.000000Z"

            manifests = []
            for i, root_path_hash in enumerate(root_path_hashes):
                manifest_path = (
                    f"{farm_id}/{queue_id}/{job_id}/{step_id}/{task_id}/"
                    f"{timestamp}_{session_action_id}/{root_path_hash}_output"
                )
                manifests.append({"outputManifestPath": manifest_path})
            session_action["manifests"] = manifests
        session_actions.append(session_action)
    return session_actions


def calculate_root_path_hashes(root_paths: list[str]) -> list[str]:
    """Calculate hashes for root paths using the same algorithm as the function under test."""
    return [ja_hash_data(root_path.encode(), DEFAULT_HASH_ALG) for root_path in root_paths]


class ManifestKeyBuilder:
    """Utility class to build S3 manifest keys following the documented pattern."""

    def __init__(self, root_prefix: str, farm_id: str, queue_id: str, job_id: str):
        self.root_prefix = root_prefix
        self.farm_id = farm_id
        self.queue_id = queue_id
        self.job_id = job_id

    def build_key(self, session_action_id: str, root_path_hash: str) -> str:
        """Build a manifest S3 key for testing.

        Pattern: <root_prefix>/Manifests/<farm_id>/<queue_id>/<job_id>/<step_id>/<task_id>/<timestamp>_<session_action_id>/<manifest_hash>_output
        """
        step_id = "step-123"
        task_id = "task-456"
        timestamp = "2023-01-01T12:00:00.000000Z"
        return (
            f"{self.root_prefix}/Manifests/{self.farm_id}/{self.queue_id}/"
            f"{self.job_id}/{step_id}/{task_id}/{timestamp}_{session_action_id}/"
            f"{root_path_hash}_output"
        )


def create_manifest_s3_objects(s3_client, bucket_name: str, manifest_keys: list[str]):
    """Create S3 objects for manifest keys with minimal valid content."""
    manifest_content = json.dumps(
        {"manifestVersion": "2023-03-03", "hashAlg": "xxh128", "paths": [], "totalSize": 0}
    )

    for key in manifest_keys:
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=manifest_content.encode("utf-8"))


# Validation utility functions


def validate_manifest_structure(manifest_entry: dict[str, Any]) -> None:
    """
    Validate that a manifest entry has the expected structure with proper "outputManifestPath" field format.

    Requirements: 4.1 - Verify that the populated manifest structure matches the expected format
    """
    assert isinstance(manifest_entry, dict), "Manifest entry must be a dictionary"
    assert "outputManifestPath" in manifest_entry, (
        "Manifest entry must contain 'outputManifestPath' field"
    )
    assert isinstance(manifest_entry["outputManifestPath"], str), (
        "outputManifestPath must be a string"
    )
    assert len(manifest_entry["outputManifestPath"]) > 0, "outputManifestPath cannot be empty"

    # Validate that the path doesn't contain the S3 prefix (should be removed)
    output_path = manifest_entry["outputManifestPath"]
    assert not output_path.startswith("test-prefix/Manifests/"), (
        "S3 prefix should be removed from outputManifestPath"
    )
    assert not output_path.startswith("/Manifests/"), (
        "Manifest prefix should be removed from outputManifestPath"
    )


def validate_root_path_hash_matching(
    job: dict[str, Any], session_actions: list[dict[str, Any]], expected_root_path_hashes: list[str]
) -> None:
    """
    Validate that manifests are populated at correct indices based on root path hash matching.

    Requirements: 4.2 - Verify that each manifest is populated at the correct index in the manifests array
    Requirements: 4.4 - Verify that manifest keys are correctly matched to job attachment manifests based on hash
    """
    job_manifests = job.get("attachments", {}).get("manifests", [])

    for session_action in session_actions:
        if "manifests" not in session_action:
            continue

        manifests = session_action["manifests"]
        assert len(manifests) == len(job_manifests), (
            "Number of manifests should match job attachment manifests"
        )

        for i, manifest in enumerate(manifests):
            if manifest and "outputManifestPath" in manifest:
                # Verify this manifest corresponds to the correct root path hash at index i
                output_path = manifest["outputManifestPath"]
                expected_hash = expected_root_path_hashes[i]

                # The output path should contain the root path hash for the corresponding job manifest
                assert expected_hash in output_path, (
                    f"Manifest at index {i} should contain root path hash {expected_hash} "
                    f"but path is {output_path}"
                )

                # Verify the session action ID is in the path
                session_action_id = session_action["sessionActionId"]
                assert session_action_id in output_path, (
                    f"Output path should contain session action ID {session_action_id}"
                )


def validate_existing_session_action_manifests_are_unmodified(
    original_session_actions: list[dict[str, Any]], processed_session_actions: list[dict[str, Any]]
) -> None:
    """
    Validate that session actions with existing manifests are not modified during processing.
    """
    for i, (original, processed) in enumerate(
        zip(original_session_actions, processed_session_actions)
    ):
        if "manifests" in original:
            # Session actions that already had manifests should remain unchanged
            assert original == processed, (
                f"Session action {i} with existing manifests should not be modified. "
                f"Original: {original}, Processed: {processed}"
            )


def validate_manifest_keys_do_not_have_prefix(
    session_actions: list[dict[str, Any]], root_prefix: str
) -> None:
    """
    Validate that output manifest paths do not start with the S3 prefix configured on the queue.
    """
    manifest_prefix = f"{root_prefix}/Manifests/"

    for session_action in session_actions:
        for manifest in session_action.get("manifests", []):
            if manifest and "outputManifestPath" in manifest:
                output_path = manifest["outputManifestPath"]

                # Verify the S3 prefix has been removed
                assert not output_path.startswith(manifest_prefix), (
                    f"Output manifest path should not start with S3 prefix '{manifest_prefix}'. "
                    f"Found: {output_path}"
                )

                # Verify it's a relative path (doesn't start with /)
                assert not output_path.startswith("/"), (
                    f"Output manifest path should be relative, not absolute. Found: {output_path}"
                )


@mock_aws
def test_add_output_manifests_from_s3_fill_in_missing_manifests(fresh_deadline_config):
    """
    Test that _add_output_manifests_from_s3 correctly fills in missing manifest information from S3.

    This test uses moto to create a mocked S3 bucket structure containing S3 objects with appropriate key names
    for the test, and a mock list_session_actions response where some manifests are already provided
    and others are not. It confirms that the list_session_actions response is updated where the manifests are
    missing, and is not modified where they are.
    """
    # Test constants
    farm_id = "farm-01234567890123456789012345678901"
    queue_id = "queue-01234567890123456789012345678901"
    job_id = "job-01234567890123456789012345678901"
    bucket_name = "test-bucket"
    root_prefix = "test-prefix"

    # Create S3 client and bucket
    s3_client = boto3.client("s3", region_name="us-west-2")
    s3_client.create_bucket(
        Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "us-west-2"}
    )

    # Create test job with multiple attachment manifests having different root paths
    root_paths = ["/tmp/input1", "/tmp/input2"]
    job = generate_test_job(job_id, root_paths)

    # Create queue structure
    queue = {
        "queueId": queue_id,
        "jobAttachmentSettings": {"s3BucketName": bucket_name, "rootPrefix": root_prefix},
    }

    # Generate session actions missing "manifests" fields
    session_action_ids = ["sessionaction-abc123-0", "sessionaction-def456-1"]
    session_actions = generate_session_actions(session_action_ids, with_manifests=False)

    # Calculate root path hashes for job attachment manifests
    root_path_hashes = calculate_root_path_hashes(root_paths)

    # Generate S3 manifest keys containing session action IDs and root path hashes
    key_builder = ManifestKeyBuilder(root_prefix, farm_id, queue_id, job_id)
    manifest_keys = []

    for session_action_id in session_action_ids:
        for root_path_hash in root_path_hashes:
            manifest_key = key_builder.build_key(session_action_id, root_path_hash)
            manifest_keys.append(manifest_key)

    # Create S3 objects for the manifest keys with minimal valid content
    create_manifest_s3_objects(s3_client, bucket_name, manifest_keys)

    # Create boto3 session for the function call
    boto3_session = boto3.Session()

    # Call the function under test
    _add_output_manifests_from_s3(
        farm_id=farm_id,
        queue=queue,
        job=job,
        boto3_session=boto3_session,
        session_action_list=session_actions,
    )

    # Validate manifest structure for all populated manifests
    for session_action in session_actions:
        assert "manifests" in session_action
        assert len(session_action["manifests"]) == len(root_paths)

        for manifest in session_action["manifests"]:
            validate_manifest_structure(manifest)

    # Validate root path hash matching
    validate_root_path_hash_matching(job, session_actions, root_path_hashes)

    # Validate manifest prefix removal
    validate_manifest_keys_do_not_have_prefix(session_actions, root_prefix)

    # Verify that all expected manifest keys were created and populated correctly
    expected_manifest_count = len(session_action_ids) * len(root_paths)
    actual_manifest_count = sum(
        len([m for m in sa["manifests"] if "outputManifestPath" in m]) for sa in session_actions
    )
    assert actual_manifest_count == expected_manifest_count


def test_add_output_manifests_from_s3_already_stored(fresh_deadline_config):
    """
    Test that _add_output_manifests_from_s3 does not modify session actions with existing manifests.

    This test confirms that no S3 APIs are accessed, and that the manifests already in the mock
    list_session_actions response are not modified when session actions already contain manifest data.
    """
    # Test constants
    farm_id = "farm-01234567890123456789012345678901"
    queue_id = "queue-01234567890123456789012345678901"
    job_id = "job-01234567890123456789012345678901"
    bucket_name = "test-bucket"
    root_prefix = "test-prefix"

    # Create test job with multiple attachment manifests having different root paths
    root_paths = ["/tmp/input1", "/tmp/input2"]
    job = generate_test_job(job_id, root_paths)

    # Create queue structure
    queue = {
        "queueId": queue_id,
        "jobAttachmentSettings": {"s3BucketName": bucket_name, "rootPrefix": root_prefix},
    }

    # Calculate root path hashes for the job attachment manifests
    root_path_hashes = calculate_root_path_hashes(root_paths)

    # Create session actions that already contain "manifests" fields
    session_action_ids = ["sessionaction-abc123-0", "sessionaction-def456-1"]
    session_actions = generate_session_actions(
        session_action_ids,
        farm_id=farm_id,
        queue_id=queue_id,
        job_id=job_id,
        root_path_hashes=root_path_hashes,
        with_manifests=True,
    )

    # Store original manifest data for comparison
    original_session_actions = json.loads(json.dumps(session_actions))

    # Create boto3 session for the function call
    boto3_session = boto3.Session()

    # Mock _get_tasks_manifests_keys_from_s3 to track if it's called
    with patch(
        "deadline.job_attachments._incremental_downloads._manifest_s3_downloads._get_tasks_manifests_keys_from_s3"
    ) as mock_get_keys:
        # Call the function under test
        _add_output_manifests_from_s3(
            farm_id=farm_id,
            queue=queue,
            job=job,
            boto3_session=boto3_session,
            session_action_list=session_actions,
        )

        # Verify that _get_tasks_manifests_keys_from_s3 was never called
        mock_get_keys.assert_not_called()

    # Existing manifests should be preserved
    validate_existing_session_action_manifests_are_unmodified(
        original_session_actions, session_actions
    )

    # Validate manifest prefix removal from S3 object keys
    validate_manifest_keys_do_not_have_prefix(session_actions, root_prefix)

    # Assert that existing manifest data in session actions remains unchanged
    assert session_actions == original_session_actions


@mock_aws
def test_add_output_manifests_from_s3_edge_cases(fresh_deadline_config):
    """
    Test that _add_output_manifests_from_s3 handles edge cases and mixed scenarios correctly.

    This test validates edge cases including empty session action lists and mixed scenarios
    where some session actions have manifests and others don't, ensuring the function
    handles various data structures robustly and maintains data integrity.
    """
    # Test constants
    farm_id = "farm-01234567890123456789012345678901"
    queue_id = "queue-01234567890123456789012345678901"
    job_id = "job-01234567890123456789012345678901"
    bucket_name = "test-bucket"
    root_prefix = "test-prefix"

    # Create S3 client and bucket
    s3_client = boto3.client("s3", region_name="us-west-2")
    s3_client.create_bucket(
        Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "us-west-2"}
    )

    # Create test job with multiple attachment manifests having different root paths
    root_paths = ["/tmp/input1", "/tmp/input2"]
    job = generate_test_job(job_id, root_paths)

    # Create queue structure
    queue = {
        "queueId": queue_id,
        "jobAttachmentSettings": {"s3BucketName": bucket_name, "rootPrefix": root_prefix},
    }

    # Calculate root path hashes for job attachment manifests
    root_path_hashes = calculate_root_path_hashes(root_paths)

    # Create boto3 session for the function call
    boto3_session = boto3.Session()

    # Test Case 1: Empty session action list
    empty_session_actions: list[dict[str, Any]] = []
    _add_output_manifests_from_s3(
        farm_id=farm_id,
        queue=queue,
        job=job,
        boto3_session=boto3_session,
        session_action_list=empty_session_actions,
    )

    # List is still empty
    assert empty_session_actions == []

    # Test Case 2: Mixed scenario - some with manifests, some without
    session_action_ids_with = ["sessionaction-with-1", "sessionaction-with-2"]
    session_action_ids_without = ["sessionaction-without-1", "sessionaction-without-2"]

    # Create session actions with existing manifests
    session_actions_with_manifests = generate_session_actions(
        session_action_ids_with,
        farm_id=farm_id,
        queue_id=queue_id,
        job_id=job_id,
        root_path_hashes=root_path_hashes,
        with_manifests=True,
    )

    # Create session actions without manifests
    session_actions_without_manifests = generate_session_actions(
        session_action_ids_without, with_manifests=False
    )

    # Combine both types for mixed scenario
    mixed_session_actions = session_actions_with_manifests + session_actions_without_manifests

    # Store original data for comparison
    original_mixed_session_actions = json.loads(json.dumps(mixed_session_actions))

    # Create S3 objects for the session actions that need manifests
    key_builder = ManifestKeyBuilder(root_prefix, farm_id, queue_id, job_id)
    manifest_keys = []

    for session_action_id in session_action_ids_without:
        for root_path_hash in root_path_hashes:
            manifest_key = key_builder.build_key(session_action_id, root_path_hash)
            manifest_keys.append(manifest_key)

    create_manifest_s3_objects(s3_client, bucket_name, manifest_keys)

    # Call the function under test with mixed scenario
    _add_output_manifests_from_s3(
        farm_id=farm_id,
        queue=queue,
        job=job,
        boto3_session=boto3_session,
        session_action_list=mixed_session_actions,
    )

    # Validate that session actions with existing manifests were not modified
    for i, session_action in enumerate(mixed_session_actions):
        if session_action["sessionActionId"] in session_action_ids_with:
            # Find corresponding original session action
            original_sa = next(
                sa
                for sa in original_mixed_session_actions
                if sa["sessionActionId"] == session_action["sessionActionId"]
            )
            assert session_action == original_sa, (
                f"Session action with existing manifests should not be modified: {session_action['sessionActionId']}"
            )

    # Validate that session actions without manifests were populated
    for session_action in mixed_session_actions:
        if session_action["sessionActionId"] in session_action_ids_without:
            assert "manifests" in session_action, (
                f"Session action without manifests should be populated: {session_action['sessionActionId']}"
            )
            for manifest in session_action["manifests"]:
                validate_manifest_structure(manifest)

    # Validate manifest prefix removal for all session actions
    validate_manifest_keys_do_not_have_prefix(mixed_session_actions, root_prefix)

    # Validate root path hash matching for all populated manifests
    validate_root_path_hash_matching(job, mixed_session_actions, root_path_hashes)
