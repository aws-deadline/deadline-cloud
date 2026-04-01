#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Cleanup orphaned test queues from integration tests.

Usage:
    python scripts/cleanup_test_queues.py --farm-id <farm-id> --region <region>
"""

import argparse
import boto3
import sys


def cleanup_test_queues(farm_id: str, region: str, dry_run: bool = False):
    """Delete all test queues matching known patterns."""
    client = boto3.client("deadline", region_name=region)

    # List all queues in the farm
    paginator = client.get_paginator("list_queues")
    page_iterator = paginator.paginate(farmId=farm_id)

    test_queue_patterns = [
        "job_attachments_test_queue",
        "job_attachments_test_no_settings_queue",
    ]

    deleted_count = 0
    for page in page_iterator:
        for queue in page.get("queues", []):
            display_name = queue.get("displayName", "")
            queue_id = queue["queueId"]

            # Check if this is a test queue
            if any(pattern in display_name for pattern in test_queue_patterns):
                print(f"Found test queue: {display_name} ({queue_id})")

                if dry_run:
                    print(f"  [DRY RUN] Would delete queue {queue_id}")
                else:
                    try:
                        client.delete_queue(farmId=farm_id, queueId=queue_id)
                        print(f"  ✓ Deleted queue {queue_id}")
                        deleted_count += 1
                    except Exception as e:
                        print(f"  ✗ Failed to delete queue {queue_id}: {e}")

    if dry_run:
        print(f"\n[DRY RUN] Would delete {deleted_count} queues")
    else:
        print(f"\n✓ Deleted {deleted_count} test queues")

    return deleted_count


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cleanup orphaned test queues")
    parser.add_argument("--farm-id", required=True, help="Farm ID")
    parser.add_argument("--region", default="us-west-2", help="AWS region")
    parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be deleted without deleting"
    )

    args = parser.parse_args()

    try:
        count = cleanup_test_queues(args.farm_id, args.region, args.dry_run)
        sys.exit(0 if count >= 0 else 1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
