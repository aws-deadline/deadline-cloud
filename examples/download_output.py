# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

#! /usr/bin/env python3
import argparse
import sys
import time

from deadline.job_attachments._aws.deadline import get_queue
from deadline.job_attachments.download import OutputDownloader

"""
A small script to download job output. Can provide just the Job ID to download all outputs
for a Job, optionally include the Step ID to get all outputs for the Job's Step, or optionally
include the Job, Step, and Task ID to get the outputs for a specific Task.

Example usage:

python download_output.py -f $FARM_ID -q $QUEUE_ID -j $JOB_ID
"""

if __name__ == "__main__":
    start_time = time.perf_counter()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-f", "--farm-id", type=str, help="Deadline Farm you want to download from.", required=True
    )
    parser.add_argument(
        "-q", "--queue-id", type=str, help="Deadline Queue you want to download.", required=True
    )
    parser.add_argument(
        "-j", "--job-id", type=str, help="Deadline Job you want outputs of.", required=True
    )
    parser.add_argument(
        "-s", "--step-id", type=str, help="Optional. Deadline Step you want outputs of."
    )
    parser.add_argument(
        "-t",
        "--task-id",
        type=str,
        help="Optional. Deadline Task you want outputs of. If specifying, must include Step ID.",
    )
    args = parser.parse_args()

    farm_id = args.farm_id
    queue_id = args.queue_id
    job_id = args.job_id
    step_id = args.step_id
    task_id = args.task_id

    if task_id and not step_id:
        print("Must specify Step ID when including Task ID! Stopping.")
        sys.exit()

    print("\nGetting queue settings...")
    settings = get_queue(farm_id, queue_id).jobAttachmentSettings

    print("\nStarting download...")
    start = time.perf_counter()
    output_downloader = OutputDownloader(
        s3_settings=settings,
        farm_id=farm_id,
        queue_id=queue_id,
        job_id=job_id,
        step_id=step_id,
        task_id=task_id,
    )
    output_downloader.download_job_output()
    total = time.perf_counter() - start
    print(f"Finished downloading after {total} seconds")
