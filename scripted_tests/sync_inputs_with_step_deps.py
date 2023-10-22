# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

#! /usr/bin/env python3
import argparse
import pathlib
from pprint import pprint
from tempfile import TemporaryDirectory
import time

from deadline.job_attachments.asset_sync import AssetSync
from deadline.job_attachments._aws.deadline import get_job, get_queue

"""
A script to manually test that input syncing is functioning well in scenarios where
there are step-step dependencies within a job. The AWS account to be tested should
have an S3 bucket set up for Job Attachments, and inside the bucket, prepare a job
that has assets and outputs on two or more different steps.

How to test:

1. Run the script with the following command:
   python3 sync_inputs_with_step_deps.py -f <farm_id> -q <queue_id> -j <job_id> -s <a list of step ids separated by a whitespace>
2. See the logs on the console to confirm whether the expected files have been
   downloaded to the temporary (session) directory. (This directory will be deleted
   when the test is finished.)
"""


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-f", "--farm-id", type=str, help="Deadline Farm to download assets from.", required=True
    )
    parser.add_argument(
        "-q", "--queue-id", type=str, help="Deadline Queue to download assets from.", required=True
    )
    parser.add_argument(
        "-j", "--job-id", type=str, help="Deadline Job to download assets from.", required=True
    )
    parser.add_argument(
        "-s",
        "--step-ids",
        nargs="+",
        type=str,
        help="IDs of steps to sync inputs from",
        required=False,
    )
    args = parser.parse_args()

    farm_id = args.farm_id
    queue_id = args.queue_id
    job_id = args.job_id
    step_ids = args.step_ids

    test_sync_inputs(farm_id, queue_id, job_id, step_ids)


def test_sync_inputs(
    farm_id: str,
    queue_id: str,
    job_id: str,
    step_ids: list[str],
):
    """
    Downloads all inputs for a given job, and the outputs of the provided steps within the job.
    """
    with TemporaryDirectory() as temp_root_dir:
        print(f"Created a temporary directory for the test: {temp_root_dir}\n")

        queue = get_queue(farm_id=farm_id, queue_id=queue_id)
        job = get_job(farm_id=farm_id, queue_id=queue_id, job_id=job_id)

        print("Starting test to sync inputs...\n")
        asset_sync = AssetSync(farm_id=farm_id)

        download_start = time.perf_counter()

        (summary_statistics, local_roots) = asset_sync.sync_inputs(
            s3_settings=queue.jobAttachmentSettings,
            attachments=job.attachments,
            queue_id=queue_id,
            job_id=job_id,
            session_dir=pathlib.Path(temp_root_dir),
            step_dependencies=step_ids,
        )

        print(f"Download Summary Statistics:\n{summary_statistics}")
        print(
            f"Finished downloading after {time.perf_counter() - download_start} seconds, returned:"
        )
        pprint(local_roots)

        print("\nListing files in the temporary directory:")
        for pathmapping in local_roots:
            all_files = _get_files_list_recursively(pathlib.Path(pathmapping["destination_path"]))
            for file in all_files:
                print(file)

    print(f"\nCleaned up the temporary directory: {temp_root_dir}")


def _get_files_list_recursively(directory: pathlib.Path):
    files_list = []

    for file in directory.iterdir():
        if file.is_file():
            files_list.append(file)

    for subdirectory in directory.iterdir():
        if subdirectory.is_dir():
            subdirectory_files = _get_files_list_recursively(subdirectory)
            files_list.extend(subdirectory_files)

    return files_list


if __name__ == "__main__":
    run()
