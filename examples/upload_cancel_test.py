# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

#! /usr/bin/env python3
import argparse
import os
import pathlib
import time
from threading import Thread

from deadline.job_attachments.aws.deadline import get_queue
from deadline.job_attachments.errors import AssetSyncCancelledError
from deadline.job_attachments.upload import S3AssetManager

NUM_SMALL_FILES = 0
NUM_MEDIUM_FILES = 0
NUM_LARGE_FILES = 1

"""
A testing script to simulate cancelling a hash/upload of assets.
First, creates a large amount of local text files and uploads them to the S3 bucket
configured for the given Farm's Queue.
Then, in the middle of hashing or uploading those files, you can send a cancel signal
by pressing 'k' and Enter keys in succession.
"""

continue_reporting = True
main_terminated = False


def run():
    start_time = time.perf_counter()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-f", "--farm-id", type=str, help="Deadline Farm you want to submit to.", required=True
    )
    parser.add_argument(
        "-q", "--queue-id", type=str, help="Deadline Queue you want to submit to.", required=True
    )
    args = parser.parse_args()

    farm_id = args.farm_id
    queue_id = args.queue_id

    print("Setting up the test...")

    files = []
    root_path = pathlib.Path("/tmp/test_submit")
    root_path.mkdir(parents=True, exist_ok=True)

    # Make small files
    if NUM_SMALL_FILES > 0:
        for i in range(0, NUM_SMALL_FILES):
            file_path = root_path / f"small_test{i}.txt"
            if not os.path.exists(file_path):
                with file_path.open("w", encoding="utf-8") as f:
                    f.write(f"test value: {i}")
            files.append(str(file_path))

    # Make medium-sized files
    if NUM_MEDIUM_FILES > 0:
        for i in range(0, NUM_MEDIUM_FILES):
            file_path = root_path / f"medium_test{i}.txt"
            if not os.path.exists(file_path):
                with file_path.open("wb") as f:
                    f.write(os.urandom(102428800))  # 100 MB files
            files.append(str(file_path))

    # Make large files
    if NUM_LARGE_FILES > 0:
        for i in range(0, NUM_LARGE_FILES):
            file_path = root_path / f"large_test{i}.txt"
            if not os.path.exists(file_path):
                for i in range(1):
                    with file_path.open("ab") as f:
                        f.write(os.urandom(1073741824))  # Write 1 GB at a time
            files.append(str(file_path))

    queue = get_queue(farm_id=farm_id, queue_id=queue_id)
    asset_manager = S3AssetManager(job_attachment_settings=queue.jobAttachmentSettings)

    print("\nStarting test...")
    start = time.perf_counter()

    try:
        print("\nStart hashing...")
        (summary_statistics_hashing, manifests) = asset_manager.hash_assets_and_create_manifest(
            files,
            [root_path / "outputs"],
            on_preparing_to_submit=mock_on_preparing_to_submit,
        )
        print(f"Hashing Summary Statistics:\n{summary_statistics_hashing}")

        print("\nStart uploading...")
        (summary_statistics_upload, attachment_settings) = asset_manager.upload_assets(
            manifests, on_uploading_assets=mock_on_uploading_assets
        )
        print(f"Upload Summary Statistics:\n{summary_statistics_upload}")

        total = time.perf_counter() - start
        print(
            f"Finished uploading after {total} seconds, created these attachment settings:\n{attachment_settings.to_dict()}"
        )
    except AssetSyncCancelledError as asce:
        print(f"AssetSyncCancelledError: {asce}")
        print(f"payload: {asce.summary_statistics}")

    print(f"\nTotal test runtime: {time.perf_counter() - start_time}")

    global main_terminated
    main_terminated = True


def mock_on_preparing_to_submit(metadata):
    print(metadata)
    return mock_on_cancellation_check()


def mock_on_uploading_assets(metadata):
    print(metadata)
    return mock_on_cancellation_check()


def mock_on_cancellation_check():
    return continue_reporting


def wait_for_cancellation_input():
    while not main_terminated:
        ch = input()
        if ch == "k":
            set_cancelled()
            break


def set_cancelled():
    global continue_reporting
    continue_reporting = False
    print("Canceled the process.")


if __name__ == "__main__":
    t = Thread(target=wait_for_cancellation_input)
    t.start()
    run()
