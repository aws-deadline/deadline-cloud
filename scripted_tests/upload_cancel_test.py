# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

#! /usr/bin/env python3
import argparse
import os
import pathlib
import time
from threading import Thread

from deadline.job_attachments._aws.deadline import get_queue
from deadline.job_attachments.exceptions import AssetSyncCancelledError
from deadline.job_attachments.upload import S3AssetManager

"""
A testing script to simulate cancelling a hash/upload of assets.
It creates a large amount of local text files and uploads them to the S3 bucket
configured for the given Farm's Queue.

How to test:

1. Run this script with the following command:
   python3 upload_cancel_test.py -f <farm_id> -q <queue_id>
2. In the middle of hashing or uploading those files, you can send a cancel
   signal by pressing 'k' and Enter keys in succession. Confirm that cancelling
   is working as expected by checking the console output.

Note: This script generates test files in the /tmp/test_submit directory for testing
purpose. But it does not delete these files after the test is completed.
"""

MESSAGE_HOW_TO_CANCEL = (
    "To stop the hash/upload process, please hit 'k' key and then 'Enter' key in succession.\n"
)

NUM_TINY_FILES = 0
NUM_SMALL_FILES = 0
NUM_MEDIUM_FILES = 0
NUM_LARGE_FILES = 1

continue_reporting = True
main_terminated = False


def run():
    print(MESSAGE_HOW_TO_CANCEL)
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

    if NUM_TINY_FILES > 0:
        for i in range(0, NUM_TINY_FILES):
            file_path = root_path / f"tiny_test{i}.txt"
            if not os.path.exists(file_path):
                with file_path.open("wb") as f:
                    f.write(os.urandom(2 * (1024**2)))  # 2 MB files
            files.append(str(file_path))

    # Make small files
    if NUM_SMALL_FILES > 0:
        for i in range(0, NUM_SMALL_FILES):
            file_path = root_path / f"small_test{i}.txt"
            if not os.path.exists(file_path):
                with file_path.open("wb") as f:
                    f.write(os.urandom(10 * (1024**2)))  # 10 MB files
            files.append(str(file_path))

    # Make medium-sized files
    if NUM_MEDIUM_FILES > 0:
        for i in range(0, NUM_MEDIUM_FILES):
            file_path = root_path / f"medium_test{i}.txt"
            if not os.path.exists(file_path):
                with file_path.open("wb") as f:
                    f.write(os.urandom(100 * (1024**2)))  # 100 MB files
            files.append(str(file_path))

    # Make large files
    if NUM_LARGE_FILES > 0:
        for i in range(0, NUM_LARGE_FILES):
            file_path = root_path / f"large_test{i}.txt"
            if not os.path.exists(file_path):
                with file_path.open("ab") as f:
                    _create_large_file_with_chunks(file_path, 20 * (1024**3), 10**9)
            files.append(str(file_path))

    queue = get_queue(farm_id=farm_id, queue_id=queue_id)
    asset_manager = S3AssetManager(
        farm_id=farm_id, queue_id=queue_id, job_attachment_settings=queue.jobAttachmentSettings
    )

    print("\nStarting test...")
    start = time.perf_counter()

    try:
        print("\nStart hashing...")
        upload_group = asset_manager.prepare_paths_for_upload(
            ".", files, [root_path / "outputs"], []
        )
        (summary_statistics_hashing, manifests) = asset_manager.hash_assets_and_create_manifest(
            asset_groups=upload_group.asset_groups,
            total_input_files=upload_group.total_input_files,
            total_input_bytes=upload_group.total_input_bytes,
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
        print(f"payload:\n{asce.summary_statistics}")

    print(f"\nTotal test runtime: {time.perf_counter() - start_time}")

    global main_terminated
    main_terminated = True


def _create_large_file_with_chunks(file_path: str, total_size: int, chunk_size: int) -> None:
    """
    Creates a large file of a given total size by writing in chunks with random data.
    It prevents MemoryError by dividing the size into manageable chunks and writing
    each chunk sequentially.
    """
    with open(file_path, "wb") as f:
        num_chunks = total_size // chunk_size
        for _ in range(num_chunks):
            f.write(os.urandom(chunk_size))
        remaining = total_size % chunk_size
        if remaining > 0:
            f.write(os.urandom(remaining))


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
