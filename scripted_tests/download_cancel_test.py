# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

#! /usr/bin/env python3
import argparse
import pathlib
from tempfile import TemporaryDirectory
import time
from threading import Thread

from deadline.job_attachments.asset_sync import AssetSync
from deadline.job_attachments._aws.deadline import get_job, get_queue
from deadline.job_attachments.download import OutputDownloader
from deadline.job_attachments.exceptions import AssetSyncCancelledError

"""
A testing script to simulate cancellation of (1) syncing inputs, or (2) downloading outputs.

How to test:

1. Run the script with the following command for each test:
  (1) To test canceling syncing inputs, run the following command:
      python3 download_cancel_test.py sync_inputs -f <farm_id> -q <queue_id> -j <job_id>
  (2) To test canceling downloading outputs, run the following command:
      python3 download_cancel_test.py download_outputs -f <farm_id> -q <queue_id> -j <job_id>
2. In the middle of downloading files, you can send a cancel signal by pressing 'k' key
   and then pressing 'Enter' key in succession. Confirm that cancelling is working as expected.
"""

MESSAGE_HOW_TO_CANCEL = (
    "To stop the download process, please hit 'k' key and then 'Enter' key in succession.\n"
)
continue_reporting = True
main_terminated = False


def run():
    print(MESSAGE_HOW_TO_CANCEL)
    parser = argparse.ArgumentParser(description=MESSAGE_HOW_TO_CANCEL)
    parser.add_argument(
        "test_to_run",
        choices=["sync_inputs", "download_outputs"],
        help="Test to run. ('sync_inputs' or 'download_outputs')",
    )
    parser.add_argument(
        "-f", "--farm-id", type=str, help="Deadline Farm to download assets from.", required=True
    )
    parser.add_argument(
        "-q", "--queue-id", type=str, help="Deadline Queue to download assets from.", required=True
    )
    parser.add_argument(
        "-j", "--job-id", type=str, help="Deadline Job to download assets from.", required=True
    )
    args = parser.parse_args()

    test_to_run = args.test_to_run
    farm_id = args.farm_id
    queue_id = args.queue_id
    job_id = args.job_id

    if test_to_run == "sync_inputs":
        test_sync_inputs(farm_id=farm_id, queue_id=queue_id, job_id=job_id)
    elif test_to_run == "download_outputs":
        test_download_outputs(farm_id=farm_id, queue_id=queue_id, job_id=job_id)


def test_sync_inputs(
    farm_id: str,
    queue_id: str,
    job_id: str,
):
    """
    Tests cancellation during execution of the `sync_inputs` function.
    """
    start_time = time.perf_counter()

    with TemporaryDirectory() as temp_root_dir:
        print(f"Created a temporary directory for the test: {temp_root_dir}")

        queue = get_queue(farm_id=farm_id, queue_id=queue_id)
        job = get_job(farm_id=farm_id, queue_id=queue_id, job_id=job_id)

        print("Starting test to sync inputs...")
        asset_sync = AssetSync(farm_id=farm_id)

        try:
            download_start = time.perf_counter()
            (summary_statistics, local_roots) = asset_sync.sync_inputs(
                s3_settings=queue.jobAttachmentSettings,
                attachments=job.attachments,
                queue_id=queue_id,
                job_id=job_id,
                session_dir=pathlib.Path(temp_root_dir),
                on_downloading_files=mock_on_downloading_files,
            )
            print(f"Download Summary Statistics:\n{summary_statistics}")
            print(
                f"Finished downloading after {time.perf_counter() - download_start} seconds, returned:\n{local_roots}"
            )

        except AssetSyncCancelledError as asce:
            print(f"AssetSyncCancelledError: {asce}")
            print(f"payload: {asce.summary_statistics}")

        print(f"\nTotal test runtime: {time.perf_counter() - start_time}")

    print(f"Cleaned up the temporary directory: {temp_root_dir}")
    global main_terminated
    main_terminated = True


def test_download_outputs(
    farm_id: str,
    queue_id: str,
    job_id: str,
):
    """
    Tests cancellation during execution of the `download_job_output` function.
    """
    start_time = time.perf_counter()

    queue = get_queue(farm_id=farm_id, queue_id=queue_id)

    print("Starting test to download outputs...")

    try:
        download_start = time.perf_counter()
        output_downloader = OutputDownloader(
            s3_settings=queue.jobAttachmentSettings,
            farm_id=farm_id,
            queue_id=queue_id,
            job_id=job_id,
        )
        summary_statistics = output_downloader.download_job_output(
            on_downloading_files=mock_on_downloading_files
        )
        print(f"Download Summary Statistics:\n{summary_statistics}")
        print(f"Finished downloading after {time.perf_counter() - download_start} seconds")

    except AssetSyncCancelledError as asce:
        print(f"AssetSyncCancelledError: {asce}")
        print(f"payload: {asce.summary_statistics}")

    print(f"\nTotal test runtime: {time.perf_counter() - start_time}")

    global main_terminated
    main_terminated = True


def mock_on_downloading_files(metadata):
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
