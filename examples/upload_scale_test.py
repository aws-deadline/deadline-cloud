# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

#! /usr/bin/env python3
import argparse
import os
import pathlib
import sys
import time

from deadline.job_attachments.aws.deadline import get_queue
from deadline.job_attachments.download import download_files_from_manifests, get_manifest_from_s3
from deadline.job_attachments.upload import S3AssetManager

NUM_SMALL_FILES = 20000
NUM_MEDIUM_FILES = 20000
NUM_LARGE_FILES = 1

"""
A simple scale testing script for measuring input file upload and hashing speed.
Creates a large amount of local text files and uploads them to the S3 bucket configured
for the given Farm's Queue.

Optionally, downloads the same files that were uploaded, to a different directory.

You can profile this by running with cProfile:
python -m cProfile -o profile.prof upload_scale_test.py -f $FARM_ID -q $QUEUE_ID

You can then visualize the data by running it through a tool like 'snakeviz' (just pip install):
snakeviz profile.prof
"""

if __name__ == "__main__":
    start_time = time.perf_counter()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-f", "--farm-id", type=str, help="Deadline Farm you want to submit to.", required=True
    )
    parser.add_argument(
        "-q", "--queue-id", type=str, help="Deadline Queue you want to submit to.", required=True
    )
    parser.add_argument(
        "-sd",
        "--skip-download",
        help="Specify this flag to skip the download step",
        required=False,
        action="store_true",
    )
    parser.add_argument(
        "-so",
        "--setup-only",
        help="Specify this flag to only generate local files for setup",
        required=False,
        action="store_true",
    )
    args = parser.parse_args()

    farm_id = args.farm_id
    queue_id = args.queue_id

    print("Setting up the test...")

    files = []
    root_path = pathlib.Path("/tmp/test_submit")
    make_test_files = not root_path.exists()
    root_path.mkdir(parents=True, exist_ok=True)

    # Make a ton of small files
    if NUM_SMALL_FILES > 0:
        for i in range(0, NUM_SMALL_FILES):
            file_path = root_path / f"small_test{i}.txt"
            if not os.path.exists(file_path):
                with file_path.open("w", encoding="utf-8") as f:
                    f.write(f"test value: {i}")
            files.append(str(file_path))

    # Make 100GB worth of 5MB files
    if NUM_MEDIUM_FILES > 0:
        for i in range(0, NUM_MEDIUM_FILES):
            file_path = root_path / f"medium_test{i}.txt"
            if not os.path.exists(file_path):
                with file_path.open("wb") as f:
                    f.write(os.urandom(5242880))  # 5 MB files
            files.append(str(file_path))

    # Make a 100GB file to test large file sizes (100 GB each)
    if NUM_LARGE_FILES > 0:
        for i in range(0, NUM_LARGE_FILES):
            file_path = root_path / f"large_test{i}.txt"
            if not os.path.exists(file_path):
                for i in range(100):  # Let's make it 100 GB for now
                    with file_path.open("ab") as f:
                        f.write(os.urandom(1073741824))  # Write 1 GB at a time
            files.append(str(file_path))

    if args.setup_only:
        print("\nFinished setup, exiting.")
        sys.exit()

    queue = get_queue(farm_id=farm_id, queue_id=queue_id)
    asset_manager = S3AssetManager(
        job_attachment_settings=queue.jobAttachmentSettings, farm_id=farm_id, queue_id=queue_id
    )

    print("\nStarting upload test...")
    start = time.perf_counter()

    (summary_statistics_hashing, manifests) = asset_manager.hash_assets_and_create_manifest(
        files, [root_path / "outputs"]
    )
    print(f"Summary Statistics for file hashing:\n{summary_statistics_hashing}")

    (summary_statistics_upload, attachment_settings) = asset_manager.upload_assets(manifests)
    print(f"Summary Statistics for file uploads:\n{summary_statistics_upload}")

    total = time.perf_counter() - start
    print(
        f"Finished uploading after {total} seconds, created these attachment settings:\n{attachment_settings.to_dict()}"
    )

    if not args.skip_download:
        print("\nStarting download test...")
        start = time.perf_counter()
        man_prefx = (
            attachment_settings.assetRoots[0]
            .requiredAssets[0]
            .manifestUrl.replace(f"s3://{queue.jobAttachmentSettings.s3BucketName}/", "")
        )
        man = get_manifest_from_s3(man_prefx, queue.jobAttachmentSettings.s3BucketName)
        download_files_from_manifests(
            queue.jobAttachmentSettings.s3BucketName,
            {"/tmp/test_download": [man]},
            queue.jobAttachmentSettings.full_cas_prefix(),
        )
        total = time.perf_counter() - start
        print(f"Finished downloading after {total} seconds")

    print(f"\nTotal test runtime: {time.perf_counter() - start_time}")
