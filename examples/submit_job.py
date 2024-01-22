# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

#! /usr/bin/env python3

import argparse
import pprint
import time
from pathlib import Path

import boto3

from deadline.job_attachments.upload import S3AssetManager
from deadline.job_attachments.models import JobAttachmentS3Settings

"""
This is a sample script that illustrates how to submit a custom job using the
Job Attachments library. Please make sure to specify `endpoint_url` to the target
endpoint you want to test, when creating a (boto3) service client for deadline.

Example usage:

python submit_job.py -f $FARM_ID -q $QUEUE_ID -i /tmp/asset_root/inputs -o /tmp/asset_root/outputs
"""


def process_job_attachments(farm_id, queue_id, inputs, outputDir, deadline_client):
    """
    Uploads all of the input files to the Job Attachments S3 bucket associated with
    the Deadline Queue, returning Attachment Settings to be associated with a Deadline Job.
    """

    print("Getting queue information...")
    start = time.perf_counter()
    queue = deadline_client.get_queue(farmId=farm_id, queueId=queue_id)
    total = time.perf_counter() - start
    print(f"Finished getting queue information after {total} seconds.\n")

    print(f"Processing {len(inputs)} job attachments...")
    start = time.perf_counter()
    asset_manager = S3AssetManager(
        farm_id=farm_id,
        queue_id=queue_id,
        job_attachment_settings=JobAttachmentS3Settings(**queue["jobAttachmentSettings"]),
    )
    upload_group = asset_manager.prepare_paths_for_upload(".", inputs, [outputDir], [])
    (_, manifests) = asset_manager.hash_assets_and_create_manifest(
        upload_group.asset_groups, upload_group.total_input_files, upload_group.total_input_bytes
    )
    (_, attachments) = asset_manager.upload_assets(manifests)
    attachments = attachments.to_dict()
    total = time.perf_counter() - start
    print(f"Finished processing job attachments after {total} seconds.\n")
    print(f"Created these attachment settings: {attachments}\n")

    return attachments


JOB_TEMPLATE = """specificationVersion: 'jobtemplate-2023-09'
name: SubmitJobExample
description: >
    A Job that counts the number of files and total size,
    and also creates a default output file.
parameterDefinitions:
  - name: DataDir
    type: PATH
    objectType: DIRECTORY
    dataFlow: INOUT
  - name: RelOutput
    type: PATH
steps:
  - name: layerDefaultFrames
    script:
        actions:
            onRun:
                command: '{{Task.File.Run}}'
        embeddedFiles:
          - name: Run
            filename: count-files.sh
            type: TEXT
            runnable: true
            data: |
                #!/bin/env bash

                set -euo pipefail
                echo 'Confirming that inputs were downloaded to the correct location'",
                echo 'Total number of inputs' && find {{Param.DataDir}} -type f | wc -l",
                echo 'Total file size' && du -hs {{Param.DataDir}}",
                echo 'Creating the expected output directory and output file'",
                mkdir -p {{Param.DataDir}}/{{Param.RelOutput}}",
                echo 'This is test output' > {{Param.DataDir}}/{{Param.RelOutput}}/output.txt",
"""


def submit_custom_job(
    farm_id, queue_id, job_template, attachment_settings, parameters, deadline_client
):
    """
    Submits a Job defined in the Job Template to the given Queue, adding the givent Attachment Settings
    to the Job definition.
    """

    # Submit the Job
    print("Submitting the job...")
    start = time.perf_counter()
    response = deadline_client.create_job(
        farmId=farm_id,
        queueId=queue_id,
        template=job_template,
        templateType="YAML",
        attachments=attachment_settings if attachment_settings else None,
        parameters=parameters,
        priority=50,
    )
    total = time.perf_counter() - start
    print(f"Submitted Job Template after {total} seconds:")
    pprint.pprint(job_template.encode())
    print(f"Job ID: {response['jobId']}")


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
        "-i",
        "--input-files",
        type=str,
        help="List of input files (or directories) you want to upload to be used with the Job.",
        action="append",
        required=True,
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=str,
        help="A single output directory used by the Job.",
        required=True,
    )
    parser.add_argument(
        "-ao",
        "--assets-only",
        help="Specify this flag to only upload input files. No job will be submitted. Helpful when pre-populating the Job Attachments S3 bucket.",
        action="store_true",
        required=False,
    )

    args = parser.parse_args()

    inputs = []
    for input in args.input_files:
        file_path = Path(input)
        if file_path.is_dir():
            inputs.extend(
                [
                    str(file)
                    for file in file_path.glob("**/*")
                    if not file.is_dir() and file.exists()
                ]
            )
        else:
            inputs.append(str(file_path))

    deadline_client = boto3.client(
        "deadline",
        region_name="us-west-2",
        endpoint_url="https://management.deadline.us-west-2.amazonaws.com",
    )

    attachments = process_job_attachments(
        args.farm_id, args.queue_id, inputs, args.output_dir, deadline_client
    )

    if not args.assets_only:
        root_dir = attachments["manifests"][0]["rootPath"]
        rel_output = str(Path(args.output_dir).relative_to(root_dir))
        submit_custom_job(
            args.farm_id,
            args.queue_id,
            JOB_TEMPLATE,
            attachments,
            {"DataDir": {"path": root_dir}, "RelOutput": {"path": rel_output}},
            deadline_client,
        )

    print(f"\nTotal submit runtime: {time.perf_counter() - start_time}")
