# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

#! /usr/bin/env python3

"""
This is a sample script that illustrates how to submit a custom job using
the job attachments library.

Example usage:
python submit_job.py -f $FARM_ID -q $QUEUE_ID -r /tmp/asset_root -i /tmp/asset_root/inputs -o /tmp/asset_root/outputs
"""

import argparse
import json
import pprint
import time
from pathlib import Path

import boto3
from openjd.model.template import SchemaVersion, TemplateModelRegistry

from deadline.job_attachments.aws.deadline import get_queue
from deadline.job_attachments.upload import S3AssetManager
from deadline.job_attachments.utils import (
    get_deadline_formatted_os,
    get_unique_dest_dir_name,
    map_source_path_to_dest_path,
)


def process_job_attachments(farm_id, queue_id, inputs, outputDir):
    """
    Uploads all of the input files to the Job Attachments S3 bucket associated with
    the Deadline Queue, returning Attachment Settings to be associated with a Deadline Job.
    """

    print("Getting queue information...")
    start = time.perf_counter()
    queue = get_queue(farm_id=farm_id, queue_id=queue_id)
    total = time.perf_counter() - start
    print(f"Finished getting queue information after {total} seconds.\n")

    print(f"Processing {len(inputs)} job attachments...")
    start = time.perf_counter()
    asset_manager = S3AssetManager(job_attachment_settings=queue.jobAttachmentSettings)
    (_, manifests) = asset_manager.hash_assets_and_create_manifest(inputs, [outputDir])
    (_, attachments) = asset_manager.upload_assets(manifests)
    attachments = attachments.to_dict()
    total = time.perf_counter() - start
    print(f"Finished processing job attachments after {total} seconds.\n")
    print(f"Created these attachment settings: {attachments}\n")

    return attachments


def create_job_template(asset_root, outputDir):
    """
    Creates a Job Template that defines a Task that counts the number of files and total size,
    and also creates a default output file.
    """

    # Since we're hardcoding paths in this job template, we need to determine what
    # the session directory will be, using the same code as the Job Attachments lib.
    session_dir = get_unique_dest_dir_name(asset_root)
    # Map everything to linux for now since Workers only run on Linux
    # The only reason we need to do this mapping is because we're hardcoding paths
    # in the job template, so we have to assume what OS the Worker will have.
    root_path = map_source_path_to_dest_path(get_deadline_formatted_os(), "linux", asset_root)
    rel_output = map_source_path_to_dest_path(
        get_deadline_formatted_os(), "linux", outputDir
    ).relative_to(root_path)

    # Create Job Template
    template_model = TemplateModelRegistry.get_template_model(version=SchemaVersion.v2022_05_01)

    script_start = ["#!/bin/env bash", "set -ex"]

    run = template_model.InlineTextAttachmentEntity(
        version=template_model.schema_version,
        name="run",
        data="\n".join(
            [
                *script_start,
                "echo 'Confirming that inputs were downloaded to the correct location'",
                f"echo 'Total number of inputs' && find {{{{ Builtin.SessionDirectory }}}}/{session_dir}/ -type f | wc -l",
                f"echo 'Total file size' && du -hs {{{{ Builtin.SessionDirectory }}}}/{session_dir}/",
                "echo 'Creating the expected output directory and output file'",
                f"mkdir -p {{{{ Builtin.SessionDirectory }}}}/{session_dir}/{rel_output}",
                f"echo 'This is test output' > {{{{ Builtin.SessionDirectory }}}}/{session_dir}/{rel_output}/output.txt",
            ]
        ),
        is_runnable=True,
    )

    step = template_model.StepTemplateEntity(
        version=template_model.schema_version,
        name="custom-step",
        script=template_model.StepScriptEntity(
            version=template_model.schema_version,
            actions=template_model.StepActionsEntity(
                version=template_model.schema_version,
                run=template_model.ActionEntity(
                    version=template_model.schema_version,
                    command=f"{{{{ {template_model.TASK_ATTACHMENT_NAMESPACE}.run.Path }}}}",
                ),
            ),
            attachments=[
                run,
            ],
        ),
        parameter_space=template_model.StepParameterSpaceEntity(
            version=template_model.schema_version,
            parameters=[
                template_model.TaskParameterEntity(
                    version=template_model.schema_version,
                    name="frame",
                    range_list=["0"],
                )
            ],
        ),
    )

    return template_model.JobTemplateEntity(
        version=template_model.schema_version, name="custom-job", steps=[step]
    )


def submit_custom_job(farm_id, queue_id, job_template, attachment_settings):
    """
    Submits a Job defined in the Job Template to the given Queue, adding the givent Attachment Settings
    to the Job definition.
    """

    template_json = json.dumps(job_template.encode())

    # Submit the Job
    print("Submitting the job...")
    start = time.perf_counter()
    deadline = boto3.client("deadline", region_name="us-west-2")
    response = deadline.create_job(
        farmId=farm_id,
        queueId=queue_id,
        template=template_json,
        templateType="JSON",
        attachmentSettings=attachment_settings if attachment_settings else None,
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

    attachments = process_job_attachments(args.farm_id, args.queue_id, inputs, args.output_dir)

    if not args.assets_only:
        template = create_job_template(attachments["manifests"][0]["rootPath"], args.output_dir)
        submit_custom_job(args.farm_id, args.queue_id, template, attachments)

    print(f"\nTotal submit runtime: {time.perf_counter() - start_time}")
