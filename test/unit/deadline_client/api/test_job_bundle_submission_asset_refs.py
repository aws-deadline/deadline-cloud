# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests the deadline.client.api functions for submitting Open Job Description job bundles,
where there are PATH parameters that carry assetReference IN/OUT metadata.
"""

import os
from unittest.mock import ANY, patch

from deadline.client import api, config
from deadline.client.api import _submit_job_bundle
from deadline.client.job_bundle import submission
from deadline.job_attachments.models import (
    Attachments,
    AssetLoadingMethod,
    AssetRootManifest,
    ManifestProperties,
    OperatingSystemFamily,
)
from deadline.job_attachments.progress_tracker import SummaryStatistics

from ..shared_constants import MOCK_FARM_ID, MOCK_QUEUE_ID
from .test_job_bundle_submission import (
    MOCK_CREATE_JOB_RESPONSE,
    MOCK_GET_JOB_RESPONSE,
    MOCK_GET_QUEUE_RESPONSE,
    _write_asset_files,
)

# A YAML job template that contains every type of (file, directory) * (none, in, out, inout) asset references
JOB_BUNDLE_RELATIVE_FILE_PATH = "./file/inside/job_bundle.txt"
JOB_BUNDLE_RELATIVE_DIR_PATH = "./dir/inside/job_bundle"
JOB_TEMPLATE_ALL_ASSET_REF_VARIANTS = f"""
specificationVersion: 'jobtemplate-2023-09'
name: Job Template to test all assetReference variants.
parameterDefinitions:
- name: FileNoneDefault
  type: PATH
  objectType: FILE
  description: FILE * NONE (default)
- name: FileNone
  type: PATH
  objectType: FILE
  dataFlow: NONE
  description: FILE * NONE
- name: FileIn
  type: PATH
  objectType: FILE
  dataFlow: IN
  description: FILE * IN
  default: {JOB_BUNDLE_RELATIVE_FILE_PATH}
- name: FileInAbsolute
  type: PATH
  objectType: FILE
  dataFlow: IN
  description: FILE * IN
  default: JOB_BUNDLE_ABSOLUTE_FILE_PATH
- name: FileOut
  type: PATH
  objectType: FILE
  dataFlow: OUT
  description: FILE * OUT
- name: FileInout
  type: PATH
  objectType: FILE
  dataFlow: INOUT
  description: FILE * INOUT
- name: DirNoneDefault
  type: PATH
  objectType: DIRECTORY
  description: DIR * NONE
- name: DirNone
  type: PATH
  objectType: DIRECTORY
  dataFlow: NONE
  description: DIR * NONE
- name: DirIn
  type: PATH
  objectType: DIRECTORY
  dataFlow: IN
  description: DIR * IN
  default: {JOB_BUNDLE_RELATIVE_DIR_PATH}
- name: DirInAbsolute
  type: PATH
  objectType: DIRECTORY
  dataFlow: IN
  description: DIR * IN
  default: JOB_BUNDLE_ABSOLUTE_DIR_PATH
- name: DirOut
  type: PATH
  objectType: DIRECTORY
  dataFlow: OUT
  description: DIR * OUT
- name: DirInout
  type: PATH
  objectType: DIRECTORY
  dataFlow: INOUT
  description: DIR * INOUT
steps:
- name: CliScript
  script:
    embeddedFiles:
    - name: runScript
      type: TEXT
      runnable: true
      data: |
        #!/usr/bin/env bash
        echo '
          {{Param.FileNoneDefault}}
          {{Param.FileNone}}
          {{Param.FileIn}}
          {{Param.FileInAbsolute}}
          {{Param.FileOut}}
          {{Param.FileInout}}
          {{Param.DirNoneDefault}}
          {{Param.DirNone}}
          {{Param.DirIn}}
          {{Param.DirInAbsolute}}
          {{Param.DirOut}}
          {{Param.DirInout}}
        '
    actions:
      onRun:
        command: '{{Task.Attachment.runScript.Path}}'
"""


def test_create_job_from_job_bundle_with_all_asset_ref_variants(
    fresh_deadline_config, temp_job_bundle_dir, temp_assets_dir
):
    """
    Test a job bundle with template from JOB_TEMPLATE_ALL_ASSET_REF_VARIANTS.
    """
    # Use a temporary directory for the job bundle
    with patch.object(_submit_job_bundle.api, "get_boto3_session"), patch.object(
        _submit_job_bundle.api, "get_boto3_client"
    ) as client_mock, patch.object(_submit_job_bundle.api, "get_queue_boto3_session"), patch.object(
        submission.S3AssetManager, "hash_assets_and_create_manifest"
    ) as mock_hash_assets, patch.object(
        submission.S3AssetManager, "upload_assets"
    ) as mock_upload_assets, patch.object(
        _submit_job_bundle.api, "get_telemetry_client"
    ):
        client_mock().create_job.side_effect = [MOCK_CREATE_JOB_RESPONSE]
        client_mock().get_queue.side_effect = [MOCK_GET_QUEUE_RESPONSE]
        client_mock().get_job.side_effect = [MOCK_GET_JOB_RESPONSE]
        mock_hash_assets.return_value = [SummaryStatistics(), AssetRootManifest()]
        mock_upload_assets.return_value = [
            SummaryStatistics(),
            Attachments(
                [
                    ManifestProperties(
                        rootPath="/mnt/root/path1",
                        osType=OperatingSystemFamily.LINUX,
                        inputManifestPath="mock-manifest",
                        inputManifestHash="mock-manifest-hash",
                        outputRelativeDirectories=["."],
                    ),
                ],
            ),
        ]

        config.set_setting("defaults.farm_id", MOCK_FARM_ID)
        config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

        # Define absolute paths for testing within temp_assets_dir
        job_bundle_absolute_file_path = os.path.normpath(temp_assets_dir + "/absolute/absolute.txt")
        job_bundle_absolute_dir_path = os.path.normpath(temp_assets_dir + "/absolutedir")

        # Insert absolute paths with temp dir into job template
        job_template_replaced = JOB_TEMPLATE_ALL_ASSET_REF_VARIANTS.replace(
            "JOB_BUNDLE_ABSOLUTE_FILE_PATH", job_bundle_absolute_file_path
        )
        job_template_replaced = job_template_replaced.replace(
            "JOB_BUNDLE_ABSOLUTE_DIR_PATH", job_bundle_absolute_dir_path
        )

        # Write the YAML template
        with open(os.path.join(temp_job_bundle_dir, "template.yaml"), "w", encoding="utf8") as f:
            f.write(job_template_replaced)

        job_parameters = [
            {
                "name": "FileNoneDefault",
                "value": os.path.join(temp_assets_dir, "file/inside/asset-dir-filenonedefault.txt"),
            },
            {
                "name": "FileNone",
                "value": os.path.join(temp_assets_dir, "file/inside/asset-dir-filenone.txt"),
            },
            # Leaving out "FileIn" so it gets the default value
            {"name": "FileOut", "value": "./file/inside/cwd.txt"},
            {
                "name": "FileInout",
                "value": os.path.join(temp_assets_dir, "file/inside/asset-dir-fileinout.txt"),
            },
            {
                "name": "DirNoneDefault",
                "value": os.path.join(temp_assets_dir, "./dir/inside/asset-dir-dirnonedefault"),
            },
            {
                "name": "DirNone",
                "value": os.path.join(temp_assets_dir, "./dir/inside/asset-dir-dirnone"),
            },
            # Leaving out "DirIn" so it gets the default value
            {"name": "DirOut", "value": "./dir/inside/cwd-dirout"},
            {
                "name": "DirInout",
                "value": os.path.join(temp_assets_dir, "./dir/inside/asset-dir-dirinout"),
            },
        ]

        # Write file contents to the job bundle dir
        _write_asset_files(
            temp_job_bundle_dir,
            {
                JOB_BUNDLE_RELATIVE_FILE_PATH: "file in",
                JOB_BUNDLE_RELATIVE_DIR_PATH + "/file1.txt": "dir in file1",
                JOB_BUNDLE_RELATIVE_DIR_PATH + "/subdir/file1.txt": "dir in file2",
            },
        )
        # Write file contents to the temporary assets dir
        _write_asset_files(
            temp_assets_dir,
            {
                "file/inside/asset-dir-fileinout.txt": "file inout",
                "././dir/inside/asset-dir-dirinout/file_x.txt": "dir inout",
                "././dir/inside/asset-dir-dirinout/subdir/file_y.txt": "dir inout",
            },
        )

        # Write file contents to absolute asset directories
        _write_asset_files(
            "/",
            {
                job_bundle_absolute_file_path: "absolute file in",
                job_bundle_absolute_dir_path + "/absolute.txt": "absolute dir in",
            },
        )

        # This is the function we're testing
        api.create_job_from_job_bundle(temp_job_bundle_dir, job_parameters=job_parameters)

        # The values of input_paths and output_paths are the first
        # thing this test needs to verify, confirming that the
        # bundle dir is used for default parameter values, and the
        # current working directory is used for job parameters.
        input_paths = sorted(
            os.path.normpath(p)
            for p in [
                temp_assets_dir + "/./dir/inside/asset-dir-dirinout/file_x.txt",
                temp_assets_dir + "/./dir/inside/asset-dir-dirinout/subdir/file_y.txt",
                temp_assets_dir + "/file/inside/asset-dir-fileinout.txt",
                temp_job_bundle_dir + "/dir/inside/job_bundle/file1.txt",
                temp_job_bundle_dir + "/dir/inside/job_bundle/subdir/file1.txt",
                temp_job_bundle_dir + "/file/inside/job_bundle.txt",
                job_bundle_absolute_file_path,
                job_bundle_absolute_dir_path + "/absolute.txt",
            ]
        )
        output_paths = sorted(
            os.path.normpath(os.path.abspath(p))
            for p in [
                temp_assets_dir + "/./dir/inside/asset-dir-dirinout",
                temp_assets_dir + "/file/inside",
                "./dir/inside/cwd-dirout",
                "./file/inside",
            ]
        )
        mock_hash_assets.assert_called_once_with(
            input_paths=input_paths,
            output_paths=output_paths,
            storage_profile_id="",
            hash_cache_dir=os.path.expanduser(os.path.join("~", ".deadline", "cache")),
            on_preparing_to_submit=ANY,
        )
        client_mock().create_job.assert_called_once_with(
            farmId=MOCK_FARM_ID,
            queueId=MOCK_QUEUE_ID,
            template=ANY,
            templateType="YAML",
            attachments={
                "manifests": [
                    {
                        "rootPath": "/mnt/root/path1",
                        "osType": OperatingSystemFamily.LINUX,
                        "inputManifestPath": "mock-manifest",
                        "inputManifestHash": "mock-manifest-hash",
                        "outputRelativeDirectories": ["."],
                    },
                ],
                "assetLoadingMethod": AssetLoadingMethod.PRELOAD.value,
            },
            # The job parameter values are the second thing this test needs to verify,
            # confirming that the parameters were processed according to their types.
            parameters={
                "FileNoneDefault": {
                    "path": os.path.join(
                        temp_assets_dir, "file", "inside", "asset-dir-filenonedefault.txt"
                    ),
                },
                "FileNone": {
                    "path": os.path.normpath(
                        os.path.join(temp_assets_dir, "file", "inside", "asset-dir-filenone.txt")
                    )
                },
                "FileOut": {"path": os.path.normpath(os.path.abspath("file/inside/cwd.txt"))},
                "FileIn": {
                    "path": os.path.normpath(temp_job_bundle_dir + "/file/inside/job_bundle.txt")
                },
                "FileInout": {
                    "path": os.path.normpath(
                        temp_assets_dir + "/file/inside/asset-dir-fileinout.txt"
                    )
                },
                "DirNoneDefault": {
                    "path": os.path.join(
                        temp_assets_dir, "dir", "inside", "asset-dir-dirnonedefault"
                    ),
                },
                "DirNone": {
                    "path": os.path.join(temp_assets_dir, "dir", "inside", "asset-dir-dirnone")
                },
                "DirIn": {
                    "path": os.path.normpath(
                        os.path.join(temp_job_bundle_dir, "dir", "inside", "job_bundle")
                    )
                },
                "DirOut": {"path": os.path.normpath(os.path.abspath("dir/inside/cwd-dirout"))},
                "DirInout": {
                    "path": os.path.normpath(
                        os.path.join(temp_assets_dir, "dir", "inside", "asset-dir-dirinout")
                    )
                },
            },
        )
