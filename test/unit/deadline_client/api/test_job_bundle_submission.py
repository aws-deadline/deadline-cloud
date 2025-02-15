# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests the deadline.client.api functions for submitting Open Job Description job bundles.
"""

import json
import os
from logging import INFO
from pathlib import Path
from typing import Any, Dict, Tuple
from unittest.mock import ANY, patch, Mock
from deadline.client import exceptions

import pytest
import time

from deadline.client import api, config
from deadline.client.api import _submit_job_bundle
from deadline.job_attachments.exceptions import MisconfiguredInputsError
from deadline.job_attachments.models import (
    AssetRootGroup,
    AssetUploadGroup,
    Attachments,
    FileSystemLocation,
    FileSystemLocationType,
    JobAttachmentsFileSystem,
    ManifestProperties,
    PathFormat,
    StorageProfile,
    StorageProfileOperatingSystemFamily,
)
from deadline.job_attachments.upload import S3AssetManager
from deadline.job_attachments.progress_tracker import SummaryStatistics, ProgressReportMetadata

from ..shared_constants import (
    MOCK_BUCKET_NAME,
    MOCK_FARM_ID,
    MOCK_STORAGE_PROFILE_ID,
    MOCK_QUEUE_ID,
)

MOCK_GET_QUEUE_RESPONSE = {
    "queueId": MOCK_QUEUE_ID,
    "displayName": "Test Queue",
    "description": "",
    "farmId": MOCK_FARM_ID,
    "status": "ACTIVE",
    "logBucketName": MOCK_BUCKET_NAME,
    "jobAttachmentSettings": {
        "s3BucketName": MOCK_BUCKET_NAME,
        "rootPrefix": "AWS Deadline Cloud",
    },
    "sessionRoleArn": "arn:aws:iam::123456789012:role/DeadlineQueueSessionRole",
    "createdAt": "2022-11-22T06:37:35+00:00",
    "createdBy": "arn:aws:sts::123456789012:assumed-role/Admin/user",
    "updatedAt": "2022-11-22T22:26:57+00:00",
    "updatedBy": "0123abcdf-abcd-0123-fa82-0123456abcd1",
}

MOCK_JOB_ID = "job-0123456789abcdefghijklmnopqrstuv"

MOCK_CREATE_JOB_RESPONSE = {"jobId": MOCK_JOB_ID}

MOCK_STATUS_MESSAGE = "Testing123"

MOCK_GET_JOB_RESPONSE = {"state": "READY", "lifecycleStatusMessage": MOCK_STATUS_MESSAGE}

MOCK_GET_STORAGE_PROFILE_FOR_QUEUE_RESPONSE = {
    "storageProfileId": MOCK_STORAGE_PROFILE_ID,
    "displayName": "SP-linux",
    "osFamily": "LINUX",
    "fileSystemLocations": [
        {"name": "FSL Local", "path": "/home/username/my_bundle", "type": "LOCAL"},
        {"name": "FSL Shared", "path": "/mnt/shared/movie1", "type": "SHARED"},
    ],
}

MOCK_STORAGE_PROFILE = StorageProfile(
    storageProfileId=MOCK_STORAGE_PROFILE_ID,
    displayName="SP-linux",
    osFamily=StorageProfileOperatingSystemFamily.LINUX,
    fileSystemLocations=[
        FileSystemLocation(
            name="FSL Local",
            path="/home/username/my_bundle",
            type=FileSystemLocationType.LOCAL,
        ),
        FileSystemLocation(
            name="FSL Shared",
            path="/mnt/shared/movie1",
            type=FileSystemLocationType.SHARED,
        ),
    ],
)


def get_minimal_json_job_template(job_name):
    return json.dumps(
        {
            "specificationVersion": "jobtemplate-2023-09",
            "name": job_name,
            "parameterDefinitions": [
                {"name": "priority", "type": "INT", "default": 10},
                {"name": "sceneFile", "type": "STRING", "default": "/tmp/scene"},
            ],
            "steps": [
                {
                    "name": "CliScript",
                    "script": {
                        "embeddedFiles": [
                            {
                                "name": "runScript",
                                "type": "TEXT",
                                "runnable": True,
                                "data": '#!/usr/bin/env bash\n\necho "Running the task"\nsleep 35\n',
                            }
                        ],
                        "actions": {"onRun": {"command": "{{Task.File.runScript}}"}},
                    },
                }
            ],
        }
    )


# This contains tuples of:
#    (file type, JSON/YAML content)
MOCK_JOB_TEMPLATE_CASES = {
    "MINIMAL_JSON": (
        "JSON",
        get_minimal_json_job_template(job_name="CLI Job"),
    ),
    "MINIMAL_YAML": (
        "YAML",
        """
specificationVersion: 'jobtemplate-2023-09'
name: CLI Job
parameterDefinitions:
- name: priority
  type: INT
  default: 10
- name: sceneFile
  type: STRING
  default: "/tmp/scene"
steps:
- name: CliScript
  script:
    embeddedFiles:
    - name: runScript
      type: TEXT
      runnable: true
      data: |
          #!/usr/bin/env bash

          echo "Running the task"
          sleep 35
    actions:
      onRun:
        command: "{{Task.File.runScript}}"
""",
    ),
}

# This contains tuples of:
#    (file type, JSON/YAML content, expected additional create_job parameters)
MOCK_PARAMETERS_CASES: Dict[str, Tuple[str, str, Dict[str, Any]]] = {
    "NO_PARAMETERS_FILE": ("", "", {}),
    # A parameter_values.json/yaml file with no parameter values
    "EMPTY_JSON": (
        "JSON",
        """
{
 "parameterValues": []
}
""",
        {},
    ),
    "EMPTY_YAML": (
        "YAML",
        """
parameterValues: []
""",
        {},
    ),
    # A parameter_values.json/yaml file with just AWS Deadline Cloud-specific values
    "DEADLINE_ONLY_JSON": (
        "JSON",
        """
{
 "parameterValues": [
    {
        "name": "deadline:priority",
        "value": 45
    },
    {
        "name": "deadline:targetTaskRunStatus",
        "value": "SUSPENDED"
    },
    {
        "name": "deadline:maxFailedTasksCount",
        "value": 20
    },
    {
        "name": "deadline:maxRetriesPerTask",
        "value": 5
    },
    {
        "name": "deadline:maxWorkerCount",
        "value": 10
    }
 ]
}
""",
        {
            "priority": 45,
            "targetTaskRunStatus": "SUSPENDED",
            "maxFailedTasksCount": 20,
            "maxRetriesPerTask": 5,
            "maxWorkerCount": 10,
        },
    ),
    "DEADLINE_ONLY_YAML": (
        "YAML",
        """
parameterValues:
- name: "deadline:priority"
  value: 45
- name: "deadline:targetTaskRunStatus"
  value: SUSPENDED
- name: "deadline:maxFailedTasksCount"
  value: 250
- name: "deadline:maxRetriesPerTask"
  value: 15
- name: "deadline:maxWorkerCount"
  value: 10
""",
        {
            "priority": 45,
            "targetTaskRunStatus": "SUSPENDED",
            "maxFailedTasksCount": 250,
            "maxRetriesPerTask": 15,
            "maxWorkerCount": 10,
        },
    ),
    # A parameter_values.json/yaml file with just job template values
    "TEMPLATE_ONLY_JSON": (
        "JSON",
        """
{
 "parameterValues": [
    {
        "name": "priority",
        "value": "500"
    },
    {
        "name": "sceneFile",
        "value": "/mnt/prod/project1/main_scene.mb"
    }
 ]
}
""",
        {
            "parameters": {
                "priority": {"int": "500"},
                "sceneFile": {"string": "/mnt/prod/project1/main_scene.mb"},
            },
        },
    ),
    "TEMPLATE_ONLY_YAML": (
        "YAML",
        """
parameterValues:
- name: "priority"
  value: "500"
- name: "sceneFile"
  value: /mnt/prod/project1/main_scene.mb
""",
        {
            "parameters": {
                "priority": {"int": "500"},
                "sceneFile": {"string": "/mnt/prod/project1/main_scene.mb"},
            },
        },
    ),
}

MOCK_PARAMETERS_JSON_NONEXISTENT_DEADLINE_PARAMETER = """
{
 "parameterValues": [
    {
        "name": "deadline:nonExistentParameter",
        "value": 45
    }
 ]
}
"""


@pytest.mark.parametrize("job_template_case", MOCK_JOB_TEMPLATE_CASES.keys())
@pytest.mark.parametrize("parameters_case", MOCK_PARAMETERS_CASES.keys())
def test_create_job_from_job_bundle(
    fresh_deadline_config, temp_job_bundle_dir, job_template_case, parameters_case
):
    """
    Test a matrix of different job template and parameters file cases.
    """
    job_template_type, job_template = MOCK_JOB_TEMPLATE_CASES[job_template_case]
    parameters_type, parameters, expected_create_job_parameters = MOCK_PARAMETERS_CASES[
        parameters_case
    ]
    with patch.object(api._session, "get_boto3_session") as session_mock:
        session_mock().client("deadline").create_job.side_effect = [MOCK_CREATE_JOB_RESPONSE]
        session_mock().client("deadline").get_job.return_value = MOCK_GET_JOB_RESPONSE
        session_mock().client(
            "deadline"
        ).get_storage_profile_for_queue.return_value = MOCK_GET_STORAGE_PROFILE_FOR_QUEUE_RESPONSE

        config.set_setting("defaults.farm_id", MOCK_FARM_ID)
        config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
        config.set_setting("settings.storage_profile_id", MOCK_STORAGE_PROFILE_ID)

        # Write the template to the job bundle
        with open(
            os.path.join(temp_job_bundle_dir, f"template.{job_template_type.lower()}"),
            "w",
            encoding="utf8",
        ) as f:
            f.write(job_template)

        # Write the parameter values to the job bundle, if the test case parameter includes them
        if parameters_type:
            with open(
                os.path.join(temp_job_bundle_dir, f"parameter_values.{parameters_type.lower()}"),
                "w",
                encoding="utf8",
            ) as f:
                f.write(parameters)

        # This is the function under test
        response = api.create_job_from_job_bundle(
            job_bundle_dir=temp_job_bundle_dir,
            queue_parameter_definitions=[],
        )

    # The response from the API is returned verbatim
    assert response == MOCK_JOB_ID
    expected_create_job_parameters_dict: dict = dict(**expected_create_job_parameters)
    expected_create_job_parameters_dict["priority"] = expected_create_job_parameters_dict.get(
        "priority", 50
    )
    session_mock().client("deadline").create_job.assert_called_once_with(
        farmId=MOCK_FARM_ID,
        queueId=MOCK_QUEUE_ID,
        template=job_template,
        templateType=job_template_type,
        storageProfileId=MOCK_STORAGE_PROFILE_ID,
        **expected_create_job_parameters_dict,
    )


def test_create_job_from_job_bundle_error_missing_template(
    fresh_deadline_config, temp_job_bundle_dir
):
    """
    Test a job bundle with missing template.
    """
    with patch.object(api._session, "get_boto3_session") as session_mock, patch.object(
        _submit_job_bundle.api, "get_deadline_cloud_library_telemetry_client"
    ):
        session_mock().client("deadline").create_job.side_effect = [MOCK_CREATE_JOB_RESPONSE]

        config.set_setting("defaults.farm_id", MOCK_FARM_ID)
        config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

        # Don't write a template file

        # Write the parameters to the job bundle, if the test case parameter includes them
        with open(
            os.path.join(temp_job_bundle_dir, "parameter_values.json"), "w", encoding="utf8"
        ) as f:
            f.write(MOCK_PARAMETERS_CASES["DEADLINE_ONLY_JSON"][1])

        # This is the function under test
        with pytest.raises(exceptions.DeadlineOperationError):
            api.create_job_from_job_bundle(
                job_bundle_dir=temp_job_bundle_dir,
                queue_parameter_definitions=[],
            )


def test_create_job_from_job_bundle_error_duplicate_template(
    fresh_deadline_config, temp_job_bundle_dir
):
    """
    Test a job bundle with both a JSON and YAML template.
    """
    with patch.object(api._session, "get_boto3_session") as session_mock, patch.object(
        _submit_job_bundle.api, "get_deadline_cloud_library_telemetry_client"
    ):
        session_mock().client("deadline").create_job.side_effect = [MOCK_CREATE_JOB_RESPONSE]

        config.set_setting("defaults.farm_id", MOCK_FARM_ID)
        config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

        # Write both a JSON and YAML template file
        with open(os.path.join(temp_job_bundle_dir, "template.json"), "w", encoding="utf8") as f:
            f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1])
        with open(os.path.join(temp_job_bundle_dir, "template.yaml"), "w", encoding="utf8") as f:
            f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_YAML"][1])

        # Write the parameters to the job bundle
        with open(
            os.path.join(temp_job_bundle_dir, "parameter_values.json"), "w", encoding="utf8"
        ) as f:
            f.write(MOCK_PARAMETERS_CASES["DEADLINE_ONLY_JSON"][1])

        # This is the function under test
        with pytest.raises(exceptions.DeadlineOperationError):
            api.create_job_from_job_bundle(
                job_bundle_dir=temp_job_bundle_dir,
                queue_parameter_definitions=[],
            )


def test_create_job_from_job_bundle_error_duplicate_parameters(
    fresh_deadline_config, temp_job_bundle_dir
):
    """
    Test a job bundle with an incorrect AWS Deadline Cloud parameter
    """
    with patch.object(api._session, "get_boto3_session") as session_mock, patch.object(
        _submit_job_bundle.api, "get_deadline_cloud_library_telemetry_client"
    ):
        session_mock().client("deadline").create_job.side_effect = [MOCK_CREATE_JOB_RESPONSE]

        config.set_setting("defaults.farm_id", MOCK_FARM_ID)
        config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

        # Write a JSON template
        with open(os.path.join(temp_job_bundle_dir, "template.json"), "w", encoding="utf8") as f:
            f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1])

        # Write the parameters file with a nonexistent AWS Deadline Cloud parameter
        with open(
            os.path.join(temp_job_bundle_dir, "parameter_values.json"), "w", encoding="utf8"
        ) as f:
            f.write(MOCK_PARAMETERS_JSON_NONEXISTENT_DEADLINE_PARAMETER)

        # This is the function under test
        with pytest.raises(exceptions.DeadlineOperationError):
            api.create_job_from_job_bundle(
                job_bundle_dir=temp_job_bundle_dir,
                queue_parameter_definitions=[],
            )


def _write_asset_files(assets_dir: str, asset_contents: Dict[str, str]):
    """
    Write a set of asset contents files to the provided assets directory.
    Each key of asset_contents is a relative path from assets_dir, and
    each value is what to write to the file.
    """
    for rel_path, contents in asset_contents.items():
        path = os.path.join(assets_dir, rel_path)
        if not os.path.isdir(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))
        if isinstance(contents, str):
            with open(path, "w", encoding="utf8") as f:
                f.write(contents)
        elif isinstance(contents, bytes):
            with open(path, "wb") as f:
                f.write(contents)
        else:
            raise ValueError("The contents provided in asset_contents must be either str or bytes.")


def test_create_job_from_job_bundle_job_attachments(
    fresh_deadline_config, temp_job_bundle_dir, temp_assets_dir
):
    """
    Test a job bundle with asset references.
    """
    # Use a temporary directory for the job bundle
    with patch.object(_submit_job_bundle.api, "get_boto3_session"), patch.object(
        _submit_job_bundle.api, "get_boto3_client"
    ) as client_mock, patch.object(
        _submit_job_bundle.api, "get_queue_user_boto3_session"
    ), patch.object(
        _submit_job_bundle, "_hash_attachments", return_value=(None, None)
    ) as mock_hash_attachments, patch.object(
        S3AssetManager,
        "prepare_paths_for_upload",
    ) as mock_prepare_paths, patch.object(
        S3AssetManager, "upload_assets"
    ) as mock_upload_assets, patch.object(
        _submit_job_bundle.api, "get_deadline_cloud_library_telemetry_client"
    ) as mock_telemetry, patch.object(
        api._telemetry, "get_deadline_endpoint_url", side_effect=["https://fake-endpoint-url"]
    ):
        client_mock().get_queue.side_effect = [MOCK_GET_QUEUE_RESPONSE]
        client_mock().create_job.side_effect = [MOCK_CREATE_JOB_RESPONSE]
        client_mock().get_job.side_effect = [MOCK_GET_JOB_RESPONSE]
        client_mock().get_storage_profile_for_queue.side_effect = [
            MOCK_GET_STORAGE_PROFILE_FOR_QUEUE_RESPONSE
        ]
        expected_upload_group = AssetUploadGroup(
            total_input_files=3, total_input_bytes=256, asset_groups=[AssetRootGroup()]
        )
        mock_prepare_paths.return_value = expected_upload_group
        mock_upload_assets.return_value = [
            SummaryStatistics(),
            Attachments([]),
        ]

        config.set_setting("defaults.farm_id", MOCK_FARM_ID)
        config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
        config.set_setting("settings.storage_profile_id", MOCK_STORAGE_PROFILE_ID)

        # Write a JSON template
        with open(os.path.join(temp_job_bundle_dir, "template.json"), "w", encoding="utf8") as f:
            f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1])

        # Create some files in the assets dir
        asset_contents = {
            "asset-1.txt": "This is asset 1",
            "somedir/asset-2.txt": "Asset 2",
            "somedir/asset-3.bat": "@echo asset 3",
        }
        _write_asset_files(temp_assets_dir, asset_contents)

        # Write the asset_references file
        asset_references = {
            "inputs": {
                "filenames": [os.path.join(temp_assets_dir, "asset-1.txt")],
                "directories": [os.path.join(temp_assets_dir, "somedir")],
            },
            "outputs": {"directories": [os.path.join(temp_assets_dir, "somedir")]},
        }
        with open(
            os.path.join(temp_job_bundle_dir, "asset_references.json"), "w", encoding="utf8"
        ) as f:
            json.dump({"assetReferences": asset_references}, f)

        def fake_hashing_callback(metadata: ProgressReportMetadata) -> bool:
            return True

        def fake_upload_callback(metadata: ProgressReportMetadata) -> bool:
            return True

        def fake_print_callback(msg: str) -> None:
            pass

        # This is the function we're testing
        api.create_job_from_job_bundle(
            temp_job_bundle_dir,
            print_function_callback=fake_print_callback,
            hashing_progress_callback=fake_hashing_callback,
            upload_progress_callback=fake_upload_callback,
            queue_parameter_definitions=[],
        )

        mock_prepare_paths.assert_called_once_with(
            input_paths=sorted(
                [
                    os.path.join(temp_assets_dir, "asset-1.txt"),
                    os.path.join(temp_assets_dir, os.path.normpath("somedir/asset-2.txt")),
                    os.path.join(temp_assets_dir, os.path.normpath("somedir/asset-3.bat")),
                ]
            ),
            output_paths=[os.path.join(temp_assets_dir, "somedir")],
            referenced_paths=[],
            storage_profile=MOCK_STORAGE_PROFILE,
            require_paths_exist=False,
        )
        mock_hash_attachments.assert_called_once_with(
            asset_manager=ANY,
            asset_groups=[AssetRootGroup()],
            total_input_files=3,
            total_input_bytes=256,
            print_function_callback=fake_print_callback,
            hashing_progress_callback=fake_hashing_callback,
        )
        client_mock().create_job.assert_called_once_with(
            farmId=MOCK_FARM_ID,
            queueId=MOCK_QUEUE_ID,
            template=ANY,
            templateType=ANY,
            priority=50,
            storageProfileId=MOCK_STORAGE_PROFILE_ID,
            attachments={
                "manifests": [],
                "fileSystem": JobAttachmentsFileSystem.COPIED,
            },
        )
        assert mock_telemetry.call_count == 3


def test_create_job_from_job_bundle_empty_job_attachments(
    fresh_deadline_config, temp_job_bundle_dir, temp_assets_dir
):
    """
    Test that when we have asset references that do not fall under Job Attachments
    (for example, if under a SHARED Storage Profile Filesystem Location), no Job
    Attachments calls are made.
    """
    # Use a temporary directory for the job bundle
    with patch.object(_submit_job_bundle.api, "get_boto3_session"), patch.object(
        _submit_job_bundle.api, "get_boto3_client"
    ) as client_mock, patch.object(
        _submit_job_bundle.api, "get_queue_user_boto3_session"
    ), patch.object(
        _submit_job_bundle, "_hash_attachments", return_value=(None, None)
    ) as mock_hash_attachments, patch.object(
        S3AssetManager,
        "prepare_paths_for_upload",
    ) as mock_prepare_paths, patch.object(
        S3AssetManager, "upload_assets"
    ) as mock_upload_assets, patch.object(
        _submit_job_bundle.api, "get_deadline_cloud_library_telemetry_client"
    ), patch.object(
        api._telemetry, "get_deadline_endpoint_url", side_effect=["https://fake-endpoint-url"]
    ):
        client_mock().get_queue.side_effect = [MOCK_GET_QUEUE_RESPONSE]
        client_mock().create_job.side_effect = [MOCK_CREATE_JOB_RESPONSE]
        client_mock().get_job.side_effect = [MOCK_GET_JOB_RESPONSE]
        client_mock().get_storage_profile_for_queue.side_effect = [
            MOCK_GET_STORAGE_PROFILE_FOR_QUEUE_RESPONSE
        ]

        # When this function returns an empty object, we skip Job Attachments calls
        expected_upload_group = AssetUploadGroup()
        mock_prepare_paths.return_value = expected_upload_group

        config.set_setting("defaults.farm_id", MOCK_FARM_ID)
        config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
        config.set_setting("settings.storage_profile_id", MOCK_STORAGE_PROFILE_ID)

        # Write a JSON template
        with open(os.path.join(temp_job_bundle_dir, "template.json"), "w", encoding="utf8") as f:
            f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1])

        # Create some files in the assets dir
        asset_contents = {
            "asset-1.txt": "This is asset 1",
            "somedir/asset-2.txt": "Asset 2",
            "somedir/asset-3.bat": "@echo asset 3",
        }
        _write_asset_files(temp_assets_dir, asset_contents)

        # Write the asset_references file
        asset_references = {
            "inputs": {
                "filenames": [os.path.join(temp_assets_dir, "asset-1.txt")],
                "directories": [os.path.join(temp_assets_dir, "somedir")],
            },
            "outputs": {"directories": [os.path.join(temp_assets_dir, "somedir")]},
        }
        with open(
            os.path.join(temp_job_bundle_dir, "asset_references.json"), "w", encoding="utf8"
        ) as f:
            json.dump({"assetReferences": asset_references}, f)

        def fake_hashing_callback(metadata: ProgressReportMetadata) -> bool:
            return True

        def fake_upload_callback(metadata: ProgressReportMetadata) -> bool:
            return True

        def fake_print_callback(msg: str) -> None:
            pass

        # This is the function we're testing
        api.create_job_from_job_bundle(
            temp_job_bundle_dir,
            print_function_callback=fake_print_callback,
            hashing_progress_callback=fake_hashing_callback,
            upload_progress_callback=fake_upload_callback,
            queue_parameter_definitions=[],
        )

        mock_prepare_paths.assert_called_once_with(
            input_paths=sorted(
                [
                    os.path.join(temp_assets_dir, "asset-1.txt"),
                    os.path.join(temp_assets_dir, os.path.normpath("somedir/asset-2.txt")),
                    os.path.join(temp_assets_dir, os.path.normpath("somedir/asset-3.bat")),
                ]
            ),
            output_paths=[os.path.join(temp_assets_dir, "somedir")],
            referenced_paths=[],
            storage_profile=MOCK_STORAGE_PROFILE,
            require_paths_exist=False,
        )
        mock_hash_attachments.assert_not_called()
        mock_upload_assets.assert_not_called()
        # Should not be called with Job Attachments
        client_mock().create_job.assert_called_once_with(
            farmId=MOCK_FARM_ID,
            queueId=MOCK_QUEUE_ID,
            template=ANY,
            templateType=ANY,
            priority=50,
            storageProfileId=MOCK_STORAGE_PROFILE_ID,
        )


def test_create_job_from_job_bundle_with_empty_asset_references(
    fresh_deadline_config, temp_job_bundle_dir
):
    """
    Test a job bundle with an asset_references file but no referenced files.
    """
    job_template_type, job_template = MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"]
    with patch.object(api._session, "get_boto3_session") as session_mock:
        session_mock().client("deadline").create_job.side_effect = [MOCK_CREATE_JOB_RESPONSE]
        session_mock().client("deadline").get_job.side_effect = [MOCK_GET_JOB_RESPONSE]
        session_mock().client("deadline").get_storage_profile_for_queue.side_effect = [
            MOCK_GET_STORAGE_PROFILE_FOR_QUEUE_RESPONSE
        ]

        config.set_setting("defaults.farm_id", MOCK_FARM_ID)
        config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
        config.set_setting("settings.storage_profile_id", MOCK_STORAGE_PROFILE_ID)

        # Write the template to the job bundle
        with open(
            os.path.join(temp_job_bundle_dir, f"template.{job_template_type.lower()}"),
            "w",
            encoding="utf8",
        ) as f:
            f.write(job_template)

        # Write an asset_references.json file with empty lists
        asset_references: dict = {
            "inputs": {"filenames": [], "directories": []},
            "outputs": {"directories": []},
        }
        with open(
            os.path.join(temp_job_bundle_dir, "asset_references.json"), "w", encoding="utf8"
        ) as f:
            json.dump({"assetReferences": asset_references}, f)

        # This is the function under test
        response = api.create_job_from_job_bundle(
            job_bundle_dir=temp_job_bundle_dir,
            queue_parameter_definitions=[],
        )

        assert response == MOCK_JOB_ID
        # There should be no job attachments section in the result
        session_mock().client("deadline").create_job.assert_called_once_with(
            farmId=MOCK_FARM_ID,
            queueId=MOCK_QUEUE_ID,
            template=job_template,
            templateType=job_template_type,
            storageProfileId=MOCK_STORAGE_PROFILE_ID,
            priority=50,
        )


def test_create_job_from_job_bundle_partially_empty_directories(
    fresh_deadline_config, temp_job_bundle_dir
):
    """
    Test a job bundle with an input directory that contains both empty directories and input files
    does not throw a MisconfiguredInputsError and successfully submits
    """
    job_template_type, job_template = MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"]
    temp_bundle_dir_as_path = Path(temp_job_bundle_dir)
    assets_directory: str = str(temp_bundle_dir_as_path / "assets")
    empty_directory = str(temp_bundle_dir_as_path / "assets" / "empty_dir")
    Path(empty_directory).mkdir(parents=True)
    (temp_bundle_dir_as_path / "assets" / "input_file").touch()

    with patch.object(_submit_job_bundle.api, "get_boto3_client") as client_mock, patch.object(
        _submit_job_bundle.api, "get_queue_user_boto3_session"
    ):
        client_mock().create_job.side_effect = [MOCK_CREATE_JOB_RESPONSE]
        client_mock().get_queue.side_effect = [MOCK_GET_QUEUE_RESPONSE]
        config.set_setting("defaults.farm_id", MOCK_FARM_ID)
        config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

        # Write the template to the job bundle
        with open(
            os.path.join(temp_job_bundle_dir, f"template.{job_template_type.lower()}"),
            "w",
            encoding="utf8",
        ) as f:
            f.write(job_template)

        # Write an asset_references.json
        asset_references: dict = {
            "inputs": {"filenames": [], "directories": [assets_directory]},
            "outputs": {"directories": []},
        }
        with open(
            os.path.join(temp_job_bundle_dir, "asset_references.json"), "w", encoding="utf8"
        ) as f:
            json.dump({"assetReferences": asset_references}, f)

        # WHEN
        response = api.create_job_from_job_bundle(
            job_bundle_dir=temp_job_bundle_dir,
            queue_parameter_definitions=[],
        )

        # THEN
        # create_job_from_job_bundle did NOT throw MisconfiguredInputsError
        assert response == MOCK_JOB_ID


def test_create_job_from_job_bundle_misconfigured_directories(
    fresh_deadline_config, temp_job_bundle_dir, caplog
):
    """
    Test that a submitting a job with the `require_paths_exist` flag set to true
    with a job bundle with input directories that do not exist throws an error.
    Also confirms that empty directories as logged and added to referenced paths.
    """
    job_template_type, job_template = MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"]
    temp_bundle_dir_as_path = Path(temp_job_bundle_dir)
    missing_directory = str(temp_bundle_dir_as_path / "does" / "not" / "exist" / "bad_path")
    empty_directory = str(temp_bundle_dir_as_path / "empty_dir")
    Path(empty_directory).mkdir()

    with patch.object(_submit_job_bundle.api, "get_boto3_session"), patch.object(
        _submit_job_bundle.api, "get_boto3_client"
    ) as client_mock, patch.object(_submit_job_bundle.api, "get_queue_user_boto3_session"):
        caplog.set_level(INFO)
        client_mock().get_queue.side_effect = [MOCK_GET_QUEUE_RESPONSE]
        config.set_setting("defaults.farm_id", MOCK_FARM_ID)
        config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

        # Write the template to the job bundle
        with open(
            os.path.join(temp_job_bundle_dir, f"template.{job_template_type.lower()}"),
            "w",
            encoding="utf8",
        ) as f:
            f.write(job_template)

        # Write an asset_references.json
        asset_references: dict = {
            "inputs": {"filenames": [], "directories": [missing_directory, empty_directory]},
            "outputs": {"directories": []},
        }
        with open(
            os.path.join(temp_job_bundle_dir, "asset_references.json"), "w", encoding="utf8"
        ) as f:
            json.dump({"assetReferences": asset_references}, f)

        # WHEN / THEN
        with pytest.raises(MisconfiguredInputsError) as execinfo:
            api.create_job_from_job_bundle(
                job_bundle_dir=temp_job_bundle_dir,
                queue_parameter_definitions=[],
                require_paths_exist=True,
            )

        assert "bad_path" in str(execinfo)
        assert "empty_dir" not in str(execinfo)
        assert "empty_dir' is empty. Adding to referenced paths." in caplog.text


def test_create_job_from_job_bundle_misconfigured_input_files(
    fresh_deadline_config, temp_job_bundle_dir, caplog
):
    """
    Test that a submitting a job without the `require_paths_exist` flag set,
    with a job bundle with input directories that do not exist does not include those
    directories in the warning message, but DOES incldue misconfigured directories that
    were specified as files, which should result in an error.
    """
    job_template_type, job_template = MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"]
    temp_bundle_dir_as_path = Path(temp_job_bundle_dir)
    missing_file = str(temp_bundle_dir_as_path / "does" / "not" / "exist.png")
    directory_pretending_to_be_file = str(temp_bundle_dir_as_path / "sneaky_bad_not_file")
    Path(directory_pretending_to_be_file).mkdir()

    with patch.object(api._session, "get_boto3_session"), patch.object(
        _submit_job_bundle.api, "get_boto3_client"
    ) as client_mock, patch.object(_submit_job_bundle.api, "get_queue_user_boto3_session"):
        caplog.set_level(INFO)
        client_mock().get_queue.side_effect = [MOCK_GET_QUEUE_RESPONSE]
        config.set_setting("defaults.farm_id", MOCK_FARM_ID)
        config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

        # Write the template to the job bundle
        with open(
            os.path.join(temp_job_bundle_dir, f"template.{job_template_type.lower()}"),
            "w",
            encoding="utf8",
        ) as f:
            f.write(job_template)

        # Write an asset_references.json
        asset_references: dict = {
            "inputs": {
                "filenames": [missing_file, directory_pretending_to_be_file],
                "directories": [],
            },
            "outputs": {"directories": []},
        }
        with open(
            os.path.join(temp_job_bundle_dir, "asset_references.json"), "w", encoding="utf8"
        ) as f:
            json.dump({"assetReferences": asset_references}, f)

        # WHEN / THEN
        with pytest.raises(MisconfiguredInputsError) as execinfo:
            api.create_job_from_job_bundle(
                job_bundle_dir=temp_job_bundle_dir,
                queue_parameter_definitions=[],
            )

        assert "sneaky_bad_not_file" in str(execinfo)
        assert "exist.png" not in str(execinfo)
        assert "exist.png' does not exist. Adding to referenced paths." in caplog.text


def test_create_job_from_job_bundle_with_single_asset_file(
    fresh_deadline_config, temp_job_bundle_dir, temp_assets_dir
):
    """
    Test a job bundle with a single input file reference and no output directories.
    """

    # Use a temporary directory for the job bundle
    with patch.object(_submit_job_bundle.api, "get_boto3_session"), patch.object(
        _submit_job_bundle.api, "get_boto3_client"
    ) as client_mock, patch.object(
        _submit_job_bundle.api, "get_queue_user_boto3_session"
    ), patch.object(
        _submit_job_bundle, "_hash_attachments", return_value=(None, None)
    ) as mock_hash_attachments, patch.object(
        S3AssetManager,
        "prepare_paths_for_upload",
    ) as mock_prepare_paths, patch.object(
        S3AssetManager, "upload_assets"
    ) as mock_upload_assets, patch.object(
        _submit_job_bundle.api, "get_deadline_cloud_library_telemetry_client"
    ), patch.object(
        api._telemetry, "get_deadline_endpoint_url", side_effect=["https://fake-endpoint-url"]
    ):
        client_mock().create_job.side_effect = [MOCK_CREATE_JOB_RESPONSE]
        client_mock().get_queue.side_effect = [MOCK_GET_QUEUE_RESPONSE]
        client_mock().get_job.side_effect = [MOCK_GET_JOB_RESPONSE]
        client_mock().get_storage_profile_for_queue.side_effect = [
            MOCK_GET_STORAGE_PROFILE_FOR_QUEUE_RESPONSE
        ]
        expected_upload_group = AssetUploadGroup(
            total_input_files=1, total_input_bytes=1, asset_groups=[AssetRootGroup()]
        )
        mock_prepare_paths.return_value = expected_upload_group
        mock_upload_assets.return_value = [
            SummaryStatistics(),
            Attachments(
                [
                    ManifestProperties(
                        rootPath="/mnt/root/path1",
                        rootPathFormat=PathFormat.POSIX,
                        inputManifestPath="mock-manifest",
                        inputManifestHash="mock-manifest-hash",
                        outputRelativeDirectories=["."],
                    ),
                ],
            ),
        ]

        config.set_setting("defaults.farm_id", MOCK_FARM_ID)
        config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
        config.set_setting("settings.storage_profile_id", MOCK_STORAGE_PROFILE_ID)

        # Write a JSON template
        with open(os.path.join(temp_job_bundle_dir, "template.json"), "w", encoding="utf8") as f:
            f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1])

        # Create some files in the assets dir
        asset_contents = {
            "asset-1.txt": "This is asset 1",
        }
        _write_asset_files(temp_assets_dir, asset_contents)

        # Write the asset_references file
        asset_references = {
            "inputs": {
                "filenames": [os.path.join(temp_assets_dir, "asset-1.txt")],
            },
        }
        with open(
            os.path.join(temp_job_bundle_dir, "asset_references.json"), "w", encoding="utf8"
        ) as f:
            json.dump({"assetReferences": asset_references}, f)

        def fake_hashing_callback(metadata: ProgressReportMetadata) -> bool:
            return True

        def fake_upload_callback(metadata: ProgressReportMetadata) -> bool:
            return True

        def fake_print_callback(msg: str) -> None:
            pass

        # This is the function we're testing
        api.create_job_from_job_bundle(
            temp_job_bundle_dir,
            print_function_callback=fake_print_callback,
            hashing_progress_callback=fake_hashing_callback,
            upload_progress_callback=fake_upload_callback,
            queue_parameter_definitions=[],
        )

        mock_prepare_paths.assert_called_once_with(
            input_paths=[os.path.join(temp_assets_dir, "asset-1.txt")],
            output_paths=[],
            referenced_paths=[],
            storage_profile=MOCK_STORAGE_PROFILE,
            require_paths_exist=False,
        )
        mock_hash_attachments.assert_called_once_with(
            asset_manager=ANY,
            asset_groups=[AssetRootGroup()],
            total_input_files=1,
            total_input_bytes=1,
            print_function_callback=fake_print_callback,
            hashing_progress_callback=fake_hashing_callback,
        )

        client_mock().create_job.assert_called_once_with(
            farmId=MOCK_FARM_ID,
            queueId=MOCK_QUEUE_ID,
            template=ANY,
            templateType=ANY,
            priority=50,
            storageProfileId=MOCK_STORAGE_PROFILE_ID,
            attachments={
                "manifests": [
                    {
                        "rootPath": "/mnt/root/path1",
                        "rootPathFormat": PathFormat.POSIX,
                        "inputManifestPath": "mock-manifest",
                        "inputManifestHash": "mock-manifest-hash",
                        "outputRelativeDirectories": ["."],
                    },
                ],
                "fileSystem": JobAttachmentsFileSystem.COPIED,
            },
        )


get_job_responses = [
    pytest.param(
        [
            "CREATE_IN_PROGRESS",
            "CREATE_COMPLETE",
        ],
        True,
        id="CreateSucceeded",
    ),
    pytest.param(
        [
            "CREATE_IN_PROGRESS",
            "CREATE_IN_PROGRESS",
            "POSSIBLE_FUTURE_STATUS",
        ],
        True,
        id="CreateSucceededUnknownStatus",
    ),
    pytest.param(
        [
            "CREATE_IN_PROGRESS",
            "CREATE_FAILED",
        ],
        False,
        id="CreateFailed",
    ),
]


@pytest.mark.parametrize("responses, final_status", get_job_responses)
def test_wait_for_create_job_to_complete(responses, final_status):
    """
    Test the waiter for calling CreateJob.
    """

    def mock_continue_callback() -> bool:
        return True

    deadline_client = Mock()

    deadline_client.get_job.side_effect = [
        {
            "lifecycleStatus": response,
            "lifecycleStatusMessage": MOCK_STATUS_MESSAGE,
        }
        for response in responses
    ]

    with patch.object(time, "sleep"):
        success, status_message = api.wait_for_create_job_to_complete(
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            job_id=MOCK_JOB_ID,
            deadline_client=deadline_client,
            continue_callback=mock_continue_callback,
        )
    assert success == final_status
    assert status_message == MOCK_STATUS_MESSAGE


def test_wait_for_create_job_to_complete_timeout():
    """
    Test the waiter for calling CreateJob when it times out.
    """

    def mock_continue_callback() -> bool:
        return True

    deadline_client = Mock()
    deadline_client.get_job.return_value = {
        "state": "CREATE_IN_PROGRESS",
        "lifecycleStatusMessage": MOCK_STATUS_MESSAGE,
    }

    with pytest.raises(TimeoutError), patch.object(time, "sleep"):
        api.wait_for_create_job_to_complete(
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            job_id=MOCK_JOB_ID,
            deadline_client=deadline_client,
            continue_callback=mock_continue_callback,
        )
