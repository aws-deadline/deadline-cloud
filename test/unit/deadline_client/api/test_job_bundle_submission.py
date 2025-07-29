# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests the deadline.client.api functions for submitting Open Job Description job bundles.
"""

import json
import os
from logging import INFO
from pathlib import Path
from typing import Any, Dict, Tuple
from unittest.mock import ANY, patch, Mock, call
from deadline.client import exceptions

import pytest
import time

from deadline.client import api, config
from deadline.job_attachments.exceptions import MisconfiguredInputsError
from deadline.job_attachments.models import (
    AssetRootGroup,
    AssetUploadGroup,
    FileSystemLocation,
    FileSystemLocationType,
    JobAttachmentsFileSystem,
    PathFormat,
    StorageProfile,
    StorageProfileOperatingSystemFamily,
)
from deadline.job_attachments.upload import S3AssetManager
from deadline.job_attachments.progress_tracker import ProgressReportMetadata

from ..testing_utilities import patch_calls_for_create_job_from_job_bundle, write_test_asset_files
from ..shared_constants import (
    MOCK_BUCKET_NAME,
    MOCK_FARM_ID,
    MOCK_STORAGE_PROFILE_ID,
    MOCK_QUEUE_ID,
    MOCK_JOB_ID,
    MOCK_STATUS_MESSAGE,
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
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("settings.storage_profile_id", MOCK_STORAGE_PROFILE_ID)

    job_template_type, job_template = MOCK_JOB_TEMPLATE_CASES[job_template_case]
    parameters_type, parameters, expected_create_job_parameters = MOCK_PARAMETERS_CASES[
        parameters_case
    ]
    with patch_calls_for_create_job_from_job_bundle() as mock:
        mock.get_boto3_client().get_storage_profile_for_queue.return_value = (
            MOCK_GET_STORAGE_PROFILE_FOR_QUEUE_RESPONSE
        )

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
    mock.get_boto3_client().create_job.assert_called_once_with(
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
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

    with patch_calls_for_create_job_from_job_bundle():
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
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

    with patch_calls_for_create_job_from_job_bundle():
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
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

    with patch_calls_for_create_job_from_job_bundle():
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


def test_create_job_from_job_bundle_job_attachments(
    fresh_deadline_config, temp_job_bundle_dir, temp_assets_dir
):
    """
    Test a job bundle with asset references.
    """
    with patch_calls_for_create_job_from_job_bundle() as mock:
        mock.get_boto3_client().get_storage_profile_for_queue.return_value = (
            MOCK_GET_STORAGE_PROFILE_FOR_QUEUE_RESPONSE
        )

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
        write_test_asset_files(temp_assets_dir, asset_contents)

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

        # This is the function we're testing
        api.create_job_from_job_bundle(
            temp_job_bundle_dir,
            print_function_callback=print,
            hashing_progress_callback=fake_hashing_callback,
            upload_progress_callback=fake_upload_callback,
            queue_parameter_definitions=[],
            known_asset_paths=[temp_assets_dir],
        )

        mock.hash_attachments.assert_called_once_with(
            asset_manager=ANY,
            asset_groups=[
                AssetRootGroup(
                    root_path=temp_assets_dir,
                    inputs={Path(temp_assets_dir) / p for p in asset_contents.keys()},
                    outputs={Path(temp_assets_dir) / "somedir"},
                )
            ],
            total_input_files=3,
            total_input_bytes=35,
            print_function_callback=print,
            hashing_progress_callback=fake_hashing_callback,
        )
        mock.get_boto3_client().create_job.assert_called_once_with(
            farmId=MOCK_FARM_ID,
            queueId=MOCK_QUEUE_ID,
            template=ANY,
            templateType="JSON",
            priority=50,
            storageProfileId=MOCK_STORAGE_PROFILE_ID,
            attachments=ANY,
        )
        mock_telemetry_client = mock.get_deadline_cloud_library_telemetry_client()
        assert mock_telemetry_client.record_hashing_summary.call_count == 1
        assert mock_telemetry_client.record_upload_summary.call_count == 1
        assert mock_telemetry_client.record_event.mock_calls == [
            call(
                event_type="com.amazon.rum.deadline.submission",
                event_details={"submitter_name": "Custom"},
                from_gui=False,
            ),
            call(
                event_type="com.amazon.rum.deadline.create_job",
                event_details={"is_success": True},
                from_gui=False,
            ),
        ]


def test_create_job_from_job_bundle_empty_job_attachments(
    fresh_deadline_config, temp_job_bundle_dir, temp_assets_dir
):
    """
    Test that when we have asset references that do not fall under Job Attachments
    (for example, if under a SHARED Storage Profile Filesystem Location), no Job
    Attachments calls are made.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("settings.storage_profile_id", MOCK_STORAGE_PROFILE_ID)

    with patch_calls_for_create_job_from_job_bundle() as mock, patch.object(
        S3AssetManager,
        "prepare_paths_for_upload",
    ) as mock_prepare_paths:
        mock.get_boto3_client().get_storage_profile_for_queue.return_value = (
            MOCK_GET_STORAGE_PROFILE_FOR_QUEUE_RESPONSE
        )
        # When this function returns an empty object, we skip Job Attachments calls
        expected_upload_group = AssetUploadGroup()
        mock_prepare_paths.return_value = expected_upload_group

        # Write a JSON template
        with open(os.path.join(temp_job_bundle_dir, "template.json"), "w", encoding="utf8") as f:
            f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1])

        # Create some files in the assets dir
        asset_contents = {
            "asset-1.txt": "This is asset 1",
            "somedir/asset-2.txt": "Asset 2",
            "somedir/asset-3.bat": "@echo asset 3",
        }
        write_test_asset_files(temp_assets_dir, asset_contents)

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

        # This is the function we're testing
        api.create_job_from_job_bundle(
            temp_job_bundle_dir,
            print_function_callback=print,
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
        mock.hash_attachments.assert_not_called()
        mock.upload_assets.assert_not_called()
        # Should not be called with Job Attachments
        mock.get_boto3_client().create_job.assert_called_once_with(
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
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("settings.storage_profile_id", MOCK_STORAGE_PROFILE_ID)

    job_template_type, job_template = MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"]
    with patch_calls_for_create_job_from_job_bundle() as mock:
        mock.get_boto3_client().get_storage_profile_for_queue.return_value = (
            MOCK_GET_STORAGE_PROFILE_FOR_QUEUE_RESPONSE
        )

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
        mock.get_boto3_client().create_job.assert_called_once_with(
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
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

    job_template_type, job_template = MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"]
    temp_bundle_dir_as_path = Path(temp_job_bundle_dir)
    assets_directory: str = str(temp_bundle_dir_as_path / "assets")
    empty_directory = str(temp_bundle_dir_as_path / "assets" / "empty_dir")
    Path(empty_directory).mkdir(parents=True)
    (temp_bundle_dir_as_path / "assets" / "input_file").touch()

    with patch_calls_for_create_job_from_job_bundle():
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
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

    job_template_type, job_template = MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"]
    temp_bundle_dir_as_path = Path(temp_job_bundle_dir)
    missing_directory = str(temp_bundle_dir_as_path / "does" / "not" / "exist" / "bad_path")
    empty_directory = str(temp_bundle_dir_as_path / "empty_dir")
    Path(empty_directory).mkdir()

    with patch_calls_for_create_job_from_job_bundle():
        caplog.set_level(INFO)

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
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

    job_template_type, job_template = MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"]
    temp_bundle_dir_as_path = Path(temp_job_bundle_dir)
    missing_file = str(temp_bundle_dir_as_path / "does" / "not" / "exist.png")
    directory_pretending_to_be_file = str(temp_bundle_dir_as_path / "sneaky_bad_not_file")
    Path(directory_pretending_to_be_file).mkdir()

    with patch_calls_for_create_job_from_job_bundle():
        caplog.set_level(INFO)

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
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("settings.storage_profile_id", MOCK_STORAGE_PROFILE_ID)

    # Use a temporary directory for the job bundle
    with patch_calls_for_create_job_from_job_bundle() as mock:
        mock.get_boto3_client().get_storage_profile_for_queue.side_effect = [
            MOCK_GET_STORAGE_PROFILE_FOR_QUEUE_RESPONSE
        ]

        # Write a JSON template
        with open(os.path.join(temp_job_bundle_dir, "template.json"), "w", encoding="utf8") as f:
            f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1])

        # Create some files in the assets dir
        asset_contents = {
            "asset-1.txt": "This is asset 1",
        }
        write_test_asset_files(temp_assets_dir, asset_contents)

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

        # This is the function we're testing
        api.create_job_from_job_bundle(
            temp_job_bundle_dir,
            print_function_callback=print,
            hashing_progress_callback=fake_hashing_callback,
            upload_progress_callback=fake_upload_callback,
            queue_parameter_definitions=[],
            known_asset_paths=[temp_assets_dir],
        )

        mock.hash_attachments.assert_called_once_with(
            asset_manager=ANY,
            asset_groups=[
                AssetRootGroup(
                    root_path=temp_assets_dir, inputs={Path(temp_assets_dir) / "asset-1.txt"}
                )
            ],
            total_input_files=1,
            total_input_bytes=15,
            print_function_callback=print,
            hashing_progress_callback=fake_hashing_callback,
        )

        assert mock.get_boto3_client().create_job.mock_calls == [
            call(
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
        ]


def test_create_job_from_job_bundle_with_target_task_run_status(
    fresh_deadline_config,
    temp_job_bundle_dir,
):
    """
    Test that create_job_from_job_bundle passes the target_task_run_status parameter to create_job.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

    # Create a minimal template file like other tests do
    with open(os.path.join(temp_job_bundle_dir, "template.json"), "w", encoding="utf8") as f:
        f.write('{"specificationVersion": "jobtemplate-2023-09", "name": "TestJob", "steps": []}')

    with patch_calls_for_create_job_from_job_bundle() as mock:
        api.create_job_from_job_bundle(
            temp_job_bundle_dir,
            target_task_run_status="SUSPENDED",
            queue_parameter_definitions=[],
        )

        # Verify that targetTaskRunStatus was passed to create_job
        create_job_call = mock.get_boto3_client().create_job.call_args
        assert create_job_call is not None
        assert create_job_call.kwargs["targetTaskRunStatus"] == "SUSPENDED"


def test_create_job_from_job_bundle_without_target_task_run_status(
    fresh_deadline_config,
    temp_job_bundle_dir,
):
    """
    Test that create_job_from_job_bundle does not pass targetTaskRunStatus when not specified.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

    # Create a minimal template file like other tests do
    with open(os.path.join(temp_job_bundle_dir, "template.json"), "w", encoding="utf8") as f:
        f.write('{"specificationVersion": "jobtemplate-2023-09", "name": "TestJob", "steps": []}')

    with patch_calls_for_create_job_from_job_bundle() as mock:
        api.create_job_from_job_bundle(
            temp_job_bundle_dir,
            queue_parameter_definitions=[],
        )

        # Verify that targetTaskRunStatus was not passed to create_job
        create_job_call = mock.get_boto3_client().create_job.call_args
        assert create_job_call is not None
        assert "targetTaskRunStatus" not in create_job_call.kwargs


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
