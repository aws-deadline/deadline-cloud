# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests the functionality for loading data from a job bundle. For example,
to load all the job parameter metadata including transformation of
relative default paths into absolute paths rooted in the job bundle.
"""

import json
import os
import sys

import pytest
import yaml

from deadline.client.exceptions import DeadlineOperationError
from deadline.client.job_bundle.loader import (
    parse_yaml_or_json_content,
    validate_directory_symlink_containment,
)
from deadline.client.job_bundle.parameters import read_job_bundle_parameters
from ...conftest import is_windows_non_admin

JOB_TEMPLATE_WITH_PARAMETERS_2023_09 = """
specificationVersion: 'jobtemplate-2023-09'
name: CLI Job
parameterDefinitions:
- name: LineEditControl
  type: STRING
  userInterface:
    control: LINE_EDIT
    label: Line Edit Control
  description: "Unrestricted line of text!"
  default: Default line edit value.
- name: IntSpinner
  type: INT
  description: A default integer spinner.
  default: 42
- name: StringDropdown
  type: STRING
  description: A dropdown with string values.
  default: WEDNESDAY
  allowedValues: [MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY]
- name: DirectoryPicker
  type: PATH
  description: Choose a directory.
- name: DirectoryPickDef1
  type: PATH
  objectType: DIRECTORY
  dataFlow: INOUT
  description: Choose a directory.
  default: ./internal/directory
- name: DirectoryPickDef2
  type: PATH
  objectType: DIRECTORY
  dataFlow: INOUT
  description: Choose a directory.
  default: ./internal/directory
steps:
- name: CliScript
  script:
    attachments:
      runScript:
        type: TEXT
        runnable: true
        data: |
            #!/usr/bin/env bash

            echo "Running the task"
            sleep 35
    actions:
      onRun:
        command: "{{Task.Attachment.runScript.Path}}"
"""

PARAMETER_VALUES = """
parameterValues:
- name: deadline:targetTaskRunStatus
  value: READY
- name: LineEditControl
  value: Testing one two three.
- name: DirectoryPicker
  value: "C:\\\\Users\\\\username\\\\mydir"
- name: DirectoryPickDef1
  value: "C:\\\\Users\\\\username\\\\value"
"""

READ_JOB_BUNDLE_PARAMETERS_RESULT = """
- name: LineEditControl
  type: STRING
  userInterface:
    control: LINE_EDIT
    label: Line Edit Control
  description: "Unrestricted line of text!"
  default: Default line edit value.
  value: Testing one two three.
- name: IntSpinner
  type: INT
  description: A default integer spinner.
  default: 42
- name: StringDropdown
  type: STRING
  description: A dropdown with string values.
  default: WEDNESDAY
  allowedValues: [MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY]
- name: DirectoryPicker
  type: PATH
  description: Choose a directory.
  value: "C:\\\\Users\\\\username\\\\mydir"
- name: DirectoryPickDef1
  type: PATH
  objectType: DIRECTORY
  dataFlow: INOUT
  description: Choose a directory.
  default: ./internal/directory
  value: "C:\\\\Users\\\\username\\\\value"
- name: DirectoryPickDef2
  type: PATH
  objectType: DIRECTORY
  dataFlow: INOUT
  description: Choose a directory.
  default: ./internal/directory
  value: {DIRECTORY_PICKER_2_VALUE}
- name: deadline:targetTaskRunStatus
  value: READY
"""


@pytest.mark.parametrize(
    "template_data,parameter_values,expected_result",
    [
        pytest.param(
            JOB_TEMPLATE_WITH_PARAMETERS_2023_09,
            PARAMETER_VALUES,
            READ_JOB_BUNDLE_PARAMETERS_RESULT,
            id="jobtemplate-2023-09",
        ),
    ],
)
def test_read_job_bundle_parameters(
    template_data,
    parameter_values,
    expected_result,
    fresh_deadline_config,
    temp_job_bundle_dir,
):
    """
    Tests that the read_job_bundle_parameters function loads the
    """
    # Write the template to the job bundle
    with open(
        os.path.join(temp_job_bundle_dir, "template.yaml"),
        "w",
        encoding="utf8",
    ) as f:
        f.write(template_data)

    # Write the parameter values to the job bundle
    with open(
        os.path.join(temp_job_bundle_dir, "parameter_values.yaml"),
        "w",
        encoding="utf8",
    ) as f:
        f.write(parameter_values)

    # Now load the parameters from this job bundle
    result = read_job_bundle_parameters(temp_job_bundle_dir)

    # In the test data, we set the directory picker 1 parameter value, but let
    # the directory picker 2 parameter value fall back to the default, which causes
    # it to expand into a path internal to the job bundle.
    directory_picker_2_value = json.dumps(
        os.path.normpath(os.path.join(temp_job_bundle_dir, "./internal/directory"))
    )
    assert result == yaml.safe_load(
        expected_result.format(DIRECTORY_PICKER_2_VALUE=directory_picker_2_value)
    )


@pytest.mark.parametrize(
    "content,type,expected_result",
    [('{"a": "b"}', "JSON", {"a": "b"}), ("a: b", "YAML", {"a": "b"})],
)
def test_parse_yaml_or_json_content_success(content, type, expected_result):
    """Test success cases of parsing YAML and JSON"""
    result = parse_yaml_or_json_content(content, type, "", "")
    assert result == expected_result


@pytest.mark.parametrize("content,type", [('{"a": "b" "c"}', "JSON"), ("a: b\n  c: d", "YAML")])
def test_parse_yaml_or_json_content_fail(content, type):
    """Test success cases of parsing YAML and JSON"""
    with pytest.raises(DeadlineOperationError):
        parse_yaml_or_json_content(content, type, "", "")


@pytest.mark.skipif(
    is_windows_non_admin(),
    reason="Windows requires Admin to create symlinks, skipping this test.",
)
def test_validate_directory_symlink_containment_success(tmpdir):
    """Test success cases for processing the job bundle from a given directory"""
    test_root = tmpdir.mkdir("root_dir")
    root_file = test_root.join("root_file.txt")
    root_file.write("test data")

    target_dir = test_root.mkdir("target_dir")
    target_file = target_dir.join("target_file.txt")
    target_file.write("this is the target")

    os.symlink(target_dir, test_root.join("symlink_dir"), target_is_directory=True)
    os.symlink(target_file, test_root.join("symlink_file.txt"))

    validate_directory_symlink_containment(str(test_root))


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Windows requires Admin to create symlinks, skipping this test.",
)
def test_validate_directory_symlink_containment_fail(tmpdir):
    """Test failure cases for processing the job bundle from a given directory"""
    test_root = tmpdir.mkdir("root_dir")
    root_file = test_root.join("root_file.txt")
    root_file.write("test data")

    target_dir = tmpdir.mkdir("target_dir")
    target_file = target_dir.join("target_file.txt")
    target_file.write("this is the target")

    symlink_dir = test_root.join("symlink_dir")
    os.symlink(target_dir, test_root.join("symlink_dir"), target_is_directory=True)
    with pytest.raises(DeadlineOperationError):
        validate_directory_symlink_containment(str(test_root))
    os.unlink(symlink_dir)

    os.symlink(target_file, test_root.join("symlink_file.txt"))
    with pytest.raises(DeadlineOperationError):
        validate_directory_symlink_containment(str(test_root))
