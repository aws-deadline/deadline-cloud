# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests the functionality for loading data from a job bundle. For example,
to load all the job parameter metadata including transformation of
relative default paths into absolute paths rooted in the job bundle.
"""

import json
import ntpath
import os
import sys
from contextlib import contextmanager
from unittest.mock import patch

import pytest
import yaml

from deadline.client.exceptions import DeadlineOperationError
from deadline.client.job_bundle import loader
from deadline.client.job_bundle.loader import (
    parse_yaml_or_json_content,
    read_yaml_or_json,
    read_yaml_or_json_object,
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
    with pytest.raises(
        DeadlineOperationError, match="resolves outside of the resolved bundle directory"
    ):
        validate_directory_symlink_containment(str(test_root))
    os.unlink(symlink_dir)

    os.symlink(target_file, test_root.join("symlink_file.txt"))
    with pytest.raises(
        DeadlineOperationError, match="resolves outside of the resolved bundle directory"
    ):
        validate_directory_symlink_containment(str(test_root))


class TestSymlinkContainmentWindowsPaths:
    """
    Windows path semantics for validate_directory_symlink_containment, exercised through
    a simulated ntpath filesystem so the cases run on every platform.

    os.path.commonpath raises ValueError for a bundle located at a UNC share root
    ('\\\\host\\share' vs '\\\\host\\share\\template.yaml' -> "Can't mix absolute and
    relative paths"), and for a symlink escaping a drive-letter bundle onto a UNC share
    ('C:\\bundle' vs '\\\\host\\share\\x' -> "Paths don't have the same drive"). Neither
    exception is caught, so both would surface as a raw ValueError rather than a
    containment verdict.
    """

    @contextmanager
    def _simulated_windows_bundle(self, bundle_dir, entries, resolves_to):
        """Simulate an ntpath filesystem holding ``entries`` under ``bundle_dir``.

        ``resolves_to`` maps a normalized path to the location it resolves to, standing
        in for a symlink target.
        """

        class _WindowsPath:
            def __getattr__(self, name):
                return getattr(ntpath, name)

            @staticmethod
            def isdir(path):
                return path == bundle_dir

            @staticmethod
            def realpath(path):
                return resolves_to.get(ntpath.normpath(path), ntpath.normpath(path))

        def walk(top):
            yield top, [], list(entries)

        with patch.object(loader.os, "walk", walk), patch.object(loader.os, "path", _WindowsPath()):
            yield

    def test_bundle_at_unc_share_root_is_valid(self):
        """A bundle directory that is itself a UNC share root contains its own files."""
        bundle_dir = r"\\host\share"
        with self._simulated_windows_bundle(bundle_dir, ["template.yaml"], {}):
            validate_directory_symlink_containment(bundle_dir)

    def test_bundle_under_unc_share_is_valid(self):
        bundle_dir = r"\\host\share\bundle"
        with self._simulated_windows_bundle(bundle_dir, ["template.yaml"], {}):
            validate_directory_symlink_containment(bundle_dir)

    def test_symlink_escaping_unc_share_root_is_rejected(self):
        bundle_dir = r"\\host\share"
        with self._simulated_windows_bundle(
            bundle_dir,
            ["escape.yaml"],
            {r"\\host\share\escape.yaml": r"\\host\other\secret.yaml"},
        ):
            with pytest.raises(
                DeadlineOperationError, match="resolves outside of the resolved bundle directory"
            ):
                validate_directory_symlink_containment(bundle_dir)

    def test_symlink_from_drive_bundle_onto_unc_share_is_rejected(self):
        bundle_dir = r"C:\bundle"
        with self._simulated_windows_bundle(
            bundle_dir,
            ["escape.yaml"],
            {r"C:\bundle\escape.yaml": r"\\host\share\secret.yaml"},
        ):
            with pytest.raises(
                DeadlineOperationError, match="resolves outside of the resolved bundle directory"
            ):
                validate_directory_symlink_containment(bundle_dir)

    def test_symlink_to_sibling_prefix_directory_is_rejected(self):
        bundle_dir = r"C:\bundle"
        with self._simulated_windows_bundle(
            bundle_dir,
            ["escape.yaml"],
            {r"C:\bundle\escape.yaml": r"C:\bundle-secret\secret.yaml"},
        ):
            with pytest.raises(
                DeadlineOperationError, match="resolves outside of the resolved bundle directory"
            ):
                validate_directory_symlink_containment(bundle_dir)


class TestHiddenParameterValidation:
    """Tests for hidden parameter validation in read_job_bundle_parameters."""

    TEMPLATE_HIDDEN_PARAM = """\
specificationVersion: jobtemplate-2023-09
name: HiddenTest
parameterDefinitions:
- name: HiddenParam
  type: {type}
  userInterface:
    control: HIDDEN
  description: A hidden parameter
  {default_line}
steps:
- name: step1
  script:
    actions:
      onRun:
        command: echo
        args:
          - test
"""

    @pytest.mark.parametrize(
        "param_type,default_line",
        [
            pytest.param("STRING", "default: ''", id="string-empty-default"),
            pytest.param("STRING", "default: some_value", id="string-nonempty-default"),
            pytest.param("INT", "default: 0", id="int-zero-default"),
        ],
    )
    def test_hidden_param_with_default_succeeds(
        self, param_type, default_line, fresh_deadline_config, temp_job_bundle_dir
    ):
        template = self.TEMPLATE_HIDDEN_PARAM.format(type=param_type, default_line=default_line)
        with open(os.path.join(temp_job_bundle_dir, "template.yaml"), "w") as f:
            f.write(template)

        result = read_job_bundle_parameters(temp_job_bundle_dir)
        assert any(p["name"] == "HiddenParam" for p in result)

    def test_hidden_param_with_value_in_parameter_values_succeeds(
        self, fresh_deadline_config, temp_job_bundle_dir
    ):
        template = self.TEMPLATE_HIDDEN_PARAM.format(type="STRING", default_line="")
        with open(os.path.join(temp_job_bundle_dir, "template.yaml"), "w") as f:
            f.write(template)
        with open(os.path.join(temp_job_bundle_dir, "parameter_values.yaml"), "w") as f:
            f.write("parameterValues:\n- name: HiddenParam\n  value: provided\n")

        result = read_job_bundle_parameters(temp_job_bundle_dir)
        assert any(p["name"] == "HiddenParam" and p["value"] == "provided" for p in result)

    def test_hidden_param_no_default_no_value_fails(
        self, fresh_deadline_config, temp_job_bundle_dir
    ):
        template = self.TEMPLATE_HIDDEN_PARAM.format(type="STRING", default_line="")
        with open(os.path.join(temp_job_bundle_dir, "template.yaml"), "w") as f:
            f.write(template)

        with pytest.raises(DeadlineOperationError, match="Hidden parameter.*missing a value"):
            read_job_bundle_parameters(temp_job_bundle_dir)


class TestReadYamlOrJson:
    def test_reads_yaml(self, tmp_path):
        (tmp_path / "template.yaml").write_text("name: test")
        contents, file_type = read_yaml_or_json(str(tmp_path), "template", True)
        assert contents == "name: test"
        assert file_type == "YAML"

    def test_reads_json(self, tmp_path):
        (tmp_path / "template.json").write_text('{"name": "test"}')
        contents, file_type = read_yaml_or_json(str(tmp_path), "template", True)
        assert contents == '{"name": "test"}'
        assert file_type == "JSON"

    def test_raises_when_both_exist(self, tmp_path):
        (tmp_path / "template.yaml").write_text("name: test")
        (tmp_path / "template.json").write_text('{"name": "test"}')
        with pytest.raises(DeadlineOperationError, match="both.*json and.*yaml"):
            read_yaml_or_json(str(tmp_path), "template", True)

    def test_raises_when_required_and_missing(self, tmp_path):
        with pytest.raises(DeadlineOperationError, match="lacks a"):
            read_yaml_or_json(str(tmp_path), "template", True)

    def test_returns_empty_when_not_required_and_missing(self, tmp_path):
        contents, file_type = read_yaml_or_json(str(tmp_path), "template", False)
        assert contents == ""
        assert file_type == ""


class TestReadYamlOrJsonObject:
    def test_reads_yaml(self, tmp_path):
        (tmp_path / "template.yaml").write_text("name: test")
        result = read_yaml_or_json_object(str(tmp_path), "template", True)
        assert result == {"name": "test"}

    def test_reads_json(self, tmp_path):
        (tmp_path / "template.json").write_text('{"name": "test"}')
        result = read_yaml_or_json_object(str(tmp_path), "template", True)
        assert result == {"name": "test"}

    def test_raises_when_required_and_missing(self, tmp_path):
        with pytest.raises(DeadlineOperationError):
            read_yaml_or_json_object(str(tmp_path), "template", True)

    def test_returns_none_when_not_required_and_missing(self, tmp_path):
        result = read_yaml_or_json_object(str(tmp_path), "template", False)
        assert result is None
