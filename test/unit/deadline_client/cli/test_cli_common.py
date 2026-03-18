# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import json
import pytest

import click
import yaml

from deadline.client.cli._common import _parse_file_parameter, _parse_multi_format_parameters


class TestParseFileParameter:
    """Test the _parse_file_parameter function."""

    @pytest.mark.parametrize(
        "filename,test_data,write_func",
        [
            pytest.param(
                "test.json",
                {"key1": "value1", "key2": {"nested": "value"}},
                lambda data: json.dumps(data),
                id="json_file",
            ),
            pytest.param(
                "test.yaml",
                {"key1": "value1", "key2": {"nested": "value"}},
                lambda data: yaml.safe_dump(data),
                id="yaml_file",
            ),
            pytest.param(
                "test.yml",
                {"key1": "value1", "list": [1, 2, 3]},
                lambda data: yaml.dump(data),
                id="yml_extension",
            ),
            pytest.param(
                "test.config",
                {"key1": "value1"},
                lambda data: yaml.dump(data),
                id="unknown_extension_as_yaml",
            ),
        ],
    )
    def test_parse_valid_files(self, tmp_path, filename, test_data, write_func):
        """Test parsing valid files with different formats and extensions."""
        file_path = tmp_path / filename
        file_path.write_text(write_func(test_data))

        result = _parse_file_parameter(file_path)
        assert result == test_data

    def test_file_doesnt_exist(self, tmp_path):
        """Test error when file doesn't exist."""
        nonexistent_file = tmp_path / "nonexistent.json"

        with pytest.raises(click.BadParameter, match="does not exist"):
            _parse_file_parameter(nonexistent_file)

    def test_path_is_directory(self, tmp_path):
        """Test error when path points to a directory."""
        directory = tmp_path / "testdir"
        directory.mkdir()

        with pytest.raises(click.BadParameter, match="is not a file"):
            _parse_file_parameter(directory)

    def test_invalid_json(self, tmp_path):
        """Test error when JSON file is malformed."""
        json_file = tmp_path / "test.json"
        json_file.write_text('{"invalid": json}')  # Missing quotes around json

        with pytest.raises(click.BadParameter, match="formatted incorrectly"):
            _parse_file_parameter(json_file)

    def test_invalid_yaml(self, tmp_path):
        """Test error when YAML file is malformed."""
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text("key: value\n  invalid: indentation")

        with pytest.raises(click.BadParameter, match="formatted incorrectly"):
            _parse_file_parameter(yaml_file)

    def test_non_dict_content_json(self, tmp_path):
        """Test error when JSON file doesn't contain a dictionary."""
        json_file = tmp_path / "test.json"
        json_file.write_text('["not", "a", "dict"]')

        with pytest.raises(click.BadParameter, match="should contain a dictionary"):
            _parse_file_parameter(json_file)

    def test_non_dict_content_yaml(self, tmp_path):
        """Test error when YAML file doesn't contain a dictionary."""
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text("- not\n- a\n- dict")

        with pytest.raises(click.BadParameter, match="should contain a dictionary"):
            _parse_file_parameter(yaml_file)


class TestParseMultiFormatParameters:
    """Test the _parse_multi_format_parameters function."""

    @pytest.mark.parametrize(
        "params,expected",
        [
            pytest.param(
                ["key1=value1", "key2=value2"],
                {"key1": "value1", "key2": "value2"},
                id="simple_key_value_pairs",
            ),
            pytest.param(
                ["url=https://example.com/path?param=value"],
                {"url": "https://example.com/path?param=value"},
                id="key_value_with_equals_in_value",
            ),
            pytest.param(["empty_key="], {"empty_key": ""}, id="key_value_empty_value"),
            pytest.param(
                ['{"key1": "value1", "key2": {"nested": "value"}}'],
                {"key1": "value1", "key2": {"nested": "value"}},
                id="inline_json_string",
            ),
            pytest.param(
                ['{"key1": "value1"}', '{"key2": "value2"}'],
                {"key1": "value1", "key2": "value2"},
                id="multiple_inline_json_objects",
            ),
            pytest.param(
                ["key1=value1", '{"key2": "value2"}'],
                {"key1": "value1", "key2": "value2"},
                id="mixed_key_value_and_json",
            ),
            pytest.param(
                ["  key=value  ", "  other=test  "],
                {"key": "value", "other": "test"},
                id="whitespace_handling",
            ),
            pytest.param([], {}, id="empty_params_list"),
        ],
    )
    def test_basic_parameter_formats(self, params, expected):
        """Test various basic parameter formats."""
        result = _parse_multi_format_parameters(params)
        assert result == expected

    def test_file_path_json(self, tmp_path):
        """Test parsing file:// paths with JSON files."""
        json_file = tmp_path / "test.json"
        test_data = {"file_key": "file_value"}
        json_file.write_text(json.dumps(test_data))

        params = [f"file://{json_file}"]
        result = _parse_multi_format_parameters(params)
        assert result == test_data

    def test_file_path_yaml(self, tmp_path):
        """Test parsing file:// paths with YAML files."""
        yaml_file = tmp_path / "test.yaml"
        test_data = {"yaml_key": "yaml_value", "list": [1, 2, 3]}
        yaml_file.write_text(yaml.dump(test_data))

        params = [f"file://{yaml_file}"]
        result = _parse_multi_format_parameters(params)
        assert result == test_data

    def test_mixed_formats(self, tmp_path):
        """Test mixing different parameter formats."""
        json_file = tmp_path / "test.json"
        json_file.write_text('{"from_file": "file_value"}')

        params = [
            "key1=value1",
            '{"from_json": "json_value"}',
            f"file://{json_file}",
            "key2=value2",
        ]
        result = _parse_multi_format_parameters(params)
        assert result == {
            "key1": "value1",
            "from_json": "json_value",
            "from_file": "file_value",
            "key2": "value2",
        }

    def test_later_values_override_earlier(self):
        """Test that later values override earlier ones for the same key."""
        params = ["key=first_value", '{"key": "second_value"}', "key=final_value"]
        result = _parse_multi_format_parameters(params)
        assert result == {"key": "final_value"}

    def test_invalid_key_value_format(self):
        """Test error with invalid key=value format."""
        params = ["invalid_format_no_equals"]

        with pytest.raises(click.BadParameter, match="not formatted correctly"):
            _parse_multi_format_parameters(params)

    def test_invalid_json_format(self):
        """Test error with malformed JSON."""
        params = ['{"invalid": json}']

        with pytest.raises(click.BadParameter, match="not formatted correctly"):
            _parse_multi_format_parameters(params)

    def test_json_array_not_recognized(self):
        """Test that JSON arrays don't match the inline JSON pattern and are treated as malformed."""
        params = ['["not", "a", "dict"]']

        # JSON arrays don't match the {.*} pattern so they're treated as malformed
        with pytest.raises(click.BadParameter, match="not formatted correctly"):
            _parse_multi_format_parameters(params)

    def test_json_non_dict_from_file(self, tmp_path):
        """Test error when file contains JSON that's not a dictionary."""
        json_file = tmp_path / "test.json"
        json_file.write_text('["not", "a", "dict"]')

        params = [f"file://{json_file}"]

        # File parsing goes through _parse_file_parameter which checks for dict
        with pytest.raises(click.BadParameter, match="should contain a dictionary"):
            _parse_multi_format_parameters(params)

    def test_file_not_found(self, tmp_path):
        """Test error when file:// path doesn't exist."""
        nonexistent = tmp_path / "nonexistent.json"
        params = [f"file://{nonexistent}"]

        with pytest.raises(click.BadParameter, match="does not exist"):
            _parse_multi_format_parameters(params)

    def test_malformed_parameter(self):
        """Test error with completely malformed parameter."""
        params = ["{{malformed}}"]

        with pytest.raises(click.BadParameter, match="not formatted correctly"):
            _parse_multi_format_parameters(params)


class TestProgressBarCallbackManager:
    """Tests for _ProgressBarCallbackManager"""

    def test_progress_bar_closes_on_completion(self):
        """
        Regression test for https://github.com/aws-deadline/deadline-cloud/issues/1008
        When the progress bar callback is called with progress equal to the bar length,
        the bar should be properly closed (emitting a newline).
        """
        from deadline.client.cli._common import _ProgressBarCallbackManager
        from deadline.job_attachments.progress_tracker import ProgressReportMetadata, ProgressStatus

        manager = _ProgressBarCallbackManager(length=100, label="Uploading Attachments")
        manager.callback(
            ProgressReportMetadata(
                status=ProgressStatus.UPLOAD_IN_PROGRESS,
                progress=100,
                transferRate=0,
                progressMessage="No files to upload",
                processedFiles=0,
            )
        )

        assert manager._bar_status == manager.BAR_CLOSED

    def test_progress_bar_not_closed_at_zero(self):
        """
        Verifies that calling the callback with progress=0 does NOT close the bar.
        This is the scenario that caused the missing newline bug in issue #1008.
        """
        from deadline.client.cli._common import _ProgressBarCallbackManager
        from deadline.job_attachments.progress_tracker import ProgressReportMetadata, ProgressStatus

        manager = _ProgressBarCallbackManager(length=100, label="Uploading Attachments")
        manager.callback(
            ProgressReportMetadata(
                status=ProgressStatus.UPLOAD_IN_PROGRESS,
                progress=0,
                transferRate=0,
                progressMessage="No files to upload",
                processedFiles=0,
            )
        )

        assert manager._bar_status == manager.BAR_CREATED
        manager._exit_stack.close()
