# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import json
from typing import Any, Dict
from unittest.mock import patch

import pytest

import click
import yaml

from deadline.client.cli._common import (
    _STANDARD_OPTION_NAMES,
    _apply_cli_options_to_config,
    _auto_select_farm,
    _auto_select_queue,
    _parse_file_parameter,
    _parse_multi_format_parameters,
)
from deadline.client.config import config_file
from deadline.client.config.config_file import (
    _SETTING_FARM_ID as SETTING_FARM_ID,
    _SETTING_QUEUE_ID as SETTING_QUEUE_ID,
)


class TestApplyCliOptionsRegion:
    """Test that _apply_cli_options_to_config handles the --region option."""

    def test_region_sets_farm_region_setting(self, fresh_deadline_config):
        config = _apply_cli_options_to_config(region="us-east-1")
        assert config_file.get_setting("defaults.farm_region", config=config) == "us-east-1"

    def test_region_none_does_not_raise_unexpected_option(self, fresh_deadline_config):
        # region=None must be consumed in the "no options provided" branch so the
        # "unexpected option" RuntimeError doesn't fire.
        config = _apply_cli_options_to_config(region=None)
        assert config_file.get_setting("defaults.farm_region", config=config) == ""

    def test_region_none_with_other_options(self, fresh_deadline_config):
        # region=None alongside a provided option must still be consumed.
        config = _apply_cli_options_to_config(farm_id="farm-1", region=None)
        assert config_file.get_setting("defaults.farm_id", config=config) == "farm-1"
        assert config_file.get_setting("defaults.farm_region", config=config) == ""


class TestApplyCliOptionsStandardNames:
    """Every standard option must be consumed whether or not any option has a value."""

    @pytest.mark.parametrize("name", _STANDARD_OPTION_NAMES)
    def test_unset_standard_option_is_consumed(self, fresh_deadline_config, name):
        # All-None args take the "no options provided" path, which must still consume
        # every standard name so the "not standard options" RuntimeError doesn't fire.
        args: Dict[str, Any] = {name: None}
        assert _apply_cli_options_to_config(**args) is None

    def test_all_unset_standard_options_are_consumed(self, fresh_deadline_config):
        args: Dict[str, Any] = {name: None for name in _STANDARD_OPTION_NAMES}
        assert _apply_cli_options_to_config(**args) is None

    def test_non_standard_option_still_raises(self, fresh_deadline_config):
        with pytest.raises(RuntimeError, match="not standard"):
            _apply_cli_options_to_config(farm_id=None, not_a_standard_option=None)


class TestCliRegionPrecedenceEndToEnd:
    """
    End-to-end region precedence and flag semantics, driven through a real command
    (``farm get``) rather than just ``_apply_cli_options_to_config``. ``farm get`` builds
    its deadline client via ``api.get_boto3_client`` -> ``api._session.get_session_client``,
    so the region the client is built for is what we assert.
    """

    @staticmethod
    def _run_farm_get(extra_args):
        from unittest.mock import MagicMock, patch
        from click.testing import CliRunner
        from deadline.client import api
        from deadline.client.cli import main

        with (
            patch.object(api._session, "get_boto3_session"),
            patch.object(api._session, "get_session_client") as get_session_client_mock,
        ):
            client = MagicMock()
            client.get_farm.return_value = {"farmId": "farm-abc", "displayName": "F"}
            get_session_client_mock.return_value = client

            runner = CliRunner()
            result = runner.invoke(main, ["farm", "get", "--farm-id", "farm-abc", *extra_args])

        regions = [
            call.kwargs.get("region")
            for call in get_session_client_mock.call_args_list
            if call.kwargs.get("service_name") == "deadline"
        ]
        return result, regions

    def test_region_flag_overrides_configured_farm_region(self, fresh_deadline_config):
        """--region wins over a previously-configured defaults.farm_region."""
        config_file.set_setting("defaults.farm_region", "us-west-2")

        result, regions = self._run_farm_get(["--region", "eu-central-1"])

        assert result.exit_code == 0, result.output
        # The deadline client was built for the flag's region, not the configured one.
        assert "eu-central-1" in regions
        assert "us-west-2" not in regions
        # And the flag overwrote the persisted setting.
        assert config_file.get_setting("defaults.farm_region") == "eu-central-1"

    def test_no_region_flag_uses_configured_farm_region(self, fresh_deadline_config):
        """with no --region, the command uses the configured defaults.farm_region."""
        # farm_region is stored per-farm (depends on defaults.farm_id), so the default
        # farm_id must be set to the same farm the command targets before storing the region.
        config_file.set_setting("defaults.farm_id", "farm-abc")
        config_file.set_setting("defaults.farm_region", "ap-south-1")

        result, regions = self._run_farm_get([])

        assert result.exit_code == 0, result.output
        assert regions == ["ap-south-1"]

    def test_no_region_flag_and_no_config_builds_client_without_region(self, fresh_deadline_config):
        """
        no --region and no configured farm_region => the deadline client is built
        with region=None (the old single-region behavior, where boto3/the session picks the
        region).
        """
        result, regions = self._run_farm_get([])

        assert result.exit_code == 0, result.output
        # region=None means get_session_client was called without an explicit region.
        assert regions == [None]

    def test_region_on_read_only_command_persists_farm_region(self, fresh_deadline_config):
        """
        --region on a read-only command (farm get) persists defaults.farm_region.
        This pins the CURRENT, intended behavior: read-only commands still write the region
        to config via _apply_cli_options_to_config, so a subsequent command reuses it.
        """
        assert config_file.get_setting("defaults.farm_region") == ""

        result, _ = self._run_farm_get(["--region", "eu-west-1"])

        assert result.exit_code == 0, result.output
        # Intended: the region is persisted even though farm get does not mutate anything.
        assert config_file.get_setting("defaults.farm_region") == "eu-west-1"


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


class TestAutoSelectFarm:
    def test_single_farm_returns_id(self, fresh_deadline_config):
        with patch("deadline.client.cli._common._api.list_farms") as mock_list:
            mock_list.return_value = {"farms": [{"farmId": "farm-abc123"}]}
            assert _auto_select_farm() == "farm-abc123"

    def test_multiple_farms_returns_none(self, fresh_deadline_config):
        with patch("deadline.client.cli._common._api.list_farms") as mock_list:
            mock_list.return_value = {"farms": [{"farmId": "farm-1"}, {"farmId": "farm-2"}]}
            assert _auto_select_farm() is None

    def test_no_farms_returns_none(self, fresh_deadline_config):
        with patch("deadline.client.cli._common._api.list_farms") as mock_list:
            mock_list.return_value = {"farms": []}
            assert _auto_select_farm() is None

    def test_api_error_returns_none(self, fresh_deadline_config):
        with patch("deadline.client.cli._common._api.list_farms") as mock_list:
            mock_list.side_effect = Exception("API error")
            assert _auto_select_farm() is None


class TestAutoSelectQueue:
    def test_single_queue_returns_id(self, fresh_deadline_config):
        config_file.set_setting("defaults.farm_id", "farm-abc123")
        with patch("deadline.client.cli._common._api.list_queues") as mock_list:
            mock_list.return_value = {"queues": [{"queueId": "queue-xyz789"}]}
            assert _auto_select_queue() == "queue-xyz789"
            mock_list.assert_called_once_with(farmId="farm-abc123", config=None)

    def test_multiple_queues_returns_none(self, fresh_deadline_config):
        config_file.set_setting("defaults.farm_id", "farm-abc123")
        with patch("deadline.client.cli._common._api.list_queues") as mock_list:
            mock_list.return_value = {"queues": [{"queueId": "queue-1"}, {"queueId": "queue-2"}]}
            assert _auto_select_queue() is None

    def test_no_queues_returns_none(self, fresh_deadline_config):
        config_file.set_setting("defaults.farm_id", "farm-abc123")
        with patch("deadline.client.cli._common._api.list_queues") as mock_list:
            mock_list.return_value = {"queues": []}
            assert _auto_select_queue() is None

    def test_no_farm_id_returns_none(self, fresh_deadline_config):
        assert _auto_select_queue() is None

    def test_api_error_returns_none(self, fresh_deadline_config):
        config_file.set_setting("defaults.farm_id", "farm-abc123")
        with patch("deadline.client.cli._common._api.list_queues") as mock_list:
            mock_list.side_effect = Exception("API error")
            assert _auto_select_queue() is None


class TestApplyCliOptionsAutoSelect:
    """End-to-end auto-select behavior through _apply_cli_options_to_config."""

    def test_auto_selects_single_farm_and_queue(self, fresh_deadline_config):
        """With one farm and one queue, both required options are auto-filled."""
        with (
            patch("deadline.client.cli._common._api.list_farms") as mock_farms,
            patch("deadline.client.cli._common._api.list_queues") as mock_queues,
        ):
            mock_farms.return_value = {"farms": [{"farmId": "farm-1"}]}
            mock_queues.return_value = {"queues": [{"queueId": "queue-1"}]}

            _apply_cli_options_to_config(required_options={"farm_id", "queue_id"})

        assert config_file.get_setting(SETTING_FARM_ID) == "farm-1"
        assert config_file.get_setting(SETTING_QUEUE_ID) == "queue-1"

    def test_raises_when_multiple_farms(self, fresh_deadline_config):
        """With multiple farms, the missing-farm UsageError is still raised."""
        with patch("deadline.client.cli._common._api.list_farms") as mock_farms:
            mock_farms.return_value = {"farms": [{"farmId": "f-1"}, {"farmId": "f-2"}]}
            with pytest.raises(click.UsageError, match="farm-id"):
                _apply_cli_options_to_config(required_options={"farm_id"})

    def test_raises_when_multiple_queues(self, fresh_deadline_config):
        """A single farm auto-selects, but multiple queues raise the queue error."""
        with (
            patch("deadline.client.cli._common._api.list_farms") as mock_farms,
            patch("deadline.client.cli._common._api.list_queues") as mock_queues,
        ):
            mock_farms.return_value = {"farms": [{"farmId": "farm-1"}]}
            mock_queues.return_value = {"queues": [{"queueId": "q-1"}, {"queueId": "q-2"}]}
            with pytest.raises(click.UsageError, match="queue-id"):
                _apply_cli_options_to_config(required_options={"farm_id", "queue_id"})

        # The farm should still have been auto-selected before the queue failure.
        assert config_file.get_setting(SETTING_FARM_ID) == "farm-1"

    def test_explicit_farm_id_skips_auto_select(self, fresh_deadline_config):
        """An explicit --farm-id is honored and list_farms is never called."""
        with patch("deadline.client.cli._common._api.list_farms") as mock_farms:
            _apply_cli_options_to_config(required_options={"farm_id"}, farm_id="farm-explicit")

        mock_farms.assert_not_called()
        assert config_file.get_setting(SETTING_FARM_ID) == "farm-explicit"
