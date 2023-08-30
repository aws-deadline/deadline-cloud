# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the CLI config command.
"""

import importlib
import logging
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from deadline.client import config
from deadline.client.cli import deadline_cli
from deadline.client.config import config_file

from ..config.test_config_file import CONFIG_SETTING_ROUND_TRIP


def test_cli_config_show_defaults(fresh_deadline_config):
    """
    Confirm that the CLI interface prints out all the configuration
    file data, when the configuration is default
    """
    runner = CliRunner()
    result = runner.invoke(deadline_cli.cli, ["config", "show"])

    assert result.exit_code == 0

    settings = config_file.SETTINGS

    # The command prints out the full config file path
    assert fresh_deadline_config in result.output

    # Assert the expected number of settings
    assert len(settings.keys()) == 12

    for setting_name in settings.keys():
        assert setting_name in result.output
        assert str(config_file.get_setting_default(setting_name)) in result.output


def test_default_log_level(fresh_deadline_config):
    """We must make sure that DEBUG is not the default log level"""
    assert config.get_setting("settings.log_level") == "INFO"
    assert config.get_setting_default("settings.log_level") == "INFO"


@pytest.mark.parametrize(
    "log_level",
    [
        "NOT_A_LOG_LEVEL",
        "DEBUG",
        "INFO",
        "WARNING",
        "ERROR",
    ],
)
def test_log_level_updated(fresh_deadline_config, caplog, log_level):
    """Tests that the logging level is set to debug"""
    # GIVEN
    config.set_setting("settings.log_level", log_level)
    # because the log level gets passed into a click decorator we need to reload the module to get
    # the updated setting. This is fine for CLI usage because the module is reloaded each invocation
    importlib.reload(deadline_cli)
    # WHEN
    with caplog.at_level(logging.DEBUG), patch.object(
        deadline_cli.logging, "basicConfig"
    ) as mock_basic_config:
        # THEN
        CliRunner().invoke(deadline_cli.cli, ["config", "show"])

    # THEN
    assert (
        f"Log Level '{log_level}' not in ['ERROR', 'WARNING', 'INFO', 'DEBUG']. Defaulting to INFO"
        in caplog.text
    ) == (log_level == "NOT_A_LOG_LEVEL")
    # This only ever gets logged if the log_level passed into click is DEBUG
    assert ("Debug logging is on" in caplog.text) == (log_level == "DEBUG")
    mock_basic_config.assert_called_with(
        level=logging.getLevelName(log_level if log_level != "NOT_A_LOG_LEVEL" else "INFO")
    )


def test_cli_config_show_modified_config(fresh_deadline_config):
    """
    Confirm that the CLI interface prints out all the configuration
    file data, when the configuration is default
    """
    config.set_setting("cloud-companion.path", "/User/auser/bin/CloudCompanion")
    config.set_setting("defaults.aws_profile_name", "EnvVarOverrideProfile")
    config.set_setting("settings.job_history_dir", "~/alternate/job_history")
    config.set_setting("settings.deadline_endpoint_url", "https://some-url-value")
    config.set_setting("defaults.farm_id", "farm-82934h23k4j23kjh")
    config.set_setting("settings.storage_profile_id", "sp-12345abcde12345")
    config.set_setting("defaults.queue_id", "queue-389348u234jhk34")
    config.set_setting("defaults.job_id", "job-239u40234jkl234nkl23")
    config.set_setting("settings.auto_accept", "False")
    config.set_setting("settings.log_level", "DEBUG")
    config.set_setting("telemetry.opt_out", "True")
    config.set_setting("telemetry.identifier", "user-id-123abc-456def")

    runner = CliRunner()
    result = runner.invoke(deadline_cli.cli, ["config", "show"])

    print(result.output)

    assert result.exit_code == 0

    # We should see all the overridden values in the output
    assert "EnvVarOverrideProfile" in result.output
    assert "~/alternate/job_history" in result.output
    assert result.output.count("False") == 1
    assert result.output.count("True") == 1
    assert "https://some-url-value" in result.output
    assert "farm-82934h23k4j23kjh" in result.output
    assert "queue-389348u234jhk34" in result.output
    assert "job-239u40234jkl234nkl23" in result.output
    assert "settings.log_level:\n   DEBUG" in result.output
    assert "user-id-123abc-456def" in result.output
    # It shouldn't say anywhere that there is a default setting
    assert "(default)" not in result.output


@pytest.mark.parametrize("setting_name,default_value,alternate_value", CONFIG_SETTING_ROUND_TRIP)
def test_config_settings_via_cli_roundtrip(
    fresh_deadline_config, setting_name, default_value, alternate_value
):
    """Test that each setting we support has the right default and roundtrips changes when called via CLI"""
    runner = CliRunner()

    result = runner.invoke(deadline_cli.cli, ["config", "get", setting_name])

    assert result.exit_code == 0
    assert result.output.strip() == str(default_value)

    result = runner.invoke(deadline_cli.cli, ["config", "set", setting_name, str(alternate_value)])
    assert result.exit_code == 0
    assert result.output.strip() == ""

    result = runner.invoke(deadline_cli.cli, ["config", "get", setting_name])
    assert result.exit_code == 0
    assert result.output.strip() == str(alternate_value)


def test_config_get_setting_nonexistant(fresh_deadline_config):
    """Test that we get an error with a non-existent setting."""
    runner = CliRunner()

    result = runner.invoke(deadline_cli.cli, ["config", "get", "settings.doesnt_exist"])

    assert result.exit_code == 1
    assert "doesnt_exist" in result.output


def test_config_set_setting_nonexistant(fresh_deadline_config):
    """Test that we get an error with a non-existent setting."""
    runner = CliRunner()

    result = runner.invoke(deadline_cli.cli, ["config", "set", "settings.doesnt_exist", "value"])

    assert result.exit_code == 1
    assert "doesnt_exist" in result.output
