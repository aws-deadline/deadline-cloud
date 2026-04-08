# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Integration tests for the `deadline config` CLI commands.

These tests exercise the config CLI end-to-end against a real (isolated) config file,
verifying that set/get/clear/show commands work correctly as a user would experience them.
"""

import json
import os
from typing import List, Tuple

import pytest
from click.testing import CliRunner

from deadline.client.cli import main
from deadline.client.config import config_file

from .test_utils import DeadlineCliTest


# ---------------------------------------------------------------------------
# Parameterized round-trip test data
#
# Each entry is:
#   (setting_name, default_value, test_value, dependency_settings)
#
# dependency_settings is a list of (setting_name, value) pairs that must be
# set before the target setting can be written (because of the "depend" chain
# in config_file.SETTINGS).
# ---------------------------------------------------------------------------

# Settings that need a farm_id set first
_FARM_DEP: List[Tuple[str, str]] = [("defaults.farm_id", "farm-0123456789abcdef0123456789abcdef")]
# Settings that need farm_id + queue_id
_QUEUE_DEP: List[Tuple[str, str]] = [
    *_FARM_DEP,
    ("defaults.queue_id", "queue-0123456789abcdef0123456789abcdef"),
]

CONFIG_SETTING_ROUND_TRIP = [
    # (setting_name, default_value, test_value, dependencies)
    ("deadline-cloud-monitor.path", "", "/usr/local/bin/DeadlineCloudMonitor", []),
    ("defaults.aws_profile_name", "(default)", "my-test-profile", []),
    (
        "settings.job_history_dir",
        os.path.join("~", ".deadline", "job_history", "(default)"),
        "~/custom/job_history",
        [],
    ),
    ("defaults.farm_id", "", "farm-0123456789abcdef0123456789abcdef", []),
    ("settings.storage_profile_id", "", "sp-0123456789abcdef", _FARM_DEP),
    ("defaults.queue_id", "", "queue-0123456789abcdef0123456789abcdef", _FARM_DEP),
    ("defaults.job_id", "", "job-0123456789abcdef0123456789abcdef", _QUEUE_DEP),
    ("settings.auto_accept", "false", "true", []),
    ("settings.conflict_resolution", "NOT_SELECTED", "CREATE_COPY", []),
    ("settings.log_level", "WARNING", "DEBUG", []),
    ("telemetry.opt_out", "false", "true", []),
    ("defaults.job_attachments_file_system", "COPIED", "VIRTUAL", _FARM_DEP),
    ("settings.s3_max_pool_connections", "50", "100", []),
    ("settings.small_file_threshold_multiplier", "20", "50", []),
    (
        "settings.known_asset_paths",
        "",
        os.pathsep.join(["/mnt/shared/assets", "/mnt/shared/textures"]),
        [],
    ),
    ("settings.locale", "", "ja_JP", []),
    ("settings.force_s3_check", "false", "true", []),
    ("settings.submitter_update_notification", "true", "false", []),
]


@pytest.mark.parametrize(
    "setting_name,default_value,test_value,dependencies",
    CONFIG_SETTING_ROUND_TRIP,
    ids=[entry[0] for entry in CONFIG_SETTING_ROUND_TRIP],
)
def test_config_setting_roundtrip(
    fresh_deadline_config: str,
    setting_name: str,
    default_value: str,
    test_value: str,
    dependencies: List[Tuple[str, str]],
) -> None:
    """
    For each config setting, verify:
    1. Default value is correct
    2. Setting a value persists
    3. Clearing restores the default
    """
    runner = CliRunner()

    # Set up any dependency settings first
    for dep_name, dep_value in dependencies:
        result = runner.invoke(main, ["config", "set", dep_name, dep_value])
        assert result.exit_code == 0, f"Failed to set dependency {dep_name}: {result.output}"

    # 1. Check default
    result = runner.invoke(main, ["config", "get", setting_name])
    assert result.exit_code == 0, result.output
    assert result.output.strip() == default_value, (
        f"{setting_name}: expected default '{default_value}', got '{result.output.strip()}'"
    )

    # 2. Set and verify
    result = runner.invoke(main, ["config", "set", setting_name, test_value])
    assert result.exit_code == 0, result.output

    result = runner.invoke(main, ["config", "get", setting_name])
    assert result.exit_code == 0, result.output
    assert result.output.strip() == test_value

    # 3. Clear and verify default is restored
    result = runner.invoke(main, ["config", "clear", setting_name])
    assert result.exit_code == 0, result.output

    result = runner.invoke(main, ["config", "get", setting_name])
    assert result.exit_code == 0, result.output
    assert result.output.strip() == default_value, (
        f"{setting_name}: expected default '{default_value}' after clear, got '{result.output.strip()}'"
    )


# ---------------------------------------------------------------------------
# config show tests
# ---------------------------------------------------------------------------


def test_config_show_returns_all_settings(fresh_deadline_config: str) -> None:
    """Verify `deadline config show` succeeds and displays all expected settings."""
    runner = CliRunner()

    result = runner.invoke(main, ["config", "show"])

    assert result.exit_code == 0, result.output
    assert fresh_deadline_config in result.output
    for setting_name in config_file.SETTINGS:
        assert setting_name in result.output, f"Missing setting: {setting_name}"


def test_config_show_json_output(fresh_deadline_config: str) -> None:
    """Verify `deadline config show --output json` returns valid JSON with all settings."""
    runner = CliRunner()

    result = runner.invoke(main, ["config", "show", "--output", "json"])

    assert result.exit_code == 0, result.output
    parsed = json.loads(result.output)
    assert isinstance(parsed, dict)
    assert "settings.config_file_path" in parsed
    for setting_name in config_file.SETTINGS:
        assert setting_name in parsed, f"Missing setting in JSON: {setting_name}"


def test_config_show_reflects_changes(
    fresh_deadline_config: str, deadline_cli_test: DeadlineCliTest
) -> None:
    """Verify that `config show` reflects values set via `config set`."""
    runner = CliRunner()

    runner.invoke(main, ["config", "set", "defaults.farm_id", deadline_cli_test.farm_id])
    runner.invoke(main, ["config", "set", "settings.log_level", "DEBUG"])

    result = runner.invoke(main, ["config", "show"])
    assert result.exit_code == 0
    assert deadline_cli_test.farm_id in result.output
    assert "DEBUG" in result.output
    for line in result.output.splitlines():
        if "defaults.farm_id:" in line:
            assert "(default)" not in line
            break


def test_config_show_json_reflects_changes(
    fresh_deadline_config: str, deadline_cli_test: DeadlineCliTest
) -> None:
    """Verify that `config show --output json` reflects values set via `config set`."""
    runner = CliRunner()

    runner.invoke(main, ["config", "set", "defaults.farm_id", deadline_cli_test.farm_id])
    runner.invoke(main, ["config", "set", "settings.log_level", "INFO"])

    result = runner.invoke(main, ["config", "show", "--output", "json"])
    assert result.exit_code == 0
    parsed = json.loads(result.output)
    assert parsed["defaults.farm_id"] == deadline_cli_test.farm_id
    assert parsed["settings.log_level"] == "INFO"


# ---------------------------------------------------------------------------
# Error handling tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "command",
    [
        pytest.param(["config", "get", "settings.does_not_exist"], id="get"),
        pytest.param(["config", "set", "settings.does_not_exist", "value"], id="set"),
        pytest.param(["config", "clear", "settings.does_not_exist"], id="clear"),
    ],
)
def test_config_nonexistent_setting(fresh_deadline_config: str, command: list) -> None:
    """Verify that get/set/clear of a nonexistent setting returns an error."""
    runner = CliRunner()

    result = runner.invoke(main, command)
    assert result.exit_code == 1
    assert "does_not_exist" in result.output


# ---------------------------------------------------------------------------
# telemetry.identifier test (separate because the fresh_deadline_config
# fixture pre-sets it, so the standard roundtrip pattern doesn't apply)
# ---------------------------------------------------------------------------


def test_config_set_telemetry_identifier(fresh_deadline_config: str) -> None:
    """
    Verify setting and clearing telemetry.identifier.

    The fresh_deadline_config fixture pre-sets this to a zeroed UUID to prevent
    mid-test config writes, so the initial value is not the true default.
    """
    runner = CliRunner()

    # Initial value is the fixture-set zeroed UUID
    result = runner.invoke(main, ["config", "get", "telemetry.identifier"])
    assert result.exit_code == 0
    assert result.output.strip() == "00000000-0000-0000-0000-000000000000"

    # Set a custom value
    identifier = "test-id-abc123-def456"
    result = runner.invoke(main, ["config", "set", "telemetry.identifier", identifier])
    assert result.exit_code == 0

    result = runner.invoke(main, ["config", "get", "telemetry.identifier"])
    assert result.output.strip() == identifier

    # Clear restores the true default (empty string), not the fixture value
    result = runner.invoke(main, ["config", "clear", "telemetry.identifier"])
    assert result.exit_code == 0

    result = runner.invoke(main, ["config", "get", "telemetry.identifier"])
    assert result.output.strip() == ""
