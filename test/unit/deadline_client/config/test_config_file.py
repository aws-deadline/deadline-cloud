# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
tests the deadline.client.config settings
"""

import os
import platform
import subprocess
from unittest.mock import patch, MagicMock
from pathlib import Path

import boto3  # type: ignore[import]
import pytest

from deadline.client import config
from deadline.client.config import (
    config_file,
)
from deadline.client.exceptions import DeadlineOperationError

# This is imported by `test_cli_config.py` for a matching CLI test
CONFIG_SETTING_ROUND_TRIP = [
    ("defaults.aws_profile_name", "(default)", "AnotherProfileName"),
    ("defaults.farm_id", "", "farm-82934h23k4j23kjh"),
    ("defaults.job_attachments_file_system", "COPIED", "VIRTUAL"),
]


@pytest.mark.parametrize("setting_name,default_value,alternate_value", CONFIG_SETTING_ROUND_TRIP)
def test_config_settings_roundtrip(
    fresh_deadline_config, setting_name, default_value, alternate_value
):
    """Test that each setting we support has the right default and roundtrips changes"""
    assert config.get_setting(setting_name) == default_value
    config.set_setting(setting_name, alternate_value)
    assert config.get_setting(setting_name) == alternate_value


def test_config_settings_hierarchy(fresh_deadline_config):
    """
    Test that settings are stored hierarchically,
    aws profile -> farm id -> queue id
    """
    # First set some settings that apply to the defaults, changing the
    # hierarchy from queue inwards.
    config.set_setting("settings.storage_profile_id", "storage-profile-for-farm-default")
    config.set_setting("defaults.queue_id", "queue-for-farm-default")
    config.set_setting("defaults.farm_id", "farm-for-profile-default")
    config.set_setting("defaults.aws_profile_name", "NonDefaultProfile")

    # Confirm that all child settings we changed are default, because they were
    # for a different profile.
    assert config.get_setting("defaults.farm_id") == ""
    assert config.get_setting("defaults.queue_id") == ""
    assert config.get_setting("settings.storage_profile_id") == ""

    # Switch back to the default profile, and check the next layer of the onion
    config.set_setting("defaults.aws_profile_name", "(default)")
    assert config.get_setting("defaults.farm_id") == "farm-for-profile-default"
    # The queue id is still default
    assert config.get_setting("defaults.queue_id") == ""
    # The storage profile id is still default
    assert config.get_setting("settings.storage_profile_id") == ""

    # Switch back to the default farm
    config.set_setting("defaults.farm_id", "")
    assert config.get_setting("defaults.queue_id") == "queue-for-farm-default"
    # Storage profile needs "profile - farm_id" so it should be back to the original
    assert config.get_setting("settings.storage_profile_id") == "storage-profile-for-farm-default"

    # Switch to default farm and default queue
    config.set_setting("defaults.queue_id", "")
    assert config.get_setting("settings.storage_profile_id") == "storage-profile-for-farm-default"


def test_config_get_setting_nonexistant(fresh_deadline_config):
    """Test the error from get_setting when a setting doesn't exist."""
    # Setting name without the '.'
    with pytest.raises(DeadlineOperationError) as excinfo:
        config.get_setting("setting_name_bad_format")
    assert "is not valid" in str(excinfo.value)
    assert "setting_name_bad_format" in str(excinfo.value)

    # Section name is wrong
    with pytest.raises(DeadlineOperationError) as excinfo:
        config.get_setting("setitngs.aws_profile_name")
    assert "has no setting" in str(excinfo.value)
    assert "setitngs" in str(excinfo.value)

    # Section is good, but no setting
    with pytest.raises(DeadlineOperationError) as excinfo:
        config.get_setting("settings.aws_porfile_name")
    assert "has no setting" in str(excinfo.value)
    assert "aws_porfile_name" in str(excinfo.value)


def test_config_set_setting_nonexistant(fresh_deadline_config):
    """Test the error from set_setting when a setting doesn't exist."""
    # Setting name without the '.'
    with pytest.raises(DeadlineOperationError) as excinfo:
        config.set_setting("setting_name_bad_format", "value")
    assert "is not valid" in str(excinfo.value)
    assert "setting_name_bad_format" in str(excinfo.value)

    # Section name is wrong
    with pytest.raises(DeadlineOperationError) as excinfo:
        config.set_setting("setitngs.aws_profile_name", "value")
    assert "has no setting" in str(excinfo.value)
    assert "setitngs" in str(excinfo.value)

    # Section is good, but no setting
    with pytest.raises(DeadlineOperationError) as excinfo:
        config.set_setting("settings.aws_porfile_name", "value")
    assert "has no setting" in str(excinfo.value)
    assert "aws_porfile_name" in str(excinfo.value)


@patch.object(config_file, "_should_read_config", MagicMock(return_value=True))
def test_config_file_env_var(fresh_deadline_config):
    """Test that setting the env var DEADLINE_CONFIG_FILE_PATH overrides the config path"""
    assert config_file.get_config_file_path() == Path(fresh_deadline_config).expanduser()

    alternate_deadline_config_file = fresh_deadline_config + "_alternative_file"

    # Set our config file to a known starting point
    config.set_setting("defaults.aws_profile_name", "EnvVarOverrideProfile")
    assert config.get_setting("defaults.aws_profile_name") == "EnvVarOverrideProfile"
    with open(fresh_deadline_config, "r", encoding="utf-8") as f:
        assert "aws_profile_name = EnvVarOverrideProfile" in f.read()

    try:
        # Set the override environment variable
        os.environ["DEADLINE_CONFIG_FILE_PATH"] = alternate_deadline_config_file
        assert (
            config_file.get_config_file_path() == Path(alternate_deadline_config_file).expanduser()
        )

        # Confirm that we see the default settings again
        assert config.get_setting("defaults.aws_profile_name") == "(default)"

        # Change the settings in this new file
        config.set_setting("defaults.aws_profile_name", "AlternateProfileName")
        assert config.get_setting("defaults.aws_profile_name") == "AlternateProfileName"
        with open(alternate_deadline_config_file, "r", encoding="utf-8") as f:
            assert "aws_profile_name = AlternateProfileName" in f.read()

        # Remove the override
        del os.environ["DEADLINE_CONFIG_FILE_PATH"]
        assert config_file.get_config_file_path() == Path(fresh_deadline_config).expanduser()

        # We should see the known starting point again
        assert config.get_setting("defaults.aws_profile_name") == "EnvVarOverrideProfile"

        # Set the override environment variable again
        os.environ["DEADLINE_CONFIG_FILE_PATH"] = alternate_deadline_config_file
        assert (
            config_file.get_config_file_path() == Path(alternate_deadline_config_file).expanduser()
        )

        assert config.get_setting("defaults.aws_profile_name") == "AlternateProfileName"
    finally:
        os.unlink(alternate_deadline_config_file)
        if "DEADLINE_CONFIG_FILE_PATH" in os.environ:
            del os.environ["DEADLINE_CONFIG_FILE_PATH"]


def test_get_best_profile_for_farm(fresh_deadline_config):
    """
    Test that it returns the exact farm + queue id match
    """
    PROFILE_SETTINGS = [
        ("Profile1", "farm-1", "queue-1"),
        ("Profile2", "farm-2", "queue-2"),
        ("Profile3", "farm-1", "queue-3"),
        ("Profile4", "farm-3", "queue-4"),
        ("Profile5", "farm-3", "queue-5"),
        ("Profile6", "", ""),
    ]
    for profile_name, farm_id, queue_id in PROFILE_SETTINGS:
        config.set_setting("defaults.aws_profile_name", profile_name)
        config.set_setting("defaults.farm_id", farm_id)
        config.set_setting("defaults.queue_id", queue_id)

    with patch.object(boto3, "Session") as boto3_session:
        MOCK_PROFILE_VALUE = {
            "sso_start_url": "https://d-012345abcd.awsapps.com/start",
            "sso_region": "us-west-2",
            "sso_account_id": "123456789012",
            "sso_role_name": "AwsProfileForDeadline",
            "region": "us-west-2",
        }
        boto3_session()._session.full_config = {
            "profiles": {
                profile_settings[0]: MOCK_PROFILE_VALUE for profile_settings in PROFILE_SETTINGS
            },
        }

        # In each case, when the default profile doesn't match the farm,
        # an exact match of farm/queue id should return the corresponding profile
        for profile_name, farm_id, queue_id in PROFILE_SETTINGS:
            if farm_id:
                assert config.get_best_profile_for_farm(farm_id, queue_id) == profile_name
                # Getting the best profile should not have modified the default
                assert config.get_setting("defaults.aws_profile_name") == "Profile6"

        # Matching just the farm id should return the first matching profile
        assert config.get_best_profile_for_farm("farm-1") == "Profile1"
        assert config.get_best_profile_for_farm("farm-2") == "Profile2"
        assert config.get_best_profile_for_farm("farm-3") == "Profile4"

        # Matching the farm id with a missing queue id should return the first matching profile
        assert config.get_best_profile_for_farm("farm-1", "queue-missing") == "Profile1"
        assert config.get_best_profile_for_farm("farm-2", "queue-missing") == "Profile2"
        assert config.get_best_profile_for_farm("farm-3", "queue-missing") == "Profile4"

        # If the farm id doesn't match, should return the default (which is Profile6)
        assert config.get_best_profile_for_farm("farm-missing") == "Profile6"
        assert config.get_best_profile_for_farm("farm-missing", "queue-missing") == "Profile6"

        # If the farm id does match, should return the default even if it isn't the first match
        config.set_setting("defaults.aws_profile_name", "Profile5")
        assert config.get_best_profile_for_farm("farm-1") == "Profile1"
        assert config.get_best_profile_for_farm("farm-2") == "Profile2"
        # For farm-3, the first match is Profile4, but the default is Profile5
        assert config.get_best_profile_for_farm("farm-3") == "Profile5"


def test_str2bool():
    assert config_file.str2bool("on") is True
    assert config_file.str2bool("true") is True
    assert config_file.str2bool("tRuE") is True
    assert config_file.str2bool("1") is True
    assert config_file.str2bool("off") is False
    assert config_file.str2bool("false") is False
    assert config_file.str2bool("FaLsE") is False
    assert config_file.str2bool("0") is False
    with pytest.raises(ValueError):
        config_file.str2bool("not_boolean")
    with pytest.raises(ValueError):
        config_file.str2bool("")


@pytest.mark.skipif(
    platform.system() != "Windows",
    reason="This test is for testing file permission changes in Windows.",
)
def test_windows_config_file_permissions(fresh_deadline_config) -> None:
    config_file_path = config_file.get_config_file_path()
    parent_dir = config_file_path.parent
    subprocess.run(
        [
            "icacls",
            str(parent_dir),
            "/grant",
            "Everyone:(OI)(CI)(F)",
            "/T",
        ],
        check=True,
    )

    config_file.set_setting("defaults.aws_profile_name", "goodguyprofile")

    result = subprocess.run(
        [
            "icacls",
            str(config_file_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert "Everyone" not in result.stdout


@pytest.mark.skipif(
    platform.system() == "Windows",
    reason="This test is for testing file permission changes in POSIX.",
)
def test_posix_config_file_permissions(fresh_deadline_config) -> None:
    config_file_path = config_file.get_config_file_path()
    config_file_path.chmod(0o777)

    config_file.set_setting("defaults.aws_profile_name", "goodguyprofile")

    assert config_file_path.stat().st_mode & 0o777 == 0o600
