# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
tests the deadline.client.config settings
"""

import os
import platform
import getpass
import tempfile
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
    ("defaults.farm_region", "", "eu-west-1"),
    ("defaults.job_attachments_file_system", "COPIED", "VIRTUAL"),
    ("settings.locale", "", "ja_JP"),
    ("settings.force_s3_check", "false", "true"),
    ("settings.deadline_regions", "", "us-east-1,eu-west-1"),
    ("settings.https_proxy", "", "http://proxy.example.com:8080"),
    # ca_bundle is an is_path setting: the stored value is normalized to the
    # native path form on read. Build the alternate value with the native
    # separator so it round-trips to itself on POSIX and Windows alike.
    ("settings.ca_bundle", "", os.path.join(os.sep, "etc", "ssl", "certs", "ca-bundle.pem")),
    # job_bundle_default_directory is an is_path setting: build the alternate
    # value with the native separator so it round-trips to itself on POSIX and
    # Windows alike (see the ca_bundle note above).
    (
        "settings.job_bundle_default_directory",
        "",
        os.path.join(os.sep, "my", "bundles"),
    ),
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
    config.clear_setting("defaults.aws_profile_name")
    assert config.get_setting("defaults.farm_id") == "farm-for-profile-default"
    # The queue id is still default
    assert config.get_setting("defaults.queue_id") == ""
    # The storage profile id is still default
    assert config.get_setting("settings.storage_profile_id") == ""

    # Switch back to the default farm
    config.clear_setting("defaults.farm_id")
    assert config.get_setting("defaults.queue_id") == "queue-for-farm-default"
    # Storage profile needs "profile - farm_id" so it should be back to the original
    assert config.get_setting("settings.storage_profile_id") == "storage-profile-for-farm-default"

    # Switch to default farm and default queue
    config.clear_setting("defaults.queue_id")
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


def test_config_clear_setting_nonexistant(fresh_deadline_config):
    """Test the error from clear_setting when a setting doesn't exist."""
    # Setting name without the '.'
    with pytest.raises(DeadlineOperationError) as excinfo:
        config.clear_setting("setting_name_bad_format")
    assert "is not valid" in str(excinfo.value)
    assert "setting_name_bad_format" in str(excinfo.value)

    # Section name is wrong
    with pytest.raises(DeadlineOperationError) as excinfo:
        config.clear_setting("setitngs.aws_profile_name")
    assert "has no setting" in str(excinfo.value)
    assert "setitngs" in str(excinfo.value)

    # Section is good, but no setting
    with pytest.raises(DeadlineOperationError) as excinfo:
        config.clear_setting("settings.aws_porfile_name")
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


def test_default_log_level(fresh_deadline_config):
    # To avoid excessive logging, the log level should not be DEBUG by default.
    assert config.get_setting("settings.log_level") != "DEBUG"
    # Verify the default log level exists and is less verbose than DEBUG
    assert config.get_setting("settings.log_level") == "WARNING"


@pytest.mark.skipif(
    platform.system() != "Windows",
    reason="This test is for testing file permission changes in Windows.",
)
def test_reset_directory_permissions_windows() -> None:
    """
    Asserts the _reset_directory_permissions_windows configures the provided
    folder with access only to the active user, the domain admin, and SYSTEM.
    """
    # GIVEN
    import ntsecuritycon
    import win32security

    path = Path(tempfile.gettempdir())
    system_sid = win32security.ConvertStringSidToSid("S-1-5-18")
    admin_sid = win32security.ConvertStringSidToSid("S-1-5-32-544")
    user_sid, _, _ = win32security.LookupAccountName(None, getpass.getuser())
    sids = [system_sid, admin_sid, user_sid]

    # WHEN
    config_file._reset_directory_permissions_windows(path)

    # THEN
    sd = win32security.GetFileSecurity(str(path.resolve()), win32security.DACL_SECURITY_INFORMATION)
    dacl = sd.GetSecurityDescriptorDacl()
    assert dacl.GetAceCount() == 3
    assert dacl.GetAclRevision() == win32security.ACL_REVISION
    for i in range(3):
        (acetype, aceflags), access, sid = dacl.GetAce(i)
        assert acetype == win32security.ACCESS_ALLOWED_ACE_TYPE
        assert aceflags == ntsecuritycon.OBJECT_INHERIT_ACE | ntsecuritycon.CONTAINER_INHERIT_ACE
        assert access == ntsecuritycon.FILE_ALL_ACCESS
        try:
            sids.remove(sid)
        except ValueError:
            assert False, f"Unexpected SID: {win32security.ConvertSidToStringSid(sid)}"


@pytest.mark.skipif(
    platform.system() != "Windows",
    reason="This test is for testing file permission changes in Windows.",
)
@patch.object(config_file, "get_config_file_path")
def test_write_config_directory_permission_windows(
    mock_get_config_file_path,
):
    """
    Tests that the config directory permissions are not modified when writing to the config file
    """
    # GIVEN
    path = Path(tempfile.gettempdir())
    config_path = path / "config"
    mock_get_config_file_path.return_value = config_path

    # ----------------------------------------------------------------------------------------------
    # Sets up a directory with an added full access entry for domain guests. Since this is not a
    # typically expected entry, it can be used to validate existing permissions were not overwritten
    import win32security
    import ntsecuritycon

    sd = win32security.GetFileSecurity(str(path.resolve()), win32security.DACL_SECURITY_INFORMATION)
    guest_sid = win32security.ConvertStringSidToSid("S-1-5-32-546")  # Domain Guests
    dacl = sd.GetSecurityDescriptorDacl()
    dacl.AddAccessAllowedAceEx(
        win32security.ACL_REVISION,
        ntsecuritycon.OBJECT_INHERIT_ACE | ntsecuritycon.CONTAINER_INHERIT_ACE,
        ntsecuritycon.FILE_ALL_ACCESS,
        guest_sid,
    )
    sd.SetSecurityDescriptorDacl(1, dacl, 0)
    win32security.SetFileSecurity(str(path.resolve()), win32security.DACL_SECURITY_INFORMATION, sd)
    # ----------------------------------------------------------------------------------------------

    # WHEN
    config_file.write_config(MagicMock())

    # THEN
    new_dacl = win32security.GetFileSecurity(
        str(path.resolve()), win32security.DACL_SECURITY_INFORMATION
    ).GetSecurityDescriptorDacl()

    new_dacl_aces = [new_dacl.GetAce(i) for i in range(new_dacl.GetAceCount())]
    dacl_aces = [dacl.GetAce(i) for i in range(dacl.GetAceCount())]

    # Assert the access control entries are identical
    assert new_dacl_aces == dacl_aces


@pytest.mark.skipif(
    platform.system() == "Windows",
    reason="This test is for testing file permission changes in POSIX.",
)
def test_posix_config_file_permissions(fresh_deadline_config) -> None:
    config_file_path = config_file.get_config_file_path()
    config_file_path.chmod(0o777)

    config_file.set_setting("defaults.aws_profile_name", "goodguyprofile")

    assert config_file_path.stat().st_mode & 0o777 == 0o600


def test_get_deadline_regions_env_override(fresh_deadline_config):
    """The DEADLINE_CLOUD_REGIONS env var takes top precedence."""
    # Even if a config setting says something else, the env var wins.
    config.set_setting("settings.deadline_regions", "ap-south-1")
    with patch.dict(os.environ, {"DEADLINE_CLOUD_REGIONS": "us-west-2, eu-west-1 ,us-west-2"}):
        result = config_file.get_deadline_regions()
    # De-duplicated, order-preserving, whitespace-stripped
    assert result == ["us-west-2", "eu-west-1"]


def test_get_deadline_regions_config_override(fresh_deadline_config):
    """The settings.deadline_regions config setting is used when the env var is not set."""
    config.set_setting("settings.deadline_regions", "eu-central-1, ca-central-1")
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("DEADLINE_CLOUD_REGIONS", None)
        result = config_file.get_deadline_regions()
    assert result == ["eu-central-1", "ca-central-1"]


def test_get_deadline_regions_honors_passed_config(fresh_deadline_config):
    """
    An explicit ``config`` argument's settings.deadline_regions is honored, not the global
    on-disk config. Pins the fix where get_deadline_regions ignored a passed-in config (so a
    CLI --profile override that lives only in memory was silently dropped from the fan-out).
    """
    from configparser import ConfigParser

    # Global/on-disk config says one thing...
    config.set_setting("settings.deadline_regions", "us-east-1")
    # ...but an in-memory config passed explicitly says another.
    in_memory = ConfigParser()
    config.set_setting("settings.deadline_regions", "eu-central-1, ca-central-1", config=in_memory)

    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("DEADLINE_CLOUD_REGIONS", None)
        result = config_file.get_deadline_regions(config=in_memory)

    # The passed config wins over the global one.
    assert result == ["eu-central-1", "ca-central-1"]


def test_get_deadline_regions_blank_overrides_fall_through(fresh_deadline_config):
    """Blank/whitespace-only env var and config setting fall through to the curated list."""
    config.set_setting("settings.deadline_regions", "   ")
    with patch.dict(os.environ, {"DEADLINE_CLOUD_REGIONS": "  "}):
        result = config_file.get_deadline_regions()
    assert result == config_file.DEADLINE_REGIONS


def test_get_deadline_regions_default_curated_list(fresh_deadline_config):
    """With no override set, the curated DEADLINE_REGIONS list is returned."""
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("DEADLINE_CLOUD_REGIONS", None)
        result = config_file.get_deadline_regions()
    assert result == config_file.DEADLINE_REGIONS
    # The returned value is a copy, not the module constant itself.
    assert result is not config_file.DEADLINE_REGIONS


def test_deadline_regions_matches_authoritative_aws_list():
    """
    Pins DEADLINE_REGIONS to the AWS Deadline Cloud "Service endpoints" table at
    https://docs.aws.amazon.com/general/latest/gr/deadlinecloud.html
    (commercial regions, reconciled 2026-06-08).

    If AWS adds/removes a Deadline Cloud region, update both that table's transcription
    here and the DEADLINE_REGIONS constant so the curated fallback doesn't drift (stale
    entries cause spurious endpoint-connect warnings during the list-farms fan-out).
    """
    expected = {
        "us-east-1",
        "us-east-2",
        "us-west-2",
        "ap-northeast-1",
        "ap-northeast-2",
        "ap-southeast-1",
        "ap-southeast-2",
        "eu-central-1",
        "eu-west-1",
        "eu-west-2",
    }
    assert set(config_file.DEADLINE_REGIONS) == expected
    # No duplicates in the curated list.
    assert len(config_file.DEADLINE_REGIONS) == len(set(config_file.DEADLINE_REGIONS))


def test_farm_region_depends_on_farm_id(fresh_deadline_config):
    """defaults.farm_region is stored per-farm (depends on defaults.farm_id)."""
    # Set a farm region for the default (empty) farm id.
    config.set_setting("defaults.farm_id", "farm-A")
    config.set_setting("defaults.farm_region", "us-west-2")
    assert config.get_setting("defaults.farm_region") == "us-west-2"

    # Switching to a different farm id yields the default (empty) region.
    config.set_setting("defaults.farm_id", "farm-B")
    assert config.get_setting("defaults.farm_region") == ""
    config.set_setting("defaults.farm_region", "eu-west-1")
    assert config.get_setting("defaults.farm_region") == "eu-west-1"

    # Switching back to farm-A restores its region.
    config.set_setting("defaults.farm_id", "farm-A")
    assert config.get_setting("defaults.farm_region") == "us-west-2"
