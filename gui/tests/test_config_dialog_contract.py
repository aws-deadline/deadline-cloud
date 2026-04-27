"""Tests for the config shim's in-memory vs disk behavior.

These test the behavioral contract that the config dialog depends on:
- read_config() returns a ConfigParser
- set_setting with config= mutates in-memory only
- set_setting without config= writes to disk
- get_setting with config= reads from in-memory
- The dialog's apply/cancel semantics work correctly

Uses the ffi-test-server stub. No real AWS calls.
"""

import os

import pytest
from configparser import ConfigParser

from deadline._native import (
    get_setting as _native_get_setting,
    set_setting as _native_set_setting,
    read_config as _native_read_config,
)
from deadline.client.config import config_file


class TestReadConfig:
    """read_config() returns a ConfigParser populated from disk."""

    @pytest.fixture(autouse=True)
    def _use_config(self, test_server, tmp_path):
        config = tmp_path / "config"
        config.write_text(
            "[defaults]\naws_profile_name = testprofile\n\n"
            "[profile-testprofile defaults]\nfarm_id = farm-aaa\n\n"
            "[profile-testprofile settings]\nlog_level = DEBUG\n\n"
            "[settings]\nauto_accept = true\n"
        )
        self._old = os.environ.get("DEADLINE_CONFIG_FILE_PATH")
        os.environ["DEADLINE_CONFIG_FILE_PATH"] = str(config)
        yield
        if self._old is None:
            os.environ.pop("DEADLINE_CONFIG_FILE_PATH", None)
        else:
            os.environ["DEADLINE_CONFIG_FILE_PATH"] = self._old

    def test_returns_configparser(self):
        result = config_file.read_config()
        assert isinstance(result, ConfigParser)

    def test_has_sections(self):
        result = config_file.read_config()
        assert "defaults" in result.sections()
        assert "profile-testprofile defaults" in result.sections()

    def test_get_setting_reads_from_configparser(self):
        cp = config_file.read_config()
        assert config_file.get_setting("defaults.aws_profile_name", config=cp) == "testprofile"

    def test_get_setting_profile_scoped(self):
        cp = config_file.read_config()
        assert config_file.get_setting("defaults.farm_id", config=cp) == "farm-aaa"

    def test_get_setting_settings_section_profile_scoped(self):
        cp = config_file.read_config()
        assert config_file.get_setting("settings.log_level", config=cp) == "DEBUG"

    def test_get_setting_settings_section_global_fallback(self):
        cp = config_file.read_config()
        assert config_file.get_setting("settings.auto_accept", config=cp) == "true"

    def test_get_setting_missing_returns_empty(self):
        cp = config_file.read_config()
        assert config_file.get_setting("defaults.queue_id", config=cp) == ""

    def test_get_setting_falls_back_to_ffi_default(self):
        """Settings with computed defaults (e.g. job_history_dir) return the
        FFI default when not explicitly set in the config file."""
        cp = config_file.read_config()
        val = config_file.get_setting("settings.job_history_dir", config=cp)
        assert val != ""
        assert "job_history" in val


class TestSetSettingInMemory:
    """set_setting with config= mutates in-memory only, no disk write."""

    @pytest.fixture(autouse=True)
    def _use_config(self, test_server, tmp_path):
        self.config_path = tmp_path / "config"
        self.config_path.write_text(
            "[defaults]\naws_profile_name = myprofile\n\n"
            "[profile-myprofile defaults]\nfarm_id = farm-original\n"
        )
        self._old = os.environ.get("DEADLINE_CONFIG_FILE_PATH")
        os.environ["DEADLINE_CONFIG_FILE_PATH"] = str(self.config_path)
        yield
        if self._old is None:
            os.environ.pop("DEADLINE_CONFIG_FILE_PATH", None)
        else:
            os.environ["DEADLINE_CONFIG_FILE_PATH"] = self._old

    def test_mutates_configparser(self):
        cp = config_file.read_config()
        config_file.set_setting("defaults.farm_id", "farm-changed", config=cp)
        assert config_file.get_setting("defaults.farm_id", config=cp) == "farm-changed"

    def test_does_not_write_to_disk(self):
        cp = config_file.read_config()
        config_file.set_setting("defaults.farm_id", "farm-changed", config=cp)
        # Disk still has original value
        assert _native_get_setting("defaults.farm_id") == "farm-original"

    def test_set_aws_profile_name_in_memory(self):
        cp = config_file.read_config()
        config_file.set_setting("defaults.aws_profile_name", "newprofile", config=cp)
        assert config_file.get_setting("defaults.aws_profile_name", config=cp) == "newprofile"
        # Disk unchanged
        assert _native_get_setting("defaults.aws_profile_name") == "myprofile"

    def test_set_creates_section_if_missing(self):
        cp = config_file.read_config()
        config_file.set_setting("settings.log_level", "INFO", config=cp)
        assert config_file.get_setting("settings.log_level", config=cp) == "INFO"


class TestSetSettingToDisk:
    """set_setting without config= writes to disk via FFI."""

    @pytest.fixture(autouse=True)
    def _use_config(self, test_server, tmp_path):
        self.config_path = tmp_path / "config"
        self.config_path.write_text(
            "[defaults]\naws_profile_name = diskprofile\n\n"
            "[profile-diskprofile defaults]\nfarm_id = farm-disk\n"
        )
        self._old = os.environ.get("DEADLINE_CONFIG_FILE_PATH")
        os.environ["DEADLINE_CONFIG_FILE_PATH"] = str(self.config_path)
        yield
        if self._old is None:
            os.environ.pop("DEADLINE_CONFIG_FILE_PATH", None)
        else:
            os.environ["DEADLINE_CONFIG_FILE_PATH"] = self._old

    def test_writes_to_disk(self):
        config_file.set_setting("defaults.farm_id", "farm-new")
        assert _native_get_setting("defaults.farm_id") == "farm-new"

    def test_readable_after_write(self):
        config_file.set_setting("defaults.farm_id", "farm-persisted")
        cp = config_file.read_config()
        assert config_file.get_setting("defaults.farm_id", config=cp) == "farm-persisted"


class TestDialogApplyCancelSemantics:
    """Simulates the config dialog's apply/cancel workflow."""

    @pytest.fixture(autouse=True)
    def _use_config(self, test_server, tmp_path):
        self.config_path = tmp_path / "config"
        self.config_path.write_text(
            "[defaults]\naws_profile_name = (default)\n\n"
            "[profile-(default) defaults]\nfarm_id = farm-orig\n\n"
            "[profile-(default) farm-orig defaults]\nqueue_id = queue-orig\n\n"
            "[settings]\nauto_accept = false\n"
        )
        self._old = os.environ.get("DEADLINE_CONFIG_FILE_PATH")
        os.environ["DEADLINE_CONFIG_FILE_PATH"] = str(self.config_path)
        yield
        if self._old is None:
            os.environ.pop("DEADLINE_CONFIG_FILE_PATH", None)
        else:
            os.environ["DEADLINE_CONFIG_FILE_PATH"] = self._old

    def test_cancel_discards_changes(self):
        """Simulate: user changes farm, then cancels."""
        # Load working copy
        working = config_file.read_config()
        # User changes farm in-memory
        config_file.set_setting("defaults.farm_id", "farm-new", config=working)
        assert config_file.get_setting("defaults.farm_id", config=working) == "farm-new"
        # Cancel: discard working copy, re-read from disk
        working = config_file.read_config()
        assert config_file.get_setting("defaults.farm_id", config=working) == "farm-orig"

    def test_apply_persists_changes(self):
        """Simulate: user changes farm, then applies."""
        # Load working copy
        working = config_file.read_config()
        changes = {"defaults.farm_id": "farm-applied"}
        # Apply: write each change to in-memory AND disk
        for name, val in changes.items():
            config_file.set_setting(name, val, config=working)
            config_file.set_setting(name, val)  # persist
        # Verify disk
        assert _native_get_setting("defaults.farm_id") == "farm-applied"
        # Verify fresh read
        fresh = config_file.read_config()
        assert config_file.get_setting("defaults.farm_id", config=fresh) == "farm-applied"

    def test_multiple_changes_apply(self):
        """Multiple settings changed then applied together."""
        working = config_file.read_config()
        changes = {
            "defaults.farm_id": "farm-multi",
            "defaults.queue_id": "queue-multi",
            "settings.auto_accept": "true",
        }
        for name, val in changes.items():
            config_file.set_setting(name, val, config=working)
            config_file.set_setting(name, val)
        assert _native_get_setting("defaults.farm_id") == "farm-multi"
        assert _native_get_setting("defaults.queue_id") == "queue-multi"
        assert _native_get_setting("settings.auto_accept") == "true"

    def test_refresh_does_not_write_disk(self):
        """Simulates refresh() which applies changes to in-memory only."""
        working = config_file.read_config()
        # Simulate multiple refreshes with pending changes
        for _ in range(5):
            config_file.set_setting("defaults.farm_id", "farm-pending", config=working)
            config_file.set_setting("defaults.queue_id", "queue-pending", config=working)
        # Disk must be unchanged
        assert _native_get_setting("defaults.farm_id") == "farm-orig"
        assert _native_get_setting("defaults.queue_id") == "queue-orig"
