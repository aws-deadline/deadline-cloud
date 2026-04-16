# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests that path settings in the config file are normalized to use forward slashes
on disk (Windows only), preventing corruption when the config file is rewritten by
external tools (like Deadline Cloud Monitor) that may interpret backslashes as escape
characters.

On read, paths are converted back to native OS format (backslashes on Windows).
"""

import os
from configparser import ConfigParser

import pytest

from deadline.client import config
from deadline.client.config.config_file import (
    _normalize_path_for_config,
    _normalize_path_from_config,
)


class TestNormalizePathForConfig:
    """Tests for the _normalize_path_for_config helper (write direction)."""

    def test_forward_slashes_unchanged(self):
        assert _normalize_path_for_config("/usr/local/bin/monitor") == "/usr/local/bin/monitor"

    @pytest.mark.skipif(os.name != "nt", reason="Backslash normalization only applies on Windows")
    def test_backslashes_converted_on_windows(self):
        assert _normalize_path_for_config("C:\\Users\\artist\\assets") == "C:/Users/artist/assets"

    @pytest.mark.skipif(os.name != "nt", reason="Backslash normalization only applies on Windows")
    def test_mixed_slashes_normalized_on_windows(self):
        assert (
            _normalize_path_for_config("C:\\Users/artist\\projects") == "C:/Users/artist/projects"
        )

    @pytest.mark.skipif(os.name == "nt", reason="Backslashes are path separators on Windows")
    def test_backslashes_unchanged_on_posix(self):
        """On POSIX, backslashes are valid filename characters and should not be modified."""
        assert _normalize_path_for_config("path\\with\\backslashes") == "path\\with\\backslashes"

    def test_empty_string(self):
        assert _normalize_path_for_config("") == ""

    @pytest.mark.skipif(os.name != "nt", reason="UNC paths only apply on Windows")
    def test_unc_path(self):
        assert _normalize_path_for_config("\\\\server\\share\\folder") == "//server/share/folder"


class TestNormalizePathFromConfig:
    """Tests for the _normalize_path_from_config helper (read direction)."""

    @pytest.mark.skipif(os.name != "nt", reason="Only converts on Windows")
    def test_forward_slashes_become_backslashes_on_windows(self):
        assert _normalize_path_from_config("C:/Users/artist/assets") == "C:\\Users\\artist\\assets"

    @pytest.mark.skipif(os.name == "nt", reason="No conversion on POSIX")
    def test_forward_slashes_unchanged_on_posix(self):
        assert _normalize_path_from_config("/mnt/shared/assets") == "/mnt/shared/assets"

    def test_empty_string(self):
        assert _normalize_path_from_config("") == ""


class TestSetSettingPathNormalization:
    """Tests that set_setting stores forward slashes and get_setting returns native paths."""

    @pytest.mark.skipif(os.name != "nt", reason="Backslash normalization only applies on Windows")
    def test_monitor_path_roundtrip(self, fresh_deadline_config):
        """Backslash monitor path should roundtrip to native format on Windows."""
        config.set_setting(
            "deadline-cloud-monitor.path",
            "C:\\Program Files\\DeadlineCloudMonitor\\monitor.exe",
        )
        assert (
            config.get_setting("deadline-cloud-monitor.path")
            == "C:\\Program Files\\DeadlineCloudMonitor\\monitor.exe"
        )

    @pytest.mark.skipif(os.name != "nt", reason="Backslash normalization only applies on Windows")
    def test_job_history_dir_roundtrip(self, fresh_deadline_config):
        """Backslash job_history_dir should roundtrip to native format on Windows."""
        config.set_setting("settings.job_history_dir", "C:\\Users\\artist\\.deadline\\job_history")
        assert (
            config.get_setting("settings.job_history_dir")
            == "C:\\Users\\artist\\.deadline\\job_history"
        )

    @pytest.mark.skipif(os.name != "nt", reason="Backslash normalization only applies on Windows")
    def test_known_asset_paths_roundtrip(self, fresh_deadline_config):
        """Backslash known_asset_paths should roundtrip to native format on Windows."""
        paths = ";".join(["C:\\Users\\artist\\assets", "D:\\shared\\textures"])
        config.set_setting("settings.known_asset_paths", paths)
        result = config.get_setting("settings.known_asset_paths")
        assert result.split(";") == ["C:\\Users\\artist\\assets", "D:\\shared\\textures"]

    def test_known_asset_paths_forward_slashes_roundtrip(self, fresh_deadline_config):
        """Forward-slash paths should roundtrip to native format."""
        paths = os.pathsep.join(["/mnt/shared/assets", "/mnt/shared/textures"])
        config.set_setting("settings.known_asset_paths", paths)
        result = config.get_setting("settings.known_asset_paths")
        result_paths = result.split(os.pathsep)
        if os.name == "nt":
            assert result_paths == ["\\mnt\\shared\\assets", "\\mnt\\shared\\textures"]
        else:
            assert result_paths == ["/mnt/shared/assets", "/mnt/shared/textures"]

    def test_known_asset_paths_empty(self, fresh_deadline_config):
        """Empty known_asset_paths should remain empty."""
        config.set_setting("settings.known_asset_paths", "")
        assert config.get_setting("settings.known_asset_paths") == ""

    def test_non_path_setting_not_modified(self, fresh_deadline_config):
        """Non-path settings should not have slashes modified."""
        config.set_setting("defaults.aws_profile_name", "my\\profile")
        assert config.get_setting("defaults.aws_profile_name") == "my\\profile"

    def test_monitor_path_with_forward_slashes_roundtrip(self, fresh_deadline_config):
        """Forward-slash monitor paths should roundtrip to native format."""
        config.set_setting("deadline-cloud-monitor.path", "/usr/local/bin/DeadlineCloudMonitor")
        result = config.get_setting("deadline-cloud-monitor.path")
        if os.name == "nt":
            assert result == "\\usr\\local\\bin\\DeadlineCloudMonitor"
        else:
            assert result == "/usr/local/bin/DeadlineCloudMonitor"


class TestConfigFileStoredAsForwardSlashes:
    """
    Verify that the on-disk config file always contains forward slashes,
    which is the whole point of this fix — preventing corruption by external tools.
    """

    @pytest.mark.skipif(os.name != "nt", reason="Backslash normalization only applies on Windows")
    def test_known_paths_stored_as_forward_slashes(self, fresh_deadline_config):
        """Config file on disk should contain forward slashes, not backslashes."""
        config.set_setting(
            "settings.known_asset_paths",
            ";".join(["C:\\Users\\artist\\assets", "D:\\projects\\shared"]),
        )

        raw_config = ConfigParser()
        raw_config.read(fresh_deadline_config, encoding="utf8")
        raw_value = None
        for section in raw_config.sections():
            if raw_config.has_option(section, "known_asset_paths"):
                raw_value = raw_config.get(section, "known_asset_paths")
                break
        assert raw_value is not None, "known_asset_paths not found in config file"
        assert "\\" not in raw_value, f"Backslashes found in stored value: {raw_value}"
        assert "C:/Users/artist/assets" in raw_value

    @pytest.mark.skipif(os.name != "nt", reason="Backslash normalization only applies on Windows")
    def test_monitor_path_stored_as_forward_slashes(self, fresh_deadline_config):
        """Monitor path on disk should contain forward slashes."""
        config.set_setting(
            "deadline-cloud-monitor.path",
            "C:\\Program Files\\AWS\\DeadlineCloudMonitor\\monitor.exe",
        )

        raw_config = ConfigParser()
        raw_config.read(fresh_deadline_config, encoding="utf8")
        raw_value = None
        for section in raw_config.sections():
            if raw_config.has_option(section, "path"):
                raw_value = raw_config.get(section, "path")
                break
        assert raw_value is not None, "deadline-cloud-monitor.path not found in config file"
        assert "\\" not in raw_value, f"Backslashes found in stored value: {raw_value}"
        assert "C:/Program Files/AWS/DeadlineCloudMonitor/monitor.exe" == raw_value


class TestDefaultPathNormalization:
    """Verify that default values for path settings also get normalized."""

    def test_job_history_dir_default_is_native(self, fresh_deadline_config):
        """The default job_history_dir uses os.path.join which produces backslashes
        on Windows. get_setting should normalize it to native format consistently."""
        result = config.get_setting("settings.job_history_dir")
        assert ".deadline" in result
        assert "job_history" in result
        if os.name == "nt":
            # On Windows, the default should use backslashes
            assert "\\" in result
        else:
            # On POSIX, the default should use forward slashes
            assert "/" in result
