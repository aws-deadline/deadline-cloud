# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the deadline.client.api._update_checker module.
"""

import json
from unittest.mock import patch, MagicMock
import socket
import urllib.error

import pytest

from deadline.client.api._update_checker import (
    UpdateCheckStatus,
    safe_check_for_updates,
    get_current_platform,
    _fetch_manifest,
    DOWNLOAD_BASE_URL,
)


SAMPLE_MANIFEST = {
    "DeadlineCloudSubmitter": {
        "versions": {
            "latest": {
                "linux": {
                    "componentVersions": {
                        "deadline-cloud": "0.54.2",
                        "deadline-cloud-for-blender": "0.6.1",
                        "deadline-cloud-for-cinema-4d": "0.10.0",
                        "deadline-cloud-for-maya": "0.15.13",
                        "deadline-cloud-for-nuke": "0.18.16",
                    },
                    "installer": "/latest/linux/DeadlineCloudSubmitter-linux-x64-installer.run",
                    "sha256": "/latest/linux/DeadlineCloudSubmitter-linux-x64-installer.run.sha256",
                },
                "macos": {
                    "componentVersions": {
                        "deadline-cloud": "0.54.2",
                        "deadline-cloud-for-cinema-4d": "0.10.0",
                        "deadline-cloud-for-maya": "0.15.13",
                        "deadline-cloud-for-nuke": "0.18.16",
                    },
                    "installer": "/latest/macos/DeadlineCloudSubmitter-osx-installer.app.zip",
                    "sha256": "/latest/macos/DeadlineCloudSubmitter-osx-installer.app.zip.sha256",
                },
                "windows": {
                    "componentVersions": {
                        "deadline-cloud": "0.54.2",
                        "deadline-cloud-for-cinema-4d": "0.10.0",
                        "deadline-cloud-for-maya": "0.15.13",
                        "deadline-cloud-for-nuke": "0.18.16",
                    },
                    "installer": "/latest/windows/DeadlineCloudSubmitter-windows-x64-installer.exe",
                    "sha256": "/latest/windows/DeadlineCloudSubmitter-windows-x64-installer.exe.sha256",
                },
            }
        }
    }
}


class TestGetCurrentPlatform:
    """Tests for get_current_platform()."""

    @pytest.mark.parametrize(
        "sys_platform, expected",
        [
            ("linux", "linux"),
            ("linux2", "linux"),
            ("darwin", "macos"),
            ("win32", "windows"),
            ("cygwin", "windows"),
            ("freebsd", "linux"),  # unknown defaults to linux
        ],
    )
    def test_platform_detection(self, sys_platform, expected):
        with patch("deadline.client.api._update_checker.sys") as mock_sys:
            mock_sys.platform = sys_platform
            assert get_current_platform() == expected


def _mock_urlopen(manifest_data):
    """Helper to create a mock urlopen context manager returning manifest JSON."""
    mock_resp = MagicMock()
    mock_resp.read.return_value = json.dumps(manifest_data).encode("utf-8")
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)
    return mock_resp


PLATFORM_INSTALLER_URLS = {
    "linux": f"{DOWNLOAD_BASE_URL}/latest/linux/DeadlineCloudSubmitter-linux-x64-installer.run",
    "macos": f"{DOWNLOAD_BASE_URL}/latest/macos/DeadlineCloudSubmitter-osx-installer.app.zip",
    "windows": f"{DOWNLOAD_BASE_URL}/latest/windows/DeadlineCloudSubmitter-windows-x64-installer.exe",
}


class TestFetchManifest:
    """Tests for _fetch_manifest() SSL context behavior."""

    @patch("deadline.client.api._update_checker.urllib.request.urlopen")
    def test_success_default_ssl(self, mock_urlopen_fn):
        """When default SSL works, no fallback is needed."""
        mock_urlopen_fn.return_value = _mock_urlopen(SAMPLE_MANIFEST)

        result = _fetch_manifest()

        assert result == SAMPLE_MANIFEST
        assert mock_urlopen_fn.call_count == 1

    @patch(
        "deadline.client.api._update_checker._get_botocore_ca_bundle",
        return_value="/fake/cacert.pem",
    )
    @patch("deadline.client.api._update_checker.ssl.create_default_context")
    @patch("deadline.client.api._update_checker.urllib.request.urlopen")
    @patch("deadline.client.api._update_checker.sys")
    def test_macos_falls_back_to_bundled_ca(
        self, mock_sys, mock_urlopen_fn, mock_ssl_ctx, mock_ca_bundle
    ):
        """On macOS, when default SSL fails, retries with botocore's CA bundle."""
        mock_sys.platform = "darwin"
        # First call fails with SSL error, second succeeds with botocore CA bundle
        mock_urlopen_fn.side_effect = [
            urllib.error.URLError("SSL: CERTIFICATE_VERIFY_FAILED"),
            _mock_urlopen(SAMPLE_MANIFEST),
        ]

        result = _fetch_manifest()

        assert result == SAMPLE_MANIFEST
        assert mock_urlopen_fn.call_count == 2
        # Second call should have an explicit SSL context
        second_call_kwargs = mock_urlopen_fn.call_args_list[1]
        ctx = second_call_kwargs.kwargs.get("context") or second_call_kwargs[1].get("context")
        assert ctx is not None
        # Verify botocore CA bundle was used
        mock_ca_bundle.assert_called_once()
        mock_ssl_ctx.return_value.load_verify_locations.assert_called_once_with("/fake/cacert.pem")

    @patch("deadline.client.api._update_checker.urllib.request.urlopen")
    @patch("deadline.client.api._update_checker.sys")
    def test_non_macos_does_not_fallback(self, mock_sys, mock_urlopen_fn):
        """On non-macOS, SSL errors are raised without fallback."""
        mock_sys.platform = "linux"
        mock_urlopen_fn.side_effect = urllib.error.URLError("SSL: CERTIFICATE_VERIFY_FAILED")

        with pytest.raises(urllib.error.URLError):
            _fetch_manifest()

        assert mock_urlopen_fn.call_count == 1

    @patch(
        "deadline.client.api._update_checker._get_botocore_ca_bundle",
        return_value="/fake/cacert.pem",
    )
    @patch("deadline.client.api._update_checker.ssl.create_default_context")
    @patch("deadline.client.api._update_checker.urllib.request.urlopen")
    @patch("deadline.client.api._update_checker.sys")
    def test_macos_fallback_also_fails(
        self, mock_sys, mock_urlopen_fn, mock_ssl_ctx, mock_ca_bundle
    ):
        """On macOS, if both attempts fail, the error propagates."""
        mock_sys.platform = "darwin"
        mock_urlopen_fn.side_effect = urllib.error.URLError("SSL error")

        with pytest.raises(urllib.error.URLError):
            _fetch_manifest()

        assert mock_urlopen_fn.call_count == 2


class TestCheckForUpdates:
    """Tests for safe_check_for_updates()."""

    @pytest.mark.parametrize("platform", ["linux", "macos", "windows"])
    @patch("deadline.client.api._update_checker._fetch_manifest")
    def test_update_available(self, mock_fetch, platform):
        mock_fetch.return_value = SAMPLE_MANIFEST

        with patch(
            "deadline.client.api._update_checker.get_current_platform", return_value=platform
        ):
            result = safe_check_for_updates("deadline-cloud-for-cinema-4d", "0.9.1")

        assert result.status == UpdateCheckStatus.SUCCESS
        assert result.update_available is True
        assert result.current_version == "0.9.1"
        assert result.latest_version == "0.10.0"
        assert result.download_url == PLATFORM_INSTALLER_URLS[platform]

    @pytest.mark.parametrize("platform", ["linux", "macos", "windows"])
    @patch("deadline.client.api._update_checker._fetch_manifest")
    def test_no_update_available(self, mock_fetch, platform):
        mock_fetch.return_value = SAMPLE_MANIFEST

        with patch(
            "deadline.client.api._update_checker.get_current_platform", return_value=platform
        ):
            result = safe_check_for_updates("deadline-cloud-for-cinema-4d", "0.10.0")

        assert result.status == UpdateCheckStatus.SUCCESS
        assert result.update_available is False
        assert result.latest_version == "0.10.0"

    @pytest.mark.parametrize("platform", ["linux", "macos", "windows"])
    @patch("deadline.client.api._update_checker._fetch_manifest")
    def test_current_version_newer_than_manifest(self, mock_fetch, platform):
        mock_fetch.return_value = SAMPLE_MANIFEST

        with patch(
            "deadline.client.api._update_checker.get_current_platform", return_value=platform
        ):
            result = safe_check_for_updates("deadline-cloud-for-cinema-4d", "1.0.0")

        assert result.status == UpdateCheckStatus.SUCCESS
        assert result.update_available is False

    @patch("deadline.client.api._update_checker._fetch_manifest")
    def test_network_error(self, mock_fetch):
        mock_fetch.side_effect = urllib.error.URLError("Connection refused")

        result = safe_check_for_updates("deadline-cloud-for-cinema-4d", "0.9.1")

        assert result.status == UpdateCheckStatus.NETWORK_ERROR
        assert result.update_available is False
        assert result.error_message is not None
        assert "Network error" in result.error_message

    @patch("deadline.client.api._update_checker._fetch_manifest")
    def test_timeout_error(self, mock_fetch):
        mock_fetch.side_effect = TimeoutError()

        result = safe_check_for_updates("deadline-cloud-for-cinema-4d", "0.9.1")

        assert result.status == UpdateCheckStatus.TIMEOUT_ERROR
        assert result.update_available is False

    @patch("deadline.client.api._update_checker._fetch_manifest")
    def test_socket_timeout_error(self, mock_fetch):
        mock_fetch.side_effect = socket.timeout("timed out")

        result = safe_check_for_updates("deadline-cloud-for-cinema-4d", "0.9.1")

        assert result.status == UpdateCheckStatus.TIMEOUT_ERROR
        assert result.update_available is False

    @patch("deadline.client.api._update_checker._fetch_manifest")
    def test_parse_error(self, mock_fetch):
        mock_fetch.side_effect = json.JSONDecodeError("bad json", "", 0)

        result = safe_check_for_updates("deadline-cloud-for-cinema-4d", "0.9.1")

        assert result.status == UpdateCheckStatus.PARSE_ERROR
        assert result.update_available is False

    @patch("deadline.client.api._update_checker._fetch_manifest")
    def test_unexpected_exception(self, mock_fetch):
        """Unexpected exceptions are caught gracefully and don't crash the caller."""
        mock_fetch.side_effect = RuntimeError("something totally unexpected")

        result = safe_check_for_updates("deadline-cloud-for-cinema-4d", "0.9.1")

        assert result.status == UpdateCheckStatus.UNEXPECTED_ERROR
        assert result.update_available is False
        assert result.error_message is not None
        assert "Unexpected error" in result.error_message

    @pytest.mark.parametrize("platform", ["linux", "macos", "windows"])
    @patch("deadline.client.api._update_checker._fetch_manifest")
    def test_integration_not_found(self, mock_fetch, platform):
        mock_fetch.return_value = SAMPLE_MANIFEST

        with patch(
            "deadline.client.api._update_checker.get_current_platform", return_value=platform
        ):
            result = safe_check_for_updates("deadline-cloud-for-houdini", "1.0.0")

        assert result.status == UpdateCheckStatus.INTEGRATION_NOT_FOUND
        assert result.update_available is False
        assert result.error_message is not None
        assert "not found" in result.error_message

    @patch("deadline.client.api._update_checker.get_current_platform", return_value="unknown_os")
    @patch("deadline.client.api._update_checker._fetch_manifest")
    def test_platform_not_in_manifest(self, mock_fetch, mock_platform):
        mock_fetch.return_value = SAMPLE_MANIFEST

        result = safe_check_for_updates("deadline-cloud-for-cinema-4d", "0.9.1")

        assert result.status == UpdateCheckStatus.PARSE_ERROR
        assert result.error_message is not None
        assert "not found in manifest" in result.error_message

    @patch("deadline.client.api._update_checker.get_current_platform", return_value="macos")
    @patch("deadline.client.api._update_checker._fetch_manifest")
    def test_invalid_version_in_manifest(self, mock_fetch, mock_platform):
        bad_manifest = {
            "DeadlineCloudSubmitter": {
                "versions": {
                    "latest": {
                        "macos": {
                            "componentVersions": {
                                "deadline-cloud-for-cinema-4d": "not.a.version!",
                            },
                        }
                    }
                }
            }
        }
        mock_fetch.return_value = bad_manifest

        result = safe_check_for_updates("deadline-cloud-for-cinema-4d", "0.9.1")

        assert result.status == UpdateCheckStatus.INVALID_VERSION
        assert result.update_available is False

    @patch("deadline.client.api._update_checker.get_current_platform", return_value="macos")
    @patch("deadline.client.api._update_checker._fetch_manifest")
    def test_missing_installer_key_returns_no_download_url(self, mock_fetch, mock_platform):
        manifest_without_installer = {
            "DeadlineCloudSubmitter": {
                "versions": {
                    "latest": {
                        "macos": {
                            "componentVersions": {
                                "deadline-cloud-for-cinema-4d": "0.10.0",
                            },
                        }
                    }
                }
            }
        }
        mock_fetch.return_value = manifest_without_installer

        result = safe_check_for_updates("deadline-cloud-for-cinema-4d", "0.9.1")

        assert result.status == UpdateCheckStatus.SUCCESS
        assert result.update_available is False
        assert result.download_url is None

    @pytest.mark.parametrize("platform", ["linux", "macos", "windows"])
    @patch("deadline.client.api._update_checker._fetch_manifest")
    def test_invalid_current_version_returns_error(self, mock_fetch, platform):
        mock_fetch.return_value = SAMPLE_MANIFEST

        with patch(
            "deadline.client.api._update_checker.get_current_platform", return_value=platform
        ):
            result = safe_check_for_updates("deadline-cloud-for-cinema-4d", "bad-version")

        assert result.status == UpdateCheckStatus.INVALID_VERSION
        assert result.current_version == "bad-version"
        assert result.error_message is not None and "Invalid version" in result.error_message


class TestUncommonVersionFormats:
    """Tests for uncommon or malformed current_version strings from DCCs."""

    @pytest.mark.parametrize(
        "current_version, expect_update",
        [
            # PEP 440 local version segment (e.g. dev builds like 1.2.3+patch1dkj3k)
            ("0.9.0+patch1dkj3k", True),
            ("0.10.0+patch1dkj3k", False),  # local segments are ignored in comparison
            ("0.11.0+local", False),
            # Pre-release versions
            ("0.10.0a1", True),  # pre-release of 0.10.0 is less than 0.10.0
            ("0.10.0rc1", True),
            # Dev versions
            ("0.10.0.dev1", True),  # dev release of 0.10.0 is less than 0.10.0
            # Post-release
            ("0.10.0.post1", False),  # post-release of 0.10.0 is greater than 0.10.0
            ("0.9.0.post1", True),
            # Leading 'v' prefix — packaging normalizes this to a valid version
            ("v0.9.0", True),
            ("v0.10.0", False),
        ],
    )
    @patch("deadline.client.api._update_checker.get_current_platform", return_value="macos")
    @patch("deadline.client.api._update_checker._fetch_manifest")
    def test_uncommon_dcc_version_formats(
        self, mock_fetch, mock_platform, current_version, expect_update
    ):
        """DCC integrations may report unusual but valid PEP 440 versions."""
        mock_fetch.return_value = SAMPLE_MANIFEST

        result = safe_check_for_updates("deadline-cloud-for-cinema-4d", current_version)

        assert result.status == UpdateCheckStatus.SUCCESS
        assert result.update_available is expect_update

    @pytest.mark.parametrize(
        "malformed_version",
        [
            "not-a-version",
            "abc.def.ghi",
            "",
            "1.2.3+patch1dkj3k+extra",  # double local segment
        ],
    )
    @patch("deadline.client.api._update_checker.get_current_platform", return_value="macos")
    @patch("deadline.client.api._update_checker._fetch_manifest")
    def test_malformed_dcc_version(self, mock_fetch, mock_platform, malformed_version):
        """Malformed version strings from DCCs should return INVALID_VERSION, not crash."""
        mock_fetch.return_value = SAMPLE_MANIFEST

        result = safe_check_for_updates("deadline-cloud-for-cinema-4d", malformed_version)

        assert result.status == UpdateCheckStatus.INVALID_VERSION
        assert result.update_available is False
        assert result.error_message is not None
        assert result.error_message is not None


class TestConfigOptOut:
    """Tests for the settings.submitter_update_notification opt-out."""

    @patch("deadline.client.api._update_checker._fetch_manifest")
    def test_notification_suppressed(self, mock_fetch, fresh_deadline_config):
        """When submitter_update_notification is false, check is skipped."""
        from deadline.client.config.config_file import set_setting

        set_setting("settings.submitter_update_notification", "false")

        result = safe_check_for_updates("deadline-cloud-for-cinema-4d", "0.9.1")

        assert result.status == UpdateCheckStatus.SUCCESS
        assert result.update_available is False
        mock_fetch.assert_not_called()

    @patch("deadline.client.api._update_checker.get_current_platform", return_value="macos")
    @patch("deadline.client.api._update_checker._fetch_manifest")
    def test_notification_enabled_by_default(
        self, mock_fetch, mock_platform, fresh_deadline_config
    ):
        """When submitter_update_notification is default (true), check proceeds."""
        mock_fetch.return_value = SAMPLE_MANIFEST

        result = safe_check_for_updates("deadline-cloud-for-cinema-4d", "0.9.1")

        assert result.status == UpdateCheckStatus.SUCCESS
        assert result.update_available is True
        mock_fetch.assert_called_once()
