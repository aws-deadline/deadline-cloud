# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Update checker module for Deadline Cloud integrations.

This module provides functionality to check if a newer version of a Deadline Cloud
integration is available by comparing the installed version against a remote manifest.
"""

from __future__ import annotations

__all__ = [
    "MANIFEST_URL",
    "DOWNLOAD_BASE_URL",
    "UpdateCheckStatus",
    "UpdateCheckResult",
    "safe_check_for_updates",
    "get_current_platform",
]

import json
import logging
import os
import socket
import ssl
import sys
import urllib.request
import urllib.error
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional
import botocore
from packaging.version import Version, InvalidVersion

from ..config import config_file

logger = logging.getLogger(__name__)

MANIFEST_URL = "https://downloads.deadlinecloud.amazonaws.com/submitters/manifest.json"
MANIFEST_TIMEOUT_SECONDS = 5
# Base URL for installer downloads, derived from the manifest URL's parent path
DOWNLOAD_BASE_URL = MANIFEST_URL.rsplit("/", 1)[0]


def _get_botocore_ca_bundle() -> str:
    """Return the path to botocore's bundled CA certificate bundle.

    botocore (installed via boto3) ships a full CA bundle at
    ``botocore/cacert.pem`` that includes Amazon Root CA 1 along with
    ~130 other root certificates.  Using this avoids the need to bundle
    our own PEM file.
    """
    return os.path.join(os.path.dirname(botocore.__file__), "cacert.pem")


class UpdateCheckStatus(Enum):
    """Status of the update check operation."""

    SUCCESS = "success"
    NETWORK_ERROR = "network_error"
    TIMEOUT_ERROR = "timeout_error"
    PARSE_ERROR = "parse_error"
    INVALID_VERSION = "invalid_version"
    INTEGRATION_NOT_FOUND = "integration_not_found"
    UNEXPECTED_ERROR = "unexpected_error"


@dataclass
class UpdateCheckResult:
    """Result of an update check operation.

    Attributes:
        status: The status of the update check operation.
        update_available: Whether an update is available. Always False on error.
        current_version: The currently installed version (from input).
        latest_version: The latest version from the manifest (if available).
        download_url: URL to download the latest submitter (if available).
        error_message: Human-readable error description (if an error occurred).
    """

    status: UpdateCheckStatus
    current_version: str
    update_available: bool = False
    latest_version: Optional[str] = None
    download_url: Optional[str] = None
    error_message: Optional[str] = None


def get_current_platform() -> str:
    """
    Detect the current operating system.

    Returns:
        "linux", "macos", or "windows" based on the current platform.
    """
    platform = sys.platform
    if platform.startswith("linux"):
        return "linux"
    elif platform == "darwin":
        return "macos"
    elif platform == "win32" or platform == "cygwin":
        return "windows"
    else:
        # Default to linux for unknown platforms
        return "linux"


def _is_update_notification_enabled() -> bool:
    """Check whether the update notification is enabled in config."""
    return config_file.str2bool(config_file.get_setting("settings.submitter_update_notification"))


def _fetch_manifest() -> Dict[str, Any]:
    """Fetch and parse the remote manifest JSON.

    On macOS, bundled Python environments (e.g. inside Cinema 4D) often
    cannot locate the system CA certificate store.  When the default SSL
    context fails, this function retries using botocore's bundled CA
    certificate bundle (which includes Amazon Root CA 1) so that the
    connection is still fully verified.

    Raises:
        urllib.error.URLError: On network errors.
        TimeoutError: On request timeout.
        socket.timeout: On socket-level timeout.
        json.JSONDecodeError: On invalid JSON.
        UnicodeDecodeError: On encoding errors.
    """
    req = urllib.request.Request(MANIFEST_URL)

    try:
        with urllib.request.urlopen(req, timeout=MANIFEST_TIMEOUT_SECONDS) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.URLError:
        if sys.platform != "darwin":
            raise

    # macOS fallback: retry with botocore's bundled CA certificate bundle.
    logger.debug("Default SSL verification failed on macOS, retrying with botocore CA bundle")
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.load_verify_locations(_get_botocore_ca_bundle())
    with urllib.request.urlopen(req, timeout=MANIFEST_TIMEOUT_SECONDS, context=ctx) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _get_platform_data(manifest: Dict[str, Any], platform: str) -> Optional[Dict[str, Any]]:
    """Extract platform-specific data from the manifest.

    Returns:
        The platform data dict, or None if the platform is not found.
    """
    try:
        return manifest["DeadlineCloudSubmitter"]["versions"]["latest"][platform]
    except (KeyError, TypeError):
        return None


def _build_download_url(platform_data: Dict[str, Any]) -> Optional[str]:
    """Build the full download URL from the installer path in platform data."""
    installer_path = platform_data.get("installer")
    if not installer_path:
        return None
    separator = "" if installer_path.startswith("/") else "/"
    return f"{DOWNLOAD_BASE_URL}{separator}{installer_path}"


def _compare_versions(current_version: str, latest_version_str: str) -> bool:
    """Compare current and latest version strings.

    Returns:
        True if the latest version is newer than the current version.

    Raises:
        InvalidVersion: If either version string is not a valid PEP 440 version.
    """
    return Version(latest_version_str) > Version(current_version)


def safe_check_for_updates(
    integration_name: str,
    current_version: str,
) -> UpdateCheckResult:
    """
    Check if a newer version of a Deadline Cloud integration is available.

    This is a *safe* wrapper that never raises exceptions.  All errors are
    captured and returned as an :class:`UpdateCheckResult` with the
    appropriate :class:`UpdateCheckStatus` and ``error_message``, so callers
    can use this in startup paths without risk of crashing the host
    application.

    Fetches the remote manifest and compares the installed version against
    the latest version listed for the current platform.

    If the ``settings.submitter_update_notification`` config value is
    ``"false"``, the check is skipped and a result with
    ``update_available=False`` is returned immediately.

    Args:
        integration_name: Package name of the integration as it appears in the
            manifest (e.g., "deadline-cloud-for-cinema-4d").
        current_version: The currently installed version string (e.g., "0.9.2").

    Returns:
        An UpdateCheckResult describing whether an update is available.
    """
    try:
        if not _is_update_notification_enabled():
            return UpdateCheckResult(
                status=UpdateCheckStatus.SUCCESS,
                update_available=False,
                current_version=current_version,
            )

        platform = get_current_platform()

        # Fetch the manifest
        try:
            manifest = _fetch_manifest()
        except urllib.error.URLError as e:
            return UpdateCheckResult(
                status=UpdateCheckStatus.NETWORK_ERROR,
                current_version=current_version,
                error_message=f"Network error: {e}",
            )
        except (TimeoutError, socket.timeout):
            return UpdateCheckResult(
                status=UpdateCheckStatus.TIMEOUT_ERROR,
                current_version=current_version,
                error_message="Request timed out",
            )
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            return UpdateCheckResult(
                status=UpdateCheckStatus.PARSE_ERROR,
                current_version=current_version,
                error_message=f"Failed to parse manifest: {e}",
            )

        # Resolve platform data
        platform_data = _get_platform_data(manifest, platform)
        if platform_data is None:
            return UpdateCheckResult(
                status=UpdateCheckStatus.PARSE_ERROR,
                current_version=current_version,
                error_message=f"Platform '{platform}' not found in manifest",
            )

        # Look up the integration version
        latest_version_str = platform_data.get("componentVersions", {}).get(integration_name)
        if latest_version_str is None:
            return UpdateCheckResult(
                status=UpdateCheckStatus.INTEGRATION_NOT_FOUND,
                current_version=current_version,
                error_message=f"Integration '{integration_name}' not found in manifest",
            )

        try:
            update_available = _compare_versions(current_version, latest_version_str)
        except InvalidVersion as e:
            return UpdateCheckResult(
                status=UpdateCheckStatus.INVALID_VERSION,
                current_version=current_version,
                latest_version=latest_version_str,
                error_message=f"Invalid version: {e}",
            )

        download_url = _build_download_url(platform_data)

        if update_available and not download_url:
            logger.warning(
                "Newer version %s found but no installer URL in manifest — skipping notification",
                latest_version_str,
            )
            update_available = False

        return UpdateCheckResult(
            status=UpdateCheckStatus.SUCCESS,
            update_available=update_available,
            current_version=current_version,
            latest_version=latest_version_str,
            download_url=download_url,
        )
    except Exception as e:
        # Top-level safety net: guarantee this function never raises.
        logger.warning("Unexpected error during update check: %s", e, exc_info=True)
        return UpdateCheckResult(
            status=UpdateCheckStatus.UNEXPECTED_ERROR,
            current_version=current_version,
            error_message=f"Unexpected error: {e}",
        )
