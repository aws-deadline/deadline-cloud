# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Internal runtime environment information.

This module provides utilities for automatically collecting runtime environment
version information including the deadline library, dependencies, Python runtime,
operating system, and Qt framework.
"""

from __future__ import annotations

import logging
import platform
import re
from dataclasses import dataclass
from importlib.metadata import requires, version as package_version

logger = logging.getLogger(__name__)


@dataclass
class _EnvironmentInfo:
    """
    Container for auto-collected runtime environment information.

    This is a private dataclass used internally by _AboutDialog to hold metadata
    about the runtime environment including the deadline library, dependencies,
    Python runtime, operating system, and Qt framework.

    Attributes:
        deadline_dep_versions: Dictionary of deadline dependency package names to versions
        os_name: Name of the operating system
        os_version: Operating system version
        os_architecture: OS architecture (e.g., "x86_64", "arm64")
        python_version: Python runtime version
        qt_version: Qt framework version
    """

    deadline_dep_versions: dict[str, str]
    """Dictionary of deadline dependency package names to versions"""

    os_name: str
    """Name of the operating system"""

    os_version: str
    """Operating system version"""

    os_architecture: str
    """OS architecture (e.g., "x86_64", "arm64")"""

    python_version: str
    """Python runtime version"""

    qt_version: str
    """Qt framework version"""

    @staticmethod
    def collect() -> "_EnvironmentInfo":
        """
        Collect runtime environment information such as Python version, dependency versions
        and more.

        Returns:
            _EnvironmentInfo object with all collected information
        """

        deadline_dep_versions = {}

        # Get the main deadline package version
        try:
            from ..._version import version

            deadline_dep_versions["deadline"] = version
        except Exception as e:
            logger.debug(f"Failed to retrieve deadline-cloud version: {e}")
            deadline_dep_versions["deadline"] = "Unknown"

        # Get direct dependency versions from package metadata
        try:
            deadline_deps = requires("deadline")
        except Exception as e:
            deadline_deps = []
            logger.debug(f"Could not retrieve deadline dependencies: {e}")

        if deadline_deps:
            for dep in deadline_deps:
                # Parse dependency string (e.g., "boto3 >= 1.39.10; python_version >= '3.9'")
                # Extract just the package name
                match = re.match(r"^([a-zA-Z0-9_-]+)", dep)
                if not match:
                    continue
                package_name = match.group(1)

                try:
                    deadline_dep_versions[package_name] = package_version(package_name)
                except Exception as e:
                    logger.debug(f"Could not retrieve version for {package_name}: {e}")

        # Collect Python version
        try:
            python_version = platform.python_version()
        except Exception as e:
            logger.warning(f"Failed to retrieve Python version: {e}")
            python_version = "Unknown"

        # Collect OS name and version
        try:
            system = platform.system()
            if system == "Darwin":
                os_name = "macOS"
                os_version = str(platform.mac_ver()[0])
            else:
                os_name = system
                os_version = str(platform.release())
        except Exception as e:
            logger.warning(f"Failed to retrieve OS name and version: {e}")
            os_name = "Unknown"
            os_version = "Unknown"

        # Collect OS architecture
        try:
            platform_machine = platform.machine()
            machine_to_arch = {
                "aarch64": "arm64",
                "amd64": "x86_64",
            }
            platform_machine = platform_machine.lower()
            os_architecture = machine_to_arch.get(platform_machine, platform_machine)
        except Exception as e:
            logger.warning(f"Failed to retrieve OS architecture: {e}")
            os_architecture = "Unknown"

        # Collect Qt version
        try:
            from qtpy.QtCore import qVersion

            qt_version = qVersion()
        except Exception as e:
            logger.warning(f"Failed to retrieve Qt version: {e}")
            qt_version = "Unknown"

        return _EnvironmentInfo(
            deadline_dep_versions=deadline_dep_versions,
            python_version=python_version,
            os_name=os_name,
            os_version=os_version,
            os_architecture=os_architecture,
            qt_version=qt_version,
        )
