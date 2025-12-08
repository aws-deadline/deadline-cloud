# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Dataclass for holding submitter environment information.
"""

from __future__ import annotations

__all__ = ["SubmitterInfo"]

from dataclasses import dataclass
from typing import Optional, Union, Dict, List

# Type alias for YAML-safe values that can be arbitrarily nested
YamlValue = Union[str, int, float, bool, None, Dict[str, "YamlValue"], List["YamlValue"]]


@dataclass
class SubmitterInfo:
    """
    Container for submitter environment information.

    This dataclass holds metadata about the application submitting jobs to AWS Deadline Cloud.
    It's used by the _AboutDialog GUI to display version information.

    Attributes:
        submitter_name: Short name of the submitter (required, e.g., "Blender", "CLI")
        submitter_package_name: Name of the submitter package using the library (optional, e.g., "deadline-cloud-for-blender")
        submitter_package_version: Version of the submitter package (optional)
        host_application_name: Name of the host application (optional, e.g., "Maya", "Blender")
        host_application_version: Version of the host application (optional)
        additional_info: Optional dictionary for arbitrary nested data that integrations can use
                        to pass additional metadata (optional). Supports nested structures with YAML-safe types:
                        str, int, float, bool, dicts, lists and None.

    Example:
        info = SubmitterInfo(
            submitter_name="Blender",
            submitter_package_name="deadline-cloud-for-blender",
            submitter_package_version="0.5.0",
            host_application_name="Blender",
            host_application_version="4.5.21",
            additional_info={
                "render_engine": "Cycles",
                "Loaded Plugins": {
                    "Plugin 1": "0.5.0",
                    "Plugin 2": "0.7.0"
                }
            }
        )
    """

    submitter_name: str
    """Short name of the submitter (required, e.g., "Blender", "CLI")"""

    submitter_package_name: Optional[str] = None
    """Name of the submitter package using the library (optional, e.g., "deadline-cloud-for-blender")"""

    submitter_package_version: Optional[str] = None
    """Version of the submitter package (optional)"""

    host_application_name: Optional[str] = None
    """Name of the host application (optional, e.g., "Maya", "Blender")"""

    host_application_version: Optional[str] = None
    """Version of the host application (optional)"""

    additional_info: Optional[Dict[str, YamlValue]] = None
    """Optional dictionary for arbitrary nested data that integrations can use to pass additional metadata (optional). 
    Supports nested structures with YAML-safe types: str, int, float, bool, dicts, lists and None."""
