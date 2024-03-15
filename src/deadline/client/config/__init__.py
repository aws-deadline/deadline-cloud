# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
This module encapsulate the configuration of AWS Deadline Cloud on a workstation.

By default, configuration is stored in `~/.deadline/config`. If a user sets
the environment variable DEADLINE_CONFIG_FILE_PATH, it is used as the configuration
file path instead.
"""
__all__ = [
    "get_setting_default",
    "get_setting",
    "set_setting",
    "get_best_profile_for_farm",
    "str2bool",
    "DEFAULT_DEADLINE_ENDPOINT_URL",
]

from .config_file import (
    DEFAULT_DEADLINE_ENDPOINT_URL,
    get_best_profile_for_farm,
    get_setting,
    get_setting_default,
    set_setting,
    str2bool,
)
