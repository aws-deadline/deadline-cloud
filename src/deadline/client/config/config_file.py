# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

__all__ = [
    "get_config_file_path",
    "get_cache_directory",
    "read_config",
    "write_config",
    "get_setting_default",
    "get_setting",
    "set_setting",
    "clear_setting",
    "get_best_profile_for_farm",
    "str2bool",
]

import getpass
import os
import platform
from configparser import ConfigParser
from pathlib import Path
from typing import Any, Dict, List, Optional
import tempfile

import boto3
from deadline.job_attachments.models import FileConflictResolution

from ..exceptions import DeadlineOperationError

# Default path where AWS Deadline Cloud's configuration lives
CONFIG_FILE_PATH = os.path.join("~", ".deadline", "config")
# Environment variable that, if set, overrides the value of CONFIG_FILE_PATH
CONFIG_FILE_PATH_ENV_VAR = "DEADLINE_CONFIG_FILE_PATH"
# The default AWS Deadline Cloud endpoint URL
# Environment variable that, if set, overrides the value of DEFAULT_DEADLINE_ENDPOINT_URL
DEFAULT_DEADLINE_ENDPOINT_URL = os.getenv(
    "AWS_ENDPOINT_URL_DEADLINE", f"https://deadline.{boto3.Session().region_name}.amazonaws.com"
)

# The default directory within which to save the history of created jobs.
DEFAULT_JOB_HISTORY_DIR = os.path.join("~", ".deadline", "job_history", "{aws_profile_name}")
DEFAULT_CACHE_DIR = os.path.join("~", ".deadline", "cache")

_TRUE_VALUES = {"yes", "on", "true", "1"}
_FALSE_VALUES = {"no", "off", "false", "0"}
_BOOL_VALUES = _TRUE_VALUES | _FALSE_VALUES

__config = ConfigParser()
__config_file_path = None
__config_mtime = None

# This value defines the AWS Deadline Cloud settings structure. For each named setting,
# it stores:
# "default" - The default value.
# "depend"  - The setting it depends on, if any. This modifies the section name of the
#             setting to embed the dependency value, e.g. default.farm_id goes in
#             section [profile-{profileName} default]
# "section_format" - How its value gets formatted into config file sections.
SETTINGS: Dict[str, Dict[str, Any]] = {
    "deadline-cloud-monitor.path": {
        "default": "",
        "description": "The filesystem path to Deadline Cloud monitor, set during login process.",
    },
    "defaults.aws_profile_name": {
        "default": "(default)",
        "section_format": "profile-{}",
        "description": "The AWS profile name to use by default. Set to '' to use the default credentials."
        + " Other settings are saved with the profile.",
    },
    "settings.job_history_dir": {
        "default": DEFAULT_JOB_HISTORY_DIR,
        "depend": "defaults.aws_profile_name",
        "description": "The directory in which to place the job submission history for this AWS profile name.",
    },
    "defaults.farm_id": {
        "default": "",
        "depend": "defaults.aws_profile_name",
        "section_format": "{}",
        "description": "The Farm ID to use by default.",
    },
    "settings.storage_profile_id": {
        "default": "",
        "depend": "defaults.farm_id",
        "section_format": "{}",
        "description": "The storage profile that this workstation conforms to. It specifies where shared file systems are mounted, and where named job attachments should go.",
    },
    "defaults.queue_id": {
        "default": "",
        "depend": "defaults.farm_id",
        "section_format": "{}",
        "description": "The Queue ID to use by default.",
    },
    "defaults.job_id": {
        "default": "",
        "depend": "defaults.queue_id",
        "description": "The Job ID to use by default. This gets updated by job submission, so is normally the most recently submitted job.",
    },
    "settings.auto_accept": {
        "default": "false",
    },
    "settings.conflict_resolution": {
        "default": FileConflictResolution.NOT_SELECTED.name,
        "description": "How to handle downloads if a file already exists",
    },
    "settings.log_level": {
        "default": "WARNING",
        "description": "The logging level to use in the CLI and GUIs.",
    },
    "telemetry.opt_out": {"default": "false"},
    "telemetry.identifier": {"default": ""},
    "defaults.job_attachments_file_system": {"default": "COPIED", "depend": "defaults.farm_id"},
    "settings.s3_max_pool_connections": {
        "default": "50",
        "description": (
            "The maximum number of connections to keep in the connection pool used by the S3's upload/download operations. "
            "If this value is not set, the default value of 50 is used. "
            "(Note: It's recommended setting this value above 10 to avoid 'Connection pool is full' warnings during the uploads/downloads.)"
        ),
    },
    "settings.small_file_threshold_multiplier": {
        "default": "20",  # By default, the small file threshold is 160 MB (since the default S3 multipart-upload chunk size is 8 MB.)
        "description": (
            "When uploading job attachments, the file size threshold is set to separate 'large' files from 'small' files so that 'large' files can be processed serially. "
            "This multiplier is used to calculate the size threshold. (Small files are defined as those smaller than or equal to the chunk size multiplied by this factor.)"
        ),
    },
}


def get_config_file_path() -> Path:
    """
    Get the config file path from the environment variable, falling back
    to our default if it is not set.
    """
    return Path(os.environ.get(CONFIG_FILE_PATH_ENV_VAR) or CONFIG_FILE_PATH).expanduser()


def get_cache_directory() -> str:
    """
    Get the cache directory.
    """
    return os.path.expanduser(DEFAULT_CACHE_DIR)


def _should_read_config(config_file_path: Path) -> bool:
    global __config_file_path
    global __config_mtime

    if (
        __config_mtime is not None
        and config_file_path == __config_file_path
        and config_file_path.is_file()
    ):
        mtime = config_file_path.stat().st_mtime
        if mtime == __config_mtime:
            return False
    return True


def read_config() -> ConfigParser:
    """
    If the config hasn't been read yet, or was modified since it was last
    read, reads the AWS Deadline Cloud configuration.
    """
    global __config
    global __config_file_path
    global __config_mtime

    config_file_path = get_config_file_path()

    if _should_read_config(config_file_path):
        # Read the config file with a fresh config parser, and update the last-modified time stamp
        __config = ConfigParser()
        __config_file_path = config_file_path
        __config.read(config_file_path)
        if config_file_path.is_file():
            __config_mtime = config_file_path.stat().st_mtime
        else:
            __config_mtime = None

    return __config


def _reset_directory_permissions_windows(directory: Path) -> None:
    if platform.system() != "Windows":
        return
    import win32security
    import ntsecuritycon

    # We don't want to propagate existing permissions, so create a new DACL
    dacl = win32security.ACL()

    # On Windows, both SYSTEM and the Administrators group normally
    # have Full Access to files in the user's home directory.
    # Use SIDs to represent the Administrators and SYSTEM to
    # support multi-language operating systems
    # Administrator(S-1-5-32-544), SYSTEM(S-1-5-18)
    # https://learn.microsoft.com/en-us/windows/win32/secauthz/well-known-sids
    system_sid = win32security.ConvertStringSidToSid("S-1-5-18")
    admin_sid = win32security.ConvertStringSidToSid("S-1-5-32-544")

    username = getpass.getuser()
    user_sid, _, _ = win32security.LookupAccountName(None, username)

    for sid in [user_sid, admin_sid, system_sid]:
        dacl.AddAccessAllowedAceEx(
            win32security.ACL_REVISION,
            ntsecuritycon.OBJECT_INHERIT_ACE | ntsecuritycon.CONTAINER_INHERIT_ACE,
            ntsecuritycon.GENERIC_ALL,
            sid,
        )

    # Get the security descriptor of the object
    sd = win32security.GetFileSecurity(
        str(directory.resolve()), win32security.DACL_SECURITY_INFORMATION
    )

    # Set the security descriptor's DACL to the newly-created DACL
    # Arguments:
    # 1. bDaclPresent = 1: Indicates that the DACL is present in the security descriptor.
    #    If set to 0, this method ignores the provided DACL and allows access to all principals.
    # 2. dacl: The discretionary access control list (DACL) to be set in the security descriptor.
    # 3. bDaclDefaulted = 0: Indicates the DACL was provided and not defaulted.
    #    If set to 1, indicates the DACL was defaulted, as in the case of permissions inherited from a parent directory.
    sd.SetSecurityDescriptorDacl(1, dacl, 0)
    win32security.SetFileSecurity(
        str(directory.resolve()), win32security.DACL_SECURITY_INFORMATION, sd
    )


def write_config(config: ConfigParser) -> None:
    """
    Writes the provided config to the AWS Deadline Cloud configuration.

    Args:
        config (ConfigParser): The config object to write. Generally this is
            a modified value from what `read_config` returns.
    """
    config_file_path = get_config_file_path()
    if not config_file_path.parent.exists():
        config_file_path.parent.mkdir(parents=True, exist_ok=True)
        if platform.system() == "Windows":
            _reset_directory_permissions_windows(config_file_path.parent)

    # Using the config file path as the prefix ensures that the tmpfile and real file are
    # on the same filesystem. This is a requirement for os.replace to be atomic.
    file_descriptor, tmp_file_name = tempfile.mkstemp(prefix=str(config_file_path), text=True)

    # Note: The file descriptor is closed when exiting the context manager.
    with os.fdopen(file_descriptor, "w", encoding="utf8") as configfile:
        config.write(configfile)

    os.replace(tmp_file_name, config_file_path)


def _get_setting_config(setting_name: str) -> dict:
    """
    Gets the setting configuration for the specified setting name,
    raising an error if it does not exist.

    Args:
        setting_name (str): The full setting name, like `section.setting`.
    """
    setting_config = SETTINGS.get(setting_name)
    if not setting_config:
        raise DeadlineOperationError(
            f"AWS Deadline Cloud configuration has no setting named {setting_name!r}."
        )
    return setting_config


def _get_default_from_setting_config(setting_config, config: Optional[ConfigParser]) -> str:
    """
    Gets the default value from a setting_config entry, performing value substitutions.

    Currently the only substitution supported is `{aws_profile_name}`.
    """
    default_value = setting_config["default"]
    # Only do the substitution if we see a pattern
    if "{" in default_value:
        default_value = default_value.format(
            aws_profile_name=get_setting("defaults.aws_profile_name", config=config)
        )
    return default_value


def get_setting_default(setting_name: str, config: Optional[ConfigParser] = None) -> str:
    """
    Gets the default value for the setting `setting_name`.
    Raises an exception if the setting does not exist.

    Args:
        setting_name (str): The full setting name, like `section.setting`.
        config: The config file read with `read_config()`.
    """
    setting_config = _get_setting_config(setting_name)
    return _get_default_from_setting_config(setting_config, config=config)


def _get_section_prefixes(setting_config: dict, config: ConfigParser) -> List[str]:
    """
    Gets a list of the section name prefixes for the specified setting section + setting config

    Args:
        setting_config: The setting config object from the SETTINGS dictionary.
        config: The config file read with `read_config()`.
    """
    if "depend" in setting_config:
        dep_setting_name = setting_config["depend"]
        dep_setting_config = SETTINGS[dep_setting_name]
        dep_section_format = dep_setting_config["section_format"]

        dep_section, dep_name = dep_setting_name.split(".", 1)
        dep_section_prefixes = _get_section_prefixes(dep_setting_config, config)
        dep_section_value: Optional[str] = config.get(
            " ".join(dep_section_prefixes + [dep_section]), dep_name, fallback=None
        )
        if dep_section_value is None:
            dep_section_value = _get_default_from_setting_config(dep_setting_config, config=config)
        formatted_section_value = dep_section_format.format(dep_section_value)
        return dep_section_prefixes + [formatted_section_value]
    else:
        return []


def get_setting(setting_name: str, config: Optional[ConfigParser] = None) -> str:
    """
    Gets the value of the specified setting, returning the default if
    not configured. Raises an exception if the setting does not exist.

    Args:
        setting_name (str): The full setting name, like `section.setting`.
        config (ConfigParser, optional): The config file read with `read_config()`.
    """
    if "." not in setting_name:
        raise DeadlineOperationError(f"The setting name {setting_name!r} is not valid.")
    section, name = setting_name.split(".", 1)

    if config is None:
        config = read_config()

    setting_config = _get_setting_config(setting_name)

    section_prefixes = _get_section_prefixes(setting_config, config)
    section = " ".join(section_prefixes + [section])

    result: Optional[str] = config.get(section, name, fallback=None)

    if result is None:
        return _get_default_from_setting_config(setting_config, config=config)
    else:
        return result


def set_setting(setting_name: str, value: str, config: Optional[ConfigParser] = None):
    """
    Sets the value of the specified setting, returning the default if
    not configured. Raises an exception if the setting does not exist.

    Args:
        setting_name (str): The full setting name, like `section.setting`.
        value (bool|int|float|str): The value to set.
        config (Optional[ConfigParser]): If provided sets the setting in the parser and does not save to disk.
    """
    if "." not in setting_name:
        raise DeadlineOperationError(f"The setting name {setting_name!r} is not valid.")
    section, name = setting_name.split(".", 1)

    # Get the type of the default to validate it is an AWS Deadline Cloud setting, and retrieve its type
    setting_config = _get_setting_config(setting_name)

    # If no config was provided, then read from disk and signal to write it later
    if not config:
        config = read_config()
        save_config = True
    else:
        save_config = False

    section_prefixes = _get_section_prefixes(setting_config, config)
    section = " ".join(section_prefixes + [section])
    if section not in config:
        config[section] = {}
    config.set(section, name, value)
    if save_config:
        write_config(config)


def clear_setting(setting_name: str, config: Optional[ConfigParser] = None):
    """
    Sets the value of the specified setting back to the default value.

    Args:
        setting_name (str): The full setting name, like `section.setting`.
        config (Optional[ConfigParser]): If provided clears the setting in the parser and does not save to disk.
    """
    if "." not in setting_name:
        raise DeadlineOperationError(f"The setting name {setting_name!r} is not valid.")

    set_setting(setting_name, get_setting_default(setting_name, config=config), config=config)


def get_best_profile_for_farm(farm_id: str, queue_id: Optional[str] = None) -> str:
    """
    Finds the best AWS profile for the specified farm and queue IDs. Chooses
    the first match from:
    1. The default AWS profile if its default farm matches.
    2. AWS profiles whose default farm and queue IDs match.
    3. AWS profiles whose default farm matches.
    4. If there were no matches, returns the default AWS profile.
    """
    # Get the full list of AWS profiles
    session = boto3.Session()
    aws_profile_names = session._session.full_config["profiles"].keys()

    # Make a deep copy of the return from read_config(), since we modify
    # it during the profile name search.
    config = ConfigParser()
    config.read_dict(read_config())

    # (For 1.) Save the default profile and return it if its default farm matches.
    default_aws_profile_name: str = str(get_setting("defaults.aws_profile_name", config=config))
    if get_setting("defaults.farm_id", config=config) == farm_id:
        return default_aws_profile_name

    # (For 3.) We'll accumulate the profiles whose farms matched here
    first_farm_aws_profile_name: Optional[str] = None

    # (For 2.) Search for a profile with both a farm and queue match
    for aws_profile_name in aws_profile_names:
        set_setting("defaults.aws_profile_name", aws_profile_name, config=config)
        if get_setting("defaults.farm_id", config=config) == farm_id:
            # Return if both matched
            if queue_id and get_setting("defaults.queue_id", config=config) == queue_id:
                return aws_profile_name
            # Save if this was the first time the farm matched
            if not first_farm_aws_profile_name:
                first_farm_aws_profile_name = aws_profile_name

    # Return the first farm-matched profile, or the default if there was none
    return first_farm_aws_profile_name or default_aws_profile_name


def str2bool(value: str) -> bool:
    """
    Converts a string to boolean, accepting a variety of on/off,
    true/false, 0/1 variants.
    """
    value = value.lower()
    if value in _BOOL_VALUES:
        return value in _TRUE_VALUES
    else:
        raise ValueError(f"{value!r} is not a valid boolean string value")
