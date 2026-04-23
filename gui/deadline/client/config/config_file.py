# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Config shim that routes through FFI to Rust.

Replaces Python's ConfigParser-based implementation. All reads/writes
go through the Rust deadline-config crate via _ffi.py.
"""

from .._ffi import DeadlineFFI, DeadlineOperationError

_TRUE_VALUES = {"yes", "on", "true", "1"}
_FALSE_VALUES = {"no", "off", "false", "0"}

_ffi = None


def _get_ffi() -> DeadlineFFI:
    global _ffi
    if _ffi is None:
        _ffi = DeadlineFFI()
    return _ffi


def get_setting(setting_name: str, config=None) -> str:
    """Get a setting value.

    If config (a ConfigParser) is provided, reads from it directly
    using the Python-side section/key layout. Otherwise routes through FFI.
    """
    if config is not None:
        return _get_setting_from_config(setting_name, config)
    try:
        return _get_ffi().get_setting(setting_name)
    except DeadlineOperationError:
        from ..exceptions import DeadlineOperationError as DOE
        raise DOE(
            f"AWS Deadline Cloud configuration has no setting named {setting_name!r}."
        )


def _get_setting_from_config(setting_name: str, config) -> str:
    """Read a setting from a ConfigParser object.

    Matches Python's get_setting behavior: settings in the 'defaults'
    section are profile-scoped, so 'defaults.farm_id' looks for
    'profile-<name> defaults' section. The profile name comes from
    'defaults.aws_profile_name' or falls back to '(default)'.
    """
    parts = setting_name.split(".", 1)
    if len(parts) != 2:
        return ""
    section, key = parts

    # Resolve profile name for profile-scoped sections
    if section == "defaults":
        profile = config.get("defaults", "aws_profile_name", fallback="(default)")
        scoped = f"profile-{profile} defaults"
        val = config.get(scoped, key, fallback=None)
        if val is not None:
            return val
        return ""
    elif section == "settings":
        profile = config.get("defaults", "aws_profile_name", fallback="(default)")
        scoped = f"profile-{profile} settings"
        val = config.get(scoped, key, fallback=None)
        if val is not None:
            return val
        # Fall back to global settings section
        return config.get("settings", key, fallback="")

    return config.get(section, key, fallback="")


def set_setting(setting_name: str, value: str, config=None) -> None:
    """Set a setting value. Always writes to disk."""
    _get_ffi().set_setting(setting_name, value)


def get_setting_default(setting_name: str, config=None) -> str:
    """Get the default value for a setting.

    LIMITATION: The FFI doesn't expose get_setting_default. This returns
    the current value, which equals the default only when the setting is
    unset. Batch 3 (config dialog) will need a proper implementation —
    either a new FFI endpoint or reading from a temp empty config.
    """
    return _get_ffi().get_setting(setting_name)


def read_config():
    """Read the config file. Returns the raw config dict from FFI."""
    return _get_ffi().read_config()


def write_config(config) -> None:
    """Write config to disk. No-op in FFI shim — set_setting writes immediately."""
    pass


def str2bool(value: str) -> bool:
    """Convert a string to boolean."""
    v = value.lower()
    if v in _TRUE_VALUES:
        return True
    if v in _FALSE_VALUES:
        return False
    raise ValueError(f"{value!r} is not a valid boolean string value")
