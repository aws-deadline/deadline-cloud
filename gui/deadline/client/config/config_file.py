# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Config shim that routes through FFI to Rust.

Replaces Python's ConfigParser-based implementation. All reads/writes
go through the Rust deadline-config crate via deadline._native.
"""

from deadline._native import (
    get_setting as _native_get_setting,
    set_setting as _native_set_setting,
    read_config as _native_read_config,
)

_TRUE_VALUES = {"yes", "on", "true", "1"}
_FALSE_VALUES = {"no", "off", "false", "0"}


def get_setting(setting_name: str, config=None) -> str:
    """Get a setting value.

    If config (a ConfigParser) is provided, reads from it directly
    using the Python-side section/key layout. Otherwise routes through FFI.
    Falls back to FFI default when config has no explicit value.
    """
    if config is not None:
        val = _get_setting_from_config(setting_name, config)
        if val is not None:
            return val
        # Setting not present in config — fall back to FFI for computed defaults
        return _native_get_setting(setting_name)
    return _native_get_setting(setting_name)


def _get_setting_from_config(setting_name: str, config) -> str | None:
    """Read a setting from a ConfigParser object.

    Returns the value if found (may be empty string ""), or None if the
    key is not present in the config at all.
    """
    parts = setting_name.split(".", 1)
    if len(parts) != 2:
        return None
    section, key = parts

    # aws_profile_name is the one setting that lives directly in [defaults]
    if setting_name == "defaults.aws_profile_name":
        return config.get("defaults", "aws_profile_name", fallback=None)

    # Resolve profile name for profile-scoped sections
    if section == "defaults":
        profile = config.get("defaults", "aws_profile_name", fallback="(default)")
        scoped = f"profile-{profile} defaults"
        return config.get(scoped, key, fallback=None)
    elif section == "settings":
        profile = config.get("defaults", "aws_profile_name", fallback="(default)")
        scoped = f"profile-{profile} settings"
        val = config.get(scoped, key, fallback=None)
        if val is not None:
            return val
        # Fall back to global settings section
        return config.get("settings", key, fallback=None)

    return config.get(section, key, fallback=None)


def set_setting(setting_name: str, value: str, config=None) -> None:
    """Set a setting value.

    If config is provided, mutates it in-memory only (no disk write).
    If config is None, writes to disk via FFI.
    """
    if config is not None:
        _set_setting_in_config(setting_name, value, config)
    else:
        _native_set_setting(setting_name, value)


def _set_setting_in_config(setting_name: str, value: str, config) -> None:
    """Write a setting into a ConfigParser object in-memory.

    Mirrors the section resolution logic of _get_setting_from_config.
    """
    parts = setting_name.split(".", 1)
    if len(parts) != 2:
        return
    section, key = parts

    # aws_profile_name lives directly in [defaults]
    if setting_name == "defaults.aws_profile_name":
        if not config.has_section("defaults"):
            config.add_section("defaults")
        config.set("defaults", "aws_profile_name", value)
        return

    if section in ("defaults", "settings"):
        profile = config.get("defaults", "aws_profile_name", fallback="(default)")
        scoped = f"profile-{profile} {section}"
        if not config.has_section(scoped):
            config.add_section(scoped)
        config.set(scoped, key, value)
    else:
        if not config.has_section(section):
            config.add_section(section)
        config.set(section, key, value)


def get_setting_default(setting_name: str, config=None) -> str:
    """Get the default value for a setting.

    LIMITATION: The FFI doesn't expose get_setting_default. This returns
    the current value, which equals the default only when the setting is
    unset. Batch 3 (config dialog) will need a proper implementation —
    either a new FFI endpoint or reading from a temp empty config.
    """
    return _native_get_setting(setting_name)


def read_config():
    """Read the config file. Returns a ConfigParser populated from FFI data."""
    from configparser import ConfigParser
    config = ConfigParser()
    data = _native_read_config()
    if data:
        config.read_dict(data)
    return config


def write_config(config) -> None:
    """Write config to disk. No-op — apply() writes changes via set_setting(name, val)."""
    pass


def str2bool(value: str) -> bool:
    """Convert a string to boolean."""
    v = value.lower()
    if v in _TRUE_VALUES:
        return True
    if v in _FALSE_VALUES:
        return False
    raise ValueError(f"{value!r} is not a valid boolean string value")
