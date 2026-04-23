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
    """Get a setting value. config parameter accepted for API compat but ignored."""
    try:
        return _get_ffi().get_setting(setting_name)
    except DeadlineOperationError:
        from ..exceptions import DeadlineOperationError as DOE
        raise DOE(
            f"AWS Deadline Cloud configuration has no setting named {setting_name!r}."
        )


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
