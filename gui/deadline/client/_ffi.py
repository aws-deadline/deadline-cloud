"""Thin ctypes wrapper around libdeadline_gui_ffi.

All other Python code calls this module's DeadlineFFI class via normal
Python methods. This is the only file that touches ctypes.
"""

import ctypes
import json
import os
import platform
from pathlib import Path
from typing import Callable, Optional


class DeadlineOperationError(Exception):
    """Raised when an FFI call returns an error."""


# ── Callback types ───────────────────────────────────────────────

_PrintCallback = ctypes.CFUNCTYPE(None, ctypes.c_char_p, ctypes.c_void_p)
_ProgressCallback = ctypes.CFUNCTYPE(ctypes.c_bool, ctypes.c_char_p, ctypes.c_void_p)
_ConfirmationCallback = ctypes.CFUNCTYPE(
    ctypes.c_bool, ctypes.c_char_p, ctypes.c_bool, ctypes.c_void_p
)
_ContinueCallback = ctypes.CFUNCTYPE(ctypes.c_bool, ctypes.c_void_p)
_StatusCallback = ctypes.CFUNCTYPE(None, ctypes.c_char_p, ctypes.c_void_p)


def _encode(s: Optional[str]) -> Optional[bytes]:
    """Encode a string to UTF-8 bytes, or return None for None."""
    if s is None:
        return None
    return s.encode("utf-8")


def _find_library() -> str:
    """Find the shared library path."""
    # 1. Environment variable override (for testing / development)
    env_path = os.environ.get("DEADLINE_FFI_LIB_PATH")
    if env_path:
        return env_path

    names = {
        "Darwin": "libdeadline_gui_ffi.dylib",
        "Linux": "libdeadline_gui_ffi.so",
        "Windows": "deadline_gui_ffi.dll",
    }
    lib_name = names.get(platform.system(), names["Linux"])

    base = Path(__file__).resolve().parent

    # 2. Installed layout: _internal/ sibling
    installed = base.parent.parent.parent / "_internal" / lib_name
    if installed.exists():
        return str(installed)

    # 3. Development layout: cargo target/debug
    dev = base.parent.parent.parent / "target" / "debug" / lib_name
    if dev.exists():
        return str(dev)

    # 4. Workspace root (when running from repo root)
    workspace = Path.cwd()
    for candidate in [
        workspace / "target" / "debug" / lib_name,
        workspace.parent / "target" / "debug" / lib_name,
    ]:
        if candidate.exists():
            return str(candidate)

    raise OSError(
        f"Could not find {lib_name}. Set DEADLINE_FFI_LIB_PATH or run "
        f"'cargo build -p deadline-gui-ffi'. Searched: {installed}, {dev}"
    )


class DeadlineFFI:
    """Thin ctypes wrapper around libdeadline_gui_ffi."""

    def __init__(self):
        self._lib = ctypes.CDLL(_find_library())
        self._declare_signatures()
        # Store callback references to prevent GC during FFI calls
        self._callbacks: list = []

    def _declare_signatures(self):
        lib = self._lib

        lib.deadline_free_string.argtypes = [ctypes.c_void_p]
        lib.deadline_free_string.restype = None

        lib.deadline_get_credentials_source.argtypes = [ctypes.c_char_p]
        lib.deadline_get_credentials_source.restype = ctypes.c_void_p

        lib.deadline_check_auth_status.argtypes = [ctypes.c_char_p]
        lib.deadline_check_auth_status.restype = ctypes.c_void_p

        lib.deadline_check_auth_status_with_progress.argtypes = [
            ctypes.c_char_p, _StatusCallback, ctypes.c_void_p,
        ]
        lib.deadline_check_auth_status_with_progress.restype = ctypes.c_void_p

        lib.deadline_read_config.argtypes = [ctypes.c_char_p]
        lib.deadline_read_config.restype = ctypes.c_void_p

        lib.deadline_get_setting.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
        lib.deadline_get_setting.restype = ctypes.c_void_p

        lib.deadline_set_setting.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p]
        lib.deadline_set_setting.restype = ctypes.c_void_p

        lib.deadline_list_farms.argtypes = [ctypes.c_char_p]
        lib.deadline_list_farms.restype = ctypes.c_void_p

        lib.deadline_list_queues.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
        lib.deadline_list_queues.restype = ctypes.c_void_p

        lib.deadline_list_storage_profiles_for_queue.argtypes = [
            ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p,
        ]
        lib.deadline_list_storage_profiles_for_queue.restype = ctypes.c_void_p

        lib.deadline_get_queue_parameter_definitions.argtypes = [
            ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p,
        ]
        lib.deadline_get_queue_parameter_definitions.restype = ctypes.c_void_p

        lib.deadline_check_api_available.argtypes = [ctypes.c_char_p]
        lib.deadline_check_api_available.restype = ctypes.c_void_p

        lib.deadline_login.argtypes = [ctypes.c_char_p]
        lib.deadline_login.restype = ctypes.c_void_p

        lib.deadline_logout.argtypes = [ctypes.c_char_p]
        lib.deadline_logout.restype = ctypes.c_void_p

        lib.deadline_create_job_from_job_bundle.argtypes = [
            ctypes.c_char_p,
            _PrintCallback, _ProgressCallback, _ProgressCallback,
            _ConfirmationCallback, _ContinueCallback,
            ctypes.c_void_p,
        ]
        lib.deadline_create_job_from_job_bundle.restype = ctypes.c_void_p

        lib.deadline_init_telemetry.argtypes = [ctypes.c_char_p]
        lib.deadline_init_telemetry.restype = ctypes.c_void_p

        lib.deadline_record_telemetry_event.argtypes = [
            ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p,
        ]
        lib.deadline_record_telemetry_event.restype = ctypes.c_void_p

        lib.deadline_free_telemetry.argtypes = [ctypes.c_void_p]
        lib.deadline_free_telemetry.restype = None

    def _call_json(self, ptr) -> dict:
        """Parse JSON from a Rust-allocated string, free it, check for errors."""
        if not ptr:
            raise DeadlineOperationError("FFI returned null pointer")
        try:
            data = json.loads(ctypes.string_at(ptr))
        finally:
            self._lib.deadline_free_string(ptr)
        if "error" in data:
            raise DeadlineOperationError(data["error"])
        return data

    # ── Auth Status ──────────────────────────────────────────────

    def get_credentials_source(self, config_path: Optional[str] = None) -> str:
        ptr = self._lib.deadline_get_credentials_source(_encode(config_path))
        data = self._call_json(ptr)
        return data["credentials_source"]

    def check_auth_status(self, config_path: Optional[str] = None) -> dict:
        ptr = self._lib.deadline_check_auth_status(_encode(config_path))
        return self._call_json(ptr)

    def check_auth_status_with_progress(
        self,
        config_path: Optional[str] = None,
        on_progress: Optional[Callable[[str], None]] = None,
    ) -> dict:
        if on_progress:
            @_StatusCallback
            def _cb(message, _user_data):
                msg = message.decode("utf-8") if message else ""
                on_progress(msg)
            self._callbacks.append(_cb)  # prevent GC
        else:
            _cb = _StatusCallback()  # null function pointer

        ptr = self._lib.deadline_check_auth_status_with_progress(
            _encode(config_path), _cb, None,
        )
        self._callbacks.clear()
        return self._call_json(ptr)

    # ── Config ───────────────────────────────────────────────────

    def read_config(self, config_path: Optional[str] = None) -> dict:
        ptr = self._lib.deadline_read_config(_encode(config_path))
        return self._call_json(ptr)

    def get_setting(self, name: str, config_path: Optional[str] = None) -> str:
        ptr = self._lib.deadline_get_setting(_encode(name), _encode(config_path))
        data = self._call_json(ptr)
        return data["value"]

    def set_setting(
        self, name: str, value: str, config_path: Optional[str] = None
    ) -> None:
        ptr = self._lib.deadline_set_setting(
            _encode(name), _encode(value), _encode(config_path),
        )
        self._call_json(ptr)

    # ── Resource Listing ─────────────────────────────────────────

    def list_farms(self, config_path: Optional[str] = None) -> dict:
        ptr = self._lib.deadline_list_farms(_encode(config_path))
        return self._call_json(ptr)

    def list_queues(
        self, farm_id: Optional[str] = None, config_path: Optional[str] = None
    ) -> dict:
        ptr = self._lib.deadline_list_queues(
            _encode(farm_id), _encode(config_path),
        )
        return self._call_json(ptr)

    def list_storage_profiles_for_queue(
        self,
        farm_id: Optional[str] = None,
        queue_id: Optional[str] = None,
        config_path: Optional[str] = None,
    ) -> dict:
        ptr = self._lib.deadline_list_storage_profiles_for_queue(
            _encode(farm_id), _encode(queue_id), _encode(config_path),
        )
        return self._call_json(ptr)

    def get_queue_parameter_definitions(
        self,
        farm_id: Optional[str] = None,
        queue_id: Optional[str] = None,
        config_path: Optional[str] = None,
    ) -> list:
        ptr = self._lib.deadline_get_queue_parameter_definitions(
            _encode(farm_id), _encode(queue_id), _encode(config_path),
        )
        data = self._call_json(ptr)
        return data["parameters"]

    # ── Auth Actions ─────────────────────────────────────────────

    def check_api_available(self, config_path: Optional[str] = None) -> bool:
        ptr = self._lib.deadline_check_api_available(_encode(config_path))
        data = self._call_json(ptr)
        return data["api_available"]

    def login(self, config_path: Optional[str] = None) -> str:
        ptr = self._lib.deadline_login(_encode(config_path))
        data = self._call_json(ptr)
        return data["success"]

    def logout(self, config_path: Optional[str] = None) -> str:
        ptr = self._lib.deadline_logout(_encode(config_path))
        data = self._call_json(ptr)
        return data["success"]

    # ── Submission ───────────────────────────────────────────────

    def create_job_from_job_bundle(
        self,
        params: dict,
        print_cb: Optional[Callable[[str], None]] = None,
        hashing_cb: Optional[Callable[[dict], bool]] = None,
        upload_cb: Optional[Callable[[dict], bool]] = None,
        confirm_cb: Optional[Callable[[str, bool], bool]] = None,
        continue_cb: Optional[Callable[[], bool]] = None,
    ) -> dict:
        if not isinstance(params, dict):
            raise DeadlineOperationError("params must be a dict")

        params_json = json.dumps(params).encode("utf-8")

        # Wrap Python callbacks into CFUNCTYPE instances
        if print_cb:
            @_PrintCallback
            def _print(msg, _ud):
                print_cb(msg.decode("utf-8") if msg else "")
            self._callbacks.append(_print)
        else:
            _print = _PrintCallback()

        if hashing_cb:
            @_ProgressCallback
            def _hash(meta_json, _ud):
                meta = json.loads(meta_json) if meta_json else {}
                return hashing_cb(meta)
            self._callbacks.append(_hash)
        else:
            _hash = _ProgressCallback()

        if upload_cb:
            @_ProgressCallback
            def _upload(meta_json, _ud):
                meta = json.loads(meta_json) if meta_json else {}
                return upload_cb(meta)
            self._callbacks.append(_upload)
        else:
            _upload = _ProgressCallback()

        if confirm_cb:
            @_ConfirmationCallback
            def _confirm(msg, default, _ud):
                return confirm_cb(msg.decode("utf-8") if msg else "", default)
            self._callbacks.append(_confirm)
        else:
            _confirm = _ConfirmationCallback()

        if continue_cb:
            @_ContinueCallback
            def _cont(_ud):
                return continue_cb()
            self._callbacks.append(_cont)
        else:
            _cont = _ContinueCallback()

        ptr = self._lib.deadline_create_job_from_job_bundle(
            params_json, _print, _hash, _upload, _confirm, _cont, None,
        )
        self._callbacks.clear()
        return self._call_json(ptr)

    # ── Telemetry ────────────────────────────────────────────────

    def init_telemetry(self, config_path: Optional[str] = None):
        """Returns an opaque handle. Must be freed with free_telemetry."""
        handle = self._lib.deadline_init_telemetry(_encode(config_path))
        if not handle:
            raise DeadlineOperationError("init_telemetry returned null")
        return handle

    def record_telemetry_event(
        self, handle, event_type: str, details: dict
    ) -> None:
        if not handle:
            raise DeadlineOperationError("telemetry handle is null")
        ptr = self._lib.deadline_record_telemetry_event(
            handle,
            _encode(event_type),
            json.dumps(details).encode("utf-8"),
        )
        self._call_json(ptr)

    def free_telemetry(self, handle) -> None:
        self._lib.deadline_free_telemetry(handle)
