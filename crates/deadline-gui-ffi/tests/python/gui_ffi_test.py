#!/usr/bin/env python3
"""
GUI FFI Round-Trip Spike — Python integration tests.

Tests all three sub-tasks:
  1. Basic C ABI call (sync, no callback)
  2. Async call from a thread (simulating QThread worker)
  3. Callback from Rust to Python

Run: python3 spike/gui_ffi_test.py

Prerequisites: cargo build -p deadline-gui-ffi
"""

import ctypes
import json
import os
import sys
import threading

LIB_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "..",
    "..",
    "..",
    "target",
    "debug",
    "libdeadline_gui_ffi.dylib",
)


def load_lib():
    """Load the Rust shared library and declare function signatures."""
    lib = ctypes.CDLL(LIB_PATH)

    # deadline_free_string(ptr)
    lib.deadline_free_string.argtypes = [ctypes.c_char_p]
    lib.deadline_free_string.restype = None

    # deadline_get_credentials_source(config_json) -> char*
    lib.deadline_get_credentials_source.argtypes = [ctypes.c_char_p]
    lib.deadline_get_credentials_source.restype = ctypes.c_char_p

    # deadline_check_auth_status(config_json) -> char*
    lib.deadline_check_auth_status.argtypes = [ctypes.c_char_p]
    lib.deadline_check_auth_status.restype = ctypes.c_char_p

    # Callback type: void (*)(const char* message, void* user_data)
    STATUS_CALLBACK = ctypes.CFUNCTYPE(None, ctypes.c_char_p, ctypes.c_void_p)

    # deadline_check_auth_status_with_progress(config, callback, user_data) -> char*
    lib.deadline_check_auth_status_with_progress.argtypes = [
        ctypes.c_char_p,
        STATUS_CALLBACK,
        ctypes.c_void_p,
    ]
    lib.deadline_check_auth_status_with_progress.restype = ctypes.c_char_p

    return lib, STATUS_CALLBACK


def test_subtask1_basic_call(lib):
    """Sub-task 1: Load .dylib, call sync function, get JSON back."""
    print("  Sub-task 1: Basic C ABI call...")

    result = lib.deadline_get_credentials_source(None)
    assert result is not None, "Result should not be None"

    data = json.loads(result)
    assert "credentials_source" in data, f"Missing key: {data}"
    assert data["credentials_source"] in (
        "HOST_PROVIDED",
        "DEADLINE_CLOUD_MONITOR_LOGIN",
        "NOT_VALID",
    ), f"Unexpected value: {data['credentials_source']}"

    print(f"    Result: {data}")
    print("    ✓ PASSED")


def test_subtask2_async_from_thread(lib):
    """Sub-task 2: Call async function from a background thread."""
    print("  Sub-task 2: Async call from worker thread...")

    result_holder = {}
    error_holder = {}

    def worker():
        try:
            result = lib.deadline_check_auth_status(None)
            result_holder["data"] = json.loads(result)
        except Exception as e:
            error_holder["error"] = str(e)

    t = threading.Thread(target=worker)
    t.start()
    t.join(timeout=30)

    assert not t.is_alive(), "Worker thread timed out (possible deadlock)"
    assert "error" not in error_holder, f"Worker error: {error_holder.get('error')}"
    assert "data" in result_holder, "No result from worker"

    data = result_holder["data"]
    assert "credentials_source" in data
    assert "auth_status" in data
    assert "api_available" in data

    print(f"    Result: {data}")
    print("    ✓ PASSED")


def test_subtask3_callback(lib, STATUS_CALLBACK):
    """Sub-task 3: Rust calls back into Python via C function pointer."""
    print("  Sub-task 3: Callback from Rust to Python...")

    messages = []

    @STATUS_CALLBACK
    def on_progress(message, user_data):
        msg = message.decode("utf-8") if message else ""
        messages.append(msg)

    result = lib.deadline_check_auth_status_with_progress(None, on_progress, None)
    assert result is not None

    data = json.loads(result)
    assert "credentials_source" in data

    assert len(messages) >= 3, f"Expected ≥3 callbacks, got {len(messages)}: {messages}"

    print(f"    Callbacks received: {messages}")
    print(f"    Result: {data}")
    print("    ✓ PASSED")


def test_subtask3_callback_from_thread(lib, STATUS_CALLBACK):
    """Sub-task 3b: Callback from Rust, called from a worker thread."""
    print("  Sub-task 3b: Callback from worker thread (simulates QThread)...")

    messages = []
    error_holder = {}

    @STATUS_CALLBACK
    def on_progress(message, user_data):
        msg = message.decode("utf-8") if message else ""
        messages.append(msg)

    def worker():
        try:
            result = lib.deadline_check_auth_status_with_progress(
                None, on_progress, None
            )
            data = json.loads(result)
            assert "credentials_source" in data
        except Exception as e:
            error_holder["error"] = str(e)

    t = threading.Thread(target=worker)
    t.start()
    t.join(timeout=30)

    assert not t.is_alive(), "Worker thread timed out (possible deadlock)"
    assert "error" not in error_holder, f"Worker error: {error_holder.get('error')}"
    assert len(messages) >= 3, f"Expected ≥3 callbacks, got {len(messages)}"

    print(f"    Callbacks received from thread: {messages}")
    print("    ✓ PASSED")


def main():
    print(f"Loading library: {LIB_PATH}")
    if not os.path.exists(LIB_PATH):
        print(f"ERROR: Library not found. Run: cargo build -p deadline-gui-ffi")
        sys.exit(1)

    lib, STATUS_CALLBACK = load_lib()
    print(f"Library loaded successfully\n")

    print("Running GUI FFI Round-Trip Spike Tests:")
    test_subtask1_basic_call(lib)
    test_subtask2_async_from_thread(lib)
    test_subtask3_callback(lib, STATUS_CALLBACK)
    test_subtask3_callback_from_thread(lib, STATUS_CALLBACK)

    print(f"\n{'='*50}")
    print("ALL SPIKE TESTS PASSED")
    print(f"{'='*50}")
    print("\nSpike results:")
    print("  ✓ ctypes loading works on macOS")
    print("  ✓ C ABI string passing works")
    print("  ✓ Async tokio runtime on worker thread works")
    print("  ✓ No deadlock with threading")
    print("  ✓ C function pointer callbacks work")
    print("  ✓ Callbacks from worker thread work")


if __name__ == "__main__":
    main()
