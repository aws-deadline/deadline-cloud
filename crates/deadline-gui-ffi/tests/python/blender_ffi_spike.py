"""
Spike: GUI FFI inside Blender.

Proves the Rust shared library loads in Blender's embedded Python
without symbol conflicts, Qt version crashes, or ctypes ABI issues.

Run: /Applications/Blender.app/Contents/MacOS/Blender --background --python blender_ffi_spike.py
"""

import ctypes
import json
import os
import sys
import threading

DYLIB = os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "target", "debug", "libdeadline_gui_ffi.dylib")

print(f"Python: {sys.version}")
print(f"Platform: {sys.platform}")
print(f"Library: {DYLIB}")
print(f"Exists: {os.path.exists(DYLIB)}")
print()

# ── Test 1: Load the library ────────────────────────────────────
print("Test 1: Load .dylib inside Blender...")
try:
    lib = ctypes.CDLL(DYLIB)
    lib.deadline_free_string.argtypes = [ctypes.c_char_p]
    lib.deadline_free_string.restype = None
    lib.deadline_get_credentials_source.argtypes = [ctypes.c_char_p]
    lib.deadline_get_credentials_source.restype = ctypes.c_char_p
    lib.deadline_check_auth_status.argtypes = [ctypes.c_char_p]
    lib.deadline_check_auth_status.restype = ctypes.c_char_p
    print("  ✓ Library loaded, no symbol conflicts\n")
except Exception as e:
    print(f"  ✗ FAILED: {e}")
    sys.exit(1)

# ── Test 2: Basic C ABI call ────────────────────────────────────
print("Test 2: Call deadline_get_credentials_source()...")
try:
    result = lib.deadline_get_credentials_source(None)
    data = json.loads(result)
    assert "credentials_source" in data, f"Missing key: {data}"
    print(f"  Result: {data}")
    print("  ✓ Basic call works\n")
except Exception as e:
    print(f"  ✗ FAILED: {e}")
    sys.exit(1)

# ── Test 3: Async call from a thread ────────────────────────────
print("Test 3: Call deadline_check_auth_status() from background thread...")
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

if t.is_alive():
    print("  ✗ FAILED: Thread timed out (deadlock?)")
    sys.exit(1)
if "error" in error_holder:
    print(f"  ✗ FAILED: {error_holder['error']}")
    sys.exit(1)

data = result_holder["data"]
assert "credentials_source" in data
assert "auth_status" in data
assert "api_available" in data
print(f"  Result: {data}")
print("  ✓ Async call from thread works\n")

# ── Test 4: Callback from Rust to Python ────────────────────────
print("Test 4: Callback from Rust into Blender's Python...")
STATUS_CALLBACK = ctypes.CFUNCTYPE(None, ctypes.c_char_p, ctypes.c_void_p)
lib.deadline_check_auth_status_with_progress.argtypes = [ctypes.c_char_p, STATUS_CALLBACK, ctypes.c_void_p]
lib.deadline_check_auth_status_with_progress.restype = ctypes.c_char_p

messages = []

@STATUS_CALLBACK
def on_progress(message, user_data):
    messages.append(message.decode("utf-8") if message else "")

result = lib.deadline_check_auth_status_with_progress(None, on_progress, None)
data = json.loads(result)
assert len(messages) >= 3, f"Expected ≥3 callbacks, got {len(messages)}"
print(f"  Callbacks: {messages}")
print(f"  Result: {data}")
print("  ✓ Callbacks work\n")

# ── Summary ─────────────────────────────────────────────────────
print("=" * 50)
print("SPIKE PASSED: GUI FFI inside Blender")
print("=" * 50)
print("  ✓ No symbol conflicts")
print("  ✓ No Qt version crash")
print("  ✓ ctypes loading works")
print("  ✓ C ABI calls work")
print("  ✓ Async tokio runtime works from thread")
print("  ✓ Rust→Python callbacks work")
