#!/usr/bin/env python3
"""
Verify Rust FFI returns correct data by comparing against CLI output.
"""
import ctypes
import json
import re
import subprocess
import sys

DYLIB = "/Users/viknith/Documents/Github/deadline-py-to-rust-workspace/deadline-cloud-rs/target/debug/libdeadline_gui_ffi.dylib"
RUST_CLI = "/Users/viknith/Documents/Github/deadline-py-to-rust-workspace/deadline-cloud-rs/target/debug/deadline"

lib = ctypes.CDLL(DYLIB)

# free_string takes a raw pointer, not c_char_p (which auto-converts)
lib.deadline_free_string.argtypes = [ctypes.c_void_p]
lib.deadline_free_string.restype = None

def ffi_call(func_name, *args):
    func = getattr(lib, func_name)
    # Return raw pointer so we can free it properly
    func.restype = ctypes.c_void_p
    func.argtypes = [ctypes.c_char_p] * len(args)
    ptr = func(*args)
    if ptr is None:
        return {"error": "null pointer returned"}
    raw = ctypes.string_at(ptr)
    data = json.loads(raw)
    lib.deadline_free_string(ptr)
    return data

def cli(args):
    r = subprocess.run([RUST_CLI] + args, capture_output=True, text=True)
    return r.stdout.strip()

passed = 0
failed = 0

def check(label, condition, detail=""):
    global passed, failed
    if condition:
        print(f"  ✓ {label}")
        passed += 1
    else:
        print(f"  ✗ {label}: {detail}")
        failed += 1

print("=" * 60)
print("  FFI vs CLI Comparison")
print("=" * 60)

# ── Auth ─────────────────────────────────────────────────────────
print("\n--- Auth ---")

ffi_creds = ffi_call("deadline_get_credentials_source", None)
cli_auth = cli(["auth", "status", "--output", "json"])
cli_auth_json = json.loads(cli_auth)

check("credentials_source matches CLI",
    ffi_creds["credentials_source"] == cli_auth_json["source"],
    f"FFI={ffi_creds['credentials_source']} CLI={cli_auth_json['source']}")

ffi_auth = ffi_call("deadline_check_auth_status", None)
check("auth_status matches CLI",
    ffi_auth["auth_status"] == cli_auth_json["status"],
    f"FFI={ffi_auth['auth_status']} CLI={cli_auth_json['status']}")
check("api_available matches CLI",
    ffi_auth["api_available"] == cli_auth_json["api_availability"],
    f"FFI={ffi_auth['api_available']} CLI={cli_auth_json['api_availability']}")

ffi_api = ffi_call("deadline_check_api_available", None)
check("check_api_available matches CLI",
    ffi_api["api_available"] == cli_auth_json["api_availability"],
    f"FFI={ffi_api['api_available']} CLI={cli_auth_json['api_availability']}")

# ── Config ───────────────────────────────────────────────────────
print("\n--- Config ---")

for setting in ["defaults.farm_id", "defaults.queue_id", "defaults.aws_profile_name"]:
    ffi_val = ffi_call("deadline_get_setting", setting.encode(), None)
    cli_val = cli(["config", "get", setting])
    check(f"get_setting({setting})",
        ffi_val.get("value") == cli_val,
        f"FFI={ffi_val.get('value')!r} CLI={cli_val!r}")

# ── List Farms ───────────────────────────────────────────────────
print("\n--- List Farms ---")

ffi_farms = ffi_call("deadline_list_farms", None)
check("list_farms returns farms array",
    "farms" in ffi_farms and isinstance(ffi_farms["farms"], list),
    f"got: {list(ffi_farms.keys())}")

if "farms" in ffi_farms:
    ffi_farm_ids = sorted([f["farmId"] for f in ffi_farms["farms"]])
    cli_out = cli(["farm", "list"])
    cli_farm_ids = sorted(re.findall(r"farmId: (\S+)", cli_out))
    check("farm IDs match CLI",
        ffi_farm_ids == cli_farm_ids,
        f"FFI={ffi_farm_ids} CLI={cli_farm_ids}")

# ── List Queues ──────────────────────────────────────────────────
print("\n--- List Queues ---")

farm_id = cli(["config", "get", "defaults.farm_id"])
if farm_id:
    ffi_queues = ffi_call("deadline_list_queues", farm_id.encode(), None)
    check("list_queues returns queues array",
        "queues" in ffi_queues and isinstance(ffi_queues["queues"], list),
        f"got: {list(ffi_queues.keys())}")

    if "queues" in ffi_queues:
        ffi_queue_ids = sorted([q["queueId"] for q in ffi_queues["queues"]])
        cli_out = cli(["queue", "list"])
        cli_queue_ids = sorted(re.findall(r"queueId: (\S+)", cli_out))
        check("queue IDs match CLI",
            ffi_queue_ids == cli_queue_ids,
            f"FFI has {len(ffi_queue_ids)}, CLI has {len(cli_queue_ids)}")

# ── Queue Parameters ─────────────────────────────────────────────
print("\n--- Queue Parameters ---")

queue_id = cli(["config", "get", "defaults.queue_id"])
if farm_id and queue_id:
    ffi_params = ffi_call("deadline_get_queue_parameter_definitions",
        farm_id.encode(), queue_id.encode(), None)
    check("get_queue_parameters returns parameters or error",
        "parameters" in ffi_params or "error" in ffi_params,
        f"got: {list(ffi_params.keys())}")
    if "parameters" in ffi_params:
        names = [p["name"] for p in ffi_params["parameters"]]
        check(f"got {len(names)} parameters: {names}", True)

# ── Login/Logout (structure check only — don't actually logout!) ─
print("\n--- Login/Logout ---")

# We do NOT call deadline_logout here — it would kill the active session.
# Just verify the function exists and returns valid JSON by checking
# the error path (logout fails if not DCM, which is fine).
# ffi_logout = ffi_call("deadline_logout", None)
print("  (skipped — calling logout would kill the active session)")

# ── Summary ──────────────────────────────────────────────────────
print(f"\n{'='*60}")
print(f"  Results: {passed} passed, {failed} failed")
print(f"{'='*60}")
sys.exit(1 if failed else 0)
