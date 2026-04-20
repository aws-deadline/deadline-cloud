# Submission Hooks — Test Specification

> Part of [AWS Deadline Cloud Client — Behavioral Test Specification](index.md)
>
> Work item: #18
> Python source: `deadline.client.job_bundle._hooks`
> Python tests: `test/unit/deadline_client/job_bundle/test_hooks.py`

---

## Section 54: Hook data models

> **Rust crate:** `deadline-job-bundle` · **Module:** `hooks` (new)
>
> **Logic under test:** Parsing hook definitions, configurations, metadata
> serialization, and result evaluation.

### `HookDefinition::from_dict(data)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Minimal dict with only `command` | `args=[]`, `timeout=60`, `env={}` | |
| 2 | Happy path | Full dict with all fields | All fields populated correctly | |

### `HookConfiguration::from_dict(data)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 3 | Happy path | Empty dict | `pre_submission=[]`, `post_submission=[]`, `version="1.0"` | |
| 4 | Happy path | Dict with explicit version | Version preserved | |
| 5 | Happy path | Dict with both pre and post hooks | Both lists populated with correct counts | |

### `HookMetadata`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 6 | Happy path | `to_dict()` with all fields | All fields serialized with camelCase keys | |
| 7 | Happy path | `to_dict()` without optional fields | `storageProfileId` and `jobId` absent | |
| 8 | Happy path | `to_json()` | Valid JSON string, round-trips through parse | |
| 9 | Happy path | `to_environment_variables()` with all fields | All `DEADLINE_*` env vars present | |
| 10 | Happy path | `to_environment_variables()` without optional fields | `DEADLINE_STORAGE_PROFILE_ID` and `DEADLINE_JOB_ID` absent | |

### `HookResult`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 11 | Happy path | `exit_code=0`, `timed_out=false` | `is_success()` returns true | |
| 12 | Happy path | `exit_code=1`, `timed_out=false` | `is_success()` returns false | |
| 13 | Happy path | `exit_code=0`, `timed_out=true` | `is_success()` returns false | |

---

## Section 55: Hook configuration validation

> **Rust crate:** `deadline-job-bundle` · **Module:** `hooks::validator`
>
> **Logic under test:** Structural validation of hooks.yaml/json content.

### `validate_configuration(data)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 14 | Happy path | Valid config with pre and post hooks | No error | |
| 15 | Error handling | `preSubmission` is not a list | Error "must be a list" | |
| 16 | Error handling | Hook entry is not a dict | Error "must be an object" | |
| 17 | Error handling | Hook missing `command` | Error "missing required 'command'" | |
| 18 | Error handling | `command` is not a string | Error "'command' must be a string" | |
| 19 | Error handling | `args` is not a list | Error "'args' must be a list" | |
| 20 | Error handling | `timeout` is zero | Error "'timeout' must be a positive integer" | |
| 21 | Error handling | `timeout` is negative | Error "'timeout' must be a positive integer" | |
| 22 | Error handling | `env` is not a dict | Error "'env' must be an object" | |
| 23 | Happy path | Valid version "1.0" | No error | |
| 24 | Error handling | Unsupported version "2.0" | Error "Unsupported hooks version" | |

### `validate_modified_payload(payload, hook_name)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 25 | Happy path | Valid dict payload | No error | |
| 26 | Error handling | Payload is not a dict | Error "must be a JSON object" | |
| 27 | Error handling | `attachments` is not a dict | Error "'attachments' must be an object" | |
| 28 | Error handling | `assetReferences` is not a dict | Error "'assetReferences' must be an object" | |
| 29 | Error handling | `inputFilenames` is not a list | Error "must be a list" | |

---

## Section 56: Hook payload merging

> **Rust crate:** `deadline-job-bundle` · **Module:** `hooks::merger`
>
> **Logic under test:** Merging hook output into the submission payload.

### `merge_asset_references(original, modified)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 30 | Happy path | Both None | Returns empty dict | |
| 31 | Happy path | Only original | Returns original | |
| 32 | Happy path | Only modified | Returns modified | |
| 33 | Happy path | Both present, modified replaces nested keys | Modified values win | |
| 34 | Happy path | All four fields (`inputFilenames`, `inputDirectories`, `outputDirectories`, `referencedPaths`) | Each field replaced independently | |

### `merge_payload(original, modified)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 35 | Happy path | Simple field override | Modified field wins, others preserved | |
| 36 | Happy path | New field added | Both original and new fields present | |
| 37 | Happy path | Asset references merged | `attachments.assetReferences` replaced, other attachment fields preserved | |

---

## Section 57: Hook loading and execution

> **Rust crate:** `deadline-job-bundle` · **Module:** `hooks::manager`, `hooks::executor`
>
> **Logic under test:** Loading hooks from YAML/JSON files, executing
> subprocess hooks, passing metadata via stdin and env vars, handling
> timeouts and failures.

### `HookManager::load_hooks()`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 38 | Happy path | No hooks file in bundle dir | Returns None | |
| 39 | Happy path | `hooks.yaml` exists | Returns parsed HookConfiguration | |
| 40 | Happy path | `hooks.json` exists | Returns parsed HookConfiguration | |
| 41 | Error handling | Both `hooks.yaml` and `hooks.json` exist | Error "both hooks.json and hooks.yaml" | |

### `HookManager::execute_pre_submission_hooks(metadata, payload)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 42 | Happy path | Hook exits 0 with no stdout | Payload unchanged | |
| 43 | Happy path | Hook outputs JSON to stdout | Payload merged with hook output | |
| 44 | Error handling | Hook exits non-zero | Error "failed with exit code" | Blocks submission |
| 45 | Error handling | Hook exceeds timeout | Error "timed out" | |
| 46 | Error handling | Hook outputs invalid JSON | Error "produced invalid JSON" | |
| 47 | Happy path | Hook receives metadata via stdin | Hook can read `jobName` from stdin JSON | |
| 48 | Happy path | Hook receives `DEADLINE_*` env vars | Hook can read `DEADLINE_JOB_NAME` etc. | |
| 49 | Happy path | Hook receives custom `env` vars | Hook can read custom env vars from hook definition | |
| 50 | Error handling | Hook command not found | Error "not found" | |
| 51 | Happy path | Hook with absolute command path | Executes successfully | |
| 52 | Happy path | `.hooks_origin` file redirects script resolution | Scripts resolved from original bundle dir | |

### `HookManager::execute_post_submission_hooks(metadata)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 53 | Happy path | Hook exits non-zero | No error raised, only warning logged | |
| 54 | Happy path | Hook exceeds timeout | No error raised, only warning logged | |
| 55 | Happy path | Hook outputs to stdout | Output logged | |
| 56 | Happy path | Post hooks not called on CreateJob failure | No side effects | |
| 57 | Happy path | Post hooks called after successful CreateJob | Hook executes, side effects visible | |

### `_generate_hooks_confirmation_message(hooks, bundle_dir)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 58 | Happy path | Config with pre and post hooks | Message lists both sections with commands and bundle path | |

---

## Section 58: Hook integration in submission flow (Level 2 CLI)

> **Rust crate:** `deadline-cli` · **Test level:** Level 2 (CLI subprocess)
>
> **Logic under test:** End-to-end hook execution during `bundle submit`,
> config gating, environment hooks, confirmation prompts.

### `deadline bundle submit` with hooks

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 59 | Happy path | Bundle with `hooks.yaml`, `settings.allow_bundle_hooks=true` | Pre-hooks run, submission proceeds | |
| 60 | Happy path | Bundle with `hooks.yaml`, `settings.allow_bundle_hooks=false` | Hooks skipped, note printed about enabling | |
| 61 | Happy path | `DEADLINE_HOOKS_DIR` set, `settings.allow_environment_hooks=true` | Env hooks run | |
| 62 | Happy path | `DEADLINE_HOOKS_DIR` set, `settings.allow_environment_hooks=false` | Env hooks skipped, warning printed | |
| 63 | Happy path | Both bundle and env hooks enabled | Env hooks run first, then bundle hooks | |
| 64 | Happy path | Pre-hook modifies payload, `--yes` | Modified payload used for CreateJob | |
| 65 | Error handling | Pre-hook fails, `--yes` | Submission canceled with hook error | |
| 66 | Happy path | Post-hook runs after successful submission | Post-hook executes, job ID available in metadata | |
| 67 | Happy path | Hooks confirmation prompt shown (not `--yes`) | User sees hook list before execution | |
