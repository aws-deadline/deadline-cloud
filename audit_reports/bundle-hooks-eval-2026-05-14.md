# bundle/hooks Evaluation Report

**Date:** 2026-05-14
**Module:** `bundle/hooks`
**Files reviewed:** `src/bundle/hooks.rs`

## Summary
Submission hook framework (54KB) for executing pre/post-submission scripts. Well-structured with clear separation between validation, execution, and merging. The timeout implementation has a potential resource leak. Grade: **B**.

## Findings

### Critical (must fix)

1. **Thread leak on hook timeout** (`hooks.rs:~560-580`): When a hook times out, the spawned thread (which owns the `Child` process handle) is never joined. The thread continues running until the child process eventually exits or is killed. On Unix, `SIGKILL` is sent, but the thread itself leaks. If many hooks timeout in sequence, this accumulates orphaned threads.

   **Fix:** Store the `JoinHandle` and join it after sending SIGKILL, or use `tokio::process::Command` with `tokio::time::timeout` instead of spawning a std thread.

### Important (should fix)

2. **`resolve_args` resolves relative paths against `script_dir`** (`hooks.rs:~500`): Any argument that happens to match a file in the script directory gets its path rewritten. This is surprising — if a hook has `args: ["--config", "default"]` and a file named `default` exists in the bundle dir, the arg becomes an absolute path. The Python client has the same behavior, but it's a footgun.

3. **`from_dict` uses `unwrap_or("")` for command** (`hooks.rs:~40`): If `command` is missing from the JSON, `HookDefinition` silently gets an empty string. This will fail later at `resolve_command` with a confusing "not found" error. Better to validate at parse time (though `validate_configuration` catches this separately, the two paths are disconnected).

4. **No structured error type for hook failures**: All hook errors use `DeadlineError::OperationError(String)`. A dedicated `HookError` variant with fields (hook_index, hook_name, exit_code, stderr) would enable better error handling by callers.

### Minor (nice to have)

5. **`pid as i32` cast** (`hooks.rs:~575`): `Child::id()` returns `u32`. Casting to `i32` for `libc::kill` is technically correct for valid PIDs but could overflow for PIDs > i32::MAX (extremely unlikely on any real system).

6. **`generate_hooks_confirmation_message` builds strings with `push`**: Could use `writeln!` to a `String` for cleaner formatting, but this is cosmetic.

7. **Post-submission hook failures are silently logged**: This is by design (matching Python), but there's no way for callers to know a post-hook failed. Consider returning a `Vec<HookResult>` from `execute_post_submission_hooks`.

## Test Coverage Assessment

| Function/Area | Happy path | Error cases | Edge cases |
|---|---|---|---|
| `validate_configuration` | ❌ | ❌ | ❌ |
| `validate_hook` | ❌ | ❌ | ❌ |
| `execute_hook` | ❌ | ❌ | ❌ |
| `merge_payload` | ❌ | ❌ | ❌ |
| `merge_asset_references` | ❌ | ❌ | ❌ |
| `resolve_command` | ❌ | ❌ | ❌ |
| `HookManager` | ❌ | ❌ | ❌ |

**No unit tests exist for this module.** This is the biggest gap. The hooks module has complex logic (timeout handling, payload merging, command resolution) that is difficult to test at the CLI level alone.

## Recommended Changes

1. [M] Fix thread leak: join the spawned thread after SIGKILL, or migrate to tokio async process
2. [M] Add Level 1 tests for `validate_configuration`, `merge_payload`, `resolve_command`
3. [S] Add a `HookError` variant to `DeadlineError` with structured fields
4. [S] Validate `command` is non-empty in `HookDefinition::from_dict`
5. [L] Add integration tests for hook execution with real scripts in a temp dir
