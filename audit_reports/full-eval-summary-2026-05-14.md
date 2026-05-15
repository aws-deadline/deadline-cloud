# deadline-lib Full Module Evaluation Summary

**Date:** 2026-05-14
**Evaluator:** Kiro (automated)
**Tests:** 179 passed, 0 failed

## Module Grades (Priority Order — most issues first)

| Module | Grade | Critical | Important | Minor | Test Coverage |
|--------|-------|----------|-----------|-------|---------------|
| `bundle/hooks` | B | 1 (thread leak) | 3 | 3 | ❌ No tests |
| `bundle/submission` | B | 0 | 4 | 4 | Partial |
| `attachments/download` | B+ | 0 | 4 | 4 | Good |
| `attachments/upload` | B+ | 0 | 4 | 3 | Partial |
| `api/session` | A- | 0 | 3 | 3 | Good |
| `attachments/incremental_download` | A- | 0 | 2 | 1 | Partial |
| `attachments/manifest_ops` | B+ | 0 | 2 | 1 | Partial |
| `attachments/diff` | A | 0 | 1 | 2 | Good |
| `config` | A | 0 | 2 | 1 | Excellent |
| `bundle/parameters` | A | 0 | 1 | 2 | Excellent |

## Top Priority Actions

### 1. Fix thread leak in `bundle/hooks` (Critical)
The hook timeout implementation spawns a thread that is never joined. On timeout, SIGKILL is sent but the thread leaks. Fix by joining after kill or migrating to tokio async process.

### 2. Add tests for `bundle/hooks` (Important)
The hooks module has zero unit tests despite complex logic (timeout handling, payload merging, command resolution, path resolution). This is the largest test coverage gap.

### 3. Cache regex compilations in `attachments/download` (Important)
`fnmatch` compiles a new regex per filter per file. For 10K files × 5 filters = 50K regex compilations. Cache compiled patterns.

### 4. Reduce mutex hold duration in `api/session` (Important)
The global `SESSION` mutex is held across `.await` points during initial config load, blocking all concurrent API callers.

### 5. Extract duplicated utilities (Quick wins)
- `normalize_path` exists in both `submission.rs` and `download.rs`
- `op_err` helper is defined in 4+ modules
- `LazyLock` missing for regex patterns compiled in hot paths

## Cross-Cutting Observations

### Strengths
- **Correctness**: All 179 tests pass. No logic errors found.
- **Error messages**: Actionable, user-facing error messages throughout.
- **Python parity**: Observable behavior matches the Python client.
- **Atomic file writes**: Config and checkpoint files use temp+rename pattern.
- **Path security**: `ensure_paths_within_directory` prevents path traversal.

### Weaknesses
- **Test coverage gaps**: `bundle/hooks`, `OutputDownloader`, `InputDownloader`, `select_latest_manifests_per_task` have no direct tests.
- **Function size**: `create_job_from_job_bundle` is ~500 lines. Several other functions exceed 100 lines.
- **Parameter counts**: Multiple functions take 7-8 parameters without options structs.
- **Duplicated code**: `normalize_path`, `op_err`, regex compilation patterns repeated across modules.

### Architecture
The crate follows the spec's guidance well:
- No mocking — real temp dirs and stub servers
- Globals earn their keep (session cache, lazy regexes)
- Observable behavior preserved from Python
- Clean module boundaries with minimal cross-module coupling

## Individual Reports

See `audit_reports/` for detailed per-module reports:
- `bundle-hooks-eval-2026-05-14.md`
- `bundle-submission-eval-2026-05-14.md`
- `attachments-download-eval-2026-05-14.md`
- `attachments-upload-eval-2026-05-14.md`
- `api-session-eval-2026-05-14.md`
- `attachments-incremental-download-eval-2026-05-14.md`
- `attachments-manifest-ops-eval-2026-05-14.md`
- `attachments-diff-eval-2026-05-14.md`
- `config-eval-2026-05-14.md`
- `bundle-parameters-eval-2026-05-14.md`
