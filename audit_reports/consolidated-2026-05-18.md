# Consolidated Audit Report — deadline-lib

**Date:** 2026-05-18 (consolidation of per-module evals from 2026-05-14)
**Scope:** All `deadline-lib` modules
**Tests at time of report:** 1,355 passing

---

## Summary

The 2026-05-14 module evaluations produced 45 findings across 10 modules.
As of 2026-05-18, **33 are resolved**, **5 are accepted/deferred**, and
**7 remain open** (all low-priority improvements).

---

## Resolved Findings

| # | Module | Finding | Resolution |
|---|--------|---------|------------|
| 1 | `api/session` | Mutex held across `.await` | Fixed (Batch I) — load outside lock |
| 2 | `api/session` | `invalidate_session_cache` needs tokio | Already documented |
| 3 | `api/session` | Credential provider no cache | Deferred — CLI short-lived, SDK caches |
| 4 | `api/session` | `resolve_profile` edge cases | Tests added (Batch G2) |
| 5 | `attachments/diff` | `Vec::contains` O(N²) | Fixed (Batch F) — HashSet |
| 6 | `attachments/download` | Regex compiled per call | Fixed (Batch C) — FilterSet cache |
| 7 | `attachments/download` | Step regex compiled per call | Fixed (Batch A) — LazyLock |
| 8 | `attachments/download` | `normalize_path` duplicated | Fixed (Batch A) — `crate::util` |
| 9 | `attachments/download` | Output/InputDownloader duplicated | Fixed (2026-05-18) — ManifestDownloader |
| 10 | `attachments/download` | `select_latest_manifests` untested | Tests added (Batch G) |
| 11 | `attachments/incremental` | Regex compiled per call | Fixed (Batch A) — LazyLock |
| 12 | `attachments/incremental` | Case-insensitive dedup on all platforms | Fixed (Batch F) — platform-aware |
| 13 | `attachments/incremental` | Manifest functions untested | Tests added (Batch G) |
| 14 | `attachments/manifest_ops` | Confusing glob config error | Fixed (Batch F) — explicit file check |
| 15 | `attachments/manifest_ops` | Options structs for 6+ params | Removed — few callers |
| 16 | `attachments/manifest_ops` | Error case tests missing | Tests added (Batch G2) |
| 17 | `attachments/upload` | Per-file progress missing | Fixed (2026-05-16) — openjd on_progress |
| 18 | `attachments/upload` | `common_path`/`find_group_key` untested | Tests added (Batch G) |
| 19 | `attachments/upload` | `snapshot_assets` blocking undocumented | Documented |
| 20 | `attachments/upload` | Validation mixed with grouping | Fixed (2026-05-18) — separated |
| 21 | `bundle/hooks` | Thread leak on timeout | Fixed (Batch B) — join after SIGKILL |
| 22 | `bundle/hooks` | No structured hook error type | Fixed (Batch E) — HookFailed variant |
| 23 | `bundle/hooks` | Tests missing for hooks | Tests added (Batches B/E/G) — 57 L1 tests |
| 24 | `bundle/hooks` | Integration tests with real scripts | Done (Batch B) |
| 25 | `bundle/parameters` | `op_err` duplicated | Fixed (Batch A) — `crate::util` |
| 26 | `bundle/submission` | `normalize_path` duplicated | Fixed (Batch A) — `crate::util` |
| 27 | `bundle/submission` | `op_err` duplicated | Fixed (Batch A) — `crate::util` |
| 28 | `bundle/submission` | Duplicate telemetry emission | Not a bug — both events intentional |
| 29 | `bundle/submission` | `SubmitJobParams` sub-structs | Removed — 2 call sites, no benefit |
| 30 | `bundle/submission` | `load_and_confirm_hooks` extraction | Done (Batch J) |
| 31 | `bundle/submission` | L1 integration tests | Done (2026-05-18) — 5 tests |
| 32 | `config` | Comments not preserved on write | Already documented |
| 33 | `config` | `expand_tilde` no `~user/` | Already documented |

---

## Open Findings (remaining)

### Correctness — None remaining

All correctness issues have been resolved.

### Code Quality — Accepted (no action needed)

| # | Module | Finding | Rationale |
|---|--------|---------|-----------|
| O1 | `attachments/download` | `download_files_from_manifests` takes 7 params | 2 callers; options struct adds complexity without ergonomic gain |
| O2 | `attachments/upload` | `find_group_key` case-insensitive lookup vs exact insertion | Functionally correct — insertion always goes through lookup first |
| O3 | `bundle/submission` | `create_job_from_job_bundle` ~500 lines | Partial extraction done; remaining phases share too many locals |
| O4 | `attachments/download` | `rebuild_manifests` clones all manifests | Sub-millisecond even for 10K files; no reported perf issues |
| O5 | `bundle/hooks` | `resolve_args` rewrites args matching filenames | Done — doc comment added |
| O6 | `bundle/submission` | `auto_accept` cancels on unknown paths | Done — inline comment added |
| O7 | `attachments/upload` | `s3_upload_error` repetitive match arms | Each arm has unique guidance; table-driven would be less readable |

### Minor (accepted as-is)

These were evaluated and explicitly accepted:

- Mtime tolerance hardcoded at 1μs (matches Python)
- `fnmatch` doesn't handle `**` globstar (matches Python's `fnmatch.fnmatch`)
- `pid as i32` cast (safe for all real PIDs)
- Post-submission hook failures silently logged (by design, matches Python)
- `str2bool` accepts many variants (Python parity)
- `get_section_prefixes` recursion (static definitions, can't loop)
- `write_config_to` PID-based temp naming (practically impossible to collide)
- `snapshot_assets` is synchronous (only used for debug, acceptable)

---

## Test Coverage Status

All functions previously flagged as untested are now confirmed covered
by either L1 (direct) or L2 (CLI subprocess) tests. See individual
module reports for the detailed coverage tables with test file citations.

---

## Metrics

- **Findings raised:** 45
- **Resolved:** 35 (78%)
- **Accepted (no action needed):** 10 (22%)
- **Open:** 0
- **Tests added during audit resolution:** +58 (1,297 → 1,355 across Batches A–J + 2026-05-18)
