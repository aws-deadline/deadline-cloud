# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

## Active Work Item

**15e — Behavioral parity audit (Batch 2: Bug & Problem Fixes)**

Batch 2 complete (sub-batches A–C). Sub-batch D (correctness fixes)
in progress. Sub-batch E (output parity) not started.

### Batch 2 — Fixed (Sub-batches A–C)

| ID | Fix |
|----|-----|
| AUDIT-001 | `queue sync-output` now downloads files via S3 manifest pipeline |
| AUDIT-018 | INI parser accepts `:` delimiter (matching Python ConfigParser) |
| AUDIT-019 | INI parser uses IndexMap — preserves section/key insertion order |
| AUDIT-035 | Path traversal validation before download (`ensure_paths_within_directory`) |
| AUDIT-044 | INI parser supports multiline continuation values |
| AUDIT-051 | Resolved — `CreateCopy` collision handling + manifest merge dedup already implemented in AUDIT-001 |

### Sub-batch D — Correctness fixes (DONE)

| ID | Title | Crate | Status |
|----|-------|-------|--------|
| AUDIT-007 | `queue sync-output` job discovery limited to 100 | deadline-api + deadline-cli | ✅ Fixed |
| AUDIT-020 | `suggest_resources` dispatch gives wrong suggestions | deadline-cli | ✅ Fixed |
| AUDIT-036 | Download manifest merge order by S3 LastModified | deadline-job-attachments + deadline-cli | ✅ Fixed |

### Sub-batch E — Output parity (NOT STARTED)

| ID | Title |
|----|-------|
| SYNC-001 | Session action count includes no-output actions |
| SYNC-002 | Missing "Manifest file system paths" per-job output |
| SYNC-003 | Missing WARNING for jobs with no output manifests |
| SYNC-004 | Path summary shows aggregate instead of per-file listing |

## Critical Context

1. **Python source is at `../deadline-cloud-python`** (sibling directory).
2. **All tests pass.** 1055 tests across all crates.
3. **Testing rules:** Level 2 CLI tests must exercise the full stack through
   the CLI binary. No stubs at the application layer — only HTTP stub server.
4. **Serialization patterns.** See `specs/patterns.md`.
5. **`serde_json` has `preserve_order` enabled** — `json!()` preserves
   insertion order, not alphabetical.

## Audit Triage Summary

### Batch 1 — Complete (quick fixes & CLI polish)

8 fixed, 5 no-issue, 1 deferred (AUDIT-056 → migrated to progress.md).

| ID | Verdict |
|----|---------|
| AUDIT-016 | Fixed — `auth status` JSON spacing |
| AUDIT-017 | No Issue — Rust is more correct |
| AUDIT-021 | No Issue — Rust error model has no "unexpected exceptions" |
| AUDIT-022 | Fixed — added `storage_profile_id` and `conflict_resolution` to CliOptions |
| AUDIT-023 | Fixed — negative timedelta formatting |
| AUDIT-025 | Fixed — worker suggest_resources |
| AUDIT-032 | Fixed — bundle submit suggest_resources |
| AUDIT-033 | No Issue — CLI already sets `defaults.job_id` equivalently |
| AUDIT-037 | Fixed — confirmation prompt re-prompt loop |
| AUDIT-038 | Fixed — INTERRUPTING and NOT_COMPATIBLE in task summary |
| AUDIT-045 | Fixed — version format matches Python |
| AUDIT-050 | No Issue — behavior is equivalent |
| AUDIT-052 | No Issue — Python also rejects empty paths |
| AUDIT-056 | Migrated to progress.md #9 |

### Pre-Batch 1 fixes (from initial audit pass)

| ID | Verdict |
|----|---------|
| AUDIT-002 | Fixed — `known_asset_paths` separator |
| AUDIT-003 | Fixed — `auto_accept` + unknown paths |
| AUDIT-004 | Fixed — INI key case sensitivity |
| AUDIT-005 | Fixed — default profile DCM detection |
| AUDIT-015 | Fixed — `auth status --output` case sensitivity |
| AUDIT-027 | Fixed — `job cancel --mark-as` validation |
| AUDIT-028 | Fixed — `job requeue-tasks --run-status` validation |
| AUDIT-029 | Fixed — `job logs` relative timestamp for auto-selected sessions |

### Feature gaps — Migrated to progress.md

These are not bugs — they are missing features tracked as gaps on their
existing work items. See `progress.md` "Audit gaps in completed items"
section for the full mapping.

AUDIT-006, 008, 009, 010, 011, 012, 013, 014, 026, 030, 031, 034,
040, 041, 042, 043, 047, 048, 049, 053, 054, 055.

### Accepted differences — Dropped

| ID | Reason |
|----|--------|
| AUDIT-024 | YAML key ordering — accepted per `patterns.md` |
| AUDIT-039 | `job wait` verbose to stderr — Rust approach is better |
| AUDIT-046 | `require_setting` exit code — function is unused dead code |

### Batch 2 — Bug & problem fixes (CURRENT)

9 findings + 4 output polish items found during CLI comparison.

| ID | Priority | Title | Category |
|----|----------|-------|----------|
| AUDIT-001 | Critical | `queue sync-output` doesn't download files | Bug |
| AUDIT-007 | High | `queue sync-output` job discovery limited to 100 | Bug |
| AUDIT-018 | Medium | INI colon delimiter not supported | Bug — silently loses config |
| AUDIT-019 | Medium | INI section/key ordering changes on write | Bug — corrupts user files |
| AUDIT-020 | Medium | `suggest_resources` dispatch gives wrong suggestions | Error handling bug |
| AUDIT-035 | Medium | Download path traversal not validated | Security bug |
| AUDIT-036 | Medium | Download manifest merge order differs | Correctness bug |
| AUDIT-044 | Low | INI multiline values not supported | Bug — silently loses config |
| AUDIT-051 | Low | Download duplicate path collision not handled | Correctness bug |
| SYNC-001 | Low | `sync-output` session action count includes no-output actions | Output parity |
| SYNC-002 | Low | `sync-output` missing "Manifest file system paths" per-job output | Output parity |
| SYNC-003 | Low | `sync-output` missing WARNING for jobs with no output manifests | Output parity |
| SYNC-004 | Low | `sync-output` path summary shows aggregate instead of per-file listing | Output parity |

## Implementation Plan — Sub-batch D

### Crate/Module Changes

| Crate | Module | Changes |
|-------|--------|---------|
| `deadline-api` | `api.rs` | Add `groupFilter` to `build_filter_expressions`; new `list_jobs_by_filter_expression` pagination function |
| `deadline-job-attachments` | `download.rs` | `download_manifest_from_s3` returns S3 `LastModified`; `get_output_manifests_by_asset_root` sorts by timestamp before merging |
| `deadline-cli` | `commands/queue.rs` | Replace hardcoded `search_jobs_with_filters(..., 0, 100)` with `list_jobs_by_filter_expression`; use real `LastModified` from manifest downloads |
| `deadline-cli` | `commands/helpers.rs` | Rewrite `suggest_resources_on_client_error` to dispatch on operation name |
| `deadline-cli` | `commands/*.rs` | All callers pass operation name string to `suggest_resources_on_client_error` |

### Cross-Reference: Test Spec → Planned Rust Tests

| Finding | Test Spec Case | Planned Rust Test Name |
|---------|---------------|----------------------|
| AUDIT-007 | cli.md §42 (new) | `sync_output_paginates_beyond_100_jobs` (L2) |
| AUDIT-007 | (unit) | `list_jobs_by_filter_expression_single_page` |
| AUDIT-007 | (unit) | `list_jobs_by_filter_expression_multi_page_deduplicates` |
| AUDIT-020 | cli.md §37 case 48 | `suggest_resources_get_queue_error_lists_queues` (L2) |
| AUDIT-020 | cli.md §37 case 49 | `suggest_resources_get_farm_error_lists_farms` (L2) |
| AUDIT-020 | cli.md §37 case 50 | `suggest_resources_get_job_error_lists_jobs_then_queues` (L2) |
| AUDIT-020 | (new) | `suggest_resources_get_fleet_error_lists_fleets` (L2) |
| AUDIT-036 | data_transfer.md case 36 | `manifest_merge_newer_overwrites_older` (L1) |
| AUDIT-036 | data_transfer.md case 39 | `manifest_merge_sorts_by_last_modified` (L1) |
| AUDIT-036 | (new) | `download_manifest_from_s3_returns_last_modified` (L1) |

### Key Design Decisions

1. **AUDIT-007 (job pagination):** New function
   `list_jobs_by_filter_expression(farm_id, queue_id, filter, config)`
   in `deadline-api`. Ports Python's `createdAt` thresholding algorithm:
   - First call: user filter only, sorted `CREATED_AT ASC`, no offset
   - If `len(jobs) < totalResults`: take last job's `createdAt`, wrap
     user filter in a `groupFilter`, AND with `dateTimeFilter(CREATED_AT
     >= threshold)`, repeat
   - Dedup by `jobId` (HashMap)
   - Edge case: raise error if all 100 jobs have identical `createdAt`
   - Requires `groupFilter` support in `build_filter_expressions`
     (currently missing — the SDK has `SearchFilterExpression::GroupFilter`
     but the JSON→SDK builder doesn't handle it)

2. **AUDIT-020 (suggest_resources):** Rewrite the function entirely.
   New signature: `suggest_resources_on_client_error(error_msg,
   operation_name, farm_id?, queue_id?, fleet_id?, config?)`.
   Dispatch table matching Python's `_OPERATION_GROUPS`:

   | Operation group | Operations | Suggestion chain |
   |----------------|------------|-----------------|
   | queue | GetQueue, ListQueues, ListQueueEnvironments | queues → farms |
   | farm | GetFarm, ListFarms | farms |
   | fleet | GetFleet, ListFleets | fleets → farms |
   | worker | GetWorker, SearchWorkers | workers → fleets |
   | job | GetJob, ListJobs, SearchJobs | jobs → queues → farms |
   | storage_profile | GetStorageProfileForQueue, ListStorageProfilesForQueue | storage profiles |

   Broken cases in current code (greedy dispatch):
   - `fleet get` (GetFleet): has farm+fleet → tries workers first (wrong, should try fleets)
   - `queue get` (GetQueue): has farm+queue → tries jobs first (wrong, should try queues)
   - `queue paramdefs` (ListQueueEnvironments): same as above

   All ~15 callers updated to pass operation name. No backwards compat
   needed — just replace the function.

3. **AUDIT-036 (manifest merge order):** Two changes:
   - `download_manifest_from_s3` returns `(Option<String>, DateTime<Utc>,
     AssetManifest)` — extracts `LastModified` from S3 `GetObjectOutput`.
     The SDK field is `Option<aws_smithy_types::DateTime>`, convert via
     epoch seconds to `chrono::DateTime<Utc>`.
   - `get_output_manifests_by_asset_root` sorts manifests by `LastModified`
     before merging (oldest first, newer wins). Currently uses arbitrary
     S3 listing order.
   - `queue.rs::incremental_output_download` uses the real `LastModified`
     instead of `Utc::now()` when building the `downloaded_manifests` vec.

## Step Status — Sub-batch D

- [x] Step 1: Study Python — complete
- [x] Step 2: Write tests (red) — complete
- [x] Step 3: Implement fixes — complete
- [x] Step 4: Compare CLIs — complete
- [x] Step 5: Audit & fix — complete (1 finding: error format diff documented)
- [x] Step 6: Spec — complete
- [x] Step 7: Commit — awaiting review
