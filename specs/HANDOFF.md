# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

## Active Work Item

**15e — Behavioral parity audit (Batch 2: Bug & Problem Fixes)**

Batch 2 partially complete. 5 of 9 audit findings fixed + 4 output
parity items identified during CLI comparison.

### Batch 2 — Fixed

| ID | Fix |
|----|-----|
| AUDIT-001 | `queue sync-output` now downloads files via S3 manifest pipeline |
| AUDIT-018 | INI parser accepts `:` delimiter (matching Python ConfigParser) |
| AUDIT-019 | INI parser uses IndexMap — preserves section/key insertion order |
| AUDIT-035 | Path traversal validation before download (`ensure_paths_within_directory`) |
| AUDIT-044 | INI parser supports multiline continuation values |

### Batch 2 — Remaining

| ID | Title |
|----|-------|
| AUDIT-007 | `queue sync-output` job discovery limited to 100 |
| AUDIT-020 | `suggest_resources` dispatch (deferred — no user-visible bug) |
| AUDIT-036 | Download manifest merge order by S3 LastModified |
| AUDIT-051 | Download duplicate path collision handling |
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

## Implementation Plan — Batch 2

### Batching Strategy

9 findings split into 3 sub-batches by crate/module affinity:

**Sub-batch A: INI parser bugs (deadline-config)**
- AUDIT-018 — colon delimiter support
- AUDIT-019 — preserve section/key ordering on write
- AUDIT-044 — multiline value support

**Sub-batch B: Download correctness bugs (deadline-job-attachments)**
- AUDIT-035 — path traversal validation
- AUDIT-036 — manifest merge by S3 LastModified (not lexicographic)
- AUDIT-051 — duplicate path collision handling

**Sub-batch C: sync-output + suggest_resources (deadline-cli)**
- AUDIT-001 — queue sync-output doesn't download files
- AUDIT-007 — job discovery limited to 100 (needs `_list_jobs_by_filter_expression` port)
- AUDIT-020 — suggest_resources dispatch gives wrong suggestions

### Crate/Module Changes

| Crate | Module | Changes |
|-------|--------|---------|
| `deadline-config` | `ini.rs` | Add `:` delimiter, `IndexMap` for ordering, multiline continuation |
| `deadline-job-attachments` | `download.rs` | Add `ensure_paths_within_directory()`, sort manifests by S3 `LastModified` |
| `deadline-job-attachments` | `download.rs` | Add duplicate path collision prefixing in `OutputDownloader` |
| `deadline-cli` | `commands/queue.rs` | Wire actual S3 downloads in `incremental_output_download` |
| `deadline-cli` | `commands/queue.rs` | Port `_list_jobs_by_filter_expression` pagination with `createdAt` thresholding |
| `deadline-cli` | `commands/helpers.rs` | Refactor `suggest_resources` to dispatch by operation name |

### Cross-Reference: Test Spec → Planned Rust Tests

#### Sub-batch A: INI parser

| Finding | Test Spec Case | Planned Rust Test Name |
|---------|---------------|----------------------|
| AUDIT-018 | config.md (new) | `parse_colon_delimiter_accepted` |
| AUDIT-018 | config.md (new) | `parse_colon_and_equals_mixed` |
| AUDIT-019 | config.md (new) | `roundtrip_preserves_section_order` |
| AUDIT-019 | config.md (new) | `roundtrip_preserves_key_order` |
| AUDIT-019 | cli.md §config | `config_set_preserves_existing_key_order` (L2) |
| AUDIT-044 | config.md (new) | `parse_multiline_continuation` |
| AUDIT-044 | config.md (new) | `parse_multiline_with_empty_continuation` |

#### Sub-batch B: Download correctness

| Finding | Test Spec Case | Planned Rust Test Name |
|---------|---------------|----------------------|
| AUDIT-035 | job_attachments_data_transfer.md (new) | `download_rejects_path_traversal` |
| AUDIT-035 | job_attachments_data_transfer.md (new) | `download_accepts_paths_within_root` |
| AUDIT-036 | job_attachments_data_transfer.md (new) | `manifest_merge_sorts_by_last_modified` |
| AUDIT-036 | job_attachments_data_transfer.md (new) | `manifest_merge_newer_overwrites_older` |
| AUDIT-051 | job_attachments_data_transfer.md (new) | `duplicate_path_collision_prefixed` |
| AUDIT-051 | job_attachments_data_transfer.md (new) | `no_collision_when_paths_differ` |

#### Sub-batch C: sync-output + suggest_resources

| Finding | Test Spec Case | Planned Rust Test Name |
|---------|---------------|----------------------|
| AUDIT-001 | cli.md §42 case 14 | `sync_output_downloads_files` (L2) |
| AUDIT-001 | cli.md §42 (new) | `sync_output_dry_run_shows_files_to_download` (L2) |
| AUDIT-007 | cli.md §42 (new) | `sync_output_paginates_beyond_100_jobs` (L2) |
| AUDIT-020 | cli.md (new) | `suggest_resources_farm_error_lists_farms` (L2) |
| AUDIT-020 | cli.md (new) | `suggest_resources_queue_error_lists_queues` (L2) |

### Key Design Decisions

1. **AUDIT-019 (INI ordering):** Replace `BTreeMap` with `IndexMap` in
   `IniConfig`. This preserves insertion order while still allowing
   O(1) lookups. New sections/keys append to end (matching Python's
   `ConfigParser`). Existing sections/keys update in-place.

2. **AUDIT-036 (manifest merge order):** The `download_manifest_from_s3`
   function must return the S3 `LastModified` timestamp alongside the
   manifest. `get_output_manifests_by_asset_root` sorts by this
   timestamp before merging (oldest first, newer wins).

3. **AUDIT-001 (sync-output downloads):** The current Rust code collects
   session actions but never calls the download functions. Need to:
   (a) download output manifests for each session action,
   (b) apply path mapping rules,
   (c) merge manifests chronologically,
   (d) call `download_files` for the merged manifest paths.
   This mirrors Python's `_incremental_output_download` flow.

4. **AUDIT-007 (job pagination):** Port Python's
   `_list_jobs_by_filter_expression` algorithm that uses `createdAt`
   thresholding to paginate through all matching jobs. Replace the
   two hardcoded `search_jobs_with_filters(..., 0, 100, ...)` calls.

5. **AUDIT-020 (suggest_resources):** Add operation name tracking to
   API error paths. Dispatch suggestion chains based on which API
   operation failed (matching Python's `exc.operation_name` approach)
   instead of the current greedy approach based on available IDs.

## Step Status

- [x] Step 1: Study Python — complete
- [x] Step 2: Write tests (red) — complete
- [x] Step 3: Implement fixes — complete
- [x] Step 4: Compare CLIs — complete
- [x] Step 5: Audit & fix — complete
- [x] Step 6: Spec — complete
- [ ] Step 7: Commit — awaiting review
