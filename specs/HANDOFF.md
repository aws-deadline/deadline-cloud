# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

## Active work item: #21b — Spec drift detection

**Step:** 2 (Implement) — Bucket 1 (implemented-but-undocumented) complete.

### Problem Statement

Specs in `specs/` have drifted from the actual Rust implementation.
Features were added/fixed (audit findings, new work items) but the
corresponding spec files were not updated. This creates confusion for
both human readers and AI agents relying on specs as ground truth.

### Drift Findings (Study Phase)

**Validated 2026-04-28.** All original findings (A–D) re-confirmed
against current code. Four new categories (E–H) discovered.

#### Category A: Specs claim features are missing/sequential that are now implemented

| Spec File | Drift | Evidence |
|-----------|-------|----------|
| `deadline-job-attachments/architecture.md` | Says "Sequential S3 operations (for now)" | Code uses `buffer_unordered` for parallel upload (`upload.rs:943`) and download (`download.rs:500`). AUDIT-011/012/014/049 all Fixed. |
| `deadline-job-attachments/README.md` | Gaps section lists "Parallel S3 transfer — proven in spike, not yet in production paths" | Same — parallel is wired in. |
| `deadline-job-attachments/README.md` | "Relationship to Python" says "Rust is currently sequential" | Contradicts actual parallel implementation. |
| `deadline-api/README.md` | Gaps: "job trace-schedule support APIs — experimental, deferred" | `trace-schedule` fully implemented (`job.rs:1564-1676`). AUDIT-041 Fixed. |

#### Category B: Implemented features with no spec documentation

| Feature | Code Location | Missing From |
|---------|---------------|--------------|
| `job trace-schedule` command | `commands/job.rs:1564-1676` | `deadline-cli/job.md` (no section), `deadline-cli/reference.md` (no section) |
| `deadline config` commands | `commands/config.rs` | `deadline-cli/reference.md` (no section) |
| `deadline farm` commands | `commands/farm.rs` | `deadline-cli/reference.md` (no section) |
| `deadline queue` commands | `commands/queue.rs` | `deadline-cli/reference.md` (no section) |
| `deadline job` commands | `commands/job.rs` | `deadline-cli/reference.md` (no section) |
| `deadline attachment` commands | `commands/attachment.rs` | `deadline-cli/reference.md` (no section) |
| `deadline handle-web-url` command | `commands/handle_web_url.rs` | `deadline-cli/reference.md` (no section) |

Note: all missing commands DO have dedicated spec files (config-and-auth.md,
resource-commands.md, queue.md, job.md, attachments-and-manifests.md,
handle-web-url.md). The gap is only in reference.md's index.

#### Category C: Settings count mismatch

| Spec File | Claims | Actual |
|-----------|--------|--------|
| `deadline-config/architecture.md` | "All 18 Settings" | Code has 21 settings |
| Missing settings: | `settings.allow_bundle_hooks`, `settings.allow_environment_hooks`, `settings.submitter_update_notification` | Added by #18 (hooks) and #19 (update checker) |

#### Category D: "Differences from Python" tables now stale

| Spec File | Stale Claim | Reality |
|-----------|-------------|---------|
| `deadline-cli/job.md` | `job download-output` conflict: "Defaults to CreateCopy with file detection warning" | AUDIT-008 fixed: interactive root path editing implemented |
| `deadline-cli/job.md` | `job download-output` conflict row in differences table | AUDIT-055 fixed: conflict detection with file list warning |

#### Category E (NEW): API spec inaccuracies

| Spec File | Drift | Evidence |
|-----------|-------|----------|
| `deadline-api/session-cache.md` | User-Agent prefix shown as `deadline-api` | Code uses `deadline-client` (`session.rs:37`) |
| `deadline-api/session-cache.md` | InternalServerException → "Please wait and retry" | Code has no retry guidance for that error; only ThrottlingException has retry text |
| `deadline-api/architecture.md` | Public API Surface shows 4 example functions | Code has 18+ public API functions not enumerated (update_job, create_job, batch_get_steps_page, etc.) |
| (none) | `check_deadline_api_available` undocumented | `auth.rs:179` — public function not in any spec |

#### Category F (NEW): Job-bundle spec drift

| Spec File | Drift | Evidence |
|-----------|-------|----------|
| `deadline-job-bundle/architecture.md` | Lists `extract_asset_references()` as public API | Function doesn't exist — asset extraction is inline in `submission.rs`/`parameters.rs` |
| `deadline-job-bundle/architecture.md` | Lists `merge_queue_parameters()` | Actual function is `merge_queue_job_parameters()` (`parameters.rs:595`) |

#### Category G (NEW): Job-attachments module layout incomplete

| Spec File | Drift | Evidence |
|-----------|-------|----------|
| `deadline-job-attachments/architecture.md` | Module layout lists 14 files | `errors.rs` exists as 15th file (`lib.rs:6` declares `pub mod errors`) |

#### Category H (NEW): Config dependency diagram incomplete

| Spec File | Drift | Evidence |
|-----------|-------|----------|
| `deadline-config/architecture.md` | Dependency chain tree under `farm_id` | Missing `defaults.job_attachments_file_system` (`settings.rs:132`: `depend: Some("defaults.farm_id")`) |
| `deadline-config/architecture.md` | Dependency chain tree under `aws_profile_name` | Missing `settings.job_history_dir` (`settings.rs:42`: `depend: Some("defaults.aws_profile_name")`) |

### Verified: No Drift

These spec files were checked and are accurate:
- `deadline-cli/queue.md` — all 6 subcommands match code
- `deadline-cli/config-and-auth.md` — config and auth commands match code
- `deadline-cli/resource-commands.md` — farm/fleet/worker commands match code
- `deadline-cli/bundle.md` — submit and gui-submit match code
- `deadline-cli/handle-web-url.md` — URL dispatch and install/uninstall match code
- `deadline-cli/attachments-and-manifests.md` — attachment and manifest commands match code
- `deadline-api/credential-scoping.md` — queue/fleet scoping logic matches code
- `deadline-api/log-retrieval.md` — session auto-selection and fleet scoping match code
- `deadline-job-attachments/s3-transfer.md` — S3 error mapping, client config match code
- `deadline-job-attachments/hash-cache.md` — table names, schema, WAL mode match code
- `deadline-job-attachments/path-mapping.md` — trie-based matching matches code
- Top-level `architecture.md` — crate dependency graph matches Cargo.toml files

### Implementation Plan

The fix is purely documentation — no code changes needed.

| Batch | Spec Files to Update | Changes |
|-------|---------------------|---------|
| 1 | `deadline-job-attachments/architecture.md`, `deadline-job-attachments/README.md` | Remove "sequential" claims, document parallel upload/download with `buffer_unordered`, update "Relationship to Python" section, add `errors.rs` to module layout (Cat A + G) |
| 2 | `deadline-api/README.md`, `deadline-api/session-cache.md`, `deadline-api/architecture.md` | Remove trace-schedule from Gaps, fix User-Agent prefix, fix InternalServerException text, enumerate public API functions (Cat A + E) |
| 3 | `deadline-config/architecture.md` | Add 3 missing settings (21 total), fix dependency chain tree to include `job_attachments_file_system` and `job_history_dir` (Cat C + H) |
| 4 | `deadline-cli/job.md` | Add `trace-schedule` subcommand section, fix `download-output` differences table (Cat B + D) |
| 5 | `deadline-cli/reference.md` | Add "See also" index pointing to per-command spec files (Cat B — Option B) |
| 6 | `deadline-job-bundle/architecture.md` | Fix `extract_asset_references` → inline note, fix `merge_queue_parameters` → `merge_queue_job_parameters` (Cat F) |

### Design Decision: reference.md scope

`reference.md` currently only documents auth, fleet, worker, bundle,
manifest, and mcp-server. The other commands (config, farm, queue, job,
attachment, handle-web-url) have dedicated spec files (`job.md`,
`queue.md`, etc.) with full documentation. Two options:

**Option A:** Add all missing commands to `reference.md` (comprehensive
single-file reference). Risk: duplication with per-command files.

**Option B:** Add a "See also" index at the top of `reference.md`
pointing to per-command spec files. Keep `reference.md` for commands
that don't have their own file. No duplication.

Recommend **Option B** — avoids duplication, keeps specs DRY.

### Bucket 1 Completion (2026-04-28)

All 16 "implemented but undocumented" drift items fixed across 6 batches:

| Batch | Files Modified | Changes |
|-------|---------------|---------|
| 1 | `deadline-job-attachments/architecture.md`, `README.md` | Replaced "sequential" with parallel docs, added `errors.rs` to module layout, updated Relationship section |
| 2 | `deadline-api/README.md`, `session-cache.md`, `architecture.md` | Removed trace-schedule from Gaps, fixed User-Agent prefix, fixed error text, added full API function table (35+7 functions) |
| 3 | `deadline-config/architecture.md` | 18→21 settings, added 3 missing settings, fixed dependency chain tree |
| 4 | `deadline-cli/job.md` | Added trace-schedule subcommand section, fixed download-output differences table |
| 5 | `deadline-cli/reference.md` | Added "See also" index (Option B) |
| 6 | `deadline-job-bundle/architecture.md`, `parameter-validation.md` | Fixed phantom `extract_asset_references` → `apply_job_parameters`, fixed `merge_queue_parameters` → `merge_queue_job_parameters` |

### Remaining Work

**Bucket 2 (spec typos):** Resolved in Batch 6 above.

**Bucket 3 (Python features missing from both Rust code and specs):**
Not in scope for #21b (spec drift detection). These are feature gaps,
not documentation gaps. Candidates for new work items:

| Priority | Feature | Notes |
|----------|---------|-------|
| High | `deadlinew` windowless launcher | Covered by #24 (production distribution) |
| Medium | `is_path` config setting normalization | Windows path backslash↔forward-slash |
| Medium | `precache_clients()` | GUI startup performance optimization |
| Low | `job logs --timezone` deprecated flag | Migration-period backward compat |
| Low | `queue export-credentials --output-format` | Single valid value, low impact |
| Low | `record_success_fail_telemetry_event()` | Telemetry decorator for submission |

### Future: Automated Drift Detection

After fixing the current drift, consider a lightweight CI check:
- Parse spec files for "Gaps:", "Not yet", "Sequential", "Missing"
- Cross-reference against code (grep for implementations)
- Flag potential drift in PRs that touch code but not specs

This is out of scope for #21b but noted for future work.
