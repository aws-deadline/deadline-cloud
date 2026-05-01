# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

No active work item. Last completed: #27b (SDK calling behavior audit).

---

## CLI Feature Parity Audit (2026-05-01)

**Report:** `audit_reports/2026-05-01-cli-feature-parity.md`
**Scope:** Full three-tier audit against Python CLI v0.56.0
**Status:** Complete — 9 open findings (1 Critical, 2 High, 4 Medium, 2 Low)

### Findings requiring action (by priority)

| ID | Title | Priority | Effort |
|----|-------|----------|--------|
| AUDIT-101 | Auth status uses STS instead of ListFarms | Critical | Small — swap STS call for ListFarms |
| AUDIT-102 | Missing config settings (`max_retries_per_task`, `max_failed_tasks_count`) | High | Small — add 2 settings + config fallback in bundle submit |
| AUDIT-103 | Missing `--ignore-storage-profiles` on `download-output` | High | Small — add flag, follow sync-output pattern |
| AUDIT-104 | Manifest snapshot includes `.manifest` files | Medium | Small — add exclusion filter |
| AUDIT-105 | `--json` doesn't suppress human-readable output | Medium | Small — redirect to stderr or suppress |
| AUDIT-106 | Telemetry `account_id` not resolved | Medium | Medium — credential fast-path |
| AUDIT-107 | Debug snapshot reduced content | Medium | Small — expand queue.json, add storage_profile.json |
| AUDIT-110 | Missing backward-compat flags | Low | Small — already tracked as #21d |
| AUDIT-111 | `manifest diff --root` required vs optional | Low | Small — make optional |

### Decisions needed

- **AUDIT-108:** Rust-only subcommands (`job get-session`, `list-sessions`,
  `list-steps`, `list-tasks`, `search`; `queue get-storage-profile`) —
  remove, keep, or document as additions?
- **AUDIT-109:** Rust-only `--json` on `bundle submit` — remove or keep?

---

## #27 — Typed SDK API layer

**Status:** ✅ Complete (all batches D5a–D5e done, audit clean)

### Design intent

**No raw HTTP interception. No `Value` from API responses. No `ResponseBodyCapture`.**

Every API call uses SDK fluent builders (typed input) and returns SDK
output structs (typed output). Period.

- **Business logic callers** chain typed accessors directly:
  `output.job_attachment_settings().unwrap().s3_bucket_name()`

- **No thin wrapper functions.** Callers call the SDK directly via
  `session::deadline_client(config).await.get_queue()...send().await`.
  The only shared infrastructure is `session::deadline_client()` (session
  caching), `client::deadline_error()` / `client::format_sdk_error()`
  (error formatting), `client::collect_paginated()` (pagination), and
  telemetry (interceptor on the client).

- **Display callers** extract every field from SDK output into a
  serializable response struct (`From<Output>` impls in `responses.rs`).
  Nested SDK types that lack `Serialize` are converted via
  `type_conversions.rs` helpers that walk typed accessors.

- **Helper functions exist only for:** pagination, error handling,
  and shared nested type conversion (deduplication).

### What was done

- Deleted `ResponseBodyCapture`, `collect_paginated_raw`, `capture_send`,
  `paginated_list`, `capture_err`, `response_capture.rs`
- Deleted 26+ thin wrappers from `api.rs` (all get/list/update/search)
- Converted all 7 response structs to `From<Output>` with manual nested
  type extraction in `type_conversions.rs`
- Migrated 50+ call sites across 11+ files to direct SDK calls
- All list operations use `collect_paginated` with native SDK paginators
- All error handling uses `format_sdk_error` / `deadline_error`

See `specs/patterns.md` §AWS SDK for Rust Usage for the lasting patterns.

---

## Queued small items (from #21b Bucket 3)

### #21d — CLI backward-compat flags (~30 lines)

- `job logs --timezone` — deprecated flag mapping to `--timestamp-format`
- `queue export-credentials --output-format` — accept `credentials_process`
- `manifest snapshot/diff -ie` — short alias for `--include-exclude-config`
- `auth status --output` — validate `verbose`/`json`

### #21e — `deadlinew` windowless launcher (~7 lines)

`#![windows_subsystem = "windows"]` binary target for GUI commands on Windows.

### #21f — Windows config path normalization (~50 lines)

Normalize `\`↔`/` for path-type config settings on Windows.

### #21g — Telemetry parity: success/fail events (~50 lines)

`asset_upload`, `asset_snapshot`, `queue_sync_output`, `download_job_output`
success/fail telemetry events.

---

## SDK Calling Behavior Audit

**Status:** ✅ Audit complete — 2026-05-01
**Scope:** Every `.rs` file in `crates/` that calls the Deadline SDK or other AWS SDKs.
**Crates audited:** deadline-api, deadline-cli, deadline-job-attachments, deadline-job-bundle, deadline-python-bindings, deadline-config, deadline-test-server

### Criteria checked

1. No raw `Value` intermediary from HTTP bodies
2. Error handling via `format_sdk_error` / `deadline_error` (no `.unwrap()` on SDK results)
3. Pagination via `collect_paginated` with native SDK paginators (no manual nextToken)
4. Telemetry via `session::deadline_client` (no raw `Client::new()` in production)
5. DCM principal via `apply_dcm_principal` on list operations that support it
6. No thin wrappers (every `api.rs` function has real logic)
7. Credential scoping for non-Deadline AWS services (S3, CloudWatch, STS)

### Findings

| File:Line | Issue | Severity | Status |
|-----------|-------|----------|--------|
| `deadline-api/src/api.rs:15-31` | Duplicate `format_sdk_error` and `sdk_err` — identical to `client.rs` versions | low | ✅ Fixed — deleted duplicates, 8 callers updated to `client::` |
| `deadline-api/src/api.rs:48` | `list_jobs_by_filter_expression` returns `Vec<Value>` built from typed accessors | med | Accepted — function has real algorithmic logic (createdAt thresholding, dedup); Value return is pragmatic for its single display-path caller |
| `deadline-api/src/api.rs:278` | `batch_get_steps_page` returns `Value` built from typed accessors | med | Accepted — function has real logic (identifier construction, error extraction); serves single display-path caller |
| `deadline-api/src/api.rs:334` | `batch_get_tasks_page` returns `Value` built from typed accessors | med | Accepted — same rationale as batch_get_steps_page |
| `deadline-cli/src/commands/worker.rs:60` | Used `api::format_sdk_error` instead of `client::` | low | ✅ Fixed |
| `deadline-cli/src/commands/mcp.rs:418` | Used `api::format_sdk_error` instead of `client::` | low | ✅ Fixed |
| `deadline-cli/src/commands/job.rs:1199` | Stale comment referenced `ResponseBodyCapture` | low | ✅ Fixed |
| `deadline-cli/src/commands/job.rs:1827` | Stale comment referenced `ResponseBodyCapture` | low | ✅ Fixed |
| `deadline-api/src/type_conversions.rs:4` | Module doc referenced `ResponseBodyCapture` | low | ✅ Fixed |
| `deadline-test-server/src/deadline_api/mod.rs:5` | Module doc referenced `ResponseBodyCapture` as current behavior | low | ✅ Fixed |

### Criteria pass/fail summary

| Criterion | Result | Notes |
|-----------|--------|-------|
| 1. No raw Value intermediary | **PASS** | All SDK calls use typed output. `Value` is only built from typed accessors for display paths. Three `api.rs` functions return `Value` but construct it from typed accessors (not HTTP bodies) — a typed return would be cleaner but not a correctness issue. |
| 2. Error handling | **PASS** | All `.send().await` errors go through `format_sdk_error` / `deadline_error` / domain-specific equivalents (`cw_sdk_err`, `format_sts_sdk_err`). No `.unwrap()` on SDK results. |
| 3. Pagination | **PASS** | All list operations use `collect_paginated` with native SDK paginators. Zero manual `nextToken` loops for Deadline APIs. S3 `ListObjectsV2` in job-attachments uses manual continuation token (correct for S3). |
| 4. Telemetry | **PASS** | All Deadline API calls use `session::deadline_client`. No raw `Client::new()` in production code. Test code uses raw clients (appropriate). |
| 5. DCM principal | **PASS** | `apply_dcm_principal` used on all four list operations that support it (`list_farms`, `list_queues`, `list_jobs`, `list_fleets`) across CLI, MCP, helpers, and python-bindings. |
| 6. No thin wrappers | **PASS** | All `api.rs` functions have real logic (algorithmic pagination, identifier construction, polling, filter building). |
| 7. Credential scoping | **PASS** | CloudWatch Logs uses queue-scoped or fleet-scoped credentials. S3 in job-attachments receives caller-provided scoped `SdkConfig`. STS calls use scoped credentials. |

### ResponseBodyCapture remnant check

- **Source code:** Zero active references in `crates/`. Four stale comments (listed in findings table).
- **Docs:** `specs/HANDOFF.md` references are historical changelog (acceptable). `specs/patterns.md` mentions it in a "don't do this" rule (acceptable prescriptive guidance).
- **`collect_paginated_raw`:** Zero source references. Only in HANDOFF.md history.
- **`response_capture` module:** Deleted. Zero imports remain.
- **Build artifacts:** Stale `.d` files in `target/` reference the deleted file — harmless, cleared by `cargo clean`.

### Bottom line

**No high-severity issues.** Three medium-severity findings (Value return
types in `api.rs`) accepted as-is — the functions have real algorithmic
logic and serve single display-path callers. All seven low-severity
findings fixed: duplicate functions deleted, import paths corrected,
stale comments updated. **Zero open findings.**
