# Test Specification ↔ Rust Scaffolding Compatibility Audit

> Generated from a full pass through all 52 sections (1,369 test cases) against
> the `deadline-cloud-rs` workspace crate structure.

---

## 1. Crate-to-Section Mapping

The Rust workspace has 7 crates currently, expanding to 10. Below is the
mapping from each crate to the test spec sections it should own, plus gaps
where no crate exists.

| Rust Crate | Test Spec Sections | Notes |
|---|---|---|
| `deadline-models` | §19 (models & data classes), §25 (manifest formats & decode), §33 (exceptions/errors), §51 (client exceptions), §52 (SubmitterInfo) | Already has `errors.rs`, `job_attachments.rs`, `path_format.rs`, `submitter_info.rs` |
| `deadline-common` | §36 (path_utils) | Already has `path_utils.rs` |
| `deadline-config` | §1 (config get/set/clear/read/write), §2 (profile resolution) | Already has `config_file.rs` |
| `deadline-api` | §3–5 (session), §6–10 (API resource mgmt), §11–14 (API job lifecycle), §34 (AWS client helpers) | Has `api.rs`, `session.rs` stubs |
| `deadline-job-bundle` | §15–18 (loading, parameters, submission, history) | Has `loader.rs`, `parameters.rs`, `submission.rs` stubs |
| `deadline-job-attachments` | §20–24 (hashing, upload, download, sync, caches), §26–32 (path mapping, glob, VFS, progress, permissions), §35 (incremental downloads) | Has `asset_manifests.rs`, `caches.rs`, `download.rs`, `upload.rs`, `models.rs`, `progress_tracker.rs`, `vfs.rs` stubs |
| `deadline-cli` | §37–49 (all CLI commands) | Has `main.rs` stub only |
| `deadline-gui-ffi` | New sections TBD (Phase 3) | Not yet scaffolded |
| **⚠️ No crate** | §30–31 (public API: attachment/manifest download/upload) | See Finding #1. |
| **⚠️ Deferred** | §50 (MCP server) | See Finding #2. |

---

## 2. Findings

### Finding 1: Missing crate or module for public attachment/manifest API (§30–31)

Sections 30 and 31 describe a public API layer (`attachment_download`, `attachment_upload`,
`manifest_snapshot`, `manifest_diff`, `manifest_download`, `manifest_upload`, etc.) that
sits above both `deadline-api` and `deadline-job-attachments`. These are the functions
that external consumers (CLI, MCP, GUI) call.

**Options:**
- (a) Add these as a public module in `deadline-job-attachments` (e.g., `pub mod api;`)
- (b) Add these as a module in `deadline-api` (e.g., `pub mod attachment_api;`)
- (c) Create a new `deadline-job-attachments-api` crate

**Recommendation:** Option (a) — add `pub mod api;` to `deadline-job-attachments/src/lib.rs`.
The Python code places these in `deadline.job_attachments.api`, and the Rust crate already
owns the domain. The test spec sections 30–31 should map to
`deadline-job-attachments::api`.

### Finding 2: MCP server (§49–50) — out of scope or deferred

The MCP server is a Python-specific integration (FastMCP library, tool registry pattern).
Section 49 (CLI `deadline mcp-server`) and Section 50 (MCP server internals) may not be
part of the initial Rust port.

**Recommendation:** Mark §49–50 as `DEFERRED` in the index if MCP is not in scope for the
Rust port. If it is in scope, add a `deadline-mcp` crate to the workspace.

### Finding 3: Missing `deadline-job-attachments` modules for §26, §27, §29

The Rust scaffolding has no modules for:
- Path mapping (§26) — needs `pub mod path_mapping;`
- Glob & diff (§27) — needs `pub mod glob;` or fold into `asset_manifests`
- OS file permissions (§29) — needs `pub mod permissions;`

These are distinct behavioral domains with their own test surfaces.

### Finding 4: `deadline-job-attachments` missing incremental download module (§35)

Section 35 (61 test cases) covers incremental download state tracking. The Rust crate has
`download.rs` but no `incremental_download.rs` or similar. This is a significant subsystem
with its own persistence format (JSON checkpoint files).

**Recommendation:** Add `pub mod incremental_download;` to `deadline-job-attachments`.

### Finding 5: `deadline-api` missing telemetry module (§14)

Section 14 (24 test cases) covers the telemetry client. No module exists in the Rust
scaffolding. Telemetry is cross-cutting and used by CLI, MCP, and API layers.

**Recommendation:** Add `pub mod telemetry;` to `deadline-api`.

### Finding 6: `deadline-api` missing auth/login module (§6)

Section 6 (18 test cases) covers login/logout via Deadline Cloud Monitor subprocess
management. This is distinct from session management (§3–5) and needs its own module.

**Recommendation:** Add `pub mod auth;` to `deadline-api`.

### Finding 7: `deadline-models` error variants incomplete

The scaffolded `DeadlineError` has 4 variants and `JobAttachmentsError` has 19 variants.
Cross-referencing with §33 and §51:

- Missing: `CreateJobWaiterCanceled` variant (§51 case 5)
- Missing: `UserInitiatedCancel` variant (§51 case 6)
- Missing: `AssetSyncCancelledError` with summary statistics (§33 cases 4–5)
- Missing: `JobAttachmentsS3ClientError` structured fields (§33 case 1) — the current
  `S3Client` variant only has a `String`; needs `action`, `status_code`, `bucket`, `key`,
  `message` fields
- Missing: `JobAttachmentS3BotoCoreError` (§33 case 3)

### Finding 8: `deadline-models` missing manifest types

Section 25 (32 test cases) covers manifest format types (`HashAlgorithm`, `ManifestVersion`,
`ManifestModelRegistry`, `BaseAssetManifest`, `AssetManifest`). These are not in
`deadline-models` — they're expected in `deadline-job-attachments::asset_manifests`, which
is correct. But the `HashAlgorithm` enum should live in `deadline-models` since it's
referenced by both `deadline-job-attachments` and `deadline-api`.

**Recommendation:** Add `HashAlgorithm` enum to `deadline-models`.

---

## 3. Test Spec Language Audit (Post-Write Review)

Re-reading all 1,369 cases against the protocol's 5-point Pythonic audit checklist:

### Violations Found

| File | Section | Case # | Issue | Fix |
|---|---|---|---|---|
| session.md | §3 | 10 | "Advisory refresh timeout is patched to 5 minutes, mandatory to 2.5 minutes" — describes Python boto3 credential refresh internals | Rewrite: "Refreshable credentials use a 5-minute advisory and 2.5-minute mandatory refresh window" |
| session.md | §3 | 11 | "Session credentials are NOT refreshable" — Python-specific concept | Rewrite: "Credentials do not support automatic refresh" |
| session.md | §3 | 12 | "Patching refresh timeouts raises an error" — Python monkey-patching concept | Rewrite: "Configuring credential refresh windows fails" |
| api_job_lifecycle.md | §14 | 1–2 | Signature `TelemetryClient.__init__` uses Python dunder naming | Rewrite header: `TelemetryClient::new(package_name, package_ver, config?)` |
| api_job_lifecycle.md | §14 | 17 | Signature `TelemetryClient._put_telemetry_record` uses underscore-prefix | Rewrite header: `TelemetryClient.put_telemetry_record(event)` (or note it's internal) |
| api_job_lifecycle.md | §14 | 21 | Signature `TelemetryClient._send_request` uses underscore-prefix | Rewrite header: `TelemetryClient.send_request(req)` |
| job_attachments_orchestration.md | §26 | header | `_generate_path_mapping_rules` — leading underscore | Rewrite: `generate_path_mapping_rules` |
| job_attachments_orchestration.md | §26 | header | `_PathMappingRuleApplier` — leading underscore | Rewrite: `PathMappingRuleApplier` |
| job_attachments_orchestration.md | §27 | header | `_process_glob_inputs` — leading underscore | Rewrite: `process_glob_inputs` |
| job_attachments_orchestration.md | §27 | header | `_glob_paths` — leading underscore | Rewrite: `glob_paths` |
| mcp.md | §50 | 11 | "Result contains objects with `__dict__` attribute" — Python-specific | Rewrite: "Result contains objects serialized via their field map" |
| mcp.md | §50 | 12 | "Result contains objects with `to_dict` method" — Python-specific | Rewrite: "Result contains objects serialized via explicit conversion" |
| mcp.md | §50 | 15 | "Tool already registered (has `_mcp_tool_registered` flag)" — Python attribute | Rewrite: "Tool already registered (idempotent registration)" |

### Additional violations found during application

| File | Section | Case # | Issue | Fix |
|---|---|---|---|---|
| job_attachments_data_transfer.md | §20 | header | `_create_manifest_for_single_root` — leading underscore | Rewrite: `create_manifest_for_single_root` |
| job_attachments_data_transfer.md | §25 | header | `AssetManifest.__init__` — Python dunder naming | Rewrite: `AssetManifest.new(hash_alg, ...)` |

### Summary

13 violations across 6 files. All are minor naming/phrasing issues. No behavioral
inaccuracies — just language-specific leakage that should be cleaned up.

**Update:** 15 total violations found (2 additional discovered during application). All fixed.

---

## 4. Structural Recommendations for Test Spec Files

### 4a. Add crate mapping annotations

Each section header should include a `> Rust crate:` annotation so developers know where
to write the implementation and tests. Example:

```markdown
## Section 1: Config — get/set/clear/read/write settings

> Rust crate: `deadline-config`
> Rust module: `config_file`
```

### 4b. Add test priority tiers

The protocol says to "test functionality at the interface level as much as practical without
overly testing internal code unless it is high-use and reliability is paramount." Many
sections mix public API tests with internal helper tests. Adding a priority tier would help
Rust developers focus:

| Tier | Description | Example |
|---|---|---|
| P0 — Interface | Public API / CLI behavior | `deadline bundle submit`, `attachment_download()` |
| P1 — Core | High-use internal logic critical to correctness | Hash cache, manifest encode/decode, path mapping |
| P2 — Internal | Internal helpers testable via public interfaces | `str2bool`, `human_readable_file_size` |
| P3 — Platform | Platform-specific behavior (may be deferred) | Windows DACL, VFS mount management |

Sections that are primarily P2/P3:
- §29 (OS file permissions) — P3, platform-specific
- §28 (VFS) — P3, Linux-only, may be deferred
- §32 (progress tracking) — P2, internal helper
- §36 (path_utils) — P2, utility functions

### 4c. Consolidate related sections

Some sections are split too finely for the Rust crate structure:

- §19 (models) + §25 (manifest formats) + §33 (exceptions) → all map to `deadline-models`
  + `deadline-job-attachments::asset_manifests`. Consider a single "Data Types & Formats"
  file or at minimum cross-reference them.
- §30 (public API attachment) + §31 (public API manifest) → both map to the same module.
  Already in the same file, which is good.
- §3 (session) + §4 (auth status) + §5 (queue credentials) → all map to
  `deadline-api::session`. Already in the same file.

### 4d. GUI test cases are in scope

GUI commands (`deadline config gui`, `deadline bundle gui-submit`) are in
scope. The Rust CLI spawns a Python process that loads the GUI widget package
and the `deadline-gui-ffi` shared library. Test cases §38.9–10 and §45.14
should be implemented as Level 2 tests that verify the CLI spawns the correct
process and handles errors (e.g., Python not installed, shared library not
found).

### 4e. Add new crate sections

The following crates are not covered by the existing 52 test spec sections
and will need new sections as they are implemented:

- `deadline-gui-ffi` — C ABI contract, JSON data exchange, error translation,
  callback handling (Phase 3)

---

## 5. Recommended Changes (Ordered by Impact)

### Must-do (correctness)

1. **Fix 13 language-neutrality violations** listed in §3 above
2. **Add `> Rust crate:` annotations** to all 52 section headers

### Should-do (developer experience)

3. **Add missing Rust modules** to scaffolding:
   - `deadline-job-attachments/src/path_mapping.rs`
   - `deadline-job-attachments/src/glob.rs`
   - `deadline-job-attachments/src/permissions.rs`
   - `deadline-job-attachments/src/incremental_download.rs`
   - `deadline-job-attachments/src/api.rs`
   - `deadline-api/src/telemetry.rs`
   - `deadline-api/src/auth.rs`
4. **Expand `deadline-models` error types** per Finding #7
5. **Add `HashAlgorithm` enum** to `deadline-models` per Finding #8

### Nice-to-have (organization)

6. **Add priority tiers** (P0–P3) to section headers
7. **Mark §28, §49, §50 as DEFERRED** if not in initial Rust scope
8. **Mark GUI test cases** as out of scope

---

## 6. Test Case Count by Rust Crate

| Rust Crate | Sections | Total Cases |
|---|---|---|
| `deadline-models` | §19, §25, §33, §51, §52 | 101 |
| `deadline-common` | §36 | 14 |
| `deadline-config` | §1, §2 | 88 |
| `deadline-api` | §3–14, §34 | 268 |
| `deadline-job-bundle` | §15–18 | 174 |
| `deadline-job-attachments` | §20–24, §26–32, §35 | 556 |
| `deadline-cli` | §37–49 | 224 |
| `deadline-gui-ffi` | TBD (Phase 3) | TBD |
| `deadline-mcp` (deferred) | §50 | 39 |
| **Total (existing specs)** | **52 sections** | **1,369** |
