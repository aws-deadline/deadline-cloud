# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

Active work item: **#31 — Crate Restructure**
See [`specs/crate-restructure.md`](crate-restructure.md) for the full plan.

---

## Current Status

**Step 9.1 (Config Resolution):** ✅ Complete. No library crate reads config from disk.

**Baseline (2026-05-13):**

| Crate | Tests | Status |
|-------|-------|--------|
| `deadline-job-attachments` | 294 | ✅ All pass |
| `deadline-job-bundle` | 249 | ✅ All pass |
| `deadline-api` | 168 | ✅ All pass |
| `deadline-cli` | 492 | ✅ All pass |

---

## What's Done (this session, 2026-05-14)

**Step 9.3 — User interaction boundary (commit `ef6b13e`):**
Replaced `print_callback`, `continue_callback`, and
`interactive_confirmation_callback` in `SubmitJobParams` with a
`SubmissionHandler` trait (`on_message`, `confirm`, `should_continue`).
`HookManager` now takes `&dyn SubmissionHandler`. Deleted `ConfirmFn` type.
Hook messages moved from stderr to stdout (6 snapshots updated).
Tests: 294/249/168/492 unchanged.

**Step 9.4 — Path types (commit `18155ae`):**
Converted `&str`/`String` filesystem path parameters to `&Path`/`PathBuf`
across all library crates. 19 files changed, 305 insertions, 258 deletions.
Struct fields: `AssetRootGroup.root_path`, `AssetRootManifest.root_path`,
`ManifestSnapshot`, `ManifestMergeResult`, `ManifestDownloadEntry`,
`HookMetadata.job_bundle_dir`, `HookManager`,
`SubmitJobParams.{job_bundle_dir,debug_snapshot_dir,known_asset_paths}`.
Functions: `glob_files`, `write_manifest`, `manifest_snapshot/diff/merge/download`,
`validate_directory_symlink_containment`, `HookManager::new`,
`generate_hooks_confirmation_message`, `read_yaml_or_json`,
`read_yaml_or_json_object`, `parse_yaml_or_json_content`,
`save_yaml_or_json_to_file`, `read_job_bundle_parameters`,
`apply_job_parameters`, `split_parameter_args`, `save_debug_snapshot`.
Reverted `FileSystemLocation.path` to `String` (remote machine path, not local).
Eliminated `bundle_dir_str` shim — all internal functions now take `&Path`.
Tests: 294/249/168/492 unchanged.

**Step 9.2 — Progress reporting boundary (commit `2a68161`):**
Removed `ProgressReportMetadata` struct and pre-formatted `progress_message`
from library API. Simplified callback type from `Fn(ProgressReportMetadata) -> bool`
to `Fn(u64, u64) -> bool` (processed_bytes, total_bytes). Removed 6 dead
`_callback` params. CLI now computes percentage from raw bytes. Added
`ProgressFn` type alias. -210 lines, +77 lines. Tests: 294/249/492 unchanged.

---

## What's Done (previous session, 2026-05-13)

**Step 9.1 Phase C — Config resolution for deadline-api (commit `7ba0287`):**
Removed `Option<&IniConfig>` from all 39 deadline-api function signatures.
Deleted 5 `get_setting_from_disk` fallback calls. All library crates now
require callers to provide config — no library crate reads config from disk.

**Step 9.1 Phase A+B — Config resolution (commit `7e0ca66`):**
Removed `Option<&IniConfig>` from `deadline-job-attachments` and
`deadline-job-bundle` library APIs. Functions now require `&IniConfig`
(caller must provide). Deleted `get_setting_from_disk` fallback from
`s3.rs`. Removed unused `_config` param from `S3UploadContext::new`.
Removed `config` param from `attachment_upload`. `deadline-api` still
uses `Option<&IniConfig>` (Phase C, next).

**Step 7c — Cleanup (commit `778b35f`):**
Deleted dead S3 concurrency helpers: `compute_upload_config`,
`compute_download_workers`, `get_small_file_threshold_multiplier`, and
constants `S3_MULTIPART_UPLOAD_CHUNK_SIZE`, `S3_UPLOAD_MAX_CONCURRENCY`,
`S3_DOWNLOAD_MAX_CONCURRENCY`. These computed worker counts for the old
manual upload/download loops replaced by openjd `S3DataCache` in 7a/7b.
Pruned 11 tests. Fixed stale doc comment on `build_s3_client`.
Tests: 305→294. -176 lines.

**Step 7 is now complete.**

**Step 7a — Upload engine swap (commit `8b0381e`):**
Replaced `hash_assets_and_create_manifest` + `S3UploadContext::upload_input_files`
with `collect_abs_snapshot` → `hash_upload_abs_manifest` via `S3DataCache`.
`upload_assets` now takes `&[AssetRootGroup]` (unhashed) instead of
`&[AssetRootManifest]` (pre-hashed). Pipelined hash+upload.
Deleted ~1,600 lines. Tests: 343→314.

**Step 7b — Download engine swap (commit `0998fd1`):**
Replaced `download_file` + `download_files_from_manifests` internals with
`download_abs_manifest` via `S3DataCache`. Dropped fallback key retry (pre-GA).
Downloads now verify content hashes (integrity improvement).
Deleted ~600 lines. Tests: 314→305.

**Earlier this session:**
- Step 5 (Diff): commit `642e53a` — type bridge + hash_diff delegate
- Step 8 (Cleanup): commit `751dfc4` — removed dead HashAlgorithm parameter
- Rewrote `specs/crate-restructure.md` with flat step numbering

---

## Next: Step 9 continued — Progress & Interaction

Step 9 (CLI/Library Boundary) sub-step order and status:

| Sub-step | What | Status |
|----------|------|--------|
| 9.1 | Config resolution (no disk reads in library) | ✅ Done |
| 9.2 | Progress reporting (library returns raw stats) | ✅ Done |
| 9.3 | User interaction (remove callbacks, return decision points) | ✅ Done |
| 9.4 | Path types (`&str` → `&Path`/`PathBuf`) | ✅ Done |
| 9.5 | Presentation utilities (move formatting to CLI) | **Next** |

**9.4** is next: convert `&str`/`String` path parameters to `&Path`/`PathBuf`
across all library crates.

**Baseline (2026-05-14, pre-9.4):**

| Crate | Tests | Status |
|-------|-------|--------|
| `deadline-job-attachments` | 294 | ✅ All pass |
| `deadline-job-bundle` | 249 | ✅ All pass |
| `deadline-api` | 168 | ✅ All pass |
| `deadline-cli` | 492 | ✅ All pass |

**9.4 Plan (assessed: HIGH complexity, ~138 call sites, mechanical):**

Phased direct swap (no strangler fig — changes are purely type-level):

- **Phase A:** `deadline-job-attachments` — struct fields (`AssetRootGroup.root_path`,
  `AssetRootManifest.root_path`, `ManifestSnapshot`, `ManifestMergeResult`,
  `ManifestDownloadEntry`, `FileSystemLocation.path`) + functions (`glob_files`,
  `set_root_path`, `matches_any_filter` stays `&str`)
- **Phase B:** `deadline-job-bundle` — `SubmitJobParams.job_bundle_dir/.debug_snapshot_dir/.known_asset_paths`,
  `HookManager`, `HookMetadata.job_bundle_dir`, `validate_directory_symlink_containment`,
  `generate_hooks_confirmation_message`
- **Phase C:** `deadline-cli` + `deadline-python-bindings` callers

Keep as `String`: `ManifestPath.path` (codec), `PathMappingRule` fields (cross-platform),
`UploadManifestInfo.output_manifest_path` (S3 key), `PathSummary.path` (display).

**Status:** ✅ Complete (commit `18155ae`).

---

## Completed Steps (all of #31)

| Step | What | Commit |
|------|------|--------|
| 1 | Hashing → openjd | (earlier session) |
| 2 | Manifest Codec → openjd | (earlier session) |
| 3 | S3CheckCache → openjd | (earlier session) |
| 4 | HashCache → openjd | (earlier session) |
| 5 | Diff → openjd (type bridge) | `642e53a` |
| 6 | Path Mapping → openjd | (earlier session) |
| 7a | Upload engine → openjd | `8b0381e` |
| 7b | Download engine → openjd | `0998fd1` |
| 7c | Cleanup (dead S3 helpers) | `29424d2` |
| 8 | Cleanup (HashAlgorithm param) | `751dfc4` |
| 9.1 | Config resolution (all library crates) | `7e0ca66`, `7ba0287` |
| 9.2 | Progress reporting (simplify callback API) | `2a68161` |
| 9.3 | User interaction (SubmissionHandler trait) | `ef6b13e` |
| 9.4 | Path types (`&str` → `&Path`/`PathBuf`) | `18155ae` |

---

## Pending Manual Verification

**GAP-1 login retest (2026-05-12):** Re-test `./target/debug/deadline auth login`
with a fresh SSO session (no cached credentials). On 2026-05-11 the first attempt
hit "was not able to log into" which may have been the bug manifesting with a stale
binary. On 2026-05-12 login succeeded, but need one more clean test to confirm.

---

## #16f — DCC Submitter Dependency Switchover

**Status:** Batches A1-A3 ✅ Done. Batch B deferred (Houdini, separate repo).
Batch C blocked on #24 (production distribution).

---

## Completed items (older)

- **Python parity audit (2026-05-11)** — GAP-1 (login session refresh),
  GAP-3 (--include/--match-paths-by on download-output), GAP-2
  (download-input command). All implemented and verified.
- **#16f Batch A1** — DCC submitter import shim layer. Committed `7f1d84a`.
- **#16f Batch A2** — TelemetryClient + ProgressReportMetadata runtime fixes.
- **#16f Batch A3** — API module wrappers for Unreal.
- **#30 Rust tooling setup** — rustfmt, release profile (LTO+strip,
  36→29MB), cargo-deny, cargo-outdated, cargo-bloat, cargo-udeps.
- **Strict clippy lint resolution** — 2064 → 0 warnings.
- **CLI feature parity audit (AUDIT-108, AUDIT-109)** — Removed
  Rust-only subcommands and `--json` flag.
- **Codebase health audit** — Report:
  `audit_reports/2026-05-01-codebase-health.md`
