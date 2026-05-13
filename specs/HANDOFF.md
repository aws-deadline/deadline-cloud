# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

Active work item: **#31 — Crate Restructure**
See [`specs/crate-restructure.md`](crate-restructure.md) for the full plan.

---

## Current Status

**Step 7 (Upload/Download Engine):** Steps 7a and 7b complete. Step 7c (cleanup) next.

**Baseline (2026-05-13):**

| Crate | Tests | Status |
|-------|-------|--------|
| `deadline-job-attachments` | 305 | ✅ All pass |
| `deadline-cli` | 492 | ✅ All pass |

---

## What's Done (this session, 2026-05-13)

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

## Next: Step 7c (Cleanup)

Delete remaining dead code from the upload/download swap:
- `s3.rs`: `compute_upload_config`, `compute_download_workers`,
  `get_small_file_threshold_multiplier` — no longer called
- `s3.rs` constants: `S3_UPLOAD_MAX_CONCURRENCY`, `S3_DOWNLOAD_MAX_CONCURRENCY`,
  `S3_MULTIPART_UPLOAD_CHUNK_SIZE` — no longer referenced
- Prune tests for deleted `s3.rs` helpers
- Remove any unused imports/types

After 7c, Step 7 is complete and the next actionable work is:
- **Step 9** (CLI/Library Boundary) — separate presentation from logic
- Or move to a different work item

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
| 8 | Cleanup (HashAlgorithm param) | `751dfc4` |

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
