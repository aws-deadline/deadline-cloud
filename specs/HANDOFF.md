# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

Active work item: **#31 — Crate Restructure (openjd-rs + CLI/library separation)**
See [`specs/crate-restructure.md`](crate-restructure.md) for the full plan.
(Previous plan in `specs/openjd-integration.md` is superseded — absorbed as Phase 1.)

**Status:** Phase 1d (HashCache) and Phase 3 (Path Mapping) complete.
Phase 2 (Diff) — deferred (see below).

**Phase 3 result:** Replaced trie-based `PathMappingRuleApplier` (~300 lines)
with thin wrapper around `openjd_expr::path_mapping::apply_rules_with_format()`.
Added `openjd-expr 0.1` dependency. Tests: 347→343 (pruned 4 trie-internal tests).
CLI: 492/492 unchanged.

**Phase 1d result:** Replaced `HashCache` + `HashCacheEntry` + `format_mtime_for_cache()`
with `openjd_snapshots::HashCache` (re-export). Accepted u64 mtime format (nanoseconds
since epoch). Removed `rusqlite` direct dependency. Tests: 358→347 (pruned 11 that
tested openjd internals). CLI: 492/492 unchanged.

**Phase 1c result:** Replaced `S3CheckCache` + `S3CheckCacheEntry` with
`openjd_snapshots::S3CheckCache` (re-export). Deleted ~80 lines of impl,
`S3CheckCacheEntry` struct, `current_timestamp()` helper. Tests: 364→358
(pruned 6 that tested openjd internals). CLI: 492/492 unchanged.

**Phase 2 (Diff) decision:** `fast_diff` has no openjd equivalent (filesystem-based).
`hash_diff` is only ~30 lines and replacing it would require an equally-sized adapter
to convert `AssetManifest ↔ Snapshot`. Deferred to Phase 4 when `AssetManifest` is
eliminated and the conversion is free.

**Phase 1b result:** Replaced `AssetManifest::encode()` and `decode_manifest()`
with `openjd_snapshots::encode_snapshot_v2023` / `decode_v2023`. Tests: 370→364
(pruned 6 that tested openjd internals). CLI: 492/492 unchanged.

**Phase 1a result:** Replaced `hash_data`/`hash_file` with `openjd-snapshots 0.1`
from crates.io. Upgraded rusqlite 0.32→0.39. Tests: 375→370 (pruned 5 redundant).
CLI: 492/492 unchanged.

### Phase 1 Baseline (2026-05-12)

| Metric | Count |
|--------|-------|
| `deadline-job-attachments` tests | 375 (240 unit + 135 integration) |
| `deadline-cli` tests | 492 (51 unit + 441 integration) |
| Phase 1 scope: `asset_manifests` tests | 31 |
| Phase 1 scope: `caches` tests | 18 |
| **All tests passing** | ✅ |

**Python parity check (2026-05-12):**
- Hashing: ✅ openjd-rs produces identical xxh128 output to Python
- Manifest codec: ✅ openjd-rs `encode_snapshot_v2023` matches Python's
  `json.dumps(sorted_keys=True, ensure_ascii=True)` behavior
- S3CheckCache: ✅ Same float-timestamp format as Python
- HashCache: ✅ Swapped to openjd-rs u64 mtime (one-time cache invalidation accepted)

**Phase checklist:**
- [x] Phase 1a: Hashing ✅
- [x] Phase 1b: Manifest Codec ✅
- [x] Phase 1c: S3CheckCache ✅
- [x] Phase 1d: HashCache ✅
- [ ] Phase 2: Diff (deferred to Phase 4)
- [x] Phase 3: Path Mapping ✅
- [ ] Phase 4: Upload/Download Engine
- [ ] Phase 5: Template Validation (deferred)

---

## Pending Manual Verification

**GAP-1 login retest (2026-05-12):** Re-test `./target/debug/deadline auth login`
with a fresh SSO session (no cached credentials). On 2026-05-11 the first attempt
hit "was not able to log into" which may have been the bug manifesting with a stale
binary. On 2026-05-12 login succeeded, but need one more clean test to confirm.

---

## #16f — DCC Submitter Dependency Switchover

**Goal:** All DCC submitters (except Houdini) work with zero code changes
when switching from `deadline-cloud-python` to `deadline-cloud-rs`.

**Status:** Batches A1-A3 ✅ Done. Batch B deferred (Houdini, separate repo).
Batch C blocked on #24 (production distribution).

### Batch B — Houdini submitter rewrite (separate repo, deferred)

Houdini is pinned to `deadline == 0.49.*` and uses APIs that cannot
exist in Rust (boto3 sessions, `S3AssetManager`, deprecated dialog method).
Requires changes in the Houdini submitter repo, not this one.

### Batch C — Dependency switch (blocked on #24)

Update `pyproject.toml` in all 9 DCC repos to depend on the new package.
Cannot happen until #24 (production distribution) publishes the package.

---

## Completed items

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
