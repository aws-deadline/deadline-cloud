# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

Active work item: **#31 — openjd-rs dependency integration**
See [`specs/openjd-integration.md`](openjd-integration.md) for the full plan.

**Status:** Planning complete. Ready to begin Phase 1 (hashing + caches).

**Phase checklist:**
- [ ] Phase 1: Hashing + Caches + Manifest Codec
- [ ] Phase 2: Diff
- [ ] Phase 3: Path Mapping
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
