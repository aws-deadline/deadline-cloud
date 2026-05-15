# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

Active work item: **None**

---

## Completed items (older)

- **Audit findings — Idiomatic Rust cleanup (2026-05-15)** — Batches A-C+E:
  shared `util.rs` (op_err + normalize_path), thread leak fix in hooks,
  FilterSet regex caching (1000x speedup), HookFailed error variant.
  +25 tests (1,297→1,322). Batch D (session mutex) deferred.
- **#31 — Crate Restructure (2026-05-14)** — All 12 steps done.
  See `audit_reports/archive/2026-05-14-cli-behavioral-audit.md`.
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
  `audit_reports/archive/2026-05-01-codebase-health.md`

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
