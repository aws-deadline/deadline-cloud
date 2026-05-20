# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

Active work item: **#34 — Library/CLI boundary refactor (Batches A+B done, C-E pending)**

---

## #34 — Library/CLI Boundary Refactor

**Goal:** Remove `&IniConfig` from all library operation function signatures.
Library operations take explicit params (farm_id, profile, etc.). Config
utilities remain in the library as a shared utility module, but operations
never read config internally. Simple CRUD API calls stay in the CLI.

**Remaining batches:**

- **Batch C: Submission** — Remove `config` field from `SubmitJobParams`.
  Add `farm_id`, `queue_id`, etc. as explicit fields.

- **Batch D: S3/Attachments** — Change `build_s3_client` to take explicit
  `max_pool: usize` instead of `&IniConfig`.

- **Batch E: Docs update** — Final review of all specs for accuracy.

**Status: Batches A+B awaiting commit**

---

## Completed items (older)

- **#34 Batches A+B — Session + Telemetry refactor (2026-05-20)** —
  Session functions take `profile: Option<&str>`, telemetry functions take
  `opt_out: bool, identifier: Option<&str>` instead of `&IniConfig`.
  Fixed 35 pre-existing Python test failures (stale `_get_ffi` mocks).
  1,355 Rust tests pass, 371/373 Python tests pass.

- **[L] Deferred refactors (2026-05-18)** — Unified `OutputDownloader`/
  `InputDownloader` into `ManifestDownloader`, separated validation from
  grouping in `prepare_paths_for_upload`, consolidated duplicate
  `format_sdk_error` functions, added 5 Level 1 tests for
  `create_job_from_job_bundle`. 1,350→1,355 tests.

- **Per-file upload progress (2026-05-16)** — Wired openjd-snapshots
  `on_progress` callback for per-file progress during upload. Progress
  bar now fills incrementally instead of jumping per-group. Added
  `signal_file_done` to ProgressTracker, `ProgressFn` → `Send + Sync`.
  +2 tests (1,348→1,350). Bumped flaky perf test budget.
- **Audit findings — Batches G+I+J (2026-05-16)** — Test coverage gaps
  (+17 tests), session mutex minimization (lock no longer held across
  network I/O), submission function extraction (`load_and_confirm_hooks`),
  HookManager constructor cleanup. 1,324→1,341 tests.
- **Audit findings — Batch F quick wins (2026-05-16)** — 13 findings
  addressed: HashSet perf fix in diff (O(N²)→O(N)), platform-aware
  normcase in incremental_download, clear error for missing glob config
  file, 10 doc comments. +2 tests (1,322→1,324). Added #33 (error type
  parity audit) to progress.md.
- **Audit findings — Idiomatic Rust cleanup (2026-05-15)** — Batches A-C+E:
  shared `util.rs` (op_err + normalize_path), thread leak fix in hooks,
  FilterSet regex caching (1000x speedup), HookFailed error variant.
  +25 tests (1,297→1,322). Batch D (session mutex) deferred.
- **#31 — Crate Restructure (2026-05-14)** — All 12 steps done.
  See `audit_reports/archive/2026-05-14-cli-behavioral-audit.md`.
- **Python parity audit (2026-05-11)** — GAP-1 (login session refresh),
  GAP-3 (--include/--match-paths-by on download-output), GAP-2
  (download-input command). All implemented and verified.

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
