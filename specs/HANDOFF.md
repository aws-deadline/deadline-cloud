# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

Active work item: **#35 — Rust-native GUI (cxx-qt) — Batch 1b+1c remaining**

**Baseline:** 1,380 tests passing (2026-05-21)

---

## Status: Batch 1 partially complete

### What's done (1a + 1d)
- `crates/deadline-gui/` — Full config dialog: logic module, QObject model,
  QML form with 4 GroupBoxes, all settings, load/apply/dirty tracking
- CLI wired: `deadline config gui` calls `deadline_gui::show_config_dialog()` directly
- L1 tests: 26 passing (logic module)
- L2 tests: 35 copied from `deadline-cloud-python/test/ui/` (acceptance criteria)

### What remains (1b + 1c)
- **1b — ResourceModel:** Async farm/queue/storage profile loading from API.
  Currently farm/queue show raw IDs from config; need ComboBoxes populated
  via `deadline_client().list_farms()` etc.
- **1c — AuthModel:** Auth status bar with login/logout buttons, file watching
  on `~/.aws/` and `~/.deadline/`, credential status display.

### Key Technical Decisions
- **cxx-qt v0.8.1** (KDAB), MIT/Apache-2.0
- **Qt 6** via system `QMAKE` env var
- **Logic + Model + View** pattern: pure Rust logic (testable) → QObject (thin bridge) → QML (view)
- **Build:** `QMAKE=/opt/homebrew/opt/qt/bin/qmake cargo build -p deadline-gui`

---

## Completed items

- **#35 Batch 1a+1d — Config Dialog + CLI wiring (2026-05-21)** — Full config
  dialog with all settings, logic module with 26 L1 tests, CLI calls Rust GUI
  directly. 1,355→1,380 tests. L2 xa11y test infrastructure ported from
  deadline-cloud-python (35 tests, acceptance criteria for visual correctness).

- **#34 — Library/CLI boundary refactor (2026-05-20)** — Removed `&IniConfig`
  from ALL library operation signatures: submission, S3, log retrieval, auth
  (`login`/`logout`/`get_credentials_source`/`check_authentication_status`),
  and update checker. Library is now fully usable without config files.
  1,355 Rust tests, 373 Python tests pass.

- **[L] Deferred refactors (2026-05-18)** — Unified `OutputDownloader`/
  `InputDownloader` into `ManifestDownloader`, separated validation from
  grouping in `prepare_paths_for_upload`, consolidated duplicate
  `format_sdk_error` functions, added 5 Level 1 tests for
  `create_job_from_job_bundle`. 1,350→1,355 tests.

- **Per-file upload progress (2026-05-16)** — Wired openjd-snapshots
  `on_progress` callback for per-file progress during upload.
  +2 tests (1,348→1,350).

- **Audit findings — Batches A-J (2026-05-15–2026-05-16)** — 45 findings,
  33 resolved, 5 accepted, 7 deferred (all low-priority). +58 tests
  (1,297→1,355).

- **#31 — Crate Restructure (2026-05-14)** — All 12 steps done.

- **Python parity audit (2026-05-11)** — GAP-1 (login session refresh),
  GAP-3 (--include/--match-paths-by on download-output), GAP-2
  (download-input command). All implemented and verified.

---

## Pending Manual Verification

**GAP-1 login retest (2026-05-12):** Re-test `./target/debug/deadline auth login`
with a fresh SSO session (no cached credentials).

---

## #16f — DCC Submitter Dependency Switchover

**Status:** Batches A1-A3 ✅ Done. Batch B deferred (Houdini, separate repo).
Batch C blocked on #24 (production distribution).
