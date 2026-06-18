# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

## Active: None

No active work item. CI is green on all 3 OSes; PR #2 is ready to merge.

---

## GitHub CI/CD Architecture

### Inner loop: `.github/workflows/ci.yml`

Triggered on every push to `mainline` and every PR targeting
`mainline`, `release`, `patch_*`, `feature_*`.

| Job | Runs on | What it does |
|-----|---------|-------------|
| **Rustfmt** | ubuntu | `cargo fmt --all -- --check` |
| **cargo-deny** | ubuntu | License, advisory, ban, source checks |
| **Build & Test** | ubuntu, macOS, Windows (matrix) | Build all targets, clippy `-D warnings`, `cargo test --workspace`, doctests |
| **Documentation** | ubuntu | `cargo doc --no-deps --workspace` with `-D warnings` |

Key CI infrastructure:
- `rust-toolchain.toml` pins stable 1.94.0 (avoids CI breakage from newer clippy)
- `*.localhost` host entries added on macOS/Windows (AWS SDK's `management.` prefix)
- Cargo cache keyed on `(os, rustc-version-hash, Cargo.lock-hash)` with stale target/ eviction
- `concurrency` cancels in-progress PR runs on new pushes; never cancels mainline
- `fail-fast: false` — all 3 OSes run to completion even if one fails

### Outer loop: `.github/workflows/conformance.yml`

Nightly scheduled (07:00 UTC) + manual dispatch. Replays
`deadline-cloud-python`'s `test/cli_e2e/` suite against the Rust binary
to catch parity drift. Uses a localhost-rewrite pytest plugin
(`conformance/`) and an xfail allowlist for known gaps.

### What's NOT in CI yet (follow-up PRs)
- xa11y GUI tests (need display server / Xvfb)
- Python binding tests (`make test-bindings`)
- Release builds / binary packaging
- Coverage reporting

---

## Recently completed

- **Cross-OS CI — GitHub Actions (2026-06-18)** — PR #2, branch
  `ci/github-workflows`. Full 3-OS CI gate: fmt, cargo-deny, doc,
  build+clippy+test on ubuntu/macOS/Windows. Fixed 5 real Windows bugs
  in production code (glob backslash escape, PID lock race, zip shell-out,
  filter_redundant separator, manifest root_prefix mismatch). 425 CLI L2
  tests + 513 lib L1 tests + 206 integration tests pass on Windows.
  16 tests remain `#[cfg(unix)]`-gated (spawn `sh`); Windows twins tracked
  in progress.md.

- **Python repo parity — telemetry + UI (2026-05-28)** — process_start event,
  stack trace sanitizer + record_error_with_trace, HoverRadioButton widget.
  Merged to mainline (commits a5d99cf, ce941a8).

---

## Deferred: Rust GUI Rewrite (QML)

**Status:** Deferred indefinitely. Work preserved on branch `qml-gui-wip`.

The attempt to replace the Python Qt GUI with Rust+QML (cxx-qt) was
deferred due to fundamental QML limitations for forms-based UIs:

- QML Repeater with JS array models destroys all delegates on any model
  change, causing TextField focus loss during typing
- No two-way binding — requires manual sync patterns
- Nested Repeater index shadowing requires separate component files
- DCC plugin extensibility (custom SceneSettingsWidget injection) has no
  proven QML equivalent

The Python Qt GUI in `gui/` remains the production GUI. CLI GUI commands
(`bundle gui-submit`, `config gui`) spawn a Python subprocess.

If revisited, the recommended approach is to use QML `ListModel` for
editable dynamic forms instead of JS array models, and to spike DCC
plugin integration before committing to the architecture.

---

## Completed items

- **Fix xa11y GUI test flakiness — `_find_app` false match (2026-05-28)** —
  `_find_app` name-based fallback matched transient macOS system services
  (e.g. `ThemeWidgetControlViewService`) instead of the test's GUI. Fixed
  by filtering fallback to only match apps containing "python"/"deadline".
  Same bug exists in `deadline-cloud-python/test/ui/helpers.py`.

- **#28d — Python linting + collect() audit (2026-05-27)** —
  Added ruff linter/formatter for `gui/` and `pytests/`. Fixed 5 clippy
  violations (unsafe impl comments, to_string on &str, collapsible if).
  Audited all collect() sites — zero needless collects found. `make lint`
  now includes `lint-python`; `make fmt` includes `fmt-python-check`.
  37 xa11y tests pass, 1,355 Rust tests pass.

- **Fix SIGBUS crash in _native.abi3.so (2026-05-27)** —
  Stack overflow on QThread (512KB) during webpki cert parsing. Fixed
  with `on_large_stack` (scoped thread, 8MB) + `py.allow_threads()`.
  Cached STS account ID in SessionCache. Aligned xa11y test infra with
  Python repo (SIGTERM handler, PYTHONUNBUFFERED, App.by_name). 37 xa11y
  tests pass (was 31 + 4 xfailed/crashing).

- **#35 Batch 2b — Queue Parameters + Attachments UI (2026-05-25)** —
  ParameterListModel, AttachmentModel, QML dynamic parameter form,
  attachment lists, ComboBox accessibility fix, farm/queue race fix,
  xa11y test infrastructure fix, mock backend queue environment support.
  1,483→1,488 Rust tests. 35→38 xa11y tests.

- **#35 Batch 2a — Submit Action + Progress + Export (2026-05-24–25)** —
  SubmitModel, ProgressModel, SubmitDialog.qml, ProgressDialog.qml,
  logic/submit.rs, CLI gui-submit wiring, job history bundles, --output json,
  dark mode, cancel handling, tilde expansion. 1,421→1,483 tests.

- **#35 Batch 1 — Config/Auth/Resource Models (2026-05-21–22)** —
  ConfigModel, AuthModel, ResourceModel, ConfigDialog.qml, logic.rs,
  CLI config gui wiring, xa11y test infrastructure. 1,355→1,421 tests.

- **#34 — Library/CLI boundary refactor (2026-05-20)** — Removed `&IniConfig`
  from all library signatures. 1,355 Rust tests, 373 Python tests pass.

- **Audit findings — Batches A-J (2026-05-15–16)** — 45 findings resolved.

- **#31 — Crate Restructure (2026-05-14)** — All 12 steps done.

---

## Pending Manual Verification

**GAP-1 login retest (2026-05-12):** Re-test `./target/debug/deadline auth login`
with a fresh SSO session (no cached credentials).

---

## #16f — DCC Submitter Dependency Switchover

**Status:** Batches A1-A3 ✅ Done. Batch B deferred (Houdini, separate repo).
Batch C blocked on #24 (production distribution).
