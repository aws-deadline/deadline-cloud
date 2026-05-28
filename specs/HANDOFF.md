# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

Active work item: **None — between items**

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
