# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

## Active: CI/CD Hardening — Phase 5

**Status:** Phases 1–4 complete. Phase 5–6 remain.

**Branch:** `ci/hardening`

---

### Phase 1: Conformance workflow verification ✅ MERGED

Completed in PR #2. Nightly conformance workflow dispatched manually
post-merge — confirmed passing.

### Phase 2: Verify conformance workflow passes ✅ DONE

Conformance run passed (2026-06-19, 8m8s). Added `pull_request` trigger
with path filters (`crates/`, `conformance/`, `Cargo.toml`, `Cargo.lock`)
so conformance runs on code-touching PRs in addition to the nightly schedule.

### Phase 3: pyo3 0.24 → 0.29 upgrade ✅ DONE

Upgraded pyo3 and pythonize to 0.29.0. Migration changes:
- `Python::with_gil` → `Python::attach`, `allow_threads` → `detach`
- `PyObject` → `Py<PyAny>`, `FromPyObject` → dual-lifetime + `FromPyObjectOwned`
- Removed `unsafe impl Send` (Py<PyAny> is Send); kept `unsafe impl Sync`
- Removed RUSTSEC-2026-0176/0177 from deny.toml

### Phase 4: Unpin rust-toolchain.toml ✅ DONE

Removed `rust-toolchain.toml` (was 1.94.0). CI now uses latest stable.
Fixed 8 new clippy lints from Rust 1.96: `map_unwrap_or`, `is_ok_and`,
`checked_div`, `sort_by_key(Reverse)`, trailing comma, `from_mins`.

### Phase 5: xa11y GUI test workflow

Add `.github/workflows/python.yml` that runs:
- `make test-bindings` (PyO3 binding tests)
- `make test-ui` (xa11y GUI accessibility tests)

Requirements:
- Needs `.venv`, PySide6, maturin, Xvfb on Linux
- Use `xvfb-run` on ubuntu-latest for headless Qt
- Consider macOS too (no Xvfb needed, native display)
- Look at how tests run locally: `make setup-python && make build && make test-ui`

### Phase 6: Windows test twins

Add `#[cfg(windows)]` companion tests for features currently Unix-only:
- Hook execution: `.cmd` batch scripts instead of `sh -c`
- DCM monitor auth: `.cmd` monitor stub instead of `#!/bin/bash`
- Symlink escape: directory junctions (no elevation needed)

Search for `TODO: remove when Windows hook twins added` to find all sites.
Files to update:
- `crates/deadline-lib/src/bundle/hooks.rs` (10 L1 tests)
- `crates/deadline-lib/tests/bundle/hooks.rs` (6 integration tests + TestHandler)
- `crates/deadline-cli/tests/cli/auth.rs` (6 tests + helpers)
- `crates/deadline-cli/tests/cli/bundle_hooks.rs` (8 tests + helpers)

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

Key infrastructure:
- `rust-toolchain.toml` pins stable 1.94.0
- `*.localhost` host entries on macOS/Windows (AWS SDK `management.` prefix)
- Cargo cache keyed on `(os, rustc-hash, Cargo.lock-hash)` with stale eviction
- `concurrency` cancels in-progress PR runs; never cancels mainline
- `fail-fast: false` — all 3 OSes complete even if one fails

### Outer loop: `.github/workflows/conformance.yml`

Nightly (07:00 UTC) + manual dispatch. Replays `deadline-cloud-python`'s
`test/cli_e2e/` against the Rust binary. Pytest plugin rewrites localhost
URLs for the SDK's host prefix. xfail allowlist for known gaps.

---

## Deferred: Rust GUI Rewrite (QML)

**Status:** Deferred indefinitely. Work preserved on branch `qml-gui-wip`.
The Python Qt GUI in `gui/` remains the production GUI.

---

## Completed items (recent)

- **Cross-OS CI — GitHub Actions (2026-06-18)** — PR #2. Full 3-OS gate.
  Fixed 5 real Windows bugs. 1,141 tests passing on Windows.

- **Python repo parity — telemetry + UI (2026-05-28)** — process_start,
  record_error_with_trace, HoverRadioButton. Merged (a5d99cf, ce941a8).

---

## Pending Manual Verification

**GAP-1 login retest (2026-05-12):** Re-test `./target/debug/deadline auth login`
with a fresh SSO session (no cached credentials).

---

## #16f — DCC Submitter Dependency Switchover

**Status:** Batches A1-A3 ✅ Done. Batch B deferred (Houdini, separate repo).
Batch C blocked on #24 (production distribution).
