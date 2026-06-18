# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

## Active: CI/CD Hardening — Phase 2

**Status:** Phase 1 merged (PR #2). Phases 2–5 below to be done in a single PR.

**Branch:** `ci/hardening` (create from mainline)

---

### Phase 1: Conformance workflow verification ✅ MERGED

Completed in PR #2. Nightly conformance workflow dispatched manually
post-merge — check result before starting Phase 2.

### Phase 2: Verify conformance workflow passes

The nightly conformance workflow (`conformance.yml`) was manually dispatched
on 2026-06-18. Check its result:

```bash
gh run list --repo viknith/deadline-cloud-rs --workflow conformance.yml --limit 1
```

If failed, root-cause and fix. Key files:
- `.github/workflows/conformance.yml`
- `conformance/rust_conformance_plugin.py` (localhost-rewrite pytest plugin)
- `conformance/xfail_allowlist.txt` (known parity gaps)
- `conformance/run_conformance.sh`

The workflow clones `deadline-cloud-python`, builds the Rust binary, then runs
Python's `test/cli_e2e/` suite against it. Consider whether conformance should
also run on PRs (openjd-rs does this) — currently nightly-only.

### Phase 3: pyo3 0.24 → 0.28 upgrade

- Current: pyo3 0.24, `deny.toml` ignores RUSTSEC-2026-0176/0177
- Upgrade to 0.28 (major breaking changes — check https://pyo3.rs/v0.28/migration)
- Key change: replace `unsafe impl Send/Sync` on `PySubmissionHandler` with `Py<PyAny>`
- After upgrade: remove RUSTSEC ignores from `deny.toml`
- Verify: `make build && make test-bindings`

### Phase 4: Unpin rust-toolchain.toml

- Currently pins stable 1.94.0 (newer stable had stricter clippy lints)
- After pyo3 upgrade, remove `rust-toolchain.toml` entirely (or update to latest)
- Fix any new clippy lints that surface
- Verify: all 3 OSes still green in CI

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
