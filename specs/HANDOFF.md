# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

Active work item: None — clippy lint resolution complete.

---

## Strict Clippy Lint Resolution (Complete)

**Status:** 2064 → 0 warnings. All resolved.

### What was done

- Added `clippy.toml` and `[workspace.lints]` config (pedantic group +
  cherry-picked restriction/nursery lints)
- `[lints] workspace = true` in all 7 crates
- `cargo clippy --fix` for mechanical fixes (redundant closures, format
  args, `.to_owned()`, collapsible ifs, `Default::default()`)
- `print_stdout`/`print_stderr`: `#![allow]` in CLI binary, lint active
  in library crates to catch stray prints
- All 74 `unwrap()` calls converted to `expect("reason")` or `?`
- `expect_used` lint disabled — `expect("reason")` is the approved
  escape hatch; `unwrap()` is still flagged
- Integration test crates have `#![allow(clippy::unwrap_used)]`
- Unreadable numeric literals fixed (74 across 18 files)
- Documented lint conventions in `specs/testing.md`
- Idiomatic Rust fixes: `let...else`, `write!`, `strip_prefix`,
  case-insensitive extension checks, `clone_from`, etc.
- Moved scoped `use`/`const` before statements, added `reason` to all
  `#[allow]` attributes
- Refactored `ManifestPath.size`/`total_size` from `i64` → `u64`
- Changed S3 config functions to return `usize` directly
- Added callback type aliases (`StatusFn`, `JobProgressFn`, `ConfirmFn`)
- `#![allow(unreachable_pub)]` in python-bindings (PyO3 requirement)
- Added `// SAFETY:` comments to all 4 unsafe blocks
- Extracted `job.rs::run_async` match arms into named functions
- Tuned lint thresholds for CLI codebase (cast lints relaxed,
  `too-many-arguments` → 10, `too-many-lines` → 200)

---

## CLI Feature Parity Audit (2026-05-01)

**Report:** `audit_reports/2026-05-01-cli-feature-parity.md`
**Status:** All findings resolved.

### Decisions resolved

- **AUDIT-108:** ✅ Removed — Rust-only subcommands (`job get-session`,
  `list-sessions`, `list-steps`, `list-tasks`, `search`;
  `queue get-storage-profile`) deleted. MCP tools unaffected.
- **AUDIT-109:** ✅ Removed — `--json` flag on `bundle submit` deleted.

---

## Codebase Health Audit (2026-05-01)

**Report:** `audit_reports/2026-05-01-codebase-health.md`
**Status:** Complete. 10 items done, 3 skipped (with rationale), 3 deferred (low urgency).

---

## Queued small items

### #21e — `deadlinew` windowless launcher (~7 lines)

`#![windows_subsystem = "windows"]` binary target for GUI commands on Windows.

### #21f — Windows config path normalization (~50 lines)

Normalize `\`↔`/` for path-type config settings on Windows.

### #21g — Telemetry parity: success/fail events (~50 lines)

`asset_upload`, `asset_snapshot`, `queue_sync_output`, `download_job_output`
success/fail telemetry events.
