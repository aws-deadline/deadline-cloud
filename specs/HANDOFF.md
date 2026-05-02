# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

Active work item: Strict clippy lint resolution (see below).

---

## Strict Clippy Lint Resolution (In Progress)

**Status:** 2064 → 362 warnings resolved. 362 remaining (all non-correctness).

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

### What remains (~362 warnings, all non-correctness)

| Category | Count | Effort |
|----------|-------|--------|
| Cast warnings (sign/precision/truncation) | ~95 | Manual — each needs judgment |
| `BTreeSet/Arc::default()` style | 40 | Mechanical |
| Items after statements | 27 | Structural — move defs up |
| Undocumented unsafe blocks | 22 | Manual — add `// SAFETY:` comments |
| `unsafe` block usage (visibility) | 24 | Already flagged, informational |
| Unreachable `pub` | 16 | Mechanical — `pub` → `pub(crate)` |
| Too many lines / too many args | ~20 | Refactoring |
| Misc pedantic | ~20 | Mixed |

**Recommended next steps:**
1. Undocumented unsafe blocks (22) — highest value, documents safety
2. Mechanical fixes (`default()` style, `pub(crate)`, `#[allow]` reasons)
3. Cast warnings — triage per call site when touching those files
4. Structural (items after statements, long functions) — address during
   regular development

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
