# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

No active work item. Last completed: #28 (CLI feature parity audit fixes).

---

## CLI Feature Parity Audit (2026-05-01)

**Report:** `audit_reports/2026-05-01-cli-feature-parity.md`
**Status:** All findings resolved.

### Decisions needed

- **AUDIT-108:** Rust-only subcommands (`job get-session`, `list-sessions`,
  `list-steps`, `list-tasks`, `search`; `queue get-storage-profile`) —
  remove, keep, or document?
- **AUDIT-109:** Rust-only `--json` on `bundle submit` — remove or keep?

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
