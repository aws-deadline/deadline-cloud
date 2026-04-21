You are auditing the Rust CLI for behavioral parity against the Python CLI.
Read `specs/audit.md` for the full process.
Check `specs/audit_reports/` for existing reports to resume.
If starting fresh, create a new report file dated today in that directory.
Key references: `specs/{crate}/` for per-crate behavioral specs,
`specs/cli/` for per-command specs, `specs/python-observations.md` for known quirks.
Compare specs ↔ code ↔ tests: flag misalignment as bugs, spec drift, or missing coverage.

**Source of truth:** The audit report in `specs/audit_reports/` is the
single source of truth for all audit finding status (open, fixed, dropped,
accepted). Do NOT duplicate finding details in `progress.md` or `HANDOFF.md`.
`progress.md` should only contain a brief pointer to the audit report.
When fixing a finding, update the finding's entry in the audit report:
mark it Fixed, update the Rust behavior description, and update the
summary table and remaining open list.
