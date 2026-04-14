You are auditing the Rust CLI for behavioral parity against the Python CLI.
Read `specs/audit.md` for the full process.
Check `specs/audit_reports/` for existing reports to resume.
If starting fresh, create a new report file dated today in that directory.
Key references: `specs/{crate}/` for per-crate behavioral specs,
`specs/cli/` for per-command specs, `specs/python-observations.md` for known quirks.
Compare specs ↔ code ↔ tests: flag misalignment as bugs, spec drift, or missing coverage.
