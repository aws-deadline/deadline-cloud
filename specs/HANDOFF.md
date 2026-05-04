# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

Active work item: None.

---

## Remaining work from #30 (low priority)

- **Reduce `serde_json::Value` usage** — 108 references in CLI code.
  Address incrementally when touching those files.
- **Audit `collect()` then iterate** — 18 sites. Quick fixes when
  touching those files.

## Dependency upgrades needed

- **rusqlite** 0.32 → 0.39 (major, breaking changes likely)
- **pyo3** 0.24 → 0.28 (major, breaking API changes)
- **rustls-webpki** advisories pinned by transitive hyper-rustls 0.24
  (awaiting AWS SDK upstream fix). See `deny.toml`.

## Queued small items

- **#21e** — `deadlinew` windowless launcher (~7 lines)
- **#21f** — Windows config path normalization (~50 lines)
- **#21g** — Telemetry parity: success/fail events (~50 lines)

---

## Completed items

- **#30 Rust tooling setup** — rustfmt, release profile (LTO+strip,
  36→29MB), cargo-deny, cargo-outdated, cargo-bloat, cargo-udeps.
  Clone audit (221→~170 correct). Profiling confirms CLI is
  network-bound (88% I/O wait). `make setup-tools` installs all tools.
- **Strict clippy lint resolution** — 2064 → 0 warnings. See
  `clippy.toml` and `[workspace.lints]` in `Cargo.toml`.
- **CLI feature parity audit (AUDIT-108, AUDIT-109)** — Removed
  Rust-only subcommands and `--json` flag.
  Report: `audit_reports/2026-05-01-cli-feature-parity.md`
- **Codebase health audit** — Report:
  `audit_reports/2026-05-01-codebase-health.md`
