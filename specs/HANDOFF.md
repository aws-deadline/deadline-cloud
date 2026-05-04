# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

Active work item: #30 — Rust tooling and optimization setup.

---

## #30 — Rust Tooling and Optimization Setup

### Quick wins (do first)

1. ✅ **`rustfmt.toml` + `cargo fmt`** — Done. 114 files reformatted.
   `edition = "2024"`, enforced with `cargo fmt --check`.
2. ✅ **`[profile.release]` with LTO + strip** — Done. 36MB → 29MB
   (19% reduction). `lto = "thin"` + `strip = true`.
3. ✅ **`cargo-deny`** — Done. All checks pass. 22 duplicate crates
   (all transitive AWS SDK deps). 4 rustls-webpki advisories
   acknowledged (pinned by hyper-rustls 0.24, awaiting upstream fix).

### Install for ongoing use

4. **`cargo-nextest`** — Faster parallel test runner. 634 tests take
   ~20s today; nextest could cut to ~8-10s.
5. **`cargo-outdated`** — Identify stale dependencies.
6. **`cargo-udeps`** — Find unused dependencies in Cargo.toml.
7. **`cargo-bloat`** — Analyze binary size breakdown (likely AWS SDK
   monomorphization).

### Optimization investigation

8. **Audit `.clone()` in hot paths** — 217 clone calls in production
   code. Many are Python-port artifacts where borrowing would suffice.
   Focus on file hashing and S3 transfer paths first.
9. **Reduce `serde_json::Value` usage** — 108 references in CLI code.
   AWS SDK returns typed structs; converting to Value for display loses
   type safety and adds heap allocation. Address incrementally.
10. **Audit `collect()` then iterate** — 18 sites where a Vec is
    collected then immediately iterated. Many can use the iterator
    directly (zero allocation).

### Install when needed

11. **`cargo-flamegraph`** — CPU profiling for when we optimize
    specific commands (bundle submit, S3 transfers).

---

## Completed items

### Strict Clippy Lint Resolution ✅

2064 → 0 warnings. See `clippy.toml` and `[workspace.lints]` in
`Cargo.toml` for configuration.

### CLI Feature Parity Audit (AUDIT-108, AUDIT-109) ✅

Removed Rust-only subcommands and `--json` flag not in Python CLI.
Report: `audit_reports/2026-05-01-cli-feature-parity.md`

### Codebase Health Audit ✅

Report: `audit_reports/2026-05-01-codebase-health.md`

---

## Queued small items

- **#21e** — `deadlinew` windowless launcher (~7 lines)
- **#21f** — Windows config path normalization (~50 lines)
- **#21g** — Telemetry parity: success/fail events (~50 lines)
