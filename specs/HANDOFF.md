# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

## Active Work Item

None. All findings from `specs/audit_reports/2026-04-15-behavioral-parity.md` resolved.

Consult the Work Items table in `specs/progress.md`
and pick the first row with status "Not started" whose dependencies
are all "✅ Done".

## Critical Context for New Sessions

1. **Python source is at `../deadline-cloud-python`** (sibling directory).
   Use it for behavioral parity verification when auditing ported
   commands, not for new Rust-only features.

2. **All tests pass.** The full workspace test suite is green.
   If a test fails, it's a real regression.

3. **Development workflow is in `specs/workflow.md`.** Follow the
   7-step loop: Study Python → Write tests → Implement → Write spec →
   Audit → Fix → Commit.

4. **Specs are in `specs/`.** Per-crate specs in `specs/{crate}/`,
   CLI command docs in `specs/cli/`. Update these as part of every
   work item (Step 4).

5. **DCM test infrastructure exists.** `cli_dcm.rs` has
   `write_dcm_aws_config()` helper. `cli_credential_scoping.rs` has
   the pattern for testing DCM vs non-DCM credential paths.

6. **`deadline-job-bundle` owns submission orchestration.** It depends
   on `deadline-api` (API calls) and `deadline-job-attachments` (S3
   upload). See `specs/architecture.md` for the dependency graph.

7. **Historical note: crate consolidation.** Early iterations had
   `deadline-models` and `deadline-common` as separate crates. These
   were dissolved into `deadline-api` and `deadline-job-attachments`
   because the separation added dependency complexity without value.
