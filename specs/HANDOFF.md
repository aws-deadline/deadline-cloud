# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

## Active Work Item

None. Pick next from `specs/progress.md`.

## Recently Completed — #16d Batch 1

Foundation modules ported into `gui/deadline/client/`:
- `exceptions.py` — copied from Python
- `dataclasses/` — `SubmitterInfo` copied from Python
- `job_bundle/` — 8 files copied from Python
- `config/config_file.py` — new FFI shim routing through Rust
- `_compat.py` — new enum types + `ProgressReportMetadata`

33 Python tests + 28 FFI tests pass. 1164 Rust tests pass.

Remaining batches for #16d:
- Batch 2: Copy ~25 Qt widget/dialog files (no rewiring)
- Batch 3: Rewire 9 files from `api.*` → `_ffi.*`
- Batch 4: Top-level submitters + integration test

### Audit findings (Batch 1)

1. (Bug, fixed) `AwsAuthenticationStatus` had `NOT_AUTHENTICATED` instead
   of `NEEDS_LOGIN` — Python uses `NEEDS_LOGIN`, widget checks for it.
2. (Limitation, documented) `get_setting_default` returns current value,
   not hardcoded default. FFI doesn't expose this endpoint. Deferred to
   Batch 3 when config dialog needs it.
3. (By design) `config` parameter on `get_setting`/`set_setting` is
   ignored — always reads from disk. Batch 3 will add temp-file support
   for config dialog preview.
