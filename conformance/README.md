# CLI Conformance (cross-repo parity)

This runs **deadline-cloud-python's own `test/cli_e2e/` suite against the Rust
`deadline` binary**, using Python's CLI tests as the oracle for our Rust
implementation. It is the *outer loop* of our CI:

- **Inner loop** (`.github/workflows/ci.yml`): `cargo test` — our ~463 L2 CLI
  tests + 280 snapshots against our own mock. Catches **Rust regressions**.
- **Outer loop** (`.github/workflows/conformance.yml`, this directory): replays
  the *latest* Python `cli_e2e` suite against our binary. Catches **parity
  gaps** — behavior Python added or changed that we haven't ported.

## Why it works

The Python `cli_e2e` tests shell out to whatever `deadline` is on `PATH` and
point it at in-process HTTP mocks via standard env vars
(`AWS_ENDPOINT_URL_DEADLINE`, `AWS_ENDPOINT_URL_S3/STS`, `DEADLINE_CONFIG_FILE_PATH`,
fake creds). Our Rust CLI honors all of these, so the suite is binary-agnostic.

## The one adaptation: `localhost`

The Rust Deadline SDK injects the `management.` host prefix and, unlike every
other AWS SDK, has **no supported way to disable it**
(see https://docs.aws.amazon.com/sdkref/latest/guide/feature-host-prefix.html —
"SDK for Rust: No"). `management.127.0.0.1` is unresolvable, but
`management.localhost` resolves to 127.0.0.1 per RFC 6761. The Python mock binds
to `127.0.0.1`, so `rust_conformance_plugin.py` rewrites the endpoint host to
`localhost`. This mirrors what `deadline-test-server`'s harness already does.

## Files

| File | Purpose |
|------|---------|
| `rust_conformance_plugin.py` | pytest plugin: `localhost` host rewrite + xfail allowlist |
| `xfail_allowlist.txt` | intentional/pending Rust-vs-Python differences (kept green) |
| `run_conformance.sh` | local runner: build binary + run `cli_e2e` against it |

## Run locally

```bash
./conformance/run_conformance.sh                          # full cli_e2e suite
./conformance/run_conformance.sh test/cli_e2e/test_farm.py  # a subset
```

Requires a `deadline-cloud-python` checkout (default `../deadline-cloud-python`,
override with `DEADLINE_PYTHON_REPO`).

## Current baseline (2026-06-16)

135 passed, 1 xfail (`test_cli_deadline_no_args_prints_help` — Rust exits 0
silently on no-args vs Python printing "Usage"; candidate Rust fix).

## Triaging a failure

When the nightly conformance job goes red, a `cli_e2e` test failed against the
Rust binary. Either:

1. **Real gap** → port the behavior to Rust (see `specs/python-parity-backlog-*.md`
   and the rust-port workflow), then the test passes.
2. **Intentional difference** → add a line to `xfail_allowlist.txt` with a reason.

## Known follow-ups

- `AWS_ENDPOINT_URL_CLOUDWATCHLOGS` (Rust) vs `AWS_ENDPOINT_URL_CLOUDWATCH_LOGS`
  (SDK-standard, used by Python) — fix in `deadline-lib/src/api/log_retrieval.rs`
  so job-logs commands reach the mock under this harness.
