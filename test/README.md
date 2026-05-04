# Testing layers

Prefer higher level tests (lower in this list) where possible. Higher level tests validate more representative behavior.

| Situation | Suite |
|---|---|
| Pure logic, error paths, internal helpers | `test/unit/` |
| "Does `deadline <cmd>` produce the right output/exit code?" | `test/cli_e2e/` |
| "Does it work against the real service?" | `test/integ/` |
| Qt dialog / widget regressions | `test/squish/` |
| Built installer / packaging | `test/installer/` |

## `test/unit/` — unit tests

Fast, in-process tests of individual functions and classes. Mocks are
used freely (via `unittest.mock` and shared fixtures) to isolate
business logic from AWS, the filesystem, Qt, etc.

Use for: pure logic, argument parsing, config handling, error-path
coverage, data-structure invariants. This is the primary
coverage-enforced suite.

Run: `hatch run test`

## `test/cli_e2e/` — CLI end-to-end tests

Subprocess-based tests that invoke the real `deadline` binary. **Nothing
inside the CLI process is patched.** The subprocess makes real HTTP
calls to in-process mock servers:

* An in-repo mock of the Deadline API, served over HTTP via
  `AWS_ENDPOINT_URL_DEADLINE`.
* `moto`'s `ThreadedMotoServer` covers S3, STS, and CloudWatch Logs,
  via the matching `AWS_ENDPOINT_URL_*` env vars.

Use for: verifying end-to-end behavior of every CLI subcommand —
subprocess startup, argument parsing, HTTP serialization, output
formatting, exit codes — without needing real AWS resources. Also the
right place for regression coverage of the Deadline API mock's HTTP
routing itself.

Don't add in-process patching or mocking here; if something needs an
in-process mock, it belongs in `test/unit/`.

Run: `hatch run test` (runs alongside `test/unit/`).

## `test/integ/` — integration tests

Pytest tests that run against real AWS Deadline Cloud and S3 resources.
Create farms, queues, jobs, and S3 buckets in the test account; assert
real service behavior.

Use for: validating the real service contract (pagination shapes,
throttling behavior, cross-account permissions, IAM role assumption) or
reproducing a customer issue end-to-end. Requires AWS credentials and
non-trivial setup.

Run: `hatch run integ:test`

## `test/squish/` — GUI tests

Automated UI tests driven by the Squish for Qt framework against the
Deadline GUI commands. Requires a Squish license and Qt runtime.

Use for: GUI regressions when you specifically want automated Qt UI
coverage. Manual GUI testing is usually sufficient for most changes.

## `test/installer/` — installer tests

Verifies the bundled installer and its PyInstaller build. Not exercised
by routine development.

Run: `hatch run test_installer`
