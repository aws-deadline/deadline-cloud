# Testing layers

Prefer higher level tests (lower in this list) where possible. Higher level tests validate more representative behavior.

| Situation | Suite |
|---|---|
| Pure logic, error paths, internal helpers | `test/unit/` |
| "Does `deadline <cmd>` produce the right output/exit code?" | `test/cli_e2e/` |
| "Does the UI actually render correctly?" | `test/ui/` |
| "Does it work against the real service?" | `test/integ/` |
| Built installer / packaging | `test/installer/` |

## `test/unit/` — unit tests

Fast, in-process tests of individual functions and classes. Mocks are
used freely (via `unittest.mock` and shared fixtures) to isolate
business logic from AWS, the filesystem, Qt, etc.

Use for: pure logic, argument parsing, config handling, error-path
coverage, data-structure invariants. This is the primary
coverage-enforced suite.

Run: `hatch run test`

### GUI unit tests (`test/unit/deadline_client/ui/gui/`)

GUI unit tests validate the Qt-based submitter and settings dialogs
using [pytest-qt](https://pytest-qt.readthedocs.io/) with an offscreen
Qt platform (no display required). They run alongside all other unit
tests as part of `hatch run test` and contribute to the unified coverage
measurement.

**When to write a GUI unit test:**

- Adding or modifying a widget, dialog, or panel in `src/deadline/client/ui/`.
- Changing user-facing behavior such as form validation, default values,
  enabled/disabled state, or signal/slot wiring.
- Fixing a bug in the UI layer — the test should reproduce the bug and
  verify the fix.

GUI unit tests are *not* required for cosmetic-only changes
(stylesheets, spacing, icons) that do not alter functionality.

**How to run:**

```sh
# Run all tests (includes GUI unit tests)
$ hatch run test

# Run only GUI unit tests
$ hatch run test test/unit/deadline_client/ui/gui

# Run a specific test file
$ hatch run test test/unit/deadline_client/ui/gui/test_settings_dialogue.py

# Run a specific test by name
$ hatch run test -k "test_host_requirements"
```

**Expectations:**

- Tests must pass in offscreen mode (`QT_QPA_PLATFORM=offscreen`) — no
  real display or user interaction.
- Tests should use the `mock_deadline_backend` fixture (see
  `test/unit/deadline_client/ui/gui/conftest.py`) instead of mocking
  individual API calls, to ensure realistic service behavior.
- Coverage reports are written to `build/coverage/`.

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

## `test/ui/` — UI tests

Subprocess-based tests that launch the real `deadline` GUI commands and
drive them through the OS accessibility tree via
[xa11y](https://xa11y.dev/). The GUI subprocess talks to the same
in-process mock Deadline backend used by `test/cli_e2e/`, so no real AWS
resources are needed. **No mocking or patching inside the GUI process** —
the full client runs end-to-end, and the tests verify that the UI
actually renders the correct widgets, labels, and controls.

The goal is to catch rendering and integration regressions that unit
tests miss: dialog layout, widget visibility, tab navigation, form
round-trips, and submission flows. Every major UI component (config
dialog, submitter dialog) should have basic UI tests here to confirm it
opens, displays the right data, and responds to user actions.

Exhaustive widget-level testing (e.g. every combo-box option, every
validation state) belongs in the `pytest-qt` suite at
`test/unit/deadline_client/ui/gui/`, which tests Qt widgets in-process
without rendering to the screen — faster and lighter weight, but lower
fidelity.

Run: `hatch run ui:test`

## `test/integ/` — integration tests

Pytest tests that run against real AWS Deadline Cloud and S3 resources.
Create farms, queues, jobs, and S3 buckets in the test account; assert
real service behavior.

Use for: validating the real service contract (pagination shapes,
throttling behavior, cross-account permissions, IAM role assumption) or
reproducing a customer issue end-to-end. Requires AWS credentials and
non-trivial setup.

Run: `hatch run integ:test`

## `test/installer/` — installer tests

Verifies the bundled installer and its PyInstaller build. Not exercised
by routine development.

Run: `hatch run test_installer`
