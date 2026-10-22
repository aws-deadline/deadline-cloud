# REVIEW.md - Review guidance for AWS Deadline Cloud Client

This file tells reviewers, human and automated, where changes in this
repository go wrong and what is worth raising. The Claude PR review reads the
copy on `mainline`, so edits take effect once merged. Coding conventions live in
[AGENTS.md](AGENTS.md) and [DEVELOPMENT.md](DEVELOPMENT.md); this file covers
risk.

## Who runs this code

This library runs as a standalone CLI, and also in-process inside DCC
applications (Maya, Houdini, Nuke, ...) on whatever Python the DCC bundles, on
Linux, Windows, and macOS. A change that is fine in a fresh virtualenv on one OS
can break a user who cannot change their Python or their other installed
packages.

## High-risk areas

Review these with the most care, and raise a finding when the failure described
is a realistic outcome of the change.

### Credentials and sessions

`client/api/_session.py`, `_loginout.py`, `_queue_credentials.py`, and
`client/config/`.

- Credentials, tokens, or session objects written to logs, telemetry, exception
  messages, or files.
- A queue role session used for a call that should use the user's credentials,
  or the reverse.
- A boto3 client created for one region and reused for another. The farm region
  (`defaults.farm_region`) can differ from the profile/monitor region.
- A change to config keys or defaults under `~/.deadline/config` that silently
  changes behavior for existing users.

### Telemetry

`client/api/_telemetry.py`, `_stack_trace_sanitizer.py`.

- Any customer content in a telemetry event: file paths, bucket names, job or
  parameter values, exception message text, source lines. Only the allowlisted
  structured fields may be sent.
- Telemetry that ignores the opt-out setting, or that can raise into, block, or
  slow the user's command.

### Files and paths

`client/_path_utils.py` (path containment), job attachments and download
code, anything writing to the user's filesystem.

- Path containment checks that a crafted path escapes: `..`, symlinks, absolute
  paths, Windows drive letters, UNC paths (`\\server\share`), case-insensitive
  filesystems.
- Files that must be readable only by the process owner created with default
  permissions, or written non-atomically where a partial file causes harm.
- POSIX-only assumptions (path separators, `os.chmod` semantics, `fork`) in code
  that also runs on Windows.

### Public contracts

Non-underscore names in public modules, CLI subcommands, options, defaults, and
`--output json` shape. See "Public Contracts" in DEVELOPMENT.md.

- A breaking change without `feat!:`/`fix!:` and a `BREAKING CHANGES:` section:
  a removed or renamed public symbol or CLI option, a new required parameter, a
  changed default, or a changed JSON output field.
- A private helper accidentally made public (a new non-underscore name or
  import in a public module), which becomes a contract.

### Dependencies and Python versions

`pyproject.toml`, `requirements*.txt`, `hatch.toml`.

- A new runtime dependency, or a tightened version range on an existing one.
  Either can conflict with packages a DCC already ships. Ask whether it is
  needed (see "Library Dependencies" in DEVELOPMENT.md).
- Syntax or stdlib APIs newer than the oldest supported Python
  (`requires-python` in `pyproject.toml`), e.g. `match`, `X | Y` type unions
  evaluated at runtime, or stdlib functions added after 3.9.
- `requires-python`, the classifiers, and the `hatch.toml` matrix drifting out
  of sync.

### Qt / GUI

`client/ui/`.

- AWS API calls on the main Qt thread.
- Qt objects that can be destroyed off the GUI thread: QObjects without a
  parent, lambdas capturing `self`, Python callback lists, a QThread deleted
  before `wait()`. See "Qt object ownership and lifetime" in DEVELOPMENT.md.

### Build, release, and CI

`.github/`, `pipeline/`, `installer/`, `scripts/`, `hatch_custom_hook.py`.

- Workflows that run PR code with secrets or a write token, untrusted input
  (PR titles, branch names) interpolated into `run:` scripts, or actions not
  pinned to a full commit SHA. `aws-deadline/*` reusable workflows are pinned
  to `@mainline` on purpose.
- Changes that alter what gets published or how it is versioned.

## Tests

Every behavior change needs a test (see AGENTS.md). Raise it when:

- A bug fix has no test that fails without the fix.
- A change weakens existing tests: deleted or loosened assertions, new `skip`
  or `xfail`, broader exception matching, longer timeouts that hide a race.
- A test reads the real user config or credentials instead of fixtures
  (`fresh_deadline_config` for GUI tests).

## Do not raise

- Style the formatter or linter enforces (`hatch run fmt`, `hatch run lint`).
- Edits to `CHANGELOG.md` made by the release tooling.
- Issues in code the PR does not change.
- Requests for more comments; AGENTS.md prefers clear code over comments.
