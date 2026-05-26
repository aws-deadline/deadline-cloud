# Progress

AWS Deadline Cloud Rust CLI — work item tracking.

## Getting started

Read `rust-port-workflow.md` and follow it. The Session Start section contains the
full reading checklist and gates all planning and implementation work.

## Reference Material

Historical migration documents have been removed (served their purpose).

## Progress

### Risk Spikes (must pass before bulk implementation)

All passed.

| Spike | Status | Proves |
|-------|--------|--------|
| GUI FFI round-trip (Python ↔ Rust ↔ Qt) | ✅ Passed | Core architecture works: PyO3 loading, function calls, callbacks, thread safety |
| GUI FFI inside DCC (Blender) | ✅ Passed | Shared library loads in real DCC Python environment without conflicts |
| S3 transfer performance | ✅ Passed | Rust S3 throughput ≥ Python boto3 transfer manager (see `specs/deadline-job-attachments/architecture.md`) |
| Job attachment hashing | ✅ Passed | Parallel xxh128 hashing is faster than Python, hashes match byte-for-byte |

### Work Items

Each row is a self-contained unit of work. See `rust-port-workflow.md` for how to
pick and execute work items.

| # | Work Item | Status | Test Spec Files | Depends On |
|---|-----------|--------|-----------------|------------|
| 0a | Error types, submitter info | ✅ Done | `common.md` | — |
| 0b | Path utilities | ✅ Done | `common.md` | — |
| 0c | TelemetryClient (common) | ✅ Done | `api_job_lifecycle.md` | — |
| 0d | Config read/write | ✅ Done | `config.md`, `cli.md` | — |
| 0e | Test server infrastructure | ✅ Done | — | — |
| 0f | CLI root & common utilities | ✅ Done | `cli.md` | 0d |
| 0g | Session creation & auth status | ✅ Done | `session.md`, `api_resource_management.md`, `cli.md` | 0d |
| 0h | Queue/job credentials & diagnostics | ✅ Done | `api_resource_management.md`, `api_job_lifecycle.md`, `cli.md` | 0g |
| 0i | GUI FFI spike | ✅ Done | — | 0g |
| 1 | Session caching & user-agent | ✅ Done | `session.md` | 0g |
| 2 | Login/logout | ✅ Done | `api_resource_management.md`, `cli.md` | 1 |
| 3 | Queue user credentials | ✅ Done | `session.md` | 1 |
| 4 | Queue parameters | ✅ Done | `api_resource_management.md`, `cli.md` | 1 |
| 5 | Telemetry API integration | ✅ Done | `api_job_lifecycle.md`, `cli.md` | 1 |
| 6 | Job monitoring & logs | ✅ Done | `api_job_lifecycle.md`, `cli.md` | 1 |
| 7 | Job bundle | ✅ Done | `job_bundle.md`, `cli.md` | 1, 4 |
| 8 | Job attachments: core | ✅ Done | `job_attachments_data_transfer.md`, `job_attachments_orchestration.md` | 1 |
| 9 | Job attachments: transfer | ✅ Done | `job_attachments_data_transfer.md`, `cli.md` | 3, 8 |
| 10 | Job attachments: orchestration | ✅ Done | `job_attachments_orchestration.md`, `job_attachments_data_transfer.md`, `cli.md` | 9 |
| 11 | Submit job bundle | ✅ Done | `api_job_lifecycle.md`, `cli.md` | 7, 9 |
| 12 | Job cancel | ✅ Done | `cli.md` | 6 |
| 12b | Job search command | ✅ Done | `cli.md` | 6 |
| 13 | Job download & sync-output | ✅ Done | `job_attachments_orchestration.md`, `job_attachments_data_transfer.md`, `cli.md` | 9 |
| 14 | Handle web URL | ✅ Done | `cli.md` | 13 |
| 15 | Job requeue-tasks | ✅ Done | `cli.md` | 6 |
| 15b | Job get search & estimated time | ✅ Done | `cli.md` | 6 |
| 15c | Job logs auto-selection messages | ✅ Done | `cli.md` | 6 |
| 15d | Level 2 test coverage audit | ✅ Done | — | 11 |
| 15e | Behavioral parity audit | ✅ Done | — | 9 |
| 15f | Wire queue/fleet assume role for all existing CLI commands | ✅ Done | `credential_scoping.md` | 15e-F1 |
| 16 | GUI FFI remaining | ✅ Done (will be superseded by #35) | — | 1-14 |
| 16a | FFI: config, resource listing, auth functions | ✅ Done | — | 0i |
| 16b | FFI: submission with callbacks, telemetry | ✅ Done | — | 16a, 11 |
| 16c | PyO3 Python bindings (`deadline._native`) | ✅ Done | — | 16b |
| 16d | Port Python Qt code into `gui/` package | ✅ Done (superseded by #35) | — | 16c |
| 16d2 | GUI CLI commands (`bundle gui-submit`, `config gui`) | ✅ Done (superseded by #35) | `cli.md` | 16d |
| 16d3 | GUI widget rendering fixes (config shim types) | ✅ Done (superseded by #35) | — | 16d2 |
| 16e | Python packaging (`gui/pyproject.toml`) | ✅ Done (superseded by #35) | — | 16d3 |
| 16f | DCC submitter dependency switchover | Superseded by #35 | — | 16e |
| 17 | MCP server | ✅ Done | `mcp.md` | 1-14 |
| 18 | Submission hooks | ✅ Done | `submission_hooks.md` | 11 |
| 19 | Update checker | ✅ Done | `new_features.md` | 0g |
| 20 | Batch get API helper | ✅ Done | `new_features.md` | — |
| 21 | Python bug-fix parity sweep | ✅ Done | `new_features.md` | — |
| 21b | Spec drift detection | ✅ Done | — | — |
| 21c | Python GUI boundary contract | Eliminated by #35 | — | 16c |
| 21d | CLI backward-compat flags | ✅ Done | — | — |
| 21e | `deadlinew` windowless launcher | ✅ Done | — | — |
| 21f | Windows config path normalization | ✅ Done | — | — |
| 21g | Telemetry parity (success/fail events) | ✅ Done | — | — |
| 21h | Telemetry body-content test coverage | ✅ Done | — | 21g |
| 21i | Telemetry `exception_type` format parity | ✅ Done | — | 21g |
| 21j | `queue_sync_output_stats` telemetry event | ✅ Done | — | — |
| 21k | MCP telemetry (`mcp.latency`, `mcp.usage`) | ✅ Done | — | 17 |
| 22 | Fuzz testing | Not started | — | — |
| 23 | Failure case handling analysis | Not started | — | — |
| 27 | Typed SDK API layer | ✅ Done | — | — |
| 27b | SDK calling behavior audit | ✅ Done | — | 27 |
| 28 | CLI feature parity audit fixes (AUDIT-101,103,106,107) | ✅ Done | — | — |
| 28b | CLI feature parity: remove Rust-only features (AUDIT-108,109) | ✅ Done | — | — |
| 28c | Strict clippy lint resolution (2064 → 0 warnings) | ✅ Done | — | — |
| 29 | Investigate Python `--save-debug-snapshot` bug on no-attachment bundles | Not started | — | — |
| 30 | Rust tooling and optimization setup | ✅ Done | — | — |
| 24 | Production distribution (Rust binary + Qt libs) | Not started | — | 35 |
| 25 | `deadline.client.api` backwards-compat shim | Not started | — | 24 |
| 26 | Installer pipeline update | Not started | — | 24 |
| 31 | Crate restructure (openjd-rs integration + boundary cleanup) | ✅ Done | — | — |
| 32 | Worker agent Python bindings (attachment operations) | Not started | `deadline-python-bindings/worker-agent-bindings.md` | — |
| 33 | Error type parity audit and resolution | Not started | — | — |
| 34 | Library/CLI boundary refactor (remove IniConfig from operations) | ✅ Done | — | — |
| 35 | Rust-native GUI via qtbridge-rust | In progress | `gui-rewrite.md` | 34 |

**Status key:** ✅ Done · ⚠️ Gaps · In progress · Not started · Blocked · Superseded · Eliminated

**Next action item:** #35 (Rust-native GUI spike) — prove qtbridge-rust
works for our use case, then #24 (distribution) and #32 (worker bindings)
can proceed in parallel.

**GUI rewrite plan:** See `specs/gui-rewrite.md` for full architecture,
spike criteria, and migration phases.

**In-progress details:** See `HANDOFF.md` for current state of any
active work items.

**Audit status:** See `audit_reports/archive/2026-04-17-behavioral-parity.md`
(all findings resolved) and `audit_reports/archive/2026-05-01-cli-feature-parity.md`
(all findings resolved). Codebase health audit
complete — see `audit_reports/archive/2026-05-01-codebase-health.md`.

**GUI FFI migration plan (#16a-16f):**

`deadline-cloud-rs` is a complete replacement for `deadline-cloud-python`.
The Python Qt GUI code must ship from this repo, backed by the Rust shared
library. Remaining sub-items:

- **#16f — DCC submitter switchover** (Blocked on #24): Batches A1-A3
  done (import shims, telemetry, API wrappers). Batch B deferred
  (Houdini, separate repo). Batch C (dependency switch in 9 DCC repos)
  blocked on #24 publishing the package.

**Production distribution plan (#24-26):**

The Rust rewrite changes how the `deadline` package is built and
distributed to customers. These items address the end-to-end packaging
story. See `specs/HANDOFF.md` for detailed analysis.

- **#24 — Production distribution (maturin + PyPI)**: Use maturin to
  build a unified `deadline` PyPI package containing the Rust CLI binary,
  the Rust shared library (`deadline._native`), and the Python GUI
  code in a single wheel per platform. Preserves `pip install deadline`
  and `pip install "deadline[gui]"` customer experience. Requires CI
  pipeline to build platform-specific wheels (linux-x64, macos-arm64,
  windows-x64). Must also implement the auto-install-PySide6 flow in
  the Rust CLI (equivalent to Python's `gui_context_for_cli`) and the
  `deadlinew` windowless launcher.
- **#25 — `deadline.client.api` backwards-compat shim**: The original
  `deadline` Python package exposes a public library API
  (`from deadline.client import api; api.list_farms()`). This API is
  documented in the public README and used by customers for pipeline
  automation. Decide on approach: subprocess-based shim that calls the
  Rust CLI, deprecation errors with migration guidance, or a lightweight
  `deadline-auth` helper package for `get_boto3_session`. Must not
  silently break existing users.
- **#26 — Installer pipeline update**: The internal installer pipeline
  currently expects a PyInstaller artifact as the "Deadline Client"
  component. Update it to accept the Rust binary + shared library +
  Python GUI code instead.

  **Pipeline architecture (studied 2026-04-25):**
  The pipeline is a two-level system: individual component installers
  (built per-repo on GitHub CI, uploaded to S3) are merged into a
  unified installer via InstallBuilder. The Deadline Client artifact
  is sourced alongside DCC submitter artifacts, verified (signing),
  then bundled into the unified installer. The unified installer runs
  each individual installer in unattended mode during installation.

  **What changes:**
  - The Deadline Client individual installer artifact changes from a
    PyInstaller bundle to a Rust binary + Python GUI assembly. The
    artifact format (a platform-specific installer) stays the same —
    only the contents change.
  - The CI step that builds the Deadline Client artifact needs to:
    (1) compile the Rust binary, (2) build `_native.abi3.so` via
    maturin, (3) bundle a Python runtime + PySide6/qtpy/PyYAML + the
    GUI Python code into `_internal/`. This replaces the PyInstaller
    `make_exe.py` step.
  - The pipeline already stores platform-specific Python distributions
    in its dependency bucket (embeddable zip for Windows, tarball for
    Linux, pkg for macOS). These are reused for the new `_internal/`
    assembly.
  - The installed layout (`DeadlineCloudSubmitter/DeadlineClient/`)
    stays the same. The `deadline` binary path is unchanged. Installer
    tests that check binary permissions and component evidence paths
    should pass without modification.
  - `_internal/` shrinks dramatically: no boto3, botocore, click,
    xxhash, psutil, dateutil, or Python CLI code. Only Python runtime
    + Qt deps + GUI code + `_native.abi3.so` remain.

  **Risks:**
  - Cross-compilation of the Rust binary for all three platforms needs
    CI infrastructure with appropriate build images.
  - Windows embeddable Python is minimal (no pip, no site-packages by
    default). The `_internal/` assembly script must handle per-platform
    Python distribution differences.
  - The `_native.abi3.so` must be built per-platform (it's a native
    extension). maturin handles this but needs the right build targets.
  - The InstallBuilder XML and wrapper components may need minor
    updates if the individual installer's internal structure changes
    (e.g. different uninstall behavior).

**Technical debt:**
- **#33 — Error type parity audit**: Python exceptions surface their
  type name to customers (in tracebacks and via `except` clauses in
  PyO3 bindings). Several Python exception types are flattened into
  generic `OperationError(String)` or `AssetSync(String)` in Rust:
  - `NonValidInputError` → currently `AssetSync` (should be distinct)
  - `VFSLaunchScriptMissingError`, `VFSRunPathNotSetError` → missing
  - `UnsupportedProfileTypeForLoginLogout` → `OperationError`
  - `PidLockAlreadyHeld` → `OperationError`
  - `JobFetchFailure` → `OperationError`
  Also audit how openjd-rs `SnapshotError` variants map to
  `JobAttachmentsError` — currently all wrapped as strings via
  `.to_string()`. Consider preserving error structure for PyO3.
- **KMS error guidance on download**: Wrap openjd's generic S3 403 errors
  during download to detect KMS issues and suggest "ensure kms:Decrypt
  permission". See `download.rs:418`.
- **Spec docs audit**: Review `specs/` docs to ensure they reflect
  current implementation.
- **#15d**: Audit Level 1 tests for conversion to Level 2.
- **Realistic test IDs**: Replace hardcoded pseudo-IDs with realistic
  Deadline Cloud ID format.
- **Test consolidation**: Audit for redundant/overlapping tests.
- **#23 — Failure case handling analysis**: Systematic audit of error
  handling across all crates.
- **GUI Python code smell audit**: Review ported `gui/` Python code for
  patterns that no longer make sense now that Rust handles business logic.
- **Pydantic boundary validation**: Explore using Pydantic to validate
  types crossing the Rust→Python boundary (e.g. `ProgressReportMetadata`,
  `IniConfig`, API response dicts). Would catch contract drift between
  the Rust structs and Python dataclasses at the PyO3 boundary.
- **API output field ordering**: Typed SDK output uses `HashMap` for maps
  (e.g. `TaskParameterValue`, `taskRunStatusCounts`), which has
  non-deterministic iteration order. Currently we sort parameter keys
  alphabetically in `requeue-tasks` output. Audit all CLI print paths
  that serialize SDK map types to ensure deterministic output order.
  The Python CLI preserved wire order via JSON parsing; Rust needs
  explicit sorting wherever maps are displayed.
- **Python formatting/linting**: Add ruff (format + lint) for `gui/`
  Python code. Currently no Python formatter is configured.
- **Reduce `serde_json::Value` usage** — 108 references in CLI code.
  Address incrementally when touching those files.
- **Audit `collect()` then iterate** — 18 sites. Quick fixes when
  touching those files.
- **JSON progress lines in `--output json` mode** — download-output and
  download-input don't emit `{"messageType":"progress",...}` lines during
  download (Python does via click progressbar callback). Low priority.
- **`make test` dependency bootstrapping** — `make test-bindings` requires
  `maturin` to be installed. Currently assumes it's on PATH. Consider
  adding `maturin>=1.7` to `[test]` deps or documenting the prerequisite.

- **Evaluate extracting `config` module into standalone crate** — After
  #34 completes, the config module is only consumed by CLI and PyO3
  (library operations take explicit params). Evaluate whether
  `deadline-config` as a separate crate improves build times, clarifies
  the dependency graph, or is unnecessary churn.
- **Evaluate Rust-native GUI (eliminate Python entirely)** — Explore
  replacing the Python Qt GUI with a Rust-native GUI framework (e.g.
  `slint`, `iced`, `cxx-qt`, `egui`). Would remove the Python runtime,
  PySide6, PyO3 bindings, and `_internal/` assembly from the distribution.
  Trade-offs: ecosystem maturity, accessibility support, DCC submitter
  integration (currently Python plugins), and development velocity.

**Dependency upgrades needed:**
- **rusqlite** 0.32 → 0.39 (major, breaking changes likely)
- **pyo3** 0.24 → 0.28 (major, breaking API changes)
- **rustls-webpki** advisories pinned by transitive hyper-rustls 0.24
  (awaiting AWS SDK upstream fix). See `deny.toml`.
- **PyO3 Send/Sync safety**: `PySubmissionHandler` uses `unsafe impl Send/Sync`
  with `#[allow(unsafe_code)]`. Investigate whether upgrading PyO3 or using
  `Py<PyAny>` (Send+Sync by design in newer PyO3) can eliminate the unsafe.
