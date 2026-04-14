# Migration Strategy

Full migration of all AWS Deadline Cloud client software from Python to Rust.
This covers the CLI, the shared GUI, and all DCC submitter
plugins.

## Goals

1. **Speed.** Compiled native code starts faster and runs faster. Job
   attachment hashing, upload, and download are the hot paths. The worker
   agent processes every job on every worker machine — performance there
   directly impacts fleet throughput.

2. **Type safety.** Rust's type system catches null references, type
   mismatches, unhandled error variants, and data races at compile time.
   Fewer runtime surprises, less time debugging production issues.

3. **Development velocity.** The Rust compiler's error messages are precise
   and actionable. `cargo build` and `cargo test` are fast. The TDD workflow
   (red → green → refactor) has a tight feedback loop. We spend less time
   writing defensive code and more time shipping features.

4. **Single codebase for logic.** One language for all business logic. Python
   exists only as thin DCC plugin glue (scene introspection) and Qt widget
   layout (~15 files of pure presentation, no business logic).

## End State

| Component | Today (Python) | End state |
|-----------|---------------|-----------|
| CLI binary | PyInstaller-packaged Python | Rust binary (`deadline-cli`) |
| Config library | `deadline.client.config` | `deadline-config` crate |
| API client | `deadline.client.api` | `deadline-client` crate |
| Job bundle library | `deadline.client.job_bundle` | `deadline-job-bundle` crate |
| Job attachments | `deadline.job_attachments` | `deadline-job-attachments` crate |
| GUI business logic | `deadline.client.ui.controllers`, API calls, auth status | Rust via `deadline-gui-ffi` shared library |
| GUI widget layout | `deadline.client.ui.dialogs`, `deadline.client.ui.widgets` (PySide/QWidgets) | Python QWidgets (~15 files, pure presentation, no business logic) |
| GUI FFI layer | N/A | `deadline-gui-ffi` crate (C ABI shared library) |
| DCC submitter plugins | Python importing `deadline.client.*` | Thin Python: scene introspection + loads `deadline-gui-ffi` |
| Unreal submitter | Python | Rust via `deadline-gui-ffi` C ABI (no Python) |
| After Effects | JSX shelling out to CLI | No change (already uses CLI binary) |
| MCP server | `deadline._mcp` (Python, FastMCP) | Rust binary (`deadline-mcp` crate, rmcp SDK) |
| `deadline-cloud-python` | 15k+ lines of business logic + GUI | Deleted — GUI widget files move into monorepo `gui/` directory |

## GUI Strategy

### Architecture

The GUI uses the same Qt QWidgets framework as today, but all business logic
moves to Rust. The Python widget code becomes a pure presentation layer:

```
Python QWidgets     ← QDialog, QFormLayout, QComboBox (existing code, refactored)
       ↕
deadline-gui-ffi    ← C ABI shared library (.so/.dylib/.dll)
       ↕
Rust crates         ← config, API calls, job bundle, attachments, telemetry
```

What stays in Python: widget layout, signal/slot wiring, styling.
What moves to Rust: config reading, API calls, auth, submission, attachments,
telemetry, parameter validation — all business logic.

### Why not Rust Qt?

We evaluated [cxx-qt](https://github.com/KDAB/cxx-qt) for writing the GUI
entirely in Rust. Findings:

- cxx-qt **does not provide Rust bindings for QWidgets APIs** — their README
  explicitly states QWidgets support is "limited"
  ([source](https://github.com/KDAB/cxx-qt))
- cxx-qt's full support is for QML (Qt's declarative UI language), not QWidgets
- All cxx-qt examples are QML-based; no QDialog/QFormLayout examples exist
- QML has maintainability concerns: dynamically typed at runtime, requires
  custom styling to match native OS look, weaker debugging than Rust
- cxx-qt is pre-1.0 (v0.8.1 as of Feb 2026) — API may change between versions

The alternative of writing QWidgets in C++ with Rust FFI was also rejected
because it introduces a second systems language to maintain, conflicting with
the single-codebase goal.

**Decision:** Keep the ~15 files of QWidgets layout code in Python. Move all
business logic to Rust. This gives us type safety and speed for all logic,
mature tooling (PySide/Qt is battle-tested), native OS appearance, and no
pre-1.0 dependencies. The Python GUI files contain zero business logic — they
are a dumb view layer that calls Rust for every operation.

## Monorepo Structure

All Rust crates, the Python GUI widget files, and documentation live in one
repository:

```
deadline-cloud-rs/
├── crates/
│   ├── deadline-cli/              # CLI binary
│   ├── deadline-gui-ffi/          # C ABI shared library for GUI + DCC plugins
│   ├── deadline-mcp/              # MCP server binary (rmcp SDK)
│   ├── deadline-config/           # Config file read/write
│   ├── deadline-client/           # AWS API calls
│   ├── deadline-job-bundle/       # Job bundle parsing and parameters
│   ├── deadline-job-attachments/  # Manifests, hashing, S3 transfer, caches
│   ├── deadline-models/           # Shared types and errors
│   ├── deadline-common/           # Utilities
│   └── deadline-test-server/      # Wiremock stub server for tests
├── gui/                           # Python GUI widgets (pure presentation)
│   ├── dialogs/
│   ├── widgets/
│   └── resources/
├── Cargo.toml
└── docs/
```

### Why monorepo?

During migration, library crates change frequently. A monorepo ensures the
CLI, GUI FFI, and Python GUI are always in sync. Cross-cutting
changes are one commit, one CI run. Can split later once libraries stabilize.

## DCC Plugin Constraints

| DCC | Plugin system | Can avoid Python? | Source |
|-----|--------------|-------------------|--------|
| **Blender** | Python only | ❌ No | "we are not planning to accept binary plugins" ([source](https://devtalk.blender.org/t/why-is-there-no-c-api/24026)) |
| **Maya** | C++ SDK + Python | ⚠️ Partially | C++ plugins supported ([source](https://help.autodesk.com/cloudhelp/2024/ENU/Maya-SDK/files/Maya-Python-API/Maya_SDK_Maya_Python_API_Maya_Python_Plug_in_Learning_html.html)) |
| **Houdini** | HDK (C++) + Python | ⚠️ Partially | HDK for C++, `hou` for Python ([source](https://www.sidefx.com/docs/houdini/hom/extendingwithcpp.html)) |
| **Nuke** | NDK (C++) + Python | ❌ Effectively no | NDK "not suitable for scripting style functionality" ([source](https://learn.foundry.com/nuke/13.1v1/content/misc/nuke_devs.html)) |
| **Cinema 4D** | C++ SDK + Python | ⚠️ Partially | Both supported, own UI framework ([source](https://www.maxon.net/en/cinema-4d/features/sdk-development-community)) |
| **3ds Max** | C++ SDK + Python | ⚠️ Partially | Python API on top of C++ SDK ([source](https://help.autodesk.com/cloudhelp/2021/ENU/Max-Python-API/about_the_3ds_max_python_api.html)) |
| **VRED** | Python only | ❌ No | "Script plugins are a special kind of Python script" ([source](https://help.autodesk.com/cloudhelp/2025/ENU/VRED-Tutorials/files/Python-Tutorials/4Tutorial-VREDPro.html)) |
| **Unreal** | C++ (UBT) + Python | ✅ Yes | Rust FFI proven ([source](https://github.com/MaikKlein/unreal-rust)) |
| **After Effects** | ExtendScript (JSX) | ✅ Yes | Already uses CLI binary |

**Decision:** All DCC plugins use a thin Python layer that loads the Rust
shared library via ctypes. This is consistent across all DCCs (required for
Blender/VRED, simpler for the rest). The Python layer is only scene
introspection (~100-200 lines per plugin). Can be revisited per-DCC later.

## Downstream Dependency Analysis

### Post-migration dependency

| Repo | Depends on | Python remains? | What Python does |
|------|-----------|-----------------|------------------|
| Blender | `deadline-gui-ffi` .so | Yes | `bpy` scene queries + GUI widgets |
| Houdini | `deadline-gui-ffi` .so | Yes | `hou` scene queries + GUI widgets |
| Maya | `deadline-gui-ffi` .so | Yes | `maya.cmds` scene queries + GUI widgets |
| Nuke | `deadline-gui-ffi` .so | Yes | `nuke` scene queries + GUI widgets |
| Cinema 4D | `deadline-gui-ffi` .so | Yes | `c4d` scene queries + GUI widgets |
| VRED | `deadline-gui-ffi` .so | Yes | `vrNodeService` scene queries + GUI widgets |
| 3ds Max | `deadline-gui-ffi` .so | Yes | `pymxs` scene queries + GUI widgets |
| Unreal | `deadline-gui-ffi` .so (C ABI) | No | N/A |
| After Effects | `deadline-cli` binary | No | N/A |

## Risk Assessment

The migration's success depended on several technical bets. All
project-blocking risks have been proven (see Risk Spikes table in
`README.md`). The risk descriptions below are retained as a decision record.

### Project-blocking risks

| Risk | Why it matters | What could go wrong |
|------|---------------|---------------------|
| **GUI FFI (Python ↔ Rust ↔ Qt)** | The entire architecture assumes Python QWidgets can call Rust via ctypes C ABI. Phases 3, 4, 6, and the DCC strategy all depend on this. | ctypes callbacks crash or deadlock with Qt's event loop. Memory ownership across the FFI boundary causes segfaults. Platform differences (`.so`/`.dylib`/`.dll`) cause loading failures. Thread safety between QThread worker and Qt main thread breaks. |
| **Job attachments S3 performance** | "Speed" is goal #1. Job attachment hashing and S3 transfer are the hot paths. If Rust is not faster than Python here, the primary justification for the migration weakens. | Rust S3 SDK throughput doesn't match boto3's transfer manager. Multipart upload/download coordination is more complex than expected. Hash computation parallelism doesn't scale. |
| **DCC plugin Qt version conflicts** | Each DCC ships its own Qt and Python version. The shared library must load cleanly in all of them. | Maya ships Qt 5.15, another DCC ships Qt 6. The Rust shared library links against system libraries that conflict with the DCC's bundled ones. Python version differences cause ctypes ABI issues. |

### Significant risks (recoverable but costly)

| Risk | Why it matters | What could go wrong |
|------|---------------|---------------------|
| **VFS (FUSE) on all platforms** | §28 has 89 test cases. VFS is Linux-only (FUSE). macOS and Windows need different approaches. | Rust FUSE libraries are immature. Platform-specific code paths multiply testing burden. May need to defer VFS and use COPIED mode only. |
| **AWS SDK for Rust limitations** | Output types lack `serde::Serialize` ([#269](https://github.com/awslabs/aws-sdk-rust/issues/269), open since 2021). Paginators don't support interceptors. | Workarounds (`ResponseBodyCapture`, manual pagination) may hit edge cases with new API shapes. See `docs/crate_specs/deadline-client.md` § "Future Improvements" for the Smithy model filtering approach. |

### Low risks (just labor)

Remaining CLI commands, config operations, telemetry, MCP server. These use
proven patterns and well-understood APIs. The risk is schedule, not feasibility.

## Fail-Fast Strategy

All four risk spikes have passed. See the Risk Spikes table in
`README.md` for the summary. The spike definitions and pass/fail
criteria below are retained as a decision record.

### Required spikes (all passed)

| Spike | Proves | Pass criteria |
|-------|--------|---------------|
| **GUI FFI round-trip** | Python ↔ Rust ↔ Qt works | Loads on Linux, macOS, Windows. Callback doesn't crash. Qt event loop stays responsive. |
| **GUI FFI inside a DCC** | Shared library loads in a real DCC Python environment | Returns correct result. No symbol conflicts. No Qt version crash. |
| **S3 transfer performance** | Rust S3 throughput ≥ Python | Rust throughput ≥ Python throughput. |
| **Job attachment hashing** | Parallel hashing is fast | Rust is measurably faster. Hashes match Python output byte-for-byte. |

## Phased Rollout

Each phase ships independently and delivers value on its own. The phases
are ordered by shipping sequence, but risk spikes (above) run first and
may cause phases to be revised or reordered.

### Phase 1: Rust CLI Binary (in progress)

**Goal:** Replace the Python CLI with a behaviorally identical Rust binary.

**Scope:**
- All CLI subcommands: `config`, `auth`, `farm`, `fleet`, `queue`, `worker`,
  `job`, `bundle submit`, `attachment`, `manifest`, `handle-web-url`
- Library crates: `deadline-config`, `deadline-client`, `deadline-job-bundle`,
  `deadline-job-attachments`, `deadline-models`, `deadline-common`
- Telemetry
- GUI commands (`config gui`, `bundle gui-submit`) print an error directing
  users to the Python package until Phase 3

**Ship criteria:**
- All test spec sections (§1–§48, excluding GUI cases) pass
- Output identical to Python CLI
- After Effects submission works end-to-end
- Ships alongside Python package initially (opt-in via env var)

### ~~Phase 2: Rust Worker Agent~~ (out of scope)

The worker agent is being rewritten in Rust in a separate project. It is
not part of this repository's scope.

### Phase 3: GUI FFI + Python Widget Refactor

**Goal:** Build `deadline-gui-ffi` shared library and refactor the Python GUI
widgets to call Rust for all business logic.

**Scope:**
- `deadline-gui-ffi` crate: C ABI shared library exposing config, auth, API
  listing, submission, telemetry
- Refactor Python GUI files: remove all `deadline.client` business logic
  imports, replace with `ctypes` calls to `deadline-gui-ffi`
- Move refactored Python GUI files into monorepo `gui/` directory
- `deadline config gui` and `deadline bundle gui-submit` work from Rust CLI
  (CLI spawns Python process that loads GUI widgets + shared library)

**Prerequisites:** Phase 1 library crates complete. GUI FFI spikes passed
(both standalone and inside-DCC).

**Ship criteria:**
- Visual and behavioral parity with current Python GUI
- GUI commands work from Rust CLI
- Shared library loads correctly from Python via ctypes
- Tested on Linux, macOS, Windows

### Phase 4: Migrate DCC Submitter Plugins

**Goal:** Rewrite each DCC plugin to use `deadline-gui-ffi` shared library
instead of importing Python `deadline.client`.

**Order:**

| Order | Plugin | Rationale |
|-------|--------|-----------|
| 1 | Blender | Simplest submitter, fewest custom widgets |
| 2 | Nuke | Similar complexity to Blender |
| 3 | Cinema 4D | No API imports, only UI + job bundle |
| 4 | VRED | Similar to Cinema 4D |
| 5 | Maya | More complex (multiple render layers, cameras) |
| 6 | 3ds Max | Similar to Maya, plus render elements widget |
| 7 | Houdini | Most complex (custom submission flow, direct S3 access) |

**Prerequisites:** Phase 3 complete. GUI FFI inside-DCC spike passed for
the target DCC.

**Ship criteria per plugin:**
- Submission produces identical job bundles
- Integration tests pass
- Visual parity for submit dialog

### Phase 5: Rust MCP Server

**Goal:** Implement the MCP server in Rust as `deadline-mcp` crate using the
official Rust MCP SDK ([rmcp](https://rust.sdk.modelcontextprotocol.io/),
4.7M+ downloads on crates.io).

**Scope:**
- `deadline-mcp` crate: MCP server binary using `rmcp`
- All existing MCP tools: `list_farms`, `list_queues`, `list_jobs`,
  `search_jobs`, `get_job`, `get_session`, `list_sessions`, `list_steps`,
  `list_tasks`, `get_session_logs`, `get_session_and_worker_logs`,
  `submit_job`, `download_job_output`, `check_authentication_status`,
  `list_fleets`, `list_storage_profiles_for_queue`
- Telemetry recording per tool invocation
- `deadline mcp-server` CLI command starts the server

**Prerequisites:** Phase 1 library crates complete (`deadline-client` for
all API calls).

**Ship criteria:**
- All MCP tools produce identical results to Python implementation
- Server starts via `deadline mcp-server` and responds to MCP protocol
- Telemetry events match Python implementation format

### Phase 6: Migrate Unreal to Full Rust

**Goal:** Rewrite Unreal submitter using `deadline-gui-ffi` C ABI directly.
No Python.

### Phase 7: Delete `deadline-cloud-python`

**Goal:** Remove the Python library repository.

**Prerequisites:** All phases complete. No downstream repo imports
`deadline.client` or `deadline.job_attachments`.

**What remains in Python across all repos:**
- `gui/` directory in monorepo (~15 files of Qt widget layout)
- DCC plugin scene introspection (~100-200 lines per plugin per DCC repo)

## Decision Record

| Decision | Rationale |
|----------|-----------|
| Python QWidgets for GUI layout | cxx-qt does not provide QWidgets bindings. QML has maintainability concerns (runtime typed, custom styling needed, pre-1.0 dependency). Writing C++ QWidgets adds a second systems language. Keeping Python QWidgets as a dumb view layer is the least-smell option — mature tooling, native OS look, minimal refactor. |
| All business logic in Rust | Config, API, job bundle, attachments, telemetry, auth — all in Rust crates, exposed via `deadline-gui-ffi` C ABI shared library. Python GUI calls Rust for every operation. |
| Thin Python layer for all DCC plugins | Blender and VRED require Python (no binary plugin support). Consistent approach across all DCCs. Python is only scene introspection + loading the Rust shared library. |
| Monorepo | During migration, library crates change frequently. Monorepo keeps CLI, GUI FFI, and Python GUI in sync. One commit, one CI run for cross-cutting changes. |
| Full Rust for Unreal | Unreal supports C++ plugins natively. Rust interops via C ABI. No Python needed. |
| After Effects unchanged | Already uses CLI binary via subprocess. |
| Migrate DCC plugins one at a time | Each is independent. Prove pattern on simplest (Blender), then parallelize. |
| Ship each phase independently | Every phase delivers value on its own. No big bang. |
