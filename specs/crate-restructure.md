# Crate Restructure Plan

**Work item:** #31 (expanded scope)
**Supersedes:** `specs/openjd-integration.md`

## Context

The deadline-cloud-rs codebase was ported from Python with a "behavioral
fidelity first" philosophy. The result is correct (492 CLI tests pass) but
carries Python's design patterns into Rust: sequential loops where rayon
fits, stringly-typed APIs, monolithic functions mixing business logic with
presentation, and ~5,500 lines of reimplemented logic that openjd-rs
already provides as a well-designed library.

This plan addresses three problems in order:
1. **Duplicated code** — replace with openjd-rs (Steps 1–8)
2. **Blurred CLI/library boundary** — separate presentation from logic (Step 9)
3. **Crate consolidation** — merge small crates (Step 10)
4. **Type bridge** — use openjd types directly, delete wrappers (Step 11)
5. **Non-idiomatic patterns** — adopt Rust conventions (Step 12)

## Constraints

- **Worker agent dependency:** The worker agent (`deadline-cloud-worker-agent`)
  imports attachment models, download/upload orchestration, and progress types.
  These must remain independently consumable without pulling in auth/session/CLI.
- **Python bindings:** `deadline-python-bindings` calls into submission and
  resource-listing functions. After restructure it adapts to the new API.
- **Test contract:** 492 CLI snapshot tests are ground truth. All must pass
  after each step.
- **Python parity:** On-disk/on-wire formats must match the production Python
  package (`aws-deadline/deadline-cloud-job-attachments`). Verify before swapping.

---

## Target Architecture

```
deadline-cli (binary)
├── Clap arg parsing, output formatting, progress bars, user prompts
├── Resolves config, defaults, policy decisions
└── depends on: deadline-cloud, openjd-snapshots (for types)

deadline-cloud (library crate, ~16K lines → ~12K after engine swap)
├── mod config       — INI config read/write (from deadline-config)
├── mod api          — AWS SDK client, auth, session, telemetry (from deadline-api)
├── mod bundle       — Submission orchestration, hooks, loader (from deadline-job-bundle)
├── mod attachments  — Deadline-specific orchestration + models (from deadline-job-attachments)
│   ├── models.rs    — Attachments, ManifestProperties, StorageProfile, etc.
│   ├── s3_cache.rs  — impl AsyncDataCache for credential-scoped S3 client
│   ├── download.rs  — Orchestration: which manifests to fetch, conflict policy
│   ├── upload.rs    — Orchestration: prepare paths, call openjd-snapshots engine
│   └── api.rs       — Deadline API calls for manifest read/write
└── re-exports openjd types used in public API

deadline-python-bindings (PyO3, unchanged role)
└── depends on: deadline-cloud

openjd-rs (external, path dep during dev, crates.io for release)
├── openjd-snapshots — hashing, manifests, caches, upload/download engine
├── openjd-expr      — path mapping, format strings
└── openjd-model     — template parsing, validation (optional)
```

**Worker agent** (future Rust component) would depend on:
- `deadline-cloud::attachments` (models + orchestration)
- `openjd-snapshots` directly (for download engine)

---

## Steps 1–8: Replace Duplicated Code (openjd-rs Integration)

Replace duplicated low-level code with openjd-rs crate dependencies.

### Methodology

**Default: Direct swap** (`.kiro/prompts/dependency-swap-{plan,execute}.md`)
```
1. PLAN     — read code, read replacement API, record baseline, assess complexity
2. EXECUTE  — swap, update callers, delete old code, prune tests, verify, commit
```

**Fallback for high-complexity swaps: Strangler fig**
Use when >10 callers across multiple crates, complex type conversions, or
behavioral differences. Add shim alongside old code → verify equivalence →
swap callers incrementally → delete old code.

---

### Step 1: Hashing ✅

| Our code | Replacement | Notes |
|----------|-------------|-------|
| `asset_manifests.rs::HashAlgorithm` | `openjd_snapshots::hash::HashAlgorithm` | Drop our `_alg` param (only xxh128 exists) |
| `asset_manifests.rs::hash_data()` | `openjd_snapshots::hash::hash_data()` | Signature: `&[u8] → String` (no alg param) |
| `asset_manifests.rs::hash_file()` | `openjd_snapshots::hash::hash_file()` | Returns `io::Result<String>`, wrap to `JobAttachmentsError` |

### Step 2: Manifest Codec ✅

| Our code | Replacement | Notes |
|----------|-------------|-------|
| `AssetManifest::encode()` | `openjd_snapshots::encode_snapshot_v2023()` | Takes `&Snapshot`, need `AssetManifest → Snapshot` conversion |
| `decode_manifest()` | `openjd_snapshots::decode_v2023()` | Returns `Snapshot`, need `Snapshot → AssetManifest` conversion |

Both produce identical output (canonical JSON + `\uXXXX` escaping). Verified against Python.

### Step 3: S3CheckCache ✅

| Our code | Replacement | Notes |
|----------|-------------|-------|
| `S3CheckCache` | `openjd_snapshots::S3CheckCache` | Same float-timestamp format. API adapter: `get_entry(&str) → Option<String>` |

### Step 4: HashCache ✅

Accepted one-time cache invalidation. openjd-rs stores mtime as u64
nanoseconds; our old code stored `str(datetime.fromtimestamp(st_mtime))`.

### Step 5: Diff ✅

| Our code | Replacement | Notes |
|----------|-------------|-------|
| `diff.rs::hash_diff` | `openjd_snapshots::diff_snapshots` | Via type bridge (`AssetManifest ↔ Snapshot`) |
| `diff.rs::fast_diff` | *(kept)* | Filesystem-based, no openjd equivalent |

### Step 6: Path Mapping ✅

| Our code | Replacement | Notes |
|----------|-------------|-------|
| `path_mapping.rs::PathMappingRuleApplier` (trie, 500 lines) | `openjd_expr::apply_rules()` | |
| `models.rs::PathFormat` | `openjd_expr::PathFormat` | Serde adapter for case difference |

### Step 7: Upload/Download Engine (BLOCKED)

| Our code | Replacement |
|----------|-------------|
| `upload.rs` (2,000 lines) | `openjd_snapshots::upload_abs_manifest` *(doesn't exist yet)* |
| `download.rs` (1,600 lines) | `openjd_snapshots::download_abs_manifest` |
| `s3.rs` (400 lines) | `openjd_snapshots::S3DataCache` |

**Blocked on openjd-snapshots contributions:**
1. **Upload:** `hash_upload_abs_manifest` rejects pre-hashed files. Need a new
   `upload_abs_manifest` function that takes already-hashed manifests. Our CLI
   separates hashing from uploading (user confirmation step between them).
2. **Download:** `S3DataCache` has no fallback key support. Our code retries
   with `{hash}` (no `.xxh128` suffix) on 404 for backward compatibility.

**What stays regardless:** ~1,500 lines of Deadline-specific orchestration
(prepare_paths_for_upload, manifest S3 key construction, output manifest
listing, incremental download checkpoints, include/exclude path filtering).

### Step 8: Cleanup (vestigial wrappers) ← CURRENT

Remove dead parameters and inline trivial delegates:
- `hash_data(data, HashAlgorithm::Xxh128)` → `hash_data(data)`
- `hash_file(path, HashAlgorithm::Xxh128)` → `hash_file(path)`

---

## Step 9: Separate (CLI vs Library Boundary)

### The Principle

**Library:** takes typed inputs, returns typed outputs. Never touches
stdout/stderr, never reads config files, never resolves default paths.

**CLI:** parses args, reads config, resolves defaults, drives user interaction,
formats output.

### Violations to Fix (ordered by execution sequence)

#### 9.1 Config resolution in library ✅ Done

**Before:**
```rust
pub fn get_s3_max_pool_connections(config: Option<&IniConfig>) -> usize {
    let val = match config {
        Some(c) => get_setting("settings.s3_max_pool_connections", c),
        None => get_setting_from_disk("settings.s3_max_pool_connections"),
    };
    val.parse().unwrap_or(50)
}
```

**After:**
```rust
pub struct S3TransferConfig {
    pub max_pool_connections: usize,  // default: 50
    pub small_file_threshold_multiplier: usize,  // default: 20
}

impl Default for S3TransferConfig { ... }
```

#### 9.2 Progress reporting in library functions ✅ Done

**Before:**
```rust
pub fn hash_assets_and_create_manifest(
    asset_groups: &[AssetRootGroup],
    total_input_files: u64,
    total_input_bytes: u64,
    hash_cache_dir: Option<&str>,
    on_preparing_to_submit: Option<Box<dyn Fn(ProgressReportMetadata) -> bool + Send>>,
) -> Result<(SummaryStatistics, Vec<AssetRootManifest>), JobAttachmentsError>
```

**After:**
```rust
pub struct HashAssetsOptions {
    pub hash_cache: Option<Arc<HashCache>>,
    pub on_progress: Option<Box<dyn Fn(&HashStatistics) -> bool + Send>>,
}

pub fn hash_assets(
    groups: &[AssetRootGroup],
    options: HashAssetsOptions,
) -> Result<HashAssetsResult, AttachmentError>

pub struct HashAssetsResult {
    pub manifests: Vec<AssetRootManifest>,
    pub stats: HashStatistics,  // raw numbers only
}
```

CLI owns the progress bar and `human_readable_file_size` formatting.

#### 9.3 User interaction in submission ← Next

**Before:**
```rust
pub struct SubmitJobParams<'a> {
    pub auto_accept: bool,
    pub print_callback: Box<dyn Fn(&str) + Send + 'a>,
    pub interactive_confirmation_callback: Option<ConfirmFn>,
}
```

**After:**
```rust
pub struct SubmitJobRequest {
    pub job_bundle_dir: PathBuf,
    pub parameters: Vec<Value>,
    pub name: Option<String>,
}

pub enum SubmitAction {
    NeedsConfirmation { message: String, default: bool },
    Ready(PreparedJob),
}

pub fn prepare_submission(req: SubmitJobRequest) -> Result<SubmitAction, SubmitError>

impl PreparedJob {
    pub async fn submit(self, client: &DeadlineClient) -> Result<String, SubmitError>
}
```

#### 9.4 Path types (`&str` → `&Path`/`PathBuf`)

**Before:** `cache_dir: &str`, `file_path: String`
**After:** `cache_dir: &Path`, `file_path: PathBuf`

#### 9.5 Presentation utilities

Move to CLI crate:
- `human_readable_file_size()`
- `ProgressReportMetadata.progress_message` (formatted string)
- Any `format!` that produces user-facing text

---

## Step 10: Merge (Crate Consolidation)

After Steps 1–9, merge the library crates into `deadline-cloud`:

```
deadline-config           → deadline-cloud::config
deadline-api              → deadline-cloud::api
deadline-job-bundle       → deadline-cloud::bundle
deadline-job-attachments  → deadline-cloud::attachments
```

**Why merge:**
- `deadline-api` is only used by CLI and python-bindings (worker agent doesn't need it)
- After Step 7, `deadline-job-attachments` is ~3,500 lines — too small to justify a separate crate
- Single crate = simpler dependency graph, easier refactoring across modules
- Estimated size: ~16K lines (reasonable for one crate)

**Why not merge yet:**
- Wait until Steps 1–9 stabilize the API surface
- Merging before deletion means moving code that's about to be deleted

**Worker agent consideration:** If the worker agent needs attachment
models without auth/API, expose `deadline-cloud::attachments::models` as
a feature-gated module. Decide after Step 7 when final shape is clear.

---

## Step 11: Collapse Type Bridge — Use openjd Types Directly

**Goal:** Delete `AssetManifest`, `ManifestPath`, `HashAlgorithm`,
`ManifestVersion`, and all conversion code. Use `openjd_snapshots::Snapshot`,
`FileEntry`, etc. directly throughout the codebase.

**What gets deleted:**
- `asset_manifests.rs` (~200 lines) — `AssetManifest`, `ManifestPath`,
  `HashAlgorithm`, `ManifestVersion`, `hash_data`, `hash_file` wrappers,
  `encode`/`decode_manifest` type-bridge functions
- `diff.rs::asset_manifest_to_snapshot()` — type bridge helper
- `caches.rs` re-exports — callers use `openjd_snapshots::HashCache` directly

**What callers change to:**
```rust
// Before
use crate::asset_manifests::{AssetManifest, ManifestPath, hash_data, decode_manifest};
let manifest = decode_manifest(json)?;
let hash = hash_data(bytes);

// After
use openjd_snapshots::{Snapshot, FileEntry, decode_v2023, hash};
let snapshot = decode_v2023(json)?;
let hash = hash::hash_data(bytes);
```

**Why this is Step 11 (not Step 10):**
- Step 10 is a guaranteed-safe mechanical move (same code, different layout)
- This step changes semantics: different types flow through the system
- Must update every caller of `AssetManifest` (~20 sites across attachments)
- Must update integration tests that construct `AssetManifest` directly
- Must verify openjd's error messages are acceptable (or map them)

**Estimated impact:** -400 lines, 0 new lines of logic (just deletions
and type substitutions).

---

## Execution Order

```
Steps 1–6 (hashing, codec, caches, diff, path mapping)  ← DONE
    ↓
Step 8 (cleanup vestigial wrappers)  ← DONE
    ↓
Step 7 (upload/download engine)  ← DONE
    ↓
Step 9 (CLI/library boundary)  ← DONE
    ↓
Step 10 (crate merge → deadline-lib)  ← DONE
    ↓
Step 11 (collapse type bridge — use openjd types directly)  ← DONE
    ↓
Step 12 (full CLI behavioral audit — verify all commands match Python)  ← NEXT
    ↓
Step 13 (idiomatic patterns)  ← ongoing, interleaved
```

---

## Step 12: Full CLI Behavioral Audit

Verify every CLI command produces correct output and matches the Python
client's behavior. Run each command against real or stubbed services,
compare output format, error messages, and exit codes.

**Scope:**
- All commands listed in `specs/deadline-cli/reference.md`
- `--help` output for every command and subcommand
- Auth flow (login/logout/status)
- Resource commands (farm, fleet, worker, queue, job)
- Bundle submit (with and without attachments)
- Attachment/manifest commands
- Error paths (missing args, bad credentials, not found)
- Python parity: compare with `deadline` Python CLI where applicable

**Process:**
1. Build the Rust binary
2. Run each command with `--help`, verify flags match spec
3. Run behavioral tests against stub server or real service
4. Document any differences from Python CLI
5. Fix bugs found; note acceptable divergences

**Output:** A report at `audit_reports/cli-behavioral-audit-{date}.md`
listing pass/fail per command and any divergences.

---

## Step 13: Redesign (Idiomatic Patterns)

Applied to remaining code after Steps 1–12.

### 13.1 Options structs with Default

```rust
// Before: positional params, hard to extend
pub fn download_files(manifests: &[Snapshot], root: &str, conflict: FileConflictResolution,
    s3_client: &S3Client, ...) -> Result<...>

// After: options struct
pub struct DownloadOptions {
    pub conflict_resolution: FileConflictResolution,
    pub max_concurrent: usize,
    pub on_progress: Option<Box<dyn Fn(&DownloadStats) -> bool + Send>>,
}
impl Default for DownloadOptions { ... }
```

### 13.2 Error propagation (no silent swallowing)

```rust
// Before
conn.execute(...).unwrap_or_else(|e| { log::warn!("...{e}"); 0 });

// After
conn.execute(...).map_err(|e| AttachmentError::Cache(e.to_string()))?;
```

### 13.3 Typed enums over string matching

```rust
// Before
let val = get_setting("defaults.job_attachments_file_system");
if val == "COPIED" { ... }

// After: already parsed by CLI layer
pub enum AttachmentFileSystem { Copied, Virtual }
```

### 13.4 Parallelism

After Step 7, openjd-snapshots handles parallel hashing and upload via tokio.
Remaining sequential code in our orchestration layer is fine — it's I/O
coordination, not CPU work.

---

## Python Bindings Impact

After restructure, `deadline-python-bindings`:
- Calls `deadline_cloud::bundle::prepare_submission()` instead of passing callback soup
- Adapts openjd-snapshots' `ProgressFn` → PyO3 callback (thin adapter in bindings crate)
- No direct openjd-rs dependency needed — `deadline-cloud` re-exports relevant types
- The PyO3 layer becomes thinner because the library API is cleaner

---

## Baseline (2026-05-12)

| Crate | Tests | Status |
|-------|-------|--------|
| `deadline-job-attachments` | 343 | ✅ All pass |
| `deadline-cli` | 492 | ✅ All pass |

---

## Open Decisions

1. **Worker agent crate boundary:** Keep `deadline-cloud::attachments` as separate sub-crate or feature-gate? Decide after Step 7.
2. **openjd-model for parameter validation:** Use it or keep deferring to server-side validation? Low priority.
3. **openjd-snapshots contributions needed for Step 7:**
   - `upload_abs_manifest` — upload-only function for pre-hashed manifests
   - Fallback key support on `S3DataCache` — retry without algorithm suffix on 404
