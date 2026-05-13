# Crate Restructure Plan

**Work item:** #31 (expanded scope)
**Supersedes:** `specs/openjd-integration.md` (absorbed as Phase 1)

## Context

The deadline-cloud-rs codebase was ported from Python with a "behavioral
fidelity first" philosophy. The result is correct (492 CLI tests pass) but
carries Python's design patterns into Rust: sequential loops where rayon
fits, stringly-typed APIs, monolithic functions mixing business logic with
presentation, and ~5,500 lines of reimplemented logic that openjd-rs
already provides as a well-designed library.

This plan addresses three problems in order:
1. **Duplicated code** — replace with openjd-rs
2. **Blurred CLI/library boundary** — separate presentation from logic
3. **Non-idiomatic patterns** — adopt Rust conventions

## Constraints

- **Worker agent dependency:** The worker agent (`deadline-cloud-worker-agent`)
  imports attachment models, download/upload orchestration, and progress types.
  These must remain independently consumable without pulling in auth/session/CLI.
- **Python bindings:** `deadline-python-bindings` calls into submission and
  resource-listing functions. After restructure it adapts to the new API.
- **Test contract:** 492 CLI snapshot tests are ground truth. All must pass
  after each phase.
- **Python parity:** On-disk/on-wire formats must match the production Python
  package (`aws-deadline/deadline-cloud-job-attachments`). Verify before swapping.

---

## Target Architecture

```
deadline-cli (binary)
├── Clap arg parsing, output formatting, progress bars, user prompts
├── Resolves config, defaults, policy decisions
└── depends on: deadline-cloud, openjd-snapshots (for types)

deadline-cloud (library crate, ~16K lines → ~12K after Phase 4)
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

## Phase 1: Delete (openjd-rs Integration)

Replace duplicated low-level code with openjd-rs crate dependencies.
Uses the strangler-fig methodology (see `.kiro/prompts/dependency-swap-step*.md`).

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

### Phase 1a: Hashing (zero risk)

| Our code | Replacement | Notes |
|----------|-------------|-------|
| `asset_manifests.rs::HashAlgorithm` | `openjd_snapshots::hash::HashAlgorithm` | Drop our `_alg` param (only xxh128 exists) |
| `asset_manifests.rs::hash_data()` | `openjd_snapshots::hash::hash_data()` | Signature: `&[u8] → String` (no alg param) |
| `asset_manifests.rs::hash_file()` | `openjd_snapshots::hash::hash_file()` | Returns `io::Result<String>`, wrap to `JobAttachmentsError` |

### Phase 1b: Manifest Codec (low risk)

| Our code | Replacement | Notes |
|----------|-------------|-------|
| `AssetManifest::encode()` | `openjd_snapshots::encode_snapshot_v2023()` | Takes `&Snapshot`, need `AssetManifest → Snapshot` conversion |
| `decode_manifest()` | `openjd_snapshots::decode_v2023()` | Returns `Snapshot`, need `Snapshot → AssetManifest` conversion |

Both produce identical output (canonical JSON + `\uXXXX` escaping). Verified against Python.

### Phase 1c: S3CheckCache (low risk)

| Our code | Replacement | Notes |
|----------|-------------|-------|
| `S3CheckCache` | `openjd_snapshots::S3CheckCache` | Same float-timestamp format. API adapter: `get_entry(&str) → Option<String>` |

### Phase 1d: HashCache (deferred)

**Blocked.** openjd-rs stores mtime as raw u64; Python (and our code) stores
`str(datetime.fromtimestamp(st_mtime))`. Incompatible on-disk format.

Options:
- Fix openjd-rs to use Python-compatible format
- Accept one-time cache invalidation
- Keep our implementation

Decision deferred until Phase 4 (upload/download engine swap) forces it.

### Phase 2: Diff (low risk)

| Our code | Replacement |
|----------|-------------|
| `diff.rs::fast_diff` | `openjd_snapshots::diff_snapshots` |
| `diff.rs::hash_diff` | `openjd_snapshots::entries_differ` |

8 tests. Likely keep all (they test our integration contract).

### Phase 3: Path Mapping (medium risk)

| Our code | Replacement | Notes |
|----------|-------------|-------|
| `path_mapping.rs::PathMappingRuleApplier` (trie, 500 lines) | `openjd_expr::apply_rules()` | |
| `models.rs::PathFormat` | `openjd_expr::PathFormat` | Serializes as `"POSIX"/"WINDOWS"` (uppercase) vs our `"posix"/"windows"`. Need serde adapter. |

37 tests. Prune ~15 that test trie internals.

### Phase 4: Upload/Download Engine (high risk, high reward)

| Our code | Replacement |
|----------|-------------|
| `upload.rs` (2,000 lines) | `openjd_snapshots::hash_upload_abs_manifest` |
| `download.rs` (1,600 lines) | `openjd_snapshots::download_abs_manifest` |
| `s3.rs` (400 lines) | `impl AsyncDataCache for DeadlineS3Cache` |

**What stays:** ~300 lines of Deadline-specific orchestration (credential scoping,
prepare_paths_for_upload, manifest S3 key construction, conflict resolution policy).

**Key adapter:**
```rust
/// Bridges openjd-snapshots' storage trait with Deadline's credential-scoped S3 client.
pub struct DeadlineS3Cache {
    client: aws_sdk_s3::Client,
    bucket: String,
    cas_prefix: String,
}

#[async_trait]
impl AsyncDataCache for DeadlineS3Cache {
    async fn get_object(&self, hash: &str, alg: &str) -> io::Result<Vec<u8>> { ... }
    async fn put_object(&self, hash: &str, alg: &str, data: Vec<u8>) -> io::Result<()> { ... }
    async fn has_object(&self, hash: &str, alg: &str) -> io::Result<bool> { ... }
}
```

### Expected Outcome

| Metric | Before | After Phase 4 |
|--------|--------|---------------|
| Lines in `deadline-job-attachments/src/` | 9,988 | ~3,500 |
| Unit tests in job-attachments | 375 | ~250 |
| CLI tests | 492 | 492 (unchanged) |
| New deps | — | +openjd-snapshots, +openjd-expr |

---

## Phase 2: Separate (CLI vs Library Boundary)

### The Principle

**Library:** takes typed inputs, returns typed outputs. Never touches
stdout/stderr, never reads config files, never resolves default paths.

**CLI:** parses args, reads config, resolves defaults, drives user interaction,
formats output.

### Violations to Fix

#### 2.1 Progress reporting in library functions

**Before:**
```rust
// Library function knows about presentation
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
// Library: pure computation, progress via openjd-snapshots' ProgressFn
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

#### 2.2 User interaction in submission

**Before:**
```rust
pub struct SubmitJobParams<'a> {
    // Data fields...
    pub auto_accept: bool,
    pub print_callback: Box<dyn Fn(&str) + Send + 'a>,
    pub interactive_confirmation_callback: Option<ConfirmFn>,
}
```

**After:**
```rust
// Library: data in, steps out
pub struct SubmitJobRequest {
    pub job_bundle_dir: PathBuf,
    pub parameters: Vec<Value>,
    pub name: Option<String>,
    // ... all data fields, no callbacks
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

CLI drives the state machine:
```rust
match prepare_submission(request)? {
    SubmitAction::NeedsConfirmation { message, default } => {
        if !prompt_user(&message, default) { return Ok(()); }
        // re-prepare with confirmation flag
    }
    SubmitAction::Ready(job) => {
        let job_id = job.submit(&client).await?;
        println!("job-{job_id}");
    }
}
```

#### 2.3 Config resolution in library

**Before:**
```rust
// Library reads config and resolves defaults
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
// Library takes resolved values
pub struct S3TransferConfig {
    pub max_pool_connections: usize,  // default: 50
    pub small_file_threshold_multiplier: usize,  // default: 20
}

impl Default for S3TransferConfig { ... }
```

CLI resolves from config:
```rust
let s3_config = S3TransferConfig {
    max_pool_connections: config.get("settings.s3_max_pool_connections")
        .and_then(|v| v.parse().ok())
        .unwrap_or(50),
    ..Default::default()
};
```

#### 2.4 Path types

**Before:** `cache_dir: &str`, `file_path: String`
**After:** `cache_dir: &Path`, `file_path: PathBuf`

#### 2.5 Presentation utilities

Move to CLI crate:
- `human_readable_file_size()`
- `ProgressReportMetadata.progress_message` (formatted string)
- Any `format!` that produces user-facing text

---

## Phase 3: Merge (Crate Consolidation)

After Phases 1-2, merge the library crates into `deadline-cloud`:

```
deadline-config           → deadline-cloud::config
deadline-api              → deadline-cloud::api
deadline-job-bundle       → deadline-cloud::bundle
deadline-job-attachments  → deadline-cloud::attachments
```

**Why merge:**
- `deadline-api` is only used by CLI and python-bindings (worker agent doesn't need it)
- After Phase 1, `deadline-job-attachments` is ~3,500 lines — too small to justify a separate crate
- Single crate = simpler dependency graph, easier refactoring across modules
- Estimated size: ~16K lines (reasonable for one crate)

**Why not merge yet:**
- Wait until Phase 1-2 stabilize the API surface
- Merging before deletion means moving code that's about to be deleted

**Worker agent consideration:** If the worker agent needs attachment
models without auth/API, expose `deadline-cloud::attachments::models` as
a feature-gated module, or keep `deadline-cloud::attachments` as a
re-export of a thin sub-crate. Decide after Phase 4 when final shape is clear.

---

## Phase 4: Redesign (Idiomatic Patterns)

Applied to remaining code after Phases 1-3.

### 4.1 Options structs with Default

```rust
// Before: positional params, hard to extend
pub fn download_files(manifests: &[AssetManifest], root: &str, conflict: FileConflictResolution,
    s3_client: &S3Client, ...) -> Result<...>

// After: options struct
pub struct DownloadOptions {
    pub conflict_resolution: FileConflictResolution,
    pub max_concurrent: usize,
    pub on_progress: Option<Box<dyn Fn(&DownloadStats) -> bool + Send>>,
}
impl Default for DownloadOptions { ... }
```

### 4.2 Error propagation (no silent swallowing)

```rust
// Before
conn.execute(...).unwrap_or_else(|e| { log::warn!("...{e}"); 0 });

// After
conn.execute(...).map_err(|e| AttachmentError::Cache(e.to_string()))?;
```

### 4.3 Typed enums over string matching

```rust
// Before
let val = get_setting("defaults.job_attachments_file_system");
if val == "COPIED" { ... }

// After: already parsed by CLI layer
pub enum AttachmentFileSystem { Copied, Virtual }
```

### 4.4 Parallelism

After Phase 1, openjd-snapshots handles parallel hashing and upload via rayon.
Remaining sequential code in our orchestration layer is fine — it's I/O
coordination, not CPU work.

---

## Execution Order

```
Phase 1a-1c (hashing + codec + S3CheckCache)  ← START HERE
    ↓
Phase 1 (diff) + Phase 1 (path mapping)
    ↓
Phase 2 (CLI/library boundary)  ← can start in parallel with Phase 1 path mapping
    ↓
Phase 1 Phase 4 (upload/download engine)  ← forces clean boundary naturally
    ↓
Phase 3 (crate merge)  ← after API surface is stable
    ↓
Phase 4 (idiomatic patterns)  ← ongoing, interleaved
```

**Dependency-swap prompts** (`.kiro/prompts/dependency-swap-{plan,execute}.md`) are used
for each Phase 1 sub-phase. Direct swap by default; strangler fig for complex cases.

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
| `deadline-job-attachments` | 364 | ✅ All pass |
| `deadline-cli` | 492 | ✅ All pass |

Phase 1a (hashing): complete. Phase 1b (manifest codec): complete.
Phase 1 scope: 18 (caches) + 8 (diff) + 37 (path_mapping) = 63 tests remaining.

---

## Open Decisions

1. **HashCache mtime format:** Fix openjd-rs or accept cache invalidation? Deferred to Phase 4.
2. **Worker agent crate boundary:** Keep `deadline-cloud::attachments` as separate sub-crate or feature-gate? Decide after Phase 4.
3. **openjd-model for parameter validation:** Use it or keep deferring to server-side validation? Low priority.
4. **Vestigial wrapper sweep (Phase 4):** As openjd-rs replaces internals, many functions collapse into trivial one-liner delegates (e.g., `hash_data`, `hash_file`, and soon `encode`/`decode_manifest`). During Phase 4 (idiomatic patterns), audit all remaining thin wrappers and either inline them at call sites or re-export the openjd function directly. Don't carry dead indirection into the merged crate.
