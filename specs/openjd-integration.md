# openjd-rs Dependency Integration

**Work item:** #31
**Goal:** Replace duplicated code in `deadline-job-attachments` and
`deadline-job-bundle` with dependencies on published `openjd-rs` crates.
Eliminate ~4,000 lines of reimplemented logic. No backwards-compat
concerns — not shipping to customers yet.

**Source:** All openjd crates are published on crates.io at `0.1.0`:
- `openjd-snapshots` — hashing, manifests, caches, S3 upload/download
- `openjd-expr` — path mapping, format strings, expression evaluation
- `openjd-model` — template parsing, validation, job creation

Use path deps during development (`../openjd-rs/crates/...`), switch to
crates.io version for CI/release.

## Methodology: Strangler Fig

Standard red/green TDD doesn't fit a dependency swap. We use a
**strangler fig** approach:

```
1. BASELINE  — cargo test -p <crate>, record pass count
2. SHIM      — Add openjd dep. Thin adapter maps openjd types → existing
               public API. Old code still compiles, delegates internally.
3. VERIFY    — Run tests. All must pass. Failures → fix adapter, NOT tests.
               Tests define the contract.
4. DELETE    — Remove old implementation. Tests still pass (they test
               public API, not internals).
5. PRUNE     — Delete tests that now test openjd-rs internals (openjd-rs
               has its own 5,300+ test suite). Document what was pruned.
6. SIMPLIFY  — If adapter is trivial (just re-exports), collapse it.
               Expose openjd-rs types directly.
7. VERIFY    — Final cargo test -p <crate> + cargo test -p deadline-cli.
               CLI snapshot tests are the ultimate ground truth.
```

**Why not pure red/green:** We're not adding new behavior — we're
replacing internals. The existing tests ARE the spec. We run them
against the new implementation, then prune the ones that became
redundant (testing library internals we no longer own).

**Safety net:** Level 2 CLI snapshot tests (`cargo test -p deadline-cli`)
exercise the full stack end-to-end. If those pass after the swap, the
refactor is correct regardless of how many unit tests were pruned.

## Phase 1: Hashing + Caches + Manifest Codec

**Replaces:**

| Our code | openjd-rs equivalent |
|----------|---------------------|
| `asset_manifests.rs` — `HashAlgorithm`, `hash_data()`, `hash_file()` | `openjd_snapshots::hash::{hash_data, hash_file, HashAlgorithm}` |
| `asset_manifests.rs` — manifest v2023 encode/decode | `openjd_snapshots::codec::{encode_snapshot_v2023, decode_v2023}` |
| `caches.rs` — `HashCache` (SQLite hashesV4) | `openjd_snapshots::HashCache` |
| `caches.rs` — `S3CheckCache` (SQLite s3checkV1) | `openjd_snapshots::S3CheckCache` |

**Tests affected:** 31 (asset_manifests) + 18 (caches) + transitive users

**Steps:**
1. Add dep: `openjd-snapshots = { path = "../openjd-rs/crates/openjd-snapshots" }`
2. Create adapter: re-export openjd types behind existing public names
3. Verify all tests pass
4. Delete old hashing/cache implementation
5. Prune ~20 tests that test hash correctness or cache schema internals
   (openjd-rs already tests these exhaustively)
6. Verify CLI tests pass

**Risk:** Low. Hash cache schema is identical (`hashesV4`, same columns,
same `str(datetime.fromtimestamp(st_mtime))` format). Also upgrades
rusqlite 0.32 → 0.39 (known tech debt item).

## Phase 2: Diff

**Replaces:**

| Our code | openjd-rs equivalent |
|----------|---------------------|
| `diff.rs` — `fast_diff`, `hash_diff` | `openjd_snapshots::{diff_snapshots, entries_differ}` |

**Tests affected:** 8 (tests/suite/diff.rs)

**Steps:**
1. Map our diff functions to openjd-snapshots equivalents
2. Verify 8 tests pass
3. Delete `diff.rs`
4. Likely keep all 8 tests (they test our integration contract)

**Risk:** Low. Pure logic.

## Phase 3: Path Mapping

**Replaces:**

| Our code | openjd-rs equivalent |
|----------|---------------------|
| `path_mapping.rs` — `PathMappingRuleApplier` (trie, 500 lines) | `openjd_expr::{PathMappingRule, apply_rules}` |
| `models.rs::PathFormat` (Posix/Windows) | `openjd_expr::PathFormat` (Posix/Windows/Uri) |

**Tests affected:** 37 (path_mapping) + some in models.rs

**Steps:**
1. Add dep: `openjd-expr = "0.1"`
2. Replace `PathFormat` with re-export from openjd-expr
3. Replace `PathMappingRuleApplier` with adapter calling `apply_rules`
4. Verify 37 tests pass
5. Delete trie implementation
6. Prune ~15 tests that test trie prefix-matching internals

**Risk:** Medium.
- openjd-expr `PathFormat` serializes as `"POSIX"/"WINDOWS"` (uppercase)
  vs our `"posix"/"windows"` (lowercase). Need serde adapter or alias.
- openjd-expr has 3 variants (adds Uri). Boundaries that match on
  PathFormat need a `_ => unreachable!()` or handle Uri gracefully.
- openjd-expr pulls in `ruff_python_parser` as transitive dep (large).
  Acceptable since we'll likely use openjd-model later anyway.

## Phase 4: Upload/Download Engine

**Replaces:**

| Our code | openjd-rs equivalent |
|----------|---------------------|
| `upload.rs` — parallel hash + S3 upload (1000 lines) | `openjd_snapshots::hash_upload_abs_manifest` |
| `download.rs` — parallel S3 download (800 lines) | `openjd_snapshots::download_abs_manifest` |
| `s3.rs` — S3 client helpers (400 lines) | `impl AsyncDataCache for DeadlineS3Cache` |

**Tests affected:** 31 (upload) + 28 (download) + 22+13+19 (inline)

**Steps:**
1. Create `DeadlineS3Cache` implementing `AsyncDataCache` + `MultipartDataCache`
   that wraps our credential-scoped S3 client
2. Adapt `snapshot_assets` → `collect_abs_snapshot` + `hash_upload_abs_manifest`
3. Adapt download → `download_abs_manifest` with `DownloadOptions`
4. Verify all upload/download tests pass
5. Delete old engine code
6. Prune ~30 tests (multipart chunking, dedup, memory management)

**Risk:** High.
- Progress reporting: openjd-snapshots has `ProgressFn` callbacks but
  they may not map 1:1 to our `ProgressTracker`/`ProgressReportMetadata`
- Credential scoping: our S3 client uses queue-scoped credentials from
  `deadline-api`. Need to inject these into the `AsyncDataCache` impl.
- If the pipeline model doesn't fit, fallback: use openjd-snapshots for
  low-level ops (hash, put_object, get_object) but keep our orchestration.

## Phase 5 (optional): Template Validation via openjd-model

**Replaces:** `deadline-job-bundle/parameters.rs` validation (~500 lines)

**Decision:** Defer. The Deadline Cloud API does server-side validation
anyway. Client-side validation is additive — nice to have but not
required for parity. Revisit if we find bugs in manual validation.

## Dependency Configuration

```toml
# Development (path deps for fast iteration)
[workspace.dependencies]
openjd-snapshots = { path = "../openjd-rs/crates/openjd-snapshots" }
openjd-expr = { path = "../openjd-rs/crates/openjd-expr" }

# CI/Release (crates.io)
[workspace.dependencies]
openjd-snapshots = "0.1"
openjd-expr = "0.1"
```

## Execution Order

```
Phase 1 (hashing+caches) → Phase 2 (diff) → Phase 3 (path mapping)
         ↓ (manifest types must be in place first)
Phase 4 (upload/download)
```

Phases 1-3 are low-risk and can be done as one batch commit.
Phase 4 is a separate effort due to complexity.

## Test Classification After Integration

| Category | Action | Example |
|----------|--------|---------|
| **Contract tests** (test our public API behavior) | KEEP | "upload_assets creates manifest with correct hash" |
| **Integration tests** (test wiring between our code and openjd-rs) | KEEP | "S3 upload uses credential-scoped client" |
| **CLI snapshot tests** (Level 2) | KEEP (ground truth) | "bundle submit produces correct output" |
| **Unit tests of deleted code** (test internals we no longer own) | PRUNE | "hash_data returns 32-char hex", "trie matches longest prefix" |
| **Adapter tests** (test our type conversion layer) | ADD if non-trivial | "AssetManifest converts to openjd Snapshot correctly" |

## Expected Outcome

| Metric | Before | After |
|--------|--------|-------|
| Lines in `deadline-job-attachments/src/` | ~5,200 | ~1,800 |
| Unit tests in job-attachments | ~371 | ~250 |
| CLI tests (Level 2) | unchanged | unchanged |
| New deps | — | +openjd-snapshots, +openjd-expr |
| rusqlite version | 0.32 | 0.39 (via openjd-snapshots) |
| Parallel hashing | sequential | rayon (CPU-parallel) |
| Upload dedup | none | broadcast-channel dedup |
| Memory-bounded transfers | no | yes (MemoryPool) |
