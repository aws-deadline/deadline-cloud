# attachments/diff Evaluation Report

**Date:** 2026-05-14
**Module:** `attachments/diff`
**Files reviewed:** `src/attachments/diff.rs`, `tests/attachments/diff.rs`

## Summary
Manifest comparison module (5KB) with two diff strategies: `fast_diff` (size+mtime) and `hash_diff` (delegates to openjd). Small, focused, correct. Grade: **A**.

## Findings

### Critical (must fix)
- None identified.

### Important (should fix)

1. **`fast_diff` uses `Vec::contains` for deleted file detection** (`diff.rs:~90`): `seen_relative.contains(&normalized)` is O(N) per manifest entry, making the deleted-file detection O(N×M) where N=manifest entries and M=current files. For large manifests (10K+ files), use a `HashSet<String>` instead.

### Minor (nice to have)

2. **Mtime tolerance of 1 microsecond is hardcoded**: Could be a parameter for callers that need different tolerance (e.g., FAT32 filesystems with 2-second resolution). Low priority since this matches Python behavior.

3. **`fast_diff` does filesystem I/O**: Same testability concern as upload — requires real files. Acceptable for this module's purpose.

## Test Coverage Assessment

| Function/Area | Happy path | Error cases | Edge cases |
|---|---|---|---|
| `fast_diff` | ✅ | ✅ | ✅ |
| `hash_diff` | ✅ | ❌ | ❌ |

Good test coverage for the size of the module.

## Recommended Changes

1. [S] Replace `Vec::contains` with `HashSet` for O(1) lookup in deleted-file detection
