---
name: eval-module
description: Evaluate a deadline-cloud-rs module for quality, idiomaticity, and correctness. Use when running `/eval-module MODULE_PATH` (e.g. `attachments/download`, `bundle/submission`, `api/session`).
tags: [rust, quality, evaluation, deadline-cloud]
---

# Eval Module

## Overview
Evaluate a `deadline-lib` module's specs, implementation, and tests for alignment, correctness, and Rust idiomaticity. Produces a report with prioritized improvements.

## Usage
Use this skill when:
- Evaluating quality of a specific module (e.g. `/eval-module attachments/download`)
- Preparing Step 12 (idiomatic patterns) work items for a module
- Auditing a module after a major refactor
- Reviewing whether a module meets the team's quality bar

## Core Concepts

### Repository Layout

```
deadline-cloud-rs/
├── specs/                          # Design docs and per-crate specs
│   ├── architecture.md             # Crate dependency graph
│   ├── testing.md                  # Test philosophy
│   ├── patterns.md                 # Coding conventions
│   └── deadline-cli/               # Per-command CLI specs
├── crates/
│   ├── deadline-lib/src/           # Library source
│   │   ├── attachments/            # Job attachments (upload, download, diff, etc.)
│   │   ├── api/                    # AWS SDK client, auth, session, telemetry
│   │   ├── bundle/                 # Submission orchestration, hooks, loader
│   │   └── config/                 # INI config read/write
│   ├── deadline-lib/tests/         # Integration tests
│   ├── deadline-cli/src/           # CLI binary
│   └── deadline-python-bindings/   # PyO3 layer
└── audit_reports/                  # Audit outputs (archive/ for resolved)
```

### Three Artifacts

Every module has three artifacts to review together:

1. **Specs** — `specs/` docs describing the module's goals and design
2. **Implementation** — Source code in `crates/deadline-lib/src/{module}/`
3. **Tests** — Unit tests (inline) + integration tests in `crates/deadline-lib/tests/`

### Evaluation Criteria

Review against these 7 criteria, in priority order:

#### 1. Correctness
- Does the code do what it claims?
- Are there logic errors, off-by-ones, race conditions?
- Do error paths handle all cases?

#### 2. Error Handling
- Are errors propagated (not swallowed with `unwrap_or_else(log::warn)`)?
- Are error messages actionable for the end user?
- Is the error type appropriate (`JobAttachmentsError` variant)?
- Are `expect()` messages descriptive?

#### 3. Public API Ergonomics
- Are function signatures clean? (≤5 params, or use options struct)
- Are types appropriate? (`&Path` not `&str` for filesystem paths)
- Is the API hard to misuse? (builder pattern, type states where appropriate)
- Are defaults sensible?
- Typed enums instead of string comparisons for known value sets?

#### 4. Naming & Consistency
- Do names match Rust conventions? (`snake_case` fns, `CamelCase` types)
- Are names consistent with the rest of the codebase?
- Do comments explain *why*, not *what*?
- Is there dead code or commented-out code?

#### 5. Performance
- No O(N²) where O(N) is possible?
- No unnecessary allocations in loops?
- No blocking I/O in async contexts?
- Appropriate use of iterators vs collecting into Vec?
- Sequential loops over CPU-bound work where rayon/parallelism fits?

#### 6. Test Coverage
- Happy path covered?
- Error/edge cases covered?
- Test names follow `{function}_{scenario}_{outcome}`?
- No tests of dependency internals (openjd-snapshots behavior)?
- Are tests testing *our* logic, not just wiring?

#### 7. Spec Alignment
- Does `specs/` accurately describe this module?
- Are there undocumented behaviors?
- Are there spec claims the code doesn't implement?

### Evaluation Procedure

1. **Identify scope**: Map module path to source file(s), test file(s), and relevant spec(s)
2. **Read specs** relevant to this module
3. **Read implementation** — note issues against each criterion
4. **Read tests** — assess coverage gaps
5. **Run tests**: `cargo test -p deadline-lib -- {module_name}` — confirm green
6. **Exploratory**: Actively look for bugs, edge cases, misuse potential
7. **Write report** to `audit_reports/{module}-eval-{date}.md`

### Report Structure

```markdown
# {Module} Evaluation Report

**Date:** YYYY-MM-DD
**Module:** `{path}`
**Files reviewed:** [list]

## Summary
One paragraph overall assessment. Grade: A/B/C/D.

## Findings

### Critical (must fix)
- [Issue]: [location] — [why it matters]

### Important (should fix)
- [Issue]: [location] — [why it matters]

### Minor (nice to have)
- [Issue]: [location] — [why it matters]

## Test Coverage Assessment
| Function/Area | Happy path | Error cases | Edge cases |
|---|---|---|---|
| ... | ✅/❌ | ✅/❌ | ✅/❌ |

## Recommended Changes
Prioritized list of concrete changes, estimated effort (S/M/L).

1. [S] ...
2. [M] ...
3. [L] ...
```

### Grading

| Grade | Meaning |
|-------|---------|
| A | Production-ready, idiomatic, well-tested |
| B | Correct and functional, minor idiom issues |
| C | Works but has significant style/ergonomic issues |
| D | Has correctness concerns or major gaps |

## Quick Reference

| Module path | Source | Tests | Relevant specs |
|---|---|---|---|
| `attachments/download` | `src/attachments/download.rs` | `tests/attachments/download.rs` | `specs/crate-restructure.md` |
| `attachments/upload` | `src/attachments/upload.rs` | `tests/attachments/upload_s3.rs` | `specs/crate-restructure.md` |
| `attachments/diff` | `src/attachments/diff.rs` | `tests/attachments/diff.rs` | — |
| `attachments/manifest_ops` | `src/attachments/manifest_ops.rs` | `tests/attachments/manifest_ops.rs` | — |
| `attachments/incremental_download` | `src/attachments/incremental_download.rs` | (inline) | — |
| `bundle/submission` | `src/bundle/submission.rs` | `tests/bundle/submission.rs` | `specs/deadline-cli/bundle-submit.md` |
| `api/session` | `src/api/session.rs` | (inline) | — |
| `config` | `src/config/` | (inline) | — |

## Common Mistakes

### Reviewing openjd internals
**Problem:** Flagging issues in openjd-snapshots behavior.
**Fix:** Only evaluate *our* code. If openjd behavior is wrong, file it separately.

### Suggesting rewrites without justification
**Problem:** "This should use a trait" without explaining the benefit.
**Fix:** Every recommendation must state the concrete problem it solves.

### Ignoring the Python parity constraint
**Problem:** Suggesting API changes that would break wire-format compatibility.
**Fix:** Check if the behavior matches the Python client. Divergences need explicit justification.
