# Common — Utilities, Exceptions & Data Structures

> Part of [AWS Deadline Cloud Client — Behavioral Test Specification](index.md)
>
> This document describes observable behavior through public interfaces.
> Error descriptions use category labels (e.g., "returns error") rather than
> language-specific exception types. The Rust implementation should produce
> equivalent observable outcomes, not necessarily identical internal mechanics.

---

## Section 36: Common — path_utils

> **Rust crate:** `deadline-common` · **Module:** `path_utils`
>
> **Logic under test:** Human-readable file size formatting (B/KB/MB/GB/TB/PB) and
> path summarization (grouping numbered files into sequences, grouping by directory).

### `human_readable_file_size(size_in_bytes) -> str`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | `0` bytes | Returns `"0 B"` | |
| 2 | Happy path | `999` bytes | Returns `"999 B"` | |
| 3 | Happy path | `1000` bytes | Returns `"1.0 KB"` | |
| 4 | Happy path | `999999` bytes | Returns `"1.0 MB"` (rounds up) | |
| 5 | Happy path | `1000000` bytes | Returns `"1.0 MB"` | |
| 6 | Happy path | `1500000000` bytes | Returns `"1.5 GB"` | |
| 7 | Boundary values | Very large value (petabytes) | Returns value in PB | |

### `summarize_paths_by_sequence`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 8 | Happy path | List of numbered files like `frame_001.png` through `frame_010.png` | Summarized as a single sequence entry with detected padding and range | |
| 9 | Happy path | Mix of numbered and non-numbered files | Numbered files grouped into sequences, others listed individually | |
| 10 | Happy path | File with zero-padded number like `frame_001.png` | Sequence summary reflects padding width (e.g., 3 digits) | |
| 11 | Happy path | File with variable-width number like `sequence_v907` | Sequence summary reflects the number and its padding range | |
| 12 | Boundary values | File with no number in its name like `"no_number.txt"` | Listed individually, not grouped into any sequence | |

### `summarize_paths_by_nested_directory`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 13 | Happy path | Files in nested directories | Grouped by common directory prefixes | |
| 14 | Boundary values | Single file | Returns single entry | |

---

## Section 51: Exceptions (client)

> **Rust crate:** `deadline-models` · **Module:** `errors`
>
> **Logic under test:** Error type hierarchy for the client library. These define the
> error contract: `DeadlineOperationError` is the base, with specialized subtypes for
> cancellation, timeout, create-job waiter cancellation, and user-initiated cancel.
> The Rust implementation should have equivalent error variants.

### Error hierarchy

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | `DeadlineOperationError("msg")` | String representation returns `"msg"` | Base error for CLI verbatim printing |
| 2 | Happy path | `DeadlineOperationCanceled()` | Default message is `"Operation canceled"` | |
| 3 | Happy path | `DeadlineOperationCanceled("custom")` | Message is `"custom"` | |
| 4 | Happy path | `DeadlineOperationTimedOut()` | Default message is `"Operation timed out"` | |
| 5 | Happy path | `CreateJobWaiterCanceled()` | Default message mentions "waiting for CreateJob" | |
| 6 | Happy path | `UserInitiatedCancel()` | Default message is `"Operation canceled by user"` | |
| 7 | Happy path | `DeadlineOperationCanceled` is a subtype of `DeadlineOperationError` | Type hierarchy check passes | |

---

## Section 52: SubmitterInfo data structure

> **Rust crate:** `deadline-models` · **Module:** `submitter_info`
>
> **Logic under test:** A simple data container for submitter environment metadata.
> Only `submitter_name` is required; all other fields are optional.

### `SubmitterInfo`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Create with only `submitter_name` | All optional fields are none | |
| 2 | Happy path | Create with all fields populated | All fields accessible | |
| 3 | Happy path | `additional_info` with nested dicts and lists | Stored as-is (YAML-safe types) | |
| 4 | Boundary values | `additional_info` is none | Attribute is none | |

---
