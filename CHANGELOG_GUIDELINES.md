# Changelog Guidelines

This document provides formal guidance on structuring and writing changelog entries for the AWS Deadline Cloud client repository.

## Table of Contents

* [Changelog Structure](#changelog-structure)
* [Writing Guidelines](#writing-guidelines)
* [Breaking Changes](#breaking-changes)
* [Deprecations](#deprecations)
* [Features](#features)
* [Bug Fixes](#bug-fixes)
* [Performance Improvements](#performance-improvements)
* [Experimental](#experimental)
* [Fixes to Unreleased Changes](#fixes-to-unreleased-changes)
* [Examples](#examples)
* [Review Checklist](#review-checklist)

## Changelog Structure

Each release in the changelog MUST follow this standardized section order:

1. **BREAKING CHANGES** (if applicable)
2. **DEPRECATIONS** (if applicable)
3. **Features**
4. **Bug Fixes**
5. **Performance Improvements** (if applicable)
6. **Experimental** (if applicable)

### Section Descriptions

**BREAKING CHANGES**: Changes that break backward compatibility and require user action.

**DEPRECATIONS**: Features or APIs that will be removed in a future release. Users should be warned but functionality still works.

**Features**: New functionality or enhancements to existing features.

**Bug Fixes**: Corrections to defects in existing functionality.

**Performance Improvements**: Changes that improve performance without altering functionality.

**Experimental**: Features behind feature flags or marked as subject to change.

## Writing Guidelines

### General Principles

1. **User-focused language**: Write from the customer's perspective, not the implementation perspective
2. **Action-oriented**: Describe what changed and the impact, not how it was implemented
3. **Concise but complete**: Provide enough context to understand the change without being verbose
4. **Present tense**: Use present tense for describing the state after the change

## Breaking Changes

Breaking changes require special attention and MUST include:

1. **Clear description of what broke**
2. **Migration path or workaround**
3. **Code examples when applicable**

### Format

```markdown
### BREAKING CHANGES

* [Brief description of the change] (#PR) ([commit])
  * [Detailed explanation of what broke]
  * [Migration path or how to adapt]
  * [Code example if applicable]
```

### Example

```markdown
### BREAKING CHANGES

* `deadline.ui.show_job_bundle_submitter`: Input parameter renamed from `submitter_name` to `submitter_info` and now expects a `deadline.dataclasses.SubmitterInfo` object as input. (#940) ([`74a3b01`])
  * The function signature has changed to accept a structured object instead of a string
  * **Migration**: Replace `show_job_bundle_submitter(submitter_name="MyName")` with `show_job_bundle_submitter(submitter_info=SubmitterInfo(submitter_name="MyName"))`
```

## Deprecations

Deprecations MUST include:

1. **What is being deprecated**
2. **What to use instead**
3. **When it will be removed** (if known)

### Format

```markdown
## DEPRECATIONS

* [What is deprecated] has been deprecated. [What to use instead] should now be used. [When removal will occur if known]
```

### Example

```markdown
## DEPRECATIONS

* The CLI `bundle gui-submit --submitter-name` option has been deprecated. `--submitter-info` should now be used to provide the name. This option will be removed in version 1.0.0.
* `--timezone` is being deprecated in favor of `--timestamp-format` for the `job logs` command. `--timezone` will be removed in a future release.
```

## Fixes to Unreleased Changes

Fixes that only impact unreleased changes (changes not yet in a published release) should generally **NOT** be included in the changelog as separate entries.

### Guidelines

Fixes to unreleased changes should generally be omitted from the changelog. Instead, describe features in their final working state without mentioning intermediate bugs or fixes. The changelog is for released functionality, not development history.

### Example

Consider two commits that were merged for the same feature:

```
feat: add job retry functionality
fix: job retry fails when retry count exceeds 5
```

Let's assume that the  `fix:` commit fixes a bug in the `feat:` commit, but the `feat:` commit had not been released before the `fix:` commit was merged.

When drafting the changelog for the release, these two changes would be merged into a single chaneglog entry:

**GOOD:**

```markdown
### Features
* Add job retry functionality with configurable retry limits
```

**BAD:**

```markdown
### Features
* Add job retry functionality
### Bug Fixes
* Fix job retry failing when retry count exceeds 5
```

## Features

Features should describe **what the user can now do** and when it's useful.

**Good examples:**
```markdown
* Add `deadline job wait` command to monitor job completion with configurable polling intervals
* Support automatic download of job attachments output
* Add detailed tooltips to grayed-out submit button
```

**Poor examples:**
```markdown
* Added new function to API
* Implemented job waiting
* Updated UI
```

## Bug Fixes

Bug fixes should describe **the problem that was fixed** as the customer experienced it, not the technical implementation.

**Good examples:**
```markdown
* Job submission error when submitting same jobs with the same title over 100 times in a single day.
* Re-queued jobs downloading the same output more than once with cli
* Process hangs on exit with high volume of telemetry
```

**Poor examples:**
```markdown
* Fixed a bug in the submission code
* Updated the download logic
* Changed the telemetry handler
```

## Performance Improvements

Performance improvements:
1. **SHOULD quantify the improvement** when possible (e.g., "2x faster", "50% reduction")
2. **MUST specify when it applies** (e.g., "for large file uploads", "during job submission")

**Good examples:**
```markdown
* Improve concurrency during bundle submission by threading local s3 cache db connections and enabling WAL mode by default (40% faster for bundles with 1000+ files)
* Speed up job bundle submissions by reducing redundant stat calls (2x faster for bundles with deep directory structures)
* Reduce memory usage during large file uploads
```

**Poor examples:**
```markdown
* Made uploads faster
* Improved performance
* Optimized code
```

## Experimental

Use a dedicated **Experimental** section at the end of the changelog for features that:
- Are behind feature flags
- Have public APIs and functional behavior under development and subject to change without following normal breaking change policies

Experimental features must be grouped under parent bullets by feature name, with specific changes as sub-bullets. When a feature group requires a feature flag, document the flag name in the parent bullet point.


### Format

```markdown
### Experimental

These changes are experimental and are subject to change.

* [Feature name] (requires `FEATURE_FLAG_NAME=true`):
  * [Specific change description]
  * [Another change for same feature]
* [Another feature name]:
  * [Change description]
```

### Example

```markdown
### Experimental

These changes are experimental and are subject to change.

* MCP Server:
  * Add get_session_logs to mcp server (#909) ([`c9f83a4`])
* Incremental/Automatic Downloads (requires `ENABLE_INCREMENTAL_DOWNLOAD=true`):
  * Add storage profile support for incremental download (#773) ([`d7fd976`])
  * Add internal functions to support path mapping (#764) ([`5a28a64`])
```

### Graduating from Experimental

When a feature graduates from experimental to stable:

1. Move it to the appropriate section (Features, etc.)
2. Note that it's now stable
3. Document any API changes made during the experimental phase

**Example:**
```markdown
### Features

* Incremental job output downloads are now stable and enabled by default (previously experimental)
  * API changes from experimental version: `download_incremental()` renamed to `download_outputs()`
```

## Examples

### Complete Release Example

```markdown
## 0.55.0 (2025-01-15)

### BREAKING CHANGES

* Remove deprecated `create_job_response` attribute from `ui.dialogs.SubmitJobToDeadlineDialog` (#791) ([`6587e4e`])
  * This attribute was deprecated in 0.51.1 and always returned `None`
  * **Migration**: Use the `job_id` attribute instead, which is set when job submission succeeds

## DEPRECATIONS

* The `--legacy-format` option has been deprecated. Use `--format=legacy` instead. This option will be removed in version 1.0.0.

### Features

* Add `deadline job wait` command to monitor job completion with configurable polling and timeout
* Support automatic download of job attachments output for completed jobs
* Add detailed tooltips to grayed-out submit button explaining why submission is disabled

### Bug Fixes

* Job submission error when submitting same jobs with the same title over 100 times in a single day
* Process hangs on exit with high volume of telemetry
* HashDB does not retry when failing to open

### Performance Improvements

* Improve concurrency during bundle submission by threading local s3 cache db connections (40% faster for bundles with 1000+ files) (#896) ([`ba15300`])
* Speed up job bundle submissions by reducing redundant stat calls (2x faster for bundles with deep directory structures) (#860) ([`6e6e3ff`])

### Experimental

These changes are experimental and are subject to change.

* Incremental Downloads:
  * Add storage profile support for incremental download (requires `ENABLE_INCREMENTAL_DOWNLOAD=true`) (#773) ([`d7fd976`])
```

## Review Checklist

Before finalizing a changelog, verify:

- [ ] Sections are in the correct order
- [ ] Breaking changes describe what broke and include migration paths
- [ ] Deprecations specify what to use instead
- [ ] Bug fixes describe the problem that was fixed from the user perspective
- [ ] Performance improvements specify when they apply and ideally quantify improvements
- [ ] Experimental features are grouped by feature name with feature flags documented
- [ ] All entries are user-focused, not implementation-focused
- [ ] Fixes to unreleased changes are omitted or merged with original features
