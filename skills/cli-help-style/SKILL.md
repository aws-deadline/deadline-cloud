---
name: cli-help-style
description: Style guide for writing CLI help text (click docstrings) in the deadline CLI. Use when adding or editing help text for any `deadline` CLI command or subcommand.
tags: [skill, cli, help-text, docstring, click]
---

# CLI Help Style

## Overview
Guidelines for writing consistent, terminal-friendly help text in the `deadline` CLI's click command docstrings.

## Usage
Use this skill when:
- Adding a new CLI command or subcommand
- Editing existing CLI help text or option descriptions
- Reviewing a PR that modifies CLI docstrings

## Core Concepts

Click renders Python docstrings as terminal help text. The docstring is the **only** documentation most users will see, so it must be complete, scannable, and render cleanly in an 80-column terminal.

### Docstring Structure

Every command docstring **MUST** follow this order:

```
1. One-line summary (what the command does)
2. Additional context (optional, 1-3 sentences max)
3. Documentation URL (required for groups and most leaf commands)
```

Example:

```python
def my_command():
    """
    List the available Deadline Cloud farms or get details of a specific farm.

    \b
    Documentation: https://docs.aws.amazon.com/deadline-cloud/latest/userguide/farms.html
    """
```

### Group Commands

Group docstrings **MUST** hint at the available subcommands so users can discover capabilities without running `--help` on each one.

Good — surfaces what's available:
```python
"""
Monitor and manage Deadline Cloud jobs in a queue.

Use `deadline bundle submit` to create a job. Then use these commands
to check status, read logs, wait for completion, download output,
cancel, or requeue failed tasks.
"""
```

Bad — tells you nothing about what you can do:
```python
"""
Commands to work with Deadline Cloud jobs.
"""
```

### Documentation URLs

Every group command and most leaf commands **MUST** include a documentation URL as the last element of the docstring. Use `\b` before the URL line to prevent click from reflowing it:

```python
"""
Summary of the command.

\b
Documentation: https://docs.aws.amazon.com/deadline-cloud/...
"""
```

Leaf commands that are simple (e.g., `get`, `list`) **MAY** omit the URL if their parent group already has one pointing to the same page.

### No Markdown Reference Links

You **MUST NOT** use click's markdown-style reference links. They render as ugly raw URL blocks in the terminal:

Bad:
```python
"""
Commands to work with [Deadline Cloud jobs] in a [queue].

[Deadline Cloud jobs]: https://docs.aws.amazon.com/...
[queue]: https://docs.aws.amazon.com/...
"""
```

Good:
```python
"""
Commands to work with Deadline Cloud jobs in a queue.

\b
Documentation: https://docs.aws.amazon.com/...
"""
```

### Backtick Usage

Use backticks **only** for:
- CLI commands and subcommands: `` `deadline bundle submit` ``
- CLI options: `` `--farm-id` ``
- Protocol strings: `` `deadline://` ``
- Literal values: `` `READY` ``, `` `SUSPENDED` ``

Do **not** use backticks for product names or concepts:
- Write `Deadline Cloud` not `` `Deadline Cloud` ``
- Write `job attachments` not `` `job attachments` ``

### Formatting for Terminal Readability

**Lists:** Use `\b` before any list or block that must preserve its formatting. Without `\b`, click reflows text into a single paragraph:

```python
"""
Session auto-selection priority:

\b
  1. Ongoing sessions, preferring most recently started
  2. Most recently ended completed session
"""
```

**Examples:** Use `\b` before code examples and break long commands across lines:

```python
"""
\b
    aws deadline get-queue \\
      --profile $(deadline config get defaults.aws_profile_name) \\
      --farm-id $(deadline config get defaults.farm_id)
"""
```

### Conciseness

- Don't repeat information that's already in option help text. If `--limit` says "Maximum number of log lines to return", the docstring doesn't need to say "adjust with --limit".
- Don't explain what options do in the docstring body — that's what the option `help=` parameter is for.
- Prefer active voice: "Download the output" not "Downloads the output of".

### Stability Prefixes

Commands in pre-release stages **MUST** be prefixed:
- `BETA - ` for beta features
- `EXPERIMENTAL - ` for experimental features

These prefixes appear in both the command's own help and in the parent group's command listing.

### Option Help Text

- Start with a noun or verb, not "The" where possible
- End without a period
- For choices, briefly explain each value inline

## Quick Reference

| Element | Rule |
|---|---|
| Reference links `[text]: url` | Never use |
| Doc URL | `\b` + `Documentation: <url>` at end |
| Backticks | CLI commands, options, literals only |
| Product names | Plain text, no backticks |
| Lists in docstrings | Precede with `\b` |
| BETA/EXPERIMENTAL | Prefix at start of summary |
| Group descriptions | Mention key subcommand capabilities |
| Redundant info | Don't repeat option help text in docstring |
