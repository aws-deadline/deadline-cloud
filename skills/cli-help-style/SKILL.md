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

### Dual rendering: terminal and docs site

These docstrings are rendered in two places:

1. **Terminal** — `deadline <command> --help` via click's built-in formatter
2. **Docs site** — https://aws-deadline.github.io/deadline-cloud/ via mkdocs-click, which passes docstring text through MkDocs as markdown

Click's terminal formatter treats docstrings as plain text with paragraph reflowing. It does not understand markdown — backticks, links, and lists all render literally. MkDocs, on the other hand, renders the same text as markdown, so backticks become `<code>` tags and links become clickable.

This dual rendering drives most of the style rules below. The goal is text that looks good in **both** contexts, prioritizing the terminal since that's where most users encounter it.

The `ContextTrackingCommand` and `ContextTrackingGroup` classes in `_main.py` override `format_help_text` to strip markdown at display time using `_markdown_strip.py`. This means you can write markdown in docstrings and get clean output in both contexts.

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

### Markdown Links

Both inline and reference-style markdown links are supported. The custom `format_help_text` override in `_main.py` strips markdown for terminal display while preserving it for the MkDocs docs site.

**Inline links** — best for "learn more" lines at the end of docstrings:

```python
"""
\b
Learn more about [job bundles](https://docs.aws.amazon.com/...).
"""
```

Terminal: `Learn more about job bundles (https://docs.aws.amazon.com/...)`
Docs site: Learn more about [job bundles](https://docs.aws.amazon.com/...)

**Reference links** — best for linking terms in body text without cluttering the sentence with URLs:

```python
"""
Commands to work with [Deadline Cloud jobs] in a [queue].

[Deadline Cloud jobs]: https://docs.aws.amazon.com/.../deadline-cloud-jobs.html
[queue]: https://docs.aws.amazon.com/.../queues.html
"""
```

Terminal: `Commands to work with Deadline Cloud jobs in a queue.`
Docs site: Terms become clickable links

### Backtick Usage

In the terminal, backticks render as literal `` ` `` characters. For CLI commands like `` `deadline bundle submit` ``, this is a useful visual delimiter that helps the command stand out — a convention terminal users expect. But wrapping product names like `` `Deadline Cloud` `` just looks like a formatting mistake with stray backtick characters.

On the docs site, backticks render as `<code>` tags (monospace), so they work well for anything that's actually code.

Use backticks **only** for:
- CLI commands and subcommands: `` `deadline bundle submit` ``
- CLI options: `` `--farm-id` ``
- Protocol strings: `` `deadline://` ``
- Literal values: `` `READY` ``, `` `SUSPENDED` ``

Do **not** use backticks for product names or concepts:
- Write `Deadline Cloud` not `` `Deadline Cloud` ``
- Write `job attachments` not `` `job attachments` ``

### Formatting for Terminal Readability

**The `\b` escape:** Click's terminal formatter reflows all text into wrapped paragraphs by default. Without `\b`, a numbered list like:

```
1. Ongoing sessions
2. Most recently ended session
```

gets smashed into a single line: `1. Ongoing sessions 2. Most recently ended session`. The `\b` on a line by itself tells click "stop reflowing, preserve formatting from here until the next blank line."

On the docs site, `\b` is stripped by mkdocs-click's `remove_ascii_art` option, so it doesn't affect markdown rendering.

**Lists:** Use `\b` before any list or block that must preserve its formatting:

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
| Inline links `[text](url)` | Use for "learn more" lines at end of docstring |
| Reference links `[text]: url` | Use for linking terms in body text |
| Doc URL | `\b` + `Learn more about [topic](url)` at end |
| Backticks | CLI commands, options, literals only |
| Product names | Plain text, no backticks |
| Lists in docstrings | Precede with `\b` |
| BETA/EXPERIMENTAL | Prefix at start of summary |
| Group descriptions | Mention key subcommand capabilities |
| Redundant info | Don't repeat option help text in docstring |
