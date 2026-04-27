# Audit Methodology

How to conduct a behavioral parity audit of the Rust CLI against the
Python CLI. Follow this document step by step.
## Audit tiers

Not all code can be tested the same way. Use the appropriate tier:

| Tier | When to use | Method |
|------|-------------|--------|
| **1: Live CLI comparison** | Any implemented CLI command | Run both CLIs, diff output |
| **2: Code review** | Library code not reachable from CLI | Read Rust + Python source, verify behavioral contract |
| **3: Schema comparison** | Data structures (caches, manifests, config) | Compare schemas, field types, compatibility |

Tier 1 is mandatory for all CLI commands. Tier 2 and 3 supplement it
for code that CLI tests can't reach.
## Report file format

Each audit report follows this structure:

```markdown
# Audit: <scope description>

**Date:** YYYY-MM-DD
**Scope:** <what's being audited>
**Status:** In progress | Complete

## Summary

| Priority | Count | Fixed | Remaining |
|----------|-------|-------|-----------|
| Critical | 0     | 0     | 0         |
| High     | 0     | 0     | 0         |
| Medium   | 0     | 0     | 0         |
| Low      | 0     | 0     | 0         |

## Findings

### AUDIT-001: <short title>

- **Category:** Bug | Behavioral gap | Extra Rust behavior | Nice-to-have
- **Priority:** Critical | High | Medium | Low
- **Command/Function:** `deadline <command>` or `module::function()`
- **Python behavior:** <what Python does, with exact output>
- **Rust behavior:** <what Rust does, with exact output>
- **Impact:** <who/what is affected, why it matters>
- **Resolution:** Pending | Fixed (commit) | Accepted difference | Deferred

## Ranked Summary

<Table of all findings sorted by priority, highest first>
```
## After the audit

1. Update the report's Summary table with final counts
2. Update HANDOFF.md to point to the report and list fixes needed
3. Fix issues in priority order using TDD (failing test → fix → verify)
4. Update each finding's Resolution field as fixes are applied
5. Commit fixes as a batch per the workflow rules
