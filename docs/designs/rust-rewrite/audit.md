# Audit Methodology

How to conduct a behavioral parity audit of the Rust CLI against the
Python CLI. Follow this document step by step.

---

## When to audit

- After completing a batch of new feature work (workflow Step 1)
- Before picking up new feature work when HANDOFF.md says to audit
- After a major refactor that touches output formatting or error handling

## Setup

1. Ensure the Rust CLI is built: `cargo build -p deadline-cli`
2. Confirm the Python CLI is available at the path in HANDOFF.md
3. Confirm AWS credentials are active (run `deadline auth status`)
4. Create a new report file: `docs/designs/rust-rewrite/audit_reports/YYYY-MM-DD-<scope>.md`

## The audit loop

For each command or function in scope, execute these steps in order:

### Step 1: Capture Python reference output

Run the Python CLI command and capture its exact output. This is the
unbiased target — do this BEFORE reading the Rust code.

```bash
# Happy path
deadline <command> [args] 2>&1

# Error paths (test each)
deadline <command> --missing-required-arg 2>&1; echo "EXIT: $?"
deadline <command> --invalid-resource-id 2>&1; echo "EXIT: $?"
```

For each command, test these error categories:
- Missing required arguments
- Invalid/non-existent resource IDs
- No credentials or expired credentials (if safe to test)
- API failure responses

Use `--yes` to bypass interactive prompts during automated comparison.

### Step 2: Capture Rust output

Run the same commands against the Rust CLI:

```bash
cd <repo-root>
./target/debug/deadline <command> [args] 2>&1
```

### Step 3: Compare

Diff the outputs. Check for:
- Exit codes (0 for success, 1 for operation errors, 2 for usage errors)
- Stdout content (field names, values, ordering, formatting)
- Stderr content
- YAML quoting (watch for YAML 1.1 boolean-like values: ON/OFF/YES/NO)
- JSON spacing (Python uses `": "` and `", "`, Rust may use `":"` and `","`)
- DateTime format (fractional seconds, timezone format)
- Error message wording and quoting style
- Missing or extra fields

### Step 4: Document findings

For each difference found, add an entry to the report file using the
finding schema below. Do NOT fix issues during the audit — complete
the full scope first, then fix in priority order.

### Step 5: Check both directions

After comparing outputs, explicitly ask:
- **Rust-only behavior:** Does Rust do something Python doesn't? Is it
  an improvement (keep) or a bug (fix)?
- **Python-only behavior:** Does Python do something Rust doesn't? Is
  it a gap (fix) or intentionally omitted (document)?

Only document these if you find something. No "nothing found" entries.

---

## Audit tiers

Not all code can be tested the same way. Use the appropriate tier:

| Tier | When to use | Method |
|------|-------------|--------|
| **1: Live CLI comparison** | Any implemented CLI command | Run both CLIs, diff output |
| **2: Code review** | Library code not reachable from CLI | Read Rust + Python source, verify behavioral contract |
| **3: Schema comparison** | Data structures (caches, manifests, config) | Compare schemas, field types, compatibility |

Tier 1 is mandatory for all CLI commands. Tier 2 and 3 supplement it
for code that CLI tests can't reach.

---

## Done criteria

A command is fully audited when:
- Happy path output compared live (Tier 1)
- At least one error path compared live (Tier 1)
- All differences documented as findings or confirmed as accepted differences
- No "deferred" items without a blocking reason

The audit is complete when all commands in scope meet the above criteria.

---

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

---

## Finding schema rules

- **IDs are sequential** (AUDIT-001, AUDIT-002, ...) in discovery order
- **Include exact output** — copy-paste from terminal, not paraphrased
- **One finding per behavioral difference** — don't combine unrelated issues
- **Priority definitions:**
  - **Critical:** Data corruption, security issue, or crash
  - **High:** Wrong output users/scripts would notice, broken functionality
  - **Medium:** Cosmetic differences, error message wording, formatting
  - **Low:** Nice-to-haves, intentional design differences worth documenting

---

## After the audit

1. Update the report's Summary table with final counts
2. Update HANDOFF.md to point to the report and list fixes needed
3. Fix issues in priority order using TDD (failing test → fix → verify)
4. Update each finding's Resolution field as fixes are applied
5. Commit fixes as a batch per the workflow rules

---

## Tips

- Use `diff <(python_cmd) <(rust_cmd)` for quick side-by-side comparison
- Use `--yes` flag to skip interactive prompts
- Test with empty config (`DEADLINE_CONFIG_FILE_PATH=/tmp/empty_config`)
  to isolate behavior from user-specific settings
- For commands requiring auth, test both authenticated and unauthenticated paths
- Watch for trailing whitespace, trailing newlines, and empty lines — they matter
