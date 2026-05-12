You are doing Step 1 (Baseline) of a dependency swap / implementation replacement.

Read these files first:
- `specs/HANDOFF.md` — confirm which swap is active and what's being replaced
- The spec file linked from HANDOFF (follow the link) — this is your reference
  for what to replace, which phase you're on, and the full plan

**Resumability:** If HANDOFF shows Step 1 was already started, review
what was done and continue. Do NOT restart from scratch.

**Identify what's being replaced:**
- Read the plan/spec to identify exact files being swapped out
- Read the source files being replaced in our codebase
- Read the replacement dependency's source/docs to understand its API
- Identify all affected test files and count tests per file

**Record baseline:**
1. Run `cargo test -p <target_crate> -- --list 2>&1 | grep ": test$" | wc -l`
2. Run `cargo test -p <target_crate>` — all must pass
3. Run `cargo test -p deadline-cli -- --list 2>&1 | grep ": test$" | wc -l`
4. Run `cargo test -p deadline-cli` — all must pass

**Constraints:**
- Do NOT begin any code changes because this step is read-only
- Do NOT proceed if baseline tests are failing because you cannot swap on top of a broken build — fix first

**Present:**
- Summary: "Replacing [X] with [Y]. N tests in crate, M in CLI."
- Baseline counts
- List of files to be replaced and their new-dependency equivalents
- Any API mismatches or concerns spotted during reading

**Update `specs/HANDOFF.md` with:**
- Baseline counts and scope summary
- "Status: Step 1 (baseline) complete, awaiting review"

⛔ GATE: Stop and wait for review. Do not begin any code changes.
