You are doing Step 4 (Prune Tests) of a dependency swap / implementation replacement.

Read `specs/HANDOFF.md` for the active swap and deletion results.

**Resumability:** If HANDOFF shows Step 4 was already started, review
what was proposed and continue. Do NOT restart from scratch.

**Identify tests that are now redundant** because they test the new
dependency's internals rather than our integration with it.

**A test is redundant if:**
- It tests pure behavior of the new dependency (e.g. "hash function returns correct output")
- It tests internal implementation details of code we deleted
- It has no domain-specific assertions (no config, no credentials, no callbacks)

**A test is NOT redundant if:**
- It tests our adapter/conversion logic
- It tests domain-specific integration (credential scoping, progress reporting, etc.)
- It tests the contract between our code and the new dependency
- It's a CLI-level (Level 2) snapshot test
- You're unsure — when in doubt, keep it

**Constraints:**
- Do NOT delete anything yet because the human approves each test removal
- Do NOT prune CLI-level (Level 2) snapshot tests because they are the ground truth
- Do NOT prune tests the human says to keep since the human has final authority

**Present the prune list as a table:**
| Test name | File | Why redundant |
|-----------|------|---------------|

**Update `specs/HANDOFF.md` with:**
- Proposed prune list
- "Status: Step 4 (prune) complete, awaiting approval"

⛔ GATE: Stop and present the prune list. Wait for explicit approval
before deleting any tests. The human may say "keep X" for specific tests.
