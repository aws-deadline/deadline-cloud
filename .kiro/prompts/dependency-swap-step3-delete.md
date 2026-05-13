You are doing Step 3 (Delete + Verify) of a dependency swap / implementation replacement.

Read `specs/HANDOFF.md` for the active swap and shim verification results.

**Resumability:** If HANDOFF shows Step 3 was already started, review
what was done and continue. Do NOT restart from scratch.

**Delete the old implementation that the adapter replaced:**
- Remove functions/structs/modules that are fully replaced
- The adapter now IS the implementation

**Constraints:**
- Do NOT delete code that has domain-specific logic not present in the new dependency since that logic has no replacement
- Do NOT delete the adapter itself — it may still be needed as glue

**After deletion:**
1. `cargo build -p <target_crate>` must compile
2. If it doesn't compile, something was still referenced — restore it
3. `cargo test -p <target_crate>` — ALL tests must pass
4. `cargo test -p deadline-cli` — ALL tests must pass

If a test fails after deletion, the deleted code was still needed.
Restore it and investigate what the adapter missed.

**Error message differences:**
If tests fail ONLY because the new dependency produces different error message
text (not different behavior — same inputs are still rejected/accepted), you
MAY update the test assertions to match the new wording. This is acceptable
when:
1. The error type/variant is unchanged (still `ManifestDecode`, etc.)
2. The rejection behavior is identical (same inputs fail, same inputs succeed)
3. No downstream code pattern-matches on the specific error string
4. The new messages are equally or more descriptive

Update assertions to check for the relevant keyword (e.g., `"totalSize"`)
rather than exact phrasing. Document which tests were updated and why in
the HANDOFF.

**Present:**
- Files/functions deleted (with line counts)
- "VERIFY DELETE: crate N/N passing, CLI M/M passing"
- Anything that couldn't be deleted and why

**Update `specs/HANDOFF.md` with:**
- What was deleted, verification results
- "Status: Step 3 (delete) complete, awaiting review"

⛔ GATE: Stop and wait for review. Do not touch tests until approved.
