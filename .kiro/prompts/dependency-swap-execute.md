You are doing Step 2 (Execute) of a dependency swap / implementation replacement.

Read `specs/HANDOFF.md` for the plan approved in Step 1.

**Do it all in one pass:**
1. Replace the implementation directly — no adapter file, no shim
2. Update all callers to use the new API/types
3. Delete dead code (old implementation, unused helpers, unused types)
4. Update tests that assert on error messages to match new wording
5. Prune tests that now only test the dependency's internals
6. `cargo test -p <crate>` — fix until green
7. `cargo test -p deadline-cli` — must pass, count must match baseline

**Error message tests:** If the new dependency rejects the same inputs with
different wording, update assertions to check for relevant keywords rather
than exact phrasing. This is fine when behavior is identical.

**Test pruning criteria (delete in same pass):**
- Tests pure behavior of the new dependency → prune
- Tests internal implementation details of deleted code → prune
- Tests our integration contract or domain logic → keep
- CLI snapshot tests → never touch

**If it blows up:** `git reset --hard` and rethink the approach.
If the direct swap is too tangled, fall back to strangler fig:
add a shim alongside old code, verify equivalence, swap callers one by one.

**Commit:**
- Message: `refactor(<crate>): replace <X> with <new-dep>`
- Body: test count delta, what was deleted, what stays

**Update `specs/HANDOFF.md`:** Mark phase complete with final test counts.

**Present:**
- Final test counts vs baseline
- What was deleted / what stayed and why
- Commit hash
