You are doing Step 5 (Finish) of a dependency swap / implementation replacement.

Read `specs/HANDOFF.md` for the active swap and approved prune list.

**Resumability:** If HANDOFF shows Step 5 was already started, review
what was done and continue. Do NOT restart from scratch.

**Delete approved tests:**
- Remove only the tests the human approved for pruning in Step 4
- Keep any tests the human said to keep

**Simplify adapter (if trivial):**
- If the adapter is just re-exports or single-line delegations, collapse it
- Expose the new dependency's types directly in the public API
- Update all callers if public types change
- If the adapter has non-trivial conversion logic, leave it as-is

**Final verification:**
1. `cargo test -p <target_crate>` — record count
2. `cargo test -p deadline-cli` — must match baseline exactly
3. `cargo build --workspace` — full workspace must compile

**Constraints:**
- Do NOT push the commit because it needs human review before sharing
- The CLI test count MUST be unchanged from baseline because CLI tests are the ground truth

**Update docs:**
- In `specs/HANDOFF.md`: mark this phase/swap as complete, remove all
  intermediate step notes (baseline counts, shim results, delete results,
  prune list) because they are stale now. Keep only a completion note.
- Update the relevant spec file with actual prune count and any deviations

**Commit:**
- Stage affected files + workspace Cargo.toml + Cargo.lock
- Message: `refactor(<crate>): replace <X> with <new-dep>`
- Body: include test count delta: "Tests: N → M (pruned P redundant unit tests)"

**Present:**
- "FINAL: crate N tests (was B, pruned P), CLI M/M passing"
- Commit message
- Summary of what changed

⛔ GATE: Stop and wait for final review before moving to next phase/item.
