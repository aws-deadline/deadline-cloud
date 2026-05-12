You are doing Step 2 (Shim + Verify) of a dependency swap / implementation replacement.

Read `specs/HANDOFF.md` for the active swap, baseline counts, and scope.

**Resumability:** If HANDOFF shows Step 2 was already started, review
what was done and continue. Do NOT restart from scratch.

**Add the new dependency and create an adapter layer:**
1. Add the new dependency to workspace `Cargo.toml`
2. Add it to the target crate's `Cargo.toml`
3. Create adapter code that maps new types → existing public API
4. Keep the adapter minimal: type aliases, delegating functions, From impls
5. The OLD implementation code stays in place — do not delete it yet

**Constraints:**
- Do NOT change any test code because tests define the contract
- Do NOT delete any existing implementation code because we verify before deleting
- If there's a version conflict, resolve by removing the direct dependency
  from our crate (get it transitively from the new dep)

**Verify:**
1. `cargo build -p <target_crate>` must compile
2. `cargo test -p <target_crate>` — ALL tests must pass
3. `cargo test -p deadline-cli` — ALL tests must pass
4. Compare test counts to baseline — must be EQUAL

If a test fails: fix the adapter, NOT the test.
If the failure reveals a genuine behavioral difference between the new
dependency and our code, document it and ask how to proceed.
Do NOT decide on your own how to resolve behavioral differences because the human owns that decision.

⛔ GATE: If behavioral differences are found, present them and STOP.
Wait for human direction.

**Present:**
- "VERIFY SHIM: crate N/N passing, CLI M/M passing"
- Adapter decisions made (type mappings, serde differences)
- Any behavioral differences discovered

**Update `specs/HANDOFF.md` with:**
- Shim verification results
- "Status: Step 2 (shim) complete, awaiting review"

⛔ GATE: Stop and wait for review. Do not delete any code until approved.
