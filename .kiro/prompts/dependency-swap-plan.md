You are doing Step 1 (Plan) of a dependency swap / implementation replacement.

Read `specs/HANDOFF.md` for context, then read the spec file it links to.

**Do:**
1. Read the source files being replaced
2. Read the replacement dependency's API (check `~/.cargo/registry/src/`)
3. Record baseline: `cargo test -p <crate> -- --list 2>&1 | grep ": test$" | wc -l`
4. Confirm baseline passes: `cargo test -p <crate>` and `cargo test -p deadline-cli`

**Present (concise):**
- What's being replaced → what it's replaced with
- API differences / type mismatches
- Callers that need updating
- Tests that will need error-message updates or pruning
- Any blockers (behavioral differences that change outcomes, not just messages)

**Assess complexity — pick the right approach:**

| Complexity | Criteria | Approach |
|------------|----------|----------|
| **Low** | ≤5 callers, simple type mapping, no behavioral differences | Direct swap (Step 2: Execute) |
| **High** | >10 callers across multiple crates, complex type conversions, behavioral differences, or public API changes that affect downstream consumers | Strangler fig: add shim alongside old code → verify equivalence → swap callers incrementally → delete old code. Present a phased plan. |

If high complexity, break the work into sub-phases and present the plan for approval.

**Update `specs/HANDOFF.md`** with baseline counts and status.

⛔ GATE: Get go/no-go before making changes.
