You are doing Step 6 (Write Spec) of the development workflow.

Read these files first:
- `specs/HANDOFF.md` — current work item, what was implemented and audited
- `specs/{crate}/README.md` — existing specs for the target crate

**Resumability:** If HANDOFF shows Step 6 was already started for this
work item, review what was written and continue from where it left off.
Do NOT restart from scratch.

**Write or update spec files** in `specs/{crate}/` to describe the final
audited state of the code. Do NOT ASSUME they are up-to-date, they can become stale
after EVERY CHANGE.

**Scope:** Check ALL specs that could be affected, not just the crate you changed:
- `specs/{crate}/architecture.md` — layout, module contracts, test counts
- `specs/architecture.md` — crate graph, data flows, crate responsibilities
- `specs/deadline-cli/*.md` — if CLI behavior changed
- `specs/deadline-lib/` — if library API, bundle, attachments, or config changed
- `specs/deadline-python-bindings/` — if bindings interface changed
- Strip stale content rather than just appending new content.
- Match the tone and length of existing sections when writing.

**Include:**
- Behavioral contract (what the code does, not how)
- Data flows
- Edge cases
- Design decisions and rationale
- Differences from Python (if any)

**Exclude:**
- Implementation mechanics (internal function signatures, private types)
- Internal APIs that callers don't use

**Constraints:**
- Do NOT write a spec if the changes are trivial (< 20 lines, no new behavior) because that creates noise
- Do NOT duplicate content already in `specs/patterns.md` because that drifts
- Update `specs/{crate}/README.md` index if new spec files were created

**Update `specs/HANDOFF.md` with:**
- Spec files created/updated (or "No spec needed — trivial change")
- "Status: Step 6 complete, awaiting review"

⛔ GATE: Present spec files written (or rationale for skipping).
Stop and wait for review.
