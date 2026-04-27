# Development Workflow

Overview of the development loop for deadline-cloud-rs. Design philosophy
lives in `specs/patterns.md`, cross-cutting audit methodology in
`specs/audit.md`.

Step-by-step prompts for each phase live in `.kiro/prompts/rust-step*.md`.
Use them in sequence: `@rust-step1-study` → `@rust-step2-tests` →
`@rust-step3-implement` → `@rust-step4-compare` → `@rust-step5-finish`.

## Session Start

1. Read `AGENTS.md`, `specs/architecture.md`, `specs/testing.md`,
   `specs/patterns.md`, and this file.
2. Read `specs/HANDOFF.md`. If it has active work, resume from where
   it left off.
3. If no active work, check `specs/progress.md` and present candidate
   work items to the human. Wait for them to choose.
4. Update `specs/HANDOFF.md` with the chosen work item.

## Rules

- **Update HANDOFF.md after every step.** If the session ends mid-work,
  the next session resumes exactly where this one left off.
- **Cross-reference Python for every feature.** The Python code often
  does things you wouldn't expect from the spec alone. Read it.
- **Improvements before new code.** Audit existing implementation
  against Python source before adding new features.
- **Commit per batch.** A batch is the complete loop (Steps 1-7). Do
  not commit partway through.
- **Specs stay in sync with code.** Before committing, `specs/{crate}/`
  must reflect what was built.
- **Defer honestly.** If blocked, document it in the Progress table.
- **Full audits use `specs/audit.md`** for cross-cutting behavioral
  audits spanning multiple work items.

## The Loop

| Step | Phase | Prompt | Gate |
|------|-------|--------|------|
| 1 | Study Python | `@rust-step1-study` | Plan + test mapping in HANDOFF.md; human reviews |
| 2 | Write tests | `@rust-step2-tests` | Tests fail (red); human approves before implementation |
| 3 | Implement | `@rust-step3-implement` | All tests pass; full workspace builds |
| 4 | Compare CLIs | `@rust-step4-compare` | Differences documented; fixes applied or rationale given |
| 5 | Audit & Fix | `@rust-step5-finish` | Written findings list; all findings resolved |
| 6 | Write spec | `@rust-step5-finish` | Spec describes final audited state |
| 7 | Commit | `@rust-step5-finish` | All tests pass; progress.md + HANDOFF.md updated |
