# Development Workflow

The workflow for porting each feature from the Python CLI to Rust.

## Rules

- **No code before approval.** Steps 0-2 are planning. Present the plan
  and wait for the human to approve before writing any code.
- **Cross-reference Python for every feature.** The Python code often does
  things you wouldn't expect from the spec alone. Read it.
- **Improvements before new code.** Before adding new features, audit
  existing implementation against the Python source. Flag behavior gaps
  and cleanups. Implement improvements first.
- **Commit per batch.** Each logical batch gets its own commit following
  red-green TDD. Keep commits small and focused.
- **Docs stay in sync.** After implementation, update `docs/specs/<crate>.md`
  with Rust-specific design decisions. Update the Progress table in
  `README.md`. Do not let docs drift from code.
- **Defer honestly.** If a feature is blocked on an unimplemented crate,
  say so and document it in the Progress table rather than building
  throwaway scaffolding.

## Design Principles

These apply at Step 0 when designing the Rust equivalent. The goal is
identical observable behavior, not identical internal structure.

1. **Don't replicate Python's module structure.** Python uses module-level
   functions with global caches because that's idiomatic Python. Rust
   should use structs that own their state. If Python has five free
   functions sharing a module-level dict, Rust probably wants one struct
   with five methods.

2. **Globals are a code smell.** If the Python uses `@lru_cache` on
   module-level functions or mutable module globals, the Rust design
   should use owned state on a struct. Ask: "who owns this data?" If
   the answer is "nobody, it's global" — that's the thing to fix.

3. **Every abstraction must earn its keep.** Before adding a type,
   wrapper, conversion layer, or any indirection that the Python
   doesn't have, ask: "what does this prevent or enable?" If the
   answer is a concrete benefit (compile-time error catching, safety,
   testability), add it. If the answer is "it's more Rust-like" or
   "it feels cleaner," don't — that's complexity without value. The
   simplest correct implementation wins.

4. **Preserve observable behavior exactly.** Same output, same errors,
   same exit codes, same caching semantics (calls are cached, force
   refresh clears cache). The test specs define the contract — internal
   structure is free to diverge.

5. **Don't over-abstract.** If the Python is a simple function that
   doesn't need to become a trait, don't make it one. Only introduce
   abstractions that solve a real problem (testability, extensibility
   the code actually needs).

6. **Ask "what would I design if the Python didn't exist?"** Read the
   test spec and the Python source, then close the Python file and
   design the Rust API from the behavioral requirements. Open the
   Python again only to verify you haven't missed edge cases.

## AWS SDK for Rust Usage

The Rust SDK (`aws-sdk-deadline`) differs from Python's boto3 in ways
that affect how we build API functions. Know these before writing code.

### SDK types don't implement `Serialize`

The SDK output types (`GetFarmOutput`, `GetQueueOutput`, etc.) and their
nested types (`JobAttachmentSettings`, `FleetConfiguration`, etc.) do
**not** implement `serde::Serialize`. You cannot call
`serde_json::to_value(resp)` to get JSON.

For `get_*` commands that dump the full response, use the
`ResponseBodyCapture` interceptor (`raw_response.rs`). This captures the
raw HTTP response body after the SDK deserializes it, then parses it as
`serde_json::Value` — the same raw dict that Python/boto3 returns.

```rust
let capture = ResponseBodyCapture::new();
client.get_farm().farm_id(id)
    .customize().interceptor(capture.clone()).send().await?;
let json = capture.json()?;  // full response as serde_json::Value
```

The interceptor post-processes the JSON to convert datetime strings to
Python format and remove null values. New API fields appear automatically
without code changes.

For `list_*` commands where we select specific fields, use the typed SDK
paginator items directly (e.g. `FarmSummary`) and build the display JSON
manually with `put`/`put_opt` helpers. This gives type safety for the
fields we care about.

### Use SDK paginators for list operations

Every `List*` API has a built-in paginator. Use it instead of manual
`next_token` loops:

```rust
let mut stream = client.list_farms()
    .principal_id(uid)
    .into_paginator()
    .items()       // flattens across pages, yields FarmSummary
    .send();       // returns PaginationStream

while let Some(item) = stream.try_next().await.map_err(sdk_err)? {
    // item is FarmSummary
}
```

The paginator handles `nextToken` internally. `.items()` flattens across
pages so you get individual items, not pages of items.

### Field ordering

Enable `serde_json`'s `preserve_order` feature (already done in
`Cargo.toml`). For `list_*` commands, insert fields into
`serde_json::Map` in the same order Python outputs them.

For `get_*` commands using the `ResponseBodyCapture` interceptor, field
order comes from the raw API response JSON. This may differ from
Python/boto3's order (boto3 reorders based on its service model). This
is an accepted difference per approach A — see the Progress table.

### DateTime formatting

The SDK uses `aws_smithy_types::DateTime`. Its `fmt(Format::DateTime)`
produces `2024-12-18T00:37:38Z`. Python/boto3 produces
`2024-12-18 00:37:38+00:00`. Use the `fmt_datetime` helper in `api.rs`
which converts between these formats.

### Error formatting

The SDK's `SdkError` `Display` impl just says `"service error"`. Use
the `sdk_err` helper in `api.rs` which extracts the error code and
message: `"AccessDeniedException: User is not authorized..."`. This is
needed for `suggest_resources_on_client_error` to detect error types.

## Steps

### 0. Plan, study Python, and get approval

Before planning, read these (in order). Skip none.

- [ ] `migration_strategy.md` — goals and constraints
- [ ] `../../ARCHITECTURE.md` — crate relationships
- [ ] `../../TESTING.md` — test philosophy and levels
- [ ] `../../specs/<crate>.md` for the target crate
- [ ] Relevant `test_specs/` sections
- [ ] Python source for the feature being ported

Read the Python source deeply — not just the happy path, but edge cases,
surprising behaviors, error messages, and how it interacts with config,
credentials, and other subsystems. The Python code often does things you
wouldn't think to do from the spec alone (quirky defaults, silent
fallbacks, format-specific output details). You need this understanding
to design the Rust API correctly.

For CLI commands that call AWS APIs, also check:
- [ ] The SDK operation docs on docs.rs (e.g.
  `aws_sdk_deadline::operation::get_farm`) for input/output types
- [ ] The output struct fields — these are the fields you must extract
- [ ] Whether a paginator exists (e.g. `ListFarmsPaginator`)
- [ ] What fields the Python CLI selects for list output (e.g.
  `["farmId", "displayName"]`) and what order
- [ ] Run the Python CLI command and capture its exact output — this is
  the target you must match

Then apply the Design Principles above and present:

- What you're implementing and which test spec cases it covers
- How you'll batch the work
- The Rust API design — not a function-by-function port, but the
  struct/trait/module layout that satisfies the test spec behaviors.
  Reference the Design Principles to justify structural divergences
  from Python.
- Edge cases and surprising Python behaviors that affect the design
- What's blocked or deferred and why

**Wait for approval before proceeding to step 1.**

### 1. Review existing implementation for improvements

Before writing new code, audit what's already implemented against the
Python source. Look for:
- Behavior gaps (e.g., stdout vs stderr for errors)
- Missing edge cases
- Opportunities to use idiomatic Rust (ValueEnum instead of manual
  FromStr, concrete error types instead of Box<dyn Error>)

Implement improvements first, commit, then proceed to new features.
Skip this step if the crate is a stub with no existing implementation.

### 2. Update the crate spec

Write up the feature's behavior and implementation approach in the relevant
`docs/specs/<crate>.md`. Describe what it does and how it should work in
Rust, but keep it at the design level — no code blocks unless they're
needed to show a non-obvious interface or data format. This becomes the
reference for both the tests and the implementation.

### 3. Red — Write failing tests

Write tests that assert on observable behavior: stdout, stderr, exit code,
file contents. Prefer Level 2 (CLI subprocess) tests. Run them and confirm
they fail. If a test passes before implementation, it's not testing anything
new.

**CLI output tests use `insta-cmd` snapshots** (see `docs/TESTING.md` for
the generic framework).

Read the relevant section in `docs/designs/rust-rewrite/test_specs/` for
test case inspiration. Do **not** reference section or case numbers in test
names or comments — the test specs are migration-era scaffolding, not
maintained after tests are written.

### 4. Green — Implement

Write the minimum code to make the tests pass.

For CLI commands that call AWS APIs, follow the patterns in the
"AWS SDK for Rust Usage" section above:
- List functions: use the SDK paginator (`.into_paginator().items().send()`)
  with typed field selection via `put`/`put_opt` helpers.
- Get functions: use the `ResponseBodyCapture` interceptor to capture the
  raw JSON response. No manual field extraction needed.
- Mock responses: include all fields the real API returns, not just the
  minimum. Check the SDK output struct on docs.rs for the complete list.

### 5. Verify snapshots against Python CLI

Before accepting any snapshot, compare it against the Python CLI output.
The Python CLI is the reference implementation — the Rust output must match.

1. Run `cargo test` — new snapshots are written as `.snap.new` files
2. For each snapshot, run the equivalent Python CLI command and capture
   its output
3. Compare the snapshot content against the Python output. Watch for:
   - YAML key ordering (Python's dict insertion order vs Rust's serde)
   - Field completeness in `get` commands (all API response fields present)
   - Header lines on `list` commands (count/offset)
   - Boolean capitalization (`True`/`False` vs `true`/`false`)
   - Error message format
4. If the Rust output differs from Python, fix the implementation first —
   do not accept a snapshot that doesn't match
5. Once verified, run `cargo insta review` to accept

When possible, also verify against the real API:
```bash
diff <(deadline <command> 2>&1) <(./target/debug/deadline <command> 2>&1)
```
This catches issues that mock-based tests miss (e.g. fields the real API
returns that the mock omits).

**Do not use `INSTA_UPDATE=always` without reviewing each snapshot.**

### 6. Refactor

Clean up the implementation. Tests must still pass. Commit.

### 7. Update docs

Update `docs/specs/<crate>.md` if the implementation diverged from the
initial spec. Update `docs/ARCHITECTURE.md` if cross-crate relationships
changed. Update the Progress table in `README.md`.
