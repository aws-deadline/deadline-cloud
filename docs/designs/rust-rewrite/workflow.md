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
`serde_json::to_value(resp)` to get JSON. This is a known limitation
of the AWS SDK for Rust (open since 2021, see
[awslabs/aws-sdk-rust#269](https://github.com/awslabs/aws-sdk-rust/issues/269)).
Other Rust cloud SDKs (Azure, Google via prost) do provide serde support.

### Standard pattern: `ResponseBodyCapture` for all API calls

**All** API functions in `api.rs` use the `ResponseBodyCapture`
interceptor to capture the raw HTTP response body as
`serde_json::Value`. This is the single, consistent approach for every
API call — `get_*`, `list_*`, and `search_*` alike.

```rust
let capture = ResponseBodyCapture::new();
client.get_farm().farm_id(id)
    .customize().interceptor(capture.clone()).send().await?;
let json = capture.json()?;  // full response as serde_json::Value
```

The interceptor post-processes the JSON to convert datetime strings to
Python format and remove null values. New API fields appear automatically
without code changes.

**Why not typed SDK output structs?** The SDK output types can't be
serialized back to JSON/YAML. Using them requires manually extracting
every field with accessor methods and rebuilding JSON — tedious for
`get_*` commands with 20+ fields and nested structs, and inconsistent
if some functions use typed extraction while others use raw JSON. The
`ResponseBodyCapture` approach matches Python/boto3 behavior (responses
are raw dicts) and scales uniformly across all API shapes.

### Pagination with `ResponseBodyCapture`

The SDK's built-in paginators (`.into_paginator()`) don't support
`.customize().interceptor()`, so paginated `list_*` functions use manual
`nextToken` loops with the interceptor on each page:

```rust
let mut all_items = Vec::new();
let mut next_token: Option<String> = None;
loop {
    let capture = ResponseBodyCapture::new();
    let mut req = client.list_farms();
    if let Some(ref uid) = user_id { req = req.principal_id(uid.as_str()); }
    if let Some(t) = next_token.take() { req = req.next_token(t); }
    req.customize().interceptor(capture.clone()).send().await.map_err(sdk_err)?;
    let page = capture.json()?;
    if let Some(items) = page["farms"].as_array() {
        all_items.extend(items.iter().cloned());
    }
    match page.get("nextToken").and_then(|t| t.as_str()) {
        Some(t) => next_token = Some(t.to_string()),
        None => break,
    }
}
```

This trades the paginator's convenience for consistency — every API
function follows the same `ResponseBodyCapture` pattern, and the CLI
layer only ever sees `serde_json::Value`.

### Search APIs (no pagination token)

`search_*` APIs use `itemOffset`/`pageSize` instead of `nextToken`.
They return a single page, so no loop is needed — just a single
`ResponseBodyCapture` call.

### Known differences from Python/boto3

The `ResponseBodyCapture` approach captures the raw HTTP JSON, which
differs from Python/boto3 in several cosmetic ways. These are accepted
differences, verified by comparing `deadline` (Python) vs
`./target/debug/deadline` (Rust) against the real API using `ada`
credentials.

- **Field order:** Raw API response order may differ from boto3, which
  reorders fields based on its Smithy service model.
- **Extra fields:** The raw response may include fields (e.g. `arn`)
  that boto3 strips based on its service model.

Both field-order and extra-field differences stem from the AWS SDK for
Rust not implementing `serde::Serialize` on output types
([awslabs/aws-sdk-rust#269](https://github.com/awslabs/aws-sdk-rust/issues/269),
open since 2021), which prevents using typed extraction. A future Smithy
model filtering step could eliminate both — see
`docs/specs/deadline-client.md` § "Future Improvements".
- **Float precision:** `serde_json` parses JSON `1.0` as integer `1`
  when there is no fractional part. Python preserves `1.0`. Affects
  fields like `costScaleFactor`. **Action item:** investigate
  `serde_json` float preservation or post-processing.
- **Fractional second precision:** The API returns milliseconds (e.g.
  `22:35:01.624Z`). boto3 parses into Python `datetime(microsecond=624000)`,
  and `str()` always displays 6 digits (`.624000`). We preserve the
  API's original precision (`.624`). The trailing zeros are a Python
  display artifact — the API wire format was verified to send `.624Z`,
  not `.624000Z`.

### DateTime formatting

The `ResponseBodyCapture` interceptor converts ISO 8601 datetime strings
to Python/boto3 format: `2024-12-18T00:37:38Z` → `2024-12-18 00:37:38+00:00`.
Fractional seconds are preserved as-is from the API response.

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
- All API functions use `ResponseBodyCapture` to capture the raw JSON
  response. This applies to `get_*`, `list_*`, and `search_*` alike.
- List functions: manual `nextToken` loop with `ResponseBodyCapture` on
  each page (SDK paginators don't support `.customize().interceptor()`).
- Search functions: single `ResponseBodyCapture` call (no pagination token).
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
   - Known accepted differences (see "Known differences from Python/boto3"
     section above)
4. If the Rust output differs from Python, fix the implementation first —
   do not accept a snapshot that doesn't match (unless it's a documented
   accepted difference)
5. Once verified, run `cargo insta review` to accept

When possible, also verify against the real API. Obtain AWS credentials
for a test account via your credential management tool (e.g. AWS SSO,
credential process, environment variables), then diff the outputs:
```bash
diff <(deadline <command> 2>&1) <(./target/debug/deadline <command> 2>&1)
```
This catches issues that mock-based tests miss (e.g. fields the real API
returns that the mock omits, datetime precision differences, extra fields
that boto3 strips). Any differences should be either fixed or documented
in the "Known differences" section.

**Do not use `INSTA_UPDATE=always` without reviewing each snapshot.**

### 6. Refactor

Clean up the implementation. Tests must still pass. Commit.

### 7. Update docs

Update `docs/specs/<crate>.md` if the implementation diverged from the
initial spec. Update `docs/ARCHITECTURE.md` if cross-crate relationships
changed. Update the Progress table in `README.md`.
