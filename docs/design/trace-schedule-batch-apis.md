# Plan: Use Batch APIs in `deadline job trace-schedule`

## Background

On 2026-04-06, AWS Deadline Cloud released 8 new batch APIs for bulk resource
operations. The `deadline job trace-schedule` command currently drives the
trace by making many individual `get_step`, `get_task`, and paginated
`list_session_actions` calls. For a large job with thousands of sessions and
tasks, this can take 30+ minutes dominated by per-call API latency.

Switching to the new batch APIs collapses `N` calls into `ceil(N/100)` calls,
reducing wall-clock time and throttling pressure dramatically.

## Scope

- **In scope:** `job_trace_schedule` (split into a new
  `_groups/_trace_schedule.py`), a new `_groups/_batch_get.py` helper,
  plus supporting mock backend additions and tests.
- **Out of scope:** Other CLI commands, other batch APIs (e.g.
  `BatchUpdateJob`, `BatchUpdateTask`) — those are not read paths used by
  trace-schedule.

## Current API call pattern

Per-job, trace-schedule issues:

1. `get_job` — 1 call
2. `list_sessions` — 1 + N pages
3. `list_session_actions` — 1 + P pages **per session** (serial loop)
4. `get_step` — up to 1 per unique step (cached)
5. `get_task` — up to 1 per unique task (cached)

For a 10,000-task job on ~500 sessions the hot paths are `get_task`
(~10,000 calls) and `list_session_actions` (~500+ calls, fully serialized).

## Target API call pattern

Per-job, trace-schedule will issue:

1. `get_job` — unchanged, 1 call
2. `list_sessions` — unchanged
3. `list_session_actions` — unchanged. It already returns full action
   payloads, so `batch_get_session_action` is not a win here.
4. `batch_get_step` — replaces the per-unique-step `get_step` loop
5. `batch_get_task` — replaces the per-unique-task `get_task` loop

Both `batch_get_step` and `batch_get_task` accept up to **100 identifiers
per request** and return `{items: [...], errors: [...]}` — partial success
is possible and expected.

## Minimum boto3 version

`boto3 >= 1.42.1`.

Python 3.9+ users already meet this via the existing
`boto3 >= 1.42.29; python_version >= '3.9'` pin in `pyproject.toml`.

The `< 3.9` pin stays at `boto3 >= 1.36.8`. Instead, gate
`trace-schedule` on the Python version, mirroring how
`deadline queue sync-output` does it today:

```python
if sys.version_info < (3, 9):
    raise DeadlineOperationError(
        "The trace-schedule command requires Python version 3.9 or later"
    )
```

## Batch-API semantics we must handle

From the docs and API reference:

- **Max 100 identifiers per request** for all batch get APIs.
- **Partial success:** response has `{items: [...], errors: [...]}`. HTTP
  200 can still carry per-item errors. Error `code` values include:
  `InternalServerErrorException`, `ResourceNotFoundException`,
  `ValidationException`, `AccessDeniedException`, `ThrottlingException`.
- Errors returned in the `errors` list carry the full identifier tuple
  (e.g. `farmId`, `queueId`, `jobId`, `stepId`, `taskId`), so the caller
  can map each error back to its request.
- Whole-call failures (throttling, 5xx) raise `ClientError` as usual.

### Retry / collect-results strategy

We need a helper that:

1. Takes a list of identifiers and a batch-get callable.
2. Chunks into batches of 100.
3. For each batch, issues the call, collects successes into a result dict,
   and collects per-item errors into a retry queue with their error code.
4. Retries items whose error is transient (`InternalServerErrorException`,
   `ThrottlingException`) with exponential backoff, up to a small maximum
   (e.g. 3 attempts). Botocore's standard retry mode handles whole-call
   throttling, but **per-item** throttling errors come back in `errors[]`
   and are our responsibility.
5. Treats `ResourceNotFoundException` / `ValidationException` /
   `AccessDeniedException` as terminal — the identifier goes into a
   "failed" list that the caller reports.
6. Never re-requests an identifier we already have a successful response
   for.

Sketch:

```python
def _batch_get(
    *,
    deadline,              # boto3 deadline client
    operation: str,        # "batch_get_task" etc.
    identifiers: list[dict],
    key_fn,                # identifier -> hashable key
    items_field: str,      # "tasks" / "steps"
    max_attempts: int = 3,
) -> tuple[dict, list[dict]]:
    """Batch-get with partial-success handling. Returns (results, terminal_errors)."""
    remaining = list(identifiers)
    results: dict = {}
    terminal: list = []
    attempt = 0
    while remaining and attempt < max_attempts:
        next_round: list = []
        for chunk in _chunks(remaining, 100):
            response = getattr(deadline, operation)(identifiers=chunk)
            for item in response.get(items_field, []):
                results[key_fn(item)] = item
            for err in response.get("errors", []):
                if err["code"] in ("InternalServerErrorException", "ThrottlingException"):
                    next_round.append({k: err[k] for k in _id_fields(operation)})
                else:
                    terminal.append(err)
        remaining = next_round
        if remaining:
            time.sleep(_backoff(attempt))
        attempt += 1
    terminal.extend({"code": "ExhaustedRetries", **ident} for ident in remaining)
    return results, terminal
```

The helper goes in a new private module
`src/deadline/client/cli/_groups/_batch_get.py`. `job_group.py` is already
1855 lines — the largest file in `_groups/` by far — so we avoid adding
to it. An existing `_job_helpers.py` file is scoped to per-command
formatting helpers (`_format_task_summary`, `_resolve_job_search`, etc.)
and isn't the right home for a general batch-get utility.

### Also split `job_trace_schedule` out

As part of this change, move the `job_trace_schedule` command body into
a new `_groups/_trace_schedule.py` module and have `job_group.py` just
import-and-register it. This keeps the scope contained (the change is
already touching the command heavily) and starts chipping away at the
size of `job_group.py`.

## Code changes

### `src/deadline/client/cli/_groups/_trace_schedule.py` (new)

Move the `job_trace_schedule` function here. Replace the per-session
`get_step`/`get_task` logic inside the `click.progressbar` loop with:

1. First pass over sessions — collect the set of unique `(stepId, taskId)`
   pairs referenced by `taskRun` actions.
2. Call `_batch_get` for steps (unique step IDs, fields:
   `farmId/queueId/jobId/stepId`), store into `steps: dict[str, Any]`.
3. Call `_batch_get` for tasks (unique `(stepId, taskId)`, fields:
   `farmId/queueId/jobId/stepId/taskId`), store into `tasks: dict[str, Any]`.
4. Second pass over sessions — attach `step`/`task` references as before.
5. If `_batch_get` returns any terminal errors, emit a warning (use
   `click.echo` with `err=True`) and continue with partial data — the trace
   should still be useful. Match the spirit of the existing code, which
   raises `DeadlineOperationError` on unexpected structural inconsistencies
   but otherwise produces a best-effort trace.

The existing check that a session runs only a single step stays intact —
we just look up `session["step"]` from the pre-populated `steps` dict.

### `src/deadline/client/cli/_groups/job_group.py`

Import the command from `_trace_schedule` and register it with the
`cli_job` group. No trace-schedule body remains in this file.

### Version gate

At the top of `job_trace_schedule`, before any batch work:

```python
if sys.version_info < (3, 9):
    raise DeadlineOperationError(
        "The trace-schedule command requires Python version 3.9 or later"
    )
```

No `pyproject.toml` changes are required.

## Mock backend additions

`test/unit/deadline_client/mock_deadline_backend.py` today covers
`get_job`, `get_step`, `get_task`, `list_sessions`, `list_session_actions`
but not the batch APIs. Trace-schedule tests will fail once we switch code
over. Additions needed:

1. `batch_get_step(identifiers)` → iterate identifiers, reuse
   `self.steps[key]` lookup; missing → `errors[]` with
   `ResourceNotFoundException`.
2. `batch_get_task(identifiers)` → same pattern with `self.tasks[key]`.
3. (Deferred) `batch_get_session`,
   `batch_get_session_action`, `batch_get_job`, `batch_get_worker`,
   `batch_update_job`, `batch_update_task` — stubs for completeness.
   Trace-schedule only needs the first two.
4. **Partial-success injection:** add a test-only knob
   `backend.inject_batch_failure(operation, identifier_key, code, attempts)`
   that records a one-shot (or N-shot) per-item error for a specific
   identifier. This lets us test the retry/collect logic for both
   transient and terminal error codes without flaky simulation.
5. Enforce the max-100-identifiers limit in the mock to catch bugs where
   chunking is forgotten. Raise a `ValidationException` when exceeded —
   same behavior as the real API.
6. Extend `set_mock_methods` to wire the new methods onto the `MagicMock`.

Put all validation through `self._validate("BatchGetStep", params)` etc. —
the botocore validator will check the schema (ensuring identifiers list
is present, types are right, max items) against the real service model.

## Tests

### Unit tests for `_batch_get` helper

New file: `test/unit/deadline_client/cli/test_batch_get.py`

Cases:

- All items succeed in one batch → returns full dict, no errors.
- More than 100 items → multiple batches, all succeed.
- One item returns `ResourceNotFoundException` → terminal error, no retry.
- One item returns `ThrottlingException` once, succeeds on retry →
  end result contains it, no terminal error.
- One item returns `InternalServerErrorException` for all attempts →
  terminal error after `max_attempts`.
- Mix: 50 succeed, 1 transient fail (recovers), 1 terminal fail.

### Integration with trace-schedule

Update `test/unit/deadline_client/cli/test_cli_job_trace_schedule.py`:

- Rewire existing tests via the mock backend's new batch methods.
  (The backend changes mean tests should pass unchanged if the CLI
  correctly uses the batch APIs against the backend.)
- Add a new test that injects a transient error on one task and asserts
  the trace output still contains that task's data (retry worked).
- Add a new test that injects a terminal error on one task and asserts
  a warning is emitted and the trace is produced without that task.

### Scale sanity check

Add a test with ~250 tasks across 3 steps on ~10 sessions to confirm:
- Exactly 3 `batch_get_step` calls (1 batch, 3 steps).
- Exactly `ceil(250/100) = 3` `batch_get_task` calls.
- The mock backend's max-100 enforcement is not tripped.

Assert call counts via the `MagicMock` wrapped around the backend.

## Performance expectations

Baseline measurement on job `job-3963bfd39f3d4f17b18ffee9b4c340a6`
(10 sessions, 10,050 session actions, 10,000 unique tasks):
**1402 s (~23.4 minutes)** end-to-end wall time. The `get_task`
loop dominates.

For a 10,000-task job with the batch APIs:

| Phase | Before | After |
|-------|--------|-------|
| `get_step` | ~U unique steps, serial | `ceil(U/100)` batch calls |
| `get_task` | ~10,000 serial calls | 100 batch calls |
| Total round-trips | ~10,000+ | ~100 |

At ~140 ms per call (observed on the baseline run), this is roughly
1400 s → 15 s for the get-*-resources phases.

## Rollout

Single PR, developed in these incremental commits:

1. `_batch_get` helper + unit tests.
2. Mock backend batch APIs + failure-injection helper.
3. Move `job_trace_schedule` to `_trace_schedule.py` and switch it to use
   the batch helper; update tests.

## Open questions

- Should `_batch_get` eventually live in `deadline.client.api` so other
  commands can use it? For now keep it private to `cli/_groups/`; promote
  later if a second caller appears.
- Should terminal errors be fatal or warn-and-continue? Current code
  sometimes raises `DeadlineOperationError`. Recommend **warn** for missing
  tasks/steps since the trace is a diagnostic and partial data is still
  valuable.
