# `deadline job trace-schedule` — batch-API design

## Summary

`deadline job trace-schedule` uses the Deadline Cloud batch-get APIs
(`BatchGetStep`, `BatchGetTask`) to hydrate step and task records for the
sessions in a job. This replaces the per-step and per-task `GetStep` /
`GetTask` loops that used to dominate the command's wall-clock time on
large jobs.

Measured on a 10,000-task job:

| | Before | After |
|-|-|-|
| Wall clock | ~1402 s (~23 min) | ~226 s (~4 min) |
| `get_task` round-trips | ~10,000 | 0 |
| `batch_get_task` round-trips | 0 | 100 |

The remaining time is dominated by the serial per-session
`list_session_actions` loop. That's not addressed here — the batch APIs
don't replace `list_*` calls (`list_session_actions` already returns full
action payloads, so `BatchGetSessionAction` would not reduce round-trips).

## Module layout

```
src/deadline/client/cli/_groups/
├── _batch_get.py         # generic batch-get-with-retry helper
├── _trace_schedule.py    # the trace-schedule command body
└── job_group.py          # imports and registers the command
```

`job_group.py` was ~1855 lines before this change (the largest file in
`_groups/`). Splitting `trace-schedule` out keeps that file focused on
the other subcommands.

`_job_helpers.py` already exists in the same directory for per-command
formatting helpers (`_format_task_summary`, `_resolve_job_search`, etc.).
It isn't the right home for a generic batch-get utility, so `_batch_get.py`
is a separate module.

## API call pattern

Per invocation, `trace-schedule` issues:

1. `GetJob` — 1 call.
2. `ListSessions` — 1 + paging.
3. `ListSessionActions` — 1 + paging per session (unchanged).
4. `BatchGetStep` — `ceil(U / 100)` calls for `U` unique step IDs.
5. `BatchGetTask` — `ceil(T / 100)` calls for `T` unique `(stepId, taskId)`
   pairs.

The command extracts step and task references from the `taskRun` action
definitions returned by `ListSessionActions`, deduplicates them, and
fetches them in a single pair of batch passes before building the trace.

## `_batch_get`

```python
def batch_get(
    *,
    deadline,                # boto3 deadline client
    operation: str,          # "batch_get_task" etc.
    identifiers: list[dict],
    key_fn: Callable[[dict], Any],
    items_field: str,        # "tasks" / "steps" / ...
    id_fields: tuple[str, ...],
    max_attempts: int = 3,
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[dict, list[dict]]: ...
```

Behavior:

- **Chunking.** Identifiers are split into groups of 100 (the Deadline
  Cloud API limit) and each group becomes one `getattr(deadline,
  operation)(identifiers=chunk)` call.
- **Successes.** Items from each chunk's `items_field` are keyed by
  `key_fn(item)` and merged into a single result dict. A key is never
  re-requested once fetched.
- **Per-item errors.** Each response's `errors` list is partitioned by
  `code`:
  - `InternalServerErrorException`, `ThrottlingException` → **transient**,
    re-queued for the next attempt (identifier reconstructed from the
    error entry via `id_fields`).
  - `ResourceNotFoundException`, `ValidationException`,
    `AccessDeniedException`, any other code → **terminal**, returned to
    the caller as-is.
- **Retries.** Up to `max_attempts` total attempts per identifier.
  Exponential backoff (`0.5 * 2^attempt` seconds) between attempts. After
  the last attempt, still-unresolved identifiers are synthesized into
  `{"code": "ExhaustedRetries", **identifier}` entries appended to the
  terminal list.

Return value is `(results, terminal_errors)`. Whole-call failures (e.g.
throttling at the request level, 5xx) propagate as `ClientError` —
botocore's standard retry mode already handles those.

The helper is private to `cli/_groups/`; promote to
`deadline.client.api` if a second caller appears.

## `_trace_schedule`

The command flow:

1. **Version gate.** `trace-schedule` requires Python 3.9+. Raises
   `DeadlineOperationError` otherwise, matching the `deadline queue
   sync-output` idiom:

   ```python
   if sys.version_info < (3, 9):
       raise DeadlineOperationError(
           "The trace-schedule command requires Python version 3.9 or later"
       )
   ```

2. **Get the job.** Single `GetJob` call; error out if `startedAt` is
   absent (job hasn't started).
3. **Get all sessions.** Paginates `ListSessions` and sorts by
   `startedAt`.
4. **Get all session actions.** For each session, paginates
   `ListSessionActions`.
5. **Fetch steps and tasks in batch** (`_fetch_steps_and_tasks`):
   - Walk the sessions' actions once to collect the set of unique
     `stepId`s and `(stepId, taskId)` pairs referenced by `taskRun`
     action definitions.
   - Call `batch_get(..., operation="batch_get_step", ...)` for the step
     IDs.
   - Call `batch_get(..., operation="batch_get_task", ...)` for the task
     refs.
   - `_warn_on_errors` emits a brief `stderr` summary if any terminal
     errors came back (showing up to 5 examples) and the trace continues
     with whatever was fetched. This matches the command's existing
     philosophy that the trace is a diagnostic and partial data is still
     valuable.
6. **Attach step/task records** (`_attach_steps_and_tasks`): walk the
   sessions again and set `session["step"]` and `action["task"]` from
   the fetched dicts. The pre-existing invariant that a session runs a
   single step (enforced with a `DeadlineOperationError`) is preserved.
7. **Build the Chrome trace** (`_build_trace_events`) and **print the
   summary** (`_print_summary`) — unchanged from the pre-batch
   implementation.
8. **Write the trace file** (`_write_trace_file`) if `--trace-file` was
   given.

The command is registered on `cli_job` in `job_group.py`:

```python
from ._trace_schedule import cli_job_trace_schedule
cli_job.add_command(cli_job_trace_schedule)
```

`cli_job_trace_schedule` is declared with `cls=ContextTrackingCommand` so
it participates in the CLI-wide user-agent tracking verified by
`test_all_cli_commands_use_context_tracking_command`.

## `MockDeadlineBackend` support

`test/unit/deadline_client/mock_deadline_backend.py` grew two batch
methods and a test-only failure-injection knob to support the CLI's new
code paths:

- `batch_get_step(identifiers=[...])` and `batch_get_task(identifiers=[...])`
  both return the real API's shape, `{items: [...], errors: [...]}`. A
  missing identifier becomes a `ResourceNotFoundException` entry in
  `errors`, not an exception.
- The 100-identifier limit is enforced explicitly (the botocore
  `ParamValidator` does not validate list `min`/`max` — only `required`
  and types). Over-limit requests raise a `ValidationException`
  `ClientError`, same as the real API.
- `inject_batch_failure(operation, identifier, code, attempts=1,
  message="injected")` queues a one-shot (or N-shot) per-item error for
  a specific identifier on the next `attempts` calls. Used by tests to
  exercise the transient/terminal error handling in `batch_get` without
  flaky simulation.
- `set_mock_methods` wires `batch_get_step` / `batch_get_task` onto the
  `deadline_mock` `MagicMock` alongside the existing methods.

The `BatchGetSession`, `BatchGetSessionAction`, `BatchGetJob`,
`BatchGetWorker`, `BatchUpdateJob`, and `BatchUpdateTask` APIs are not
implemented on the mock backend — deferred until a consumer needs them.

## Tests

**`test/unit/deadline_client/cli/test_batch_get.py`** — the `batch_get`
helper in isolation:

- Single batch, all succeed.
- >100 identifiers chunk into multiple calls (100, 100, 50).
- Terminal error (`ResourceNotFoundException`) is not retried.
- Transient error (`ThrottlingException`) is retried once and recovers.
- Transient error that never resolves produces a single
  `ExhaustedRetries` entry after `max_attempts`.
- Mix of success, transient-that-recovers, and terminal in one input.
- Empty identifiers list makes no API call.
- Both `ThrottlingException` and `InternalServerErrorException` are
  treated as transient (parametrized test).

**`test/unit/deadline_client/test_mock_deadline_backend.py`** —
`TestBatchGetApis`:

- `batch_get_step` returns existing + error for missing.
- `batch_get_task` returns existing + error for missing.
- `inject_batch_failure` returns the injected code on the first call,
  then falls back to real data.
- Supplying >100 identifiers raises `ValidationException`.

**`test/unit/deadline_client/cli/test_cli_job_trace_schedule.py`** —
`TestTraceScheduleBatchBehavior`:

- The command uses `batch_get_task` and `batch_get_step` (and never
  falls back to `get_task` / `get_step`).
- 150 tasks chunk into two `batch_get_task` calls of sizes 100 and 50.
- Transient per-task error gets retried, trace completes with all data.
- Terminal per-task error produces a `could not retrieve 1 task(s)`
  warning on stderr and the trace still completes.

The pre-existing `TestTraceScheduleWithMockBackend` suite continues to
pass unchanged — the mock backend's batch methods produce the same
step/task payloads the single-item methods did.
