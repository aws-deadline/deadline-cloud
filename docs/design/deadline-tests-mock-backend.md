# MockDeadlineBackend Design

## Overview

`MockDeadlineBackend` is an in-memory Deadline Cloud simulator for unit testing. It holds resource state in dictionaries and implements a subset of Deadline Cloud APIs, enabling tests to use realistic API flows while maintaining coherent, interconnected data.

## Motivation

Current test patterns have limitations:

1. **Incoherent mock data**: Tests set up individual mock return values that don't reference each other correctly. For example, a session's `stepId` might not match any step in `get_step`.

2. **Brittle exact-match tests**: Tests like `test_cli_job_trace_schedule` compare entire output strings, making them fragile to any formatting changes.

3. **Limited coverage**: Complex commands like `trace-schedule` traverse multiple related resources (jobs → sessions → actions → tasks). Testing all code paths requires coherent data across these resources.

## Prior Art

The file `test/unit/deadline_client/mock_deadline_job_apis.py` implements partial simulations of `search_jobs` and `get_job` APIs with filtering, sorting, and pagination. This functionality will be migrated into `MockDeadlineBackend` to provide a unified testing approach.

## Design Principles

### 1. Parameter Validation via Botocore

The backend uses botocore's service model to validate API parameters against the actual Deadline Cloud API schema. This catches incorrect parameter names, types, and missing required fields - ensuring tests use realistic API calls.

Validation can be disabled with `MockDeadlineBackend(validate_params=False)` for tests that intentionally use invalid parameters.

### 2. Realistic Error Responses

Errors match the real Deadline Cloud API format using `botocore.exceptions.ClientError`:
- `ResourceNotFoundException` for missing resources
- `ValidationException` for invalid templates or parameters

### 3. Job Template Parsing with openjd-model

When `create_job` is called, the backend parses the job template using the `openjd-model` library (added as a test dependency) and automatically creates:
- Steps from the template's step definitions
- Tasks by iterating the parameter space with `StepParameterSpaceIterator`

This means tests can submit real job templates and get realistic job structures without manual setup.

### 4. Simulation Helpers

For tests that need session/action data:
- `simulate_task_runs(job_id, step_id, task_ids=None, *, worker_id, duration_seconds, env_duration_seconds, overhead_seconds)`

Parameters:
- `task_ids`: Optional list of task IDs to run. If `None`, simulates running all tasks in the step.
- `worker_id`: Worker ID for the session (default: "worker-1")
- `duration_seconds`: Duration per task action (default: 30)
- `env_duration_seconds`: Duration per env enter/exit action (default: 2)
- `overhead_seconds`: Idle time at session start before actions begin (default: 0)

This automatically includes env enter/exit actions based on `jobEnvironments` and `stepEnvironments` defined in the job template. Env enters happen at session start (in definition order), env exits happen at session end (reverse order).

## API Coverage

| Category | Methods |
|----------|---------|
| Farm | `create_farm`, `get_farm` |
| Queue | `create_queue`, `get_queue` |
| Job | `create_job`, `get_job`, `search_jobs` |
| Step | `get_step` |
| Task | `get_task` |
| Session | `list_sessions`, `list_session_actions` |

## Integration with Existing Fixtures

The `set_mock_methods(deadline_mock)` method wires the backend to the existing `deadline_mock` pytest fixture, allowing seamless use with current test infrastructure.

## File Location

```
test/unit/deadline_client/
├── conftest.py                        # Existing deadline_mock fixture
├── mock_deadline_backend.py           # MockDeadlineBackend class
├── test_mock_deadline_backend.py      # Unit tests for the backend itself
└── cli/
    └── test_cli_job_trace_schedule.py # Tests using MockDeadlineBackend
```

## Dependencies

- **Python 3.9+**: Required for `openjd-model` library
- **openjd-model**: Job template parsing and parameter space iteration
- **botocore**: Parameter validation against Deadline API schema

Tests using `MockDeadlineBackend` are skipped on Python < 3.9.

## Migration Plan

1. **Phase 1** (complete): Initial `MockDeadlineBackend` with core APIs and template parsing
2. **Phase 2**: Migrate filter/sort logic from `mock_deadline_job_apis.py` into `search_jobs()`
3. **Phase 3**: Update existing tests to use `MockDeadlineBackend`
4. **Phase 4**: Deprecate `mock_deadline_job_apis.py`

## Future Enhancements

1. **Complete search_jobs**: Full filter/sort support
2. **Pagination**: Simulate `nextToken` for list operations
3. **More search APIs**: `search_workers`, `search_steps`, `search_tasks`
4. **State transitions**: Simulate job lifecycle (PENDING → RUNNING → SUCCEEDED)
