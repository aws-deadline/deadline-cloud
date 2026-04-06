# Decision: Job Retry and Failure Default Values

## Overview

Deadline Cloud submitters expose two job-level settings that control how task failures
are handled: `maxRetriesPerTask` and `maxFailedTasksCount`. Their default values were
chosen after discussion with internal rendering and pipeline teams and represent a
balance between resilience to transient errors and fast feedback on genuine failures.

## Current Defaults

| Parameter | Default | Description |
|-----------|---------|-------------|
| `maxRetriesPerTask` | 5 | Maximum times a task is retried before it is marked as failed. |
| `maxFailedTasksCount` | 20 | Maximum tasks that can fail before the entire job is marked as failed. |

DCC-specific submitters may override these values when their workload characteristics
justify it. For example, the Unreal Engine submitter uses `maxRetriesPerTask=50` and
`maxFailedTasksCount=1` because Unreal renders are more prone to transient failures
but a single genuine failure usually indicates a scene-level problem.

## Rationale

### Why `maxRetriesPerTask` defaults to 5

Render tasks can fail for transient reasons that are unrelated to the scene itself:

- Temporary network issues when downloading job attachments or writing outputs.
- Worker-side resource pressure (out of memory, GPU driver timeout) that resolves
  when the task is scheduled on a different worker or after the worker recovers.
- License server contention in environments with floating licenses.

A default of 5 retries gives the system a reasonable chance to recover from these
transient issues without user intervention. Setting this to 0 would cause jobs to
fail immediately on the first transient error, which would be disruptive in
production pipelines where artists submit jobs and walk away.

Users who want faster iteration during development (the use case described in
[GitHub issue #1085](https://github.com/aws-deadline/deadline-cloud/issues/1085))
can set `maxRetriesPerTask` to 0 on a per-job basis via the submitter UI, CLI
(`--max-retries-per-task 0`), or in their `parameter_values.yaml`.

### Why `maxFailedTasksCount` defaults to 20

Large jobs can contain thousands of tasks. A small number of failures in a large job
does not necessarily mean the entire job is broken — it may be a handful of frames
with edge-case geometry or lighting that triggers a renderer bug. Allowing up to 20
task failures before killing the whole job lets the majority of the work complete,
which is often preferable to resubmitting the entire job.

## Overriding the Defaults

All submitters allow these values to be changed at submission time:

- **GUI submitters**: DCC submitters expose these as "Maximum retries per task" and
  "Maximum failed tasks count" spin boxes in the Job Properties panel. For example,
  Cinema 4D defines its defaults in
  [`deadline-cloud-for-cinema-4d/.../data_classes.py`](https://github.com/aws-deadline/deadline-cloud-for-cinema-4d/blob/mainline/src/deadline/cinema4d_submitter/data_classes.py):

  ```python
  max_failed_tasks_count: int = field(default=20, metadata={"sticky": True})
  max_retries_per_task: int = field(default=5, metadata={"sticky": True})
  ```
- **CLI**: `deadline bundle submit --max-retries-per-task 0 --max-failed-tasks-count 0`
  (see [`bundle_group.py`](../../src/deadline/client/cli/_groups/bundle_group.py))
- **Job bundles**: Set the values in `parameter_values.yaml`:

```yaml
- name: deadline:maxRetriesPerTask
  value: 0
- name: deadline:maxFailedTasksCount
  value: 0
```
