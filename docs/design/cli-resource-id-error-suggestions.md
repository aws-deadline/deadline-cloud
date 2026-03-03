# Design: CLI Resource ID Error Suggestions

## Overview

When users provide an incorrect resource ID (farm, queue, fleet, job, worker, storage profile) and receive an access denied or not found error, the CLI automatically suggests available resources they have access to. This helps users quickly identify and fix typos in resource IDs.

## Implemented Commands

The following commands show resource suggestions on access/not-found errors:

| Command | Suggestions Shown |
|---------|-------------------|
| `deadline bundle submit` | queues → farms (fallback) |
| `deadline farm list` | farms |
| `deadline farm get` | farms |
| `deadline fleet list` | fleets → farms (fallback) |
| `deadline fleet get` | fleets → farms (fallback) |
| `deadline queue list` | queues → farms (fallback) |
| `deadline queue get` | queues → farms (fallback) |
| `deadline queue paramdefs` | queues → farms (fallback) |
| `deadline job list` | jobs → queues → farms (fallback) |
| `deadline job get` | jobs → queues → farms (fallback) |
| `deadline job cancel` | jobs → queues → farms (fallback) |
| `deadline worker list` | workers → fleets (fallback) |
| `deadline worker get` | workers → fleets (fallback) |

## Key Behaviors

- **CLI-only feature**: Library users (`deadline.client.api`) receive original `ClientError` exceptions
- **Operation detection**: Uses `exc.operation_name` attribute for reliable operation identification
- **Fuzzy matching for workers**: When a worker_id is provided, uses `searchTermFilter` with fuzzy matching
- **Fallback hierarchy**: When List APIs fail, falls back up the resource hierarchy
- **Permission hint**: When all List APIs fail, shows hint about missing IAM List permissions
- **Truncation**: Lists are truncated at 10 items with "... and N more" message

## Resource Hierarchy

When an error occurs, the CLI attempts to list resources, falling back up the hierarchy if needed:

```
Farm
├── Queue
│   ├── Job
│   └── Storage Profile
└── Fleet
    └── Worker
```

## Output Examples

**Queue ID error with available queues:**
```
Failed to submit the job bundle to AWS Deadline Cloud:
An error occurred (AccessDeniedException) when calling the GetQueue operation: ...

Available queues in farm farm-0123456789abcdef0123456789abcdef:
  queue-abcd1234567890abcdef1234567890ab  Production Render Queue
  queue-efgh5678901234cdef5678901234cdef  Test Queue
```

**Farm ID error (cascading fallback):**
```
Failed to get Jobs from Deadline:
An error occurred (ResourceNotFoundException) when calling the SearchJobs operation: ...

Farm farm-wrongid12345678901234567890 may be incorrect. Available farms:
  farm-0123456789abcdef0123456789abcdef  Studio Farm
  farm-abcdef01234567890abcdef012345678  Development Farm
```

**Truncation for long lists:**
```
Available queues in farm farm-0123456789abcdef0123456789abcdef:
  queue-abcd1234567890abcdef1234567890ab  Production Render Queue
  queue-efgh5678901234cdef5678901234cdef  Test Queue
  ... and 8 more
```

**Permission hint when all List APIs fail:**
```
Could not list available resources to suggest alternatives.
This may indicate your IAM policy is missing List permissions.
```

## Handled Error Codes

- `AccessDeniedException` - User lacks permission (likely wrong ID)
- `ResourceNotFoundException` - Resource doesn't exist
- `ValidationException` - Invalid ID format

## Use Cases

### UC1: Mistyped Queue ID
User provides wrong queue ID. CLI suggests available queues in the specified farm.

### UC2: Mistyped Farm ID
User provides wrong farm ID. CLI suggests available farms.

### UC3: Mistyped Farm ID (cascading)
User provides wrong farm ID when specifying a queue. Both GetQueue and ListQueues fail. CLI suggests available farms.

### UC4: Mistyped Fleet ID
User provides wrong fleet ID. CLI suggests available fleets in the farm.

### UC5: Mistyped Job ID
User provides wrong job ID. CLI suggests recent jobs in the queue.

### UC6: Mistyped Worker ID
User provides wrong worker ID. CLI suggests available workers in the fleet.

### UC7: Mistyped Storage Profile ID
User provides wrong storage profile ID. CLI suggests available storage profiles for the queue.
