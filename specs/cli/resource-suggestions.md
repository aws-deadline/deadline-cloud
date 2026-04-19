# Resource Suggestions on Error

## Overview

When an API call fails with `AccessDeniedException`, `ResourceNotFoundException`,
or `ValidationException`, the CLI attempts to list alternative resources and
appends suggestions to the error message. This helps users who mistyped an ID
or lack permissions to a specific resource.

## Trigger Conditions

`suggest_resources_on_client_error()` checks the error message string for the
three exception names. Other error types (throttling, internal server error)
do not trigger suggestions.

## Dispatch by Operation Name

The suggestion chain is determined by which API operation failed, not by
which resource IDs happen to be available. Each caller passes the operation
name (e.g. `"GetQueue"`, `"GetFleet"`) and the function dispatches to the
correct suggestion chain. This matches Python's `_OPERATION_GROUPS` pattern.

| Operation group | Operations | Suggestion chain |
|----------------|------------|-----------------|
| queue | `GetQueue`, `ListQueues`, `ListQueueEnvironments` | queues → farms |
| farm | `GetFarm`, `ListFarms` | farms |
| fleet | `GetFleet`, `ListFleets` | fleets → farms |
| worker | `GetWorker`, `SearchWorkers` | workers → fleets |
| job | `GetJob`, `ListJobs`, `SearchJobs`, `CreateJob` | jobs → queues → farms |
| storage_profile | `GetStorageProfileForQueue`, `ListStorageProfilesForQueue` | (not yet implemented) |

Unknown operation names fall back to listing farms.

Each step in the chain calls the corresponding List API. If the list call
succeeds and returns results, the suggestions are formatted and returned.
If it fails (e.g., the user also lacks List permissions), the chain
continues to the next level.

If all list calls fail: returns a message suggesting the IAM policy may be
missing List permissions.

## Output Format

```
Available queues in farm farm-abc123:
  queue-def456  My Render Queue
  queue-ghi789  My Test Queue
```

Shows up to 10 items with ID and display name. If more exist, appends
"... and N more".

## Known Difference from Python

The error message prefix differs. Python's boto3 `ClientError` formats as:
```
An error occurred (AccessDeniedException) when calling the GetQueue operation: <message>
```
Rust's `format_sdk_error` produces:
```
AccessDeniedException: <message>
```
The suggestion content (resource list) is identical. The error prefix
difference is inherent to how the AWS SDK for Rust formats errors vs
boto3 and applies to all API error messages, not just suggestions.
