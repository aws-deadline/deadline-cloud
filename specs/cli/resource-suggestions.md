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

## Resolution Chain

Suggestions are attempted in order of specificity based on which resource IDs
are available:

1. If farm + queue available → try listing jobs in that queue
2. If farm + fleet available → try listing workers in that fleet
3. If farm available → try listing queues, then fleets
4. Fall back to listing farms

Each attempt calls the corresponding List API. If the list call succeeds and
returns results, the suggestions are formatted and returned. If it fails (e.g.,
the user also lacks List permissions), the chain continues to the next level.

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

## Design Decision

This is a free function called explicitly on error paths, not middleware.
Each command controls when and whether to suggest alternatives — some error
paths don't benefit from suggestions (e.g., auth failures).
