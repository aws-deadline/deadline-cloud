# Design: `deadline job get` with Search Term

## Overview

Enable users to find jobs using a search term instead of requiring job IDs. `deadline job get "search term"` searches for jobs matching that term across multiple fields, displaying full details if one matches or a summary list if multiple match.

## Problem Statement

Users must know the exact job ID to use commands like `deadline job get --job-id job-xxx`. This is cumbersome because:
- Job IDs are opaque 32-character hex strings
- Users typically remember job names, not IDs
- Finding a job ID requires running `deadline job list` first

## CLI Interface

```bash
# Search by name (single result shows full details, multiple shows summary)
deadline job get "Blender Render"

# Job ID as positional argument
deadline job get job-eee5a7898b6c493d84aaa12ea384156b

# Prefix of Job ID as positional argument
deadline job get job-eee5a

# Job ID with --job-id flag
deadline job get --job-id job-eee5a7898b6c493d84aaa12ea384156b

# No arguments uses default job ID from config
deadline job get
```

## Command Behavior

1. If `SEARCH_TERM` matches job ID pattern (`job-[0-9a-f]{32}`): get full job details
2. If `SEARCH_TERM` is any other string: search jobs with that term
   - Exactly one match: get full job details via `GetJob` API
   - Multiple matches: display summary table
   - No matches: display "No jobs found" message
3. If no `SEARCH_TERM` provided: use `--job-id` or default job ID from config

## Output Format

**Single result or job ID:** Full job details from `GetJob` API

**Multiple results:**
```
Found 10 job(s) matching "job", showing most recent 5:

  Turntable with Maya/Arnold
    job-ca34b643089742609bdf36391b4f0fe0  SUCCEEDED     2026-01-23 17:09:04 -0800
    Tasks: 52 succeeded

  Turntable with Maya/Arnold
    job-d4c36c381d3d4854b0dfec8e35486e5d  FAILED        2026-01-23 17:06:58 -0800
    Tasks: 1 failed, 51 canceled

  This is a very long job name to test truncation - P...ender Pass - Version 3.2.1
    job-725b3675bdaa45868501f421caeca9be  SUCCEEDED     2026-01-23 17:03:55 -0800
    Tasks: 20 succeeded

  ...

  ... and 5 more

To get details, run: deadline job get --job-id <job-id>
```

- Job names up to 80 characters, truncated in the middle with `...` if longer
- Each job displays on multiple lines: name, job ID/status/timestamp, task summary
- Task summary shows counts for running, ready, pending, succeeded, failed, canceled
- Timestamps converted to local timezone with UTC offset
- Limited to 5 most recent matches

**No results:**
```
No jobs found matching "nonexistent term"
```

## Implementation Details

**Files modified:**
- `src/deadline/client/cli/_groups/job_group.py` - Modified `job_get` command
- `src/deadline/client/cli/_groups/_job_helpers.py` - New helper module

**Key functions:**
- `_resolve_job_search(config, search_term)` - Searches jobs via `SearchJobs` API, returns job ID if single match, prints summary if multiple matches
- `_print_job_details(config, job_id)` - Fetches and prints full job details via `GetJob` API
- `_format_timestamp(dt)` - Converts datetime to local timezone with offset
- `_truncate_middle(text, max_length)` - Truncates text in the middle with `...`
- `_format_task_summary(task_counts)` - Formats task status counts into brief summary

**API usage:**
- `SearchJobs` with `searchTermFilter` using `matchType: CONTAINS` for case-insensitive substring matching
- Results sorted by `CREATED_AT` descending, limited to 5 results
- `GetJob` for full job details when single result or job ID provided

**Config integration:**
- Uses `_apply_cli_options_to_config` with `required_options={"job_id"}` when not searching, enabling fallback to `defaults.job_id` from config

## Testing

Unit tests are in `test/unit/deadline_client/cli/test_cli_job_get_with_search_term.py`.

**Test cases:**
- `test_search_single_result_shows_full_details` - Single match calls `GetJob` and shows full details
- `test_search_multiple_results_shows_summary` - Multiple matches show summary table, no `GetJob` call
- `test_search_no_results` - No matches show "No jobs found" message
- `test_job_id_as_positional_arg` - Job ID pattern bypasses search, calls `GetJob` directly
- `test_job_id_with_flag` - `--job-id` flag calls `GetJob` directly
- `test_no_args_uses_default_job_id` - No args uses `defaults.job_id` from config
- `test_no_args_no_default_job_id_shows_error` - No args and no default shows error

## Future Iterations

1. **Interactive selection**: Prompt user to select from multiple results
2. **Extend to other commands**: `deadline job cancel`, `deadline job logs`, etc.
3. **Fuzzy match option**: Add `--fuzzy` flag for broader matching
