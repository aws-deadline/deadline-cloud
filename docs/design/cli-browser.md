# CLI Job TUI Design

## Overview

Interactive TUI (Terminal User Interface) for browsing Deadline Cloud jobs, steps, tasks, sessions, and job attachments. Uses the `rich` library for terminal rendering. Provides two entry points:

1. `deadline job tui` — hierarchical browser: jobs → steps → tasks, with session listing and attachment browsing
2. `deadline browse --job-id <id>` — direct job attachment browser (existing, unchanged)

## Commands

```bash
# Hierarchical job/step/task browser
deadline job tui [--farm-id] [--queue-id] [--profile]

# Direct job attachment browser (existing entry point, kept as-is)
deadline browse [--job-id <job-id>] [--farm-id] [--queue-id] [--profile]
```

## Navigation Model

```
deadline job tui
    │
    ▼
┌─────────┐  Enter   ┌──────────┐  Enter   ┌──────────┐
│ Job List │ ───────► │ Step List│ ───────► │ Task List│
└─────────┘  Esc ◄── └──────────┘  Esc ◄── └──────────┘
    │                                           │  │
    │ a (job attachments)                  l    │  │ a (task attachments)
    ▼                                      ▼    │  ▼
┌──────────────────┐               ┌──────────┐│ ┌──────────────────┐
│ Job Attachment    │               │ Session  ││ │ Task Attachment   │
│ Browser (in/out) │               │ List     ││ │ Browser (in/out)  │
└──────────────────┘               └──────────┘│ └──────────────────┘
    Esc back to Job List                       │     Esc back to Task List
                                               │
                                          Esc back to Task List
```

## Pagination

All list screens (jobs, steps, tasks) use the same pagination strategy:

1. On launch, detect terminal height and compute `page_size = terminal_lines - chrome_lines` (header + help bar + padding, ~8 lines)
2. Fetch exactly `page_size` items per page using the search/list API
3. `n`/`p` keys move between pages, each page triggers a fresh API call
4. Cursor wraps within the current page only
5. ←/→ arrows navigate the hierarchy (→ goes deeper, ← goes back)

For jobs, use `search_jobs` with `pageSize` and `itemOffset`.
For steps, use `list_steps` with `maxResults` and `nextToken`.
For tasks, use `list_tasks` with `maxResults` and `nextToken`.

## UI Screens

### Job List Screen

Jobs have two status fields:
- `taskRunStatus` — aggregate task run status (PENDING, READY, ASSIGNED, STARTING, SCHEDULED, INTERRUPTING, RUNNING, SUSPENDED, CANCELED, FAILED, SUCCEEDED, NOT_COMPATIBLE)
- `targetTaskRunStatus` — the target status the job is transitioning to (READY, FAILED, SUCCEEDED, CANCELED, SUSPENDED, PENDING). Only shown when set and differs from `taskRunStatus`.

The TUI shows `taskRunStatus` as the primary indicator. When `targetTaskRunStatus` is set and differs from `taskRunStatus`, it's shown as a secondary arrow badge (e.g. `→CANCELED`).

```
╭─────────────────── Deadline Job TUI ───────────────────────╮
│ 🎬 Jobs                                     Queue: queue-xx│
╰────────────────────────────────────────────────────────────╯

▶  ✓ SUCCEEDED  My Render Job              2m ago   job-abc123
   ● RUNNING    Animation Batch →CANCELED 10m ago   job-def456
   ✗ FAILED     Test Job                   1h ago   job-ghi789
   ✓ SUCCEEDED  Scene Export              2h ago    job-jkl012

                                        Page 1/5 (1-15 of 73)

╭────────────────────────────────────────────────────────────╮
│ ↑↓ nav  →/Enter steps  J attachments  n/p page  r refresh  q quit │
╰────────────────────────────────────────────────────────────╯
```

| Key | Action |
|-----|--------|
| ↑/↓ | Move cursor |
| →/Enter | Open step list for selected job |
| a | Browse job attachments (input + output) |
| c | Copy full job ID to clipboard |
| n/p | Next/previous page |
| r | Refresh current page |
| q | Quit TUI |

### Step List Screen

Steps have three status fields:
- `taskRunStatus` — aggregate task run status (same values as job: PENDING, READY, ASSIGNED, STARTING, SCHEDULED, INTERRUPTING, RUNNING, SUSPENDED, CANCELED, FAILED, SUCCEEDED, NOT_COMPATIBLE)
- `targetTaskRunStatus` — the target status the step is transitioning to (READY, FAILED, SUCCEEDED, CANCELED, SUSPENDED, PENDING). Only shown when set and differs from `taskRunStatus`.
- `lifecycleStatus` — step lifecycle (CREATE_COMPLETE, UPDATE_IN_PROGRESS, UPDATE_FAILED, UPDATE_SUCCEEDED)

The TUI shows `taskRunStatus` as the primary indicator. When `targetTaskRunStatus` is set and differs from `taskRunStatus`, it's shown as an arrow badge (e.g. `→CANCELED`). When `lifecycleStatus` is not CREATE_COMPLETE, it's shown as a secondary bracket badge.

```
╭─────────────────── Deadline Job TUI ───────────────────────╮
│ 🎬 My Render Job                            ✓ SUCCEEDED    │
╰────────────────────────────────────────────────────────────╯
📍 Steps

▶  ✓ SUCCEEDED  Render Frames         120 tasks   step-aaa111
   ● RUNNING    Composite →CANCELED    1 task      step-bbb222
   ✗ FAILED     Cleanup  [UPD_FAIL]   3 tasks     step-ccc333

IDs are displayed as short hashes (e.g. `step-aaa111`) like git short hashes, truncated from the full 32-char hex ID.

                                        Page 1/1 (1-3 of 3)

╭────────────────────────────────────────────────────────────╮
│ ↑↓ nav  →/Enter tasks  ←/Esc back  n/p page  r refresh  q quit │
╰────────────────────────────────────────────────────────────╯
```

Lifecycle status styling:
| lifecycleStatus | Badge | Color |
|-----------------|-------|-------|
| CREATE_COMPLETE | (hidden) | — |
| UPDATE_IN_PROGRESS | [UPDATING] | yellow |
| UPDATE_FAILED | [UPD_FAIL] | red |
| UPDATE_SUCCEEDED | [UPDATED] | green |

| Key | Action |
|-----|--------|
| ↑/↓ | Move cursor |
| →/Enter | Open task list for selected step |
| ←/Esc | Back to job list |
| c | Copy full step ID to clipboard |
| n/p | Next/previous page |
| r | Refresh |
| q | Quit |

### Task List Screen

Tasks have two status fields:
- `runStatus` — current task status (PENDING, READY, ASSIGNED, STARTING, SCHEDULED, INTERRUPTING, RUNNING, SUSPENDED, CANCELED, FAILED, SUCCEEDED, NOT_COMPATIBLE)
- `targetRunStatus` — the target status the task is transitioning to (READY, FAILED, SUCCEEDED, CANCELED, SUSPENDED, PENDING). Only shown when set and differs from `runStatus`.

The TUI shows `runStatus` as the primary indicator. When `targetRunStatus` is set and differs from `runStatus`, it's shown as a secondary arrow badge (e.g. `→CANCELED`).

```
╭─────────────────── Deadline Job TUI ───────────────────────╮
│ 🎬 My Render Job › Render Frames            ✓ SUCCEEDED    │
╰────────────────────────────────────────────────────────────╯
📍 Tasks

▶  ✓ SUCCEEDED  Frame=1                              task-001
   ✓ SUCCEEDED  Frame=2                              task-002
   ✗ FAILED     Frame=3                              task-003
   ● RUNNING    Frame=4  →CANCELED                   task-004

                                      Page 1/8 (1-15 of 120)

╭────────────────────────────────────────────────────────────╮
│ ↑↓ nav  ←/Esc back  l sessions  j attachments  n/p page  q quit │
╰────────────────────────────────────────────────────────────╯
```

| Key | Action |
|-----|--------|
| ↑/↓ | Move cursor |
| ←/Esc | Back to step list |
| l | List sessions for selected task |
| a | Browse task attachments (input + output) |
| c | Copy full task ID to clipboard |
| n/p | Next/previous page |
| r | Refresh |
| q | Quit |

### Session List Screen

Shown as an overlay/sub-screen when pressing `l` on a task. Lists all sessions
that ran this task by filtering `list_session_actions` for the task's step+task,
then resolving unique session IDs.

```
╭─────────────────── Deadline Job TUI ───────────────────────╮
│ 📋 Sessions for Task Frame=3                               │
╰────────────────────────────────────────────────────────────╯

▶  ● ENDED   session-aaa   worker-xxx   2m ago
   ● ENDED   session-bbb   worker-yyy   5m ago

╭────────────────────────────────────────────────────────────╮
│ ↑↓ nav  Enter details  Esc back  q quit                    │
╰────────────────────────────────────────────────────────────╯
```

| Key | Action |
|-----|--------|
| ↑/↓ | Move cursor |
| Enter | Show session details (status, worker, timestamps) |
| Esc | Back to task list |
| q | Quit |

### Attachment Browser Screen

Reuses the existing `JobBrowserTUI` file tree for manifest browsing. Entered via:
- `a` from job list → shows all job-level input + output attachments
- `a` from task list → shows task-level input + output attachments (passes step_id + task_id for scoped output manifests)

```
╭─────────────────── Deadline Job TUI ───────────────────────╮
│ 🎬 My Render Job                            ✓ SUCCEEDED    │
╰────────────────────────────────────────────────────────────╯
📍 Job › output › /renders

▶  💾 /renders/frames                              120 files
   📁 textures                                      45 files
   📄 scene.blend                                  2.4 MB
   🖼️  preview.png                                 156 KB

╭────────────────────────────────────────────────────────────╮
│ ←→↑↓ nav  Enter open  d download  i info  v view  Esc back  q quit │
╰────────────────────────────────────────────────────────────╯
```

| Key | Action |
|-----|--------|
| ↑/↓ | Move cursor |
| ←/→/Enter | Navigate tree |
| d | Download file or folder |
| i | File info |
| v | Open image in system viewer |
| m | List all manifests |
| Esc | Back to job/step/task list |
| q | Quit |

## Data Flow

### Job List
```
1. Detect terminal height, compute page_size
2. search_jobs(farmId, queueIds, pageSize, itemOffset, sortBy=CREATED_AT DESC)
3. Render page, wait for input
4. n/p pages through results, re-fetches
```

### Step List
```
1. list_steps(farmId, queueId, jobId, maxResults=page_size)
2. Paginate with nextToken on n/p
```

### Task List
```
1. list_tasks(farmId, queueId, jobId, stepId, maxResults=page_size)
2. Paginate with nextToken on n/p
3. Display task parameters as summary (e.g. "Frame=1")
```

### Session List (for a task)
```
1. list_session_actions(farmId, queueId, jobId, sessionId=...) 
   — OR filter sessions by iterating list_sessions and matching task actions
2. For each session: get_session() for details (worker, status, timestamps)
```

### Job Attachments (J from job list)
```
1. get_job() for attachments metadata
2. get_queue() for jobAttachmentSettings → S3Settings
3. get_queue_user_boto3_session() for role session
4. load_input_manifests() from job.attachments.manifests[].inputManifestPath
5. load_output_manifests() via get_output_manifests_by_asset_root()
6. Merge manifests, build tree, launch attachment browser
7. Esc returns to job list
```

### Task Attachments (j from task list)
```
1. Same as job attachments but scoped:
   - Input manifests: same as job-level (tasks share job inputs)
   - Output manifests: get_output_manifests_by_asset_root() with step_id + task_id
     to scope to that task's outputs only
2. Esc returns to task list
```

## Icons

| Type | Icon |
|------|------|
| Category (input/output) | 📦 |
| Manifest Root | 💾 |
| Folder | 📁 |
| File | 📄 |
| Image | 🖼️ |
| Text/Log | 📝 |
| Script | 📜 |

## Dependencies

- `rich` — terminal rendering (already in project dependencies)

## Implementation

### File Structure

- `src/deadline/client/cli/_groups/job_group.py` — add `tui` subcommand to existing `cli_job` group
- `src/deadline/client/cli/_groups/browse_group.py` — keep existing `deadline browse` entry point unchanged

The TUI classes live in a new module:
```
src/deadline/client/cli/_groups/_job_tui/
├── __init__.py
├── _common.py          # Shared rendering utils (render_header, render_help_bar, format_size, etc.)
├── _job_list.py        # JobListTUI — paginated job browser
├── _step_list.py       # StepListTUI — paginated step browser
├── _task_list.py       # TaskListTUI — paginated task browser
├── _session_list.py    # SessionListTUI — session list for a task
└── _attachment_browser.py  # AttachmentBrowserTUI — file tree browser (refactored from browse_group)
```

### Registration

Add to `job_group.py`:
```python
@cli_job.command(name="tui")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The farm to use.")
@click.option("--queue-id", help="The queue to use.")
@_handle_error
def job_tui(**args):
    """
    Interactive TUI for browsing jobs, steps, tasks, sessions, and attachments.
    """
    ...
```

### Pseudo Code

```python
# === _common.py ===

console = Console()

def get_terminal_page_size(chrome_lines: int = 8) -> int:
    """Compute how many list items fit on screen."""
    return max(5, console.height - chrome_lines)

def render_header(title: str, subtitle: str = "") -> None: ...
def render_help_bar(keys: list[tuple[str, str]]) -> None: ...
def format_size(size: int) -> str: ...
def format_time_ago(dt: datetime) -> str: ...
def get_status_style(status: str) -> tuple[str, str]:
    """
    Return (color, icon) for all taskRunStatus values:
      SUCCEEDED  → green,  ✓
      RUNNING    → yellow, ●
      STARTING   → yellow, ●
      SCHEDULED  → yellow, ●
      ASSIGNED   → yellow, ●
      PENDING    → blue,   ○
      READY      → cyan,   ◉
      INTERRUPTING → yellow, ⚡
      SUSPENDED  → magenta, ⏸
      CANCELED   → red,    ✗
      FAILED     → red,    ✗
      NOT_COMPATIBLE → red, ⚠
      unknown    → dim,    ○
    """

def read_key() -> str:
    """Read a single keypress, returning normalized key name."""


# === _job_list.py ===

class JobListTUI:
    def __init__(self, farm_id: str, queue_id: str, deadline_client, config):
        self.page_size = get_terminal_page_size()
        ...

    def load_page(self) -> None:
        """Fetch one page of jobs via search_jobs(pageSize=self.page_size, itemOffset=...)."""

    def render(self) -> None:
        """
        Render job list with:
        - taskRunStatus icon+color (primary)
        - targetTaskRunStatus arrow badge when set and differs from taskRunStatus (e.g. →CANCELED)
        - name, time ago, short job ID
        """

    def run(self) -> Optional[tuple[str, str]]:
        """
        Main loop. Returns:
        - ("select", job_id) when Enter pressed
        - ("attachments", job_id) when J pressed
        - None when q pressed
        """


# === _step_list.py ===

class StepListTUI:
    def __init__(self, farm_id: str, queue_id: str, job_id: str, job_name: str,
                 job_status: str, deadline_client):
        self.page_size = get_terminal_page_size()
        ...

    def load_page(self) -> None:
        """Fetch one page of steps via list_steps(maxResults=self.page_size, nextToken=...)."""

    def render(self) -> None:
        """
        Render step list with:
        - taskRunStatus icon+color (primary)
        - targetTaskRunStatus arrow badge when set and differs (e.g. →CANCELED)
        - lifecycleStatus badge when not CREATE_COMPLETE (secondary)
        - name, task count, step ID
        """

    def run(self) -> Optional[tuple[str, str]]:
        """
        Returns:
        - ("select", step_id) when Enter pressed
        - None/("back", "") when Esc pressed
        """


# === _task_list.py ===

class TaskListTUI:
    def __init__(self, farm_id: str, queue_id: str, job_id: str, job_name: str,
                 step_id: str, step_name: str, deadline_client):
        self.page_size = get_terminal_page_size()
        ...

    def load_page(self) -> None:
        """Fetch one page of tasks via list_tasks(maxResults=self.page_size, nextToken=...)."""

    def render(self) -> None:
        """
        Render task list with:
        - runStatus icon+color (primary, uses same get_status_style as jobs)
        - targetRunStatus arrow badge when set and differs from runStatus (e.g. →CANCELED)
        - parameter summary (e.g. "Frame=1"), task ID
        """

    def run(self) -> Optional[tuple[str, str]]:
        """
        Returns:
        - ("sessions", task_id) when l pressed
        - ("attachments", task_id) when j pressed
        - None/("back", "") when Esc pressed
        """


# === _session_list.py ===

class SessionListTUI:
    def __init__(self, farm_id: str, queue_id: str, job_id: str,
                 step_id: str, task_id: str, deadline_client):
        ...

    def load_sessions(self) -> None:
        """
        Find sessions for this task:
        1. list_sessions(farmId, queueId, jobId)
        2. For each session, list_session_actions and filter for matching stepId+taskId
        3. Collect unique sessions
        """

    def render(self) -> None:
        """Render session list with status, session ID, worker ID, time."""

    def run(self) -> None:
        """Browse sessions. Enter shows detail panel. Esc returns."""


# === _attachment_browser.py ===

class AttachmentBrowserTUI:
    """
    Refactored from existing JobBrowserTUI in browse_group.py.
    Adds Esc-to-return behavior and optional step_id/task_id scoping.
    """
    def __init__(self, farm_id: str, queue_id: str, job_id: str,
                 job_name: str, job_status: str,
                 boto3_session, queue_role_session, s3_settings,
                 step_id: Optional[str] = None,
                 task_id: Optional[str] = None):
        ...

    def load_manifests(self) -> None:
        """
        Load input manifests (always job-level).
        Load output manifests:
        - If step_id/task_id provided: scope to task outputs
        - Otherwise: all job outputs
        """

    def run(self) -> None:
        """File tree browser. Esc returns to caller."""


# === Entry point in job_group.py ===

@cli_job.command(name="tui")
def job_tui(**args):
    """
    1. Build config, get farm_id/queue_id
    2. Create deadline client
    3. Loop:
       a. Show JobListTUI
       b. On ("select", job_id): show StepListTUI
          - On ("select", step_id): show TaskListTUI
            - On ("sessions", task_id): show SessionListTUI, then return to TaskListTUI
            - On ("attachments", task_id): show AttachmentBrowserTUI(step_id, task_id), then return
            - On Esc: return to StepListTUI
          - On Esc: return to JobListTUI
       c. On ("attachments", job_id): show AttachmentBrowserTUI(job-level), then return to JobListTUI
       d. On quit: exit
    """
```

## Screen Rendering & Clearing

The TUI uses a two-mode screen clearing strategy to balance flicker-free rendering with clean screen transitions.

### The Problem

Terminal UIs face a tradeoff: erasing the entire screen before each frame prevents stale content but causes visible flicker. Only repositioning the cursor and overwriting in-place eliminates flicker but leaves stale lines when transitioning between screens of different lengths (e.g. a 50-job list → a 3-step list leaves 47 ghost lines).

### Two-Mode Approach

`clear_screen(full)` in `_common.py` implements both modes:

- **Soft clear** (`full=False`): Writes `\033[H` (cursor home) only. Content overwrites in-place with zero flicker. Used for same-screen re-renders (scrolling, cursor movement, page changes within the same list).
- **Hard clear** (`full=True`): Writes `\033[H\033[2J` (cursor home + erase entire screen). Used when transitioning between different screens (job list → step list, step list → task list, back navigation) to prevent stale content from the previous screen bleeding through.

### The `_needs_full_clear` Flag

Each TUI class (`JobListTUI`, `StepListTUI`, `TaskListTUI`, `SessionListTUI`, `AttachmentBrowserTUI`) manages a `_needs_full_clear: bool` instance flag:

1. Set to `True` in `__init__()` (initial construction).
2. Set to `True` at the top of `run()` (re-entering the screen after returning from a child screen).
3. Passed to `clear_screen(full=self._needs_full_clear)` at the start of `render()`.
4. Immediately set to `False` after the `clear_screen()` call in `render()`, so subsequent frames within the same screen use soft clear.

```python
# Pattern used in every TUI class:
def render(self) -> None:
    clear_screen(full=self._needs_full_clear)
    self._needs_full_clear = False
    # ... render content ...

def run(self) -> ...:
    self._needs_full_clear = True  # hard clear on first frame
    self.load_page()
    while True:
        self.render()  # first call uses hard clear, rest use soft clear
        key = read_key()
        # ... handle input ...
```

### Attachment Browser Folder Navigation

The `AttachmentBrowserTUI` applies the same two-mode clearing to folder transitions within the file tree. When the user navigates into a subfolder (`→`/`Enter` on a non-file node) or back out (`←` to parent), `_needs_full_clear` is set to `True` before the next render. This prevents stale file listings from a longer folder bleeding through when entering a shorter folder — the same class of bug that screen transitions between TUI screens solve.

Scrolling within the same folder (↑/↓) continues to use soft clear for flicker-free rendering.

### Erase-Below in Help Bar

`render_help_bar()` writes `\033[J` (erase from cursor to end of screen) after rendering the help panel. This cleans up leftover lines from a previous longer frame without requiring a full-screen erase — for example, when the current page has fewer items than the previous page on the same screen.

### Terminal Width Constraint

All `Panel` and `Table` widgets are constrained to `width=console.width - 1` to prevent terminal line wrapping, which would cause rendering artifacts and misaligned content.

### Cursor Hiding & Alternate Screen Buffer

- `enter_alt_screen()` switches to the terminal's alternate screen buffer and hides the cursor (`\033[?1049h\033[?25l`). This preserves the user's scrollback history and prevents a blinking cursor from distracting during TUI operation.
- `leave_alt_screen()` restores the cursor and returns to the main screen buffer (`\033[?25h\033[?1049l`).

These are called by the top-level orchestrator in `job_group.py` around the entire TUI session, not by individual screens.

### Function Size Limits

All functions must be ≤75 lines. Complex logic should be split:
- `load_page()` handles API call only
- `render()` delegates to `render_header()` + list table + `render_help_bar()`
- `run()` delegates to `render()` + `read_key()` + action dispatch

### Shared Code with `deadline browse`

The existing `browse_group.py` keeps its `cli_browse` entry point and `JobBrowserTUI` unchanged.
The new `_attachment_browser.py` refactors the tree/manifest/download logic into a reusable module
that both `browse_group.py` and `job tui` can import. Common utilities (icons, formatting, rendering)
move to `_common.py`.

## Implementation Plan (Sub-Agent Tasks)

The implementation is split into 5 sequential tasks. Each task builds on the previous one.
Run them in order — later tasks depend on earlier ones being complete.

### Task 1: Common utilities and key input (`_common.py`)

Create `src/deadline/client/cli/_groups/_job_tui/__init__.py` and `_common.py`.

Extract and refactor shared utilities from `browse_group.py` into `_common.py`:
- `console` — shared Rich Console instance
- `get_terminal_page_size(chrome_lines=8) -> int` — compute page size from terminal height
- `get_status_style(status) -> tuple[str, str]` — color+icon for all 12 taskRunStatus values
- `get_lifecycle_badge(status) -> Optional[tuple[str, str]]` — badge+color for step lifecycleStatus
- `format_size(size) -> str` — bytes to human readable
- `format_time_ago(dt) -> str` — datetime to relative time
- `format_short_id(full_id) -> str` — truncate `xxx-<32hex>` to `xxx-<6hex>` like git short hash
- `copy_to_clipboard(text) -> bool` — copy to clipboard using `pbcopy` (macOS), `xclip` (Linux), `clip` (Windows)
- `render_header(title, subtitle)` — Rich Panel header
- `render_help_bar(keys)` — Rich Panel help bar
- `read_key() -> str` — read single keypress, normalize arrow keys to `"up"`, `"down"`, `"left"`, `"right"`, escape to `"esc"`, etc.

Update `browse_group.py` to import from `_common.py` instead of defining its own copies.
Add unit tests for all pure functions (format_size, format_time_ago, get_status_style, format_short_id, get_lifecycle_badge).

Files created/modified:
- `src/deadline/client/cli/_groups/_job_tui/__init__.py` (new)
- `src/deadline/client/cli/_groups/_job_tui/_common.py` (new)
- `src/deadline/client/cli/_groups/browse_group.py` (modified — import from _common)
- `test/unit/deadline_client/cli/groups/job_tui/test_common.py` (new)

### Task 2: Job list TUI (`_job_list.py`)

Create `_job_list.py` with `JobListTUI` class:
- `__init__(farm_id, queue_id, deadline_client, config)` — store params, compute page_size
- `load_page()` — call `search_jobs(pageSize=page_size, itemOffset=page*page_size, sortBy=CREATED_AT DESC)`, store jobs + total count
- `render()` — clear screen, render_header, render job table (status icon, target badge, name, time_ago, short ID), pagination info, render_help_bar
- `run() -> Optional[tuple[str, str]]` — main loop: render, read_key, dispatch:
  - ↑/↓ move cursor
  - →/Enter returns `("select", job_id)`
  - `a` returns `("attachments", job_id)`
  - `c` copies full job ID to clipboard
  - `n`/`p` next/prev page
  - `r` refresh
  - `q` returns None

Add unit tests mocking the deadline client to verify page loading, cursor movement, and return values.

Files created:
- `src/deadline/client/cli/_groups/_job_tui/_job_list.py` (new)
- `test/unit/deadline_client/cli/groups/job_tui/test_job_list.py` (new)

### Task 3: Step list and task list TUIs (`_step_list.py`, `_task_list.py`)

Create `_step_list.py` with `StepListTUI` class:
- `__init__(farm_id, queue_id, job_id, job_name, job_status, deadline_client)`
- `load_page()` — call `list_steps(maxResults=page_size, nextToken=...)`, store steps + tokens
- `render()` — header with job name+status, step table (taskRunStatus icon, target badge, lifecycle badge, name, task count, short ID), pagination, help bar
- `run() -> Optional[tuple[str, str]]` — →/Enter returns `("select", step_id)`, ←/Esc returns `("back", "")`, `c` copies step ID, `n`/`p` pages, `r` refresh, `q` quit

Create `_task_list.py` with `TaskListTUI` class:
- `__init__(farm_id, queue_id, job_id, job_name, step_id, step_name, deadline_client)`
- `load_page()` — call `list_tasks(maxResults=page_size, nextToken=...)`, store tasks + tokens
- `render()` — header with job›step name, task table (runStatus icon, target badge, parameter summary, short ID), pagination, help bar
- `run() -> Optional[tuple[str, str]]` — `l` returns `("sessions", task_id)`, `a` returns `("attachments", task_id)`, ←/Esc returns `("back", "")`, `c` copies task ID, `n`/`p` pages, `r` refresh, `q` quit

Add unit tests for both classes.

Files created:
- `src/deadline/client/cli/_groups/_job_tui/_step_list.py` (new)
- `src/deadline/client/cli/_groups/_job_tui/_task_list.py` (new)
- `test/unit/deadline_client/cli/groups/job_tui/test_step_list.py` (new)
- `test/unit/deadline_client/cli/groups/job_tui/test_task_list.py` (new)

### Task 4: Session list and attachment browser (`_session_list.py`, `_attachment_browser.py`)

Create `_session_list.py` with `SessionListTUI` class:
- `__init__(farm_id, queue_id, job_id, step_id, task_id, deadline_client)`
- `load_sessions()` — list_sessions for the job, then for each session list_session_actions filtering for matching step_id+task_id, collect unique sessions with details
- `render()` — header with task info, session table (status, session ID, worker ID, time)
- `run()` — ↑/↓ nav, Enter shows session detail panel, Esc returns to task list, `q` quit

Create `_attachment_browser.py` with `AttachmentBrowserTUI` class:
- Refactor tree/manifest/download logic from `browse_group.py` into this module
- `__init__(...)` — same as existing JobBrowserTUI but adds optional `step_id`/`task_id` params
- `load_manifests()` — load input manifests (job-level), load output manifests (scoped to task if step_id/task_id provided)
- `run()` — same file tree navigation as existing, but Esc returns to caller instead of quitting

Update `browse_group.py` to use `AttachmentBrowserTUI` from this module (delegate, don't duplicate).

Add unit tests for session list and attachment browser.

Files created/modified:
- `src/deadline/client/cli/_groups/_job_tui/_session_list.py` (new)
- `src/deadline/client/cli/_groups/_job_tui/_attachment_browser.py` (new)
- `src/deadline/client/cli/_groups/browse_group.py` (modified — delegate to _attachment_browser)
- `test/unit/deadline_client/cli/groups/job_tui/test_session_list.py` (new)
- `test/unit/deadline_client/cli/groups/job_tui/test_attachment_browser.py` (new)

### Task 5: Entry point and orchestration (`job_group.py` + wiring)

Add `deadline job tui` command to `job_group.py`:
- Register `@cli_job.command(name="tui")` with `--profile`, `--farm-id`, `--queue-id` options
- Build config, get farm_id/queue_id, create deadline client
- Implement the main orchestration loop:
  ```
  while True:
      result = JobListTUI(...).run()
      if result is None: break
      if result[0] == "select": enter step loop for result[1]
      if result[0] == "attachments": launch AttachmentBrowserTUI for result[1]
  ```
- Step loop: StepListTUI → on select enter task loop, on back return to job loop
- Task loop: TaskListTUI → on sessions launch SessionListTUI, on attachments launch AttachmentBrowserTUI(step_id, task_id), on back return to step loop
- Verify TTY check (`sys.stdin.isatty()`)

Run `hatch run fmt` and `hatch run lint` to ensure code passes formatting and linting.
Run `hatch run test --numprocesses=1 -k "job_tui"` to verify all new tests pass.

Files modified:
- `src/deadline/client/cli/_groups/job_group.py` (modified — add tui command)
