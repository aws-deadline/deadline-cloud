# Deadline Job TUI - User Guide

An interactive terminal UI for browsing jobs, steps, tasks, sessions, and attachments in AWS Deadline Cloud.

## Quick Start

```bash
# Interactive job/step/task browser
deadline job tui

# Direct job attachment browser (browse files of a specific job)
deadline browse --job-id job-abc123def456
```

## Job List Screen

When you run `deadline job tui`, you see a paginated list of recent jobs. The page size adapts to your terminal height.

```
╭─────────────────── Deadline Job TUI ───────────────────────╮
│ 🎬 Jobs                                     Queue: queue-xx│
╰────────────────────────────────────────────────────────────╯

▶  ✓ SUCCEEDED  My Render Job              2m ago   ...abc123
   ● RUNNING    Animation Batch           10m ago   ...def456
   ✗ FAILED     Test Job                   1h ago   ...ghi789
```

### Controls
| Key | Action |
|-----|--------|
| ↑/↓ | Move selection up/down |
| →/Enter | Open step list for selected job |
| J | Browse job attachments |
| c | Copy full job ID to clipboard |
| n/p | Next/previous page |
| r | Refresh |
| q | Quit |

## Step List Screen

After selecting a job, browse its steps:

```
╭─────────────────── Deadline Job TUI ───────────────────────╮
│ 🎬 My Render Job                            ✓ SUCCEEDED    │
╰────────────────────────────────────────────────────────────╯
📍 Steps

▶  ✓ SUCCEEDED  Render Frames         120 tasks   step-aaa111
   ✓ SUCCEEDED  Composite             1 task      step-bbb222
   ✗ FAILED     Cleanup               3 tasks     step-ccc333
```

### Controls
| Key | Action |
|-----|--------|
| ↑/↓ | Move selection up/down |
| →/Enter | Open task list for selected step |
| ←/Esc | Back to job list |
| c | Copy full step ID to clipboard |
| n/p | Next/previous page |
| r | Refresh |
| q | Quit |

## Task List Screen

After selecting a step, browse its tasks:

```
╭─────────────────── Deadline Job TUI ───────────────────────╮
│ 🎬 My Render Job › Render Frames            ✓ SUCCEEDED    │
╰────────────────────────────────────────────────────────────╯
📍 Tasks

▶  ✓ SUCCEEDED  Frame=1                              task-001
   ✓ SUCCEEDED  Frame=2                              task-002
   ✗ FAILED     Frame=3                              task-003
```

### Controls
| Key | Action |
|-----|--------|
| ↑/↓ | Move selection up/down |
| ←/Esc | Back to step list |
| l | List sessions for selected task |
| j | Browse task attachments |
| c | Copy full task ID to clipboard |
| n/p | Next/previous page |
| r | Refresh |
| q | Quit |

## Session List

Press `l` on a task to see its sessions:

```
╭─────────────────── Deadline Job TUI ───────────────────────╮
│ 📋 Sessions for Task Frame=3                               │
╰────────────────────────────────────────────────────────────╯

▶  ● ENDED   session-aaa   worker-xxx   2m ago
   ● ENDED   session-bbb   worker-yyy   5m ago
```

### Controls
| Key | Action |
|-----|--------|
| ↑/↓ | Move selection |
| Enter | Show session details |
| Esc | Back to task list |
| q | Quit |

## Attachment Browser

Press `J` on a job or `j` on a task to browse input/output file attachments:

```
╭─────────────────── Deadline Job TUI ───────────────────────╮
│ 🎬 My Render Job                            ✓ SUCCEEDED    │
╰────────────────────────────────────────────────────────────╯
📍 Job › output › /renders

▶  💾 /renders/frames                              120 files
   📁 textures                                      45 files
   📄 scene.blend                                  2.4 MB
   🖼️  preview.png                                 156 KB
```

### Controls
| Key | Action |
|-----|--------|
| ↑/↓ | Move selection |
| ←/→/Enter | Navigate file tree |
| d | Download file or folder |
| i | Show file info |
| v | Open image in system viewer |
| m | List all manifests |
| Esc | Back to job/task list |
| q | Quit |

### Downloading Files

Press `d` on any file or folder:

```
Download to: /home/user/downloads
Downloading... ━━━━━━━━━━━━━━━━━━━━ 100%
✅ Downloaded 120 files to /home/user/downloads
```

## File Icons

| Icon | Type |
|------|------|
| 📦 | Category (input/output) |
| 💾 | Manifest root path |
| 📁 | Folder |
| 📄 | File |
| 🖼️ | Image file |
| 📝 | Text/log file |

## Options

```bash
# Job TUI (hierarchical browser)
deadline job tui [OPTIONS]

Options:
  --farm-id TEXT   Override default farm
  --queue-id TEXT  Override default queue
  --profile TEXT   AWS profile to use

# Direct attachment browser (existing)
deadline browse [OPTIONS]

Options:
  --job-id TEXT    Job ID to browse directly
  --farm-id TEXT   Override default farm
  --queue-id TEXT  Override default queue
  --profile TEXT   AWS profile to use
```

## Examples

```bash
# Browse jobs interactively, drill into steps and tasks
deadline job tui

# Use a specific farm/queue
deadline job tui --farm-id farm-xxx --queue-id queue-yyy

# Directly browse a specific job's attachments
deadline browse --job-id job-f2bb8e71194c418db5a57b45590638d5
```

## Requirements

- Interactive terminal (TTY)
- AWS credentials configured
- Default farm and queue set in Deadline config (or provided via options)
