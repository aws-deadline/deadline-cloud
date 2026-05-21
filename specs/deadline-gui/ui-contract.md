# GUI UI Contract

This document defines the user-facing behavior of the AWS Deadline Cloud GUI.
It serves as the source of truth for what the Rust+QML implementation must do.

---

## Overview

The GUI consists of two main workflows:

1. **Configure** — The user sets up their workstation (which AWS profile,
   farm, and queue to use). Launched via `deadline config gui`.

2. **Submit** — The user submits a render job. They review settings,
   attachments, and parameters, then click Submit. Launched via
   `deadline bundle gui-submit` or from a DCC plugin (Maya, Blender, etc.).

Both workflows share an **auth status bar** that shows whether the user
is logged in and provides login/logout/profile-switch actions.

### User Journey: First-Time Setup

```
User installs Deadline Cloud CLI
  → Runs `deadline config gui`
  → Selects AWS profile from dropdown
  → Clicks Login → browser opens for SSO
  → After login: farm list populates
  → Selects farm → queue list populates
  → Selects queue → storage profile list populates
  → Clicks Apply → settings saved to ~/.deadline/config
  → Closes dialog
```

### User Journey: Submitting a Job

```
User runs `deadline bundle gui-submit --job-bundle-dir ./my-job`
  → Submit dialog opens with job name pre-filled from template
  → Queue parameters load automatically (async)
  → User adjusts parameters, adds attachments if needed
  → Clicks Submit
  → Progress dialog: hashing files → uploading → creating job
  → Success: shows job ID, user clicks OK
  → Dialog closes
```

### User Journey: DCC Plugin (Maya)

```
Artist opens Maya, loads scene
  → Clicks "Submit to Deadline" in Maya menu
  → Maya plugin introspects scene, builds settings
  → Submit dialog opens (same as above, but with Maya-specific tab)
  → Artist adjusts render settings, clicks Submit
  → Job submitted, dialog closes
  → Artist continues working in Maya
```

---

## 1. Config Dialog

**Trigger:** `deadline config gui` or "Settings..." button in Submit dialog

**Window:** "AWS Deadline Cloud workstation configuration" — scrollable form,
~650×850px

### Sections (top to bottom)

**Global Settings**
- AWS Profile dropdown — lists profiles from `~/.aws/config`. Changing this
  clears farm/queue and triggers a cascading reload.

**Profile Settings**
- Job history directory — text field + Browse button
- Default farm — dropdown, async-loaded from API after login

**Farm Settings**
- Default queue — dropdown, async-loaded when farm is selected
- Default storage profile — dropdown, filtered to current OS
- Job attachments filesystem — dropdown: "COPIED" or "VIRTUAL"

**General Settings**
- Auto accept prompt defaults — checkbox
- Telemetry opt out — checkbox
- Always check S3 job attachments — checkbox
- Show submitter update notifications — checkbox
- Conflict resolution — dropdown: CREATE_COPY / SKIP / OVERWRITE
- Logging level — dropdown: ERROR / WARNING / INFO / DEBUG
- Language — dropdown with 12 locales (change takes effect on next open)
- Known asset paths — list with Add/Edit/Remove buttons

**Auth Status Bar** (bottom)
- Shows login state, Login/Logout buttons

**Buttons:** OK (apply + close), Cancel (revert + close), Apply (save to disk)

### Key Behaviors

- **Cascading refresh:** Profile → Farm → Queue → Storage Profile. Each
  selection clears and reloads the next level.
- **Dirty tracking:** Changed settings get an orange border on their label.
  Apply button is only enabled when changes exist.
- **Async loading:** Farm/queue/storage profile dropdowns show loading state.
  If API is unavailable, they remain empty (no error dialog).

---

## 2. Submit Dialog

**Trigger:** `deadline bundle gui-submit` or DCC plugin

**Window:** "Submit to AWS Deadline Cloud" — tabbed, ~540×700px

### Tabs

1. **Shared job settings** — common to all job types
2. **Job-specific settings** — DCC-provided or bundle parameters
3. **Job attachments** — input/output files
4. **Host requirements** — (optional) hardware/capability constraints

### Shared Job Settings Tab

- Job name (text, max 128 chars)
- Description (text, max 2048 chars)
- Priority (spin box, 0-100, default 50)
- Initial state (dropdown: READY / SUSPENDED)
- Max failed tasks count (spin box)
- Max retries per task (spin box)
- Max worker count (radio: Unlimited or Limited to N)
- Farm/Queue display (read-only, shows names from config)
- Queue parameters (dynamic form, loaded async from API)

### Job Attachments Tab

Three sections (input files, input directories, output directories), each with:
- File/directory list
- "Show auto-detected" checkbox (toggles visibility of DCC-detected paths)
- Add / Remove buttons
- Status count ("N total")
- "Require all input paths exist" checkbox (top)

Auto-detected paths are shown in italics and cannot be removed.

### Host Requirements Tab

- Mode: "Run on any worker" (default, hides fields) or "Use custom"
- Hardware: CPU min/max, Memory min/max, GPU count/memory
- Custom amounts: dynamic list (name + min + max)
- Custom attributes: dynamic list (name + values + include/exclude)

### Dynamic Parameter Form

Parameters rendered based on type from API:

| Type | Widget |
|------|--------|
| STRING | Text field (with optional regex validation) |
| INT | Spin box with min/max |
| FLOAT | Double spin box with min/max |
| PATH | Text field + Browse button |
| Dropdown (allowedValues) | Combo box |

Parameters with `groupLabel` are grouped under collapsible sections.

### Buttons

- **Settings...** → opens Config dialog
- **Help** → opens Help dialog (version info, copy button)
- **Submit** → starts submission (disabled until auth + farm + queue valid)
- **Export bundle** → saves to disk without submitting
- **Load Bundle** → (only in browse mode) switch to different bundle

### Submit Button Rules

Disabled when ANY of: API unavailable, no farm, no queue, invalid parameters.
Tooltip explains why. Enter key does NOT trigger submit.

---

## 3. Progress Dialog

**Trigger:** Submit button clicked

**Window:** "AWS Deadline Cloud submission" — modal, ~600×700px

### Flow

```
"Preparing files..."
  → Hashing progress bar (0-100%, per-file messages)
  → [If large upload] Warning dialog: "Continue?" / "Do not ask again"
  → Upload progress bar (0-100%, per-file messages)
  → CreateJob API call
  → "Submission complete" + OK button
     OR "Submission canceled" + Close button
     OR "Submission error" + error in log + Close button
```

Cancel button available throughout. Closing the window = cancel.

---

## 4. Login Dialog

**Trigger:** Login button in auth status bar

Simple message box: "Logging you in..." → opens browser for SSO →
success closes dialog, failure shows error with Close button.

---

## 5. Help Dialog

**Trigger:** Help button in Submit dialog

Shows: documentation link, submitter name/version, host app, OS, Qt version.
Copy button puts all info on clipboard. Close button dismisses.

---

## 6. Warning/Confirmation Dialog

**Trigger:** During submission (large upload, etc.)

Scrollable message + OK/Cancel. Optional "Do not ask again" button
(sets `settings.auto_accept=true`). Default focus depends on context.

---

## 7. Update Available Dialog

**Trigger:** On submitter open if newer version detected

Shows: current version, available version, Download button (opens browser),
Dismiss button. If user clicks Download, submitter doesn't open.

---

## 8. DCC Plugin Differences

When opened from a DCC (Maya, Blender, etc.):
- Uses DCC's existing QApplication (no new one created)
- Window stays in front of DCC (`Qt.Tool` flag)
- Window title includes DCC name and version
- "Job-specific settings" tab is DCC-provided (render settings, etc.)
- Auto-detected attachments come from DCC scene introspection
- After successful submission, dialog auto-closes

---

## 9. Shared Behaviors

- **i18n:** All strings translatable. 12 locales supported.
- **File watching:** Auth status auto-refreshes when `~/.aws/` or `~/.deadline/` change.
- **Error handling:** API errors during list loading → empty list, debug log.
  API errors during submission → shown in progress log.
- **Window sizing:** Adapts to screen size (max 90% of available height).
