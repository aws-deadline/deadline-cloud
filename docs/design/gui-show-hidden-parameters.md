# Design: `--show-hidden-parameters` for GUI Job Submission

## Problem

Open Job Description templates can mark parameters with `userInterface.control: HIDDEN` to
exclude them from the submission UI. This is useful for parameters that are set programmatically,
have fixed values, or are generally not changed by users. However, during development and debugging,
there is no way to inspect or override these values through the GUI — you have to edit `parameter_values.yaml`
by hand and re-submit from the CLI.

## Solution

Add a `--show-hidden-parameters` flag to `deadline bundle gui-submit`. When set, every `HIDDEN` parameter
is rendered with the default widget for its type instead of being invisible. Users can see the
current value, modify it, and submit with the override — all within the normal GUI workflow.

```bash
deadline bundle gui-submit --show-hidden-parameters /path/to/bundle
```

Without the flag, behavior is unchanged.

## How It Works

### Data flow

The flag threads through the system as a boolean:

```
CLI (--show-hidden-parameters)
  → show_job_bundle_submitter(show_hidden_parameters=...)
    → JobBundleSettings.show_hidden_parameters
    → SubmitJobToDeadlineDialog(show_hidden_parameters=...)
      ├─ SharedJobSettingsWidget(show_hidden_parameters=...)     # queue environment parameters
      │    └─ OpenJDParametersWidget(show_hidden_parameters=...)
      └─ JobBundleSettingsWidget(initial_settings)    # job bundle parameters
           └─ OpenJDParametersWidget(show_hidden_parameters=...)
```

Both the job bundle parameters tab and the shared (queue environment) parameters tab receive
the flag, so hidden parameters from any source are revealed.

### Widget resolution

The core logic lives in `OpenJDParametersWidget.rebuild_ui`. For each parameter:

1. Resolve the control type via `get_ui_control_for_parameter_definition(parameter)`.
2. If the result is `HIDDEN` and `show_hidden_parameters` is true, create a shallow copy of the parameter
   dict without `userInterface` and re-resolve. This produces the default widget for the
   parameter's type (e.g. `LINE_EDIT` for `STRING`, `SPIN_BOX` for `INT`).
3. Construct the widget with the **original** parameter dict so that all constraints
   (`allowedValues`, `minValue`, `maxValue`, `decimals`, etc.) are preserved.
4. Mark the widget with an info icon (ℹ️) so the user can tell it is normally hidden.

The widget mapping when revealed:

| Parameter type | Has `allowedValues`? | Widget |
|---|---|---|
| `STRING` | No | `LINE_EDIT` |
| `INT` | No | `SPIN_BOX` (integer) |
| `FLOAT` | No | `SPIN_BOX` (float) |
| `PATH` (directory) | No | `CHOOSE_DIRECTORY` |
| `PATH` (file, input) | No | `CHOOSE_INPUT_FILE` |
| `PATH` (file, output) | No | `CHOOSE_OUTPUT_FILE` |
| Any type | Yes | `DROPDOWN_LIST` |

Note: controls that require an explicit `userInterface.control` hint (like `MULTILINE_EDIT` or
`CHECK_BOX`) cannot be inferred. A revealed hidden `STRING` always becomes `LINE_EDIT`, not
`MULTILINE_EDIT`, because the `HIDDEN` control replaced whatever the author might have intended.

### Visual indicator

Revealed hidden parameters display a small ℹ️ icon before the label. The icon has:
- A **tooltip** ("This parameter is normally hidden") for sighted users on hover.
- An **accessibleDescription** with the same text, announced by screen readers.

This ensures the distinction is conveyed through both visual and non-visual channels.

### Loading a different bundle

When the user clicks "Load Bundle" to switch to a different job bundle, `on_load_bundle`
creates a fresh `JobBundleSettings` and propagates `show_hidden_parameters` from the widget's stored
state. This ensures the flag survives bundle switches within the same dialog session.

### Queue environment parameters

`--show-hidden-parameters` intentionally applies to queue environment parameters, not just job bundle
parameters. During debugging, users need visibility into all parameters regardless of source.

### Third-party submitters

`SubmitJobToDeadlineDialog` accepts `show_hidden_parameters` as an optional keyword argument (default
`False`). DCC submitters (Maya, Blender, etc.) that instantiate the dialog directly can opt in
by passing `show_hidden_parameters=True`. No existing callers are affected.

## Files Changed

| File | Change |
|------|--------|
| `cli/_groups/bundle_group.py` | `--show-hidden-parameters` click option |
| `ui/dataclasses/__init__.py` | `show_hidden_parameters` field on `JobBundleSettings` |
| `ui/job_bundle_submitter.py` | Threads flag to dialog |
| `ui/dialogs/submit_job_to_deadline_dialog.py` | Stores and passes flag to tab widgets |
| `ui/widgets/shared_job_settings_tab.py` | Passes flag to queue `OpenJDParametersWidget` |
| `ui/widgets/job_bundle_settings_tab.py` | Passes flag to bundle `OpenJDParametersWidget`; propagates on bundle load |
| `ui/widgets/openjd_parameters_widget.py` | Core logic: widget resolution, info icon indicator |

## Testing

Tests: `test/unit/deadline_client/ui/widgets/test_show_hidden_parameters.py`
Test data: `test/unit/deadline_client/ui/test_data/job_bundle_with_hidden_params/`

| Test | What it verifies |
|------|-----------------|
| `test_hidden_params_not_visible_by_default` | `HIDDEN` parameters use `_JobTemplateHiddenWidget` and are not in the layout |
| `test_show_hidden_makes_hidden_params_visible` | Each type maps to the correct widget, is in the layout, and has its default value |
| `test_get_parameters_round_trip_with_show_hidden` | `get_parameters()` returns correct values after modifying revealed hidden params |
| `test_show_hidden_with_queue_environment_parameters` | Queue environment hidden params are revealed the same way |
| `test_show_hidden_adds_indicator_icon` | Info icon present on revealed params with correct `accessibleDescription`; absent on normal params |
