# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""UI e2e tests for preGUI submission hooks in ``deadline bundle gui-submit``.

These launch the real GUI submitter as a subprocess and drive it through the OS
accessibility tree (xa11y), against the in-process mock Deadline backend. A preGUI hook
runs before the dialog opens; the tests assert on the rendered ``Name`` field to confirm
the hook ran (or, when hooks are disabled, that it did not), and that a hook-provided
``deadline:priority`` reaches both the Priority spinner and the final CreateJob.
"""

from __future__ import annotations

import json

import pytest

from helpers import SAMPLE_TEMPLATE, SubmitterDialog, cli_set

# A preGUI hook that pre-fills the job name field before the dialog opens.
_PRE_GUI_HOOKS_YAML = "version: '1.0'\npreGUI:\n  - command: python3\n    args: [prefill.py]\n"
_PRE_GUI_SCRIPT = 'import json; print(json.dumps({"name": "PREGUI_RAN"}))\n'

# A preGUI hook that overrides the deadline:priority job property before the dialog opens.
_PRE_GUI_PRIORITY_HOOKS_YAML = (
    "version: '1.0'\npreGUI:\n  - command: python3\n    args: [set_priority.py]\n"
)
_PRE_GUI_PRIORITY_SCRIPT = (
    'import json; print(json.dumps({"parameters": {"deadline:priority": 88}}))\n'
)


@pytest.fixture
def hook_bundle_dir(tmp_path) -> str:
    """A job bundle whose preGUI hook pre-fills the job name."""
    d = tmp_path / "bundle"
    d.mkdir()
    (d / "template.json").write_text(json.dumps(SAMPLE_TEMPLATE))
    (d / "hooks.yaml").write_text(_PRE_GUI_HOOKS_YAML)
    (d / "prefill.py").write_text(_PRE_GUI_SCRIPT)
    return str(d)


@pytest.fixture
def priority_hook_bundle_dir(tmp_path) -> str:
    """A job bundle whose preGUI hook overrides ``deadline:priority``.

    The bundle carries a ``parameter_values`` entry for ``deadline:priority`` so that
    ``read_job_bundle_parameters`` keeps it in ``initial_settings.parameters`` (making it a
    "template-param name"). This is what reproduces the regression: without it, the old code's
    fall-through ``else`` branch already routed the hook value to the shared values correctly.
    With it, the old code wrote the hook value into the unread list entry while the stale
    bundle value (50) drove the spinner + CreateJob.
    """
    d = tmp_path / "bundle"
    d.mkdir()
    (d / "template.json").write_text(json.dumps(SAMPLE_TEMPLATE))
    (d / "parameter_values.json").write_text(
        json.dumps({"parameterValues": [{"name": "deadline:priority", "value": 50}]})
    )
    (d / "hooks.yaml").write_text(_PRE_GUI_PRIORITY_HOOKS_YAML)
    (d / "set_priority.py").write_text(_PRE_GUI_PRIORITY_SCRIPT)
    return str(d)


def test_pre_gui_hook_prefills_job_name(hook_bundle_dir, submitter_env):
    """With bundle hooks enabled, the preGUI hook runs before the dialog opens and
    pre-fills the Name field with its output."""
    env = submitter_env
    cli_set(env, "settings.allow_bundle_hooks", "true")
    cli_set(env, "settings.auto_accept", "true")

    with SubmitterDialog.open(hook_bundle_dir, env=env) as app:
        app.wait_farm_resolved()
        assert app.job_name == "PREGUI_RAN"


def test_pre_gui_hook_not_run_when_disabled(hook_bundle_dir, submitter_env):
    """With bundle hooks disabled (default), the preGUI hook does not run and the Name
    field keeps the job template's name."""
    env = submitter_env
    cli_set(env, "settings.auto_accept", "true")
    # deliberately do NOT enable allow_bundle_hooks

    with SubmitterDialog.open(hook_bundle_dir, env=env) as app:
        app.wait_farm_resolved()
        assert app.job_name == SAMPLE_TEMPLATE["name"]  # "Test Render Job", unchanged


def test_pre_gui_hook_overrides_deadline_priority(
    priority_hook_bundle_dir, submitter_env, deadline_env
):
    """Regression: a preGUI hook returning ``deadline:priority`` reaches both the visible
    Priority spinner and the final CreateJob, even though the bundle carries a stale value
    of 50 in its ``parameter_values``.

    Before the fix, the hook value landed in the unread ``initial_settings.parameters`` list
    entry while the stale 50 drove the spinner and CreateJob, so the job came out priority 50.
    """
    backend, _ = deadline_env  # same backend instance the submitter_env fixture seeds
    env = submitter_env
    cli_set(env, "settings.allow_bundle_hooks", "true")
    cli_set(env, "settings.auto_accept", "true")

    with SubmitterDialog.open(priority_hook_bundle_dir, env=env) as app:
        app.wait_farm_resolved()
        # The visible spinner reflects the hook's 88, not the bundle's stale 50.
        assert any(
            (getattr(sb, "value", "") or "") == "88" for sb in app.elements_by_role("spin_button")
        ), "Priority spinner did not show the hook-provided value 88"
        app.submit_and_ok()

    # ...and that value flows all the way to CreateJob, not the bundle's stale 50.
    assert backend.call_counts.get("CreateJob", 0) == 1, backend.call_counts
    priorities = [job["priority"] for job in backend.jobs.values()]
    assert priorities == [88], f"CreateJob priority was not the hook value 88: {priorities}"
