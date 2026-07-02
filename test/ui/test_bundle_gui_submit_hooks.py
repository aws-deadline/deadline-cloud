# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""UI e2e tests for preGUI submission hooks in ``deadline bundle gui-submit``.

These launch the real GUI submitter as a subprocess and drive it through the OS
accessibility tree (xa11y), against the in-process mock Deadline backend. A preGUI hook
runs before the dialog opens and pre-fills the job name; the tests assert on the rendered
``Name`` field to confirm the hook ran (or, when hooks are disabled, that it did not).
"""

from __future__ import annotations

import json

import pytest

from helpers import SAMPLE_TEMPLATE, SubmitterDialog, cli_set

# A preGUI hook that pre-fills the job name field before the dialog opens.
_PRE_GUI_HOOKS_YAML = "version: '1.0'\npreGUI:\n  - command: python3\n    args: [prefill.py]\n"
_PRE_GUI_SCRIPT = 'import json; print(json.dumps({"name": "PREGUI_RAN"}))\n'


@pytest.fixture
def hook_bundle_dir(tmp_path) -> str:
    """A job bundle whose preGUI hook pre-fills the job name."""
    d = tmp_path / "bundle"
    d.mkdir()
    (d / "template.json").write_text(json.dumps(SAMPLE_TEMPLATE))
    (d / "hooks.yaml").write_text(_PRE_GUI_HOOKS_YAML)
    (d / "prefill.py").write_text(_PRE_GUI_SCRIPT)
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
