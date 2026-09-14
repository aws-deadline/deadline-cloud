# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests that ``show_job_bundle_submitter`` always opts into the deadline-cloud-v2 Conda channel
by passing ``use_deadline_cloud_v2_channel=True`` to the ``SubmitJobToDeadlineDialog`` it
constructs, with the Qt-heavy dependencies patched out.
"""

import os
from unittest.mock import patch

from deadline.client.ui.job_bundle_submitter import show_job_bundle_submitter

MODULE = "deadline.client.ui.job_bundle_submitter"
# run_pre_gui_hooks resolves its allow_*_hooks gates through this module's _get_setting; patch
# it too so ambient config can't enable hooks during the test.
HOOKS_MODULE = "deadline.client.ui.pre_gui_hooks"


def _make_bundle(tmp_path):
    bundle_dir = str(tmp_path / "bundle")
    os.makedirs(bundle_dir)
    with open(os.path.join(bundle_dir, "template.yaml"), "w") as f:
        f.write("name: Bundle Job\nsteps: []\n")
    return bundle_dir


def _run_submitter(bundle_dir):
    """Drive show_job_bundle_submitter with Qt/deps patched, returning the patched
    SubmitJobToDeadlineDialog so callers can inspect its constructor arguments."""
    template = {"name": "Bundle Job", "steps": []}
    with (
        patch(f"{MODULE}.validate_directory_symlink_containment"),
        patch(
            f"{MODULE}.read_yaml_or_json_object",
            side_effect=lambda _dir, name, *a, **k: template if name == "template" else None,
        ),
        patch(f"{MODULE}.read_job_bundle_parameters", return_value=[]),
        patch(f"{MODULE}.SubmitJobToDeadlineDialog") as dialog_cls,
        patch(f"{MODULE}.QApplication"),
        patch(f"{MODULE}.QMessageBox"),
        patch(f"{MODULE}._get_setting", side_effect=lambda name, config=None: "false"),
        patch(f"{HOOKS_MODULE}._get_setting", side_effect=lambda name, config=None: "false"),
        patch.dict(os.environ, {}, clear=False),
    ):
        # Ensure no ambient studio hooks directory leaks into the test.
        os.environ.pop("DEADLINE_HOOKS_DIR", None)
        show_job_bundle_submitter(input_job_bundle_dir=bundle_dir)
    return dialog_cls


def test_v2_channel_always_enabled_on_dialog(tmp_path):
    dialog_cls = _run_submitter(_make_bundle(tmp_path))
    assert dialog_cls.call_args.kwargs["use_deadline_cloud_v2_channel"] is True
