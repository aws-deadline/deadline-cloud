# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""UI tests for ``deadline bundle gui-submit`` against the mock backend."""

from __future__ import annotations

import json
import os

from helpers import SubmitterDialog


class TestSubmitterOpens:
    def test_dialog_is_visible(self, gui_submit: SubmitterDialog):
        assert gui_submit.dialog().element().visible

    def test_farm_name_resolved(self, gui_submit: SubmitterDialog):
        assert gui_submit.tree_contains_text("TestFarm")

    def test_queue_name_resolved(self, gui_submit: SubmitterDialog):
        assert gui_submit.tree_contains_text("TestQueue")

    def test_job_name_displayed(self, gui_submit: SubmitterDialog):
        assert gui_submit.job_name == "Test Render Job"

    def test_has_tabs(self, gui_submit: SubmitterDialog):
        tab_group = gui_submit.locator("tab_group")
        assert tab_group.exists()
        for tab_name in (
            "Shared job settings",
            "Job-specific settings",
            "Job attachments",
            "Host requirements",
        ):
            assert gui_submit.tab_exists(tab_name), f"Tab {tab_name!r} not found"

    def test_has_submit_and_export_buttons(self, gui_submit: SubmitterDialog):
        assert gui_submit.button("Submit").exists()
        assert gui_submit.button("Save bundle as").exists()


class TestExportBundle:
    def test_export_creates_bundle(self, bundle_dir, submitter_env):
        # A local "Save bundle as" writes to the configured job-bundle default
        # directory, which the fixture points at a known location.
        export_root = submitter_env["_EXPORT_DIR"]
        with SubmitterDialog.open(bundle_dir, env=submitter_env) as app:
            app.wait_farm_resolved()
            app.export_bundle()

            templates = []
            for root, _dirs, files in os.walk(export_root):
                for fn in files:
                    if fn.startswith("template."):
                        templates.append(os.path.join(root, fn))
            assert templates, "No template file found in exported bundle"
            with open(templates[0]) as f:
                exported = json.load(f)
            assert exported["name"] == "Test Render Job"
