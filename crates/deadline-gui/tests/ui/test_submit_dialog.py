# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
L2 xa11y tests for the submit dialog — launched via gui-test-harness.

Tests the deadline-gui crate in isolation from deadline-cli.
These tests will pass once show_submit_dialog() is implemented (Step D).
"""

from __future__ import annotations

import pytest

from conftest import GUIHarness


@pytest.mark.skip(reason="show_submit_dialog not yet implemented (Step D)")
class TestSubmitDialogOpens:
    """Basic submit dialog rendering tests."""

    def test_dialog_is_visible(self, bundle_dir, seeded_env):
        with GUIHarness.open_submit(bundle_dir, env=seeded_env) as app:
            assert app.dialog().element().visible

    def test_has_tabs(self, bundle_dir, seeded_env):
        with GUIHarness.open_submit(bundle_dir, env=seeded_env) as app:
            for tab_name in (
                "Shared job settings",
                "Job-specific settings",
                "Job attachments",
                "Host requirements",
            ):
                assert app.tab_exists(tab_name), f"Tab {tab_name!r} not found"

    def test_has_submit_and_export_buttons(self, bundle_dir, seeded_env):
        with GUIHarness.open_submit(bundle_dir, env=seeded_env) as app:
            assert app.button("Submit").exists()
            assert app.button("Export bundle").exists()

    def test_farm_name_resolved(self, bundle_dir, seeded_env):
        with GUIHarness.open_submit(bundle_dir, env=seeded_env) as app:
            assert app.tree_contains_text("TestFarm")

    def test_job_name_from_template(self, bundle_dir, seeded_env):
        with GUIHarness.open_submit(bundle_dir, env=seeded_env) as app:
            assert app.tree_contains_text("Test Render Job")
