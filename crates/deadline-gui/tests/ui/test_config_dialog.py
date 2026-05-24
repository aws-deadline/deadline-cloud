# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
L2 xa11y tests for the config dialog — launched via gui-test-harness.

Tests the deadline-gui crate in isolation from deadline-cli.
"""

from __future__ import annotations

from conftest import GUIHarness


class TestConfigDialogOpens:
    """Basic config dialog rendering tests."""

    def test_dialog_is_visible(self, config_dialog: GUIHarness):
        assert config_dialog.dialog().element().visible

    def test_has_settings_groups(self, config_dialog: GUIHarness):
        for group_name in (
            "Global settings",
            "Profile settings",
            "Farm settings",
            "General settings",
        ):
            assert config_dialog.locator(f'group[name="{group_name}"]').exists(), (
                f"{group_name!r} group missing"
            )

    def test_auth_status_shows_default_profile(self, config_dialog: GUIHarness):
        assert config_dialog.tree_contains_text("(default)")
