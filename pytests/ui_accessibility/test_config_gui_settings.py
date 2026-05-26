# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""UI tests for ``deadline config gui`` — workstation settings.

On macOS, Qt combo-box popup items cannot be reliably activated via the
accessibility API, so these tests use ``deadline config set`` to mutate
values and assert the GUI reflects them.
"""

from __future__ import annotations

from helpers import ConfigDialog, cli_get, cli_set


class TestConfigGuiSettingsSections:
    """The config dialog exposes the four expected settings groups."""

    def test_all_settings_groups_visible(self, deadline_env):
        _, env = deadline_env
        with ConfigDialog.open(env=env) as app:
            for group_name in (
                "Global settings",
                "Profile settings",
                "Farm settings",
                "General settings",
            ):
                assert app.locator(f'group[name="{group_name}"]').exists(), (
                    f"{group_name!r} group missing"
                )

    def test_auth_status_widget_shows_default_profile(self, deadline_env):
        _, env = deadline_env
        with ConfigDialog.open(env=env) as app:
            assert app.tree_contains_text("(default)"), (
                "'(default)' profile name not found anywhere in the auth status widget"
            )


class TestConflictResolutionSetting:
    """A non-log-level combo-box setting round-trips from CLI to GUI."""

    def test_changing_conflict_resolution_is_reflected_in_gui(self, deadline_env):
        _, env = deadline_env
        cli_set(env, "settings.conflict_resolution", "OVERWRITE")
        with ConfigDialog.open(env=env) as app:
            assert app.conflict_resolution == "OVERWRITE"

    def test_changing_conflict_resolution_persists_across_reopens(self, deadline_env):
        _, env = deadline_env
        cli_set(env, "settings.conflict_resolution", "SKIP")
        with ConfigDialog.open(env=env) as app:
            assert app.conflict_resolution == "SKIP"
            app.close("Ok")
        assert cli_get(env, "settings.conflict_resolution") == "SKIP"


class TestTelemetryCheckboxRoundTrip:
    """Checkbox setting (telemetry.opt_out) round-trips via CLI writes."""

    def test_checkbox_reflects_cli_value(self, deadline_env):
        _, env = deadline_env
        cli_set(env, "telemetry.opt_out", "true")
        with ConfigDialog.open(env=env) as app:
            assert app.dialog().element().visible
        assert cli_get(env, "telemetry.opt_out").lower() == "true"
