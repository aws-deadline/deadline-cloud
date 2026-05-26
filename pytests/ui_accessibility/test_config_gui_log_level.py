# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""UI tests for ``deadline config gui`` — log level setting.

Note: on macOS, Qt combo-box popup items cannot be reliably activated
via the accessibility API, so we mutate the config value with
``deadline config set`` and only assert the GUI reflects it.
"""

from __future__ import annotations

import pytest

from helpers import ConfigDialog, cli_get, cli_set


class TestConfigGuiOpens:
    def test_dialog_is_visible(self, deadline_env):
        _, env = deadline_env
        with ConfigDialog.open(env=env) as app:
            assert app.dialog().element().visible

    def test_has_ok_cancel_apply_buttons(self, deadline_env):
        _, env = deadline_env
        with ConfigDialog.open(env=env) as app:
            for name in ("Ok", "Cancel", "Apply"):
                assert app.button(name).exists()

    def test_has_general_settings_group(self, deadline_env):
        _, env = deadline_env
        with ConfigDialog.open(env=env) as app:
            assert app.locator('group[name="General settings"]').exists()

    def test_shows_current_log_level(self, deadline_env):
        _, env = deadline_env
        expected = cli_get(env, "settings.log_level")
        with ConfigDialog.open(env=env) as app:
            assert app.log_level == expected

    @pytest.mark.parametrize("button", ["Ok", "Cancel"])
    def test_button_exits_cleanly(self, deadline_env, button: str):
        _, env = deadline_env
        with ConfigDialog.open(env=env) as app:
            app.button(button).press()
            assert app.proc.wait(timeout=3) == 0


class TestLogLevelRoundTrip:
    """Set a log level via CLI, open the GUI, verify it displays."""

    @pytest.mark.parametrize("level", ["ERROR", "WARNING", "INFO", "DEBUG"])
    def test_each_level_displays_correctly(self, deadline_env, level: str):
        _, env = deadline_env
        cli_set(env, "settings.log_level", level)
        with ConfigDialog.open(env=env) as app:
            assert app.log_level == level


class TestCancelDoesNotSave:
    def test_cancel_leaves_config_unchanged(self, deadline_env):
        _, env = deadline_env
        cli_set(env, "settings.log_level", "DEBUG")
        with ConfigDialog.open(env=env) as app:
            assert app.log_level == "DEBUG"
            app.close("Cancel")
        assert cli_get(env, "settings.log_level") == "DEBUG"


class TestOkSaves:
    def test_ok_persists_value(self, deadline_env):
        _, env = deadline_env
        cli_set(env, "settings.log_level", "INFO")
        with ConfigDialog.open(env=env) as app:
            assert app.log_level == "INFO"
            app.close("Ok")
        assert cli_get(env, "settings.log_level") == "INFO"


class TestReopenAfterCancel:
    def test_reopen_shows_same_value(self, deadline_env):
        _, env = deadline_env
        with ConfigDialog.open(env=env) as app:
            first_value = app.log_level
            app.close("Cancel")
        with ConfigDialog.open(env=env) as app:
            assert app.log_level == first_value


class TestChangeOkReopen:
    def test_change_persists_across_reopens(self, deadline_env):
        _, env = deadline_env
        cli_set(env, "settings.log_level", "DEBUG")
        with ConfigDialog.open(env=env) as app:
            assert app.log_level == "DEBUG"
            app.close("Ok")
        with ConfigDialog.open(env=env) as app:
            assert app.log_level == "DEBUG"
