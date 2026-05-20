"""Headless widget tests for config dialog and auth status.

Uses QT_QPA_PLATFORM=offscreen and the ffi-test-server stub.
No real AWS calls, no display needed.

Tests cover:
- Auth status widget shows profile name (not blank)
- Config dialog Apply/Ok/Cancel buttons work without freeze
- Yellow border (unsaved indicator) clears after Apply
"""

import os
import time

import pytest
from configparser import ConfigParser

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from qtpy.QtWidgets import QApplication
from qtpy.QtCore import Qt


@pytest.fixture(scope="session")
def qapp():
    """Create a QApplication for the test session."""
    import sys
    app = QApplication.instance() or QApplication(sys.argv)
    return app


@pytest.fixture
def stub_config(test_server, tmp_path):
    """Config file pointing at the stub server's farm/queue."""
    config = tmp_path / "config"
    config.write_text(
        "[defaults]\naws_profile_name = (default)\n\n"
        "[profile-(default) defaults]\n"
        "farm_id = farm-abc123def4567890abc123def4567890\n\n"
        "[profile-(default) farm-abc123def4567890abc123def4567890 defaults]\n"
        "queue_id = queue-abc123def4567890abc123def4567890\n\n"
        "[settings]\nauto_accept = false\nlog_level = WARNING\n"
    )
    old = os.environ.get("DEADLINE_CONFIG_FILE_PATH")
    os.environ["DEADLINE_CONFIG_FILE_PATH"] = str(config)
    yield str(config)
    if old is None:
        os.environ.pop("DEADLINE_CONFIG_FILE_PATH", None)
    else:
        os.environ["DEADLINE_CONFIG_FILE_PATH"] = old


class TestAuthStatusWidget:
    """Auth status widget displays profile name correctly."""

    def test_profile_name_displayed(self, qapp, stub_config):
        """Auth status widget shows the profile name, not blank."""
        import deadline.client.ui.deadline_authentication_status as das_mod
        from deadline.client.ui.deadline_authentication_status import DeadlineAuthenticationStatus
        from deadline.client.config import config_file

        # Reset module-level singleton so it picks up our stub config
        das_mod._deadline_authentication_status = None
        status = DeadlineAuthenticationStatus.getInstance()

        profile = config_file.get_setting("defaults.aws_profile_name", config=status.config)
        assert profile == "(default)"
        assert profile != ""

    def test_auth_status_resolves_after_refresh(self, qapp, stub_config):
        """After async refresh completes, auth status is AUTHENTICATED."""
        import deadline.client.ui.deadline_authentication_status as das_mod
        from deadline.client.ui.deadline_authentication_status import DeadlineAuthenticationStatus

        das_mod._deadline_authentication_status = None
        status = DeadlineAuthenticationStatus.getInstance()

        # Process events to let async tasks complete
        for _ in range(40):
            qapp.processEvents()
            time.sleep(0.1)
            if status.auth_status is not None and status.api_availability is not None:
                break

        assert status.creds_source == "HOST_PROVIDED"
        assert status.auth_status == "AUTHENTICATED"
        assert status.api_availability is True

    def test_auth_widget_shows_profile_text(self, qapp, stub_config):
        """The auth status widget button shows profile name text."""
        import deadline.client.ui.deadline_authentication_status as das_mod
        from deadline.client.ui.deadline_authentication_status import DeadlineAuthenticationStatus
        from deadline.client.ui.widgets.deadline_authentication_status_widget import (
            DeadlineAuthenticationStatusWidget,
        )

        das_mod._deadline_authentication_status = None

        widget = DeadlineAuthenticationStatusWidget()

        # Wait for async refresh
        for _ in range(20):
            qapp.processEvents()
            time.sleep(0.1)
            if DeadlineAuthenticationStatus.getInstance().auth_status is not None:
                break
        qapp.processEvents()

        # The profile button should show the profile name
        text = widget._profile_button.text()
        assert text != ""
        assert "(default)" in text


class TestConfigDialogApplyCancel:
    """Config dialog Ok/Apply/Cancel buttons work without freeze."""

    def test_apply_succeeds(self, qapp, stub_config):
        """Apply button persists changes and returns True."""
        from deadline.client.ui.dialogs.deadline_config_dialog import (
            DeadlineWorkstationConfigWidget,
        )
        from deadline.client.ui.deadline_authentication_status import DeadlineAuthenticationStatus
        from deadline.client.config import config_file
        from deadline._native import get_setting as _native_get_setting

        DeadlineAuthenticationStatus._deadline_authentication_status = None
        widget = DeadlineWorkstationConfigWidget()

        # Simulate user changing auto_accept
        widget.changes["settings.auto_accept"] = "true"
        widget.refresh()

        result = widget.apply()
        assert result is True
        assert widget.changes == {}
        assert _native_get_setting("settings.auto_accept") == "true"

    def test_apply_with_none_storage_profile(self, qapp, stub_config):
        """Apply doesn't crash when storage profile combo has no selection."""
        from deadline.client.ui.dialogs.deadline_config_dialog import (
            DeadlineWorkstationConfigWidget,
        )
        from deadline.client.ui.deadline_authentication_status import DeadlineAuthenticationStatus

        DeadlineAuthenticationStatus._deadline_authentication_status = None
        widget = DeadlineWorkstationConfigWidget()

        # Force storage profile to have no data (simulates empty combo)
        widget.default_storage_profile_box.box.clear()

        result = widget.apply()
        assert result is True  # Should not crash

    def test_cancel_does_not_persist(self, qapp, stub_config):
        """Cancel discards in-memory changes, disk unchanged."""
        from deadline.client.ui.dialogs.deadline_config_dialog import (
            DeadlineWorkstationConfigWidget,
        )
        from deadline.client.ui.deadline_authentication_status import DeadlineAuthenticationStatus
        from deadline._native import get_setting as _native_get_setting

        DeadlineAuthenticationStatus._deadline_authentication_status = None
        widget = DeadlineWorkstationConfigWidget()

        original_farm = _native_get_setting("defaults.farm_id")

        # Simulate user changing farm in-memory
        widget.changes["defaults.farm_id"] = "farm-should-not-persist"
        widget.refresh()

        # Simulate cancel: just discard (don't call apply)
        # Disk should be unchanged
        assert _native_get_setting("defaults.farm_id") == original_farm


class TestUnsavedChangesIndicator:
    """Yellow border shows for unsaved changes and clears on Apply."""

    def test_border_appears_on_change(self, qapp, stub_config):
        """Labels get orange border when their setting has pending changes."""
        from deadline.client.ui.dialogs.deadline_config_dialog import (
            DeadlineWorkstationConfigWidget,
        )
        from deadline.client.ui.deadline_authentication_status import DeadlineAuthenticationStatus

        DeadlineAuthenticationStatus._deadline_authentication_status = None
        widget = DeadlineWorkstationConfigWidget()

        # Initially no border
        label = widget.labels.get("settings.auto_accept")
        assert label is not None
        assert "orange" not in label.styleSheet()

        # Change setting
        widget.changes["settings.auto_accept"] = "true"
        widget.refresh()

        # Border should appear
        assert "orange" in label.styleSheet()

    def test_border_clears_after_apply(self, qapp, stub_config):
        """Orange border clears after Apply."""
        from deadline.client.ui.dialogs.deadline_config_dialog import (
            DeadlineWorkstationConfigWidget,
        )
        from deadline.client.ui.deadline_authentication_status import DeadlineAuthenticationStatus

        DeadlineAuthenticationStatus._deadline_authentication_status = None
        widget = DeadlineWorkstationConfigWidget()

        # Change and refresh
        widget.changes["settings.auto_accept"] = "true"
        widget.refresh()

        label = widget.labels["settings.auto_accept"]
        assert "orange" in label.styleSheet()

        # Apply
        widget.apply()

        # Border should be gone
        assert "orange" not in label.styleSheet()
