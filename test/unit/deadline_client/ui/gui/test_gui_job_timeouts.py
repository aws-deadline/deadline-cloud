# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for TimeoutTableWidget and TimeoutEntryWidget."""

import pytest

try:
    from deadline.client.ui.widgets.job_timeouts_widget import (
        TimeoutEntryWidget,
        TimeoutTableWidget,
        WARNING_ICON,
        ERROR_ICON,
    )
    from deadline.client.ui.dataclasses.timeouts import TimeoutEntry, TimeoutTableEntries
except ImportError:
    pytest.skip("GUI dependencies not available", allow_module_level=True)


def _make_timeouts(**entries):
    """Helper to create TimeoutTableEntries."""
    return TimeoutTableEntries(
        entries={
            label: TimeoutEntry(tooltip=f"Tooltip for {label}", **kwargs)
            for label, kwargs in entries.items()
        }
    )


class TestTimeoutEntryWidget:
    def test_set_timeout_seconds(self, qtbot):
        """Verify set_timeout correctly distributes seconds across days/hours/minutes."""
        widget = TimeoutEntryWidget("Test timeout", "A test tooltip")
        qtbot.addWidget(widget)

        # 1 day, 2 hours, 30 minutes = 86400 + 7200 + 1800 = 95400 seconds
        widget.set_timeout(95400)

        assert widget.days_box.value() == 1
        assert widget.hours_box.value() == 2
        assert widget.minutes_box.value() == 30

    def test_get_timeout_seconds(self, qtbot):
        """Verify get_timeout_seconds calculates total correctly."""
        widget = TimeoutEntryWidget("Test timeout", "A test tooltip")
        qtbot.addWidget(widget)

        widget.days_box.setValue(2)
        widget.hours_box.setValue(3)
        widget.minutes_box.setValue(15)

        # 2*86400 + 3*3600 + 15*60 = 172800 + 10800 + 900 = 184500
        assert widget.get_timeout_seconds() == 184500

    def test_checkbox_unchecked_disables_spinboxes(self, qtbot):
        """Verify unchecking the checkbox disables time input fields."""
        widget = TimeoutEntryWidget("Test timeout", "A test tooltip")
        qtbot.addWidget(widget)

        widget.checkbox.setChecked(False)
        widget.update_state()

        assert not widget.days_box.isEnabled()
        assert not widget.hours_box.isEnabled()
        assert not widget.minutes_box.isEnabled()

    def test_checkbox_checked_enables_spinboxes(self, qtbot):
        """Verify checking the checkbox enables time input fields."""
        widget = TimeoutEntryWidget("Test timeout", "A test tooltip")
        qtbot.addWidget(widget)

        widget.checkbox.setChecked(False)
        widget.update_state()
        widget.checkbox.setChecked(True)
        widget.update_state()

        assert widget.days_box.isEnabled()
        assert widget.hours_box.isEnabled()
        assert widget.minutes_box.isEnabled()

    def test_zero_timeout_does_not_show_error_immediately(self, qtbot):
        """Verify error icon does NOT appear immediately when timeout becomes zero."""
        widget = TimeoutEntryWidget("Test timeout", "A test tooltip")
        qtbot.addWidget(widget)

        widget.days_box.setValue(0)
        widget.hours_box.setValue(0)
        widget.minutes_box.setValue(0)
        widget.update_state()

        assert widget.status_icon.text() != ERROR_ICON

    def test_zero_timeout_shows_error_after_focus_leave(self, qtbot):
        """Verify error icon appears after _update_error_state is called (focus left)."""
        widget = TimeoutEntryWidget("Test timeout", "A test tooltip")
        qtbot.addWidget(widget)

        widget.days_box.setValue(0)
        widget.hours_box.setValue(0)
        widget.minutes_box.setValue(0)
        widget._update_error_state()

        assert widget.status_icon.text() == ERROR_ICON

    def test_nonzero_timeout_clears_error_immediately(self, qtbot):
        """Verify error clears as soon as the value becomes non-zero."""
        widget = TimeoutEntryWidget("Test timeout", "A test tooltip")
        qtbot.addWidget(widget)

        # First, put into error state
        widget.days_box.setValue(0)
        widget.hours_box.setValue(0)
        widget.minutes_box.setValue(0)
        widget._update_error_state()
        assert widget.status_icon.text() == ERROR_ICON

        # Now set a valid value - error should clear immediately via update_state
        widget.minutes_box.setValue(30)
        widget.update_state()
        assert widget.status_icon.text() == ""

    def test_unchecked_shows_warning_icon(self, qtbot):
        """Verify warning icon appears when checkbox is unchecked."""
        widget = TimeoutEntryWidget("Test timeout", "A test tooltip")
        qtbot.addWidget(widget)

        widget.checkbox.setChecked(False)
        widget.update_state()

        assert widget.status_icon.text() == WARNING_ICON

    def test_valid_timeout_shows_no_icon(self, qtbot):
        """Verify no icon when timeout is valid and enabled."""
        widget = TimeoutEntryWidget("Test timeout", "A test tooltip")
        qtbot.addWidget(widget)

        widget.days_box.setValue(1)
        widget.update_state()

        assert widget.status_icon.text() == ""

    def test_set_timeout_zero_seconds(self, qtbot):
        """Verify set_timeout(0) sets all fields to zero."""
        widget = TimeoutEntryWidget("Test timeout", "A test tooltip")
        qtbot.addWidget(widget)

        widget.set_timeout(0)
        assert widget.days_box.value() == 0
        assert widget.hours_box.value() == 0
        assert widget.minutes_box.value() == 0


class TestTimeoutTableWidget:
    def test_creation_with_entries(self, qtbot):
        """Verify table widget creates rows for each timeout entry."""
        timeouts = _make_timeouts(
            **{"Task timeout": {"seconds": 3600}, "Session timeout": {"seconds": 7200}}
        )
        widget = TimeoutTableWidget(timeouts=timeouts)
        qtbot.addWidget(widget)

        assert "Task timeout" in widget.timeout_rows
        assert "Session timeout" in widget.timeout_rows

    def test_refresh_ui_updates_values(self, qtbot):
        """Verify refresh_ui updates row values from TimeoutTableEntries."""
        timeouts = _make_timeouts(**{"Task timeout": {"seconds": 3600}})
        widget = TimeoutTableWidget(timeouts=timeouts)
        qtbot.addWidget(widget)

        row = widget.timeout_rows["Task timeout"]
        assert row.get_timeout_seconds() == 3600

        # Refresh with new values
        new_timeouts = _make_timeouts(**{"Task timeout": {"seconds": 7200}})
        widget.refresh_ui(new_timeouts)

        assert row.get_timeout_seconds() == 7200

    def test_error_label_not_visible_on_zero_without_focus_leave(self, qtbot):
        """Verify error message does NOT appear when timeout is set to zero mid-edit."""
        timeouts = _make_timeouts(**{"Task timeout": {"seconds": 3600}})
        widget = TimeoutTableWidget(timeouts=timeouts)
        qtbot.addWidget(widget)
        widget.show()

        row = widget.timeout_rows["Task timeout"]
        row.days_box.setValue(0)
        row.hours_box.setValue(0)
        row.minutes_box.setValue(0)

        assert not widget.error_label.isVisible()

    def test_error_label_visible_after_focus_leave(self, qtbot):
        """Verify error message appears when focus leaves a row with zero timeout."""
        timeouts = _make_timeouts(**{"Task timeout": {"seconds": 3600}})
        widget = TimeoutTableWidget(timeouts=timeouts)
        qtbot.addWidget(widget)
        widget.show()

        row = widget.timeout_rows["Task timeout"]
        row.days_box.setValue(0)
        row.hours_box.setValue(0)
        row.minutes_box.setValue(0)
        row.checkbox.setChecked(True)

        # Simulate focus leaving the row
        widget._update_error_states()

        assert widget.error_label.isVisible()
        assert "zero" in widget.error_label.text().lower()

    def test_error_label_clears_when_value_becomes_valid(self, qtbot):
        """Verify error message clears immediately when timeout becomes non-zero."""
        timeouts = _make_timeouts(**{"Task timeout": {"seconds": 3600}})
        widget = TimeoutTableWidget(timeouts=timeouts)
        qtbot.addWidget(widget)
        widget.show()

        row = widget.timeout_rows["Task timeout"]
        row.days_box.setValue(0)
        row.hours_box.setValue(0)
        row.minutes_box.setValue(0)
        row.checkbox.setChecked(True)

        # Put into error state
        widget._update_error_states()
        assert widget.error_label.isVisible()

        # Fix the value - error should clear immediately via _on_row_changed
        row.minutes_box.setValue(30)
        row._on_change()

        assert not widget.error_label.isVisible()

    def test_warning_label_visible_when_deactivated(self, qtbot):
        """Verify warning message appears when a timeout is deactivated."""
        timeouts = _make_timeouts(**{"Task timeout": {"seconds": 3600}})
        widget = TimeoutTableWidget(timeouts=timeouts)
        qtbot.addWidget(widget)
        widget.show()

        row = widget.timeout_rows["Task timeout"]
        row.checkbox.setChecked(False)
        row._on_change()

        assert widget.warning_label.isVisible()
        assert "indefinitely" in widget.warning_label.text().lower()

    def test_no_warnings_when_all_active_and_nonzero(self, qtbot):
        """Verify no warnings or errors when all timeouts are valid."""
        timeouts = _make_timeouts(**{"Task timeout": {"seconds": 3600}})
        widget = TimeoutTableWidget(timeouts=timeouts)
        qtbot.addWidget(widget)
        widget.show()

        assert not widget.error_label.isVisible()
        assert not widget.warning_label.isVisible()

    def test_update_settings(self, qtbot):
        """Verify update_settings writes current UI values back to TimeoutTableEntries."""
        timeouts = _make_timeouts(**{"Task timeout": {"seconds": 3600}})
        widget = TimeoutTableWidget(timeouts=timeouts)
        qtbot.addWidget(widget)

        # Change value in UI
        row = widget.timeout_rows["Task timeout"]
        row.days_box.setValue(2)
        row.hours_box.setValue(0)
        row.minutes_box.setValue(0)

        widget.update_settings(timeouts)
        assert timeouts.entries["Task timeout"].seconds == 172800  # 2 days

    def test_update_settings_deactivation(self, qtbot):
        """Verify update_settings records deactivation state."""
        timeouts = _make_timeouts(**{"Task timeout": {"seconds": 3600}})
        widget = TimeoutTableWidget(timeouts=timeouts)
        qtbot.addWidget(widget)

        row = widget.timeout_rows["Task timeout"]
        row.checkbox.setChecked(False)

        widget.update_settings(timeouts)
        assert timeouts.entries["Task timeout"].is_activated is False
