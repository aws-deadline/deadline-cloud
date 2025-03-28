# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
A UI Widget containing the timeout settings widget.
"""

from __future__ import annotations

from typing import Dict
from datetime import timedelta

from qtpy.QtWidgets import (  # type: ignore
    QFormLayout,
    QGroupBox,
    QLabel,
    QSpinBox,
    QCheckBox,
    QGridLayout,
    QWidget,
)
from qtpy.QtCore import Signal
from ..dataclasses.timeouts import TimeoutTableEntries

# UI Constants
WARNING_ICON = "⚠️"
ERROR_ICON = "❌"
ERROR_BG_COLOR = "#FFE4E1"  # Light red
WARNING_BG_COLOR = "#FFF3CD"  # Light yellow


class TimeoutEntryWidget(QWidget):
    """
    A widget representing a single timeout row with checkbox, status icon, and time input fields.

    Contains all the UI elements and logic for managing a single timeout entry including
    checkbox state, validation indicators, and time value calculations.
    """

    # Signal to indicate any change in the widget
    changed = Signal()

    def __init__(self, label: str, tooltip: str):
        super().__init__()

        self.checkbox = QCheckBox(label)
        self.checkbox.setChecked(True)
        self.checkbox.setToolTip(tooltip)

        self.status_icon = QLabel()
        self.status_icon.setFixedSize(16, 16)

        self.days_box = QSpinBox(self, minimum=0, maximum=365)
        self.days_box.setSuffix(" days")
        self.days_box.setFixedWidth(90)

        self.hours_box = QSpinBox(self, minimum=0, maximum=23)
        self.hours_box.setSuffix(" hours")
        self.hours_box.setFixedWidth(90)

        self.minutes_box = QSpinBox(self, minimum=0, maximum=59)
        self.minutes_box.setSuffix(" minutes")
        self.minutes_box.setFixedWidth(90)

        self._build_ui()
        self._connect_signals()

    def _build_ui(self):
        """
        Builds the internal layout of the timeout row.
        """
        layout = QGridLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        layout.addWidget(self.checkbox, 0, 0)
        layout.addWidget(self.status_icon, 0, 1)
        layout.addWidget(self.days_box, 0, 2)
        layout.addWidget(self.hours_box, 0, 3)
        layout.addWidget(self.minutes_box, 0, 4)

    def _connect_signals(self):
        """
        Connects all internal widgets to emit the changed signal.
        """
        self.checkbox.clicked.connect(self._on_change)
        self.days_box.valueChanged.connect(self._on_change)
        self.hours_box.valueChanged.connect(self._on_change)
        self.minutes_box.valueChanged.connect(self._on_change)

    def _on_change(self):
        """
        Handler for any changes in the widget's state.
        Updates the widget state and emits the changed signal.
        """
        self.update_state()
        self.changed.emit()

    def set_enabled(self, enabled: bool):
        """
        Enables or deactivates all time input fields in this row.

        Args:
            enabled: If True, enables the input fields; if False, deactivates them
        """
        self.days_box.setEnabled(enabled)
        self.hours_box.setEnabled(enabled)
        self.minutes_box.setEnabled(enabled)

    def set_timeout(self, total_seconds: int) -> None:
        """
        Sets the timeout value by converting seconds into days, hours, and minutes.

        Args:
            total_seconds: Total number of seconds to be distributed across time fields
        """
        td = timedelta(seconds=total_seconds)

        # Extract days from timedelta (1 day = 86400 seconds)
        days = td.days

        # Extract hours from remaining seconds (td.seconds is always < 86400)
        hours = td.seconds // 3600  # 3600 = 60 minutes * 60 seconds

        # Extract minutes from remaining seconds after hours are removed
        minutes = (td.seconds // 60) % 60

        self.days_box.setValue(days)
        self.hours_box.setValue(hours)
        self.minutes_box.setValue(minutes)

    def get_timeout_seconds(self) -> int:
        """
        Calculates the total timeout in seconds from the current days, hours, and minutes values.

        Returns:
            int: Total number of seconds represented by the current time values
        """
        return int(
            timedelta(
                days=self.days_box.value(),
                hours=self.hours_box.value(),
                minutes=self.minutes_box.value(),
            ).total_seconds()
        )

    def update_suffix(self):
        """
        Updates the suffix of time input fields to be singular or plural based on their values.
        For example: "1 day" vs "2 days"
        """
        for box in [self.days_box, self.hours_box, self.minutes_box]:
            suffix = box.suffix().rstrip("s")
            box.setSuffix(suffix if box.value() == 1 else suffix + "s")

    def set_status_icon(self, status: str):
        """
        Sets the status icon (warning or error) next to the checkbox.

        Args:
            status: The icon to display (WARNING_ICON, ERROR_ICON, or empty string)
        """
        self.status_icon.setText(status)

    def set_error_style(self, is_error: bool):
        """
        Sets the error styling for the time input fields.

        Args:
            is_error: If True, applies error styling; if False, removes it
        """
        style = f"QSpinBox {{ background-color: {ERROR_BG_COLOR}; }}" if is_error else ""
        for box in [self.days_box, self.hours_box, self.minutes_box]:
            box.setStyleSheet(style)

    def update_state(self):
        """
        Updates the complete state of the row including enabled state, validation indicators,
        and suffixes based on current values and checkbox state.
        """
        is_checked = self.checkbox.isChecked()
        if not is_checked:
            self.set_status_icon(WARNING_ICON)
            self.set_error_style(False)
        elif self.get_timeout_seconds() == 0:
            self.set_status_icon(ERROR_ICON)
            self.set_error_style(True)
        else:
            self.set_status_icon("")
            self.set_error_style(False)
        self.set_enabled(is_checked)
        self.update_suffix()


class TimeoutTableWidget(QGroupBox):
    """
    A widget for managing multiple timeout settings in a tabular format.

    This widget provides a user interface for configuring multiple timeout entries,
    each consisting of a checkbox for activation/deactivation and time input fields
    for days, hours, and minutes. It includes validation, warning indicators, and
    error messages for invalid configurations.

    Features:
    - Individual timeout rows with checkbox activation
    - Time input fields for days, hours, and minutes
    - Visual indicators for warnings and errors
    - Real-time validation and feedback
    - Automatic suffix updates (singular/plural)
    - Comprehensive error messaging

    Args:
        timeouts (TimeoutTableEntries): Configuration object containing timeout entries
        parent (QWidget, optional): Parent widget. Defaults to None.
    """

    def __init__(self, *, timeouts: TimeoutTableEntries, parent=None):
        super().__init__("Timeouts", parent=parent)
        self.timeout_rows: Dict[str, TimeoutEntryWidget] = {}
        self._build_ui(timeouts)
        self.refresh_ui(timeouts)

    def _build_ui(self, timeouts: TimeoutTableEntries):
        """
        Constructs the complete UI layout with all timeout rows and message labels.

        Args:
            entries: List of TimeoutEntry objects defining the timeout settings
        """
        self.layout = QFormLayout(self)
        self.layout.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)

        self.timeouts_box = QWidget()
        timeouts_layout = QGridLayout(self.timeouts_box)

        for index, (label, entry) in enumerate(timeouts.entries.items()):
            timeout_row = TimeoutEntryWidget(label, entry.tooltip)
            timeouts_layout.addWidget(timeout_row, index, 0, 1, 4)
            self.timeout_rows[label] = timeout_row
            timeout_row.changed.connect(self._update_ui_state)

        self.error_label = self._create_message_label(ERROR_BG_COLOR)
        self.warning_label = self._create_message_label(WARNING_BG_COLOR)

        timeouts_layout.addWidget(self.error_label, len(timeouts.entries), 0, 1, 4)
        timeouts_layout.addWidget(self.warning_label, len(timeouts.entries) + 1, 0, 1, 4)

        self.layout.addRow(self.timeouts_box)

    def _create_message_label(self, bg_color: str) -> QLabel:
        """
        Creates a styled label for displaying error or warning messages.

        Args:
            bg_color: Background color for the message label

        Returns:
            QLabel: A configured label with appropriate styling
        """
        label = QLabel()
        label.setStyleSheet(f"""
            QLabel {{ 
                background-color: {bg_color};
                color: black;
                padding: 5px;
                border-radius: 3px;
            }}
        """)
        label.setWordWrap(True)
        return label

    def _update_ui_state(self):
        """
        Updates the UI state for error and warning messages.
        """
        self._update_error_states()
        self._update_warning_states()

    def _update_error_states(self):
        """
        Updates the error message label based on validation of all timeout rows.
        """
        any_zero = any(
            row.get_timeout_seconds() == 0 and row.checkbox.isChecked()
            for row in self.timeout_rows.values()
        )
        self.error_label.setText("Error: Timeout cannot be set to zero." if any_zero else "")
        self.error_label.setVisible(any_zero)

    def _update_warning_states(self):
        """
        Updates the warning message label based on the state of all timeout rows.
        """
        any_deactivated = any(not row.checkbox.isChecked() for row in self.timeout_rows.values())
        warning_text = (
            "Warning: Without a specified timeout, tasks may run indefinitely if issues occur."
            if any_deactivated
            else ""
        )
        self.warning_label.setText(warning_text)
        self.warning_label.setVisible(any_deactivated)

    def refresh_ui(self, timeouts: TimeoutTableEntries):
        """
        Refreshes all UI elements to reflect the current timeout settings.

        Args:
            entries: List of TimeoutEntry objects containing the current settings
        """
        for label, entry in timeouts.entries.items():
            if label in self.timeout_rows:
                row = self.timeout_rows[label]
                row.checkbox.setChecked(entry.is_activated)
                row.set_timeout(entry.seconds)
        self._update_ui_state()

    def update_settings(self, timeouts: TimeoutTableEntries):
        """
        Updates the timeouts with the current values from the UI.
        """
        for label, row in self.timeout_rows.items():
            if label in timeouts.entries:
                entry = timeouts.entries[label]
                entry.is_activated = row.checkbox.isChecked()
                entry.seconds = row.get_timeout_seconds()
