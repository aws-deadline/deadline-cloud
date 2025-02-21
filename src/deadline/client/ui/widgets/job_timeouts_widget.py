# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
A UI Widget containing the timeout settings widget.
"""

from __future__ import annotations

from typing import List, Any

from qtpy.QtWidgets import (  # type: ignore
    QFormLayout,
    QGroupBox,
    QLabel,
    QSpinBox,
    QCheckBox,
    QGridLayout,
    QWidget,
)

from ...job_bundle.timeouts import (
    TimeoutEntry,
    SECONDS_IN_A_DAY,
    SECONDS_IN_A_MINUTE,
    SECONDS_IN_AN_HOUR,
)

# UI Constants
WARNING_ICON = "⚠️"
ERROR_ICON = "❌"
ERROR_BG_COLOR = "#FFE4E1"  # Light red
WARNING_BG_COLOR = "#FFF3CD"  # Light yellow


class TimeoutRow:
    """
    A widget representing a single timeout row with checkbox, status icon, and time input fields.

    Contains all the UI elements and logic for managing a single timeout entry including
    checkbox state, validation indicators, and time value calculations.
    """

    def __init__(self, label: str, tooltip: str, parent: QWidget):
        self.checkbox = QCheckBox(label)
        self.checkbox.setChecked(True)
        self.checkbox.setToolTip(tooltip)

        self.status_icon = QLabel()
        self.status_icon.setFixedSize(16, 16)

        self.days_box = QSpinBox(parent, minimum=0, maximum=365)
        self.days_box.setSuffix(" days")

        self.hours_box = QSpinBox(parent, minimum=0, maximum=23)
        self.hours_box.setSuffix(" hours")

        self.minutes_box = QSpinBox(parent, minimum=0, maximum=59)
        self.minutes_box.setSuffix(" minutes")

    def add_to_layout(self, layout: QGridLayout, row: int):
        """
        Adds all components of this timeout row to the specified grid layout.

        Args:
            layout: The QGridLayout to add the components to
            row: The row number in the grid where components should be placed
        """

        container = QWidget()
        container_layout = QGridLayout(container)
        container_layout.setContentsMargins(0, 0, 0, 0)
        container_layout.addWidget(self.checkbox, 0, 0)
        container_layout.addWidget(self.status_icon, 0, 1)

        layout.addWidget(container, row, 0)
        layout.addWidget(self.days_box, row, 1)
        layout.addWidget(self.hours_box, row, 2)
        layout.addWidget(self.minutes_box, row, 3)

    def set_enabled(self, enabled: bool):
        """
        Enables or deactivates all time input fields in this row.

        Args:
            enabled: If True, enables the input fields; if False, deactivates them
        """
        self.days_box.setEnabled(enabled)
        self.hours_box.setEnabled(enabled)
        self.minutes_box.setEnabled(enabled)

    def set_timeout(self, seconds: int):
        """
        Sets the timeout value by converting seconds into days, hours, and minutes.

        Args:
            seconds: Total number of seconds to be distributed across time fields
        """
        days = seconds // SECONDS_IN_A_DAY
        hours = (seconds % SECONDS_IN_A_DAY) // SECONDS_IN_AN_HOUR
        minutes = (seconds % SECONDS_IN_AN_HOUR) // SECONDS_IN_A_MINUTE
        self.days_box.setValue(days)
        self.hours_box.setValue(hours)
        self.minutes_box.setValue(minutes)

    def get_timeout_seconds(self) -> int:
        """
        Calculates the total timeout in seconds from the current days, hours, and minutes values.

        Returns:
            int: Total number of seconds represented by the current time values
        """
        return (
            self.days_box.value() * SECONDS_IN_A_DAY
            + self.hours_box.value() * SECONDS_IN_AN_HOUR
            + self.minutes_box.value() * SECONDS_IN_A_MINUTE
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
        Applies or removes error styling from the time input fields.

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


class JobTimeoutsWidget(QGroupBox):
    """
    UI element to hold timeout settings of the submission

    The settings object should be a dataclass with:
      - `timeouts: List[TimeoutEntry]`. The timeouts to be applied for the job.
    """

    def __init__(self, *, settings: Any, parent=None):
        super().__init__("Timeouts", parent=parent)
        self.timeout_rows: List[TimeoutRow] = []
        self._build_ui(settings.timeouts)
        self.refresh_ui(settings.timeouts)

    def _build_ui(self, entries: List[TimeoutEntry]):
        """
        Constructs the complete UI layout with all timeout rows and message labels.

        Args:
            entries: List of TimeoutEntry objects defining the timeout settings
        """
        self.layout = QFormLayout(self)
        self.layout.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)

        self.timeouts_box = QWidget()
        timeouts_layout = QGridLayout(self.timeouts_box)

        for index, entry in enumerate(entries):
            timeout_row = TimeoutRow(entry.label, entry.tooltip, self)
            timeout_row.add_to_layout(timeouts_layout, index)
            self.timeout_rows.append(timeout_row)
            self._hookup_change_callback(timeout_row)

        self.error_label = self._create_message_label(ERROR_BG_COLOR)
        self.warning_label = self._create_message_label(WARNING_BG_COLOR)

        timeouts_layout.addWidget(self.error_label, len(entries), 0, 1, 4)
        timeouts_layout.addWidget(self.warning_label, len(entries) + 1, 0, 1, 4)

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

    def _hookup_change_callback(self, timeout_row: TimeoutRow):
        """
        Connects UI element signals to update handlers for a timeout row.

        Args:
            timeout_row: The TimeoutRow instance to hook up callbacks for
        """
        timeout_row.checkbox.clicked.connect(self._update_ui_state)
        for spinbox in [timeout_row.days_box, timeout_row.hours_box, timeout_row.minutes_box]:
            spinbox.valueChanged.connect(self._update_ui_state)

    def _update_ui_state(self):
        """
        Updates the complete UI state including all timeout rows and message labels.
        """
        self._update_error_states()
        self._update_warning_states()
        self._update_row_states()

    def _update_error_states(self):
        """
        Updates the error message label based on validation of all timeout rows.
        """
        any_zero = any(
            row.get_timeout_seconds() == 0 and row.checkbox.isChecked() for row in self.timeout_rows
        )
        self.error_label.setText("Error: Timeout cannot be set to zero." if any_zero else "")
        self.error_label.setVisible(any_zero)

    def _update_warning_states(self):
        """
        Updates the warning message label based on the state of all timeout rows.
        """
        any_disabled = any(not row.checkbox.isChecked() for row in self.timeout_rows)
        warning_text = (
            "Warning: Without a specified timeout, tasks may run indefinitely if issues occur, "
            "or they will use OpenJD's default timeouts if they exist."
            if any_disabled
            else ""
        )
        self.warning_label.setText(warning_text)
        self.warning_label.setVisible(any_disabled)

    def _update_row_states(self):
        """
        Updates the state of all timeout rows.
        """
        for row in self.timeout_rows:
            row.update_state()

    def refresh_ui(self, entries: List[TimeoutEntry]):
        """
        Refreshes all UI elements to reflect the current timeout settings.

        Args:
            entries: List of TimeoutEntry objects containing the current settings
        """
        for index in range(len(entries)):
            current_row = self.timeout_rows[index]
            current_entry = entries[index]

            current_row.checkbox.setChecked(current_entry.is_activated)
            current_row.set_timeout(current_entry.seconds)

        self._update_ui_state()

    def update_settings(self, settings):
        """
        Updates the settings object with the current values from the UI.

        Args:
            settings: Settings object to update with current timeout values
        """
        for index in range(len(settings.timeouts)):
            current_row = self.timeout_rows[index]
            current_entry = settings.timeouts[index]

            current_entry.is_activated = current_row.checkbox.isChecked()
            current_entry.seconds = current_row.get_timeout_seconds()
