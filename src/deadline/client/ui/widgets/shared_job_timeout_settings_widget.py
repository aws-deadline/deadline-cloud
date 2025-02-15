# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
A UI Widget containing the timeout settings widget.
"""

from __future__ import annotations

from typing import Any

from qtpy.QtWidgets import (  # type: ignore
    QFormLayout,
    QGroupBox,
    QLabel,
    QMessageBox,
    QSpinBox,
    QCheckBox,
    QGridLayout,
    QAbstractSpinBox,
)


SECONDS_IN_A_MINUTE = 60
SECONDS_IN_AN_HOUR = 60 * 60
SECONDS_IN_A_DAY = SECONDS_IN_AN_HOUR * 24


class SharedJobTimeoutSettingsWidget(QGroupBox):  # pylint: disable=too-few-public-methods
    """
    UI element to hold timeout settings of the submission

    The settings object should be a dataclass with:
      - `timeout_settings: TimeoutSettings`  The timeout settings to be applied for the job.
    """

    def __init__(self, *, initial_settings, parent=None):
        super().__init__("Timeout settings", parent=parent)

        self._build_ui()
        self.refresh_ui(initial_settings)

    def _build_ui(self):
        self.layout = QFormLayout(self)
        self.layout.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)

        self.activate_timeouts_checkbox = QCheckBox("Use timeouts", self)
        self.activate_timeouts_checkbox.setChecked(True)
        self.activate_timeouts_checkbox.clicked.connect(self.activate_timeouts_changed)
        self.activate_timeouts_checkbox.setToolTip(
            "Set a maximum duration for actions from this job. See AWS Deadline Cloud documentation to learn more."
        )
        self.activate_timeouts_checkbox_subtext = QLabel(
            "Set a maximum duration for actions from this job."
        )
        self.activate_timeouts_checkbox_subtext.setStyleSheet("font-style: italic")
        self.layout.addRow(self.activate_timeouts_checkbox, self.activate_timeouts_checkbox_subtext)

        self.timeouts_box = QGroupBox()
        timeouts_layout = QGridLayout(self.timeouts_box)

        def create_timeout_row(label, tooltip, row):
            qlabel = QLabel(label)
            qlabel.setToolTip(tooltip)
            timeouts_layout.addWidget(qlabel, row, 0)

            days_box = QSpinBox(self, minimum=0, maximum=365)
            days_box.setSuffix(" days")
            days_box.setButtonSymbols(QAbstractSpinBox.UpDownArrows)
            timeouts_layout.addWidget(days_box, row, 1)

            hours_box = QSpinBox(self, minimum=0, maximum=23)
            hours_box.setSuffix(" hours")
            timeouts_layout.addWidget(hours_box, row, 2)

            minutes_box = QSpinBox(self, minimum=0, maximum=59)
            minutes_box.setSuffix(" minutes")
            timeouts_layout.addWidget(minutes_box, row, 3)

            return qlabel, days_box, hours_box, minutes_box

        def hookup_zero_callback(timeout_boxes: tuple[QLabel, QSpinBox, QSpinBox, QSpinBox]):
            def indicate_is_valid_callback(value: int):
                self.indicate_if_valid(timeout_boxes)

            for timeout_box in timeout_boxes[1:]:
                timeout_box.valueChanged.connect(indicate_is_valid_callback)

        self.on_run_timeouts = create_timeout_row(
            label="Render Task Timeout",
            tooltip="Maximum duration of each action which performs a render.",
            row=0,
        )
        hookup_zero_callback(self.on_run_timeouts)

        self.on_enter_timeouts = create_timeout_row(
            label="Setup Timeout",
            tooltip="Maximum duration of each action which sets up the job for rendering, such as scene load.",
            row=1,
        )
        hookup_zero_callback(self.on_enter_timeouts)

        self.on_exit_timeouts = create_timeout_row(
            label="Teardown Timeout",
            tooltip="Maximum duration of action which tears down the setup required for rendering.",
            row=2,
        )
        hookup_zero_callback(self.on_exit_timeouts)

        self.layout.addRow(self.timeouts_box)

    def refresh_ui(self, settings: Any):
        self.activate_timeouts_checkbox.setChecked(settings.timeout_settings.is_activated)

        def _set_timeout(
            timeout_boxes: tuple[QLabel, QSpinBox, QSpinBox, QSpinBox], timeout_seconds: int
        ):
            days = timeout_seconds // 86400
            hours = (timeout_seconds % 86400) // 3600
            minutes = (timeout_seconds % 3600) // 60
            timeout_boxes[1].setValue(days)
            timeout_boxes[2].setValue(hours)
            timeout_boxes[3].setValue(minutes)

        _set_timeout(self.on_run_timeouts, settings.timeout_settings.on_run_timeout_seconds)
        _set_timeout(self.on_enter_timeouts, settings.timeout_settings.on_enter_timeout_seconds)
        _set_timeout(self.on_exit_timeouts, settings.timeout_settings.on_exit_timeout_seconds)

        self.activate_timeouts_changed(warn=False)  # don't warn when loading from sticky settings

    def update_settings(self, settings: Any):
        """
        Update a given instance of scene settings with updated values.
        """
        settings.timeout_settings.is_activated = self.activate_timeouts_checkbox.isChecked()
        settings.timeout_settings.on_run_timeout_seconds = self.on_run_timeout_seconds
        settings.timeout_settings.on_enter_timeout_seconds = self.on_enter_timeout_seconds
        settings.timeout_settings.on_exit_timeout_seconds = self.on_exit_timeout_seconds

    def indicate_if_valid(self, timeout_boxes: tuple[QLabel, QSpinBox, QSpinBox, QSpinBox]):
        if (
            self._calculate_timeout_seconds(timeout_boxes) == 0
            and not self.timeouts_box.isChecked()
        ):
            timeout_boxes[0].setStyleSheet("color: red")
        else:
            timeout_boxes[0].setStyleSheet("")

        # If the spin box has a value of 1, we should not make the suffix plural.
        for box in timeout_boxes[1:4]:
            if box.value() == 1:
                box.setSuffix(box.suffix().strip("s"))
            elif not box.suffix().endswith("s"):
                box.setSuffix(box.suffix() + "s")

    def activate_timeouts_changed(self, _=None, warn=True):
        is_checkbox_checked = self.activate_timeouts_checkbox.isChecked()
        if not is_checkbox_checked and warn:
            result = QMessageBox.warning(
                self,
                "Warning",
                "Removing timeouts in your submission can result in a task that runs indefinitely. Are you sure you want to remove timeouts?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if result == QMessageBox.No:
                self.activate_timeouts_checkbox.setChecked(True)
        for timeout_boxes in (self.on_run_timeouts, self.on_enter_timeouts, self.on_exit_timeouts):
            for timeout_box in timeout_boxes:
                is_checkbox_checked = self.activate_timeouts_checkbox.isChecked()
                timeout_box.setEnabled(is_checkbox_checked)
            self.indicate_if_valid(timeout_boxes)

    def _calculate_timeout_seconds(
        self, timeout_boxes: tuple[QLabel, QSpinBox, QSpinBox, QSpinBox]
    ):
        return (
            timeout_boxes[1].value() * SECONDS_IN_A_DAY
            + timeout_boxes[2].value() * SECONDS_IN_AN_HOUR
            + timeout_boxes[3].value() * SECONDS_IN_A_MINUTE
        )

    @property
    def on_run_timeout_seconds(self):
        return self._calculate_timeout_seconds(self.on_run_timeouts)

    @property
    def on_enter_timeout_seconds(self):
        return self._calculate_timeout_seconds(self.on_enter_timeouts)

    @property
    def on_exit_timeout_seconds(self):
        return self._calculate_timeout_seconds(self.on_exit_timeouts)
