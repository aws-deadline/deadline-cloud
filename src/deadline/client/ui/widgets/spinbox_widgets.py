# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

__all__ = ["DecimalMode", "FloatDragSpinBox", "IntDragSpinBox"]

import enum
from math import ceil, floor, log10

from qtpy.QtCore import QEvent, QObject, Qt
from qtpy.QtGui import QCursor, QKeyEvent, QMouseEvent
from qtpy.QtWidgets import QAbstractSpinBox, QDoubleSpinBox, QSpinBox, QWidget


class DecimalMode(enum.Enum):
    """
    Defines the valid Decimal Modes that can be used with a FloatDragSpinBox and
    IntDragSpinBox.
    """

    FIXED_DECIMAL = enum.auto()
    ADAPTIVE_DECIMAL = enum.auto()


class FloatDragSpinBox(QDoubleSpinBox):
    """
    A improved QDoubleSpinBox that adds the ability to click and drag the
    SpinBox arrow buttons to increment/decrement the value. This Widget also
    adds the ability to adaptively set the number of decimal places based on the
    size of the value.
    """

    MAX_ADAPTIVE_DECIMALS = 5
    MAX_FLOAT_VALUE = 1e308
    MIN_FLOAT_VALUE = -1e308

    def __init__(self, parent: QWidget = None):
        super().__init__(parent)

        self.setKeyboardTracking(False)
        self.setMouseTracking(False)
        self.setCursor(Qt.SizeVerCursor)
        self.setMaximum(self.MAX_FLOAT_VALUE)
        self.setMinimum(self.MIN_FLOAT_VALUE)
        self.setDragMultiplier(0.1)
        self.setDecimalMode(DecimalMode.ADAPTIVE_DECIMAL)
        self.setStepType(QDoubleSpinBox.AdaptiveDecimalStepType)
        self.lineEdit().installEventFilter(self)

        self.editingFinished.connect(self._stop_editing)
        self.valueChanged.connect(self._set_adaptive_decimals)

        self._cursor_start_pos = self.mapToGlobal(self.pos())
        self._editing = False
        self._dragging = False

    def mousePressEvent(self, event: QMouseEvent) -> None:
        """
        Hides the cursor on click and records the current cursor position to be
        used if the user then decides to start dragging the mouse.
        """
        super().mousePressEvent(event)

        self.setCursor(Qt.BlankCursor)
        self._cursor_start_pos = event.globalPos()

    def mouseMoveEvent(self, event: QMouseEvent) -> None:
        """
        Sets the Widget's value based on the position difference that the cursor
        was moved while holding the mouse button down.
        """
        super().mouseMoveEvent(event)

        self._dragging = True
        cursor_current_pos = event.globalPos()
        cursor_offset = self._cursor_start_pos.y() - cursor_current_pos.y()
        new_value = self.value() + (cursor_offset * self.dragMultiplier())
        new_value = min(new_value, self.maximum())
        new_value = max(new_value, self.minimum())

        self.setValue(new_value)
        QCursor.setPos(self._cursor_start_pos)

    def mouseReleaseEvent(self, event: QMouseEvent) -> None:
        """
        Makes the cursor visible again once the mouse click is released and sets
        that the user is no longer dragging the mouse.
        """
        super().mouseReleaseEvent(event)

        self.setCursor(Qt.SizeVerCursor)
        self._dragging = False

    def stepEnabled(self) -> QAbstractSpinBox.StepEnabled:  # type: ignore[name-defined]
        """
        Disables the arrow buttons if the user is using the hold and drag
        functionaltiy to change the value.
        """
        if self._dragging:
            return QAbstractSpinBox.StepNone
        return super().stepEnabled()

    def keyPressEvent(self, event: QKeyEvent) -> None:
        """
        Captures any key press events to set that the user is likely manually
        editing the QLineEdit that is part of the Widget.
        """
        self._start_editing()
        super().keyPressEvent(event)

    def eventFilter(self, object: QObject, event: QEvent) -> bool:
        """
        Captures when the user clicks on the QLineEdit that is part of the
        Widget and tries to set that the user is currently editing manually.
        """
        if object == self.lineEdit() and event.type() == QEvent.MouseButtonPress:
            self._start_editing()
        return False

    def decimalMode(self) -> DecimalMode:
        return self._decimal_mode

    def setDecimalMode(self, mode: DecimalMode) -> None:
        self._decimal_mode = mode

    def dragMultiplier(self) -> float:
        return self._drag_multiplier

    def setDragMultiplier(self, multiplier: float) -> None:
        self._drag_multiplier = max(0.0, multiplier)

    def _set_adaptive_decimals(self, value: float) -> None:
        """
        Adaptively sets the number of decimal places to use based on the current
        value when ADAPTIVE_DECIMAL mode is being used.
        """
        if self.decimalMode() == DecimalMode.ADAPTIVE_DECIMAL:
            if value < -1 or value > 1:  # Don't take log10 of (-1,1)
                self.setDecimals(max(0, self.MAX_ADAPTIVE_DECIMALS - floor(abs(log10(abs(value))))))
            else:
                self.setDecimals(self.MAX_ADAPTIVE_DECIMALS)

    def _start_editing(self) -> None:
        """
        Sets the number of decimal places to the max if using ADAPTIVE_DECIMAL
        mode. This is done while the user is editing in the QLineEdit so they
        aren't prevented from entering a small value that would have more
        decimal places than the current adaptive decimal places.
        """
        if not self._editing:
            if self.decimalMode() == DecimalMode.ADAPTIVE_DECIMAL:
                self.setDecimals(self.MAX_ADAPTIVE_DECIMALS)
            self._editing = True

    def _stop_editing(self) -> None:
        """
        Tries to set the number of decimal places back to being adaptively
        calculated once the user is finished editing. If ADAPTIVE_DECIMAL mode
        is not being used then nothing will be changed.
        """
        self._set_adaptive_decimals(self.value())
        self._editing = False


class IntDragSpinBox(QSpinBox):
    """
    A improved QSpinBox that adds the ability to click and drag the
    SpinBox arrow buttons to increment/decrement the value.
    """

    MAX_INT_VALUE = (2**31) - 1
    MIN_INT_VALUE = -(2**31) + 1

    def __init__(self, parent: QWidget = None) -> None:
        super().__init__(parent)
        self.setKeyboardTracking(False)
        self.setMouseTracking(False)
        self.setCursor(Qt.SizeVerCursor)
        self.setMaximum(self.MAX_INT_VALUE)
        self.setMinimum(self.MIN_INT_VALUE)
        self.setDragMultiplier(0.1)
        self.setStepType(QSpinBox.AdaptiveDecimalStepType)

        self._cursor_start_pos = self.mapToGlobal(self.pos())
        self._is_dragging = False

    def mousePressEvent(self, event: QMouseEvent) -> None:
        """
        Hides the cursor on click and records the current cursor position to be
        used if the user then decides to start dragging the mouse.
        """
        super().mousePressEvent(event)
        self.setCursor(Qt.BlankCursor)
        self._cursor_start_pos = event.globalPos()

    def mouseMoveEvent(self, event: QMouseEvent) -> None:
        """
        Sets the Widget's value based on the position difference that the cursor
        was moved while holding the mouse button down.
        """
        super().mouseMoveEvent(event)

        self._is_dragging = True

        cursor_current_pos = event.globalPos()
        cursor_offset = self._cursor_start_pos.y() - cursor_current_pos.y()
        new_value = self.value() + (cursor_offset * self.dragMultiplier())
        # Always increment / decrement by at least 1 regardless of the offset
        new_value = ceil(new_value) if cursor_offset > 0 else floor(new_value)
        new_value = min(new_value, self.maximum())
        new_value = max(new_value, self.minimum())

        self.setValue(new_value)
        QCursor.setPos(self._cursor_start_pos)

    def mouseReleaseEvent(self, event: QMouseEvent) -> None:
        """
        Makes the cursor visible again once the mouse click is released and sets
        that the user is no longer dragging the mouse.
        """
        super().mouseReleaseEvent(event)
        self.setCursor(Qt.SizeVerCursor)
        self._is_dragging = False

    def stepEnabled(self) -> QAbstractSpinBox.StepEnabled:  # type: ignore[name-defined]
        """
        Disables the arrow buttons if the user is using the hold and drag
        functionaltiy to change the value.
        """
        if self._is_dragging:
            return QAbstractSpinBox.StepNone
        return super().stepEnabled()

    def dragMultiplier(self) -> float:
        return self._drag_multiplier

    def setDragMultiplier(self, multiplier: float) -> None:
        self._drag_multiplier = max(0.0, multiplier)
