# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""GUI tests for UI utilities and custom spinbox widgets."""

import warnings

import pytest
from qtpy.QtWidgets import QWidget

from deadline.client.ui._utils import block_signals, CancelationFlag
from deadline.client.ui.widgets.spinbox_widgets import (
    DecimalMode,
    FloatDragSpinBox,
    IntDragSpinBox,
)


# ---------- block_signals ----------


class TestBlockSignals:
    """Tests for the block_signals context manager."""

    def test_signals_blocked_inside_context_and_restored(self, qtbot):
        """Signals are blocked inside the context manager and restored after exit."""
        widget = QWidget()
        qtbot.addWidget(widget)

        assert not widget.signalsBlocked(), "Signals should not be blocked initially"

        with block_signals(widget):
            assert widget.signalsBlocked(), "Signals should be blocked inside context"

        assert not widget.signalsBlocked(), "Signals should be restored after context"

    def test_signals_stay_blocked_if_already_blocked(self, qtbot):
        """If signals were already blocked, they remain blocked after exit."""
        widget = QWidget()
        qtbot.addWidget(widget)

        widget.blockSignals(True)
        assert widget.signalsBlocked()

        with block_signals(widget):
            assert widget.signalsBlocked()

        assert widget.signalsBlocked(), (
            "Signals should remain blocked because they were blocked before entering"
        )


# ---------- CancelationFlag ----------


class TestCancelationFlag:
    """Tests for the CancelationFlag helper class."""

    def test_initial_state_and_set_canceled(self):
        """CancelationFlag starts as False; set_canceled makes it True; bool works."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            flag = CancelationFlag()

        assert not flag, "Flag should be falsy initially"
        assert flag.canceled is False

        flag.set_canceled()

        assert flag, "Flag should be truthy after set_canceled"
        assert flag.canceled is True

    def test_emits_deprecation_warning(self):
        """Creating a CancelationFlag emits a DeprecationWarning."""
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            CancelationFlag()

        deprecation_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        assert len(deprecation_warnings) == 1
        assert "deprecated" in str(deprecation_warnings[0].message).lower()


# ---------- FloatDragSpinBox ----------


class TestFloatDragSpinBox:
    """Tests for the FloatDragSpinBox widget."""

    def test_default_range(self, qtbot):
        """Default range should be -1e308 to 1e308."""
        spin = FloatDragSpinBox()
        qtbot.addWidget(spin)

        assert spin.maximum() == FloatDragSpinBox.MAX_FLOAT_VALUE
        assert spin.minimum() == FloatDragSpinBox.MIN_FLOAT_VALUE

    def test_drag_multiplier_getter_setter(self, qtbot):
        """setDragMultiplier stores the value, and negative values clamp to 0."""
        spin = FloatDragSpinBox()
        qtbot.addWidget(spin)

        spin.setDragMultiplier(2.5)
        assert spin.dragMultiplier() == 2.5

        spin.setDragMultiplier(-1.0)
        assert spin.dragMultiplier() == 0.0, "Negative multiplier should clamp to 0.0"

        spin.setDragMultiplier(0.0)
        assert spin.dragMultiplier() == 0.0

    def test_decimal_mode_getter_setter(self, qtbot):
        """setDecimalMode / decimalMode round-trip correctly."""
        spin = FloatDragSpinBox()
        qtbot.addWidget(spin)

        # Default is ADAPTIVE_DECIMAL per __init__
        assert spin.decimalMode() == DecimalMode.ADAPTIVE_DECIMAL

        spin.setDecimalMode(DecimalMode.FIXED_DECIMAL)
        assert spin.decimalMode() == DecimalMode.FIXED_DECIMAL

        spin.setDecimalMode(DecimalMode.ADAPTIVE_DECIMAL)
        assert spin.decimalMode() == DecimalMode.ADAPTIVE_DECIMAL

    def test_set_value_within_range(self, qtbot):
        """setValue works and the value is retrievable."""
        spin = FloatDragSpinBox()
        qtbot.addWidget(spin)

        spin.setValue(42.5)
        assert spin.value() == pytest.approx(42.5)

        spin.setValue(-100.25)
        assert spin.value() == pytest.approx(-100.25)

        spin.setValue(0.0)
        assert spin.value() == pytest.approx(0.0)


# ---------- IntDragSpinBox ----------


class TestIntDragSpinBox:
    """Tests for the IntDragSpinBox widget."""

    def test_default_range(self, qtbot):
        """Default range should be MIN_INT_VALUE to MAX_INT_VALUE."""
        spin = IntDragSpinBox()
        qtbot.addWidget(spin)

        assert spin.maximum() == IntDragSpinBox.MAX_INT_VALUE
        assert spin.minimum() == IntDragSpinBox.MIN_INT_VALUE
        assert IntDragSpinBox.MAX_INT_VALUE == (2**31) - 1
        assert IntDragSpinBox.MIN_INT_VALUE == -(2**31) + 1

    def test_drag_multiplier_getter_setter(self, qtbot):
        """setDragMultiplier stores the value, and negative values clamp to 0."""
        spin = IntDragSpinBox()
        qtbot.addWidget(spin)

        spin.setDragMultiplier(3.0)
        assert spin.dragMultiplier() == 3.0

        spin.setDragMultiplier(-5.0)
        assert spin.dragMultiplier() == 0.0, "Negative multiplier should clamp to 0.0"

    def test_set_value_and_value(self, qtbot):
        """setValue and value() work correctly for integer values."""
        spin = IntDragSpinBox()
        qtbot.addWidget(spin)

        spin.setValue(100)
        assert spin.value() == 100

        spin.setValue(-999)
        assert spin.value() == -999

        spin.setValue(0)
        assert spin.value() == 0
