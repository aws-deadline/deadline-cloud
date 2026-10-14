# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for the SPIN_BOX_LIST control used by LIST[INT] and LIST[FLOAT] job parameters."""

import logging
from unittest.mock import MagicMock, patch

import pytest
from qtpy.QtCore import QEvent, QPoint, QPointF, Qt
from qtpy.QtGui import QHoverEvent, QMouseEvent
from qtpy.QtWidgets import QAbstractItemView, QApplication, QStyle

from deadline.client.ui.widgets.openjd_parameters_widget import (
    OpenJDParametersWidget,
    _JobTemplateHiddenWidget,
    _JobTemplateSpinBoxListWidget,
    _SpinBoxListDelegate,
)
from deadline.client.ui.widgets.spinbox_widgets import (
    DecimalMode,
    FloatDragSpinBox,
    IntDragSpinBox,
)


def _list_param(name="Frames", list_type="LIST[INT]", default=None, control=None, **extra):
    param = {"name": name, "type": list_type, **extra}
    if control is not None:
        param.setdefault("userInterface", {})["control"] = control
    if default is not None:
        param["default"] = default
    return param


def _make(qtbot, *params):
    widget = OpenJDParametersWidget(parameter_definitions=list(params))
    qtbot.addWidget(widget)
    return widget


def _spin_values(control):
    return [control.spin_box(i).value() for i in range(control.edit_control.count())]


class TestSpinBoxListWidget:
    @pytest.mark.parametrize(
        ("list_type", "default", "spin_box_type"),
        [
            pytest.param("LIST[INT]", [1, 5, 10], IntDragSpinBox, id="int"),
            pytest.param("LIST[FLOAT]", [0.5, 2.0], FloatDragSpinBox, id="float"),
        ],
    )
    def test_default_control_is_a_spin_box_per_row(self, qtbot, list_type, default, spin_box_type):
        """Without a control, a numeric list uses SPIN_BOX_LIST, where every row is the
        spin box the scalar INT or FLOAT parameter uses."""
        widget = _make(qtbot, _list_param(list_type=list_type, default=default))
        control = widget.controls["Frames"]
        assert isinstance(control, _JobTemplateSpinBoxListWidget)
        assert control.value() == default
        assert widget.get_parameters()[0]["value"] == default
        assert control.count_label.text() == f"Items: {len(default)}"
        assert [type(control.spin_box(i)) for i in range(len(default))] == [spin_box_type] * len(
            default
        )
        assert _spin_values(control) == default

    def test_float_list_converts_int_items(self, qtbot):
        widget = _make(qtbot, _list_param(list_type="LIST[FLOAT]", default=[1, 2]))
        value = widget.controls["Frames"].value()
        assert value == [1.0, 2.0]
        assert all(type(v) is float for v in value)

    def test_no_default_starts_empty(self, qtbot):
        widget = _make(qtbot, _list_param())
        control = widget.controls["Frames"]
        assert control.value() == []
        assert control.is_valid()
        assert control.add_button.isEnabled()
        assert not control.remove_button.isEnabled()
        # There is no Edit button: every row is already a spin box.
        assert not hasattr(control, "edit_button")

    def test_value_takes_precedence_over_default(self, qtbot):
        widget = _make(qtbot, _list_param(default=[1], value=[2, 3]))
        assert widget.controls["Frames"].value() == [2, 3]

    def test_editing_a_spin_box_changes_the_value(self, qtbot):
        widget = _make(qtbot, _list_param(default=[1, 5, 10]))
        control = widget.controls["Frames"]
        changed = MagicMock()
        widget.parameter_changed.connect(changed)

        control.spin_box(1).setValue(7)
        assert control.value() == [1, 7, 10]
        assert changed.call_args.args[0]["value"] == [1, 7, 10]

    def test_typing_into_a_spin_box_changes_the_value(self, qtbot):
        widget = _make(qtbot, _list_param(list_type="LIST[FLOAT]", default=[1.0]))
        control = widget.controls["Frames"]
        widget.show()
        qtbot.waitExposed(widget)
        spin_box = control.spin_box(0)
        spin_box.lineEdit().selectAll()
        qtbot.keyClicks(spin_box, "2.25")
        qtbot.keyClick(spin_box, Qt.Key.Key_Return)
        assert control.value() == [2.25]

    def test_spin_box_steps_change_the_value(self, qtbot):
        widget = _make(
            qtbot,
            _list_param(default=[10], userInterface={"singleStepDelta": 5}),
        )
        control = widget.controls["Frames"]
        control.spin_box(0).stepUp()
        assert control.value() == [15]

    def test_add_copies_the_last_value_and_focuses_it(self, qtbot):
        widget = _make(qtbot, _list_param(default=[1, 4]))
        control = widget.controls["Frames"]
        changed = MagicMock()
        widget.parameter_changed.connect(changed)
        widget.show()
        qtbot.waitExposed(widget)

        control.add_button.click()
        QApplication.processEvents()
        assert control.value() == [1, 4, 4]
        assert changed.call_args.args[0]["value"] == [1, 4, 4]
        assert control.count_label.text() == "Items: 3"
        assert isinstance(control.spin_box(2), IntDragSpinBox)
        assert control.edit_control.currentRow() == 2
        assert QApplication.focusWidget() is control.spin_box(2)

    @pytest.mark.parametrize(
        ("item", "expected"),
        [
            pytest.param({}, 0, id="zero"),
            pytest.param({"minValue": 5}, 5, id="min-above-zero"),
            pytest.param({"maxValue": -3}, -3, id="max-below-zero"),
            pytest.param({"allowedValues": [7, 9]}, 7, id="first-allowed"),
        ],
    )
    def test_add_to_empty_list_uses_a_valid_value(self, qtbot, item, expected):
        widget = _make(qtbot, _list_param(item=item))
        control = widget.controls["Frames"]
        control.add_button.click()
        assert control.value() == [expected]
        assert control.is_valid()

    def test_add_to_empty_float_list_is_float(self, qtbot):
        widget = _make(qtbot, _list_param(list_type="LIST[FLOAT]"))
        control = widget.controls["Frames"]
        control.add_button.click()
        assert control.value() == [0.0]
        assert type(control.value()[0]) is float

    def test_focusing_a_spin_box_selects_its_row_for_remove(self, qtbot):
        widget = _make(qtbot, _list_param(default=[1, 2, 3]))
        control = widget.controls["Frames"]
        widget.show()
        qtbot.waitExposed(widget)

        control.spin_box(1).setFocus()
        QApplication.processEvents()
        assert control.edit_control.currentRow() == 1
        assert control.remove_button.isEnabled()
        qtbot.mouseClick(control.remove_button, Qt.MouseButton.LeftButton)
        assert control.value() == [1, 3]
        assert _spin_values(control) == [1, 3]

    def test_remove_without_selection_is_disabled(self, qtbot):
        widget = _make(qtbot, _list_param(default=[1, 2]))
        control = widget.controls["Frames"]
        control.edit_control.setCurrentRow(0)
        assert control.remove_button.isEnabled()
        control.edit_control.clearSelection()
        assert not control.remove_button.isEnabled()
        control.remove_button.click()
        assert control.value() == [1, 2]

    def test_reorder_keeps_spin_boxes_with_their_values(self, qtbot):
        """A drag moves rows through a model row move; every row keeps a spin box
        showing its value."""
        widget = _make(qtbot, _list_param(default=[1, 2, 3]))
        control = widget.controls["Frames"]
        changed = MagicMock()
        widget.parameter_changed.connect(changed)

        model = control.edit_control.model()
        assert model.moveRow(model.index(0, 0).parent(), 2, model.index(0, 0).parent(), 0)
        QApplication.processEvents()

        assert control.value() == [3, 1, 2]
        assert changed.call_args.args[0]["value"] == [3, 1, 2]
        assert _spin_values(control) == [3, 1, 2]

    def test_items_are_draggable_but_not_text_editable(self, qtbot):
        widget = _make(qtbot, _list_param(default=[1]))
        flags = widget.controls["Frames"].edit_control.item(0).flags()
        assert flags & Qt.ItemFlag.ItemIsDragEnabled
        assert not flags & Qt.ItemFlag.ItemIsEditable
        assert not flags & Qt.ItemFlag.ItemIsDropEnabled

    def test_pressing_the_drag_strip_starts_a_drag(self, qtbot):
        """Pressing a row's strip and moving starts dragging the row. The view must not
        hand the press to the row's spin box, which would make the move change the
        selection instead, until the pointer left the list."""
        widget = _make(qtbot, _list_param(default=[1, 2, 3]))
        control = widget.controls["Frames"]
        widget.show()
        qtbot.waitExposed(widget)
        list_widget = control.edit_control
        viewport = list_widget.viewport()
        row_rect = list_widget.visualItemRect(list_widget.item(0))
        start = QPoint(row_rect.left() + 4, row_rect.center().y())

        def send(event_type, pos, buttons):
            event = QMouseEvent(
                event_type,
                QPointF(pos),
                QPointF(viewport.mapToGlobal(pos)),
                Qt.MouseButton.LeftButton,
                buttons,
                Qt.KeyboardModifier.NoModifier,
            )
            QApplication.sendEvent(viewport, event)

        send(QEvent.Type.MouseButtonPress, start, Qt.MouseButton.LeftButton)
        assert list_widget.selectedItems() == [list_widget.item(0)]
        assert QApplication.focusWidget() is not control.spin_box(0)
        # startDrag is not run: an offscreen drag would return at once and reset the state.
        with patch.object(list_widget, "startDrag"):
            send(QEvent.Type.MouseMove, start + QPoint(0, 3), Qt.MouseButton.LeftButton)
            assert list_widget.state() == QAbstractItemView.State.DraggingState
        send(QEvent.Type.MouseButtonRelease, start + QPoint(0, 3), Qt.MouseButton.NoButton)

    def test_hovered_row_is_not_highlighted(self, qtbot, monkeypatch):
        """Each row is a spin box that shows its own hover state, so the row behind it is
        not painted as hovered."""
        painted = []
        original_paint = _SpinBoxListDelegate.paint

        def recording_paint(self, painter, option, index):
            painted.append((index.row(), bool(option.state & QStyle.StateFlag.State_MouseOver)))
            original_paint(self, painter, option, index)

        monkeypatch.setattr(_SpinBoxListDelegate, "paint", recording_paint)
        widget = _make(qtbot, _list_param(default=[1, 2]))
        list_widget = widget.controls["Frames"].edit_control
        widget.show()
        qtbot.waitExposed(widget)
        viewport = list_widget.viewport()
        rect = list_widget.visualItemRect(list_widget.item(1))
        pos = QPointF(rect.left() + 4, rect.center().y())
        for event_type in (QEvent.Type.HoverEnter, QEvent.Type.HoverMove):
            QApplication.sendEvent(viewport, QHoverEvent(event_type, pos, pos, QPointF(-1, -1)))

        painted.clear()
        viewport.grab()
        assert {row for row, _ in painted} == {0, 1}
        assert not any(hovered for _, hovered in painted)

    def test_spin_boxes_leave_row_drops_to_the_list(self, qtbot):
        """A row dragged over the other rows' spin boxes must reach the list, which moves
        it, rather than a spin box's line edit, which would refuse it."""
        widget = _make(qtbot, _list_param(default=[1, 2, 3]))
        control = widget.controls["Frames"]
        assert control.edit_control.viewport().acceptDrops()
        for row in range(3):
            spin_box = control.spin_box(row)
            assert not spin_box.acceptDrops()
            assert not spin_box.lineEdit().acceptDrops()
        control.add_button.click()
        assert not control.spin_box(3).lineEdit().acceptDrops()

    def test_duplicates_and_order_preserved(self, qtbot):
        widget = _make(qtbot, _list_param(default=[3, 1, 3]))
        assert widget.controls["Frames"].value() == [3, 1, 3]

    def test_item_constraints_configure_every_spin_box(self, qtbot):
        widget = _make(
            qtbot,
            _list_param(
                list_type="LIST[FLOAT]",
                default=[0.5, 1.0],
                item={"minValue": 0.0, "maxValue": 10.0},
                userInterface={"decimals": 2, "singleStepDelta": 0.25},
                description="Scale factors",
            ),
        )
        control = widget.controls["Frames"]
        for row in range(2):
            spin_box = control.spin_box(row)
            assert spin_box.minimum() == 0.0
            assert spin_box.maximum() == 10.0
            assert spin_box.decimalMode() == DecimalMode.FIXED_DECIMAL
            assert spin_box.decimals() == 2
            assert spin_box.singleStep() == 0.25
            assert spin_box.toolTip() == "Scale factors"
        control.add_button.click()
        assert control.spin_box(2).maximum() == 10.0

    def test_int_item_range_beyond_spin_box_range_is_clamped(self, qtbot):
        """The spin box range is limited to what the spin box can show."""
        widget = _make(
            qtbot, _list_param(default=[0], item={"minValue": -(2**63), "maxValue": 2**63 - 1})
        )
        spin_box = widget.controls["Frames"].spin_box(0)
        assert spin_box.minimum() == IntDragSpinBox.MIN_INT_VALUE
        assert spin_box.maximum() == IntDragSpinBox.MAX_INT_VALUE

    def test_value_outside_item_range_is_kept_and_marked_invalid(self, qtbot):
        """A value from the bundle outside the item range is not silently clamped by the
        spin box, so it is reported instead of submitted as a different number."""
        widget = _make(qtbot, _list_param(default=[1, 500, 3], item={"maxValue": 100}))
        control = widget.controls["Frames"]

        assert control.value() == [1, 500, 3]
        # The spin box shows the value itself, not the nearest bound.
        assert _spin_values(control) == [1, 500, 3]
        assert control.spin_box(1).lineEdit().text() == "500"
        assert not control.is_valid()
        assert widget.invalid_parameter_names() == ["Frames"]
        items = [control.edit_control.item(i) for i in range(3)]
        assert [item.icon().isNull() for item in items] == [True, False, True]
        assert "500 is greater than item maxValue 100" in items[1].toolTip()
        assert "500 is greater than item maxValue 100" in control.spin_box(1).toolTip()
        assert "red" in control.edit_control.styleSheet()

        control.spin_box(1).setValue(50)
        assert control.value() == [1, 50, 3]
        assert control.is_valid()
        assert items[1].icon().isNull()
        assert control.edit_control.styleSheet() == ""
        # Once the value is back in range, the spin box's range is the item range again.
        assert control.spin_box(1).maximum() == 100

    def test_float_value_outside_item_range_is_shown(self, qtbot):
        widget = _make(
            qtbot,
            _list_param(
                list_type="LIST[FLOAT]",
                default=[500.0, -2.5],
                item={"minValue": 0.0, "maxValue": 100.0},
            ),
        )
        control = widget.controls["Frames"]
        assert control.value() == [500.0, -2.5]
        assert _spin_values(control) == [500.0, -2.5]
        assert not control.is_valid()

    def test_ints_beyond_the_spin_box_range_are_shown_and_kept(self, qtbot):
        """LIST[INT] is 64-bit but a spin box holds 32-bit ints. Such a value is shown as
        its own text and submitted unchanged, rather than shown as the spin box's bound."""
        values = [2**63 - 1, -(2**63), 2**31, 5]
        widget = _make(qtbot, _list_param(default=values))
        control = widget.controls["Frames"]

        assert control.value() == values
        assert control.is_valid()
        assert [control.spin_box(i).lineEdit().text() for i in range(4)] == [str(v) for v in values]

        # Stepping edits the value, starting from the spin box's minimum.
        spin_box = control.spin_box(0)
        spin_box.stepUp()
        assert spin_box.value() > spin_box.minimum()
        assert control.value()[0] == spin_box.value()
        assert spin_box.specialValueText() == ""

    def test_ints_beyond_64_bits_degrade_to_empty(self, qtbot, caplog):
        """A value from an untrusted bundle can be any size of int, which Qt cannot hold."""
        widget = _make(qtbot, _list_param(default=[1]))
        control = widget.controls["Frames"]
        with caplog.at_level(logging.WARNING):
            control.set_value([2**70, 1])
        assert control.value() == []
        assert "not a list of 64-bit integers" in caplog.text

    def test_in_progress_edit_survives_changes_to_other_rows(self, qtbot):
        """A spin box commits typed text on Enter or focus loss. A change to another row
        refreshes every row's feedback and must not revert the text being typed."""
        widget = _make(qtbot, _list_param(default=[1, 2]))
        control = widget.controls["Frames"]
        widget.show()
        qtbot.waitExposed(widget)

        editing = control.spin_box(0)
        editing.setFocus()
        editing.lineEdit().selectAll()
        qtbot.keyClicks(editing, "42")
        control.spin_box(1).setValue(9)
        assert editing.lineEdit().text() == "42"
        assert control.value() == [1, 9]

        qtbot.keyClick(editing, Qt.Key.Key_Return)
        assert control.value() == [42, 9]
        assert editing.lineEdit().text() == "42"

    def test_resize_grip_does_not_cover_a_spin_box(self, qtbot):
        widget = _make(qtbot, _list_param(default=[1, 2, 3]))
        control = widget.controls["Frames"]
        widget.show()
        qtbot.waitExposed(widget)
        QApplication.processEvents()

        grip = control.corner_grip
        assert grip.isVisible()
        for row in range(3):
            spin_box = control.spin_box(row)
            spin_box_rect = spin_box.geometry().translated(
                spin_box.parentWidget().mapTo(control.edit_control, QPoint(0, 0))
            )
            assert not spin_box_rect.intersects(grip.geometry())

    def test_item_allowed_values_feedback(self, qtbot):
        widget = _make(qtbot, _list_param(default=[1], item={"allowedValues": [1, 2]}))
        control = widget.controls["Frames"]
        control.set_value([1, 3])
        assert not control.is_valid()
        assert "3 is not an allowed value" in control.edit_control.item(1).toolTip()

    def test_list_length_feedback(self, qtbot):
        widget = _make(qtbot, _list_param(default=[1], minLength=1))
        control = widget.controls["Frames"]
        assert control.is_valid()
        control.set_value([])
        assert not control.is_valid()
        assert "has 0 items but minLength is 1" in control.edit_control.toolTip()

    def test_add_disabled_at_max_length(self, qtbot):
        widget = _make(qtbot, _list_param(default=[1], maxLength=2))
        control = widget.controls["Frames"]
        assert control.add_button.isEnabled()
        control.add_button.click()
        assert not control.add_button.isEnabled()

    def test_only_the_focused_row_shows_selected_text(self, qtbot):
        widget = _make(qtbot, _list_param(list_type="LIST[FLOAT]", default=[0.5, 1.5]))
        control = widget.controls["Frames"]
        widget.show()
        qtbot.waitExposed(widget)
        QApplication.processEvents()
        assert [control.spin_box(i).lineEdit().hasSelectedText() for i in range(2)] == [
            False,
            False,
        ]
        control.add_button.click()
        QApplication.processEvents()
        assert [control.spin_box(i).lineEdit().hasSelectedText() for i in range(3)] == [
            False,
            False,
            True,
        ]

    def test_rows_fit_a_spin_box_and_three_are_shown(self, qtbot):
        widget = _make(qtbot, _list_param(default=[1, 2, 3]))
        control = widget.controls["Frames"]
        widget.show()
        QApplication.processEvents()
        frame = 2 * control.edit_control.frameWidth()
        assert control.visible_rows() == 3
        assert control.edit_control.height() == frame + 3 * control._row_height
        assert control._row_height >= control.spin_box(0).sizeHint().height()
        assert control.edit_control.verticalScrollBar().maximum() == 0

    def test_resize_clamps_to_the_default_visible_row_cap(self, qtbot):
        widget = _make(qtbot, _list_param(default=[1]))
        control = widget.controls["Frames"]
        control.set_visible_rows(1)
        assert control.visible_rows() == 3
        control.resize_to_height(control._row_height * 10)
        assert 8 <= control.visible_rows() <= 10

    def test_set_value_accepts_json_string(self, qtbot):
        widget = _make(qtbot, _list_param(list_type="LIST[FLOAT]"))
        widget.set_parameter_value({"name": "Frames", "value": "[0.5, 2]"})
        assert widget.controls["Frames"].value() == [0.5, 2.0]

    @pytest.mark.parametrize(
        ("list_type", "bad"),
        [
            ("LIST[INT]", "not json"),
            ("LIST[INT]", 5),
            ("LIST[INT]", [1, "2"]),
            ("LIST[INT]", [1.5]),
            ("LIST[INT]", [True]),
            ("LIST[FLOAT]", ["0.5"]),
            ("LIST[FLOAT]", {"a": 1}),
        ],
    )
    def test_set_value_invalid_degrades_to_empty(self, qtbot, caplog, list_type, bad):
        widget = _make(qtbot, _list_param(list_type=list_type, default=[1]))
        control = widget.controls["Frames"]
        with caplog.at_level(logging.WARNING):
            control.set_value(bad)
        assert control.value() == []
        kind = "64-bit integers" if list_type == "LIST[INT]" else "numbers"
        assert f"not a list of {kind}" in caplog.text

    def test_set_parameter_value_emits_validity(self, qtbot):
        widget = _make(qtbot, _list_param(default=[1], minLength=1))
        validity = MagicMock()
        widget.valid_parameters.connect(validity)
        widget.set_parameter_value({"name": "Frames", "value": []})
        assert validity.call_args.args[0] is False
        widget.set_parameter_value({"name": "Frames", "value": [4]})
        assert validity.call_args.args[0] is True
        assert _spin_values(widget.controls["Frames"]) == [4]

    def test_rejects_top_level_allowed_values(self, qtbot):
        with pytest.raises(RuntimeError, match="must not provide field 'allowedValues'"):
            _JobTemplateSpinBoxListWidget(None, _list_param(allowedValues=[1]))


class TestHiddenListNumber:
    @pytest.mark.parametrize(
        ("list_type", "default"), [("LIST[INT]", [1, 2]), ("LIST[FLOAT]", [0.5])]
    )
    def test_hidden_default_and_value(self, qtbot, list_type, default):
        widget = _make(qtbot, _list_param(list_type=list_type, control="HIDDEN", default=default))
        control = widget.controls["Frames"]
        assert isinstance(control, _JobTemplateHiddenWidget)
        assert control.value() == default

    def test_hidden_without_default_is_empty_list(self, qtbot):
        widget = _make(qtbot, _list_param(control="HIDDEN"))
        assert widget.controls["Frames"].value() == []

    def test_hidden_converts_json_string(self, qtbot):
        widget = _make(qtbot, _list_param(list_type="LIST[FLOAT]", control="HIDDEN"))
        widget.set_parameter_value({"name": "Frames", "value": "[1, 2.5]"})
        assert widget.controls["Frames"].value() == [1.0, 2.5]

    def test_hidden_keeps_invalid_value_for_submission_to_report(self, qtbot):
        widget = _make(qtbot, _list_param(control="HIDDEN"))
        widget.set_parameter_value({"name": "Frames", "value": "1-10"})
        assert widget.controls["Frames"].value() == "1-10"
