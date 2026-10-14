# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for the LINE_EDIT_LIST control used by LIST[STRING] job parameters."""

import logging
from unittest.mock import MagicMock

import pytest
from qtpy.QtCore import QEvent, QPointF, Qt
from qtpy.QtGui import QMouseEvent
from qtpy.QtWidgets import QApplication, QLineEdit

from deadline.client.ui.widgets.openjd_parameters_widget import (
    OpenJDParametersWidget,
    _JobTemplateHiddenWidget,
    _JobTemplateLineEditListWidget,
)


def _list_param(name="Cameras", default=None, control="LINE_EDIT_LIST", **extra):
    param = {
        "name": name,
        "type": "LIST[STRING]",
        "userInterface": {"control": control, "label": name},
        **extra,
    }
    if default is not None:
        param["default"] = default
    return param


def _make(qtbot, *params):
    widget = OpenJDParametersWidget(parameter_definitions=list(params))
    qtbot.addWidget(widget)
    return widget


def _texts(control):
    return [control.edit_control.item(i).text() for i in range(control.edit_control.count())]


def _type_into_open_editor(qtbot, control, text):
    """Types into the in-place editor Qt opened for the current item and presses Enter."""
    QApplication.processEvents()
    editor = control.edit_control.viewport().findChild(QLineEdit)
    assert editor is not None, "expected an open item editor"
    editor.setText(text)
    qtbot.keyClick(editor, Qt.Key.Key_Return)
    QApplication.processEvents()


class TestLineEditListWidget:
    def test_default_control_and_value(self, qtbot):
        """A LIST[STRING] parameter without a control uses LINE_EDIT_LIST, showing the default."""
        param = {"name": "Cameras", "type": "LIST[STRING]", "default": ["main", "closeup"]}
        widget = _make(qtbot, param)

        control = widget.controls["Cameras"]
        assert isinstance(control, _JobTemplateLineEditListWidget)
        assert control.value() == ["main", "closeup"]
        assert _texts(control) == ["main", "closeup"]
        assert control.count_label.text() == "Items: 2"
        assert widget.get_parameters()[0]["value"] == ["main", "closeup"]

    def test_no_default_starts_empty(self, qtbot):
        widget = _make(qtbot, _list_param())
        control = widget.controls["Cameras"]
        assert control.value() == []
        assert control.is_valid()
        # Nothing is selected, so only Add is available.
        assert control.add_button.isEnabled()
        assert not control.edit_button.isEnabled()
        assert not control.remove_button.isEnabled()

    def test_value_takes_precedence_over_default(self, qtbot):
        widget = _make(qtbot, _list_param(default=["a"], value=["b", "c"]))
        assert widget.controls["Cameras"].value() == ["b", "c"]

    def test_fixed_height_shows_three_rows(self, qtbot):
        """The list is sized to exactly 3 rows and does not stretch to fill the dialog."""
        assert _JobTemplateLineEditListWidget.IS_VERTICAL_EXPANDING is False
        widget = _make(qtbot, _list_param(default=["a"]))
        control = widget.controls["Cameras"]
        frame = 2 * control.edit_control.frameWidth()
        assert control.visible_rows() == 3
        assert control.edit_control.height() == frame + 3 * control._row_height
        assert control.edit_control.minimumHeight() == control.edit_control.maximumHeight()

    @pytest.mark.parametrize(
        "items",
        [
            pytest.param(["a", "b", "c"], id="valid"),
            pytest.param(["", "b", "c"], id="invalid-first"),
            pytest.param(["a", "", ""], id="invalid-later"),
        ],
    )
    def test_three_rows_fit_with_or_without_warning_icons(self, qtbot, items):
        """Rows keep one height whether or not they carry a warning icon, so 3 items fit
        exactly without a scroll bar."""
        widget = _make(qtbot, _list_param(item={"minLength": 1}))
        control = widget.controls["Cameras"]
        widget.show()
        control.set_value(items)
        QApplication.processEvents()
        list_widget = control.edit_control
        assert [not list_widget.item(i).icon().isNull() for i in range(3)] == [
            text == "" for text in items
        ]

        row_heights = {list_widget.visualItemRect(list_widget.item(i)).height() for i in range(3)}
        assert len(row_heights) == 1
        frame = 2 * list_widget.frameWidth()
        assert list_widget.height() == frame + 3 * row_heights.pop()
        assert list_widget.verticalScrollBar().maximum() == 0

    def test_scrolls_beyond_visible_rows(self, qtbot):
        widget = _make(qtbot, _list_param(default=["a", "b", "c"]))
        control = widget.controls["Cameras"]
        widget.show()
        QApplication.processEvents()
        scroll_bar = control.edit_control.verticalScrollBar()
        assert scroll_bar.maximum() == 0
        # With no scroll bar the grip sits in the list's corner.
        assert control.corner_grip.isVisible()

        control.set_value(["a", "b", "c", "d"])
        QApplication.processEvents()
        assert scroll_bar.maximum() > 0
        # With a scroll bar the grip moves into its area, below the down arrow.
        assert not control.corner_grip.isVisible()
        assert control.scroll_bar_grip.isVisible()
        assert (
            control.edit_control.horizontalScrollBarPolicy()
            == Qt.ScrollBarPolicy.ScrollBarAlwaysOff
        )

    def test_resize_snaps_to_whole_rows_and_clamps(self, qtbot):
        widget = _make(qtbot, _list_param())
        control = widget.controls["Cameras"]
        frame = 2 * control.edit_control.frameWidth()
        row = control._row_height

        control.resize_to_height(frame + int(5.4 * row))
        assert control.visible_rows() == 5
        assert control.edit_control.height() == frame + 5 * row
        control.resize_to_height(frame + int(5.6 * row))
        assert control.visible_rows() == 6
        control.resize_to_height(0)
        assert control.visible_rows() == 3
        control.set_visible_rows(1000)
        assert control.visible_rows() == 64

    def test_grip_drag_resizes(self, qtbot):
        widget = _make(qtbot, _list_param(default=["a"]))
        control = widget.controls["Cameras"]
        widget.show()
        QApplication.processEvents()
        grip = control.corner_grip
        row = control._row_height
        start = grip.mapToGlobal(grip.rect().center())

        def send(event_type, global_y, buttons):
            point = QPointF(start.x(), global_y)
            local = QPointF(grip.mapFromGlobal(point.toPoint()))
            event = QMouseEvent(
                event_type,
                local,
                point,
                Qt.MouseButton.LeftButton,
                buttons,
                Qt.KeyboardModifier.NoModifier,
            )
            QApplication.sendEvent(grip, event)

        send(QEvent.Type.MouseButtonPress, start.y(), Qt.MouseButton.LeftButton)
        send(QEvent.Type.MouseMove, start.y() + 2 * row, Qt.MouseButton.LeftButton)
        assert control.visible_rows() == 5
        send(QEvent.Type.MouseMove, start.y() - 10 * row, Qt.MouseButton.LeftButton)
        assert control.visible_rows() == 3
        send(QEvent.Type.MouseButtonRelease, start.y(), Qt.MouseButton.NoButton)
        # Moving without a button held after release does nothing.
        send(QEvent.Type.MouseMove, start.y() + 4 * row, Qt.MouseButton.NoButton)
        assert control.visible_rows() == 3

    def test_add_edit_remove(self, qtbot):
        widget = _make(qtbot, _list_param(default=["main"]))
        control = widget.controls["Cameras"]
        changed = MagicMock()
        widget.parameter_changed.connect(changed)
        control.show()

        control.add_button.click()
        _type_into_open_editor(qtbot, control, "closeup")
        assert control.value() == ["main", "closeup"]
        assert changed.call_args.args[0]["value"] == ["main", "closeup"]
        assert control.count_label.text() == "Items: 2"

        control.edit_control.setCurrentRow(0)
        assert control.edit_button.isEnabled() and control.remove_button.isEnabled()
        control.edit_button.click()
        _type_into_open_editor(qtbot, control, "wide")
        assert control.value() == ["wide", "closeup"]

        control.remove_button.click()
        assert control.value() == ["closeup"]
        assert changed.call_args.args[0]["value"] == ["closeup"]

    def test_add_then_enter_adds_empty_string(self, qtbot):
        """Pressing Enter on a just-added row commits it, even when it is empty."""
        widget = _make(qtbot, _list_param(default=["main"]))
        control = widget.controls["Cameras"]
        changed = MagicMock()
        widget.parameter_changed.connect(changed)
        control.show()

        control.add_button.click()
        _type_into_open_editor(qtbot, control, "")
        assert control.value() == ["main", ""]
        assert changed.call_args.args[0]["value"] == ["main", ""]

        # The committed empty row is an ordinary item, so a later empty edit keeps it.
        control.edit_control.setCurrentRow(1)
        control.edit_button.click()
        _type_into_open_editor(qtbot, control, "")
        assert control.value() == ["main", ""]

    @pytest.mark.parametrize("button", ["remove_button", "edit_button"])
    def test_add_then_other_button_leaves_existing_items(self, qtbot, button):
        """Clicking another button while a just-added row is still empty discards that row
        and must not act on the item that becomes current in its place."""
        widget = _make(qtbot, _list_param(default=["main", "closeup"]))
        control = widget.controls["Cameras"]
        widget.show()
        qtbot.waitExposed(widget)

        control.add_button.click()
        QApplication.processEvents()
        assert control.edit_control.viewport().findChild(QLineEdit) is not None
        # A real click moves focus to the button before it reports the click, which
        # closes the empty editor first.
        qtbot.mouseClick(getattr(control, button), Qt.MouseButton.LeftButton)
        QApplication.processEvents()

        assert control.value() == ["main", "closeup"]
        assert control.edit_control.selectedItems() == []
        assert control.edit_control.viewport().findChild(QLineEdit) is None
        assert not control.edit_button.isEnabled()
        assert not control.remove_button.isEnabled()

    def test_add_then_type_then_remove_removes_the_new_item(self, qtbot):
        """Typed text is committed when focus leaves the editor, and Remove then removes it."""
        widget = _make(qtbot, _list_param(default=["main"]))
        control = widget.controls["Cameras"]
        widget.show()
        qtbot.waitExposed(widget)

        control.add_button.click()
        QApplication.processEvents()
        control.edit_control.viewport().findChild(QLineEdit).setText("wide")
        qtbot.mouseClick(control.remove_button, Qt.MouseButton.LeftButton)
        QApplication.processEvents()
        assert control.value() == ["main"]

    def test_add_twice_keeps_one_empty_row(self, qtbot):
        """A second Add discards the first still-empty row even when the view, rather
        than the user, closed its editor."""
        widget = _make(qtbot, _list_param(default=["a"]))
        control = widget.controls["Cameras"]
        control.show()
        control.add_button.click()
        control.add_button.click()
        assert control.value() == ["a", ""]

    def test_remove_without_selection_is_disabled(self, qtbot):
        widget = _make(qtbot, _list_param(default=["a", "b"]))
        control = widget.controls["Cameras"]
        control.edit_control.setCurrentRow(0)
        assert control.remove_button.isEnabled()
        control.edit_control.clearSelection()
        assert not control.remove_button.isEnabled()
        assert not control.edit_button.isEnabled()
        control.remove_button.click()
        assert control.value() == ["a", "b"]

    def test_add_then_escape_discards_row(self, qtbot):
        widget = _make(qtbot, _list_param(default=["main"]))
        control = widget.controls["Cameras"]
        control.show()

        control.add_button.click()
        QApplication.processEvents()
        editor = control.edit_control.viewport().findChild(QLineEdit)
        editor.setText("typed then cancelled")
        qtbot.keyClick(editor, Qt.Key.Key_Escape)
        QApplication.processEvents()
        assert control.value() == ["main"]

    def test_existing_item_can_be_edited_to_empty(self, qtbot):
        """Only a just-added row is discarded when empty; an edited item may be empty."""
        widget = _make(qtbot, _list_param(default=["main", "closeup"]))
        control = widget.controls["Cameras"]
        control.show()

        control.edit_control.setCurrentRow(0)
        control.edit_button.click()
        _type_into_open_editor(qtbot, control, "")
        assert control.value() == ["", "closeup"]

    def test_duplicates_and_order_preserved(self, qtbot):
        widget = _make(qtbot, _list_param(default=["b", "a", "b"]))
        assert widget.controls["Cameras"].value() == ["b", "a", "b"]

    def test_items_are_editable_and_draggable(self, qtbot):
        widget = _make(qtbot, _list_param(default=["a"]))
        flags = widget.controls["Cameras"].edit_control.item(0).flags()
        assert flags & Qt.ItemFlag.ItemIsEditable
        assert flags & Qt.ItemFlag.ItemIsDragEnabled
        # Items are not drop targets, so a drag moves between rows instead of onto an item.
        assert not flags & Qt.ItemFlag.ItemIsDropEnabled

    def test_reorder_reports_change(self, qtbot):
        """Reordering (as a drag does, through a model row move) emits the new order."""
        widget = _make(qtbot, _list_param(default=["a", "b", "c"]))
        control = widget.controls["Cameras"]
        changed = MagicMock()
        widget.parameter_changed.connect(changed)

        model = control.edit_control.model()
        assert model.moveRow(model.index(0, 0).parent(), 2, model.index(0, 0).parent(), 0)

        assert control.value() == ["c", "a", "b"]
        assert changed.call_args.args[0]["value"] == ["c", "a", "b"]

    def test_add_disabled_at_max_length(self, qtbot):
        widget = _make(qtbot, _list_param(default=["a"], maxLength=2))
        control = widget.controls["Cameras"]
        assert control.add_button.isEnabled()
        control.set_value(["a", "b"])
        assert not control.add_button.isEnabled()

    def test_add_disabled_at_service_cap(self, qtbot):
        widget = _make(qtbot, _list_param(default=["x"] * 64, maxLength=100))
        assert not widget.controls["Cameras"].add_button.isEnabled()

    def test_set_value_accepts_json_string(self, qtbot):
        """CLI -p and pre-GUI hook values can arrive as a JSON array string."""
        widget = _make(qtbot, _list_param())
        widget.set_parameter_value({"name": "Cameras", "value": '["main", "wide"]'})
        assert widget.controls["Cameras"].value() == ["main", "wide"]

    def test_set_value_invalid_degrades_to_empty(self, qtbot, caplog):
        widget = _make(qtbot, _list_param(default=["a"]))
        control = widget.controls["Cameras"]
        for bad in ("not json", 5, ["a", 1], {"a": 1}):
            control.set_value(["a"])
            with caplog.at_level(logging.WARNING):
                control.set_value(bad)
            assert control.value() == []
            assert "not a list of strings" in caplog.text
            caplog.clear()

    def test_set_parameter_value_emits_validity(self, qtbot):
        """Setting a value from outside refreshes validity, so the Submit button stays current."""
        widget = _make(qtbot, _list_param(default=["a"], minLength=1))
        validity = MagicMock()
        widget.valid_parameters.connect(validity)

        widget.set_parameter_value({"name": "Cameras", "value": []})
        assert validity.call_args.args[0] is False
        widget.set_parameter_value({"name": "Cameras", "value": ["a"]})
        assert validity.call_args.args[0] is True

    def test_list_length_feedback(self, qtbot):
        widget = _make(qtbot, _list_param(default=["a"], minLength=1))
        control = widget.controls["Cameras"]
        assert control.is_valid()
        assert control.edit_control.styleSheet() == ""

        control.set_value([])
        assert not control.is_valid()
        assert widget.invalid_parameter_names() == ["Cameras"]
        assert "red" in control.edit_control.styleSheet()
        assert "has 0 items but minLength is 1" in control.edit_control.toolTip()

        control.set_value(["b"])
        assert control.is_valid()
        assert control.edit_control.styleSheet() == ""

    def test_item_feedback_marks_each_invalid_item(self, qtbot):
        widget = _make(
            qtbot,
            _list_param(
                default=["main"],
                description="Cameras to render",
                item={"allowedValues": ["main", "closeup"], "minLength": 2},
            ),
        )
        control = widget.controls["Cameras"]
        control.set_value(["main", "x", "side"])

        assert not control.is_valid()
        items = [control.edit_control.item(i) for i in range(3)]
        assert items[0].toolTip() == ""
        assert items[0].icon().isNull()
        # Every invalid item is marked, not only the first.
        assert "'x' is shorter than item minLength 2" in items[1].toolTip()
        assert "'side' is not an allowed value" in items[2].toolTip()
        assert not items[1].icon().isNull()
        assert not items[2].icon().isNull()
        # The list tooltip explains the first problem.
        assert "item 1 'x'" in control.edit_control.toolTip()

        control.set_value(["main", "closeup"])
        assert control.is_valid()
        assert all(control.edit_control.item(i).toolTip() == "" for i in range(2))
        assert all(control.edit_control.item(i).icon().isNull() for i in range(2))
        assert control.edit_control.toolTip() == "Cameras to render"

    def test_rejects_top_level_allowed_values(self, qtbot):
        with pytest.raises(RuntimeError, match="must not provide field 'allowedValues'"):
            _JobTemplateLineEditListWidget(None, _list_param(allowedValues=["a"]))


class TestHiddenListString:
    def test_hidden_default_and_value(self, qtbot):
        widget = _make(qtbot, _list_param(control="HIDDEN", default=["a", "b"]))
        control = widget.controls["Cameras"]
        assert isinstance(control, _JobTemplateHiddenWidget)
        assert control.value() == ["a", "b"]

    def test_hidden_without_default_is_empty_list(self, qtbot):
        widget = _make(qtbot, _list_param(control="HIDDEN"))
        assert widget.controls["Cameras"].value() == []

    def test_hidden_converts_json_string(self, qtbot):
        widget = _make(qtbot, _list_param(control="HIDDEN"))
        widget.set_parameter_value({"name": "Cameras", "value": '["x"]'})
        assert widget.controls["Cameras"].value() == ["x"]

    def test_hidden_keeps_invalid_value_for_submission_to_report(self, qtbot):
        widget = _make(qtbot, _list_param(control="HIDDEN"))
        widget.set_parameter_value({"name": "Cameras", "value": "oops"})
        assert widget.controls["Cameras"].value() == "oops"
