# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for the CHECK_BOX_LIST control used by LIST[BOOL] job parameters."""

import logging
from unittest.mock import MagicMock, patch

import pytest
from qtpy.QtCore import QEvent, QPoint, QPointF, QRect, Qt
from qtpy.QtGui import QImage, QMouseEvent, QPainter
from qtpy.QtWidgets import (
    QApplication,
    QProxyStyle,
    QStyle,
    QStyleFactory,
    QStyleOptionViewItem,
)

from deadline.client.ui.widgets.openjd_parameters_widget import (
    OpenJDParametersWidget,
    _JobTemplateCheckBoxListWidget,
    _JobTemplateHiddenWidget,
)


def _list_param(name="Flags", default=None, control=None, **extra):
    param = {"name": name, "type": "LIST[BOOL]", **extra}
    if control is not None:
        param.setdefault("userInterface", {})["control"] = control
    if default is not None:
        param["default"] = default
    return param


def _make(qtbot, *params):
    widget = OpenJDParametersWidget(parameter_definitions=list(params))
    qtbot.addWidget(widget)
    return widget


def _texts(control):
    return [control.edit_control.item(i).text() for i in range(control.edit_control.count())]


def _row_points(control, row):
    """The centers of a row's drag strip and checkbox, and a point on its label, as the
    style lays them out."""
    list_widget = control.edit_control
    index = list_widget.model().index(row, 0)
    delegate = list_widget.itemDelegate()
    option = QStyleOptionViewItem()
    option.rect = list_widget.visualRect(index)
    content = delegate.content_option(option)
    delegate.initStyleOption(content, index)
    style = list_widget.style()
    check = style.subElementRect(
        QStyle.SubElement.SE_ItemViewItemCheckIndicator, content, list_widget
    )
    text = style.subElementRect(QStyle.SubElement.SE_ItemViewItemText, content, list_widget)
    strip = QPoint(option.rect.left() + delegate._strip_width // 2, option.rect.center().y())
    return strip, check.center(), QPoint(text.left() + 10, text.center().y())


def _shown(qtbot, widget):
    widget.show()
    qtbot.waitExposed(widget)
    QApplication.processEvents()


def _send_mouse(control, event_type, pos, button, buttons):
    viewport = control.edit_control.viewport()
    event = QMouseEvent(
        event_type,
        QPointF(pos),
        QPointF(viewport.mapToGlobal(pos)),
        button,
        buttons,
        Qt.KeyboardModifier.NoModifier,
    )
    QApplication.sendEvent(viewport, event)


class TestCheckBoxListWidget:
    def test_default_control_is_a_checkbox_per_row(self, qtbot):
        widget = _make(qtbot, _list_param(default=[True, False, True]))
        control = widget.controls["Flags"]
        assert isinstance(control, _JobTemplateCheckBoxListWidget)
        assert control.value() == [True, False, True]
        assert widget.get_parameters()[0]["value"] == [True, False, True]
        assert control.count_label.text() == "Items: 3"
        assert _texts(control) == ["Item 1", "Item 2", "Item 3"]
        states = [control.edit_control.item(i).checkState() for i in range(3)]
        assert states == [Qt.CheckState.Checked, Qt.CheckState.Unchecked, Qt.CheckState.Checked]

    def test_default_spellings_are_converted(self, qtbot):
        widget = _make(qtbot, _list_param(default=["yes", 0, "OFF", 1.0]))
        assert widget.controls["Flags"].value() == [True, False, False, True]

    def test_no_default_starts_empty(self, qtbot):
        widget = _make(qtbot, _list_param())
        control = widget.controls["Flags"]
        assert control.value() == []
        assert control.is_valid()
        assert not control.remove_button.isEnabled()
        assert not hasattr(control, "edit_button")

    def test_value_takes_precedence_over_default(self, qtbot):
        widget = _make(qtbot, _list_param(default=[True], value=[False, False]))
        assert widget.controls["Flags"].value() == [False, False]

    def test_toggling_a_checkbox_changes_the_value_not_the_label(self, qtbot):
        widget = _make(qtbot, _list_param(default=[True, False]))
        control = widget.controls["Flags"]
        changed = MagicMock()
        widget.parameter_changed.connect(changed)

        control.edit_control.item(1).setCheckState(Qt.CheckState.Checked)
        assert control.value() == [True, True]
        assert changed.call_args.args[0]["value"] == [True, True]
        assert _texts(control) == ["Item 1", "Item 2"]

    def test_clicking_the_checkbox_toggles_it(self, qtbot):
        widget = _make(qtbot, _list_param(default=[False]))
        control = widget.controls["Flags"]
        changed = MagicMock()
        widget.parameter_changed.connect(changed)
        _shown(qtbot, widget)
        viewport = control.edit_control.viewport()
        _, check, _ = _row_points(control, 0)

        qtbot.mouseClick(viewport, Qt.MouseButton.LeftButton, pos=check)
        assert control.value() == [True]
        assert changed.call_count == 1
        qtbot.mouseClick(viewport, Qt.MouseButton.LeftButton, pos=check)
        assert control.value() == [False]
        assert changed.call_count == 2

    def test_checkbox_is_right_of_the_drag_strip(self, qtbot):
        widget = _make(qtbot, _list_param(default=[False]))
        control = widget.controls["Flags"]
        _shown(qtbot, widget)
        row_rect = control.edit_control.visualItemRect(control.edit_control.item(0))
        _, check, _ = _row_points(control, 0)
        assert check.x() > row_rect.left() + control._strip_width

    def test_clicking_the_label_toggles_it_and_selects_the_row(self, qtbot):
        widget = _make(qtbot, _list_param(default=[False, True]))
        control = widget.controls["Flags"]
        changed = MagicMock()
        widget.parameter_changed.connect(changed)
        _shown(qtbot, widget)
        list_widget = control.edit_control
        _, _, label = _row_points(control, 1)

        qtbot.mouseClick(list_widget.viewport(), Qt.MouseButton.LeftButton, pos=label)
        assert control.value() == [False, False]
        assert changed.call_count == 1
        assert changed.call_args.args[0]["value"] == [False, False]
        assert list_widget.selectedItems() == [list_widget.item(1)]
        qtbot.mouseClick(list_widget.viewport(), Qt.MouseButton.LeftButton, pos=label)
        assert control.value() == [False, True]
        assert changed.call_count == 2

    def test_clicking_the_strip_selects_the_row_without_toggling(self, qtbot):
        widget = _make(qtbot, _list_param(default=[True, False]))
        control = widget.controls["Flags"]
        changed = MagicMock()
        widget.parameter_changed.connect(changed)
        _shown(qtbot, widget)
        list_widget = control.edit_control
        strip, _, _ = _row_points(control, 1)

        qtbot.mouseClick(list_widget.viewport(), Qt.MouseButton.LeftButton, pos=strip)
        assert control.value() == [True, False]
        assert changed.call_count == 0
        assert list_widget.selectedItems() == [list_widget.item(1)]
        assert control.remove_button.isEnabled()
        control.remove_button.click()
        assert control.value() == [True]

    def test_pressing_the_strip_starts_a_drag(self, qtbot):
        widget = _make(qtbot, _list_param(default=[True, False, True]))
        control = widget.controls["Flags"]
        _shown(qtbot, widget)
        list_widget = control.edit_control
        strip, _, _ = _row_points(control, 0)
        left = Qt.MouseButton.LeftButton

        _send_mouse(control, QEvent.Type.MouseButtonPress, strip, left, left)
        moved = strip + QPoint(0, QApplication.startDragDistance() + 2)
        # startDrag is not run: an offscreen drag would return at once.
        with patch.object(list_widget, "startDrag") as start_drag:
            _send_mouse(control, QEvent.Type.MouseMove, moved, Qt.MouseButton.NoButton, left)
        _send_mouse(control, QEvent.Type.MouseButtonRelease, moved, left, Qt.MouseButton.NoButton)
        start_drag.assert_called_once()
        assert control.value() == [True, False, True]

    def test_press_that_ends_elsewhere_does_not_toggle_or_leave_stale_state(self, qtbot):
        """A press on a label that is released on another row toggles nothing, and does
        not stop a later click on a checkbox from toggling it."""
        widget = _make(qtbot, _list_param(default=[False, False]))
        control = widget.controls["Flags"]
        _shown(qtbot, widget)
        viewport = control.edit_control.viewport()
        _, check0, label0 = _row_points(control, 0)
        _, _, label1 = _row_points(control, 1)

        qtbot.mousePress(viewport, Qt.MouseButton.LeftButton, pos=label0)
        qtbot.mouseRelease(viewport, Qt.MouseButton.LeftButton, pos=label1)
        assert control.value() == [False, False]

        control.edit_control.setCurrentRow(0)
        qtbot.keyClick(control.edit_control, Qt.Key.Key_Space)
        assert control.value() == [True, False]
        qtbot.mouseClick(viewport, Qt.MouseButton.LeftButton, pos=check0)
        assert control.value() == [False, False]

    def test_right_click_on_the_label_does_not_toggle(self, qtbot):
        widget = _make(qtbot, _list_param(default=[False]))
        control = widget.controls["Flags"]
        _shown(qtbot, widget)
        _, _, label = _row_points(control, 0)
        qtbot.mouseClick(control.edit_control.viewport(), Qt.MouseButton.RightButton, pos=label)
        assert control.value() == [False]

    @pytest.mark.parametrize("target", ["checkbox", "label"])
    def test_double_click_toggles_once(self, qtbot, target):
        """A double click on the label behaves like one on the checkbox, which Qt
        toggles once."""
        widget = _make(qtbot, _list_param(default=[False]))
        control = widget.controls["Flags"]
        changed = MagicMock()
        widget.parameter_changed.connect(changed)
        _shown(qtbot, widget)
        _, check, label = _row_points(control, 0)
        pos = check if target == "checkbox" else label
        left, none = Qt.MouseButton.LeftButton, Qt.MouseButton.NoButton
        # The events of a real double click; QTest.mouseDClick sends only the third.
        _send_mouse(control, QEvent.Type.MouseButtonPress, pos, left, left)
        _send_mouse(control, QEvent.Type.MouseButtonRelease, pos, left, none)
        _send_mouse(control, QEvent.Type.MouseButtonDblClick, pos, left, left)
        _send_mouse(control, QEvent.Type.MouseButtonRelease, pos, left, none)
        assert control.value() == [True]
        assert changed.call_count == 1

    def test_space_toggles_the_current_row(self, qtbot):
        widget = _make(qtbot, _list_param(default=[False, False]))
        control = widget.controls["Flags"]
        widget.show()
        qtbot.waitExposed(widget)
        control.edit_control.setFocus()
        control.edit_control.setCurrentRow(1)
        qtbot.keyClick(control.edit_control, Qt.Key.Key_Space)
        assert control.value() == [False, True]

    def test_items_are_checkable_and_draggable_but_not_text_editable(self, qtbot):
        widget = _make(qtbot, _list_param(default=[True]))
        flags = widget.controls["Flags"].edit_control.item(0).flags()
        assert flags & Qt.ItemFlag.ItemIsUserCheckable
        assert flags & Qt.ItemFlag.ItemIsDragEnabled
        assert not flags & Qt.ItemFlag.ItemIsEditable

    @pytest.mark.parametrize(
        ("default", "expected"),
        [
            pytest.param([True], [True, True], id="copies-true"),
            pytest.param([True, False], [True, False, False], id="copies-false"),
            pytest.param([], [False], id="empty-adds-false"),
        ],
    )
    def test_add_copies_the_last_value(self, qtbot, default, expected):
        widget = _make(qtbot, _list_param(default=default))
        control = widget.controls["Flags"]
        changed = MagicMock()
        widget.parameter_changed.connect(changed)
        control.add_button.click()
        assert control.value() == expected
        assert changed.call_args.args[0]["value"] == expected
        assert control.edit_control.currentRow() == len(expected) - 1

    def test_add_then_space_toggles_the_new_row(self, qtbot):
        widget = _make(qtbot, _list_param(default=[False]))
        control = widget.controls["Flags"]
        widget.show()
        qtbot.waitExposed(widget)
        control.add_button.click()
        QApplication.processEvents()
        assert QApplication.focusWidget() is control.edit_control
        qtbot.keyClick(control.edit_control, Qt.Key.Key_Space)
        assert control.value() == [False, True]

    def test_remove_selected_renumbers_the_rows(self, qtbot):
        widget = _make(qtbot, _list_param(default=[True, False, True]))
        control = widget.controls["Flags"]
        control.edit_control.setCurrentRow(1)
        assert control.remove_button.isEnabled()
        control.remove_button.click()
        assert control.value() == [True, True]
        assert _texts(control) == ["Item 1", "Item 2"]

    def test_add_labels_the_new_row_with_the_next_number(self, qtbot):
        widget = _make(qtbot, _list_param(default=[True, False]))
        control = widget.controls["Flags"]
        control.add_button.click()
        assert _texts(control) == ["Item 1", "Item 2", "Item 3"]

    def test_reorder_keeps_values_and_numbers_rows_in_order(self, qtbot):
        widget = _make(qtbot, _list_param(default=[True, False, False]))
        control = widget.controls["Flags"]
        changed = MagicMock()
        widget.parameter_changed.connect(changed)
        model = control.edit_control.model()
        parent = model.index(0, 0).parent()
        assert model.moveRow(parent, 0, parent, 3)
        assert control.value() == [False, False, True]
        assert changed.call_args.args[0]["value"] == [False, False, True]
        # The moved row takes the label of its new position.
        assert _texts(control) == ["Item 1", "Item 2", "Item 3"]
        assert control.edit_control.item(2).checkState() == Qt.CheckState.Checked

    def test_list_length_feedback(self, qtbot):
        widget = _make(qtbot, _list_param(default=[True], minLength=1))
        control = widget.controls["Flags"]
        assert control.is_valid()
        control.set_value([])
        assert not control.is_valid()
        assert widget.invalid_parameter_names() == ["Flags"]
        assert "has 0 items but minLength is 1" in control.edit_control.toolTip()

    def test_add_disabled_at_max_length(self, qtbot):
        widget = _make(qtbot, _list_param(default=[True], maxLength=2))
        control = widget.controls["Flags"]
        control.add_button.click()
        assert not control.add_button.isEnabled()

    def test_rows_fit_a_checkbox_and_three_are_shown(self, qtbot):
        widget = _make(qtbot, _list_param(default=[True, False, True]))
        control = widget.controls["Flags"]
        widget.show()
        QApplication.processEvents()
        frame = 2 * control.edit_control.frameWidth()
        assert control.visible_rows() == 3
        assert control.edit_control.height() == frame + 3 * control._row_height
        assert control.edit_control.verticalScrollBar().maximum() == 0

    def test_description_is_the_tooltip(self, qtbot):
        widget = _make(qtbot, _list_param(default=[True], description="Per-layer flags"))
        control = widget.controls["Flags"]
        assert control.label.toolTip() == "Per-layer flags"
        assert control.edit_control.toolTip() == "Per-layer flags"

    def test_set_value_accepts_json_string(self, qtbot):
        widget = _make(qtbot, _list_param())
        widget.set_parameter_value({"name": "Flags", "value": '[true, "no", 1]'})
        assert widget.controls["Flags"].value() == [True, False, True]

    @pytest.mark.parametrize("bad", ["not json", "true", True, ["maybe"], [2], [None], {"a": True}])
    def test_set_value_invalid_degrades_to_empty(self, qtbot, caplog, bad):
        widget = _make(qtbot, _list_param(default=[True]))
        control = widget.controls["Flags"]
        with caplog.at_level(logging.WARNING):
            control.set_value(bad)
        assert control.value() == []
        assert "not a list of booleans" in caplog.text

    def test_set_parameter_value_emits_validity(self, qtbot):
        widget = _make(qtbot, _list_param(default=[True], minLength=1))
        validity = MagicMock()
        widget.valid_parameters.connect(validity)
        widget.set_parameter_value({"name": "Flags", "value": []})
        assert validity.call_args.args[0] is False
        widget.set_parameter_value({"name": "Flags", "value": [False]})
        assert validity.call_args.args[0] is True

    def test_group_label(self, qtbot):
        widget = _make(
            qtbot,
            _list_param(default=[True], userInterface={"label": "Layers", "groupLabel": "Opts"}),
        )
        control = widget.controls["Flags"]
        assert control.label.text() == "Layers"
        assert control.parentWidget().title() == "Opts"

    def test_rejects_top_level_allowed_values(self, qtbot):
        with pytest.raises(RuntimeError, match="must not provide field 'allowedValues'"):
            _JobTemplateCheckBoxListWidget(None, _list_param(allowedValues=[True]))


class TestHiddenListBool:
    def test_hidden_default_is_converted(self, qtbot):
        widget = _make(qtbot, _list_param(control="HIDDEN", default=["yes", 0]))
        control = widget.controls["Flags"]
        assert isinstance(control, _JobTemplateHiddenWidget)
        assert control.value() == [True, False]

    def test_hidden_without_default_is_empty_list(self, qtbot):
        widget = _make(qtbot, _list_param(control="HIDDEN"))
        assert widget.controls["Flags"].value() == []

    def test_hidden_converts_json_string(self, qtbot):
        widget = _make(qtbot, _list_param(control="HIDDEN"))
        widget.set_parameter_value({"name": "Flags", "value": "[false, true]"})
        assert widget.controls["Flags"].value() == [False, True]

    def test_hidden_keeps_invalid_value_for_submission_to_report(self, qtbot):
        widget = _make(qtbot, _list_param(control="HIDDEN"))
        widget.set_parameter_value({"name": "Flags", "value": "maybe"})
        assert widget.controls["Flags"].value() == "maybe"


class TestListControlEventFilterDuringTeardown:
    """Qt can deliver an event to a list control's event filter while the control is
    being destroyed, after its Python attributes are gone."""

    @pytest.mark.parametrize(
        ("list_type", "event_type"),
        [
            ("LIST[BOOL]", QEvent.Type.Resize),
            ("LIST[INT]", QEvent.Type.FocusIn),
            ("LIST[STRING]", QEvent.Type.Resize),
        ],
    )
    def test_event_filter_without_the_list(self, qtbot, list_type, event_type):
        widget = _make(qtbot, {"name": "Values", "type": list_type})
        control = widget.controls["Values"]
        list_widget = control.edit_control
        del control.edit_control
        try:
            assert control.eventFilter(list_widget, QEvent(event_type)) is False
        finally:
            control.edit_control = list_widget


class _NoDecorationSelectedStyle(QProxyStyle):
    """A style that, unlike the built-in ones for a list view, highlights only the text
    rectangle of a selected item."""

    def styleHint(self, hint, option=None, widget=None, returnData=None):
        if hint == QStyle.StyleHint.SH_ItemView_ShowDecorationSelected:
            return 0
        return super().styleHint(hint, option, widget, returnData)


@pytest.mark.parametrize("hint_false", [False, True], ids=["default-style", "hint-false"])
def test_selection_highlight_covers_the_drag_strip(qtbot, hint_false):
    widget = _make(qtbot, _list_param(default=[True]))
    list_widget = widget.controls["Flags"].edit_control
    if hint_false:
        style = _NoDecorationSelectedStyle(QStyleFactory.create("Fusion"))
        style.setParent(widget)
        list_widget.setStyle(style)
    style = list_widget.style()
    index = list_widget.model().index(0, 0)
    rect = list_widget.visualRect(index)

    def render(selected):
        option = QStyleOptionViewItem()
        option.initFrom(list_widget)
        option.rect = QRect(0, 0, rect.width(), rect.height())
        option.widget = list_widget
        # As the view sets it, from the style.
        option.showDecorationSelected = bool(
            style.styleHint(QStyle.StyleHint.SH_ItemView_ShowDecorationSelected, None, list_widget)
        )
        if selected:
            option.state |= QStyle.StateFlag.State_Selected
        image = QImage(option.rect.size(), QImage.Format.Format_ARGB32)
        image.fill(Qt.GlobalColor.transparent)
        painter = QPainter(image)
        list_widget.itemDelegate().paint(painter, option, index)
        painter.end()
        return image

    selected = render(True)
    unselected = render(False)
    strip = QPoint(2, rect.height() // 2)
    row_end = QPoint(rect.width() - 3, rect.height() // 2)
    assert selected.pixel(strip) == selected.pixel(row_end)
    assert selected.pixel(strip) != unselected.pixel(strip)
