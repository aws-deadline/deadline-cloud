# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for the CHOOSE_INPUT_FILE_LIST, CHOOSE_OUTPUT_FILE_LIST and CHOOSE_DIRECTORY_LIST
controls used by LIST[PATH] job parameters."""

import logging
import os
from unittest.mock import MagicMock, patch

import pytest
from qtpy.QtCore import QEvent, QPoint, QPointF, Qt
from qtpy.QtGui import QMouseEvent
from qtpy.QtWidgets import QAbstractItemView, QApplication, QPushButton

from deadline.client.ui.widgets.openjd_parameters_widget import (
    OpenJDParametersWidget,
    _JobTemplateDirectoryListWidget,
    _JobTemplateHiddenWidget,
    _JobTemplateInputFileListWidget,
    _JobTemplateOutputFileListWidget,
)
from deadline.client.ui.widgets.path_widgets import (
    DirectoryPickerWidget,
    InputFilePickerWidget,
    OutputFilePickerWidget,
)


def _list_param(name="Paths", default=None, control=None, **extra):
    param = {"name": name, "type": "LIST[PATH]", **extra}
    if control is not None:
        param.setdefault("userInterface", {})["control"] = control
    if default is not None:
        param["default"] = default
    return param


def _make(qtbot, *params):
    widget = OpenJDParametersWidget(parameter_definitions=list(params))
    qtbot.addWidget(widget)
    return widget


def _shown(qtbot, widget):
    widget.show()
    qtbot.waitExposed(widget)
    QApplication.processEvents()


def _picker_kwargs(picker_type):
    if picker_type is DirectoryPickerWidget:
        return {"initial_directory": "", "directory_label": "Paths"}
    return {"initial_filename": "", "file_label": "Paths", "filter": "", "selected_filter": ""}


def test_uri_and_empty_items_are_kept(qtbot):
    widget = _make(qtbot, _list_param(default=["s3://bucket/a/b", ""]))
    assert widget.controls["Paths"].value() == ["s3://bucket/a/b", ""]


def _texts(control):
    return [control.row_editor(i).text() for i in range(control.edit_control.count())]


def _line_edit(control, row):
    return control._editor_line_edit(control.row_editor(row))


def _button(control, row):
    return control.row_editor(row).findChild(QPushButton)


class TestPathListWidget:
    @pytest.mark.parametrize(
        ("definition", "control_type", "picker_type"),
        [
            pytest.param({}, _JobTemplateDirectoryListWidget, DirectoryPickerWidget, id="dir"),
            pytest.param(
                {"objectType": "FILE", "dataFlow": "IN"},
                _JobTemplateInputFileListWidget,
                InputFilePickerWidget,
                id="file-in",
            ),
            pytest.param(
                {"objectType": "FILE", "dataFlow": "OUT"},
                _JobTemplateOutputFileListWidget,
                OutputFilePickerWidget,
                id="file-out",
            ),
            pytest.param(
                {"control": "CHOOSE_INPUT_FILE_LIST"},
                _JobTemplateInputFileListWidget,
                InputFilePickerWidget,
                id="explicit-input",
            ),
            pytest.param(
                {"control": "CHOOSE_OUTPUT_FILE_LIST"},
                _JobTemplateOutputFileListWidget,
                OutputFilePickerWidget,
                id="explicit-output",
            ),
            pytest.param(
                {"control": "CHOOSE_DIRECTORY_LIST", "objectType": "FILE"},
                _JobTemplateDirectoryListWidget,
                DirectoryPickerWidget,
                id="explicit-dir",
            ),
        ],
    )
    def test_each_row_is_the_picker_of_the_matching_path_control(
        self, qtbot, definition, control_type, picker_type
    ):
        widget = _make(qtbot, _list_param(default=["a", "b/c"], **definition))
        control = widget.controls["Paths"]
        assert isinstance(control, control_type)
        # Like the PATH control's picker, a value it is given is normalized, and the
        # normalized path is the one submitted.
        shown = ["a", os.path.normpath("b/c")]
        assert control.value() == shown
        assert widget.get_parameters()[0]["value"] == shown
        assert control.count_label.text() == "Items: 2"
        assert [type(control.row_editor(i)) for i in range(2)] == [picker_type] * 2
        assert _texts(control) == shown
        picker = picker_type(**_picker_kwargs(picker_type))
        picker.setText("b/c")
        assert picker.text() == shown[1]

    def test_no_default_starts_empty(self, qtbot):
        widget = _make(qtbot, _list_param())
        control = widget.controls["Paths"]
        assert control.value() == []
        assert control.is_valid()
        assert not control.remove_button.isEnabled()
        assert not hasattr(control, "edit_button")

    def test_value_takes_precedence_over_default(self, qtbot):
        widget = _make(qtbot, _list_param(default=["a"], value=["b", "c"]))
        assert widget.controls["Paths"].value() == ["b", "c"]

    def test_typing_changes_the_value_as_it_is_typed(self, qtbot):
        widget = _make(qtbot, _list_param(default=["a", "b"]))
        control = widget.controls["Paths"]
        changed = MagicMock()
        widget.parameter_changed.connect(changed)
        _shown(qtbot, widget)

        line_edit = _line_edit(control, 1)
        line_edit.setFocus()
        line_edit.selectAll()
        qtbot.keyClicks(line_edit, "new")
        assert control.value() == ["a", "new"]
        assert changed.call_args.args[0]["value"] == ["a", "new"]
        assert line_edit.text() == "new"

    def test_in_progress_edit_survives_changes_to_other_rows(self, qtbot):
        widget = _make(qtbot, _list_param(default=["a", "b"]))
        control = widget.controls["Paths"]
        _shown(qtbot, widget)
        line_edit = _line_edit(control, 0)
        line_edit.setFocus()
        line_edit.setCursorPosition(1)
        qtbot.keyClicks(line_edit, "xy")
        control.row_editor(1).setText("other")
        assert line_edit.text() == "axy"
        assert line_edit.cursorPosition() == 3
        assert control.value() == ["axy", os.path.normpath("other")]

    @pytest.mark.parametrize(
        ("definition", "dialog_function"),
        [
            pytest.param(
                {"objectType": "FILE", "dataFlow": "IN"}, "getOpenFileName", id="input-file"
            ),
            pytest.param(
                {"objectType": "FILE", "dataFlow": "OUT"}, "getSaveFileName", id="output-file"
            ),
        ],
    )
    def test_file_button_opens_the_dialog_with_the_file_filters(
        self, qtbot, tmp_path, definition, dialog_function
    ):
        chosen = str(tmp_path / "chosen.blend")
        file_filters = {
            "fileFilters": [
                {"label": "Any", "patterns": ["*"]},
                {"label": "Blender", "patterns": ["*.blend", "*.blend1"]},
            ],
            "fileFilterDefault": {"label": "Blender", "patterns": ["*.blend", "*.blend1"]},
        }
        widget = _make(
            qtbot, _list_param(default=["a", "b"], userInterface=file_filters, **definition)
        )
        control = widget.controls["Paths"]
        changed = MagicMock()
        widget.parameter_changed.connect(changed)

        with patch(
            f"deadline.client.ui.widgets.path_widgets.QFileDialog.{dialog_function}",
            return_value=(chosen, "Blender (*.blend *.blend1)"),
        ) as dialog:
            _button(control, 1).click()

        args = dialog.call_args.args
        assert args[1] == "Choose Paths"
        assert args[3] == "Any (*);;Blender (*.blend *.blend1)"
        assert args[4] == "Blender (*.blend *.blend1)"
        assert control.value() == ["a", os.path.normpath(chosen)]
        assert changed.call_args.args[0]["value"] == ["a", os.path.normpath(chosen)]

    def test_directory_button_opens_the_directory_dialog(self, qtbot, tmp_path):
        widget = _make(qtbot, _list_param(default=[str(tmp_path)]))
        control = widget.controls["Paths"]
        with patch(
            "deadline.client.ui.widgets.path_widgets.QFileDialog.getExistingDirectory",
            return_value=str(tmp_path / "picked"),
        ) as dialog:
            _button(control, 0).click()
        assert dialog.call_args.args[2] == str(tmp_path)
        assert control.value() == [os.path.normpath(str(tmp_path / "picked"))]

    def test_canceling_the_dialog_keeps_the_value(self, qtbot):
        widget = _make(qtbot, _list_param(default=["a"], objectType="FILE"))
        control = widget.controls["Paths"]
        with patch(
            "deadline.client.ui.widgets.path_widgets.QFileDialog.getOpenFileName",
            return_value=("", ""),
        ):
            _button(control, 0).click()
        assert control.value() == ["a"]

    @pytest.mark.parametrize(
        ("definition", "dialog_function", "dialog_result", "picker_type"),
        [
            pytest.param(
                {"objectType": "FILE", "dataFlow": "IN"},
                "getOpenFileName",
                ("chosen.blend", "Blender (*.blend)"),
                InputFilePickerWidget,
                id="input-file",
            ),
            pytest.param(
                {"objectType": "FILE", "dataFlow": "OUT"},
                "getSaveFileName",
                ("chosen.blend", "Blender (*.blend)"),
                OutputFilePickerWidget,
                id="output-file",
            ),
            pytest.param(
                {"objectType": "DIRECTORY"},
                "getExistingDirectory",
                "chosen",
                DirectoryPickerWidget,
                id="directory",
            ),
        ],
    )
    def test_add_opens_the_row_dialog_and_appends_the_chosen_path(
        self, qtbot, tmp_path, definition, dialog_function, dialog_result, picker_type
    ):
        file_filters = {
            "fileFilters": [
                {"label": "Any", "patterns": ["*"]},
                {"label": "Blender", "patterns": ["*.blend"]},
            ],
            "fileFilterDefault": {"label": "Blender", "patterns": ["*.blend"]},
        }
        widget = _make(qtbot, _list_param(default=["a"], userInterface=file_filters, **definition))
        control = widget.controls["Paths"]
        changed = MagicMock()
        widget.parameter_changed.connect(changed)
        if isinstance(dialog_result, tuple):
            chosen = str(tmp_path / dialog_result[0])
            dialog_result = (chosen, dialog_result[1])
        else:
            chosen = str(tmp_path / dialog_result)
            dialog_result = chosen

        with patch(
            f"deadline.client.ui.widgets.path_widgets.QFileDialog.{dialog_function}",
            return_value=dialog_result,
        ) as dialog:
            control.add_button.click()

        # The same dialog, with the same caption and filters, as the row's "..." button.
        dialog.assert_called_once()
        args = dialog.call_args.args
        assert args[1] == "Choose Paths"
        if picker_type is not DirectoryPickerWidget:
            assert args[3] == "Any (*);;Blender (*.blend)"
            assert args[4] == "Blender (*.blend)"
        assert isinstance(control.row_editor(1), picker_type)
        assert control.value() == ["a", os.path.normpath(chosen)]
        assert changed.call_args.args[0]["value"] == ["a", os.path.normpath(chosen)]

    def test_canceling_the_add_dialog_leaves_an_empty_row_to_type_in(self, qtbot):
        widget = _make(qtbot, _list_param(default=["a"], objectType="FILE"))
        control = widget.controls["Paths"]
        changed = MagicMock()
        widget.parameter_changed.connect(changed)
        _shown(qtbot, widget)

        with patch(
            "deadline.client.ui.widgets.path_widgets.QFileDialog.getOpenFileName",
            return_value=("", ""),
        ) as dialog:
            control.add_button.click()
        QApplication.processEvents()
        dialog.assert_called_once()
        assert control.value() == ["a", ""]
        assert changed.call_args.args[0]["value"] == ["a", ""]
        assert isinstance(control.row_editor(1), InputFilePickerWidget)
        assert control.edit_control.currentRow() == 1
        assert QApplication.focusWidget() is _line_edit(control, 1)

        # A URI is a path value the dialog can't choose.
        qtbot.keyClicks(_line_edit(control, 1), "s3://bucket/key.blend")
        assert control.value() == ["a", "s3://bucket/key.blend"]

    def test_add_disabled_at_max_length(self, qtbot):
        widget = _make(qtbot, _list_param(default=["a"], maxLength=2))
        control = widget.controls["Paths"]
        with patch(
            "deadline.client.ui.widgets.path_widgets.QFileDialog.getExistingDirectory",
            return_value="",
        ):
            control.add_button.click()
        assert not control.add_button.isEnabled()

    @pytest.mark.parametrize("focus", ["line_edit", "button"])
    def test_focusing_a_row_selects_it_for_remove(self, qtbot, focus):
        widget = _make(qtbot, _list_param(default=["a", "b", "c"]))
        control = widget.controls["Paths"]
        _shown(qtbot, widget)

        target = _line_edit(control, 1) if focus == "line_edit" else _button(control, 1)
        target.setFocus(Qt.FocusReason.MouseFocusReason)
        QApplication.processEvents()
        assert control.edit_control.currentRow() == 1
        assert control.remove_button.isEnabled()
        control.remove_button.click()
        assert control.value() == ["a", "c"]
        assert _texts(control) == ["a", "c"]

    def test_reorder_keeps_pickers_with_their_values(self, qtbot):
        widget = _make(qtbot, _list_param(default=["a", "b", "c"]))
        control = widget.controls["Paths"]
        changed = MagicMock()
        widget.parameter_changed.connect(changed)

        model = control.edit_control.model()
        root = model.index(0, 0).parent()
        assert model.moveRow(root, 2, root, 0)
        QApplication.processEvents()

        assert control.value() == ["c", "a", "b"]
        assert changed.call_args.args[0]["value"] == ["c", "a", "b"]
        assert _texts(control) == ["c", "a", "b"]

    def test_items_are_draggable_but_not_text_editable(self, qtbot):
        widget = _make(qtbot, _list_param(default=["a"]))
        control = widget.controls["Paths"]
        flags = control.edit_control.item(0).flags()
        assert flags & Qt.ItemFlag.ItemIsDragEnabled
        assert not flags & Qt.ItemFlag.ItemIsEditable
        # Drops go to the list, so a dragged row is not dropped into a row's line edit.
        assert not control.row_editor(0).acceptDrops()
        assert not _line_edit(control, 0).acceptDrops()

    def test_pressing_the_drag_strip_starts_a_drag(self, qtbot):
        widget = _make(qtbot, _list_param(default=["a", "b"]))
        control = widget.controls["Paths"]
        _shown(qtbot, widget)
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
        assert QApplication.focusWidget() is not _line_edit(control, 0)
        with patch.object(list_widget, "startDrag"):
            send(QEvent.Type.MouseMove, start + QPoint(0, 3), Qt.MouseButton.LeftButton)
            assert list_widget.state() == QAbstractItemView.State.DraggingState
        send(QEvent.Type.MouseButtonRelease, start + QPoint(0, 3), Qt.MouseButton.NoButton)

    def test_picker_is_right_of_the_drag_strip_and_clear_of_the_grip(self, qtbot):
        widget = _make(qtbot, _list_param(default=["a", "b", "c"]))
        control = widget.controls["Paths"]
        _shown(qtbot, widget)
        grip = control.corner_grip
        assert grip.isVisible()
        for row in range(3):
            editor = control.row_editor(row)
            rect = editor.geometry().translated(
                editor.parentWidget().mapTo(control.edit_control, QPoint(0, 0))
            )
            row_rect = control.edit_control.visualItemRect(control.edit_control.item(row))
            assert rect.left() >= row_rect.left() + control._strip_width
            assert not rect.intersects(grip.geometry())

    def test_rows_fit_a_picker(self, qtbot):
        widget = _make(qtbot, _list_param(default=["a"]))
        control = widget.controls["Paths"]
        assert control._row_height >= control.row_editor(0).sizeHint().height()
        assert control.visible_rows() == 3

    def test_item_constraint_feedback(self, qtbot):
        widget = _make(
            qtbot,
            _list_param(default=["ok", "x"], item={"minLength": 2}, description="Scene files"),
        )
        control = widget.controls["Paths"]
        assert not control.is_valid()
        assert widget.invalid_parameter_names() == ["Paths"]
        items = [control.edit_control.item(i) for i in range(2)]
        assert [item.icon().isNull() for item in items] == [True, False]
        assert "'x' is shorter than item minLength 2" in items[1].toolTip()
        assert "shorter than item minLength 2" in control.row_editor(1).toolTip()
        assert control.row_editor(0).toolTip() == "Scene files"
        assert "red" in control.edit_control.styleSheet()
        assert items[1].data(Qt.ItemDataRole.AccessibleTextRole) == "x"

        control.row_editor(1).setText("fixed")
        assert control.is_valid()
        assert items[1].icon().isNull()
        assert control.edit_control.styleSheet() == ""

    def test_item_allowed_values_feedback(self, qtbot):
        widget = _make(qtbot, _list_param(default=["a"], item={"allowedValues": ["a", "b"]}))
        control = widget.controls["Paths"]
        control.set_value(["a", "z"])
        assert not control.is_valid()
        assert "'z' is not an allowed value" in control.edit_control.item(1).toolTip()

    def test_list_length_feedback(self, qtbot):
        widget = _make(qtbot, _list_param(default=["a"], minLength=1))
        control = widget.controls["Paths"]
        control.set_value([])
        assert not control.is_valid()
        assert "has 0 items but minLength is 1" in control.edit_control.toolTip()

    def test_set_value_accepts_json_string(self, qtbot):
        widget = _make(qtbot, _list_param())
        control = widget.controls["Paths"]
        control.set_value('["/a", "b"]')
        assert control.value() == [os.path.normpath("/a"), "b"]
        assert _texts(control) == [os.path.normpath("/a"), "b"]

    @pytest.mark.parametrize("bad", [["a", 1], "not json", '{"a": 1}', 5])
    def test_set_value_invalid_degrades_to_empty(self, qtbot, caplog, bad):
        widget = _make(qtbot, _list_param(default=["a"]))
        control = widget.controls["Paths"]
        with caplog.at_level(logging.WARNING):
            control.set_value(bad)
        assert control.value() == []
        assert "not a list of paths" in caplog.text

    def test_set_parameter_value_emits_validity(self, qtbot):
        widget = _make(qtbot, _list_param(default=["a"], minLength=1))
        validity = MagicMock()
        widget.valid_parameters.connect(validity)
        widget.set_parameter_value({"name": "Paths", "value": []})
        assert validity.call_args.args[0] is False
        widget.set_parameter_value({"name": "Paths", "value": ["b"]})
        assert validity.call_args.args[0] is True

    def test_rejects_top_level_allowed_values(self, qtbot):
        with pytest.raises(RuntimeError, match="must not provide field 'allowedValues'"):
            _JobTemplateDirectoryListWidget(None, _list_param(allowedValues=["a"]))


class TestHiddenPathList:
    def test_hidden_default_and_value(self, qtbot):
        widget = _make(qtbot, _list_param(default=["a", "b"], control="HIDDEN"))
        control = widget.controls["Paths"]
        assert isinstance(control, _JobTemplateHiddenWidget)
        assert control.value() == ["a", "b"]

    def test_hidden_without_default_is_empty_list(self, qtbot):
        widget = _make(qtbot, _list_param(control="HIDDEN"))
        assert widget.controls["Paths"].value() == []

    def test_hidden_converts_json_string(self, qtbot):
        widget = _make(qtbot, _list_param(control="HIDDEN", value='["a"]'))
        assert widget.controls["Paths"].value() == ["a"]

    def test_hidden_keeps_invalid_value_for_submission_to_report(self, qtbot):
        widget = _make(qtbot, _list_param(control="HIDDEN", value="a,b"))
        assert widget.controls["Paths"].value() == "a,b"
