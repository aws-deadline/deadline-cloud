# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for the JSON text edit the client shows for LIST[LIST[INT]] job parameters, which
OpenJD gives no editing control."""

import logging
import os
from unittest.mock import MagicMock, patch

import pytest
from qtpy.QtWidgets import QPlainTextEdit

from deadline.client.exceptions import DeadlineOperationError
from deadline.client.ui.widgets.openjd_parameters_widget import (
    OpenJDParametersWidget,
    _JobTemplateHiddenWidget,
    _JobTemplateJsonEditWidget,
)

pytestmark = pytest.mark.usefixtures("fresh_deadline_config")

TYPE = "LIST[LIST[INT]]"


def _param(name="Edges", default=None, control=None, **extra):
    param = {"name": name, "type": TYPE, **extra}
    if control is not None:
        param.setdefault("userInterface", {})["control"] = control
    if default is not None:
        param["default"] = default
    return param


def _make(qtbot, *params):
    widget = OpenJDParametersWidget(parameter_definitions=list(params))
    qtbot.addWidget(widget)
    return widget


class TestJsonEditControl:
    def test_default_control_is_json_edit_showing_compact_json(self, qtbot):
        widget = _make(qtbot, _param(default=[[1, 2], [3]]))
        control = widget.controls["Edges"]
        assert isinstance(control, _JobTemplateJsonEditWidget)
        assert isinstance(control.edit_control, QPlainTextEdit)
        assert control.edit_control.toPlainText() == "[[1, 2], [3]]"
        assert control.value() == [[1, 2], [3]]
        assert control.is_valid()
        assert not control.warning_icon.isVisibleTo(control)
        assert widget.invalid_parameter_names() == []

    def test_no_default_starts_with_an_empty_list(self, qtbot):
        widget = _make(qtbot, _param())
        control = widget.controls["Edges"]
        assert control.edit_control.toPlainText() == "[]"
        assert control.value() == []
        assert control.is_valid()

    def test_value_from_bundle_is_preferred_over_default(self, qtbot):
        widget = _make(qtbot, _param(default=[[1]], value=[[7, 8]]))
        assert widget.controls["Edges"].edit_control.toPlainText() == "[[7, 8]]"

    def test_label_and_description(self, qtbot):
        widget = _make(
            qtbot,
            _param(description="Task dependencies", userInterface={"label": "Dependencies"}),
        )
        control = widget.controls["Edges"]
        assert control.label.text() == "Dependencies"
        assert control.label.toolTip() == "Task dependencies"
        assert control.edit_control.toolTip() == "Task dependencies"

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("[[1, 2], [3]]", [[1, 2], [3]]),
            ("[\n  [1, 2],\n  []\n]", [[1, 2], []]),
            ("[]", []),
        ],
    )
    def test_typed_json_is_the_value(self, qtbot, text, expected):
        widget = _make(qtbot, _param())
        control = widget.controls["Edges"]
        changed = MagicMock()
        widget.parameter_changed.connect(changed)
        control.edit_control.setPlainText(text)
        assert control.value() == expected
        assert control.is_valid()
        assert changed.call_args.args[0]["value"] == expected

    @pytest.mark.parametrize(
        ("text", "error"),
        [
            pytest.param("[[1, 2], [3]", "not valid JSON", id="incomplete"),
            pytest.param("", "not valid JSON", id="empty"),
            pytest.param("1, 2", "not valid JSON", id="not-json"),
            pytest.param("5", "JSON but not a list", id="scalar"),
            pytest.param('"[[1]]"', "JSON but not a list", id="json-string"),
            pytest.param('{"a": [1]}', "JSON but not a list", id="object"),
            pytest.param("[1, 2]", "item 0 is 1 of type", id="flat"),
            pytest.param('[["1"]]', "item 0 element 0 is '1'", id="string-element"),
            pytest.param("[[true]]", "item 0 element 0 is True", id="bool-element"),
            pytest.param("[[1.5]]", "item 0 element 0 is 1.5", id="float-element"),
            pytest.param(f"[[{2**63}]]", "outside the 64-bit integer range", id="int64"),
        ],
    )
    def test_invalid_text_is_marked_and_explained(self, qtbot, text, error):
        widget = _make(qtbot, _param(default=[[1]], description="Deps"))
        control = widget.controls["Edges"]
        valid = MagicMock()
        widget.valid_parameters.connect(valid)

        control.edit_control.setPlainText(text)

        assert not control.is_valid()
        assert widget.invalid_parameter_names() == ["Edges"]
        valid.assert_called_with(False)
        assert error in control.edit_control.toolTip()
        assert error in control.warning_icon.toolTip()
        assert control.warning_icon.isVisibleTo(control)
        assert "red" in control.edit_control.styleSheet()

        # Correcting the text clears the warning and restores the description.
        control.edit_control.setPlainText("[[2]]")
        assert control.is_valid()
        valid.assert_called_with(True)
        assert not control.warning_icon.isVisibleTo(control)
        assert control.edit_control.styleSheet() == ""
        assert control.edit_control.toolTip() == "Deps"

    def test_invalid_text_value_is_kept_as_typed(self, qtbot):
        widget = _make(qtbot, _param())
        control = widget.controls["Edges"]
        control.edit_control.setPlainText("[[1, 2]")
        assert control.value() == "[[1, 2]"
        # A list that fails the constraints is kept as text too.
        control.edit_control.setPlainText("[1, 2]")
        assert control.value() == "[1, 2]"

    def test_parameter_constraints_are_checked(self, qtbot):
        widget = _make(
            qtbot,
            _param(
                default=[[1]],
                minLength=1,
                maxLength=2,
                item={"maxLength": 2, "item": {"minValue": 0, "maxValue": 9}},
            ),
        )
        control = widget.controls["Edges"]
        for text, error in [
            ("[]", "has 0 items but minLength is 1"),
            ("[[1], [2], [3]]", "has 3 items but at most 2 are allowed"),
            ("[[1, 2, 3]]", "item 0 has 3 elements but at most 2 are allowed"),
            ("[[10]]", "item 0 element 0 10 is greater than"),
            ("[[-1]]", "item 0 element 0 -1 is less than"),
        ]:
            control.edit_control.setPlainText(text)
            assert not control.is_valid(), text
            assert error in control.edit_control.toolTip()
        control.edit_control.setPlainText("[[0, 9], []]")
        assert control.is_valid()

    @pytest.mark.parametrize(
        ("value", "text"),
        [
            pytest.param([[4, 5]], "[[4, 5]]", id="list"),
            pytest.param("[[4,5],[ 6 ]]", "[[4, 5], [6]]", id="json-is-reformatted"),
            pytest.param("[[4, 5]", "[[4, 5]", id="invalid-json-shown-as-written"),
            pytest.param("[1]", "[1]", id="wrong-shape-reformatted"),
        ],
    )
    def test_set_parameter_value(self, qtbot, value, text):
        widget = _make(qtbot, _param())
        widget.set_parameter_value({"name": "Edges", "value": value})
        assert widget.controls["Edges"].edit_control.toPlainText() == text

    def test_untrusted_bundle_value_does_not_break_the_dialog(self, qtbot):
        widget = _make(qtbot, _param(value={"not": "a list"}))
        control = widget.controls["Edges"]
        assert control.edit_control.toPlainText() == '{"not": "a list"}'
        assert not control.is_valid()

    def test_get_parameters_reports_the_list(self, qtbot):
        widget = _make(qtbot, _param(default=[[1]]))
        widget.controls["Edges"].edit_control.setPlainText("[[3], [1, 2]]")
        (parameter,) = widget.get_parameters()
        assert parameter["value"] == [[3], [1, 2]]

    def test_does_not_expand_to_fill_the_tab(self, qtbot):
        assert _JobTemplateJsonEditWidget.IS_VERTICAL_EXPANDING is False
        widget = _make(qtbot, _param())
        edit = widget.controls["Edges"].edit_control
        assert edit.minimumHeight() == edit.maximumHeight()
        assert edit.maximumHeight() > 2 * edit.fontMetrics().height()

    def test_child_objects_are_parented(self, qtbot):
        widget = _make(qtbot, _param())
        control = widget.controls["Edges"]
        for child in (control.label, control.warning_icon, control.edit_control):
            assert child.parent() is control


class TestHiddenListListInt:
    def test_hidden_control_keeps_the_value(self, qtbot):
        widget = _make(qtbot, _param(default=[[1, 2]], control="HIDDEN"))
        control = widget.controls["Edges"]
        assert isinstance(control, _JobTemplateHiddenWidget)
        assert control.value() == [[1, 2]]

    def test_hidden_parses_json_values(self, qtbot):
        widget = _make(qtbot, _param(default=[[1]], control="HIDDEN"))
        widget.set_parameter_value({"name": "Edges", "value": "[[3], []]"})
        assert widget.controls["Edges"].value() == [[3], []]

    def test_hidden_keeps_invalid_value_for_submission_to_report(self, qtbot):
        widget = _make(qtbot, _param(default=[[1]], control="HIDDEN"))
        widget.set_parameter_value({"name": "Edges", "value": "[[x]]"})
        assert widget.controls["Edges"].value() == "[[x]]"

    @pytest.mark.parametrize("control", ["MULTILINE_EDIT", "LINE_EDIT", "SPIN_BOX_LIST"])
    def test_template_cannot_select_another_control(self, qtbot, control):
        with pytest.raises(DeadlineOperationError, match=f"unsupported control '{control}'"):
            OpenJDParametersWidget(parameter_definitions=[_param(control=control)])


def test_logs_nothing_for_a_valid_default(qtbot, caplog):
    with caplog.at_level(logging.WARNING):
        _make(qtbot, _param(default=[[1]]))
    assert caplog.records == []


def _make_bundle(tmp_path):
    bundle_dir = str(tmp_path / "bundle")
    os.makedirs(bundle_dir)
    return bundle_dir


@pytest.mark.parametrize(
    ("cli_value", "expected"),
    [
        pytest.param("[[1, 2], [3]]", [[1, 2], [3]], id="json"),
        pytest.param("[]", [], id="empty"),
    ],
)
def test_gui_submit_list_list_int_parameter(qtbot, tmp_path, cli_value, expected):
    """A 'bundle gui-submit -p' value is parsed from JSON before the dialog shows it."""
    from deadline.client.ui.job_bundle_submitter import show_job_bundle_submitter

    module = "deadline.client.ui.job_bundle_submitter"
    bundle_dir = _make_bundle(tmp_path)
    captured = {}

    def fake_dialog(**kwargs):
        captured.update(kwargs)
        return MagicMock()

    with (
        patch(f"{module}.validate_directory_symlink_containment"),
        patch(
            f"{module}.read_yaml_or_json_object",
            side_effect=lambda _dir, name, *a, **k: (
                {"name": "Bundle Job", "steps": []} if name == "template" else None
            ),
        ),
        patch(f"{module}.read_job_bundle_parameters", return_value=[_param(default=[[0]])]),
        patch(f"{module}.run_pre_gui_hooks", return_value={}),
        patch(f"{module}.SubmitJobToDeadlineDialog", side_effect=fake_dialog),
        patch(f"{module}.QApplication"),
        patch(f"{module}.QMessageBox"),
        patch(f"{module}._get_setting", side_effect=lambda name, config=None: "false"),
        patch(f"{module}._config_file") as cfg,
        patch(f"{module}._validate_and_warn_about_parameters", return_value=True),
    ):
        cfg.str2bool.side_effect = lambda v: str(v).lower() == "true"
        show_job_bundle_submitter(
            input_job_bundle_dir=bundle_dir,
            job_parameters=[{"name": "Edges", "value": cli_value}],
        )

    (edges,) = captured["initial_job_settings"].parameters
    assert edges["value"] == expected
    assert captured["initial_shared_parameter_values"]["Edges"] == expected


def test_gui_submit_rejects_invalid_list_list_int_parameter(qtbot, tmp_path):
    from deadline.client.ui.job_bundle_submitter import _validated_cli_value

    with pytest.raises(DeadlineOperationError, match="item 0 element 0 is 'a'"):
        _validated_cli_value(_param(), '[["a"]]')
