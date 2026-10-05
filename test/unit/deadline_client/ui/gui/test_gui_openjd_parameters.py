# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for OpenJDParametersWidget covering all control types."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from deadline.client.ui.widgets.openjd_parameters_widget import OpenJDParametersWidget


def _line_edit_param(name="MyString", default="hello"):
    return {
        "name": name,
        "type": "STRING",
        "default": default,
        "userInterface": {"control": "LINE_EDIT", "label": name},
    }


def _multiline_param(name="Notes", default=""):
    return {
        "name": name,
        "type": "STRING",
        "default": default,
        "userInterface": {"control": "MULTILINE_EDIT", "label": name},
    }


def _dropdown_param(name="Format", default="PNG", allowed=None):
    return {
        "name": name,
        "type": "STRING",
        "default": default,
        "allowedValues": allowed or ["PNG", "EXR", "JPEG"],
        "userInterface": {"control": "DROPDOWN_LIST", "label": name},
    }


def _checkbox_param(name="EnableFeature", default="True"):
    return {
        "name": name,
        "type": "STRING",
        "default": default,
        "allowedValues": ["True", "False"],
        "userInterface": {"control": "CHECK_BOX", "label": name},
    }


def _bool_param(name="EnableFeature", default=True, control="CHECK_BOX"):
    return {
        "name": name,
        "type": "BOOL",
        "default": default,
        "userInterface": {"control": control, "label": name},
    }


def _range_expr_param(name="Frames", default="1-100", control="LINE_EDIT", **extra):
    return {
        "name": name,
        "type": "RANGE_EXPR",
        "default": default,
        "userInterface": {"control": control, "label": name},
        **extra,
    }


def _int_spinbox_param(name="Frames", default=10, min_val=1, max_val=1000):
    return {
        "name": name,
        "type": "INT",
        "default": default,
        "minValue": min_val,
        "maxValue": max_val,
        "userInterface": {"control": "SPIN_BOX", "label": name},
    }


def _float_spinbox_param(name="Scale", default=1.0):
    return {
        "name": name,
        "type": "FLOAT",
        "default": default,
        "userInterface": {"control": "SPIN_BOX", "label": name, "decimals": 2},
    }


def _hidden_param(name="InternalId", default="abc123"):
    return {
        "name": name,
        "type": "STRING",
        "default": default,
        "userInterface": {"control": "HIDDEN"},
    }


def _directory_param(name="OutputDir", default="/tmp/output"):
    return {
        "name": name,
        "type": "PATH",
        "default": default,
        "userInterface": {"control": "CHOOSE_DIRECTORY", "label": name},
    }


def _input_file_param(name="SceneFile", default="/tmp/scene.blend"):
    return {
        "name": name,
        "type": "PATH",
        "default": default,
        "userInterface": {"control": "CHOOSE_INPUT_FILE", "label": name},
    }


def _grouped_params():
    return [
        {
            "name": "ResX",
            "type": "INT",
            "default": 1920,
            "userInterface": {
                "control": "SPIN_BOX",
                "label": "Resolution X",
                "groupLabel": "Resolution",
            },
        },
        {
            "name": "ResY",
            "type": "INT",
            "default": 1080,
            "userInterface": {
                "control": "SPIN_BOX",
                "label": "Resolution Y",
                "groupLabel": "Resolution",
            },
        },
    ]


class TestOpenJDParametersWidget:
    def test_line_edit_creation_and_value(self, qtbot):
        """Verify LINE_EDIT widget is created with correct default value."""
        widget = OpenJDParametersWidget(parameter_definitions=[_line_edit_param()])
        qtbot.addWidget(widget)

        assert "MyString" in widget.controls
        assert widget.controls["MyString"].value() == "hello"

    def test_line_edit_set_value(self, qtbot):
        """Verify LINE_EDIT value can be updated programmatically."""
        widget = OpenJDParametersWidget(parameter_definitions=[_line_edit_param()])
        qtbot.addWidget(widget)

        widget.set_parameter_value({"name": "MyString", "value": "world"})
        assert widget.controls["MyString"].value() == "world"

    def test_multiline_edit_creation(self, qtbot):
        """Verify MULTILINE_EDIT widget is created and handles text."""
        widget = OpenJDParametersWidget(
            parameter_definitions=[_multiline_param(default="line1\nline2")]
        )
        qtbot.addWidget(widget)

        assert widget.controls["Notes"].value() == "line1\nline2"

    def test_dropdown_list_creation_and_value(self, qtbot):
        """Verify DROPDOWN_LIST widget is created with correct allowed values."""
        widget = OpenJDParametersWidget(parameter_definitions=[_dropdown_param()])
        qtbot.addWidget(widget)

        assert widget.controls["Format"].value() == "PNG"

    def test_dropdown_list_set_value(self, qtbot):
        """Verify DROPDOWN_LIST value can be changed."""
        widget = OpenJDParametersWidget(parameter_definitions=[_dropdown_param()])
        qtbot.addWidget(widget)

        widget.set_parameter_value({"name": "Format", "value": "EXR"})
        assert widget.controls["Format"].value() == "EXR"

    def test_checkbox_true_false(self, qtbot):
        """Verify CHECK_BOX widget handles True/False allowed values."""
        widget = OpenJDParametersWidget(parameter_definitions=[_checkbox_param(default="True")])
        qtbot.addWidget(widget)

        assert widget.controls["EnableFeature"].value() == "True"

        widget.set_parameter_value({"name": "EnableFeature", "value": "False"})
        assert widget.controls["EnableFeature"].value() == "False"

    def test_checkbox_yes_no(self, qtbot):
        """Verify CHECK_BOX widget handles Yes/No allowed values."""
        param = {
            "name": "Confirm",
            "type": "STRING",
            "default": "Yes",
            "allowedValues": ["Yes", "No"],
            "userInterface": {"control": "CHECK_BOX", "label": "Confirm"},
        }
        widget = OpenJDParametersWidget(parameter_definitions=[param])  # type: ignore[list-item]
        qtbot.addWidget(widget)

        assert widget.controls["Confirm"].value() == "Yes"
        widget.set_parameter_value({"name": "Confirm", "value": "No"})
        assert widget.controls["Confirm"].value() == "No"

    def test_native_bool_checkbox(self, qtbot):
        """Verify BOOL parameters use native boolean checkbox values."""
        widget = OpenJDParametersWidget(parameter_definitions=[_bool_param()])
        qtbot.addWidget(widget)

        assert widget.controls["EnableFeature"].value() is True

        widget.set_parameter_value({"name": "EnableFeature", "value": False})
        assert widget.controls["EnableFeature"].value() is False

    def test_native_bool_checkbox_coerces_string_values(self, qtbot):
        """Verify BOOL checkboxes coerce OpenJD's accepted string values."""
        widget = OpenJDParametersWidget(parameter_definitions=[_bool_param(default="true")])
        qtbot.addWidget(widget)

        assert widget.controls["EnableFeature"].value() is True

        widget.set_parameter_value({"name": "EnableFeature", "value": "false"})
        assert widget.controls["EnableFeature"].value() is False

    def test_native_bool_checkbox_defaults_false(self, qtbot):
        """Verify a BOOL parameter without a value defaults to false."""
        parameter = {"name": "EnableFeature", "type": "BOOL"}
        widget = OpenJDParametersWidget(parameter_definitions=[parameter])  # type: ignore[list-item]
        qtbot.addWidget(widget)

        assert widget.controls["EnableFeature"].value() is False

    def test_native_bool_checkbox_invalid_value_degrades_gracefully(self, qtbot):
        """Verify an invalid BOOL value from an untrusted bundle leaves the box
        unchecked instead of raising out of widget construction."""
        parameter = {"name": "EnableFeature", "type": "BOOL", "value": "maybe"}
        widget = OpenJDParametersWidget(parameter_definitions=[parameter])  # type: ignore[list-item]
        qtbot.addWidget(widget)

        assert widget.controls["EnableFeature"].value() is False

        # Same for set_parameter_value after construction: an invalid value
        # unchecks the box rather than raising.
        widget.set_parameter_value({"name": "EnableFeature", "value": True})
        assert widget.controls["EnableFeature"].value() is True
        widget.set_parameter_value({"name": "EnableFeature", "value": "maybe"})
        assert widget.controls["EnableFeature"].value() is False

    def test_int_spinbox_creation_and_range(self, qtbot):
        """Verify INT SPIN_BOX respects min/max values."""
        widget = OpenJDParametersWidget(
            parameter_definitions=[_int_spinbox_param(default=10, min_val=1, max_val=100)]
        )
        qtbot.addWidget(widget)

        control = widget.controls["Frames"]
        assert control.value() == 10
        assert control.edit_control.minimum() == 1
        assert control.edit_control.maximum() == 100

    def test_float_spinbox_creation(self, qtbot):
        """Verify FLOAT SPIN_BOX is created with correct default."""
        widget = OpenJDParametersWidget(parameter_definitions=[_float_spinbox_param(default=2.5)])
        qtbot.addWidget(widget)

        assert abs(widget.controls["Scale"].value() - 2.5) < 0.01

    def test_hidden_widget_not_visible(self, qtbot):
        """Verify HIDDEN widget stores value but has no visible UI."""
        widget = OpenJDParametersWidget(parameter_definitions=[_hidden_param()])
        qtbot.addWidget(widget)

        assert widget.controls["InternalId"].value() == "abc123"
        widget.set_parameter_value({"name": "InternalId", "value": "xyz789"})
        assert widget.controls["InternalId"].value() == "xyz789"

    def test_hidden_bool_widget(self, qtbot):
        """Verify a hidden BOOL parameter stores a native boolean value."""
        widget = OpenJDParametersWidget(
            parameter_definitions=[_bool_param(default=False, control="HIDDEN")]
        )
        qtbot.addWidget(widget)

        assert widget.controls["EnableFeature"].value() is False

    def test_hidden_bool_widget_without_default_is_false(self, qtbot):
        """Verify a hidden BOOL with no value or default holds False, not the string ""."""
        parameter = {
            "name": "EnableFeature",
            "type": "BOOL",
            "userInterface": {"control": "HIDDEN"},
        }
        widget = OpenJDParametersWidget(parameter_definitions=[parameter])  # type: ignore[list-item]
        qtbot.addWidget(widget)

        assert widget.controls["EnableFeature"].value() is False

    def test_hidden_bool_widget_coerces_values(self, qtbot):
        """Verify a hidden BOOL coerces valid spellings to bool and keeps invalid
        values for the submit path to report."""
        widget = OpenJDParametersWidget(
            parameter_definitions=[_bool_param(default=False, control="HIDDEN")]
        )
        qtbot.addWidget(widget)

        widget.set_parameter_value({"name": "EnableFeature", "value": "yes"})
        assert widget.controls["EnableFeature"].value() is True
        widget.set_parameter_value({"name": "EnableFeature", "value": 0})
        assert widget.controls["EnableFeature"].value() is False
        widget.set_parameter_value({"name": "EnableFeature", "value": "maybe"})
        assert widget.controls["EnableFeature"].value() == "maybe"

    def test_range_expr_line_edit_creation_and_value(self, qtbot):
        """Verify a RANGE_EXPR parameter uses a line edit holding the expression string."""
        widget = OpenJDParametersWidget(parameter_definitions=[_range_expr_param()])
        qtbot.addWidget(widget)

        control = widget.controls["Frames"]
        assert control.value() == "1-100"
        assert control.edit_control.hasAcceptableInput()

        widget.set_parameter_value({"name": "Frames", "value": "1-10:2,20,30-40"})
        assert control.value() == "1-10:2,20,30-40"
        assert control.edit_control.hasAcceptableInput()

    def test_range_expr_line_edit_is_default_control(self, qtbot):
        """Verify a RANGE_EXPR parameter without userInterface gets a LINE_EDIT."""
        parameter = {"name": "Frames", "type": "RANGE_EXPR", "default": "1-5"}
        widget = OpenJDParametersWidget(parameter_definitions=[parameter])  # type: ignore[list-item]
        qtbot.addWidget(widget)

        assert widget.controls["Frames"].value() == "1-5"
        assert widget.controls["Frames"].edit_control.placeholderText()

    @pytest.mark.parametrize(
        "text",
        [
            pytest.param("", id="empty"),
            pytest.param("1-", id="incomplete"),
            pytest.param("1-10,5-15", id="overlap"),
            pytest.param("5-1", id="descending"),
            pytest.param("1-10:0", id="zero-step"),
        ],
    )
    def test_range_expr_line_edit_flags_invalid_text(self, qtbot, text):
        """Verify text that is not a valid range expression is flagged, kept editable,
        and still reported as the value so submission can report the error."""
        widget = OpenJDParametersWidget(parameter_definitions=[_range_expr_param()])
        qtbot.addWidget(widget)
        control = widget.controls["Frames"]

        control.edit_control.setText(text)

        assert control.value() == text
        assert not control.edit_control.hasAcceptableInput()
        assert "red" in control.edit_control.styleSheet()
        if text:
            assert "not a valid range expression" in control.edit_control.toolTip()

    def test_range_expr_line_edit_clears_flag_when_valid(self, qtbot):
        """Verify the invalid flag is removed once the text becomes valid, restoring
        the description tooltip."""
        widget = OpenJDParametersWidget(
            parameter_definitions=[_range_expr_param(description="Frames to render")]
        )
        qtbot.addWidget(widget)
        control = widget.controls["Frames"]

        control.edit_control.setText("1-10,5-15")
        assert "red" in control.edit_control.styleSheet()

        control.edit_control.setText("1-10,15-20")
        assert control.edit_control.styleSheet() == ""
        assert control.edit_control.toolTip() == "Frames to render"
        assert control.edit_control.hasAcceptableInput()

    def test_range_expr_line_edit_rejects_foreign_characters(self, qtbot):
        """Verify the validator refuses characters that can never be part of a range
        expression, so typing them has no effect."""
        widget = OpenJDParametersWidget(parameter_definitions=[_range_expr_param(default="")])
        qtbot.addWidget(widget)
        control = widget.controls["Frames"]

        qtbot.keyClicks(control.edit_control, "1a-b5c:x2")

        assert control.value() == "1-5:2"
        assert control.edit_control.hasAcceptableInput()

    def test_range_expr_line_edit_length_constraints(self, qtbot):
        """Verify minLength/maxLength are enforced by the validator."""
        widget = OpenJDParametersWidget(
            parameter_definitions=[_range_expr_param(default="1-10", minLength=3, maxLength=6)]
        )
        qtbot.addWidget(widget)
        control = widget.controls["Frames"]

        control.edit_control.setText("1")
        assert not control.edit_control.hasAcceptableInput()

        control.edit_control.setText("1-100")
        assert control.edit_control.hasAcceptableInput()

        qtbot.keyClicks(control.edit_control, ":10")
        # Over maxLength: the extra characters are refused.
        assert control.value() == "1-100:"
        assert not control.edit_control.hasAcceptableInput()

    def test_range_expr_line_edit_service_length_cap_is_consistent(self, qtbot):
        """Verify that with no explicit maxLength, a grammatically valid expression longer than
        the service's 1024-character limit is reported invalid by both the validator (which
        disables Submit) and the feedback path (red border and tooltip), so the user is told why."""
        long_expr = ",".join(str(i) for i in range(0, 1000, 2))
        assert len(long_expr) > 1024
        widget = OpenJDParametersWidget(parameter_definitions=[_range_expr_param()])
        qtbot.addWidget(widget)
        control = widget.controls["Frames"]

        # setText bypasses the validator's input filtering, like a value loaded from a bundle.
        control.edit_control.setText(long_expr)

        assert widget.invalid_parameter_names() == ["Frames"]
        assert "red" in control.edit_control.styleSheet()
        assert "at most 1024" in control.edit_control.toolTip()

    def test_range_expr_line_edit_emits_parameter_changed(self, qtbot):
        """Verify edits emit parameter_changed with the RANGE_EXPR definition and text."""
        widget = OpenJDParametersWidget(parameter_definitions=[_range_expr_param()])
        qtbot.addWidget(widget)
        callback = MagicMock()
        widget.parameter_changed.connect(callback)

        widget.controls["Frames"].edit_control.setText("1-5")

        message = callback.call_args.args[0]
        assert message["name"] == "Frames"
        assert message["type"] == "RANGE_EXPR"
        assert message["value"] == "1-5"

    def test_hidden_range_expr_widget(self, qtbot):
        """Verify a hidden RANGE_EXPR parameter stores the expression string."""
        widget = OpenJDParametersWidget(
            parameter_definitions=[_range_expr_param(default="1-10:2", control="HIDDEN")]
        )
        qtbot.addWidget(widget)

        assert widget.controls["Frames"].value() == "1-10:2"
        widget.set_parameter_value({"name": "Frames", "value": "1,2,3"})
        assert widget.controls["Frames"].value() == "1,2,3"

    def test_invalid_parameter_names_tracks_range_expr_validity(self, qtbot):
        """Verify the widget reports which parameters hold invalid values, and emits
        valid_parameters as that changes. Controls without validation are always valid."""
        widget = OpenJDParametersWidget(
            parameter_definitions=[
                _range_expr_param(),
                _line_edit_param(),
                _bool_param(),
                _int_spinbox_param(name="Count"),
            ]
        )
        qtbot.addWidget(widget)
        validity = MagicMock()
        widget.valid_parameters.connect(validity)

        assert widget.invalid_parameter_names() == []

        widget.controls["Frames"].edit_control.setText("1-10,")
        assert widget.invalid_parameter_names() == ["Frames"]
        assert validity.call_args.args[0] is False

        widget.controls["Frames"].edit_control.setText("1-10,15")
        assert widget.invalid_parameter_names() == []
        assert validity.call_args.args[0] is True

    def test_invalid_parameter_names_string_min_length(self, qtbot):
        """Verify a STRING with minLength is reported invalid until it is long enough."""
        parameter = {**_line_edit_param(default="abc"), "minLength": 3}
        widget = OpenJDParametersWidget(parameter_definitions=[parameter])  # type: ignore[list-item]
        qtbot.addWidget(widget)

        assert widget.invalid_parameter_names() == []
        widget.controls["MyString"].edit_control.setText("ab")
        assert widget.invalid_parameter_names() == ["MyString"]

    def test_directory_picker_creation(self, qtbot):
        """Verify CHOOSE_DIRECTORY widget is created with correct default."""
        widget = OpenJDParametersWidget(parameter_definitions=[_directory_param()])
        qtbot.addWidget(widget)

        assert widget.controls["OutputDir"].value() == str(Path("/tmp/output"))

    def test_input_file_picker_creation(self, qtbot):
        """Verify CHOOSE_INPUT_FILE widget is created with correct default."""
        widget = OpenJDParametersWidget(parameter_definitions=[_input_file_param()])
        qtbot.addWidget(widget)

        assert widget.controls["SceneFile"].value() == str(Path("/tmp/scene.blend"))

    def test_get_parameters_returns_all_controls(self, qtbot):
        """Verify get_parameters returns values for all controls."""
        params = [_line_edit_param(), _int_spinbox_param(), _hidden_param()]
        widget = OpenJDParametersWidget(parameter_definitions=params)
        qtbot.addWidget(widget)

        result = widget.get_parameters()
        names = [p["name"] for p in result]
        assert "MyString" in names
        assert "Frames" in names
        assert "InternalId" in names

        values = {p["name"]: p["value"] for p in result}
        assert values["MyString"] == "hello"
        assert values["Frames"] == 10
        assert values["InternalId"] == "abc123"

    def test_parameter_changed_signal_emitted(self, qtbot):
        """Verify parameter_changed signal fires when a control value changes."""
        widget = OpenJDParametersWidget(parameter_definitions=[_line_edit_param()])
        qtbot.addWidget(widget)

        handler = MagicMock()
        widget.parameter_changed.connect(handler)

        widget.controls["MyString"].edit_control.setText("new value")
        assert handler.called
        assert handler.call_args[0][0]["value"] == "new value"

    def test_async_loading_state(self, qtbot):
        """Verify widget shows loading message when async_loading_state is set."""
        widget = OpenJDParametersWidget(async_loading_state="Loading queue parameters...")
        qtbot.addWidget(widget)

        assert widget.async_loading_state == "Loading queue parameters..."
        assert len(widget.controls) == 0

    def test_rebuild_ui_replaces_controls(self, qtbot):
        """Verify rebuild_ui replaces existing controls with new ones."""
        widget = OpenJDParametersWidget(parameter_definitions=[_line_edit_param()])
        qtbot.addWidget(widget)
        assert "MyString" in widget.controls

        widget.rebuild_ui(parameter_definitions=[_int_spinbox_param()])
        assert "MyString" not in widget.controls
        assert "Frames" in widget.controls

    def test_group_label_creates_group_box(self, qtbot):
        """Verify parameters with groupLabel are placed inside a QGroupBox."""
        from deadline.client.ui.widgets.openjd_parameters_widget import _JobTemplateGroupLayout

        widget = OpenJDParametersWidget(parameter_definitions=_grouped_params())
        qtbot.addWidget(widget)

        groups: list = list(widget.findChildren(_JobTemplateGroupLayout))  # type: ignore[arg-type]
        assert len(groups) == 1
        assert groups[0].title() == "Resolution"

    def test_colon_parameters_skipped(self, qtbot):
        """Verify parameters with ':' in name (like deadline:priority) are skipped."""
        params = [
            {"name": "deadline:priority", "type": "INT", "default": 50},
            _line_edit_param(),
        ]
        widget = OpenJDParametersWidget(parameter_definitions=params)
        qtbot.addWidget(widget)

        assert "deadline:priority" not in widget.controls
        assert "MyString" in widget.controls

    def test_set_parameter_value_raises_for_unknown(self, qtbot):
        """Verify set_parameter_value raises KeyError for unknown parameter."""
        widget = OpenJDParametersWidget(parameter_definitions=[_line_edit_param()])
        qtbot.addWidget(widget)

        with pytest.raises(KeyError):
            widget.set_parameter_value({"name": "NonExistent", "value": "x"})


# One parameter definition for every control the widget can build.
_EVERY_CONTROL = [
    pytest.param({"type": "STRING"}, id="LINE_EDIT"),
    pytest.param(
        {"type": "STRING", "userInterface": {"control": "MULTILINE_EDIT"}}, id="MULTILINE_EDIT"
    ),
    pytest.param({"type": "RANGE_EXPR", "default": "1-3"}, id="RANGE_EXPR"),
    pytest.param({"type": "INT"}, id="INT_SPIN_BOX"),
    pytest.param({"type": "FLOAT"}, id="FLOAT_SPIN_BOX"),
    pytest.param({"type": "STRING", "allowedValues": ["a", "b"]}, id="DROPDOWN_LIST"),
    pytest.param({"type": "PATH", "objectType": "FILE"}, id="CHOOSE_INPUT_FILE"),
    pytest.param(
        {"type": "PATH", "objectType": "FILE", "dataFlow": "OUT"}, id="CHOOSE_OUTPUT_FILE"
    ),
    pytest.param({"type": "PATH"}, id="CHOOSE_DIRECTORY"),
    pytest.param(
        {
            "type": "STRING",
            "allowedValues": ["TRUE", "FALSE"],
            "userInterface": {"control": "CHECK_BOX"},
        },
        id="STRING_CHECK_BOX",
    ),
    pytest.param({"type": "BOOL"}, id="BOOL_CHECK_BOX"),
    pytest.param({"type": "LIST[STRING]"}, id="LINE_EDIT_LIST"),
    pytest.param({"type": "LIST[INT]"}, id="INT_SPIN_BOX_LIST"),
    pytest.param({"type": "LIST[FLOAT]"}, id="FLOAT_SPIN_BOX_LIST"),
    pytest.param(
        {"type": "LIST[PATH]", "objectType": "FILE", "default": ["a"]},
        id="CHOOSE_INPUT_FILE_LIST",
    ),
    pytest.param(
        {"type": "LIST[PATH]", "objectType": "FILE", "dataFlow": "OUT", "default": ["a"]},
        id="CHOOSE_OUTPUT_FILE_LIST",
    ),
    pytest.param({"type": "LIST[PATH]", "default": ["a"]}, id="CHOOSE_DIRECTORY_LIST"),
    pytest.param(
        {"type": "STRING", "default": "x", "userInterface": {"control": "HIDDEN"}}, id="HIDDEN"
    ),
]


class TestControlsAreNotInReferenceCycles:
    """Qt widgets must only be destroyed on the GUI thread. A widget that only the cyclic
    garbage collector can free, or that is never freed, may instead be destroyed on
    whichever thread the collector runs, such as a background task's thread. Every
    control must therefore be freed by reference counting alone."""

    @pytest.mark.parametrize("definition", _EVERY_CONTROL)
    def test_dropped_widget_is_freed_without_the_garbage_collector(self, qtbot, definition):
        import gc
        import weakref

        gc.collect()
        gc.disable()
        try:
            widget = OpenJDParametersWidget(parameter_definitions=[{"name": "Value", **definition}])
            widget.parameter_changed.connect(lambda message: None)
            ref = weakref.ref(widget)
            del widget
            assert ref() is None
        finally:
            gc.enable()

    @pytest.mark.parametrize(
        ("definition", "edit", "expected"),
        [
            pytest.param(
                {"type": "STRING"}, lambda c: c.edit_control.setText("hi"), "hi", id="LINE_EDIT"
            ),
            pytest.param({"type": "INT"}, lambda c: c.edit_control.setValue(7), 7, id="INT"),
            pytest.param(
                {"type": "FLOAT"}, lambda c: c.edit_control.setValue(2.5), 2.5, id="FLOAT"
            ),
            pytest.param(
                {"type": "STRING", "allowedValues": ["a", "b"]},
                lambda c: c.edit_control.setCurrentIndex(1),
                "b",
                id="DROPDOWN_LIST",
            ),
            pytest.param(
                {"type": "BOOL"}, lambda c: c.edit_control.setChecked(True), True, id="BOOL"
            ),
            pytest.param(
                {"type": "PATH"}, lambda c: c.edit_control.setText("out"), "out", id="PATH"
            ),
        ],
    )
    def test_each_edit_reports_one_change_with_the_value(self, qtbot, definition, edit, expected):
        widget = OpenJDParametersWidget(parameter_definitions=[{"name": "Value", **definition}])
        qtbot.addWidget(widget)
        changed = MagicMock()
        widget.parameter_changed.connect(changed)
        edit(widget.controls["Value"])
        assert changed.call_count == 1
        message = changed.call_args.args[0]
        assert message["name"] == "Value"
        assert message["value"] == expected
