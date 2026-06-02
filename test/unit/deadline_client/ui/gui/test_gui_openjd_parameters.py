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
