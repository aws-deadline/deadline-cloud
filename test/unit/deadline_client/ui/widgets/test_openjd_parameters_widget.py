# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
from conftest import STRING_FIELD_MAX_LENGTH
from pathlib import Path

try:
    from deadline.client.ui.widgets.openjd_parameters_widget import (
        _JobTemplateLineEditWidget,
        _JobTemplateIntSpinBoxWidget,
        _JobTemplateFloatSpinBoxWidget,
    )
except ImportError:
    # The tests in this file should be skipped if Qt UI related modules cannot be loaded
    pytest.importorskip("deadline.client.ui.widgets.openjd_parameters_widget")


def test_input_in_line_edit_widget_should_be_truncated(qtbot):
    """
    Test that the line edit widget is created and can be edited.
    """
    widget = _JobTemplateLineEditWidget(None, {"type": "STRING", "name": "test-name"})
    qtbot.addWidget(widget)

    invalid_str = "a" * (STRING_FIELD_MAX_LENGTH + 1)
    widget.set_value(invalid_str)
    assert widget.value() == invalid_str[:STRING_FIELD_MAX_LENGTH]


@pytest.mark.parametrize(
    "object,expected_string",
    [
        pytest.param(5, "5", id="Integer to string"),
        pytest.param(9.4, "9.4", id="Float to string"),
        pytest.param(Path("/some/path"), "/some/path", id="Path to string"),
    ],
)
def test_input_in_line_edit_widget_should_be_casted(qtbot, object, expected_string):
    """
    Tests that inputs to a line edit widget can be converted to strings.
    """

    # GIVEN
    widget = _JobTemplateLineEditWidget(None, {"type": "STRING", "name": "value-to-convert"})
    qtbot.addWidget(widget)

    # WHEN
    widget.set_value(object)

    # THEN
    assert widget.value() == expected_string


def test_input_in_int_spin_box_widget_should_be_integer_within_range(qtbot):
    """
    Test that the line edit widget is created and can be edited.
    """
    widget = _JobTemplateIntSpinBoxWidget(
        None, {"type": "INT", "name": "test-name", "minValue": 0, "maxValue": 99}
    )
    qtbot.addWidget(widget)

    widget.set_value(-1)
    assert widget.value() == 0

    widget.set_value(100)
    assert widget.value() == 99


def test_input_in_float_spin_box_widget_should_be_float_within_range(qtbot):
    """
    Test that the line edit widget is created and can be edited.
    """
    widget = _JobTemplateFloatSpinBoxWidget(
        None, {"type": "FLOAT", "name": "test-name", "minValue": 0, "maxValue": 99}
    )
    qtbot.addWidget(widget)

    widget.set_value(-1)
    assert widget.value() == 0

    widget.set_value(100)
    assert widget.value() == 99
