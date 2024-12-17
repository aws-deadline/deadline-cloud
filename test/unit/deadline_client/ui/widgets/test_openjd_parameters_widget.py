# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from deadline.client.ui.widgets.openjd_parameters_widget import (
    _JobTemplateLineEditWidget,
    _JobTemplateIntSpinBoxWidget,
    _JobTemplateFloatSpinBoxWidget,
)
from conftest import STRING_FIELD_MAX_LENGHTH


def test_input_in_line_edit_widget_should_be_truncated(qtbot):
    """
    Test that the line edit widget is created and can be edited.
    """
    widget = _JobTemplateLineEditWidget(None, {"type": "STRING", "name": "test-name"})
    qtbot.addWidget(widget)

    invalid_str = "a" * (STRING_FIELD_MAX_LENGHTH + 1)
    widget.set_value(invalid_str)
    assert widget.value() == invalid_str[:STRING_FIELD_MAX_LENGHTH]


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
