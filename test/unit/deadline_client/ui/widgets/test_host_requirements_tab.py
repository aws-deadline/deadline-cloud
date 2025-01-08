# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
from unittest.mock import MagicMock

try:
    from deadline.client.ui.widgets.host_requirements_tab import (
        HardwareRequirementsWidget,
        CustomAmountWidget,
        CustomAttributeWidget,
        CustomAttributeValueWidget,
        CustomRequirementsWidget,
        ATTRIBUTE_CAPABILITY_PREFIX,
        AMOUNT_CAPABILITY_PREFIX,
        MIN_INT_VALUE,
        MAX_INT_VALUE,
    )
except ImportError:
    # The tests in this file should be skipped if Qt UI related modules cannot be loaded
    pytest.importorskip("deadline.client.ui.widgets.host_requirements_tab")


AMOUNT_NAME_MAX_LENGTH = 100 - len(AMOUNT_CAPABILITY_PREFIX)
ATTRIBUTE_NAME_MAX_LENGTH = 100 - len(ATTRIBUTE_CAPABILITY_PREFIX)


def test_input_in_hardware_requirements_widget_should_be_integer_within_range(qtbot):
    widget = HardwareRequirementsWidget()
    qtbot.addWidget(widget)

    assert widget.cpu_row.min_spin_box.min == 0
    assert widget.cpu_row.min_spin_box.max == 100000
    assert widget.cpu_row.max_spin_box.min == 0
    assert widget.cpu_row.max_spin_box.max == 100000

    assert widget.memory_row.min_spin_box.min == 0
    assert widget.memory_row.min_spin_box.max == 100000
    assert widget.memory_row.max_spin_box.min == 0
    assert widget.memory_row.max_spin_box.max == 100000

    assert widget.gpu_row.min_spin_box.min == 0
    assert widget.gpu_row.min_spin_box.max == 100000
    assert widget.gpu_row.max_spin_box.min == 0
    assert widget.gpu_row.max_spin_box.max == 100000

    assert widget.gpu_memory_row.min_spin_box.min == 0
    assert widget.gpu_memory_row.min_spin_box.max == 100000
    assert widget.gpu_memory_row.max_spin_box.min == 0
    assert widget.gpu_memory_row.max_spin_box.max == 100000

    assert widget.scratch_space_row.min_spin_box.min == 0
    assert widget.scratch_space_row.min_spin_box.max == 100000
    assert widget.scratch_space_row.max_spin_box.min == 0
    assert widget.scratch_space_row.max_spin_box.max == 100000


def test_name_in_custom_amount_widget_should_be_truncated(qtbot):
    widget = CustomAmountWidget(MagicMock(), 1)
    qtbot.addWidget(widget)

    invalid_str = "a" * (AMOUNT_NAME_MAX_LENGTH + 1)
    widget.name_line_edit.setText(invalid_str)
    assert widget.name_line_edit.text() == invalid_str[:AMOUNT_NAME_MAX_LENGTH]


def test_name_in_custom_amount_widget_should_follow_regex_pattern(qtbot):
    widget = CustomAmountWidget(MagicMock(), 1)
    qtbot.addWidget(widget)

    invalid_str = ""
    widget.name_line_edit.setText(invalid_str)
    assert widget.name_line_edit.hasAcceptableInput() is False


def test_value_in_custom_amount_widget_should_be_integer_within_range(qtbot):
    widget = CustomAmountWidget(MagicMock(), 1)
    qtbot.addWidget(widget)

    assert widget.min_spin_box.min == MIN_INT_VALUE
    assert widget.min_spin_box.max == MAX_INT_VALUE
    assert widget.max_spin_box.min == MIN_INT_VALUE
    assert widget.max_spin_box.max == MAX_INT_VALUE


def test_name_in_custom_attribute_widget_should_be_truncated(qtbot):
    widget = CustomAttributeWidget(MagicMock(), 1, CustomRequirementsWidget())
    qtbot.addWidget(widget)

    invalid_str = "a" * (ATTRIBUTE_NAME_MAX_LENGTH + 1)
    widget.name_line_edit.setText(invalid_str)
    assert widget.name_line_edit.text() == invalid_str[:ATTRIBUTE_NAME_MAX_LENGTH]


def test_name_in_custom_attribute_widget_should_follow_regex_pattern(qtbot):
    widget = CustomAttributeWidget(MagicMock(), 1, CustomRequirementsWidget())
    qtbot.addWidget(widget)

    invalid_str = ""
    widget.name_line_edit.setText(invalid_str)
    assert widget.name_line_edit.hasAcceptableInput() is False


def test_value_in_custom_attribute_widget_should_be_truncated(qtbot):
    parent_widget = CustomAttributeWidget(MagicMock(), 1, CustomRequirementsWidget())
    widget = CustomAttributeValueWidget(MagicMock(), parent_widget)
    qtbot.addWidget(widget)

    invalid_str = "a" * 101
    widget.line_edit.setText(invalid_str)
    assert widget.line_edit.text() == invalid_str[:100]


def test_value_in_custom_attribute_widget_should_follow_regex_pattern(qtbot):
    parent_widget = CustomAttributeWidget(MagicMock(), 1, CustomRequirementsWidget())
    widget = CustomAttributeValueWidget(MagicMock(), parent_widget)
    qtbot.addWidget(widget)

    invalid_str = ""
    widget.line_edit.setText(invalid_str)
    assert widget.line_edit.hasAcceptableInput() is False
