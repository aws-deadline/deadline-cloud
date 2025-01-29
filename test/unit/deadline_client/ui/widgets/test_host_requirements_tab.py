# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import math
from unittest.mock import MagicMock

import pytest

try:
    from deadline.client.ui.widgets.host_requirements_tab import (
        AMOUNT_CAPABILITY_PREFIX,
        ATTRIBUTE_CAPABILITY_PREFIX,
        MAX_INT_VALUE,
        RESERVED_FIRST_IDENTIFIERS,
        CustomAmountWidget,
        CustomAttributeValueWidget,
        CustomAttributeWidget,
        CustomRequirementsWidget,
        HardwareRequirementsWidget,
    )
except ImportError:
    # The tests in this file should be skipped if Qt UI related modules cannot be loaded
    pytest.importorskip("deadline.client.ui.widgets.host_requirements_tab")

from deadline.client.exceptions import NonValidInputError

IDENTFIER_MAX_LENGTH = 64
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


def test_name_in_custom_amount_widget_should_not_allow_invalid_chars(qtbot):
    widget = CustomAmountWidget(MagicMock(), 1)
    qtbot.addWidget(widget)

    invalid_str = ""
    widget.name_line_edit.setText(invalid_str)
    assert widget.name_line_edit.hasAcceptableInput() is False


def test_name_in_custom_amount_widget_should_allow_identifiers(qtbot):
    widget = CustomAmountWidget(MagicMock(), 1)
    qtbot.addWidget(widget)

    valid_identifier = "a" + (".a" * math.floor((AMOUNT_NAME_MAX_LENGTH - 1) / 2))
    widget.name_line_edit.setText(valid_identifier)
    assert widget.name_line_edit.hasAcceptableInput()


def test_name_in_custom_amount_widget_does_not_allow_invalid_identifiers(qtbot):
    widget = CustomAmountWidget(MagicMock(), 1)
    qtbot.addWidget(widget)

    valid_identifier = "a"
    invalid_identifier = "a" * (IDENTFIER_MAX_LENGTH + 1)

    widget.name_line_edit.setText(".".join([valid_identifier, invalid_identifier]))
    assert widget.name_line_edit.hasAcceptableInput() is False


def test_name_in_custom_amount_widget_should_not_allow_missing_identifiers(qtbot):
    widget = CustomAmountWidget(MagicMock(), 1)
    qtbot.addWidget(widget)

    missing_identifier = "a..a"
    widget.name_line_edit.setText(missing_identifier)
    assert widget.name_line_edit.hasAcceptableInput() is False


def test_name_in_custom_amount_widget_should_not_allow_reserved_first_identifier(qtbot):
    widget = CustomAmountWidget(MagicMock(), 1)
    qtbot.addWidget(widget)

    for reserved_identifier in RESERVED_FIRST_IDENTIFIERS:
        widget.name_line_edit.setText(reserved_identifier)
        with pytest.raises(NonValidInputError) as e:
            widget.get_requirement()

        assert str(
            e.value
        ) == "Please make sure that the first identifier in your name is not a reserved identifier. " + str(
            RESERVED_FIRST_IDENTIFIERS
        )


def test_value_in_custom_amount_widget_should_be_integer_within_range(qtbot):
    widget = CustomAmountWidget(MagicMock(), 1)
    qtbot.addWidget(widget)

    assert widget.min_spin_box.min == 0
    assert widget.min_spin_box.max == MAX_INT_VALUE
    assert widget.max_spin_box.min == 0
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


def test_name_in_custom_attribute_widget_should_allow_identifiers(qtbot):
    widget = CustomAttributeWidget(MagicMock(), 1, CustomRequirementsWidget())
    qtbot.addWidget(widget)

    valid_identifier = "a" + (".a" * math.floor((AMOUNT_NAME_MAX_LENGTH - 1) / 2))
    widget.name_line_edit.setText(valid_identifier)
    assert widget.name_line_edit.hasAcceptableInput()


def test_name_in_custom_attribute_widget_does_not_allow_invalid_identifiers(qtbot):
    widget = CustomAttributeWidget(MagicMock(), 1, CustomRequirementsWidget())
    qtbot.addWidget(widget)

    valid_identifier = "a"
    invalid_identifier = "a" * (IDENTFIER_MAX_LENGTH + 1)

    widget.name_line_edit.setText(".".join([valid_identifier, invalid_identifier]))
    assert widget.name_line_edit.hasAcceptableInput() is False


def test_name_in_custom_attribute_widget_should_not_allow_missing_identifiers(qtbot):
    widget = CustomAttributeWidget(MagicMock(), 1, CustomRequirementsWidget())
    qtbot.addWidget(widget)

    missing_identifier = "a..a"
    widget.name_line_edit.setText(missing_identifier)
    assert widget.name_line_edit.hasAcceptableInput() is False


def test_name_in_custom_attribute_widget_should_not_end_with_period(qtbot):
    widget = CustomAttributeWidget(MagicMock(), 1, CustomRequirementsWidget())
    value_widget = CustomAttributeValueWidget(MagicMock(), widget)
    qtbot.addWidget(widget)
    qtbot.addWidget(value_widget)

    value_widget.line_edit.setText("test")

    identifier_ends_with_period = "a."
    widget.name_line_edit.setText(identifier_ends_with_period)
    with pytest.raises(NonValidInputError) as e:
        widget.get_requirement()

    assert str(e.value) == "Your requirement name cannot end with a period."


def test_name_in_custom_attribute_widget_should_not_allow_reserved_first_identifier(qtbot):
    widget = CustomAttributeWidget(MagicMock(), 1, CustomRequirementsWidget())
    value_widget = CustomAttributeValueWidget(MagicMock(), widget)
    qtbot.addWidget(widget)
    qtbot.addWidget(value_widget)

    value_widget.line_edit.setText("test")

    for reserved_identifier in RESERVED_FIRST_IDENTIFIERS:
        widget.name_line_edit.setText(reserved_identifier)
        with pytest.raises(NonValidInputError) as e:
            widget.get_requirement()

        assert str(
            e.value
        ) == "Please make sure that the first identifier in your name is not a reserved identifier. " + str(
            RESERVED_FIRST_IDENTIFIERS
        )
