# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

__all__ = ["ControlType"]
import enum


class ControlType(enum.Enum):
    """
    Defines all the valid GUI control types that are supported in an Open Job Description
    job template parameter's "userInterface" property.
    """

    LINE_EDIT = enum.auto()
    MULTILINE_EDIT = enum.auto()
    # SPIN_BOX for INT type
    INT_SPIN_BOX = enum.auto()
    # SPIN_BOX for FLOAT type
    FLOAT_SPIN_BOX = enum.auto()
    DROPDOWN_LIST = enum.auto()
    CHOOSE_INPUT_FILE = enum.auto()
    CHOOSE_OUTPUT_FILE = enum.auto()
    CHOOSE_DIRECTORY = enum.auto()
    CHECK_BOX = enum.auto()
    HIDDEN = enum.auto()
