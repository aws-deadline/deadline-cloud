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
    LINE_EDIT_LIST = enum.auto()
    SPIN_BOX_LIST = enum.auto()
    CHECK_BOX_LIST = enum.auto()
    CHOOSE_INPUT_FILE_LIST = enum.auto()
    CHOOSE_OUTPUT_FILE_LIST = enum.auto()
    CHOOSE_DIRECTORY_LIST = enum.auto()
