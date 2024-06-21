# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
UI widgets for the Scene Settings tab.
"""
from __future__ import annotations

import os
from typing import Any, Dict, List
from copy import deepcopy

from qtpy.QtCore import QRegularExpression, Qt, Signal  # type: ignore
from qtpy.QtGui import QValidator
from qtpy.QtWidgets import (  # type: ignore
    QCheckBox,
    QComboBox,
    QDoubleSpinBox,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QSizePolicy,
    QSpacerItem,
    QSpinBox,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from ...job_bundle.job_template import ControlType
from ...job_bundle.parameters import JobParameter, get_ui_control_for_parameter_definition
from .path_widgets import (
    DirectoryPickerWidget,
    InputFilePickerWidget,
    OutputFilePickerWidget,
)
from .spinbox_widgets import DecimalMode, FloatDragSpinBox, IntDragSpinBox


class OpenJDParametersWidget(QWidget):
    """
    Widget that takes the set of Open Job Description parameters, for example from a job template or a queue,
    and generates a UI form to edit them.

    Open Job Description has optional UI metadata for each parameter specified under "userInterface".

    Signals:
        parameter_changed: This is sent whenever a parameter value in the widget changes. The message
            is a copy of the parameter definition with the "value" key containing the new value.

    Args:
        parameter_definitions (List[Dict[str, Any]]): A list of Open Job Description parameter definitions.
        async_loading_state (str): A message to show its async loading state. Cannot provide both this
            message and the parameter_definitions.
        parent: The parent Qt Widget.
    """

    parameter_changed = Signal(dict)

    def __init__(
        self,
        *,
        parameter_definitions: List[JobParameter] = [],
        async_loading_state: str = "",
        parent=None,
    ):
        super().__init__(parent=parent)

        self.rebuild_ui(
            parameter_definitions=parameter_definitions, async_loading_state=async_loading_state
        )

    def rebuild_ui(
        self,
        *,
        parameter_definitions: list[JobParameter] = [],
        async_loading_state: str = "",
    ):
        """
        Rebuilds the widget's UI to the new parameter_definitions, or to display the
        async_loading_state message.
        """
        if parameter_definitions and async_loading_state:
            raise RuntimeError(
                "Constructing or updating an OpenJD parameters widget in the "
                + "async_loading_state requires an empty parameter_definitions list."
            )

        layout = self.layout()
        if isinstance(layout, QVBoxLayout):
            for index in reversed(range(layout.count())):
                child = layout.takeAt(index)
                if child.widget():
                    child.widget().deleteLater()
                elif child.layout():
                    child.layout().deleteLater()
        else:
            layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        self.controls: dict[str, Any] = {}

        if async_loading_state:
            loading = QLabel(async_loading_state, self)
            loading.setAlignment(Qt.AlignCenter)
            loading.setMinimumSize(100, 30)
            loading.setTextInteractionFlags(Qt.TextSelectableByMouse)
            loading.setWordWrap(True)
            layout.addWidget(loading)
            layout.addItem(QSpacerItem(0, 0, QSizePolicy.Minimum, QSizePolicy.Expanding))
            self.async_loading_state = async_loading_state
            return
        else:
            self.async_loading_state = ""

        need_spacer = True

        control_map = {
            ControlType.LINE_EDIT.name: _JobTemplateLineEditWidget,
            ControlType.MULTILINE_EDIT.name: _JobTemplateMultiLineEditWidget,
            ControlType.DROPDOWN_LIST.name: _JobTemplateDropdownListWidget,
            ControlType.CHOOSE_INPUT_FILE.name: _JobTemplateInputFileWidget,
            ControlType.CHOOSE_OUTPUT_FILE.name: _JobTemplateOutputFileWidget,
            ControlType.CHOOSE_DIRECTORY.name: _JobTemplateDirectoryWidget,
            ControlType.CHECK_BOX.name: _JobTemplateCheckBoxWidget,
            ControlType.HIDDEN.name: _JobTemplateHiddenWidget,
        }

        for parameter in parameter_definitions:
            # Skip application-specific parameters like "deadline:priority"
            if ":" in parameter["name"]:
                continue

            # Skip any parameters that do not have a type defined.
            # This can happen when a queue environment parameter was
            # saved to the bundle, but the template itself does not contain
            # that parameter.
            if "type" not in parameter:
                continue

            control_type_name = get_ui_control_for_parameter_definition(parameter)

            if parameter["type"] == "INT" and control_type_name == "SPIN_BOX":
                control_widget = _JobTemplateIntSpinBoxWidget
            elif parameter["type"] == "FLOAT" and control_type_name == "SPIN_BOX":
                control_widget = _JobTemplateFloatSpinBoxWidget
            else:
                control_widget = control_map[control_type_name]

            group_label = parameter.get("userInterface", {}).get("groupLabel", "")

            control = control_widget(self, parameter)
            self.controls[control.name()] = control
            control.connect_parameter_changed(lambda message: self.parameter_changed.emit(message))

            if control_type_name != ControlType.HIDDEN.name:
                if group_label:
                    group_layout = self.findChild(_JobTemplateGroupLayout, group_label)
                    if not group_layout:
                        group_layout = _JobTemplateGroupLayout(self, group_label)
                        group_layout.setObjectName(group_label)
                        layout.addWidget(group_layout)
                    group_layout.layout().addWidget(control)
                else:
                    layout.addWidget(control)

                if control_widget.IS_VERTICAL_EXPANDING:
                    # Turn off the spacer at the end, as there's already a stretchy control
                    need_spacer = False

        if need_spacer:
            layout.addItem(QSpacerItem(0, 0, QSizePolicy.Minimum, QSizePolicy.Expanding))

    def get_parameters(self):
        """
        Returns a list of OpenJD parameter definition dicts with
        a "value" key filled from the widget.
        """
        parameter_values = []
        for control in self.controls.values():
            parameter = deepcopy(control.job_template_parameter)
            parameter["value"] = control.value()
            parameter_values.append(parameter)
        return parameter_values

    def set_parameter_value(self, parameter: dict[str, Any]):
        """
        Given an OpenJD parameter definition with a "value" key,
        set the parameter value in the widget.

        If the parameter value cannot be set, raises a KeyError.
        """
        self.controls[parameter["name"]].set_value(parameter["value"])


def _get_parameter_label(parameter):
    """
    Returns the label to use for this parameter. Default to the label from "userInterface",
    then the parameter name.
    """
    name = parameter["name"]
    if "userInterface" in parameter:
        return parameter["userInterface"].get("label", name)
    else:
        return name


class _JobTemplateLineEditValidator(QValidator):
    def __init__(self, parameter_name, min_length: int, max_length: int, allowed_pattern: str):
        super().__init__()
        self.min_length = min_length
        self.max_length = max_length
        self.allowed_pattern = QRegularExpression(allowed_pattern)
        if not self.allowed_pattern.isValid():
            raise RuntimeError(
                f"Could not process 'allowedPattern' for Job Template parameter {parameter_name} "
                + f"with control LINE_EDIT:\n{self.allowed_pattern.errorString()}"
            )

    def validate(self, s, pos):
        if self.max_length is not None and len(s) > self.max_length:
            return (QValidator.Invalid, s, pos)

        if self.allowed_pattern is not None:
            match = self.allowed_pattern.match(
                s, matchType=QRegularExpression.PartialPreferFirstMatch
            )
            if match.hasPartialMatch():
                return (QValidator.Intermediate, s, pos)
            elif not match.hasMatch():
                return (QValidator.Invalid, s, pos)

        if self.min_length is not None and len(s) < self.min_length:
            return (QValidator.Intermediate, s, pos)

        return (QValidator.Acceptable, s, pos)


class _JobTemplateWidget(QWidget):
    IS_VERTICAL_EXPANDING: bool = False

    def __init__(self, parent, parameter):
        super().__init__(parent)

        self.job_template_parameter = parameter

        # Validate that the template parameter has the right type and fields
        if parameter["type"] not in self.OPENJD_TYPES:
            if len(self.OPENJD_TYPES) == 1:
                raise RuntimeError(
                    f"Job Template parameter {parameter['name']} with control "
                    + f"{self.OPENJD_CONTROL_TYPE} has type {parameter['type']} but "
                    + f"must have type {self.OPENJD_TYPES[0]}."
                )
            else:
                raise RuntimeError(
                    f"Job Template parameter {parameter['name']} with control "
                    + f"{self.OPENJD_CONTROL_TYPE} has type {parameter['type']} but "
                    + f"must have one of type: {[v[0] for v in self.OPENJD_TYPES]}"
                )

        for field in self.OPENJD_REQUIRED_PARAMETER_FIELDS:
            if field not in parameter:
                raise RuntimeError(
                    f"Job Template parameter {parameter['name']} with control "
                    + f"{self.OPENJD_CONTROL_TYPE} is missing required field '{field}'."
                )
        for field in self.OPENJD_DISALLOWED_PARAMETER_FIELDS:
            if field in parameter:
                raise RuntimeError(
                    f"Job Template parameter {parameter['name']} with control "
                    + f"{self.OPENJD_CONTROL_TYPE} must not provide field '{field}'."
                )

        self._build_ui(parameter)

        # Set the initial value to the first of the value, default or a type default
        value = parameter.get("value", parameter.get("default", self.OPENJD_DEFAULT_VALUE))
        self.set_value(value)

    def name(self):
        return self.job_template_parameter["name"]

    def type(self):
        return self.job_template_parameter["type"]


class _JobTemplateLineEditWidget(_JobTemplateWidget):
    OPENJD_CONTROL_TYPE: ControlType = ControlType.LINE_EDIT
    OPENJD_TYPES: List[str] = ["STRING"]
    OPENJD_DEFAULT_VALUE: str = ""
    OPENJD_REQUIRED_PARAMETER_FIELDS: List[str] = []
    OPENJD_DISALLOWED_PARAMETER_FIELDS: List[str] = ["allowedValues"]

    def _build_ui(self, parameter):
        # Create the edit widget
        layout = QHBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        self.label = QLabel(_get_parameter_label(parameter))
        self.edit_control = QLineEdit(self)
        self.edit_control.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
        layout.addWidget(self.label)
        layout.addWidget(self.edit_control)
        self.setLayout(layout)

        # Enable validation if specified
        if "minLength" in parameter or "maxLength" in parameter or "allowedPattern" in parameter:
            self.edit_control.setValidator(
                _JobTemplateLineEditValidator(
                    parameter["name"],
                    parameter.get("minLength", None),
                    parameter.get("maxLength", None),
                    parameter.get("allowedPattern", None),
                )
            )

        # Add the decription as a tooltip if provided
        if "description" in parameter:
            for widget in (self.label, self.edit_control):
                widget.setToolTip(parameter["description"])

    def value(self):
        return self.edit_control.text()

    def set_value(self, value):
        self.edit_control.setText(value)

    def _handle_text_changed(self, text, callback):
        message = deepcopy(self.job_template_parameter)
        message["value"] = text
        callback(message)

    def connect_parameter_changed(self, callback):
        self.edit_control.textChanged.connect(
            lambda text: self._handle_text_changed(text, callback)
        )


class _JobTemplateMultiLineEditWidget(_JobTemplateWidget):
    OPENJD_CONTROL_TYPE: ControlType = ControlType.MULTILINE_EDIT
    OPENJD_TYPES: List[str] = ["STRING"]
    OPENJD_DEFAULT_VALUE: str = ""
    OPENJD_REQUIRED_PARAMETER_FIELDS: List[str] = []
    OPENJD_DISALLOWED_PARAMETER_FIELDS: List[str] = ["allowedValues"]
    IS_VERTICAL_EXPANDING: bool = True

    def _build_ui(self, parameter):
        # Create the edit widget
        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        self.label = QLabel(_get_parameter_label(parameter))
        self.edit_control = QTextEdit(self)
        self.edit_control.setAcceptRichText(False)
        if os.name == "nt":
            font_family = "Consolas"
        elif os.name == "darwin":
            font_family = "Monaco"
        else:
            font_family = "Monospace"
        font = self.edit_control.currentFont()
        font.setFamily(font_family)
        font.setFixedPitch(True)
        font.setKerning(False)
        font.setPointSize(font.pointSize() + 1)
        self.edit_control.setCurrentFont(font)
        self.edit_control.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        layout.addWidget(self.label)
        layout.addWidget(self.edit_control)
        self.setLayout(layout)

        # Add the decription as a tooltip if provided
        if "description" in parameter:
            for widget in (self.label, self.edit_control):
                widget.setToolTip(parameter["description"])

    def value(self):
        return self.edit_control.toPlainText()

    def set_value(self, value):
        self.edit_control.setPlainText(value)

    def _handle_text_changed(self, text, callback):
        message = deepcopy(self.job_template_parameter)
        message["value"] = text
        callback(message)

    def connect_parameter_changed(self, callback):
        self.edit_control.textChanged.connect(
            lambda: self._handle_text_changed(self.value(), callback)
        )


class _JobTemplateIntSpinBoxWidget(_JobTemplateWidget):
    OPENJD_CONTROL_TYPE: ControlType = ControlType.INT_SPIN_BOX
    OPENJD_TYPES: List[str] = ["INT"]
    OPENJD_DEFAULT_VALUE: int = 0
    OPENJD_REQUIRED_PARAMETER_FIELDS: List[str] = []
    OPENJD_DISALLOWED_PARAMETER_FIELDS: List[str] = ["allowedValues"]

    def _build_ui(self, parameter):
        # Create the edit widget
        layout = QHBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        self.label = QLabel(_get_parameter_label(parameter))
        self.edit_control = IntDragSpinBox(self)
        self.edit_control.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
        layout.addWidget(self.label)
        layout.addWidget(self.edit_control)
        self.setLayout(layout)

        # Enable validation if specified
        if "minValue" in parameter:
            min_value = parameter["minValue"]
            if isinstance(min_value, str):
                try:
                    min_value = int(min_value)
                except ValueError:
                    raise RuntimeError(
                        f"Job Template parameter {parameter['name']} with INT type has non-integer 'minValue' of {min_value!r}"
                    )
            self.edit_control.setMinimum(min_value)

        if "maxValue" in parameter:
            max_value = parameter["maxValue"]
            if isinstance(max_value, str):
                try:
                    max_value = int(max_value)
                except ValueError:
                    raise RuntimeError(
                        f"Job Template parameter {parameter['name']} with INT type has non-integer 'maxValue' of {max_value!r}"
                    )
            self.edit_control.setMaximum(max_value)

        # Control customizations
        if "userInterface" in parameter:
            single_step_delta = parameter["userInterface"].get("singleStepDelta", -1)
            drag_multiplier = -1.0  # TODO: Make a good default based on single_step_delta
        else:
            single_step_delta = -1
            drag_multiplier = -1.0

        if single_step_delta >= 0:  # Set to fixed step mode
            self.edit_control.setSingleStep(single_step_delta)
            self.edit_control.setStepType(QSpinBox.DefaultStepType)

        if drag_multiplier >= 0:  # Change drag multiplier from default
            self.edit_control.setDragMultiplier(drag_multiplier)

        # Add the decription as a tooltip if provided
        if "description" in parameter:
            for widget in (self.label, self.edit_control):
                widget.setToolTip(parameter["description"])

    def value(self):
        return self.edit_control.value()

    def set_value(self, value):
        self.edit_control.setValue(value)

    def _handle_value_changed(self, value, callback):
        message = deepcopy(self.job_template_parameter)
        message["value"] = value
        callback(message)

    def connect_parameter_changed(self, callback):
        self.edit_control.valueChanged.connect(
            lambda value: self._handle_value_changed(value, callback)
        )


class _JobTemplateFloatSpinBoxWidget(_JobTemplateWidget):
    OPENJD_CONTROL_TYPE: ControlType = ControlType.FLOAT_SPIN_BOX
    OPENJD_TYPES: List[str] = ["FLOAT"]
    OPENJD_DEFAULT_VALUE: float = 0.0
    OPENJD_REQUIRED_PARAMETER_FIELDS: List[str] = []
    OPENJD_DISALLOWED_PARAMETER_FIELDS: List[str] = ["allowedValues"]

    def _build_ui(self, parameter):
        # Create the edit widget
        layout = QHBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        self.label = QLabel(_get_parameter_label(parameter))
        self.edit_control = FloatDragSpinBox(self)
        self.edit_control.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
        layout.addWidget(self.label)
        layout.addWidget(self.edit_control)
        self.setLayout(layout)

        # Enable validation if specified
        if "minValue" in parameter:
            min_value = parameter["minValue"]
            if isinstance(min_value, str):
                try:
                    min_value = float(min_value)
                except ValueError:
                    raise RuntimeError(
                        f"Job template parameter {parameter['name']} with FLOAT type has non-numeric 'minValue' of {min_value!r}"
                    )
            self.edit_control.setMinimum(min_value)

        if "maxValue" in parameter:
            max_value = parameter["maxValue"]
            if isinstance(max_value, str):
                try:
                    max_value = float(max_value)
                except ValueError:
                    raise RuntimeError(
                        f"Job template parameter {parameter['name']} with FLOAT type has non-numeric 'maxValue' of {max_value!r}"
                    )
            self.edit_control.setMaximum(max_value)

        # Control customizations
        # Control customizations
        if "userInterface" in parameter:
            decimals = parameter["userInterface"].get("decimals", -1)
            single_step_delta = parameter["userInterface"].get("singleStepDelta", -1)
            drag_multiplier = -1.0  # TODO: Make a good default based on single_step_delta
        else:
            decimals = -1
            single_step_delta = -1
            drag_multiplier = -1.0

        if decimals >= 0:  # Set to fixed decimal mode
            self.edit_control.setDecimalMode(DecimalMode.FIXED_DECIMAL)
            self.edit_control.setDecimals(decimals)

        if single_step_delta >= 0:  # Set to fixed step mode
            self.edit_control.setSingleStep(single_step_delta)
            self.edit_control.setStepType(QDoubleSpinBox.DefaultStepType)

        if drag_multiplier >= 0:  # Change drag multiplier from default
            self.edit_control.setDragMultiplier(drag_multiplier)

        # Add the decription as a tooltip if provided
        if "description" in parameter:
            for widget in (self.label, self.edit_control):
                widget.setToolTip(parameter["description"])

    def value(self):
        return self.edit_control.value()

    def set_value(self, value):
        self.edit_control.setValue(value)

    def _handle_value_changed(self, value, callback):
        message = deepcopy(self.job_template_parameter)
        message["value"] = value
        callback(message)

    def connect_parameter_changed(self, callback):
        self.edit_control.valueChanged.connect(
            lambda value: self._handle_value_changed(value, callback)
        )


class _JobTemplateDropdownListWidget(_JobTemplateWidget):
    OPENJD_CONTROL_TYPE: ControlType = ControlType.DROPDOWN_LIST
    OPENJD_TYPES: List[str] = [
        "STRING",
        "INT",
        "FLOAT",
        "PATH",
    ]
    OPENJD_REQUIRED_PARAMETER_FIELDS: List[str] = ["allowedValues"]
    OPENJD_DISALLOWED_PARAMETER_FIELDS: List[str] = ["minValue", "maxValue", "allowedPattern"]

    def _build_ui(self, parameter):
        # Create the edit widget
        layout = QHBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        self.label = QLabel(_get_parameter_label(parameter))
        self.edit_control = QComboBox(self)
        self.edit_control.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
        layout.addWidget(self.label)
        layout.addWidget(self.edit_control)
        self.setLayout(layout)

        # Populate the list of values
        for value in parameter["allowedValues"]:
            self.edit_control.addItem(str(value), value)

        # Default to the first item in the list
        self.OPENJD_DEFAULT_VALUE = parameter["allowedValues"][0]

        # Add the decription as a tooltip if provided
        if "description" in parameter:
            for widget in (self.label, self.edit_control):
                widget.setToolTip(parameter["description"])

    def value(self):
        return self.edit_control.currentData()

    def set_value(self, value):
        index = self.edit_control.findData(value)
        if index >= 0:
            self.edit_control.setCurrentIndex(index)

    def _handle_index_changed(self, value, callback):
        message = deepcopy(self.job_template_parameter)
        message["value"] = value
        callback(message)

    def connect_parameter_changed(self, callback):
        self.edit_control.currentIndexChanged.connect(
            lambda _: self._handle_index_changed(self.value(), callback)
        )


class _JobTemplateBaseFileWidget(_JobTemplateWidget):
    OPENJD_TYPES: List[str] = ["PATH"]
    OPENJD_DEFAULT_VALUE: str = ""
    OPENJD_REQUIRED_PARAMETER_FIELDS: List[str] = []
    OPENJD_DISALLOWED_PARAMETER_FIELDS: List[str] = ["allowedValues"]

    def _build_ui(self, parameter):
        # Get the filters
        filetype_filter = "Any files (*)"
        selected_filter = ""
        if "userInterface" in parameter:
            file_filter_list = parameter["userInterface"].get("fileFilters")
            if file_filter_list:
                filetype_filter = ";;".join(
                    f"{file_filter['label']} ({' '.join(file_filter['patterns'])})"
                    for file_filter in file_filter_list
                )
            file_filter_default = parameter["userInterface"].get("fileFilterDefault")
            if file_filter_default:
                selected_filter = (
                    f"{file_filter_default['label']} ({' '.join(file_filter_default['patterns'])})"
                )

        if not selected_filter:
            selected_filter = filetype_filter.split(";", 1)[0]

        # Create the edit widget
        layout = QHBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        self.label = QLabel(_get_parameter_label(parameter))
        self.edit_control = self.FILE_PICKER_WIDGET(
            initial_filename="",
            file_label=parameter["name"],
            filter=filetype_filter,
            selected_filter=selected_filter,
            parent=self,
        )
        self.edit_control.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
        layout.addWidget(self.label)
        layout.addWidget(self.edit_control)
        self.setLayout(layout)

        # Add the decription as a tooltip if provided
        if "description" in parameter:
            for widget in (self.label, self.edit_control):
                widget.setToolTip(parameter["description"])

    def value(self):
        return self.edit_control.text()

    def set_value(self, value):
        self.edit_control.setText(value)

    def _handle_path_changed(self, value, callback):
        message = deepcopy(self.job_template_parameter)
        message["value"] = value
        callback(message)

    def connect_parameter_changed(self, callback):
        self.edit_control.path_changed.connect(
            lambda path: self._handle_path_changed(path, callback)
        )


class _JobTemplateInputFileWidget(_JobTemplateBaseFileWidget):
    OPENJD_CONTROL_TYPE: ControlType = ControlType.CHOOSE_INPUT_FILE
    FILE_PICKER_WIDGET = InputFilePickerWidget


class _JobTemplateOutputFileWidget(_JobTemplateBaseFileWidget):
    OPENJD_CONTROL_TYPE: ControlType = ControlType.CHOOSE_OUTPUT_FILE
    FILE_PICKER_WIDGET = OutputFilePickerWidget


class _JobTemplateDirectoryWidget(_JobTemplateWidget):
    OPENJD_CONTROL_TYPE: ControlType = ControlType.CHOOSE_DIRECTORY
    OPENJD_TYPES: List[str] = ["PATH"]
    OPENJD_DEFAULT_VALUE: str = ""
    OPENJD_REQUIRED_PARAMETER_FIELDS: List[str] = []
    OPENJD_DISALLOWED_PARAMETER_FIELDS: List[str] = ["allowedValues"]

    def _build_ui(self, parameter):
        # Create the edit widget
        layout = QHBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        self.label = QLabel(_get_parameter_label(parameter))
        self.edit_control = DirectoryPickerWidget(
            initial_directory="", directory_label=parameter["name"], parent=self
        )
        self.edit_control.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
        layout.addWidget(self.label)
        layout.addWidget(self.edit_control)
        self.setLayout(layout)

        # Add the decription as a tooltip if provided
        if "description" in parameter:
            for widget in (self.label, self.edit_control):
                widget.setToolTip(parameter["description"])

    def value(self):
        return self.edit_control.text()

    def set_value(self, value):
        self.edit_control.setText(value)

    def _handle_path_changed(self, value, callback):
        message = deepcopy(self.job_template_parameter)
        message["value"] = value
        callback(message)

    def connect_parameter_changed(self, callback):
        self.edit_control.path_changed.connect(
            lambda path: self._handle_path_changed(path, callback)
        )


# These are the permitted sets of values that can be in a string job parameter 'allowedValues'
# when the user interface control is CHECK_BOX.
ALLOWED_VALUES_FOR_CHECK_BOX = (["TRUE", "FALSE"], ["YES", "NO"], ["ON", "OFF"], ["1", "0"])


class _JobTemplateCheckBoxWidget(_JobTemplateWidget):
    OPENJD_CONTROL_TYPE: ControlType = ControlType.CHECK_BOX
    OPENJD_TYPES: List[str] = ["STRING"]
    OPENJD_DEFAULT_VALUE: str = "false"
    OPENJD_REQUIRED_PARAMETER_FIELDS: List[str] = ["allowedValues"]
    OPENJD_DISALLOWED_PARAMETER_FIELDS: List[str] = [
        "maxValue",
        "minValue",
    ]

    def _build_ui(self, parameter: Dict[str, Any]) -> None:
        # Create the edit widget
        layout = QHBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        self.label = QLabel(_get_parameter_label(parameter))
        self.edit_control = QCheckBox(self)
        layout.addWidget(self.label)
        layout.addWidget(self.edit_control, Qt.AlignLeft)
        self.setLayout(layout)

        # Validate that 'allowedValues' is correct
        allowed_values = parameter.get("allowedValues", [])
        allowed_values_set = set(v.upper() for v in allowed_values)
        if allowed_values_set not in [set(allowed) for allowed in ALLOWED_VALUES_FOR_CHECK_BOX]:
            raise RuntimeError(
                f"Job template parameter {parameter['name']} with CHECK_BOX user interface control requires that 'allowedValues' be "
                + f"one of {ALLOWED_VALUES_FOR_CHECK_BOX} (case and order insensitive)"
            )

        # Determine the true/false correspondence
        true_values = [allowed[0] for allowed in ALLOWED_VALUES_FOR_CHECK_BOX]
        if allowed_values[0].upper() in true_values:
            self.true_value = allowed_values[0]
            self.false_value = allowed_values[1]
        else:
            self.true_value = allowed_values[1]
            self.false_value = allowed_values[0]

        # Add the decription as a tooltip if provided
        if "description" in parameter:
            for widget in (self.label, self.edit_control):
                widget.setToolTip(parameter["description"])

    def value(self) -> str:
        if self.edit_control.isChecked():
            return self.true_value
        else:
            return self.false_value

    def set_value(self, value: str) -> None:
        if value.lower() == "true":
            self.edit_control.setChecked(True)
        else:
            self.edit_control.setChecked(False)

    def _handle_value_changed(self, value, callback):
        message = deepcopy(self.job_template_parameter)
        message["value"] = value
        callback(message)

    def connect_parameter_changed(self, callback):
        self.edit_control.stateChanged.connect(
            lambda _: self._handle_value_changed(self.value(), callback)
        )


class _JobTemplateHiddenWidget(_JobTemplateWidget):
    OPENJD_CONTROL_TYPE: ControlType = ControlType.HIDDEN
    OPENJD_TYPES: List[str] = [
        "PATH",
        "INT",
        "FLOAT",
        "STRING",
    ]
    OPENJD_DEFAULT_VALUE: str = ""  # All hidden fields require a default value to be provided
    OPENJD_REQUIRED_PARAMETER_FIELDS: List[str] = ["default"]
    OPENJD_DISALLOWED_PARAMETER_FIELDS: List[str] = []

    def __init__(self, parent: QWidget, parameter: Dict[str, Any]):
        super().__init__(parent, parameter)

    def _build_ui(self, parameter: Dict[str, Any]) -> None:
        pass

    def value(self) -> Any:
        return self._value

    def set_value(self, value: Any) -> None:
        self._value = value

    def connect_parameter_changed(self, callback):
        pass


class _JobTemplateGroupLayout(QGroupBox):
    def __init__(self, parent: QWidget, group_name: str):
        super().__init__(parent)
        self.setTitle(group_name)
        self.setLayout(QVBoxLayout())
