# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
UI widgets for the Scene Settings tab.
"""
import os
from typing import Any, Dict, List

from PySide2.QtCore import QRegularExpression, Qt  # type: ignore
from PySide2.QtGui import QValidator
from PySide2.QtWidgets import (  # type: ignore
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
from ..widgets.path_widgets import (
    DirectoryPickerWidget,
    InputFilePickerWidget,
    OutputFilePickerWidget,
)
from ..widgets.spinbox_widgets import DecimalMode, FloatDragSpinBox, IntDragSpinBox


class JobTemplateParametersWidget(QWidget):
    """
    Widget that takes the set of parameters from a job template, and generated
    a UI form to edit them with.

    OpenJobIO has optional UI metadata for each parameter specified under "userInterface".

    Args:
        initial_job_parameters (Dict[str, Any]): OpenJobIO parameters block.
        parent: The parent Qt Widget.
    """

    def __init__(self, job_parameters: List[Dict[str, Any]], parent=None):
        super().__init__(parent=parent)

        self._build_ui(job_parameters)

    def _build_ui(self, job_parameters: List[Dict[str, Any]]):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        self.controls: List[Any] = []
        need_spacer = True

        control_map = {
            ControlType.LINE_EDIT: _JobTemplateLineEditWidget,
            ControlType.MULTILINE_EDIT: _JobTemplateMultiLineEditWidget,
            ControlType.INT_SPIN_BOX: _JobTemplateIntSpinBoxWidget,
            ControlType.FLOAT_SPIN_BOX: _JobTemplateFloatSpinBoxWidget,
            ControlType.DROPDOWN_LIST: _JobTemplateDropdownListWidget,
            ControlType.CHOOSE_INPUT_FILE: _JobTemplateInputFileWidget,
            ControlType.CHOOSE_OUTPUT_FILE: _JobTemplateOutputFileWidget,
            ControlType.CHOOSE_DIRECTORY: _JobTemplateDirectoryWidget,
            ControlType.CHECK_BOX: _JobTemplateCheckBoxWidget,
            ControlType.HIDDEN: _JobTemplateHiddenWidget,
        }

        for parameter in job_parameters:
            # Skip application-specific parameters like "deadline:priority"
            if ":" in parameter["name"]:
                continue

            try:
                control_type_name = ""
                if "userInterface" in parameter:
                    control_type_name = parameter["userInterface"].get("control", "")

                # If not explicitly provided, determine the default control type name based on the OJIO specification
                if not control_type_name:
                    if parameter.get("allowedValues"):
                        control_type_name = "DROPDOWN_LIST"
                    else:
                        if parameter["type"] == "STRING":
                            control_type_name = "LINE_EDIT"
                        elif parameter["type"] == "PATH":
                            if parameter.get("objectType") == "FILE":
                                if parameter.get("dataFlow") == "OUT":
                                    control_type_name = "CHOOSE_OUTPUT_FILE"
                                else:
                                    control_type_name = "CHOOSE_INPUT_FILE"
                            else:
                                control_type_name = "CHOOSE_DIRECTORY"
                        elif parameter["type"] in ["INT", "FLOAT"]:
                            control_type_name = "SPIN_BOX"

                if control_type_name == "SPIN_BOX":
                    control_type_name = f"{parameter['type']}_{control_type_name}"

                control_type = ControlType[control_type_name]
            except KeyError:
                raise RuntimeError(
                    f"Job Template parameter {parameter['name']} specifies unsupported control type {control_type.name}."
                )

            if "userInterface" in parameter:
                group_label = parameter["userInterface"].get("groupLabel", "")
            else:
                group_label = ""

            control_widget = control_map[control_type]
            self.controls.append(control_widget(self, parameter))

            if control_type != ControlType.HIDDEN:
                if group_label:
                    group_layout = self.findChild(_JobTemplateGroupLayout, group_label)
                    if not group_layout:
                        group_layout = _JobTemplateGroupLayout(self, group_label)
                        group_layout.setObjectName(group_label)
                        layout.addWidget(group_layout)
                    group_layout.layout().addWidget(self.controls[-1])
                else:
                    layout.addWidget(self.controls[-1])

                if control_widget.IS_VERTICAL_EXPANDING:
                    # Turn off the spacer at the end, as there's already a stretchy control
                    need_spacer = False

        if need_spacer:
            layout.addItem(QSpacerItem(0, 0, QSizePolicy.Minimum, QSizePolicy.Expanding))

    def get_parameter_values(self):
        return [{"name": control.name(), "value": control.value()} for control in self.controls]


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
        if parameter["type"] not in self.OJIO_TYPES:
            if len(self.OJIO_TYPES) == 1:
                raise RuntimeError(
                    f"Job Template parameter {parameter['name']} with control "
                    + f"{self.OJIO_CONTROL_TYPE} has type {parameter['type']} but "
                    + f"must have type {self.OJIO_TYPES[0]}."
                )
            else:
                raise RuntimeError(
                    f"Job Template parameter {parameter['name']} with control "
                    + f"{self.OJIO_CONTROL_TYPE} has type {parameter['type']} but "
                    + f"must have one of type: {[v[0] for v in self.OJIO_TYPES]}"
                )

        for field in self.OJIO_REQUIRED_PARAMETER_FIELDS:
            if field not in parameter:
                raise RuntimeError(
                    f"Job Template parameter {parameter['name']} with control "
                    + f"{self.OJIO_CONTROL_TYPE} is missing required field '{field}'."
                )
        for field in self.OJIO_DISALLOWED_PARAMETER_FIELDS:
            if field in parameter:
                raise RuntimeError(
                    f"Job Template parameter {parameter['name']} with control "
                    + f"{self.OJIO_CONTROL_TYPE} must not provide field '{field}'."
                )

        self._build_ui(parameter)

        # Set the initial value to the first of the value, default or a type default
        value = parameter.get("value", parameter.get("default", self.OJIO_DEFAULT_VALUE))
        self.set_value(value)


class _JobTemplateLineEditWidget(_JobTemplateWidget):
    OJIO_CONTROL_TYPE: ControlType = ControlType.LINE_EDIT
    OJIO_TYPES: List[str] = ["STRING"]
    OJIO_DEFAULT_VALUE: str = ""
    OJIO_REQUIRED_PARAMETER_FIELDS: List[str] = []
    OJIO_DISALLOWED_PARAMETER_FIELDS: List[str] = ["allowedValues"]

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

    def name(self):
        return self.job_template_parameter["name"]

    def value(self):
        return self.edit_control.text()

    def set_value(self, value):
        self.edit_control.setText(value)


class _JobTemplateMultiLineEditWidget(_JobTemplateWidget):
    OJIO_CONTROL_TYPE: ControlType = ControlType.MULTILINE_EDIT
    OJIO_TYPES: List[str] = ["STRING"]
    OJIO_DEFAULT_VALUE: str = ""
    OJIO_REQUIRED_PARAMETER_FIELDS: List[str] = []
    OJIO_DISALLOWED_PARAMETER_FIELDS: List[str] = ["allowedValues"]
    IS_VERTICAL_EXPANDING: bool = True

    def _build_ui(self, parameter):
        # Create the edit widget
        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        self.label = QLabel(_get_parameter_label(parameter))
        self.edit_control = QTextEdit(self)
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

    def name(self):
        return self.job_template_parameter["name"]

    def value(self):
        return self.edit_control.toPlainText()

    def set_value(self, value):
        self.edit_control.setPlainText(value)


class _JobTemplateIntSpinBoxWidget(_JobTemplateWidget):
    OJIO_CONTROL_TYPE: ControlType = ControlType.INT_SPIN_BOX
    OJIO_TYPES: List[str] = ["INT"]
    OJIO_DEFAULT_VALUE: int = 0
    OJIO_REQUIRED_PARAMETER_FIELDS: List[str] = []
    OJIO_DISALLOWED_PARAMETER_FIELDS: List[str] = ["allowedValues"]

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

    def name(self):
        return self.job_template_parameter["name"]

    def value(self):
        return self.edit_control.value()

    def set_value(self, value):
        self.edit_control.setValue(value)


class _JobTemplateFloatSpinBoxWidget(_JobTemplateWidget):
    OJIO_CONTROL_TYPE: ControlType = ControlType.FLOAT_SPIN_BOX
    OJIO_TYPES: List[str] = ["FLOAT"]
    OJIO_DEFAULT_VALUE: float = 0.0
    OJIO_REQUIRED_PARAMETER_FIELDS: List[str] = []
    OJIO_DISALLOWED_PARAMETER_FIELDS: List[str] = ["allowedValues"]

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
                        f"Job Template parameter {parameter['name']} with FLOAT type has non-numeric 'minValue' of {min_value!r}"
                    )
            self.edit_control.setMinimum(min_value)

        if "maxValue" in parameter:
            max_value = parameter["maxValue"]
            if isinstance(max_value, str):
                try:
                    max_value = float(max_value)
                except ValueError:
                    raise RuntimeError(
                        f"Job Template parameter {parameter['name']} with FLOAT type has non-numeric 'maxValue' of {max_value!r}"
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

    def name(self):
        return self.job_template_parameter["name"]

    def value(self):
        return self.edit_control.value()

    def set_value(self, value):
        self.edit_control.setValue(value)


class _JobTemplateDropdownListWidget(_JobTemplateWidget):
    OJIO_CONTROL_TYPE: ControlType = ControlType.DROPDOWN_LIST
    OJIO_TYPES: List[str] = [
        "STRING",
        "INT",
        "FLOAT",
        "PATH",
    ]
    OJIO_REQUIRED_PARAMETER_FIELDS: List[str] = ["allowedValues"]
    OJIO_DISALLOWED_PARAMETER_FIELDS: List[str] = ["minValue", "maxValue", "allowedPattern"]

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
        self.OJIO_DEFAULT_VALUE = parameter["allowedValues"][0]

        # Add the decription as a tooltip if provided
        if "description" in parameter:
            for widget in (self.label, self.edit_control):
                widget.setToolTip(parameter["description"])

    def name(self):
        return self.job_template_parameter["name"]

    def value(self):
        return self.edit_control.currentData()

    def set_value(self, value):
        index = self.edit_control.findData(value)
        if index >= 0:
            self.edit_control.setCurrentIndex(index)


class _JobTemplateBaseFileWidget(_JobTemplateWidget):
    OJIO_TYPES: List[str] = ["PATH"]
    OJIO_DEFAULT_VALUE: str = ""
    OJIO_REQUIRED_PARAMETER_FIELDS: List[str] = []
    OJIO_DISALLOWED_PARAMETER_FIELDS: List[str] = ["allowedValues"]

    def _build_ui(self, parameter):
        # Get the filters
        filetype_filter = "Any Files (*)"
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

    def name(self):
        return self.job_template_parameter["name"]

    def value(self):
        return self.edit_control.text()

    def set_value(self, value):
        self.edit_control.setText(value)


class _JobTemplateInputFileWidget(_JobTemplateBaseFileWidget):
    OJIO_CONTROL_TYPE: ControlType = ControlType.CHOOSE_INPUT_FILE
    FILE_PICKER_WIDGET = InputFilePickerWidget


class _JobTemplateOutputFileWidget(_JobTemplateBaseFileWidget):
    OJIO_CONTROL_TYPE: ControlType = ControlType.CHOOSE_OUTPUT_FILE
    FILE_PICKER_WIDGET = OutputFilePickerWidget


class _JobTemplateDirectoryWidget(_JobTemplateWidget):
    OJIO_CONTROL_TYPE: ControlType = ControlType.CHOOSE_DIRECTORY
    OJIO_TYPES: List[str] = ["PATH"]
    OJIO_DEFAULT_VALUE: str = ""
    OJIO_REQUIRED_PARAMETER_FIELDS: List[str] = []
    OJIO_DISALLOWED_PARAMETER_FIELDS: List[str] = ["allowedValues"]

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

    def name(self):
        return self.job_template_parameter["name"]

    def value(self):
        return self.edit_control.text()

    def set_value(self, value):
        self.edit_control.setText(value)


# These are the permitted sets of values that can be in a string job parameter 'allowedValues'
# when the user interface control is CHECK_BOX.
ALLOWED_VALUES_FOR_CHECK_BOX = (["TRUE", "FALSE"], ["YES", "NO"], ["ON", "OFF"], ["1", "0"])


class _JobTemplateCheckBoxWidget(_JobTemplateWidget):
    OJIO_CONTROL_TYPE: ControlType = ControlType.CHECK_BOX
    OJIO_TYPES: List[str] = ["STRING"]
    OJIO_DEFAULT_VALUE: str = "false"
    OJIO_REQUIRED_PARAMETER_FIELDS: List[str] = ["allowedValues"]
    OJIO_DISALLOWED_PARAMETER_FIELDS: List[str] = [
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
                f"Job Template parameter {parameter['name']} with CHECK_BOX user interface control requires that 'allowedValues' be "
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

    def name(self) -> str:
        return self.job_template_parameter["name"]

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


class _JobTemplateHiddenWidget(_JobTemplateWidget):
    OJIO_CONTROL_TYPE: ControlType = ControlType.HIDDEN
    OJIO_TYPES: List[str] = [
        "PATH",
        "INT",
        "FLOAT",
        "STRING",
    ]
    OJIO_DEFAULT_VALUE: str = ""  # All hidden fields require a default value to be provided
    OJIO_REQUIRED_PARAMETER_FIELDS: List[str] = ["default"]
    OJIO_DISALLOWED_PARAMETER_FIELDS: List[str] = []

    def __init__(self, parent: QWidget, parameter: Dict[str, Any]):
        super().__init__(parent, parameter)

    def _build_ui(self, parameter: Dict[str, Any]) -> None:
        pass

    def name(self) -> str:
        return self.job_template_parameter["name"]

    def value(self) -> Any:
        return self._value

    def set_value(self, value: Any) -> None:
        self._value = value


class _JobTemplateGroupLayout(QGroupBox):
    def __init__(self, parent: QWidget, group_name: str):
        super().__init__(parent)
        self.setTitle(group_name)
        self.setLayout(QVBoxLayout())
