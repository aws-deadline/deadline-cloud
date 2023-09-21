# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
UI widgets for the Host Requirements tab.
"""
from typing import Any, Dict, List, Optional

from PySide2.QtCore import Qt  # type: ignore
from PySide2.QtGui import QFont, QValidator, QIntValidator, QBrush, QPalette
from PySide2.QtWidgets import (  # type: ignore
    QComboBox,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QRadioButton,
    QSizePolicy,
    QSpacerItem,
    QSpinBox,
    QVBoxLayout,
    QWidget,
    QPushButton,
)

LABLE_FIXED_WIDTH: int = 150
BUTTON_FIXED_WIDTH: int = 150
PLACEHOLDER_TEXT = "-"


class HostRequirementsWidget(QWidget):  # pylint: disable=too-few-public-methods
    """
    UI Elements that hold host requirements settings across all job types.

    Args:
        parent: The parent Qt Widget.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)

        self.mode_selection_box = OverrideRequirementsWidget(self)
        layout.addWidget(self.mode_selection_box)

        self.os_requirements_box = OSRequirementsWidget(self)
        self.os_requirements_box.setEnabled(False)
        layout.addWidget(self.os_requirements_box)

        self.hardware_requirements_box = HardwareRequirementsWidget(self)
        self.hardware_requirements_box.setEnabled(False)
        layout.addWidget(self.hardware_requirements_box)

        self.custom_requirements_box = CustomRequirementsWidget(self)
        self.custom_requirements_box.setEnabled(False)
        layout.addWidget(self.custom_requirements_box)

        layout.addItem(QSpacerItem(0, 0, QSizePolicy.Minimum, QSizePolicy.Expanding))

        self.mode_selection_box.use_custom_button.toggled.connect(
            self._on_mode_selection_use_custom_button_toggled
        )

    def _on_mode_selection_use_custom_button_toggled(self, state):
        """
        Enable all settings when use_custom_button is selected. Otherwise disable all settings.
        """
        self.os_requirements_box.setEnabled(state)
        self.hardware_requirements_box.setEnabled(state)
        self.custom_requirements_box.setEnabled(state)

    def _is_custom_requirements_selected(self) -> bool:
        return self.mode_selection_box.use_custom_button.isChecked()

    def get_requirements(self) -> Optional[Dict[str, Any]]:
        """
        Returns a list of OpenJD parameter definition dicts with values filled from the widget.
        If requirement settings are not enabled, then return None.

        host_requirements: dict[str, Any] = {
           "amounts": [ <AmountRequirement>, ... ], # @optional
           "attributes": [ <AttributeRequirement>, ... ] # @optional
        }
        """
        if not self._is_custom_requirements_selected():
            return None

        os_requirements = self.os_requirements_box.get_requirements()
        hardware_requirements = self.hardware_requirements_box.get_requirements()
        # TODO: add custom requirements

        requirements = {}
        if os_requirements:
            # OS requirements are currently all amount type capabilities
            requirements["attributes"] = os_requirements

        if hardware_requirements:
            # hardware requirements are currently all amount
            requirements["amounts"] = hardware_requirements

        return requirements


class OverrideRequirementsWidget(QGroupBox):  # pylint: disable=too-few-public-methods
    """
    UI elements to hold top level selection between using all workers or selected workers that meet requirements.

    Args:
        parent: The parent Qt Widget.
    """

    def __init__(self, parent=None):
        super().__init__("", parent)
        self.layout = QVBoxLayout(self)
        self._build_ui()

    def _build_ui(self):
        # Use Fleet Default button
        self.use_default_button = QRadioButton("Run on all available worker hosts")
        self.use_default_button.setChecked(True)

        # Use customized settings button + tip
        self.use_custom_button = QRadioButton(
            "Run on worker hosts that meet the following requirements"
        )
        self.use_custom_button_tip = QHBoxLayout()
        self.custom_button_tip_text = QLabel("All fields below are optional")
        custom_button_label_font = self.custom_button_tip_text.font()
        custom_button_label_font.setPointSize(10)
        custom_button_label_font.setItalic(True)
        custom_button_label_font.setWeight(QFont.Light)
        self.custom_button_tip_text.setFont(custom_button_label_font)
        self.custom_button_tip_text.setWordWrap(True)
        self.custom_button_tip_text.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
        self.custom_button_tip_text.setAlignment(Qt.AlignTop)

        # account for 2pt spacing left/right of radio button
        self.use_custom_button_tip.addSpacing(self.use_custom_button.iconSize().width() + 4)
        self.use_custom_button_tip.addWidget(self.custom_button_tip_text)

        # Stack the components
        self.layout.addWidget(self.use_default_button)
        self.layout.addSpacing(6)
        self.layout.addWidget(self.use_custom_button)
        self.layout.addLayout(self.use_custom_button_tip)


class OSRequirementsWidget(QGroupBox):
    """
    UI elements to hold OS settings.

    Args:
        parent: The parent Qt Widget.
    """

    def __init__(self, parent=None):
        super().__init__("", parent=parent)
        self.layout = QVBoxLayout(self)
        self._build_ui()

    def _build_ui(self):
        self.os_row = OSRequirementRowWidget("Operating System", ["linux", "macos", "windows"])
        self.cpu_row = OSRequirementRowWidget("CPU Architecture", ["x86_64", "arm64"])

        self.layout.addWidget(self.os_row)
        self.layout.addWidget(self.cpu_row)

    def get_requirements(self) -> List[Dict[str, Any]]:
        """
        Returns a list of OpenJD parameter definition dicts with
        a "value" key filled from the widget.

        Set the following capabilities according to OpenJD spec.
        - attr.worker.os.family
        - attr.worker.cpu.arch
        """

        # TODO: currently only supports "AnyOf" from the UI
        requirements: List[dict] = []
        if self.os_row.combo_box.has_input():
            requirements.append(
                {
                    "name": "attr.worker.os.family",
                    "anyOf": [self.os_row.combo_box.currentText()],
                }
            )
        if self.cpu_row.combo_box.has_input():
            requirements.append(
                {
                    "name": "attr.worker.cpu.arch",
                    "anyOf": [self.cpu_row.combo_box.currentText()],
                }
            )
        return requirements


class HardwareRequirementsWidget(QGroupBox):  # pylint: disable=too-few-public-methods
    """
    UI elements to hold list of hardware requirements.

    Args:
        parent: The parent Qt Widget.
    """

    def __init__(self, parent=None):
        super().__init__("Hardware requirements", parent)
        self.layout = QVBoxLayout(self)
        self._build_ui()

    def _build_ui(self):
        # Build a custom row widget for each selectable option
        self.cpu_row = HardwareRequirementsRowWidget("vCPUs", self)
        self.memory_row = HardwareRequirementsRowWidget("Memory (GiB)", self)
        self.gpu_row = HardwareRequirementsRowWidget("GPUs", self)
        self.gpu_memory_row = HardwareRequirementsRowWidget("GPU Memory (GiB)", self)
        self.scratch_space_row = HardwareRequirementsRowWidget("Scratch Space", self)

        # Add all rows to layout
        self.layout.addWidget(self.cpu_row)
        self.layout.addWidget(self.memory_row)
        self.layout.addWidget(self.gpu_row)
        self.layout.addWidget(self.gpu_memory_row)
        self.layout.addWidget(self.scratch_space_row)

    def get_requirements(self) -> List[Dict[str, Any]]:
        """
        Returns a list of OpenJD parameter definition dicts with
        a "value" key filled from the widget.

        Set the following capabilities according to OpenJD spec.
        - amount.worker.vcpu
        - amount.worker.memory
        - amount.worker.gpu
        - amount.worker.gpu.memory
        - amount.worker.disk.scratch
        """
        requirements: List[Dict[str, Any]] = []
        self.cpu_row.add_requirement(requirements, "amount.worker.vcpu")
        # Memory capability has UI unit in GiB but template unit in MiB, so setting scaling factor to 1024
        self.memory_row.add_requirement(requirements, "amount.worker.memory", 1024)
        self.gpu_row.add_requirement(requirements, "amount.worker.gpu")
        # GPU Memory capability has UI unit in GiB but template unit in MiB, so set scaling factor to 1024
        self.gpu_memory_row.add_requirement(requirements, "amount.worker.gpu.memory", 1024)
        # Disk Scratch capability has unit in GiB
        self.scratch_space_row.add_requirement(requirements, "amount.worker.disk.scratch")
        return requirements


class CustomRequirementsWidget(QGroupBox):
    """
    UI elements to hold custom host requirements.

    Args:
        parent: The parent Qt Widget.
    """

    def __init__(self, parent=None):
        super().__init__("Custom host requirements", parent)
        self.layout = QVBoxLayout(self)
        self._build_ui()

    def _build_ui(self):
        # TODO: make the "More Info" text open a pop-up or tool tip
        # self.info_icon = QIcon(QStyle.SP_MessageBoxInformation)
        # self.info_label = QLabel("More info")

        # Add a row with two buttons
        self.add_amount_button = QPushButton("Add amount")
        self.add_amount_button.setFixedWidth(BUTTON_FIXED_WIDTH)

        self.add_attr_button = QPushButton("Add attribute")
        self.add_attr_button.setFixedWidth(BUTTON_FIXED_WIDTH)

        self.buttons_row = QHBoxLayout(self)
        self.buttons_row.setAlignment(Qt.AlignLeft)
        self.buttons_row.addWidget(self.add_amount_button)
        self.buttons_row.addWidget(self.add_attr_button)

        self.add_amount_button.clicked.connect(self._add_new_custom_amount)
        self.add_attr_button.clicked.connect(self._add_new_custom_attr)

        self.layout.addLayout(self.buttons_row)

    def _add_new_custom_amount(self):
        print("Feature not yet supported!")
        # TODO: insert widget once UI design is finalized
        # self.layout.insertWidget(0, CustomAmountWidget())

    def _add_new_custom_attr(self):
        print("Feature not yet supported!")
        # TODO: insert widget once UI design is finalized
        # self.layout.insertWidget(0, CustomAttributeWidget())

    def get_requirements(self):
        """
        Returns a list of OpenJD parameter definition dicts
        """
        print("Feature not yet supported!")


class CustomAmountWidget(QWidget):
    """
    UI element to hold a single custom attribute.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.layout = QHBoxLayout(self)
        # remove default spaces around BoxLayout
        self.layout.setContentsMargins(0, 0, 0, 0)
        # TODO: build widget once UI design is finalized


class CustomAttributeWidget(QWidget):
    """
    UI element to hold a single custom attribute.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.layout = QHBoxLayout(self)
        # remove default spaces around BoxLayout
        self.layout.setContentsMargins(0, 0, 0, 0)
        # TODO: build widget once UI design is finalized


class OSRequirementRowWidget(QWidget):
    """
    UI element to hold a single row of OS requirement components.

    Args:
        label: the name of the requirement
        items: selectable options to display inside a comboBox
        parent: The parent Qt Widget
    """

    def __init__(self, label: str, items: List[str], parent=None):
        super().__init__(parent)
        self.layout = QHBoxLayout(self)
        # remove default spaces around BoxLayout
        self.layout.setContentsMargins(0, 0, 0, 0)
        self._build_ui(label, items)

    def _build_ui(self, label: str, items: List[str]):
        self.label = QLabel(label)
        self.label.setFixedWidth(LABLE_FIXED_WIDTH)
        self.combo_box = OptionalComboBox(items, parent=self)
        self.layout.addWidget(self.label)
        self.layout.addWidget(self.combo_box)


class HardwareRequirementsRowWidget(QWidget):
    """
    UI element to hold a single row of hardware requirement components.

    Args:
        label: the name of the requirement
        parent: The parent Qt Widget
    """

    def __init__(self, label: str, parent=None):
        super().__init__(parent)
        self.layout = QHBoxLayout(self)
        # remove default spaces around BoxLayout
        self.layout.setContentsMargins(0, 0, 0, 0)
        self._build_ui(label)

    def _build_ui(self, label: str):
        self.label = QLabel(label)
        self.label.setFixedWidth(LABLE_FIXED_WIDTH)

        # Create "Min" label, and set label to fixed width
        self.min_label = QLabel("Min")
        self.min_label.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)

        # Create "Max" label, and set label to fixed width
        self.max_label = QLabel("Max")
        self.max_label.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)

        # Create spin box for "Min"
        self.min_spin_box = OptionalSpinBox(min=0, max=100000, parent=self)

        # Create spin box for "Max"
        self.max_spin_box = OptionalSpinBox(min=0, max=100000, parent=self)

        self.layout.addWidget(self.label)
        self.layout.addWidget(self.min_label)
        self.layout.addWidget(self.min_spin_box)
        self.layout.addWidget(self.max_label)
        self.layout.addWidget(self.max_spin_box)

    def add_requirement(self, requirements: List, name: str, scaling_factor: int = 1):
        """
        Create a dict based on whether inputs have been received for the components in the row widget,
        then append the dict to an existing list of requirements.

        Args:
            requirements: the list of requirements to append to
            name: the name of the capability
            scaling_factor: for some of the amount capabilities, the unit displayed on the UI is different
                then the unit used within template, so use this factor to scale the input values.
        """
        if self.min_spin_box.has_input() or self.max_spin_box.has_input():
            requirement = {"name": name}
            if self.min_spin_box.has_input():
                requirement["min"] = self.min_spin_box.value() * scaling_factor
            if self.max_spin_box.has_input():
                requirement["max"] = self.max_spin_box.value() * scaling_factor
            requirements.append(requirement)


class OptionalComboBox(QComboBox):
    """
    A custom QComboBox that always have a grayed out placeholder option.
    """

    def __init__(self, items: List[str], parent=None):
        super().__init__(parent)
        self.addItem(PLACEHOLDER_TEXT)
        self.setItemData(0, QBrush(Qt.gray), Qt.TextColorRole)
        self.addItems(items)

    def has_input(self) -> bool:
        return PLACEHOLDER_TEXT != self.currentText()


class OptionalSpinBox(QSpinBox):
    """
    A custom QSpinBox that set min - 1 value as "-" to represent value not set.
    """

    NAN_VALUE = -(2**31)
    MAX_INT_VALUE = (2**31) - 1
    MIN_INT_VALUE = -(2**31) + 1
    palette = QPalette()

    def __init__(self, min: int = MIN_INT_VALUE, max: int = MAX_INT_VALUE, parent=None) -> None:
        super().__init__(parent)
        self.min = min
        self.max = max
        # Set the range to include NaN as a valid value
        self.setRange(self.NAN_VALUE, self.MAX_INT_VALUE)
        self.setValue(self.NAN_VALUE)

    def validate(self, input: str, pos: int) -> QValidator.State:
        """
        Override validate function to treat empty string "" as acceptable.
        """
        if input == "" or input == PLACEHOLDER_TEXT:
            return QValidator.Acceptable
        else:
            # Override the range that can be accepted by the validator by actual min/max values.
            validator = QIntValidator()
            validator.setBottom(self.min)
            validator.setTop(self.max)
            return validator.validate(input, pos)

    def valueFromText(self, text: str) -> int:
        """
        Override valueFromText function to return NaN if input string is empty or placeholder.
        """
        if text == "" or text == PLACEHOLDER_TEXT:
            return self.NAN_VALUE
        else:
            return super().valueFromText(text)

    def textFromValue(self, val: int) -> str:
        """
        Override textFromValue function to return placeholder text if value is NaN.
        """
        if val == self.NAN_VALUE:
            return PLACEHOLDER_TEXT
        else:
            return super().textFromValue(val)

    def has_input(self) -> bool:
        """
        Custom function to indicate whether the SpinBox has received input.
        """
        return self.NAN_VALUE != self.value()
