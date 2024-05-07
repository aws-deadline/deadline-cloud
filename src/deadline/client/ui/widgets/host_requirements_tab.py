# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
UI widgets for the host requirements tab.
"""
from typing import Any, Dict, List, Optional, Union
from pathlib import Path

from qtpy.QtCore import Qt  # type: ignore
from qtpy.QtGui import QFont, QValidator, QIntValidator, QBrush, QIcon, QRegularExpressionValidator  # type: ignore
from qtpy.QtWidgets import (  # type: ignore
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
    QListWidget,
    QListWidgetItem,
    QFrame,
    QLineEdit,
    QListView,
)

from deadline.client.exceptions import NonValidInputError

from logging import getLogger

logger = getLogger(__name__)

MAX_INT_VALUE = (2**31) - 1
MIN_INT_VALUE = -(2**31) + 1
LABEL_FIXED_WIDTH: int = 150
BUTTON_FIXED_WIDTH: int = 150

ATTRIBUTE = "Attribute"
AMOUNT = "Amount"
PLACEHOLDER_TEXT = "-"
INFO_ICON_PATH = str(Path(__file__).parent.parent / "resources" / "info.svg")
CUSTOM_REQUIREMENT_TOOL_TIP = (
    "<html>"
    "<p>"
    "<b>Custom worker requirements</b><br>"
    "With this feature, you can define your own custom worker "
    "capabilities. There are two kinds of worker capabilities you can add."
    "</p>"
    "<ul>"
    "<li><b>Amount</b> - you can define the quantity of something that the worker needs to have for "
    "a step to run. For example, you might define the number of licenses required.</li>"
    "<li><b>Attribute</b> - You can define a property or attribute the worker needs for a step to run. "
    "The attributes are always defined as a set of strings. For example, for a software configuration, "
    "you might define it as 'SoftwareConfig = Option1'.</li>"
    "</ul>"
    "</html>"
)

CUSTOM_CAPABILITY_NAME_REGEX = "^(\\.[a-zA-Z][a-zA-Z0-9]{0,63})+$"

ATTRIBUTE_CAPABILITY_VALUE_REGEX = "^[a-zA-Z_]([a-zA-Z0-9_\\-]{0,99})$"

ATTRIBUTE_CAPABILITY_PREFIX = "attr.worker."
AMOUNT_CAPABILITY_PREFIX = "amount.worker."


class AddIcon(QIcon):
    def __init__(self):
        file_path = str(Path(__file__).parent.parent / "resources" / "add.svg")
        super().__init__(file_path)


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
        custom_requirements = self.custom_requirements_box.get_requirements()

        requirements: Dict[str, Union[List[str], List[int]]] = {}
        if os_requirements:
            requirements.setdefault("attributes", []).extend(os_requirements)  # type: ignore
        if hardware_requirements:
            requirements.setdefault("amounts", []).extend(hardware_requirements)  # type: ignore
        if custom_requirements["amounts"]:
            requirements.setdefault("amounts", []).extend(custom_requirements["amounts"])  # type: ignore
        if custom_requirements["attributes"]:
            requirements.setdefault("attributes", []).extend(custom_requirements["attributes"])  # type: ignore

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
        self.os_row = OSRequirementRowWidget("Operating system", ["linux", "macos", "windows"])
        self.cpu_row = OSRequirementRowWidget("CPU architecture", ["x86_64", "arm64"])

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
        self.gpu_memory_row = HardwareRequirementsRowWidget("GPU memory (GiB)", self)
        self.scratch_space_row = HardwareRequirementsRowWidget("Scratch space", self)

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
        self.attribute_index_numbers = set()
        self.amount_index_numbers = set()
        self._build_ui()

    def _build_ui(self):
        # Add a label that will display tool tip when hovered above
        self.info = QLabel(
            f"<html><img src={INFO_ICON_PATH} width='10' height='10'> More info</html>"
        )
        info_font = self.info.font()
        info_font.setPointSize(10)
        self.info.setFont(info_font)
        self.info.setToolTip(CUSTOM_REQUIREMENT_TOOL_TIP)

        self.info_row = QHBoxLayout()
        self.info_row.setAlignment(Qt.AlignLeft)
        self.info_row.addWidget(self.info)

        # Create a list widget for placing custom capability items
        # - no frame & no background
        # - disable directly selecting list items
        # - no scroll bars
        self.list_widget = QListWidget(self)
        self.list_widget.setSpacing(2)
        self.list_widget.setSelectionMode(QListView.NoSelection)
        self.list_widget.viewport().setAutoFillBackground(False)
        self.list_widget.setFrameStyle(QFrame.NoFrame)
        self.list_widget.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.list_widget.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.list_widget.setSizeAdjustPolicy(QListWidget.AdjustToContents)
        self.resize_list_to_fit()

        # Add a row with Add Amount and Add Attribute buttons
        self.add_amount_button = QPushButton("Add amount")
        self.add_amount_button.setFixedWidth(BUTTON_FIXED_WIDTH)

        self.add_attr_button = QPushButton("Add attribute")
        self.add_attr_button.setFixedWidth(BUTTON_FIXED_WIDTH)

        self.buttons_row = QHBoxLayout()
        self.buttons_row.setAlignment(Qt.AlignLeft)
        self.buttons_row.addWidget(self.add_amount_button)
        self.buttons_row.addWidget(self.add_attr_button)

        self.add_amount_button.clicked.connect(self._add_new_custom_amount)
        self.add_attr_button.clicked.connect(self._add_new_custom_attr)

        # Add everything together
        self.layout.addLayout(self.info_row)
        self.layout.addWidget(self.list_widget)
        self.layout.addLayout(self.buttons_row)

    def _add_new_custom_amount(self):
        self._add_new_item(AMOUNT)

    def _add_new_custom_attr(self):
        self._add_new_item(ATTRIBUTE)

    def _add_new_item(self, type):
        list_item = QListWidgetItem(self.list_widget)

        if type == ATTRIBUTE:
            new_attribute_number = max(self.attribute_index_numbers, default=0) + 1
            item = CustomAttributeWidget(list_item, new_attribute_number, self)
            self.attribute_index_numbers.add(new_attribute_number)
        elif type == AMOUNT:
            new_amount_number = max(self.amount_index_numbers, default=0) + 1
            item = CustomAmountWidget(list_item, new_amount_number, self)
            self.amount_index_numbers.add(new_amount_number)
        else:
            raise NonValidInputError(f"Unexpected item type when adding new item: {type}")

        list_item.setSizeHint(item.sizeHint())

        self.list_widget.addItem(list_item)
        self.list_widget.setItemWidget(list_item, item)
        self.resize_list_to_fit()

    def remove_widget_item(self, custom_capability_widget):
        # remove the ListWidgetItem from list
        if custom_capability_widget.capability_type == ATTRIBUTE:
            self.attribute_index_numbers.remove(custom_capability_widget.item_number)
        elif custom_capability_widget.capability_type == AMOUNT:
            self.amount_index_numbers.remove(custom_capability_widget.item_number)
        else:
            raise NonValidInputError(
                f"Unexpected item type when removing item: {custom_capability_widget.capability_type}"
            )

        item = custom_capability_widget.list_item
        self.list_widget.takeItem(self.list_widget.indexFromItem(item).row())

        self.resize_list_to_fit()

    def resize_list_to_fit(self):
        # Resize the list widget to based on the size of the contents
        if self.list_widget.count() == 0:
            self.list_widget.setFixedHeight(0)
        else:
            current_height = 0
            for i in range(self.list_widget.count()):
                widget = self.list_widget.itemWidget(self.list_widget.item(i))
                if widget is not None:
                    current_height += widget.height() + 2 * self.list_widget.spacing()

            self.list_widget.setFixedHeight(current_height + 2 * self.list_widget.frameWidth())

    def get_requirements(self) -> Dict[str, List]:
        """
        Returns two lists of OpenJD parameter definition dicts
        for both amounts and attributes requirements.
        """
        requirements: Dict[str, Any] = {"amounts": [], "attributes": []}
        for i in range(self.list_widget.count()):
            widget = self.list_widget.itemWidget(self.list_widget.item(i))
            widget_requirement = widget.get_requirement()
            if widget_requirement:
                if isinstance(widget, CustomAmountWidget):
                    requirements["amounts"].append(widget_requirement)
                elif isinstance(widget, CustomAttributeWidget):
                    requirements["attributes"].append(widget_requirement)
                else:
                    logger.warning(
                        f"Widget requirement is not a valid expected type: {type(widget)}"
                    )

        return requirements


class CustomCapabilityWidget(QGroupBox):
    """
    UI element to hold a single custom requirement, either Attribute or Amount.
    """

    def __init__(
        self,
        capability_type: str,
        list_item: QListWidgetItem,
        item_number: int,
        parent: CustomRequirementsWidget,
    ):
        super().__init__(parent)
        self._parent: CustomRequirementsWidget = parent
        self.capability_type = capability_type
        self.list_item = list_item
        self.item_number = item_number

        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.title_label = QLabel(f"{capability_type} {item_number}")
        self.title_label.setStyleSheet("font-weight: bold")

        self.delete_button = QPushButton("Delete")
        self.delete_button.clicked.connect(self._delete)

        self.title_row = QHBoxLayout()
        self.title_row.addWidget(self.title_label)
        self.title_row.addStretch()
        self.title_row.addWidget(self.delete_button)

        self.layout.addLayout(self.title_row)

    def _delete(self):
        self._parent.remove_widget_item(self)
        self.setParent(None)
        self.deleteLater()


class CustomAmountWidget(CustomCapabilityWidget):
    """
    UI element to hold a single custom attribute.
    """

    def __init__(self, list_item: QListWidgetItem, item_number: int, parent=None):
        super().__init__(AMOUNT, list_item, item_number, parent)
        self._build_ui()

    def _build_ui(self):
        # Name / Value
        self.name_label = QLabel("Amount name")
        self.name_label.setFixedWidth(LABEL_FIXED_WIDTH)
        self.name_line_edit = QLineEdit()
        self.name_line_edit.setFixedWidth(LABEL_FIXED_WIDTH)
        self.name_line_edit.setValidator(
            QRegularExpressionValidator(ATTRIBUTE_CAPABILITY_VALUE_REGEX)
        )
        assert (100 - len(AMOUNT_CAPABILITY_PREFIX)) > 0
        self.name_line_edit.setMaxLength(100 - len(AMOUNT_CAPABILITY_PREFIX))

        # Create layout with min/max spinbox
        self.min_label = QLabel("Min")
        self.min_label.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.max_label = QLabel("Max")
        self.max_label.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.min_spin_box = OptionalSpinBox(min=MIN_INT_VALUE, max=MAX_INT_VALUE, parent=self)
        self.min_spin_box.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.max_spin_box = OptionalSpinBox(min=MIN_INT_VALUE, max=MAX_INT_VALUE, parent=self)
        self.max_spin_box.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

        self.min_max_row = QHBoxLayout()
        self.min_max_row.addWidget(self.min_label)
        self.min_max_row.addWidget(self.min_spin_box)
        self.min_max_row.addWidget(self.max_label)
        self.min_max_row.addWidget(self.max_spin_box)

        self.name_column = QVBoxLayout()
        self.name_column.setContentsMargins(2, 0, 0, 0)
        self.name_column.addWidget(self.name_label)
        self.name_column.addWidget(self.name_line_edit)

        self.value_column = QVBoxLayout()
        self.value_column.setContentsMargins(0, 0, 0, 0)
        self.value_column.addStretch()
        self.value_column.addLayout(self.min_max_row)

        self.columns = QHBoxLayout()
        self.columns.setContentsMargins(0, 0, 0, 15)

        self.columns.addLayout(self.name_column)
        self.columns.addLayout(self.value_column)

        # LineEdit  / LineEdit / Optional [X]
        self.layout.addLayout(self.columns)

    def get_requirement(self) -> Dict[str, Any]:
        """
        Returns an OpenJD parameter definition dict with
        a "value" key filled from the widget.

        An amount capability is prefixed with "amount.worker.".
        """
        requirement: Dict[str, Any] = {}
        if self.name_line_edit.text():
            requirement = {"name": AMOUNT_CAPABILITY_PREFIX + self.name_line_edit.text()}

            if self.min_spin_box.has_input() and self.max_spin_box.has_input():
                minimum = self.min_spin_box.value()
                requirement["min"] = minimum

                maximum = self.max_spin_box.value()
                requirement["max"] = maximum

                if minimum > maximum:
                    raise NonValidInputError(
                        "Please make sure that the custom amounts in the custom host requirement options have valid min/max ranges!"
                    )
            elif self.min_spin_box.has_input():
                minimum = self.min_spin_box.value()
                requirement["min"] = minimum
            elif self.max_spin_box.has_input():
                maximum = self.max_spin_box.value()
                requirement["max"] = maximum

        else:
            raise NonValidInputError(
                "Please fill out all custom amount names in the custom host requirement options!"
            )
        return requirement


class CustomAttributeWidget(CustomCapabilityWidget):
    """
    UI element to hold a single custom attribute.
    """

    def __init__(
        self, list_item: QListWidgetItem, item_number: int, parent=CustomRequirementsWidget
    ):
        super().__init__(ATTRIBUTE, list_item, item_number, parent)
        self._build_ui()

    def _build_ui(self):
        # Name / Value / All / Any
        self.name_label = QLabel("Attribute name")
        self.name_label.setFixedWidth(LABEL_FIXED_WIDTH)
        self.value_label = QLabel("Value(s)")
        self.all_of_button = QRadioButton("All")
        self.all_of_button.setChecked(True)
        self.any_of_button = QRadioButton("Any")
        self.name_line_edit = QLineEdit()
        self.name_line_edit.setFixedWidth(LABEL_FIXED_WIDTH)
        assert (100 - len(ATTRIBUTE_CAPABILITY_PREFIX)) > 0
        self.name_line_edit.setMaxLength(100 - len(ATTRIBUTE_CAPABILITY_PREFIX))
        self.name_line_edit.setValidator(
            QRegularExpressionValidator(ATTRIBUTE_CAPABILITY_VALUE_REGEX)
        )
        self.add_value_button = None

        self.top_row = QHBoxLayout()
        self.top_row.addWidget(self.value_label)

        self.top_row.addStretch()
        self.top_row.addWidget(self.all_of_button)
        self.top_row.addWidget(self.any_of_button)

        # Create a list widget for placing custom attribute values
        self.value_list_widget = QListWidget(self)
        self.value_list_widget.setSelectionMode(QListView.NoSelection)
        self.value_list_widget.viewport().setAutoFillBackground(False)
        self.value_list_widget.setFrameStyle(QFrame.NoFrame)
        self.value_list_widget.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.value_list_widget.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.value_list_widget.setSizeAdjustPolicy(QListWidget.AdjustToContents)

        self.name_column = QVBoxLayout()
        self.name_column.setContentsMargins(2, 0, 0, 0)
        self.name_column.setAlignment(Qt.AlignTop)
        self.name_column.addWidget(self.name_label)
        self.name_column.addWidget(self.name_line_edit)

        self.value_column = QVBoxLayout()
        self.value_column.setContentsMargins(0, 0, 0, 0)
        self.value_column.addLayout(self.top_row)
        self.value_column.addWidget(self.value_list_widget)

        # Reduce the spacing between top row and value list
        self.value_column.setSpacing(2)

        self.columns_widget = QWidget(self)
        self.value_column_widget = QWidget(self.columns_widget)
        self.value_column_widget.setLayout(self.value_column)
        self.columns = QVBoxLayout()
        self.columns.setContentsMargins(0, 15, 0, 0)
        self.columns.addLayout(self.name_column)
        self.columns.addWidget(self.value_column_widget)

        # LineEdit  / LineEdit / Optional [X]
        self.columns_widget.setLayout(self.columns)
        self.layout.addWidget(self.columns_widget)
        self._add_value()

    def _add_value(self):
        value_list_item = QListWidgetItem(self.value_list_widget)
        value = CustomAttributeValueWidget(value_list_item, self)
        value_list_item.setSizeHint(value.sizeHint())
        self.value_list_widget.addItem(value_list_item)
        self.value_list_widget.setItemWidget(value_list_item, value)
        self._resize_value_list_to_fit(1)
        self._move_add_button_to_last_item()
        self._set_remove_button_for_first_item()

    def remove_value_item(self, value):
        # remove the ListWidgetItem from value_list_item
        self.value_list_widget.takeItem(self.value_list_widget.indexFromItem(value).row())
        self._resize_value_list_to_fit(-1)
        self._move_add_button_to_last_item()
        self._set_remove_button_for_first_item()

    def _resize_value_list_to_fit(self, item_count_change: int):
        # Resize the list widget as well as parents to based on the size of the contents
        self.value_column_widget.setFixedHeight(
            self.value_column_widget.height()
            + item_count_change
            * self.value_list_widget.itemWidget(self.value_list_widget.item(0)).height()
        )
        self.columns_widget.adjustSize()
        self.adjustSize()
        self.list_item.setSizeHint(self.sizeHint())
        self._parent.resize_list_to_fit()

    def _move_add_button_to_last_item(self):
        # Add value button
        if self.add_value_button is not None:
            self.add_value_button.setParent(None)

        else:
            self.add_value_button = QPushButton("Add")
            self.add_value_button.setStyleSheet("border-width: 0px")
            self.add_value_button.setToolTip(
                "Add a new value to evaluate against for this attribute"
            )
            self.add_value_button.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)

            self.add_value_button.clicked.connect(self._add_value)

        last_item = self.value_list_widget.itemWidget(
            self.value_list_widget.item(self.value_list_widget.count() - 1)
        )
        last_item.layout.insertWidget(last_item.layout.count() - 2, self.add_value_button)

    def _set_remove_button_for_first_item(self):
        if self.value_list_widget.count() == 1:
            first_item = self.value_list_widget.itemWidget(self.value_list_widget.item(0))
            first_item.remove_button.setEnabled(False)
            first_item.remove_button.unsetCursor()

        if self.value_list_widget.count() >= 2:
            first_item = self.value_list_widget.itemWidget(self.value_list_widget.item(0))
            first_item.remove_button.setEnabled(True)

    def get_requirement(self) -> Dict[str, Any]:
        """
        Return an OpenJD parameter definition dict with
        a "value" key filled from the widget, and a list of values.

        An attribute capability is prefixed with "attr.worker".
        """
        requirement: Dict[str, Any] = {}
        requirements_are_valid = True

        if self.name_line_edit.text():
            option = "anyOf" if self.any_of_button.isChecked() else "allOf"
            values = []
            for i in range(self.value_list_widget.count()):
                value = self.value_list_widget.itemWidget(self.value_list_widget.item(i))
                if value.line_edit.text():
                    values.append(value.line_edit.text())
                else:
                    requirements_are_valid = False
            if values:
                requirement = {
                    "name": ATTRIBUTE_CAPABILITY_PREFIX + self.name_line_edit.text(),
                    f"{option}": values,
                }
        else:
            requirements_are_valid = False

        if not requirements_are_valid:
            raise NonValidInputError(
                "Please fill out all custom attribute names and values in the custom host requirements options!"
            )
        return requirement


class CustomAttributeValueWidget(QWidget):
    """
    UI element to hold a single custom attribute value.
    """

    def __init__(self, value_list_item: QListWidgetItem, parent: CustomAttributeWidget):
        super().__init__(parent)
        self._parent: CustomAttributeWidget = parent
        self.value_list_item = value_list_item

        self.line_edit = QLineEdit()
        self.line_edit.setFixedWidth(LABEL_FIXED_WIDTH + 20)
        self.line_edit.setMaxLength(100)
        self.line_edit.setValidator(QRegularExpressionValidator(ATTRIBUTE_CAPABILITY_VALUE_REGEX))

        self.remove_button = QPushButton("Remove")
        self.remove_button.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.remove_button.clicked.connect(self._remove)

        self.layout = QHBoxLayout(self)
        self.layout.setContentsMargins(2, 0, 0, 0)
        self.layout.addWidget(self.line_edit)
        self.layout.addWidget(self.remove_button)
        self.layout.addStretch()
        self.layout.setAlignment(Qt.AlignLeft)

    def _remove(self):
        self._parent.remove_value_item(self.value_list_item)
        self.setParent(None)
        self.deleteLater()


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
        self.label.setFixedWidth(LABEL_FIXED_WIDTH)
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
        self.label.setFixedWidth(LABEL_FIXED_WIDTH)

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

    def __init__(self, min: int = MIN_INT_VALUE, max: int = MAX_INT_VALUE, parent=None) -> None:
        super().__init__(parent)
        self.min = min
        self.max = max
        self.no_input_value = min - 1
        # Set the range to include min-1 as a valid value
        self.setRange(self.no_input_value, MAX_INT_VALUE)
        self.setValue(self.no_input_value)

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
            return validator.validate(input, pos)  # type: ignore[return-value]

    def valueFromText(self, text: str) -> int:
        """
        Override valueFromText function to return no-input-value if input string is empty or placeholder.
        """
        if text == "" or text == PLACEHOLDER_TEXT:
            return self.no_input_value
        else:
            return super().valueFromText(text)

    def textFromValue(self, val: int) -> str:
        """
        Override textFromValue function to return placeholder text if value is no-input-value.
        """
        if val == self.no_input_value:
            return PLACEHOLDER_TEXT
        else:
            return super().textFromValue(val)

    def wheelEvent(self, event):
        """
        Override wheelEvent to disable scrolling from accidentally gaining focus and changing the numbers.
        """
        event.ignore()

    def has_input(self) -> bool:
        """
        Custom function to indicate whether the SpinBox has received input.
        """
        return self.no_input_value != self.value()

    def stepBy(self, steps: int) -> None:
        current_value: int = self.value()
        result_value = self.value() + steps
        if (
            result_value == self.no_input_value
            or result_value > self.maximum()
            or result_value < self.minimum()
        ):
            # If result value is not a valid value, do not go to that value
            return

        if (
            current_value == self.no_input_value and steps == 1
        ):  # We should allow the user to increment from null value to a valid value
            super().setValue(max(self.min, 0))
        else:
            super().stepBy(steps)
