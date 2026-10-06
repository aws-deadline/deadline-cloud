# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
UI widgets for the Scene Settings tab.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
from copy import deepcopy

from qtpy.QtCore import QEvent, QPersistentModelIndex, QPoint, QRegularExpression, Qt, Signal  # type: ignore
from qtpy.QtGui import QIcon, QPainter, QValidator
from qtpy.QtWidgets import (  # type: ignore
    QAbstractItemDelegate,
    QAbstractItemView,
    QApplication,
    QCheckBox,
    QComboBox,
    QDoubleSpinBox,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QPushButton,
    QSizePolicy,
    QSpacerItem,
    QSpinBox,
    QStyle,
    QStyledItemDelegate,
    QStyleOptionSizeGrip,
    QStyleOptionViewItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from ...job_bundle.job_template import ControlType
from ...job_bundle._range_expr import (
    MAX_RANGE_EXPR_LENGTH as _MAX_RANGE_EXPR_LENGTH,
    parse_int_range_expr as _parse_int_range_expr,
)
from ...job_bundle.parameters import (
    JobParameter,
    _LIST_TYPES,
    _MAX_INT64,
    _MAX_LIST_ITEMS,
    _MAX_STRING_LIST_ITEMS,
    _MIN_INT64,
    _to_bool,
    get_ui_control_for_parameter_definition,
)
from ...job_bundle.parameters import (
    validate_job_parameter_value as _validate_job_parameter_value,
)
from .._utils import tr as _tr
from .path_widgets import (
    DirectoryPickerWidget,
    InputFilePickerWidget,
    OutputFilePickerWidget,
    _normalize_path_text,
)
from .spinbox_widgets import DecimalMode, FloatDragSpinBox, IntDragSpinBox

_logger = logging.getLogger(__name__)


class OpenJDParametersWidget(QWidget):
    """
    Widget that takes the set of Open Job Description parameters, for example from a job template or a queue,
    and generates a UI form to edit them.

    Open Job Description has optional UI metadata for each parameter specified under "userInterface".

    Signals:
        parameter_changed: This is sent whenever a parameter value in the widget changes. The message
            is a copy of the parameter definition with the "value" key containing the new value.
        valid_parameters: Sent after each change with whether every parameter's control currently
            holds a valid value (see invalid_parameter_names).

    Args:
        parameter_definitions (List[Dict[str, Any]]): A list of Open Job Description parameter definitions.
        async_loading_state (str): A message to show its async loading state. Cannot provide both this
            message and the parameter_definitions.
        parent: The parent Qt Widget.
    """

    parameter_changed = Signal(dict)
    valid_parameters = Signal(bool)

    def __init__(
        self,
        *,
        parameter_definitions: List[JobParameter] = [],
        async_loading_state: str = "",
        parent: Optional[QWidget] = None,
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
                if child is None:
                    continue
                widget = child.widget()
                if widget:
                    widget.deleteLater()
                elif child.layout():
                    child.layout().deleteLater()  # type: ignore[union-attr]
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
            ControlType.LINE_EDIT_LIST.name: _JobTemplateLineEditListWidget,
            ControlType.SPIN_BOX_LIST.name: _JobTemplateSpinBoxListWidget,
            ControlType.CHECK_BOX_LIST.name: _JobTemplateCheckBoxListWidget,
            ControlType.CHOOSE_INPUT_FILE_LIST.name: _JobTemplateInputFileListWidget,
            ControlType.CHOOSE_OUTPUT_FILE_LIST.name: _JobTemplateOutputFileListWidget,
            ControlType.CHOOSE_DIRECTORY_LIST.name: _JobTemplateDirectoryListWidget,
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
            control.connect_parameter_changed(self._on_control_changed)

            if control_type_name != ControlType.HIDDEN.name:
                if group_label:
                    group_layout = self.findChild(_JobTemplateGroupLayout, group_label)
                    if not group_layout:
                        group_layout = _JobTemplateGroupLayout(self, group_label)
                        group_layout.setObjectName(group_label)
                        layout.addWidget(group_layout)
                    group_layout.layout().addWidget(control)  # type: ignore[union-attr]
                else:
                    layout.addWidget(control)

                if control_widget.IS_VERTICAL_EXPANDING:
                    # Turn off the spacer at the end, as there's already a stretchy control
                    need_spacer = False

        if need_spacer:
            layout.addItem(QSpacerItem(0, 0, QSizePolicy.Minimum, QSizePolicy.Expanding))

        self.valid_parameters.emit(not self.invalid_parameter_names())

    def _on_control_changed(self, message: dict[str, Any]) -> None:
        self.parameter_changed.emit(message)
        self.valid_parameters.emit(not self.invalid_parameter_names())

    def invalid_parameter_names(self) -> list[str]:
        """
        Returns the names of parameters whose control holds a value that fails the
        parameter's constraints, e.g. an incomplete RANGE_EXPR or a STRING shorter
        than its minLength.
        """
        return [name for name, control in self.controls.items() if not control.is_valid()]

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


_RANGE_EXPR_CHARACTERS = frozenset("0123456789-:, \t")


class _JobTemplateRangeExprValidator(QValidator):
    """Validates a RANGE_EXPR line edit against the <IntRangeExpr> grammar.

    Characters that can never appear in a range expression are rejected as they
    are typed, as is text beyond the length the service accepts. Text that is not
    (yet) a complete range expression is Intermediate so the user can keep editing;
    only a valid expression is Acceptable.
    """

    def __init__(self, min_length: Optional[int], max_length: Optional[int]):
        super().__init__()
        self.min_length = min_length
        self.max_length = (
            min(max_length, _MAX_RANGE_EXPR_LENGTH)
            if max_length is not None
            else _MAX_RANGE_EXPR_LENGTH
        )

    def validate(self, s, pos):
        if len(s) > self.max_length or not set(s) <= _RANGE_EXPR_CHARACTERS:
            return (QValidator.Invalid, s, pos)
        if self.min_length is not None and len(s) < self.min_length:
            return (QValidator.Intermediate, s, pos)
        try:
            _parse_int_range_expr(s)
        except ValueError:
            return (QValidator.Intermediate, s, pos)
        return (QValidator.Acceptable, s, pos)


class _JobTemplateWidget(QWidget):
    IS_VERTICAL_EXPANDING: bool = False

    # Reports a change as a copy of the parameter definition with "value" set. Controls
    # report changes through this Qt signal, connected to their own bound method, rather
    # than through Python closures or containers that capture the parent's callbacks:
    # those form reference cycles or leaks, so the widget could only be freed by the
    # garbage collector, on whichever thread it happens to run.
    _value_changed = Signal(dict)

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

        # Connected after the initial value is set, so setting it reports no change.
        change_signal = self._change_signal()
        if change_signal is not None:
            change_signal.connect(self._emit_value_changed)

    def _change_signal(self) -> Any:
        """The child widget's signal that reports a user edit, or None."""
        return None

    def _emit_value_changed(self, *args) -> None:
        message = deepcopy(self.job_template_parameter)
        message["value"] = self.value()
        self._value_changed.emit(message)

    def connect_parameter_changed(self, callback) -> None:
        self._value_changed.connect(callback)

    def name(self):
        return self.job_template_parameter["name"]

    def type(self):
        return self.job_template_parameter["type"]

    def is_valid(self) -> bool:
        """Whether the control's current value satisfies the parameter's constraints."""
        return True


class _JobTemplateLineEditWidget(_JobTemplateWidget):
    OPENJD_CONTROL_TYPE: ControlType = ControlType.LINE_EDIT
    OPENJD_TYPES: List[str] = ["STRING", "RANGE_EXPR"]
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
        if parameter["type"] == "RANGE_EXPR":
            self.edit_control.setValidator(
                _JobTemplateRangeExprValidator(
                    parameter.get("minLength", None),
                    parameter.get("maxLength", None),
                )
            )
            self.edit_control.setPlaceholderText("e.g. 1-100, 1-100:10, 1,3,5")
            self.edit_control.textChanged.connect(self._update_range_expr_feedback)
        elif "minLength" in parameter or "maxLength" in parameter or "allowedPattern" in parameter:
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

    def _update_range_expr_feedback(self, text: str) -> None:
        """Highlights the edit and explains the problem when the text is not a valid range expression."""
        try:
            _validate_job_parameter_value(self.job_template_parameter, text)
        except (ValueError, TypeError) as e:
            self.edit_control.setStyleSheet("QLineEdit { border: 1px solid red; }")
            self.edit_control.setToolTip(str(e))
        else:
            self.edit_control.setStyleSheet("")
            self.edit_control.setToolTip(self.job_template_parameter.get("description", ""))

    def is_valid(self) -> bool:
        # True when there is no validator, or the validator reports Acceptable.
        return self.edit_control.hasAcceptableInput()

    def value(self):
        return self.edit_control.text()

    def set_value(self, value):
        self.edit_control.setText(value.as_posix() if isinstance(value, Path) else str(value))

    def _change_signal(self):
        return self.edit_control.textChanged


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
        self.edit_control.setPlainText(value.as_posix() if isinstance(value, Path) else str(value))

    def _change_signal(self):
        return self.edit_control.textChanged


class _FixedRowHeightDelegate(QStyledItemDelegate):
    """Gives every row of a list the same height, whatever decoration it carries."""

    def __init__(self, parent: QWidget, row_height: int):
        super().__init__(parent)
        self._row_height = row_height

    def sizeHint(self, option, index):
        size = super().sizeHint(option, index)
        size.setHeight(self._row_height)
        return size


class _ListResizeGrip(QWidget):
    """A size grip that drags the height of a list control's list, in whole rows.

    QSizeGrip only resizes top-level windows, so this draws the platform size grip and
    handles the drag itself.
    """

    def __init__(self, owner: "_JobTemplateListWidgetBase"):
        super().__init__(owner)
        self._owner = owner
        self._drag_start: Optional[tuple[int, int]] = None
        extent = self.style().pixelMetric(QStyle.PM_SizeGripSize, None, self)
        self.setFixedSize(extent, extent)
        # Only the height changes; the width follows the dialog.
        self.setCursor(Qt.SizeVerCursor)
        self.setToolTip(_tr("Drag to show more or fewer rows"))

    def paintEvent(self, event) -> None:
        option = QStyleOptionSizeGrip()
        option.initFrom(self)
        option.corner = Qt.BottomRightCorner
        painter = QPainter(self)
        self.style().drawControl(QStyle.CE_SizeGrip, option, painter, self)

    @staticmethod
    def _global_y(event) -> int:
        # globalPosition is Qt 6; globalY is its Qt 5 equivalent.
        if hasattr(event, "globalPosition"):
            return int(event.globalPosition().y())
        return event.globalY()

    def mousePressEvent(self, event) -> None:
        if event.button() == Qt.LeftButton:
            self._drag_start = (self._global_y(event), self._owner.edit_control.height())
            event.accept()

    def mouseMoveEvent(self, event) -> None:
        if self._drag_start is not None:
            start_y, start_height = self._drag_start
            self._owner.resize_to_height(start_height + self._global_y(event) - start_y)
            event.accept()

    def mouseReleaseEvent(self, event) -> None:
        self._drag_start = None


class _JobTemplateListWidgetBase(_JobTemplateWidget):
    """The shared layout and behavior of the controls for list job parameters.

    Laid out like the known asset paths list in the settings dialog: a button row above
    a list. Items can be reordered by dragging since the order of a list parameter's
    values is significant. Subclasses provide how an item holds and edits its value.
    """

    OPENJD_DEFAULT_VALUE: List[Any] = []
    OPENJD_REQUIRED_PARAMETER_FIELDS: List[str] = []
    OPENJD_DISALLOWED_PARAMETER_FIELDS: List[str] = ["allowedValues"]
    # A fixed height that the user resizes with the grip, so other parameters stay in view.
    IS_VERTICAL_EXPANDING: bool = False
    MIN_VISIBLE_ROWS: int = 3
    MAX_VISIBLE_ROWS: int = _MAX_STRING_LIST_ITEMS
    # Describes the item type in the warning logged for a value of the wrong type.
    ITEM_KIND: str = ""

    _ITEM_FLAGS = Qt.ItemIsSelectable | Qt.ItemIsEnabled | Qt.ItemIsEditable | Qt.ItemIsDragEnabled

    def _build_ui(self, parameter):
        self._suppress_changes = False
        max_items_cap = _MAX_LIST_ITEMS[parameter["type"]]
        self.max_items = min(parameter.get("maxLength", max_items_cap), max_items_cap)

        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        self.label = QLabel(_get_parameter_label(parameter))
        layout.addWidget(self.label)

        button_layout = QHBoxLayout()
        self.add_button = QPushButton(_tr("Add"), self)
        self.add_button.clicked.connect(self._on_add)
        button_layout.addWidget(self.add_button)
        for button in self._create_extra_buttons():
            button_layout.addWidget(button)
        self.remove_button = QPushButton(_tr("Remove Selected"), self)
        self.remove_button.clicked.connect(self._on_remove)
        self.count_label = QLabel(self)
        button_layout.addWidget(self.remove_button)
        button_layout.addWidget(self.count_label)
        button_layout.addStretch()
        layout.addLayout(button_layout)

        self.edit_control = self._create_list_widget()
        self.edit_control.setAlternatingRowColors(True)
        self.edit_control.setDragDropMode(QAbstractItemView.InternalMove)
        self.edit_control.setDefaultDropAction(Qt.MoveAction)
        self.edit_control.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        # Rows elide instead of scrolling sideways, so the height is a whole number of rows.
        self.edit_control.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.edit_control.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.edit_control.setUniformItemSizes(True)
        # Measure a row with the warning icon, which is at least as tall as one without,
        # and give every row that height so the list is a whole number of rows either way.
        self._warning_icon = self.style().standardIcon(QStyle.SP_MessageBoxWarning)
        probe = QListWidgetItem("")
        self.edit_control.addItem(probe)
        plain_height = self.edit_control.sizeHintForRow(0)
        probe.setIcon(self._warning_icon)
        self._row_height = max(
            plain_height, self.edit_control.sizeHintForRow(0), self._min_row_height(), 1
        )
        self.edit_control.clear()
        self.edit_control.setItemDelegate(self._create_delegate(self._row_height))
        self._visible_rows = self.MIN_VISIBLE_ROWS
        self._apply_visible_rows()
        layout.addWidget(self.edit_control)
        self.setLayout(layout)

        # The grip sits in the scroll bar's area when that is shown, so it doesn't cover
        # the scroll bar's down arrow, and in the list's bottom-right corner otherwise.
        self.scroll_bar_grip = _ListResizeGrip(self)
        self.edit_control.addScrollBarWidget(self.scroll_bar_grip, Qt.AlignBottom)
        self.corner_grip = _ListResizeGrip(self)
        self.corner_grip.setParent(self.edit_control)
        self.edit_control.verticalScrollBar().rangeChanged.connect(self._place_grip)
        self.edit_control.installEventFilter(self)
        self._place_grip()

        self.edit_control.itemSelectionChanged.connect(self._update_buttons)
        model = self.edit_control.model()
        for signal in (model.rowsInserted, model.rowsRemoved, model.rowsMoved, model.dataChanged):
            signal.connect(self._on_list_changed)

        if "description" in parameter:
            self.label.setToolTip(parameter["description"])

    # Hooks for subclasses.

    def _create_list_widget(self) -> QListWidget:
        return QListWidget(self)

    def _create_extra_buttons(self) -> List[QPushButton]:
        """Buttons placed between Add and Remove Selected."""
        return []

    def _min_row_height(self) -> int:
        """The height a row needs for its editor, beyond the height of its text and icon."""
        return 0

    def _create_delegate(self, row_height: int) -> QStyledItemDelegate:
        return _FixedRowHeightDelegate(self.edit_control, row_height)

    def _new_item(self, value: Any) -> QListWidgetItem:
        raise NotImplementedError

    def _item_value(self, item: QListWidgetItem) -> Any:
        raise NotImplementedError

    def _to_item_values(self, value: Any) -> Optional[List[Any]]:
        """Returns the list items of a set_value value, or None if it is not a list of the
        item type."""
        raise NotImplementedError

    def _on_add(self) -> None:
        raise NotImplementedError

    def _on_rows_changed(self) -> None:
        """Called on every change to the list, before the feedback is updated."""

    def _set_item_feedback(self, item: QListWidgetItem, error: str) -> None:
        # An icon rather than a text color, so an empty item and a selected item are
        # still visibly marked, and the cue does not rely on color alone.
        item.setIcon(self._warning_icon if error else QIcon())
        item.setToolTip(error)

    # Shared behavior.

    def visible_rows(self) -> int:
        """The number of rows the list is sized to show before it scrolls."""
        return self._visible_rows

    def set_visible_rows(self, rows: int) -> None:
        self._visible_rows = max(self.MIN_VISIBLE_ROWS, min(rows, self.MAX_VISIBLE_ROWS))
        self._apply_visible_rows()

    def resize_to_height(self, height: int) -> None:
        """Sizes the list to the whole number of rows nearest the given pixel height."""
        frame = 2 * self.edit_control.frameWidth()
        self.set_visible_rows(round((height - frame) / self._row_height))

    def _apply_visible_rows(self) -> None:
        frame = 2 * self.edit_control.frameWidth()
        self.edit_control.setFixedHeight(frame + self._visible_rows * self._row_height)

    def _place_grip(self, *args) -> None:
        scroll_bar = self.edit_control.verticalScrollBar()
        scrolls = scroll_bar.maximum() > scroll_bar.minimum()
        self.corner_grip.setVisible(not scrolls)
        if not scrolls:
            frame = self.edit_control.frameWidth()
            size = self.corner_grip.size()
            self.corner_grip.move(
                self.edit_control.width() - frame - size.width(),
                self.edit_control.height() - frame - size.height(),
            )
            self.corner_grip.raise_()

    def eventFilter(self, watched, event) -> bool:
        # Qt can still deliver events while the control is being destroyed, after its
        # attributes are gone.
        edit_control = getattr(self, "edit_control", None)
        if edit_control is not None and watched is edit_control and event.type() == QEvent.Resize:
            self._place_grip()
        return super().eventFilter(watched, event)

    def _selected_item(self) -> Optional[QListWidgetItem]:
        # Buttons act on the selection, not the current row: the current row can
        # remain after the selection is cleared, or move to a neighbour when a row is removed.
        selected = self.edit_control.selectedItems()
        return selected[0] if selected else None

    def _on_remove(self) -> None:
        item = self._selected_item()
        if item is not None:
            self.edit_control.takeItem(self.edit_control.row(item))

    def _on_list_changed(self, *args) -> None:
        # Updating feedback sets item icons, which reports dataChanged again.
        if self._suppress_changes:
            return
        self._suppress_changes = True
        try:
            self._on_rows_changed()
            self._update_feedback()
            self._update_buttons()
        finally:
            self._suppress_changes = False
        self._emit_value_changed()

    def _update_buttons(self) -> None:
        count = self.edit_control.count()
        self.add_button.setEnabled(count < self.max_items)
        self.remove_button.setEnabled(self._selected_item() is not None)
        self.count_label.setText(_tr("Items: {count}").format(count=count))

    def _update_feedback(self) -> None:
        """Highlights the list and each invalid item, with tooltips that explain why."""
        # Validate each item on its own against just the item constraints, so every
        # invalid item is marked instead of only the first.
        item_definition: Any = {
            "name": self.name(),
            "type": self.type(),
            "item": self.job_template_parameter.get("item", {}),
        }
        for i in range(self.edit_control.count()):
            item = self.edit_control.item(i)
            try:
                _validate_job_parameter_value(item_definition, [self._item_value(item)])
            except (ValueError, TypeError) as e:
                self._set_item_feedback(item, str(e).replace("item 0 ", "", 1))
            else:
                self._set_item_feedback(item, "")

        error = self._validation_error()
        if error:
            self.edit_control.setStyleSheet("QListWidget { border: 1px solid red; }")
            self.edit_control.setToolTip(error)
        else:
            self.edit_control.setStyleSheet("")
            self.edit_control.setToolTip(self.job_template_parameter.get("description", ""))

    def _validation_error(self) -> str:
        try:
            _validate_job_parameter_value(self.job_template_parameter, self.value())
        except (ValueError, TypeError) as e:
            return str(e)
        return ""

    def is_valid(self) -> bool:
        return not self._validation_error()

    def value(self) -> List[Any]:
        return [
            self._item_value(self.edit_control.item(i)) for i in range(self.edit_control.count())
        ]

    def set_value(self, value: Any) -> None:
        original_value = value
        if isinstance(value, str):
            # CLI and pre-GUI hook values arrive as a JSON array string.
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                value = None
        items = self._to_item_values(value)
        if items is None:
            # The value may come from an untrusted job bundle. Degrade gracefully like the
            # other controls instead of breaking the dialog.
            _logger.warning(
                "Job parameter %r has value %r that is not a list of %s; starting with an empty list.",
                self.name(),
                original_value,
                self.ITEM_KIND,
            )
            items = []
        self._before_set_value()
        self._suppress_changes = True
        try:
            self.edit_control.clear()
            for item_value in items:
                self.edit_control.addItem(self._new_item(item_value))
        finally:
            self._suppress_changes = False
        self._on_list_changed()

    def _before_set_value(self) -> None:
        """Called before set_value replaces the items."""


class _JobTemplateLineEditListWidget(_JobTemplateListWidgetBase):
    """An editable, reorderable list of single-line strings for a LIST[STRING] parameter.

    Items are edited in place, with Add, Edit and Remove Selected buttons.
    """

    OPENJD_CONTROL_TYPE: ControlType = ControlType.LINE_EDIT_LIST
    OPENJD_TYPES: List[str] = ["LIST[STRING]"]
    ITEM_KIND: str = "strings"

    def _build_ui(self, parameter):
        # The row added by the Add button, removed again if its editor closes while empty.
        self._pending_new_item: Optional[QListWidgetItem] = None
        super()._build_ui(parameter)
        self.edit_control.setEditTriggers(
            QAbstractItemView.DoubleClicked
            | QAbstractItemView.EditKeyPressed
            | QAbstractItemView.SelectedClicked
        )
        self.edit_control.itemDelegate().closeEditor.connect(self._on_editor_closed)

    def _create_extra_buttons(self) -> List[QPushButton]:
        self.edit_button = QPushButton(_tr("Edit"), self)
        self.edit_button.clicked.connect(self._on_edit)
        return [self.edit_button]

    def _new_item(self, value: Any) -> QListWidgetItem:
        item = QListWidgetItem(value)
        item.setFlags(self._ITEM_FLAGS)
        return item

    def _item_value(self, item: QListWidgetItem) -> str:
        return item.text()

    def _to_item_values(self, value: Any) -> Optional[List[Any]]:
        if isinstance(value, (list, tuple)) and all(isinstance(v, str) for v in value):
            return list(value)
        return None

    def _before_set_value(self) -> None:
        self._pending_new_item = None

    def _on_add(self) -> None:
        # The view closes an open editor itself when the current row changes, without
        # telling the delegate, so discard a still-empty previous row here too.
        self._discard_pending_item_if_empty()
        item = self._new_item("")
        self.edit_control.addItem(item)
        self.edit_control.setCurrentItem(item)
        self._pending_new_item = item
        self.edit_control.editItem(item)

    def _on_edit(self) -> None:
        item = self._selected_item()
        if item is not None:
            self.edit_control.editItem(item)

    def _on_editor_closed(self, editor=None, hint=None) -> None:
        if hint in (
            QAbstractItemDelegate.SubmitModelCache,
            QAbstractItemDelegate.EditNextItem,
            QAbstractItemDelegate.EditPreviousItem,
        ):
            # Enter or Tab commits the just-added row, even when it is empty.
            self._pending_new_item = None
        else:
            # Escape, or leaving the editor by clicking elsewhere, discards a just-added
            # row that is still empty, so Add without typing anything is a no-op.
            self._discard_pending_item_if_empty()

    def _discard_pending_item_if_empty(self) -> None:
        item = self._pending_new_item
        self._pending_new_item = None
        if item is not None and item.text() == "":
            row = self.edit_control.row(item)
            if row >= 0:
                self.edit_control.takeItem(row)
                # Removing the row makes a neighbour current and selected. Clear that, so a
                # button click that closed the editor (Remove, Edit) does not act on it.
                self.edit_control.setCurrentRow(-1)
                self.edit_control.clearSelection()

    def _update_buttons(self) -> None:
        super()._update_buttons()
        self.edit_button.setEnabled(self._selected_item() is not None)


# The item data role that holds a SPIN_BOX_LIST item's number.
_NUMBER_ROLE = Qt.UserRole
# The item data role that holds a path list item's path.
_PATH_ROLE = Qt.UserRole
# The range of the int that a QSpinBox holds.
_SPIN_BOX_INT_MIN = -(2**31)
_SPIN_BOX_INT_MAX = 2**31 - 1


class _EditorRowListView(QListWidget):
    """The list of a SPIN_BOX_LIST or path list control, whose rows each hold an
    always-open editor.

    A view focuses a row's persistent editor when the row is pressed or becomes current.
    Pressing a row's drag strip would then hand the press to the editor and the view
    would never start dragging the row, so neither moves focus into the editor; an
    editor still takes focus when it is clicked.
    """

    def edit(self, index, trigger=None, event=None):  # type: ignore[override]
        if trigger is None:
            return super().edit(index)
        if trigger in (
            QAbstractItemView.EditTrigger.NoEditTriggers,
            QAbstractItemView.EditTrigger.CurrentChanged,
        ):
            return False
        return super().edit(index, trigger, event)

    def viewportEvent(self, event) -> bool:
        # Rows are editors, which show their own hover state, so the row behind one is
        # not highlighted too. The view tracks the hovered row from these events alone.
        if event.type() in (QEvent.HoverEnter, QEvent.HoverMove, QEvent.HoverLeave):
            return True
        return super().viewportEvent(event)


class _EditorRowListWidgetBase(_JobTemplateListWidgetBase):
    """The shared behavior of the list controls whose rows each hold an always-open editor,
    right of a strip that holds the row's warning icon and is where a row is grabbed to
    drag it. Subclasses create the editor, and a delegate that places and loads it."""

    _ITEM_FLAGS = Qt.ItemIsSelectable | Qt.ItemIsEnabled | Qt.ItemIsDragEnabled

    def _build_ui(self, parameter):
        icon_size = self.style().pixelMetric(QStyle.PM_SmallIconSize, None, self)
        self._strip_width = icon_size + 10
        super()._build_ui(parameter)
        # The editors are opened for every row.
        self.edit_control.setEditTriggers(QAbstractItemView.NoEditTriggers)

    def create_editor(self, parent: QWidget) -> QWidget:
        """Creates the editor of one row."""
        raise NotImplementedError

    def _editor_line_edit(self, editor: Any) -> QLineEdit:
        """The line edit inside an editor, where its text is typed."""
        raise NotImplementedError

    def _editor_accessible_text(self, item: QListWidgetItem) -> str:
        """What a screen reader announces for a row, since the row itself has no text."""
        return str(self._item_value(item))

    def _min_row_height(self) -> int:
        probe = self.create_editor(self)
        height = probe.sizeHint().height()
        probe.hide()
        probe.deleteLater()
        return height

    def _create_list_widget(self) -> QListWidget:
        return _EditorRowListView(self)

    def row_editor(self, row: int) -> Any:
        """The editor of the given row."""
        return self.edit_control.indexWidget(self.edit_control.model().index(row, 0))

    def _on_rows_changed(self) -> None:
        # Rows that are added, or recreated by a drag, need their editor opened.
        for i in range(self.edit_control.count()):
            item = self.edit_control.item(i)
            if not self.edit_control.isPersistentEditorOpen(item):
                self.edit_control.openPersistentEditor(item)
                # Opening an editor selects its text; only a focused row should show a
                # selection.
                editor = self.edit_control.itemWidget(item)
                if editor is not None:
                    line_edit = self._editor_line_edit(editor)
                    if not line_edit.hasFocus():
                        line_edit.deselect()

    def _set_item_feedback(self, item: QListWidgetItem, error: str) -> None:
        super()._set_item_feedback(item, error)
        item.setData(Qt.AccessibleTextRole, self._editor_accessible_text(item))
        editor = self.edit_control.itemWidget(item)
        if editor is not None:
            editor.setToolTip(error or self.job_template_parameter.get("description", ""))

    def eventFilter(self, watched, event) -> bool:
        # Focusing a row's editor, or a widget inside it, selects the row so Remove
        # Selected acts on it.
        edit_control = getattr(self, "edit_control", None)
        if edit_control is not None and event.type() == QEvent.FocusIn:
            for i in range(edit_control.count()):
                editor = self.row_editor(i)
                if editor is not None and (editor is watched or editor.isAncestorOf(watched)):
                    edit_control.setCurrentRow(i)
                    break
        return super().eventFilter(watched, event)


class _SpinBoxListDelegate(_FixedRowHeightDelegate):
    """Gives each row of a SPIN_BOX_LIST a spin box, right of a strip that holds the
    row's warning icon and is where a row is grabbed to drag it."""

    def __init__(
        self,
        owner: "_JobTemplateSpinBoxListWidget",
        row_height: int,
        strip_width: int,
        right_inset: int,
    ):
        super().__init__(owner.edit_control, row_height)
        self._owner = owner
        self._strip_width = strip_width
        self._right_inset = right_inset

    def createEditor(self, parent, option, index):
        editor = self._owner.create_spin_box(parent)
        editor._loading = False
        # A spin box's line edit accepts drops, which would make each row, instead of the
        # list, the target of a row being dragged to reorder it.
        editor.setAcceptDrops(False)
        editor.lineEdit().setAcceptDrops(False)
        editor.valueChanged.connect(lambda _value, e=editor: self._on_value_changed(e))
        # Focusing a spin box selects its row, so Remove Selected acts on it.
        editor.installEventFilter(self._owner)
        return editor

    def _on_value_changed(self, editor) -> None:
        if not editor._loading:
            self.commitData.emit(editor)

    def setEditorData(self, editor, index):
        value = index.data(_NUMBER_ROLE)
        # The view reloads every editor on any dataChanged, including the icon and
        # tooltip updates of other rows. Reloading only a changed value keeps text the user
        # is still typing, which the spin box does not commit until Enter or focus loss.
        if value is None or getattr(editor, "_loaded_value", None) == value:
            return
        # Don't write the value back: the item keeps its value even when the spin box
        # cannot step to it, so an out-of-range value is reported instead of changed.
        editor._loading = True
        try:
            self._owner.show_value(editor, value)
            editor._loaded_value = value
        finally:
            editor._loading = False

    def setModelData(self, editor, model, index):
        model.setData(index, editor.value(), _NUMBER_ROLE)

    def updateEditorGeometry(self, editor, option, index):
        # The right inset keeps the list's resize grip clear of the last row's arrows.
        editor.setGeometry(option.rect.adjusted(self._strip_width, 0, -self._right_inset, 0))


class _JobTemplateSpinBoxListWidget(_EditorRowListWidgetBase):
    """A reorderable list of numbers for a LIST[INT] or LIST[FLOAT] parameter.

    Every row is the same spin box that an INT or FLOAT parameter uses, always open for
    editing. Each row's item constraints and userInterface settings apply to every spin box.
    """

    OPENJD_CONTROL_TYPE: ControlType = ControlType.SPIN_BOX_LIST
    OPENJD_TYPES: List[str] = ["LIST[INT]", "LIST[FLOAT]"]

    def _build_ui(self, parameter):
        self._is_float = parameter["type"] == "LIST[FLOAT]"
        self.ITEM_KIND = "numbers" if self._is_float else "64-bit integers"
        super()._build_ui(parameter)

    def create_editor(self, parent: QWidget) -> QWidget:
        return self.create_spin_box(parent)

    def _editor_line_edit(self, editor: Any) -> QLineEdit:
        return editor.lineEdit()

    def create_spin_box(self, parent: QWidget) -> QWidget:
        """Creates a spin box configured like the one for an INT or FLOAT parameter with
        the item constraints and userInterface settings of this parameter."""
        item_constraints = self.job_template_parameter.get("item", {})
        user_interface = self.job_template_parameter.get("userInterface", {})
        spin_box: Any
        if self._is_float:
            spin_box = FloatDragSpinBox(parent)
            decimals = user_interface.get("decimals", -1)
            if decimals >= 0:
                spin_box.setDecimalMode(DecimalMode.FIXED_DECIMAL)
                spin_box.setDecimals(decimals)
            step_type = QDoubleSpinBox.DefaultStepType
        else:
            spin_box = IntDragSpinBox(parent)
            step_type = QSpinBox.DefaultStepType
        lowest, highest = spin_box.minimum(), spin_box.maximum()
        if "minValue" in item_constraints:
            spin_box.setMinimum(max(lowest, min(item_constraints["minValue"], highest)))
        if "maxValue" in item_constraints:
            spin_box.setMaximum(max(lowest, min(item_constraints["maxValue"], highest)))
        # show_value widens the range to show a value outside it, and restores it after.
        spin_box._base_range = (spin_box.minimum(), spin_box.maximum())
        single_step_delta = user_interface.get("singleStepDelta", -1)
        if single_step_delta >= 0:
            spin_box.setSingleStep(single_step_delta)
            spin_box.setStepType(step_type)
        if "description" in self.job_template_parameter:
            spin_box.setToolTip(self.job_template_parameter["description"])
        return spin_box

    def show_value(self, spin_box: Any, value: Any) -> None:
        """Shows an item's value in its spin box without changing the value.

        A value outside the item constraints' range widens the spin box's range to include
        it, so the spin box shows the value rather than the nearest bound. A LIST[INT]
        value beyond the range a spin box can hold is shown as text in place of the
        spin box's minimum; stepping from it edits the value.
        """
        base_min, base_max = spin_box._base_range
        if self._is_float:
            spin_box.setSpecialValueText("")
            spin_box.setRange(min(base_min, value), max(base_max, value))
            if spin_box.decimalMode() == DecimalMode.ADAPTIVE_DECIMAL:
                # Show the value at full precision; the spin box adapts after setValue.
                spin_box.setDecimals(FloatDragSpinBox.MAX_ADAPTIVE_DECIMALS)
            spin_box.setValue(value)
            return
        low = max(_SPIN_BOX_INT_MIN, min(base_min, value))
        high = min(_SPIN_BOX_INT_MAX, max(base_max, value))
        spin_box.setRange(low, high)
        if low <= value <= high:
            spin_box.setSpecialValueText("")
            spin_box.setValue(value)
        else:
            spin_box.setSpecialValueText(str(value))
            spin_box.setValue(spin_box.minimum())

    def _create_delegate(self, row_height: int) -> QStyledItemDelegate:
        grip_size = self.style().pixelMetric(QStyle.PM_SizeGripSize, None, self)
        return _SpinBoxListDelegate(self, row_height, self._strip_width, grip_size)

    def spin_box(self, row: int) -> Any:
        """The spin box that edits the given row."""
        return self.row_editor(row)

    def _new_item(self, value: Any) -> QListWidgetItem:
        item = QListWidgetItem()
        item.setFlags(self._ITEM_FLAGS)
        item.setData(_NUMBER_ROLE, value)
        return item

    def _item_value(self, item: QListWidgetItem) -> Any:
        return item.data(_NUMBER_ROLE)

    def _to_item_values(self, value: Any) -> Optional[List[Any]]:
        if not isinstance(value, (list, tuple)):
            return None
        item_types: Any = (int, float) if self._is_float else int
        if not all(isinstance(v, item_types) and not isinstance(v, bool) for v in value):
            return None
        if self._is_float:
            return [float(v) for v in value]
        # Qt, and so the parameter_changed signal, cannot carry larger ints.
        if not all(_MIN_INT64 <= v <= _MAX_INT64 for v in value):
            return None
        return list(value)

    def _new_item_value(self) -> Any:
        """The value of a row the Add button adds: a copy of the last row, or else the
        first allowed value, or else zero moved into the item value range."""
        count = self.edit_control.count()
        if count:
            return self._item_value(self.edit_control.item(count - 1))
        item_constraints = self.job_template_parameter.get("item", {})
        if "allowedValues" in item_constraints:
            value = item_constraints["allowedValues"][0]
        else:
            value = 0
            if "minValue" in item_constraints:
                value = max(value, item_constraints["minValue"])
            if "maxValue" in item_constraints:
                value = min(value, item_constraints["maxValue"])
        return float(value) if self._is_float else value

    def _on_add(self) -> None:
        item = self._new_item(self._new_item_value())
        self.edit_control.addItem(item)
        self.edit_control.setCurrentItem(item)
        spin_box = self.spin_box(self.edit_control.row(item))
        if spin_box is not None:
            spin_box.setFocus()
            spin_box.selectAll()


def _file_dialog_filters(parameter: Dict[str, Any]) -> tuple[str, str]:
    """The file dialog's filter string, and its initially selected filter, from a PATH or
    LIST[PATH] parameter's userInterface fileFilters and fileFilterDefault."""
    filetype_filter = "Any files (*)"
    selected_filter = ""
    user_interface = parameter.get("userInterface", {})
    file_filter_list = user_interface.get("fileFilters")
    if file_filter_list:
        filetype_filter = ";;".join(
            f"{file_filter['label']} ({' '.join(file_filter['patterns'])})"
            for file_filter in file_filter_list
        )
    file_filter_default = user_interface.get("fileFilterDefault")
    if file_filter_default:
        selected_filter = (
            f"{file_filter_default['label']} ({' '.join(file_filter_default['patterns'])})"
        )
    if not selected_filter:
        selected_filter = filetype_filter.split(";", 1)[0]
    return filetype_filter, selected_filter


class _PathListDelegate(_FixedRowHeightDelegate):
    """Gives each row of a path list control a path picker, a line edit with a button that
    opens a file or directory dialog, right of a strip that holds the row's warning icon
    and is where a row is grabbed to drag it."""

    def __init__(
        self,
        owner: "_JobTemplatePathListWidgetBase",
        row_height: int,
        strip_width: int,
        right_inset: int,
    ):
        super().__init__(owner.edit_control, row_height)
        self._owner = owner
        self._strip_width = strip_width
        self._right_inset = right_inset

    def createEditor(self, parent, option, index):
        editor = self._owner.create_editor(parent)
        line_edit = self._owner._editor_line_edit(editor)
        # A line edit accepts drops, which would make each row, instead of the list, the
        # target of a row being dragged to reorder it.
        editor.setAcceptDrops(False)
        line_edit.setAcceptDrops(False)
        # Commit as the user types, so the list's feedback and the Submit button follow
        # the text, and when the dialog button picks a path.
        line_edit.textEdited.connect(lambda _text, e=editor: self.commitData.emit(e))
        editor.path_changed.connect(lambda _path, e=editor: self.commitData.emit(e))
        # Focusing a row's line edit or button selects its row, so Remove Selected acts on it.
        for child in editor.findChildren(QWidget):
            child.installEventFilter(self._owner)
        return editor

    def setEditorData(self, editor, index):
        value = index.data(_PATH_ROLE)
        line_edit = self._owner._editor_line_edit(editor)
        # The view reloads every editor on any dataChanged, including the icon and tooltip
        # updates of other rows. Only a changed value is loaded, so the cursor stays put.
        if value is None or line_edit.text() == value:
            return
        # Set the line edit, not the picker: the item value is already normalized as the
        # picker would show it, and the picker's setText would also report a change.
        line_edit.setText(value)

    def setModelData(self, editor, model, index):
        model.setData(index, editor.text(), _PATH_ROLE)

    def updateEditorGeometry(self, editor, option, index):
        # The right inset keeps the list's resize grip clear of the last row's button.
        editor.setGeometry(option.rect.adjusted(self._strip_width, 0, -self._right_inset, 0))


class _JobTemplatePathListWidgetBase(_EditorRowListWidgetBase):
    """A reorderable list of paths for a LIST[PATH] parameter.

    Every row is the same path picker that a PATH parameter with the corresponding control
    uses, always open for editing. Add appends an empty row, focuses it, and opens the
    row's file or directory dialog.
    """

    OPENJD_TYPES: List[str] = ["LIST[PATH]"]
    ITEM_KIND: str = "paths"

    def _build_ui(self, parameter):
        self._file_filter, self._selected_file_filter = _file_dialog_filters(parameter)
        super()._build_ui(parameter)

    def _editor_line_edit(self, editor: Any) -> QLineEdit:
        return editor.findChild(QLineEdit)

    def _configure_editor(self, editor: QWidget) -> QWidget:
        editor.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
        if "description" in self.job_template_parameter:
            editor.setToolTip(self.job_template_parameter["description"])
        return editor

    def _create_delegate(self, row_height: int) -> QStyledItemDelegate:
        grip_size = self.style().pixelMetric(QStyle.PM_SizeGripSize, None, self)
        return _PathListDelegate(self, row_height, self._strip_width, grip_size)

    def _new_item(self, value: Any) -> QListWidgetItem:
        item = QListWidgetItem()
        item.setFlags(self._ITEM_FLAGS)
        item.setData(_PATH_ROLE, value)
        return item

    def _item_value(self, item: QListWidgetItem) -> str:
        return item.data(_PATH_ROLE)

    def _to_item_values(self, value: Any) -> Optional[List[Any]]:
        if isinstance(value, (list, tuple)) and all(isinstance(v, str) for v in value):
            # Shown as the PATH control's picker shows a value it is given.
            return [_normalize_path_text(v) for v in value]
        return None

    def _on_add(self) -> None:
        item = self._new_item("")
        self.edit_control.addItem(item)
        self.edit_control.setCurrentItem(item)
        editor = self.row_editor(self.edit_control.row(item))
        if editor is not None:
            self._editor_line_edit(editor).setFocus()
            # Canceling leaves the empty row for typing a path, such as a URI, that the
            # dialog can't choose.
            self._open_dialog(editor)

    def _open_dialog(self, editor: Any) -> None:
        """Opens the dialog that the row's "..." button opens."""
        editor.on_choose_file()


class _JobTemplateInputFileListWidget(_JobTemplatePathListWidgetBase):
    OPENJD_CONTROL_TYPE: ControlType = ControlType.CHOOSE_INPUT_FILE_LIST

    def create_editor(self, parent: QWidget) -> QWidget:
        return self._configure_editor(
            InputFilePickerWidget(
                initial_filename="",
                file_label=self.name(),
                filter=self._file_filter,
                selected_filter=self._selected_file_filter,
                parent=parent,
            )
        )


class _JobTemplateOutputFileListWidget(_JobTemplatePathListWidgetBase):
    OPENJD_CONTROL_TYPE: ControlType = ControlType.CHOOSE_OUTPUT_FILE_LIST

    def create_editor(self, parent: QWidget) -> QWidget:
        return self._configure_editor(
            OutputFilePickerWidget(
                initial_filename="",
                file_label=self.name(),
                filter=self._file_filter,
                selected_filter=self._selected_file_filter,
                parent=parent,
            )
        )


class _JobTemplateDirectoryListWidget(_JobTemplatePathListWidgetBase):
    OPENJD_CONTROL_TYPE: ControlType = ControlType.CHOOSE_DIRECTORY_LIST

    def create_editor(self, parent: QWidget) -> QWidget:
        return self._configure_editor(
            DirectoryPickerWidget(initial_directory="", directory_label=self.name(), parent=parent)
        )

    def _open_dialog(self, editor: Any) -> None:
        editor.on_choose_directory()


class _CheckBoxListDelegate(_FixedRowHeightDelegate):
    """Draws each row of a CHECK_BOX_LIST right of a strip where the row is grabbed to drag
    it, and toggles a row when its label is clicked as well as its checkbox."""

    def __init__(self, view: QListWidget, row_height: int, strip_width: int):
        super().__init__(view, row_height)
        self._view = view
        self._strip_width = strip_width
        # The row whose label the left button was last pressed on. A click toggles only
        # when it is released on the label of that same row, so a drag or a press that
        # ends elsewhere does not toggle anything.
        self._label_pressed_row: Optional[QPersistentModelIndex] = None

    def content_option(self, option: QStyleOptionViewItem) -> QStyleOptionViewItem:
        """The option for the checkbox and label, which sit right of the drag strip."""
        content = QStyleOptionViewItem(option)
        content.rect = option.rect.adjusted(self._strip_width, 0, 0, 0)
        return content

    def paint(self, painter, option, index):
        # The selection and hover highlight span the whole row, including the strip. A style
        # whose ShowDecorationSelected hint is false for a list view would otherwise fill
        # only the text rectangle.
        background = QStyleOptionViewItem(option)
        self.initStyleOption(background, index)
        background.showDecorationSelected = True
        style = option.widget.style() if option.widget else QApplication.style()
        style.drawPrimitive(QStyle.PE_PanelItemViewItem, background, painter, option.widget)
        super().paint(painter, self.content_option(option), index)

    @staticmethod
    def _event_pos(event) -> QPoint:
        # position is Qt 6; pos is its Qt 5 equivalent.
        if hasattr(event, "position"):
            return event.position().toPoint()
        return event.pos()

    def editorEvent(self, event, model, option, index):
        content = self.content_option(option)
        # The base class toggles a click on the checkbox, and reports it as handled.
        handled = super().editorEvent(event, model, content, index)
        event_type = event.type()
        if event_type not in (
            QEvent.MouseButtonPress,
            QEvent.MouseButtonDblClick,
            QEvent.MouseButtonRelease,
        ):
            return handled
        on_label = (
            not handled
            and event.button() == Qt.LeftButton
            and content.rect.contains(self._event_pos(event))
        )
        if event_type != QEvent.MouseButtonRelease:
            self._label_pressed_row = QPersistentModelIndex(index) if on_label else None
            return handled
        pressed_row, self._label_pressed_row = self._label_pressed_row, None
        if on_label and pressed_row is not None and pressed_row == index:
            item = self._view.itemFromIndex(index)
            item.setCheckState(Qt.Unchecked if item.checkState() == Qt.Checked else Qt.Checked)
            return True
        return handled


class _JobTemplateCheckBoxListWidget(_JobTemplateListWidgetBase):
    """A reorderable list of checkboxes for a LIST[BOOL] parameter.

    Each row is a checkable item labelled by its position, "Item 1", "Item 2" and so on,
    so the labels stay in order when rows are reordered. Clicking a row's checkbox or
    label, or pressing Space on the current row, toggles it. Like a SPIN_BOX_LIST, a row
    is dragged by the strip left of its checkbox, and clicking the strip selects the row
    without toggling it.
    """

    OPENJD_CONTROL_TYPE: ControlType = ControlType.CHECK_BOX_LIST
    OPENJD_TYPES: List[str] = ["LIST[BOOL]"]
    ITEM_KIND: str = "booleans"

    _ITEM_FLAGS = (
        Qt.ItemIsSelectable | Qt.ItemIsEnabled | Qt.ItemIsUserCheckable | Qt.ItemIsDragEnabled
    )

    def _build_ui(self, parameter):
        icon_size = self.style().pixelMetric(QStyle.PM_SmallIconSize, None, self)
        self._strip_width = icon_size + 10
        super()._build_ui(parameter)
        self.edit_control.setEditTriggers(QAbstractItemView.NoEditTriggers)

    def _create_delegate(self, row_height: int) -> QStyledItemDelegate:
        return _CheckBoxListDelegate(self.edit_control, row_height, self._strip_width)

    def _min_row_height(self) -> int:
        probe = self._new_item(True)
        probe.setText(self._item_label(0))
        self.edit_control.addItem(probe)
        height = self.edit_control.sizeHintForRow(self.edit_control.row(probe))
        self.edit_control.takeItem(self.edit_control.row(probe))
        return height

    def _new_item(self, value: Any) -> QListWidgetItem:
        # The label is set from the row's position once it is in the list.
        item = QListWidgetItem()
        item.setFlags(self._ITEM_FLAGS)
        item.setCheckState(Qt.Checked if value else Qt.Unchecked)
        return item

    @staticmethod
    def _item_label(row: int) -> str:
        return _tr("Item {number}").format(number=row + 1)

    def _item_value(self, item: QListWidgetItem) -> bool:
        return item.checkState() == Qt.Checked

    def _to_item_values(self, value: Any) -> Optional[List[Any]]:
        if not isinstance(value, (list, tuple)):
            return None
        items = [_to_bool(v) for v in value]
        if any(v is None for v in items):
            return None
        return items

    def _on_add(self) -> None:
        count = self.edit_control.count()
        value = self._item_value(self.edit_control.item(count - 1)) if count else False
        item = self._new_item(value)
        self.edit_control.addItem(item)
        # The new row is current, so Space toggles it.
        self.edit_control.setCurrentItem(item)
        self.edit_control.setFocus()

    def _on_rows_changed(self) -> None:
        # Number the rows by position, so a row moved by a drag takes its new number.
        for i in range(self.edit_control.count()):
            item = self.edit_control.item(i)
            label = self._item_label(i)
            if item.text() != label:
                item.setText(label)


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
        self.edit_control.interpretText()
        return self.edit_control.value()

    def set_value(self, value):
        self.edit_control.setValue(value)

    def _change_signal(self):
        return self.edit_control.valueChanged


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
        self.edit_control.interpretText()
        return self.edit_control.value()

    def set_value(self, value):
        self.edit_control.setValue(value)

    def _change_signal(self):
        return self.edit_control.valueChanged


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

    def _change_signal(self):
        return self.edit_control.currentIndexChanged


class _JobTemplateBaseFileWidget(_JobTemplateWidget):
    OPENJD_TYPES: List[str] = ["PATH"]
    OPENJD_DEFAULT_VALUE: str = ""
    OPENJD_REQUIRED_PARAMETER_FIELDS: List[str] = []
    OPENJD_DISALLOWED_PARAMETER_FIELDS: List[str] = ["allowedValues"]

    def _build_ui(self, parameter):
        filetype_filter, selected_filter = _file_dialog_filters(parameter)

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

    def _change_signal(self):
        return self.edit_control.path_changed


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

    def _change_signal(self):
        return self.edit_control.path_changed


# These are the permitted sets of values that can be in a string job parameter 'allowedValues'
# when the user interface control is CHECK_BOX.
ALLOWED_VALUES_FOR_CHECK_BOX = (["TRUE", "FALSE"], ["YES", "NO"], ["ON", "OFF"], ["1", "0"])


class _JobTemplateCheckBoxWidget(_JobTemplateWidget):
    OPENJD_CONTROL_TYPE: ControlType = ControlType.CHECK_BOX
    OPENJD_TYPES: List[str] = ["STRING", "BOOL"]
    OPENJD_DEFAULT_VALUE: bool = False
    OPENJD_REQUIRED_PARAMETER_FIELDS: List[str] = []
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

        if parameter["type"] == "BOOL":
            self.true_value = True
            self.false_value = False
        else:
            # STRING checkboxes represent boolean values through allowedValues.
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

        # Add the description as a tooltip if provided
        if "description" in parameter:
            for widget in (self.label, self.edit_control):
                widget.setToolTip(parameter["description"])

    def value(self) -> str | bool:
        if self.edit_control.isChecked():
            return self.true_value
        else:
            return self.false_value

    def set_value(self, value: str | bool) -> None:
        if self.job_template_parameter["type"] == "BOOL":
            try:
                checked = _validate_job_parameter_value(self.job_template_parameter, value) is True
            except (ValueError, TypeError):
                # The value may come from an untrusted job bundle, e.g. a shared queue
                # bundle's parameter_values.yaml. Degrade gracefully like the other
                # controls (STRING checkbox, dropdown) instead of breaking the dialog.
                _logger.warning(
                    "Job parameter %r has non-boolean value %r; falling back to unchecked.",
                    self.job_template_parameter["name"],
                    value,
                )
                checked = False
        else:
            checked = value == self.true_value
        self.edit_control.setChecked(checked)

    def _change_signal(self):
        return self.edit_control.stateChanged


class _JobTemplateHiddenWidget(_JobTemplateWidget):
    OPENJD_CONTROL_TYPE: ControlType = ControlType.HIDDEN
    OPENJD_TYPES: List[str] = [
        "PATH",
        "INT",
        "FLOAT",
        "STRING",
        "BOOL",
        "RANGE_EXPR",
        "LIST[STRING]",
        "LIST[PATH]",
        "LIST[INT]",
        "LIST[FLOAT]",
        "LIST[BOOL]",
    ]

    OPENJD_DEFAULT_VALUE: str = ""  # Hidden parameters do not require defaults
    OPENJD_REQUIRED_PARAMETER_FIELDS: List[str] = []
    OPENJD_DISALLOWED_PARAMETER_FIELDS: List[str] = []

    def __init__(self, parent: QWidget, parameter: Dict[str, Any]):
        super().__init__(parent, parameter)

    def _build_ui(self, parameter: Dict[str, Any]) -> None:
        pass

    def value(self) -> Any:
        return self._value

    def set_value(self, value: Any) -> None:
        if self.job_template_parameter["type"] == "BOOL":
            if value == self.OPENJD_DEFAULT_VALUE:
                value = False
            else:
                try:
                    value = _validate_job_parameter_value(self.job_template_parameter, value)
                except (ValueError, TypeError):
                    # Keep the value as-is so submission reports it as an error.
                    pass
        elif self.job_template_parameter["type"] in _LIST_TYPES:
            if value == self.OPENJD_DEFAULT_VALUE:
                value = []
            else:
                try:
                    value = _validate_job_parameter_value(self.job_template_parameter, value)
                except (ValueError, TypeError):
                    # Keep the value as-is so submission reports it as an error.
                    pass
        self._value = value


class _JobTemplateGroupLayout(QGroupBox):
    def __init__(self, parent: QWidget, group_name: str):
        super().__init__(parent)
        self.setTitle(group_name)
        self.setLayout(QVBoxLayout())
