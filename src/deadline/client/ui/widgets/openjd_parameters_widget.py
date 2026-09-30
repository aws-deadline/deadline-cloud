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

from qtpy.QtCore import QEvent, QRegularExpression, Qt, Signal  # type: ignore
from qtpy.QtGui import QIcon, QPainter, QValidator
from qtpy.QtWidgets import (  # type: ignore
    QAbstractItemDelegate,
    QAbstractItemView,
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
    _MAX_STRING_LIST_ITEMS,
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
        self.edit_control.setPlainText(value.as_posix() if isinstance(value, Path) else str(value))

    def _handle_text_changed(self, text, callback):
        message = deepcopy(self.job_template_parameter)
        message["value"] = text
        callback(message)

    def connect_parameter_changed(self, callback):
        self.edit_control.textChanged.connect(
            lambda: self._handle_text_changed(self.value(), callback)
        )


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
    """A size grip that drags the height of a LINE_EDIT_LIST's list, in whole rows.

    QSizeGrip only resizes top-level windows, so this draws the platform size grip and
    handles the drag itself.
    """

    def __init__(self, owner: "_JobTemplateLineEditListWidget"):
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


class _JobTemplateLineEditListWidget(_JobTemplateWidget):
    """An editable, reorderable list of single-line strings for a LIST[STRING] parameter.

    Laid out like the known asset paths list in the settings dialog: a button row above
    a list. Items are edited in place, and can be reordered by dragging since the order
    of a list parameter's values is significant.
    """

    OPENJD_CONTROL_TYPE: ControlType = ControlType.LINE_EDIT_LIST
    OPENJD_TYPES: List[str] = ["LIST[STRING]"]
    OPENJD_DEFAULT_VALUE: List[str] = []
    OPENJD_REQUIRED_PARAMETER_FIELDS: List[str] = []
    OPENJD_DISALLOWED_PARAMETER_FIELDS: List[str] = ["allowedValues"]
    # A fixed height that the user resizes with the grip, so other parameters stay in view.
    IS_VERTICAL_EXPANDING: bool = False
    MIN_VISIBLE_ROWS: int = 3
    MAX_VISIBLE_ROWS: int = _MAX_STRING_LIST_ITEMS

    _ITEM_FLAGS = Qt.ItemIsSelectable | Qt.ItemIsEnabled | Qt.ItemIsEditable | Qt.ItemIsDragEnabled

    def _build_ui(self, parameter):
        self._suppress_changes = False
        self._change_callbacks: List[Any] = []
        # The row added by the Add button, removed again if its editor closes while empty.
        self._pending_new_item: Optional[QListWidgetItem] = None
        self.max_items = min(
            parameter.get("maxLength", _MAX_STRING_LIST_ITEMS), _MAX_STRING_LIST_ITEMS
        )

        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        self.label = QLabel(_get_parameter_label(parameter))
        layout.addWidget(self.label)

        button_layout = QHBoxLayout()
        self.add_button = QPushButton(_tr("Add"), self)
        self.add_button.clicked.connect(self._on_add)
        self.edit_button = QPushButton(_tr("Edit"), self)
        self.edit_button.clicked.connect(self._on_edit)
        self.remove_button = QPushButton(_tr("Remove Selected"), self)
        self.remove_button.clicked.connect(self._on_remove)
        self.count_label = QLabel(self)
        button_layout.addWidget(self.add_button)
        button_layout.addWidget(self.edit_button)
        button_layout.addWidget(self.remove_button)
        button_layout.addWidget(self.count_label)
        button_layout.addStretch()
        layout.addLayout(button_layout)

        self.edit_control = QListWidget(self)
        self.edit_control.setAlternatingRowColors(True)
        self.edit_control.setDragDropMode(QAbstractItemView.InternalMove)
        self.edit_control.setDefaultDropAction(Qt.MoveAction)
        self.edit_control.setEditTriggers(
            QAbstractItemView.DoubleClicked
            | QAbstractItemView.EditKeyPressed
            | QAbstractItemView.SelectedClicked
        )
        self.edit_control.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        # Rows elide instead of scrolling sideways, so the height is a whole number of rows.
        self.edit_control.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.edit_control.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.edit_control.setUniformItemSizes(True)
        # Measure a row with the warning icon, which is at least as tall as one without,
        # and give every row that height so the list is a whole number of rows either way.
        self._warning_icon = self.style().standardIcon(QStyle.SP_MessageBoxWarning)
        probe = self._new_item("")
        self.edit_control.addItem(probe)
        plain_height = self.edit_control.sizeHintForRow(0)
        probe.setIcon(self._warning_icon)
        self._row_height = max(plain_height, self.edit_control.sizeHintForRow(0), 1)
        self.edit_control.clear()
        self.edit_control.setItemDelegate(
            _FixedRowHeightDelegate(self.edit_control, self._row_height)
        )
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
        self.edit_control.itemDelegate().closeEditor.connect(self._on_editor_closed)
        model = self.edit_control.model()
        for signal in (model.rowsInserted, model.rowsRemoved, model.rowsMoved, model.dataChanged):
            signal.connect(self._on_list_changed)

        if "description" in parameter:
            self.label.setToolTip(parameter["description"])

    def _new_item(self, text: str) -> QListWidgetItem:
        item = QListWidgetItem(text)
        item.setFlags(self._ITEM_FLAGS)
        return item

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
        if watched is self.edit_control and event.type() == QEvent.Resize:
            self._place_grip()
        return super().eventFilter(watched, event)

    def _on_add(self) -> None:
        # The view closes an open editor itself when the current row changes, without
        # telling the delegate, so discard a still-empty previous row here too.
        self._discard_pending_item_if_empty()
        item = self._new_item("")
        self.edit_control.addItem(item)
        self.edit_control.setCurrentItem(item)
        self._pending_new_item = item
        self.edit_control.editItem(item)

    def _selected_item(self) -> Optional[QListWidgetItem]:
        # Buttons act on the selection, not the current row: the current row can
        # remain after the selection is cleared, or move to a neighbour when a row is removed.
        selected = self.edit_control.selectedItems()
        return selected[0] if selected else None

    def _on_edit(self) -> None:
        item = self._selected_item()
        if item is not None:
            self.edit_control.editItem(item)

    def _on_remove(self) -> None:
        item = self._selected_item()
        if item is not None:
            self.edit_control.takeItem(self.edit_control.row(item))

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

    def _on_list_changed(self, *args) -> None:
        # Updating feedback sets item colors, which reports dataChanged again.
        if self._suppress_changes:
            return
        self._suppress_changes = True
        try:
            self._update_feedback()
            self._update_buttons()
        finally:
            self._suppress_changes = False
        message = deepcopy(self.job_template_parameter)
        message["value"] = self.value()
        for callback in self._change_callbacks:
            callback(message)

    def _update_buttons(self) -> None:
        count = self.edit_control.count()
        has_selection = self._selected_item() is not None
        self.add_button.setEnabled(count < self.max_items)
        self.edit_button.setEnabled(has_selection)
        self.remove_button.setEnabled(has_selection)
        self.count_label.setText(_tr("Items: {count}").format(count=count))

    def _update_feedback(self) -> None:
        """Highlights the list and each invalid item, with tooltips that explain why."""
        # Validate each item on its own against just the item constraints, so every
        # invalid item is marked instead of only the first.
        item_definition: Any = {
            "name": self.name(),
            "type": "LIST[STRING]",
            "item": self.job_template_parameter.get("item", {}),
        }
        warning_icon = self._warning_icon
        for i in range(self.edit_control.count()):
            item = self.edit_control.item(i)
            try:
                _validate_job_parameter_value(item_definition, [item.text()])
            except (ValueError, TypeError) as e:
                # An icon rather than a text color, so an empty item and a selected item
                # are still visibly marked, and the cue does not rely on color alone.
                item.setIcon(warning_icon)
                item.setToolTip(str(e).replace("item 0 ", "", 1))
            else:
                item.setIcon(QIcon())
                item.setToolTip("")

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

    def value(self) -> List[str]:
        return [self.edit_control.item(i).text() for i in range(self.edit_control.count())]

    def set_value(self, value: Any) -> None:
        original_value = value
        if isinstance(value, str):
            # CLI and pre-GUI hook values arrive as a JSON array string.
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                value = None
        if not isinstance(value, (list, tuple)) or not all(isinstance(v, str) for v in value):
            # The value may come from an untrusted job bundle. Degrade gracefully like the
            # other controls instead of breaking the dialog.
            _logger.warning(
                "Job parameter %r has value %r that is not a list of strings; starting with an empty list.",
                self.name(),
                original_value,
            )
            value = []
        self._pending_new_item = None
        self._suppress_changes = True
        try:
            self.edit_control.clear()
            for text in value:
                self.edit_control.addItem(self._new_item(text))
        finally:
            self._suppress_changes = False
        self._on_list_changed()

    def connect_parameter_changed(self, callback):
        self._change_callbacks.append(callback)


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
        self.edit_control.interpretText()
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
        "BOOL",
        "RANGE_EXPR",
        "LIST[STRING]",
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
        elif self.job_template_parameter["type"] == "LIST[STRING]":
            if value == self.OPENJD_DEFAULT_VALUE:
                value = []
            else:
                try:
                    value = _validate_job_parameter_value(self.job_template_parameter, value)
                except (ValueError, TypeError):
                    # Keep the value as-is so submission reports it as an error.
                    pass
        self._value = value

    def connect_parameter_changed(self, callback):
        pass


class _JobTemplateGroupLayout(QGroupBox):
    def __init__(self, parent: QWidget, group_name: str):
        super().__init__(parent)
        self.setTitle(group_name)
        self.setLayout(QVBoxLayout())
