# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Expandable section widget — a clickable header that toggles content visibility.
Similar to Cloudscape's ExpandableSection component.
"""

from __future__ import annotations

from qtpy.QtCore import Qt, QSize, Signal  # type: ignore
from qtpy.QtWidgets import QToolButton, QVBoxLayout, QWidget  # type: ignore


class ExpandableSection(QWidget):
    """A collapsible section with a toggle arrow and title.

    The arrow and title act as a single clickable header. Clicking toggles
    the visibility of the content widget.

    Example::

        section = ExpandableSection("Parameters (5)")
        section.set_content(my_table_widget)
        layout.addWidget(section)
    """

    toggled = Signal(bool)

    def __init__(
        self,
        title: str = "",
        expanded: bool = False,
        disable_content_paddings: bool = False,
        parent=None,
    ):
        super().__init__(parent)
        self._layout = QVBoxLayout(self)
        self._layout.setContentsMargins(0, 0, 0, 0)
        self._layout.setSpacing(0)
        self._disable_content_paddings = disable_content_paddings

        self._header = QToolButton()
        self._header.setStyleSheet("QToolButton { border: none; padding: 0px; }")
        self._header.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
        self._header.setIconSize(QSize(7, 7))
        self._header.setCheckable(True)
        self._header.setChecked(expanded)
        self._header.setArrowType(Qt.DownArrow if expanded else Qt.RightArrow)
        if title:
            self._header.setText(f" {title}")
        self._header.toggled.connect(self._on_toggled)
        self._layout.addWidget(self._header)

        self._content: QWidget | None = None

    def set_title(self, title: str):
        """Update the section header text."""
        self._header.setText(f" {title}")

    def set_header_style(self, qss: str):
        """Set custom stylesheet on the header button."""
        self._header.setStyleSheet(f"QToolButton {{ border: none; padding: 0px; {qss} }}")

    def set_content(self, widget: QWidget):
        """Set the content widget that will be shown/hidden."""
        old = self._content
        if old is not None:
            self._layout.removeWidget(old)
        self._content = widget
        if not self._disable_content_paddings:
            widget.setContentsMargins(16, 4, 0, 0)
        widget.setVisible(self._header.isChecked())
        self._layout.addWidget(widget)

    def is_expanded(self) -> bool:
        return self._header.isChecked()

    def set_expanded(self, expanded: bool):
        self._header.setChecked(expanded)

    def _on_toggled(self, checked: bool):
        self._header.setArrowType(Qt.DownArrow if checked else Qt.RightArrow)
        if self._content is not None:
            self._content.setVisible(checked)
        self.toggled.emit(checked)
