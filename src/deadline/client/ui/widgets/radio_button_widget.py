# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from typing import Optional

from qtpy.QtCore import Qt  # type: ignore
from qtpy.QtWidgets import QRadioButton, QWidget  # type: ignore


_HOVER_STYLE = """
QRadioButton {
    padding: 2px 4px;
    border-radius: 3px;
}
QRadioButton:hover {
    background-color: palette(midlight);
}
"""


class HoverRadioButton(QRadioButton):
    """
    A QRadioButton with a subtle background highlight on hover
    and a pointing-hand cursor.
    """

    def __init__(self, text: str = "", parent: Optional[QWidget] = None):
        super().__init__(text, parent)
        self.setCursor(Qt.PointingHandCursor)
        self.setStyleSheet(_HOVER_STYLE)
