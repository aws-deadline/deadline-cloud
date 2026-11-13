# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for _apply_macos_dark_input_contrast, which keeps text inputs visible on macOS dark mode."""

from unittest.mock import patch

import pytest
from qtpy.QtGui import QColor, QPalette
from qtpy.QtWidgets import (
    QApplication,
    QDialog,
    QGroupBox,
    QLineEdit,
    QPlainTextEdit,
    QSpinBox,
    QVBoxLayout,
)

from deadline.client.ui import _utils
from deadline.client.ui._utils import _apply_macos_dark_input_contrast

# Native macOS dark-mode colors: the text-field Base is darker than the window, so empty
# fields vanish into the dialog's tab/scroll panes.
_DARK_WINDOW = QColor("#323232")
_DARK_BASE = QColor("#171717")
_LIGHT_WINDOW = QColor("#ececec")
_LIGHT_BASE = QColor("#ffffff")

_BASE = QPalette.ColorRole.Base
_WINDOW = QPalette.ColorRole.Window


def _palette(window: QColor, base: QColor) -> QPalette:
    pal = QPalette(QApplication.palette())
    pal.setColor(_WINDOW, window)
    pal.setColor(_BASE, base)
    return pal


@pytest.fixture
def app_palette(qapp):
    """Set the application palette the way a system light/dark switch would."""

    def set_palette(palette: QPalette) -> None:
        qapp.setPalette(palette)
        # ApplicationPaletteChange is posted, not sent; let the event loop deliver it.
        qapp.processEvents()

    original = QPalette(qapp.palette())
    original_qss = qapp.styleSheet()
    yield set_palette
    qapp.setStyleSheet(original_qss)
    set_palette(original)


@pytest.fixture
def macos():
    with patch.object(_utils, "_uses_macos_style", return_value=True):
        yield


def _dialog(qtbot):
    dialog = QDialog()
    line_edit = QLineEdit(dialog)
    QVBoxLayout(dialog).addWidget(line_edit)
    qtbot.addWidget(dialog)
    return dialog, line_edit


def _base(widget) -> QColor:
    """The fill color the input actually paints, sampled from its empty center.

    Read from pixels rather than widget.palette(): under a stylesheet Qt doesn't always
    write the painted background back into the palette.
    """
    widget.window().resize(320, 240)
    widget.window().layout().activate()
    image = widget.grab().toImage()
    return image.pixelColor(image.width() // 2, image.height() // 2)


def _is_lifted(widget) -> bool:
    return _base(widget).lightness() > _DARK_WINDOW.lightness()


def test_lifts_input_base_above_window_on_macos_dark(qtbot, app_palette, macos):
    app_palette(_palette(_DARK_WINDOW, _DARK_BASE))
    dialog, line_edit = _dialog(qtbot)
    spin, text = QSpinBox(), QPlainTextEdit()
    dialog.layout().addWidget(spin)
    dialog.layout().addWidget(text)

    _apply_macos_dark_input_contrast(dialog)

    assert _is_lifted(line_edit) and _is_lifted(spin) and _is_lifted(text)


@pytest.mark.parametrize(
    "is_macos, window, base",
    [
        pytest.param(False, _DARK_WINDOW, _DARK_BASE, id="other-style-dark"),
        pytest.param(True, _LIGHT_WINDOW, _LIGHT_BASE, id="macos-light"),
        # A host's own dark theme that already draws inputs lighter: never darken it.
        pytest.param(True, QColor("#202020"), QColor("#505050"), id="host-base-lighter"),
    ],
)
def test_leaves_inputs_alone_when_already_visible(qtbot, app_palette, is_macos, window, base):
    app_palette(_palette(window, base))
    with patch.object(_utils, "_uses_macos_style", return_value=is_macos):
        dialog, line_edit = _dialog(qtbot)
        _apply_macos_dark_input_contrast(dialog)

    assert dialog.styleSheet() == ""
    assert _base(line_edit) == base


def test_reaches_inputs_under_a_nested_stylesheet(qtbot, app_palette, macos):
    """The config dialog styles its group boxes; inputs inside must still be lifted."""
    app_palette(_palette(_DARK_WINDOW, _DARK_BASE))
    dialog, _ = _dialog(qtbot)
    group = QGroupBox()
    group.setStyleSheet("QGroupBox { font-weight: bold; }")
    nested = QLineEdit(group)
    QVBoxLayout(group).addWidget(nested)
    dialog.layout().addWidget(group)

    _apply_macos_dark_input_contrast(dialog)

    assert _is_lifted(nested)


def test_widgets_added_later_are_lifted(qtbot, app_palette, macos):
    """Host submitters add their own widgets after the dialog is constructed."""
    app_palette(_palette(_DARK_WINDOW, _DARK_BASE))
    dialog, _ = _dialog(qtbot)
    _apply_macos_dark_input_contrast(dialog)
    late = QLineEdit()
    dialog.layout().addWidget(late)

    assert _is_lifted(late)


def test_an_inputs_own_stylesheet_still_wins(qtbot, app_palette, macos):
    """e.g. the job timeouts widget paints invalid spin boxes red."""
    app_palette(_palette(_DARK_WINDOW, _DARK_BASE))
    dialog, _ = _dialog(qtbot)
    spin = QSpinBox()
    spin.setStyleSheet("QSpinBox { background-color: #550000; }")
    dialog.layout().addWidget(spin)

    _apply_macos_dark_input_contrast(dialog)

    assert _base(spin) == QColor("#550000")


def test_preserves_and_survives_other_dialog_stylesheets(qtbot, app_palette, macos):
    app_palette(_palette(_DARK_WINDOW, _DARK_BASE))
    dialog, line_edit = _dialog(qtbot)
    dialog.setStyleSheet("QLabel { color: red; }")
    _apply_macos_dark_input_contrast(dialog)
    assert "QLabel { color: red; }" in dialog.styleSheet()
    assert _is_lifted(line_edit)

    # Replacing the dialog's stylesheet later (StyleChange) re-adds the rule once.
    dialog.setStyleSheet("QPushButton { color: red; }")
    assert "QPushButton { color: red; }" in dialog.styleSheet()
    assert dialog.styleSheet().count(_utils._INPUT_CONTRAST_BEGIN) == 1
    assert _is_lifted(line_edit)

    # Switching to light mode removes only this helper's rule.
    app_palette(_palette(_LIGHT_WINDOW, _LIGHT_BASE))
    assert dialog.styleSheet() == "QPushButton { color: red; }"


@pytest.mark.parametrize(
    "host_qss",
    [
        pytest.param("QLineEdit { background-color: #224422; }", id="styles-inputs"),
        # e.g. Maya on macOS: its own style, dark palette, an app stylesheet that never
        # names the input classes. The host themes itself, so leave it alone.
        pytest.param("QToolTip { color: white; }", id="unrelated-rules"),
    ],
)
def test_defers_to_any_host_app_stylesheet(qtbot, app_palette, qapp, host_qss):
    app_palette(_palette(_DARK_WINDOW, _DARK_BASE))
    # Even if the style underneath were native macOS, it can't be identified any more.
    with patch.object(QApplication.style(), "name", return_value="macos"):
        qapp.setStyleSheet(host_qss)
        dialog, _ = _dialog(qtbot)
        _apply_macos_dark_input_contrast(dialog)

    assert dialog.styleSheet() == ""


def test_native_style_found_beneath_dialog_stylesheets(qtbot, app_palette):
    """Without a host app stylesheet, a dialog's own stylesheet doesn't hide the style."""
    app_palette(_palette(_DARK_WINDOW, _DARK_BASE))
    with patch.object(QApplication.style(), "name", return_value="macos"):
        dialog, line_edit = _dialog(qtbot)
        dialog.setStyleSheet("QLabel { color: red; }")
        _apply_macos_dark_input_contrast(dialog)
        assert _is_lifted(line_edit)

        # A host replacing the dialog stylesheet later re-runs detection; still found.
        dialog.setStyleSheet("QPushButton { color: red; }")
        assert dialog.styleSheet().count(_utils._INPUT_CONTRAST_BEGIN) == 1
        assert _is_lifted(line_edit)


def test_follows_system_light_dark_switches(qtbot, app_palette, macos):
    app_palette(_palette(_DARK_WINDOW, _DARK_BASE))
    dialog, line_edit = _dialog(qtbot)
    _apply_macos_dark_input_contrast(dialog)
    assert _is_lifted(line_edit)

    # Light mode: the rule is removed and inputs follow the system again.
    app_palette(_palette(_LIGHT_WINDOW, _LIGHT_BASE))
    assert dialog.styleSheet() == ""
    assert _base(line_edit) == _LIGHT_BASE

    # A later light palette (e.g. an accessibility contrast change) is still tracked.
    app_palette(_palette(_LIGHT_WINDOW, QColor("#fafafa")))
    assert _base(line_edit) == QColor("#fafafa")

    # And dark again re-applies the lift.
    app_palette(_palette(_DARK_WINDOW, _DARK_BASE))
    assert _is_lifted(line_edit)


def test_applying_twice_is_a_no_op(qtbot, app_palette, macos):
    app_palette(_palette(_DARK_WINDOW, _DARK_BASE))
    dialog, _ = _dialog(qtbot)
    _apply_macos_dark_input_contrast(dialog)
    _apply_macos_dark_input_contrast(dialog)

    assert dialog.styleSheet().count(_utils._INPUT_CONTRAST_BEGIN) == 1


@pytest.mark.parametrize(
    "name, expected", [("macos", True), ("Fusion", False), ("windows11", False)]
)
def test_uses_macos_style(qtbot, name, expected):
    dialog, _ = _dialog(qtbot)
    with patch.object(dialog.style(), "name", return_value=name, create=True):
        assert _utils._uses_macos_style(dialog) is expected


@pytest.mark.parametrize("app_style, expected", [("macos", True), ("fusion", False)])
def test_widget_stylesheet_falls_back_to_the_app_style(qtbot, app_style, expected):
    """A widget stylesheet wraps the real style and blanks its name; read the app's."""
    dialog, _ = _dialog(qtbot)
    dialog.setStyleSheet("QPushButton { color: red; }")
    assert dialog.style().metaObject().className() == "QStyleSheetStyle"
    with patch.object(QApplication.style(), "name", return_value=app_style):
        assert _utils._uses_macos_style(dialog) is expected


def test_app_stylesheet_means_not_native(qtbot, qapp):
    original = qapp.styleSheet()
    try:
        qapp.setStyleSheet("QToolTip { color: white; }")
        dialog, _ = _dialog(qtbot)
        assert _utils._uses_macos_style(dialog) is False
    finally:
        qapp.setStyleSheet(original)


def test_uses_macos_style_qt5_object_name_fallback():
    """Qt 5 has no QStyle.name(); its macOS style's objectName is "macintosh"."""

    class _Qt5Style:
        def metaObject(self):
            return type("_Meta", (), {"className": lambda self: "QMacStyle"})()

        def objectName(self):
            return "macintosh"

    widget = type("_Widget", (), {"style": lambda self: _Qt5Style()})()
    assert _utils._uses_macos_style(widget) is True


def test_proxy_wrapped_native_style_is_detected(qtbot):
    from qtpy.QtWidgets import QProxyStyle, QStyleFactory

    dialog, _ = _dialog(qtbot)
    base = QStyleFactory.create("fusion")
    proxy = QProxyStyle(base)
    dialog.setStyle(proxy)
    assert _utils._uses_macos_style(dialog) is False
    with patch.object(proxy.baseStyle(), "name", return_value="macos"):
        assert _utils._uses_macos_style(dialog) is True


def test_own_style_hidden_by_a_stylesheet_is_not_guessed(qtbot):
    """A dialog with its own setStyle() isn't assumed to share the app's native style."""
    from qtpy.QtWidgets import QStyleFactory

    dialog, _ = _dialog(qtbot)
    own_style = QStyleFactory.create("fusion")
    dialog.setStyle(own_style)
    dialog.setStyleSheet("QLabel { color: red; }")
    with patch.object(QApplication.style(), "name", return_value="macos"):
        assert _utils._uses_macos_style(dialog) is False


def test_stray_marker_text_in_other_stylesheets_is_preserved(qtbot, app_palette, macos):
    app_palette(_palette(_DARK_WINDOW, _DARK_BASE))
    dialog, _ = _dialog(qtbot)
    user_qss = f"{_utils._INPUT_CONTRAST_BEGIN}\nQLabel {{ color: red; }}"
    dialog.setStyleSheet(user_qss)
    _apply_macos_dark_input_contrast(dialog)
    # Force a refresh after the helper's own block has been appended.
    dialog.setStyleSheet(dialog.styleSheet())
    _utils._refresh_input_contrast(dialog)

    assert dialog.styleSheet().startswith(user_qss)
    assert dialog.styleSheet().count(_utils._INPUT_CONTRAST_END) == 1
