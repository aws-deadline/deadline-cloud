# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from contextlib import contextmanager
from functools import lru_cache
from typing import Any, Dict, TYPE_CHECKING
import json
import locale as locale_module
from pathlib import Path

from ..exceptions import DeadlineOperationError
from ..config import config_file

# Import TranslationKey type only during type checking to avoid runtime errors
# if _translation_keys.py doesn't exist (it's generated during build)
if TYPE_CHECKING:
    from ._translation_keys import TranslationKey
else:
    TranslationKey = str

_LD_LIBRARY_PATH = "LD_LIBRARY_PATH"
_LD_LIBRARY_PATH_ORIG = "LD_LIBRARY_PATH_ORIG"


@lru_cache(maxsize=1)
def _get_translations() -> Dict[str, str]:
    """Load UI translations from locale-specific JSON."""

    # Check config setting first, then fall back to system locale
    current_locale = config_file.get_setting("settings.locale")
    if not current_locale:
        current_locale, _ = locale_module.getdefaultlocale()
    if not current_locale:
        current_locale = "en_US"

    # Try locale-specific file, fallback to en_US
    translations_dir = Path(__file__).parent / "translations" / "locales"
    locale_file = translations_dir / f"{current_locale}.json"
    if not locale_file.exists():
        locale_file = translations_dir / "en_US.json"

    try:
        # These catalogs are UTF-8, but open() defaults to the platform encoding, which is
        # cp1252 on Windows. Without this, the CJK catalogs raise UnicodeDecodeError and every
        # string falls back to its key, while the en_US bullets (U+2022) decode to mojibake
        # instead of failing outright, so the key lookup misses and the mangled text renders.
        with open(locale_file, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def tr(text: TranslationKey) -> str:
    """Translate text using JSON translations."""
    return _get_translations().get(text, text)


def warning_banner_qss(widget) -> str:
    """Stylesheet for an inline warning banner that adapts to the active theme.

    The submitter has no in-app theme toggle — it inherits the OS/Qt palette — so a
    hardcoded light-amber banner looks out of place in dark mode. This derives a
    light- or dark-amber treatment from the widget's palette. Both pass WCAG AA
    contrast for the body text (light 7.35:1, dark 9.35:1).
    """
    from qtpy.QtGui import QPalette  # type: ignore

    is_dark = widget.palette().color(QPalette.Window).lightness() < 128
    if is_dark:
        text, bg, border = "#ffcc80", "#3a2a10", "#8a5a20"
    else:
        text, bg, border = "#7a4200", "#fff3e0", "#e0a040"
    return (
        f"QLabel {{ color: {text}; background-color: {bg};"
        f" border: 1px solid {border}; border-radius: 4px;"
        " padding: 4px 8px; }"
    )


# How much lighter than the window background text-input fields are drawn in macOS dark
# mode, on Qt's 0-255 HSL lightness scale.
_MACOS_DARK_INPUT_BASE_LIFT = 12
_MACOS_STYLE_NAMES = ("macos", "macintosh")  # Qt 6 / Qt 5 names of the native macOS style
# Text-input classes the native macOS style fills with QPalette.Base. Editable combo boxes
# are left out: a stylesheet background drops them to non-native rendering, and their
# bezel and arrow button already keep them visible.
_TEXT_INPUT_CLASSES = ("QLineEdit", "QAbstractSpinBox", "QTextEdit", "QPlainTextEdit")
# Wraps the rule this helper adds to a widget's stylesheet, so it can be found and replaced
# without disturbing anything else in that stylesheet.
_INPUT_CONTRAST_BEGIN = "/* deadline-cloud: macos-dark-input-contrast { */"
_INPUT_CONTRAST_END = "/* } deadline-cloud: macos-dark-input-contrast */"


def _uses_macos_style(widget) -> bool:
    style = widget.style()
    if style.metaObject().className() == "QStyleSheetStyle":
        # Any app- or widget-level stylesheet wraps the real style and hides its name, but
        # the native style still draws unstyled inputs. It is the default on macOS, so
        # assume it there. A wrong guess only ever raises input contrast (see the
        # Base-darker-than-window check in _refresh_input_contrast).
        import sys

        return sys.platform == "darwin"
    name = style.name() if hasattr(style, "name") else style.objectName()
    return (name or "").lower() in _MACOS_STYLE_NAMES


def _input_contrast_rule(widget) -> str:
    """The stylesheet rule ``widget`` needs, or "" when its inputs are already visible."""
    from qtpy.QtGui import QColor, QPalette  # type: ignore
    from qtpy.QtWidgets import QApplication  # type: ignore

    app = QApplication.instance()
    host_qss = app.styleSheet() if app is not None else ""
    if any(cls in host_qss for cls in _TEXT_INPUT_CLASSES + ("QSpinBox",)):
        return ""  # The host themes its own inputs; don't override its choice.

    palette = QApplication.palette(widget)
    window = palette.color(QPalette.Window)
    lifted_lightness = min(255, window.lightness() + _MACOS_DARK_INPUT_BASE_LIFT)
    if not (
        _uses_macos_style(widget)
        and window.lightness() < 128
        # A host's own dark theme may already draw inputs lighter; never reduce that.
        and palette.color(QPalette.Base).lightness() < lifted_lightness
    ):
        return ""

    lifted = QColor.fromHsl(window.hslHue(), window.hslSaturation(), lifted_lightness)
    # background-color alone keeps the native bezel, focus ring, and spin arrows: Qt hands
    # the color to the native style as Base. Adding a border would switch to plain drawing.
    return (
        f"{_INPUT_CONTRAST_BEGIN}\n"
        f"{', '.join(_TEXT_INPUT_CLASSES)} {{ background-color: {lifted.name()}; }}\n"
        f"{_INPUT_CONTRAST_END}"
    )


def _refresh_input_contrast(widget) -> None:
    current = widget.styleSheet()
    start = current.find(_INPUT_CONTRAST_BEGIN)
    end = current.find(_INPUT_CONTRAST_END)
    if start != -1 and end != -1:
        others = (current[:start] + current[end + len(_INPUT_CONTRAST_END) :]).strip()
    else:
        others = current.strip()

    rule = _input_contrast_rule(widget)
    updated = "\n".join(part for part in (others, rule) if part)
    if updated != current:
        from qtpy.QtWidgets import QWidget  # type: ignore

        # Replacing one non-empty stylesheet with another on a dialog that isn't shown
        # yet can leave its unpolished children painting with stale cached rules (seen
        # with Qt 6.8 and 6.10 once any stylesheet has been used in the process).
        # Polishing the tree first, then clearing before setting, avoids that.
        widget.ensurePolished()
        for child in widget.findChildren(QWidget):
            child.ensurePolished()
        # setStyleSheet sends StyleChange, which calls back in here; don't recurse.
        widget._deadline_input_contrast_updating = True
        try:
            widget.setStyleSheet("")
            widget.setStyleSheet(updated)
        finally:
            widget._deadline_input_contrast_updating = False


def apply_macos_dark_input_contrast(widget) -> None:
    """Keep text inputs visible under the native macOS Qt style in dark mode.

    In dark mode the native macOS style fills QLineEdit/QSpinBox/QTextEdit with
    ``QPalette.Base``, which is darker than the window, and draws only a faint
    near-black bezel. Inside the submitter's tab and scroll-area panes that makes
    empty fields look like blank space, and users don't realize they can type
    there. Combo boxes are unaffected because the style draws them as raised buttons.

    This adds a background-only rule to ``widget``'s stylesheet that draws those
    inputs slightly lighter than the window, while the native style still draws
    their bezel, focus ring, and spin arrows. A stylesheet is used rather than a
    palette because Qt's stylesheet style re-polishes every widget under any
    stylesheet (the config dialog's group boxes, a host's app stylesheet) from the
    application palette, which would discard a palette set on the dialog.

    It does nothing for other styles, in light mode, or when the host's app
    stylesheet already styles inputs, so hosts with their own Qt theme (Maya,
    Houdini, Nuke, ...) are unaffected. It re-evaluates when the system palette or
    the style changes, for example on a light/dark switch, or when someone replaces the
    dialog's stylesheet.
    """
    from qtpy.QtCore import QEvent, QObject  # type: ignore

    class _InputContrastFilter(QObject):
        def eventFilter(self, obj, event):  # noqa: N802 (Qt override)
            if event.type() in (
                QEvent.ApplicationPaletteChange,
                QEvent.StyleChange,
            ) and not getattr(obj, "_deadline_input_contrast_updating", False):
                _refresh_input_contrast(obj)
            return False

    if getattr(widget, "_deadline_input_contrast_filter", None) is not None:
        return  # already applied

    _refresh_input_contrast(widget)
    # Qt parenting alone doesn't keep the Python side of the filter alive under PySide:
    # without this reference it is garbage collected and never sees a palette change.
    widget._deadline_input_contrast_filter = _InputContrastFilter(widget)
    widget.installEventFilter(widget._deadline_input_contrast_filter)


@contextmanager
def block_signals(element):
    """
    Context manager used to turn off signals for a UI element.
    """
    old_value = element.blockSignals(True)
    try:
        yield
    finally:
        element.blockSignals(old_value)


@contextmanager
def gui_error_handler(message_title: str, parent: Any = None):
    """
    A context manager that initializes a Qt GUI context that
    catches errors and shows them in a message box instead of
    punting them to a CLI interface.

    For example:

    with gui_context():
        from deadline.client.ui.cli_job_submitter import show_cli_job_submitter

        show_cli_job_submitter()

    """
    import click

    try:
        from qtpy.QtWidgets import QMessageBox
    except ImportError as e:
        click.echo(f"Failed to import qtpy/PySide/Qt, which is required to show the GUI:\n{e}")
        raise

    try:
        yield
    except DeadlineOperationError as e:
        QMessageBox.warning(parent, message_title, str(e))  # type: ignore[call-arg]
    except Exception:
        import traceback

        QMessageBox.warning(parent, message_title, f"Exception caught:\n{traceback.format_exc()}")  # type: ignore[call-arg]


@contextmanager
def gui_context_for_cli(automatically_install_dependencies: bool):
    """
    A context manager that initializes a Qt GUI context for
    the CLI handler to use.

    For example:

    with gui_context_for_cli(automatically_install_dependencies) as app:
        from deadline.client.ui.cli_job_submitter import show_cli_job_submitter

        show_cli_job_submitter()

        app.exec()

    If automatically_install_dependencies is true, dependencies will be installed without prompting the user. Useful
    if the command is triggered not from a command line.
    """
    import importlib
    import os
    from os.path import basename, dirname, join, normpath
    import shlex
    import shutil
    import subprocess
    import sys
    from pathlib import Path

    import click

    has_pyside6 = importlib.util.find_spec("PySide6")
    has_pyside2 = importlib.util.find_spec("PySide2")
    if not (has_pyside6 or has_pyside2):
        if not automatically_install_dependencies:
            message = "Optional GUI components for deadline are unavailable. Would you like to install PySide?"
            will_install_gui = click.confirm(message, default=False)
            if not will_install_gui:
                click.echo("Unable to continue without GUI, exiting")
                sys.exit(1)

        # this should match what's in the pyproject.toml
        pyside6_pypi = "PySide6-essentials >= 6.6,< 6.9"
        if "deadline" in basename(sys.executable).lower():
            # running with a deadline executable, not standard python.
            # So exit the deadline folder into the main deps dir
            deps_folder = normpath(
                join(
                    dirname(__file__),
                    "..",
                    "..",
                    "..",
                )
            )
            runtime_version = f"{sys.version_info.major}.{sys.version_info.minor}"
            pip_command = [
                "-m",
                "pip",
                "install",
                pyside6_pypi,
                "--python-version",
                runtime_version,
                "--only-binary=:all:",
                "-t",
                deps_folder,
            ]
            python_executable = shutil.which("python3") or shutil.which("python")
            if python_executable:
                # https://pyinstaller.org/en/stable/common-issues-and-pitfalls.html#linux-and-unix-like-oses
                env = os.environ.copy()
                if os.name != "nt":
                    if env.get(_LD_LIBRARY_PATH_ORIG) is not None:
                        env[_LD_LIBRARY_PATH] = env[_LD_LIBRARY_PATH_ORIG]
                    else:
                        # This happens when LD_LIBRARY_PATH was not set.
                        env.pop(_LD_LIBRARY_PATH, None)

                subprocess.run([python_executable] + pip_command, env=env)
            else:
                click.echo(
                    "Unable to install GUI dependencies, if you have python available you can install it by running:"
                )
                click.echo()
                click.echo(f"\t{' '.join(shlex.quote(v) for v in ['python'] + pip_command)}")
                click.echo()
                sys.exit(1)
        else:
            # standard python sys.executable
            # TODO: swap to deadline[gui]==version once published and at the same
            # time consider local editables `pip install .[gui]`
            subprocess.run([sys.executable, "-m", "pip", "install", pyside6_pypi])

    # set QT_API to inform qtpy which dependencies to look for.
    # default to pyside6 and fallback to pyside2.
    # Does not work with PyQt5 which is qtpy default
    os.environ["QT_API"] = "pyside6"
    if has_pyside2:
        os.environ["QT_API"] = "pyside2"
    try:
        from qtpy.QtGui import QIcon
        from qtpy.QtWidgets import QApplication, QMessageBox
    except ImportError as e:
        click.echo(f"Failed to import qtpy/PySide/Qt, which is required to show the GUI:\n{e}")
        sys.exit(1)

    try:
        app = QApplication(sys.argv)
        app.setApplicationName("AWS Deadline Cloud")
        icon = QIcon(str(Path(__file__).parent.parent / "ui" / "resources" / "deadline_logo.svg"))
        app.setWindowIcon(icon)

        yield app
    except DeadlineOperationError as e:
        import os
        import shlex

        command = f"{os.path.basename(sys.argv[0])} " + " ".join(
            shlex.quote(v) for v in sys.argv[1:]
        )
        QMessageBox.warning(None, f'Error running "{command}"', str(e))  # type: ignore[call-overload, call-arg, arg-type]
    except Exception:
        import os
        import shlex
        import traceback

        command = f"{os.path.basename(sys.argv[0])} " + " ".join(
            shlex.quote(v) for v in sys.argv[1:]
        )
        QMessageBox.warning(  # type: ignore[call-overload, call-arg]
            None,  # type: ignore[arg-type]
            f'Error running "{command}"',
            f"Exception caught:\n{traceback.format_exc()}",
        )


class CancelationFlag:
    """
    Helper object for background thread cancelation.
    The `destroyed` event cannot be connected to a member
    function of the class. With this object, you can bind it
    to the cancelation flag's set_canceled method instead.

    .. deprecated::
        This class is deprecated and will be removed in a future release.
        Use Qt's native threading mechanisms instead:
        - For simple async operations, use `AsyncTaskRunner` from
          `deadline.client.ui.controllers`
        - For complex operations with progress callbacks, use a
          `QThread` subclass with signals

    Example usage:

    class MyWidget(QWidget):
        thread_event = Signal(str)

        def __init__(self):
            self.canceled = CancelationFlag()
            self.destroyed.connect(self.canceled.set_canceled)

        def _my_thread_function(self):
            ...<processing>...

            if not self.canceled:
                self.thread_event.emit(result)
    """

    def __init__(self):
        import warnings

        warnings.warn(
            "CancelationFlag is deprecated and will be removed in a future release. "
            "Use AsyncTaskRunner from deadline.client.ui.controllers for simple async operations, "
            "or a QThread subclass with signals for complex operations with progress callbacks.",
            DeprecationWarning,
            stacklevel=2,
        )
        self.canceled = False

    def set_canceled(self):
        self.canceled = True

    def __bool__(self):
        return self.canceled
