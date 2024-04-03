# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from contextlib import contextmanager
from typing import Any

from ..exceptions import DeadlineOperationError


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
    try:
        from qtpy.QtWidgets import QMessageBox

        yield
    except DeadlineOperationError as e:
        QMessageBox.warning(parent, message_title, str(e))
    except Exception:
        import traceback

        QMessageBox.warning(parent, message_title, f"Exception caught:\n{traceback.format_exc()}")


@contextmanager
def gui_context_for_cli():
    """
    A context manager that initializes a Qt GUI context for
    the CLI handler to use.

    For example:

    with gui_context_for_cli() as app:
        from deadline.client.ui.cli_job_submitter import show_cli_job_submitter

        show_cli_job_submitter()

        app.exec()
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
        message = "Optional GUI components for deadline are unavailable. Would you like to install PySide?"
        will_install_gui = click.confirm(message, default=False)
        if not will_install_gui:
            click.echo("Unable to continue without GUI, exiting")
            sys.exit(1)

        # this should match what's in the pyproject.toml
        pyside6_pypi = "PySide6-essentials==6.6.*"
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
                command = " ".join(shlex.quote(v) for v in [python_executable] + pip_command)
                subprocess.run([python_executable] + pip_command)
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
        QMessageBox.warning(None, f'Error running "{command}"', str(e))  # type: ignore[call-overload]
    except Exception:
        import os
        import shlex
        import traceback

        command = f"{os.path.basename(sys.argv[0])} " + " ".join(
            shlex.quote(v) for v in sys.argv[1:]
        )
        QMessageBox.warning(  # type: ignore[call-overload]
            None, f'Error running "{command}"', f"Exception caught:\n{traceback.format_exc()}"
        )


class CancelationFlag:
    """
    Helper object for background thread cancelation.
    The `destroyed` event cannot be connected to a member
    function of the class. With this object, you can bind it
    to the cancelation flag's set_canceled method instead.

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
        self.canceled = False

    def set_canceled(self):
        self.canceled = True

    def __bool__(self):
        return self.canceled
