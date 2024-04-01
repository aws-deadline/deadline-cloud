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
    import shlex
    import sys
    from pathlib import Path

    import click

    _ensure_pyside()
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


def _ensure_pyside():
    """Attempts to ensure that pyside is available in the runtime environment.

    In a nutshell, it does this via 2 different, yet similar methods:
        * if it's a standard python installation:
            * start a sys.executable `python -m pip install` subprocess
        * if it's a pyinstaller build:
            * check PATH for python, and
            * use `/path/to/python -m to pip install` subprocess

    There's a lot of error-cases to potentially deal with:
        * does a python install exist
        * does the process have write access to the target location
        * is python an alias to the microsoft store
        * are there any other missing system libraries a user has to install
        * etc.

    And so attempts should be made here to give users the easiest path forward
    to setting up their environment.
    """
    import importlib
    from os.path import basename, dirname, join, normpath
    from pathlib import Path
    import shlex
    import shutil
    import subprocess
    import sys

    import click

    has_pyside = importlib.util.find_spec("PySide6") or importlib.util.find_spec("PySide2")
    if has_pyside:
        return

    message = (
        "Optional GUI components for deadline are unavailable. Would you like to install PySide?"
    )
    will_install_gui = click.confirm(message, default=False)
    if not will_install_gui:
        click.echo("Unable to continue without GUI, exiting")
        sys.exit(1)

    # TODO: swap to deadline[gui]=={this_client_version} once published
    pyside6_pypi = "PySide6-essentials==6.6.*"
    # Check if not pyinstaller (https://pyinstaller.org/en/stable/runtime-information.html)
    if not (getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS")):
        # standard python sys.executable
        # TODO: consider local editables `pip install .[gui]` for a dev env
        command = [sys.executable, "-m", "pip", "install", pyside6_pypi]
        if all(path in sys.executable.lower() for path in ("microsoft", "windowsapps")):
            # pip error when python installed from microsoft store:
            #     ERROR: Can not combine '--user' and '--target'
            # so we specify --no-user
            command += ["--no-user"]
        printed_command = " ".join(shlex.quote(v) for v in command)
        click.echo(f"running command: {printed_command}")
        subprocess.run(command)
        return

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
        "--target",
        deps_folder,
    ]
    # Linux, python may be the built-in python2, check for python3 first
    python_executable = shutil.which("python3") or shutil.which("python")
    if sys.platform == "win32":
        # reverse the order for Windows, since a standard install of python will have
        # python.exe, but not necessarily python3. python3 might still be an alias to
        # the windows store.
        python_executable = shutil.which("python") or shutil.which("python3")
        if python_executable and all(
            path in python_executable.lower() for path in ("microsoft", "windowsapps")
        ):
            # pip error when python installed from microsoft store:
            #     ERROR: Can not combine '--user' and '--target'
            # so we specify --no-user
            pip_command += ["--no-user"]

    if not python_executable:
        python = "python" if sys.platform == "win32" else "python3"
        command = [python] + pip_command
        printed_command = " ".join(shlex.quote(v) for v in command)
        if sys.platform == "win32":
            # windows definitely doesn't like shlex, ' != "
            printed_command = " ".join(command)
        click.echo(
            "Unable to install GUI dependencies, if you have python you can finish installing by running:"
        )
        click.echo()
        click.echo(f"\t{printed_command}")
        click.echo()
        sys.exit(1)

    command = [python_executable] + pip_command
    printed_command = " ".join(shlex.quote(v) for v in command)
    if sys.platform == "win32":
        # windows definitely doesn't like shlex, ' != "
        printed_command = " ".join(command)

    # Attempt to write to deps folder to ensure we have permission to do so
    test_file = Path(deps_folder) / "test_file"
    try:
        test_file.parent.mkdir(parents=True, exist_ok=True)
        test_file.touch()
    except Exception:
        click.echo(
            f"Unable to install GUI dependencies, you do not have the permissions to write to '{deps_folder}'."
        )
        click.echo(
            "You can finish the install by running the following command as a user who can write to that folder:"
        )
        click.echo()
        click.echo(f"\t{printed_command}")
        click.echo()
        sys.exit(1)
    else:
        test_file.unlink()

    click.echo(f"running command: {printed_command}")
    result = subprocess.run(command, capture_output=True, encoding="utf-8")

    if "run without arguments to install from the Microsoft Store".lower() in result.stderr.lower():
        click.echo(f"The python install, {python_executable}, is an alias to the Microsoft store.")
        click.echo(
            "To install AWS Deadline Cloud's GUI dependencies, install python and re-run this command."
        )
        sys.exit(1)

    if "ModuleNotFoundError: No module named 'encodings'".lower() in result.stderr.lower():
        # Occurred on deadline 0.46 - this seemed to happen when running on linux using /usr/bin/python3,
        # could not reproduce when built on the same machine the install fails on.
        # Debug info DOES indicate it's /usr/bin/python3 running and not happening within
        # the deadline executable. Running the command manually also appears to work fine
        click.echo("Unable to install GUI dependencies, you can fix this by running:")
        click.echo()
        click.echo(f"\t{printed_command}")
        click.echo()
        sys.exit(1)


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
