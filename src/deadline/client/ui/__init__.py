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
        from PySide2.QtWidgets import QMessageBox

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

        app.exec_()
    """
    import sys
    from pathlib import Path

    import click

    try:
        from PySide2.QtGui import QIcon
        from PySide2.QtWidgets import QApplication, QMessageBox
    except ImportError as e:
        click.echo(f"Failed to import PySide2, which is required to show the GUI:\n{e}")
        sys.exit(1)

    try:
        app = QApplication(sys.argv)
        icon = QIcon(str(Path(__file__).parent.parent / "ui" / "resources" / "deadline_logo.svg"))
        app.setWindowIcon(icon)

        yield app
    except DeadlineOperationError as e:
        import os
        import shlex

        command = f"{os.path.basename(sys.argv[0])} " + " ".join(
            shlex.quote(v) for v in sys.argv[1:]
        )
        QMessageBox.warning(None, f'Error running "{command}"', str(e))
    except Exception:
        import os
        import shlex
        import traceback

        command = f"{os.path.basename(sys.argv[0])} " + " ".join(
            shlex.quote(v) for v in sys.argv[1:]
        )
        QMessageBox.warning(
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
