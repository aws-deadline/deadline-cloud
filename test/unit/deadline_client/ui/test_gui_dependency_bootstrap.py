# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import importlib.util
import sys
from unittest.mock import Mock

from packaging.requirements import Requirement
import pytest
from qtpy import QtWidgets

from deadline.client.ui._utils import gui_context_for_cli


@pytest.mark.parametrize("frozen", [False, True])
def test_gui_bootstrap_accepts_python_314_compatible_qt(monkeypatch, frozen):
    real_find_spec = importlib.util.find_spec
    monkeypatch.setattr(
        importlib.util,
        "find_spec",
        lambda name: None if name in ("PySide6", "PySide2") else real_find_spec(name),
    )
    monkeypatch.setattr(sys, "executable", "/tmp/deadline" if frozen else "/tmp/python")
    monkeypatch.setattr("shutil.which", lambda _: "/tmp/external-python")
    run = Mock()
    monkeypatch.setattr("subprocess.run", run)
    application = Mock()
    monkeypatch.setattr(QtWidgets, "QApplication", Mock(return_value=application))

    with gui_context_for_cli(automatically_install_dependencies=True) as app:
        assert app is application

    command = run.call_args.args[0]
    requirement = Requirement(command[4])
    assert requirement.name.lower() == "pyside6-essentials"
    assert "6.10.3" in requirement.specifier
    assert "6.12.0" not in requirement.specifier
    if frozen:
        assert command[command.index("--python-version") + 1] == (
            f"{sys.version_info.major}.{sys.version_info.minor}"
        )
        assert "--only-binary=:all:" in command
