# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import runpy
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest


_SPEC = Path(__file__).absolute().parents[2] / "scripts/pyinstaller/deadline_cli.spec"


def _run_windows_spec(monkeypatch, binaries):
    hooks = SimpleNamespace(collect_all=Mock(return_value=([], [], [])))
    analysis = SimpleNamespace(
        binaries=binaries,
        datas=[],
        pure=[],
        scripts=[],
        exclude_system_libraries=Mock(),
    )
    namespace = {
        "Analysis": Mock(return_value=analysis),
        "PYZ": Mock(),
        "EXE": Mock(),
        "COLLECT": Mock(),
    }
    with monkeypatch.context() as context:
        context.setitem(sys.modules, "PyInstaller.utils.hooks", hooks)
        context.setattr("sys.platform", "win32")
        runpy.run_path(str(_SPEC), init_globals=namespace)
    return analysis


def test_windows_spec_keeps_existing_stable_abi_dll(monkeypatch, tmp_path):
    dll = tmp_path / "python3.dll"
    dll.touch()
    binaries = [("python3.dll", str(dll), "BINARY")]

    analysis = _run_windows_spec(monkeypatch, binaries)

    assert analysis.binaries == binaries
    assert len(analysis.binaries) == 1


@pytest.mark.parametrize("minor", ["313", "314"])
def test_windows_spec_adds_stable_abi_dll_beside_runtime(monkeypatch, tmp_path, minor):
    runtime = tmp_path / f"python{minor}.dll"
    stable_abi = tmp_path / "python3.dll"
    runtime.touch()
    stable_abi.touch()

    analysis = _run_windows_spec(monkeypatch, [(runtime.name, str(runtime), "BINARY")])

    assert ("python3.dll", str(stable_abi), "BINARY") in analysis.binaries


def test_windows_spec_rejects_missing_stable_abi_dll(monkeypatch, tmp_path):
    runtime = tmp_path / "python314.dll"
    runtime.touch()

    with pytest.raises(RuntimeError, match="python3.dll is required"):
        _run_windows_spec(monkeypatch, [(runtime.name, str(runtime), "BINARY")])
