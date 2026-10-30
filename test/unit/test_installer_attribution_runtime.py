# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import importlib.util
from pathlib import Path
import sys
from unittest.mock import Mock

import pytest


@pytest.fixture
def attributions():
    path = Path(__file__).absolute().parents[2] / "scripts/attributions/cli.py"
    spec = importlib.util.spec_from_file_location("_installer_attributions", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("platform", ["Darwin", "Linux", "Windows"])
def test_attributions_accepts_python_31318_on_every_installer_platform(
    attributions, monkeypatch, platform
):
    monkeypatch.setattr(attributions.platform, "system", lambda: platform)
    monkeypatch.setattr(
        attributions.subprocess,
        "check_output",
        Mock(return_value="Python 3.13.18\n"),
    )

    interpreter = attributions.PythonInstall(
        str(Path(sys.executable)), attributions._get_desired_python_version(), False
    )

    assert interpreter.get_uv_venv_python_args() == [
        "--python",
        str(Path(sys.executable)),
        "--python-preference",
        "only-system",
        "--no-python-downloads",
    ]


@pytest.mark.parametrize("platform", ["Darwin", "Linux", "Windows"])
def test_attributions_rejects_python_314_for_the_python_313_installer(
    attributions, monkeypatch, platform
):
    monkeypatch.setattr(attributions.platform, "system", lambda: platform)
    monkeypatch.setattr(
        attributions.subprocess, "check_output", Mock(return_value="Python 3.14.10\n")
    )

    with pytest.raises(RuntimeError, match="does not match specified version 3.13"):
        attributions.PythonInstall(
            str(Path(sys.executable)), attributions._get_desired_python_version(), False
        )


@pytest.mark.parametrize("platform", ["Darwin", "Linux", "Windows"])
def test_zstd_license_matches_python_313_installer_platforms(attributions, platform):
    directory = Path(attributions.__file__).parent
    license_text = (directory / "additional/LIBZSTD_LICENSE.txt").read_text()
    document = (directory / "approved_text" / platform / "THIRD_PARTY_LICENSES").read_text()

    assert (f"libzstd\n\n{license_text}\n" in document) == (platform == "Linux")


@pytest.mark.parametrize("platform", ["Darwin", "Linux", "Windows"])
def test_installer_notices_match_the_pinned_qt_version(attributions, platform):
    directory = Path(attributions.__file__).parent
    document = (directory / "approved_text" / platform / "THIRD_PARTY_LICENSES").read_text()

    assert "qt-6.8.3-src.tar.gz" in document
    assert "pyside-setup-6.8.3-src.tar.gz" in document
    assert "qt-6.10.3-src.tar.gz" not in document
    assert "pyside-setup-6.10.3-src.tar.gz" not in document
