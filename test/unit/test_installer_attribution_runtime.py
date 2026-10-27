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


@pytest.mark.parametrize(
    "component,license_file",
    [
        ("bzip2", "BZIP2_LICENSE.txt"),
        ("zlib", "ZLIB_1_3_2_LICENSE.txt"),
        ("libffi", "LIBFFI_LICENSE.txt"),
    ],
)
def test_source_runtime_library_licenses_are_attributed_on_linux(
    attributions, component, license_file
):
    entry = next(
        value
        for value in attributions._ADDITIONAL_ATTRIBUTIONS
        if value["name"] == component
        and ("platforms" not in value or "Linux" in value["platforms"])
    )
    assert entry["attribution_path"] == license_file
    assert "platforms" not in entry or "Linux" in entry["platforms"]
    directory = Path(attributions.__file__).parent
    text = (directory / "additional" / license_file).read_text()
    document = (directory / "approved_text/Linux/THIRD_PARTY_LICENSES").read_text()

    assert f"{component}\n\n{text}\n" in document


@pytest.mark.parametrize(
    "platform,license_file",
    [
        ("Darwin", "ZLIB_LICENSE.txt"),
        ("Linux", "ZLIB_1_3_2_LICENSE.txt"),
        ("Windows", "ZLIB_LICENSE.txt"),
    ],
)
def test_each_installer_platform_has_exactly_one_zlib_notice(attributions, platform, license_file):
    entries = [
        value
        for value in attributions._ADDITIONAL_ATTRIBUTIONS
        if value["name"] == "zlib" and ("platforms" not in value or platform in value["platforms"])
    ]
    assert len(entries) == 1
    assert entries[0]["attribution_path"] == license_file
    directory = Path(attributions.__file__).parent
    text = (directory / "additional" / license_file).read_text()
    document = (directory / "approved_text" / platform / "THIRD_PARTY_LICENSES").read_text()

    assert document.count(f"zlib\n\n{text}\n") == 1
