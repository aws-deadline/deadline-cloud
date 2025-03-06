# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
import os
import platform
import subprocess
import time
from pathlib import Path
from typing import Tuple

import pytest

RETRY_DELAY = 1  # seconds
MAX_RETRIES = 60  # 60 * 1 = 1 minute max wait


@pytest.fixture(scope="session")
def installer_path():
    path = "DeadlineCloudClient-{platform}-{arch}-installer.{ext}"

    if platform.system() == "Darwin":
        path = os.path.join(
            path.format(platform="osx", arch="x64", ext="app"),
            "Contents",
            "MacOS",
            "installbuilder.sh",
        )
    elif platform.system() == "Windows":
        path = path.format(platform="windows", arch="x64", ext="exe")
    elif platform.system() == "Linux":
        path = path.format(platform="linux", arch="x64", ext="run")

    if not os.path.isfile(path):
        raise FileNotFoundError(f"Installer not found at '{path}'")

    if not os.access(path, os.X_OK) and not platform.system() == "Darwin":
        raise PermissionError(f"Installer at '{path}' is not executable")

    yield Path(path).absolute()


@pytest.fixture(scope="function")
def installed(installer_path, tmp_path):
    args = [installer_path, "--mode", "unattended", "--installscope", "user", "--prefix", tmp_path]
    result = subprocess.run(args, check=True)
    assert result.returncode == 0

    yield tmp_path


def _uninstaller_name_and_path(install_dir: Path) -> Tuple[str, Path]:
    uninstaller_name = "uninstall"
    uninstaller_path = install_dir.joinpath("uninstall")

    if platform.system() == "Darwin":
        uninstaller_name = f"{uninstaller_name}.app"
        uninstaller_path = install_dir.joinpath(
            uninstaller_name, "Contents", "MacOS", "installbuilder.sh"
        )
    elif platform.system() == "Windows":
        uninstaller_name = f"{uninstaller_name}.exe"
        uninstaller_path = uninstaller_path.with_suffix(".exe")

    return uninstaller_name, uninstaller_path


def test_install(installed: Path) -> None:
    """
    Test that a fresh install of the Deadline Client contains certain files and
    modules from the installation to validate that it was installed correctly.
    """
    # GIVEN / WHEN
    uninstaller_name, _ = _uninstaller_name_and_path(installed)

    # THEN
    top_level_dir = [f.name for f in installed.iterdir()]
    assert "DeadlineClient" in top_level_dir
    assert "installer_version.txt" in top_level_dir
    assert uninstaller_name in top_level_dir

    # Just check that we have dependencies in this folder
    cli_dir = installed.joinpath("DeadlineClient", "cli")
    cli_dir_contents = [f.name for f in (cli_dir).iterdir()]
    assert "deadline" in cli_dir_contents

    # Check the deadline module is here and there's a version file
    client_dir = [f.name for f in (cli_dir.joinpath("deadline", "client")).iterdir()]
    assert "_version.py" in client_dir


def test_uninstall(installed: Path) -> None:
    """
    Test that a fresh intsall of the Deadline Client can be uninstalled
    in unattended mode and the installation directory and all contents are
    removed.
    """

    # GIVEN
    assert installed.exists()
    _, uninstaller_path = _uninstaller_name_and_path(installed)

    # WHEN
    result = subprocess.run([str(uninstaller_path), "--mode", "unattended"], check=True)

    # THEN
    assert result.returncode == 0
    for _ in range(MAX_RETRIES):
        if not installed.exists():
            break
        time.sleep(RETRY_DELAY)

    assert not installed.exists()
