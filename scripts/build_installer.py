# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""Script to create platform-specific Deadline Client installers using InstallBuilder."""

import os
import platform
import sys
import shutil
import tempfile
from datetime import datetime
from typing import Optional
from pathlib import Path

from common import EvaluationBuildError, run
from find_installbuilder import InstallBuilderSelection

# This is derived from <installerFilename> in installer/DeadlineCloudClient.xml
# See "Supported Platforms" table in https://releases.installbuilder.com/installbuilder/docs/installbuilder-userguide.html
INSTALLER_FILENAMES = {
    "Windows": "DeadlineCloudClient-windows-x64-installer.exe",
    "Linux": "DeadlineCloudClient-linux-x64-installer.run",
    "MacOS": "DeadlineCloudClient-osx-installer.app",
}
EVALUATION_VERSION_STRING = "Built with an evaluation version of InstallBuilder"


def setup_install_builder(
    workdir: Path,
    install_builder_location: Optional[Path],
    license_file_path: Optional[Path],
    install_builder_s3_bucket: Optional[str] = None,
    install_builder_s3_key: Optional[str] = None,
) -> Path:
    """
    Ensure installbuilder is installed in some way and return the path
    to the installation directory.
    The method of installing/finding installbuilder is based on the inputs:
        - If `install_builder_location` is provided, look for an installbuilder installation at that path
        - Else if `install_builder_s3_bucket` is provided, attempt to download and unpack install builder from that bucket
        - Else search for it at the default installation path (handy for dev mode)
    """
    if install_builder_location is not None:
        selection = InstallBuilderSelection.from_path(install_builder_location)
    elif install_builder_s3_bucket is not None:
        selection = InstallBuilderSelection.from_s3(
            install_builder_s3_bucket, workdir, install_builder_s3_key
        )
    else:
        selection = InstallBuilderSelection.from_search()

    install_builder_path = selection.resolve_install_builder_installation(workdir)

    if platform.system() == "Windows":
        binary_name = "builder.exe"
    else:
        binary_name = "builder"

    if (
        not install_builder_path.is_dir()
        or not (install_builder_path / "bin" / binary_name).is_file()
    ):
        raise FileNotFoundError(
            f"InstallBuilder path '{install_builder_path}' must be a directory containing 'bin/{binary_name}'."
        )

    if license_file_path is not None:
        shutil.copy(license_file_path, install_builder_path / "license.xml")

    return install_builder_path


def build_installer(
    workdir: Path,
    component_file_path: Path,
    install_builder_location: Path,
    installer_platform: str,
    dev: bool,
) -> Path:
    """
    Actually build the installer
    """
    if install_builder_location is None:
        raise FileNotFoundError(
            "Could not find a default InstallBuilder path. Please specify one with '--install-builder-location'."
        )

    if not install_builder_location.is_dir():
        raise FileNotFoundError(
            f"InstallBuilder path '{install_builder_location}' must be a directory containing 'bin/builder'."
        )

    if installer_platform == "Linux":
        installbuilder_platform = "linux-x64"
    elif installer_platform == "MacOS":
        installbuilder_platform = "osx"
    elif installer_platform == "Windows":
        installbuilder_platform = "windows-x64"
    else:
        raise ValueError(f"Unknown platform '{installer_platform}'")

    install_builder_cli = install_builder_location / "bin" / "builder"
    out_dir = workdir / "out"
    installer_version = os.getenv("INSTALLER_VERSION") if not dev else "00000000"
    if installer_version is None:
        raise ValueError("INSTALLER_VERSION environment variable must be set.")
    output = run(
        [
            install_builder_cli,
            "build",
            str(component_file_path),
            installbuilder_platform,
            "--setvars",
            f"project.outputDirectory={out_dir}",
            f"project.version={installer_version[:8]}-{datetime.today().date()}",
        ]
    )
    sys.stdout.write(
        f"{'-' * 30}\nBegin Install Builder Output\n{'-' * 30}\n"
        f"{output}\n"
        f"{'-' * 30}\nEnd Install Builder Output\n{'-' * 30}\n"
    )

    if EVALUATION_VERSION_STRING in output and not dev:
        raise EvaluationBuildError("InstallBuilder was detected using an evaluation version.")
    elif dev and EVALUATION_VERSION_STRING not in output:
        raise EvaluationBuildError(
            "InstallBuilder was not detected using an evaluation version when running a dev build. "
            "This could indicate that the error messaging when using an evaluation version has changed.\n"
            "Please check the InstallBuilder logs to confirm if the error messaging has changed from "
            f"'{EVALUATION_VERSION_STRING}' and update the build_installer.py script accordingly."
        )
    return out_dir


def main(
    dev: bool,
    install_builder_location: Optional[Path],
    install_builder_license_path: Optional[Path],
    install_builder_s3_bucket: Optional[str],
    install_builder_s3_key: Optional[str],
    output_dir: Optional[Path],
    cleanup: bool,
    installer_platform: str,
    installer_source_path: Path,
) -> None:
    with tempfile.TemporaryDirectory() as wd:
        workdir = Path(wd)
        print(f"cwd: {os.getcwd()}")
        print(f"working directory: {str(workdir)}")

        installer_folder = Path(__file__).absolute().parent.parent / "installer"
        components_dir = installer_folder / "components"

        try:
            installbuilder_path = setup_install_builder(
                workdir=workdir,
                install_builder_location=install_builder_location,
                license_file_path=install_builder_license_path,
                install_builder_s3_bucket=install_builder_s3_bucket,
                install_builder_s3_key=install_builder_s3_key,
            )
            installer_dir = build_installer(
                workdir=workdir,
                component_file_path=installer_source_path,
                install_builder_location=installbuilder_path,
                dev=dev,
                installer_platform=installer_platform,
            )
        except Exception:
            if cleanup:
                shutil.rmtree(components_dir)
            raise

        installer_filename = INSTALLER_FILENAMES[installer_platform]
        installer_path = installer_dir / installer_filename

        # The macOS .app installer will always be a directory, not a file.
        # Other OS installers will be files.
        if (
            not installer_path.is_dir()
            if installer_platform == "MacOS"
            else not installer_path.is_file()
        ):
            raise FileNotFoundError(
                f"Expected installer file {installer_filename} not found in {installer_dir}.\n"
                f"Found:\n\t{os.linesep.join([str(i) for i in installer_dir.iterdir()])}"
            )

        output_path = installer_filename
        if output_dir:
            output_dir.mkdir(exist_ok=True)
            output_path = output_dir / output_path
        shutil.move(installer_path, output_path)

        if cleanup:
            shutil.rmtree(components_dir)
            print(f"Deleted build directory: {components_dir}")
