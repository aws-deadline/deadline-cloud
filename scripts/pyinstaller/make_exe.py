# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

#!/usr/bin/env python
"""Script to create a pyinstaller executable.
This exe can then be wrapped in a platform specific installer for each
supported platform.

Here is an example sequence of commands to run this script:
$ mamba create -n pyinst python=3.9
$ mamba activate pyinst
$ pip install pyinstaller
$ pip install -e .
$ python scripts/pyinstaller/make_exe.py
"""

import argparse
import json
import os
import shutil
import subprocess
import time
import zipfile
from importlib.metadata import version
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).absolute().parents[2]
PYINSTALLER_DIR = ROOT / "scripts" / "pyinstaller"

# These are created by PyInstaller: https://pyinstaller.org/en/stable/usage.html
PYINSTALLER_DIST_DIR = PYINSTALLER_DIR / "dist"
PYINSTALLER_BUILD_DIR = PYINSTALLER_DIR / "build"

# Defined in deadline_cli.spec as "name" kwarg to COLLECT
DEADLINE_CLI_SPEC_PATH = PYINSTALLER_DIR / "deadline_cli.spec"
DEADLINE_CLI_DIST_PATH = PYINSTALLER_DIST_DIR / "deadline"

DEFAULT_OUTPUT_ZIP = "deadline-client-exe.zip"


def make_exe(exe_zipfile: Path, cleanup=True, version_file: Optional[Path] = None) -> None:
    clean_pyinstaller_build_dirs()

    if version_file is not None:
        pyinstaller_version = version("pyinstaller")
        with open(version_file, "w", encoding="utf8") as f:
            json.dump({"pyinstaller": pyinstaller_version}, f)

    # Create Deadline CLI dist
    pyinstaller(str(DEADLINE_CLI_SPEC_PATH))

    # Remove unused Qt modules that get pulled in via binary dependencies
    remove_unused_qt_modules(DEADLINE_CLI_DIST_PATH)

    # Sometimes the files output by pyinstaller have a last modified
    # date of the unix epoch. This causes make_archive to fail.
    # Touch each file in the directory we are archiving to make
    # sure they all have non-epoch modified dates.
    for dirpath, _, filenames in os.walk(DEADLINE_CLI_DIST_PATH):
        for filename in filenames:
            (Path(dirpath) / filename).touch(exist_ok=True)

    if os.name == "nt":
        shutil.make_archive(exe_zipfile.with_suffix(""), "zip", DEADLINE_CLI_DIST_PATH)
    else:
        # preserve symlinks in archive on unix
        zip_with_symlinks(DEADLINE_CLI_DIST_PATH, exe_zipfile.with_suffix(".zip"))

    if cleanup:
        clean_pyinstaller_build_dirs()

    print(f"Exe build is available at: {str(exe_zipfile)}")


def pyinstaller(*args: tuple):
    if "--onefile" in args or "-F" in args:
        raise Exception(
            "Cannot use --onefile/-F option for PyInstaller due to libreadline being licensed under GPL"
        )

    ################################# WARNING ##################################
    # Do not change this to use one-file mode (do not add `--onefile` / `-F`   #
    # to the command-line arguments).                                          #
    #                                                                          #
    # Doing so causes pyinstaller to bundle libreadline which is licensed      #
    # under GPL.                                                               #
    ################################# WARNING ##################################
    subprocess.run(["pyinstaller", *args], cwd=PYINSTALLER_DIR, check=True)


def clean_pyinstaller_build_dirs():
    for location in [
        PYINSTALLER_BUILD_DIR,
        PYINSTALLER_DIST_DIR,
    ]:
        shutil.rmtree(location, ignore_errors=True)
        print(f"Deleted build directory: {str(location)}")


# Qt modules to keep - only the ones we actually use.
# This is a removal filter, not an inclusion list. PyInstaller only bundles
# modules that exist on the build platform, so platform-specific entries
# (e.g. QtDBus on Linux/macOS, Wayland/Xcb on Linux) are harmless on other OSes.
# Direct dependencies:
#   QtCore, QtGui, QtWidgets - used by deadline.client.ui via qtpy
#   QtDBus - required by Qt on Linux/macOS for system integration
# Transitive dependencies (pulled in by Qt platform plugins):
#   QtXcbQpa - X11 platform plugin (Linux)
#   QtWaylandClient, QtWaylandEglClientHwIntegration, QtWlShellIntegration - Wayland (Linux)
QT_MODULES_TO_KEEP = {
    "QtCore",
    "QtGui",
    "QtWidgets",
    "QtSvg",
    "QtDBus",
    "QtXcbQpa",
    "QtWaylandClient",
    "QtWaylandEglClientHwIntegration",
    "QtWlShellIntegration",
}


def _find_qt_plugins_dir(pyside_dir: Path) -> Path:
    """Find the Qt plugins directory, checking both possible layouts.

    PySide6 places plugins under Qt/plugins on macOS/Linux and uses
    plugins/ directly on Windows.

    Raises RuntimeError if PySide6 is present but no plugins directory is found.
    """
    for candidate in (pyside_dir / "Qt" / "plugins", pyside_dir / "plugins"):
        if candidate.exists():
            return candidate
    raise RuntimeError(
        f"Expected Qt plugins directory not found under {pyside_dir}. "
        "The PySide6 plugin layout may have changed."
    )


def remove_unused_qt_modules(dist_path: Path) -> None:
    """Remove Qt modules pulled in via binary dependencies that we don't need."""
    internal_dir = dist_path / "_internal"
    if not internal_dir.exists():
        raise RuntimeError(f"Expected directory {internal_dir} does not exist")

    # Remove top-level Qt symlinks (macOS: QtCore, Linux: libQt6Core.so.6)
    for item in internal_dir.iterdir():
        if item.name.startswith("Qt") and item.name not in QT_MODULES_TO_KEEP:
            if item.is_symlink() or item.is_file():
                item.unlink()
                print(f"Removed unused Qt symlink: {item.name}")
        elif item.name.startswith("libQt6") and (item.is_symlink() or item.is_file()):
            module_name = item.name.split(".")[0].replace("libQt6", "Qt")
            if module_name not in QT_MODULES_TO_KEEP:
                item.unlink()
                print(f"Removed unused Qt symlink: {item.name}")

    # Remove Qt framework directories (macOS) and DLLs (Windows)
    pyside_dir = internal_dir / "PySide6"
    if pyside_dir.exists():
        # macOS frameworks
        qt_lib_dir = pyside_dir / "Qt" / "lib"
        if qt_lib_dir.exists():
            for framework in qt_lib_dir.iterdir():
                if framework.name.endswith(".framework"):
                    module_name = framework.name.replace(".framework", "")
                    if module_name not in QT_MODULES_TO_KEEP:
                        shutil.rmtree(framework)
                        print(f"Removed unused Qt framework: {framework.name}")

        # Windows DLLs
        for dll in pyside_dir.glob("Qt6*.dll"):
            module_name = dll.stem.replace("Qt6", "Qt")
            if module_name not in QT_MODULES_TO_KEEP:
                dll.unlink()
                print(f"Removed unused Qt DLL: {dll.name}")

        # Linux shared libraries
        qt_lib_dir_linux = pyside_dir / "Qt" / "lib"
        if qt_lib_dir_linux.exists():
            for so in qt_lib_dir_linux.glob("libQt6*.so*"):
                # Extract module name from libQt6Core.so.6 -> QtCore
                module_name = so.name.split(".")[0].replace("libQt6", "Qt")
                if module_name not in QT_MODULES_TO_KEEP:
                    so.unlink()
                    print(f"Removed unused Qt library: {so.name}")

        # Remove unused Qt plugins. Only keep plugins required by the deadline GUI:
        #   platforms/ - platform integration (cocoa, xcb, wayland, minimal)
        #   styles/ - native widget styling
        #   iconengines/ - SVG icon support (used for deadline_logo.svg, info.svg)
        #   imageformats/qsvg - SVG image format support
        QT_PLUGIN_DIRS_TO_KEEP = {"platforms", "styles", "iconengines", "wayland-shell-integration"}
        # libqsvg on macOS/Linux, qsvg on Windows
        QT_IMAGEFORMAT_PLUGINS_TO_KEEP = {"libqsvg", "qsvg"}
        plugins_dir = _find_qt_plugins_dir(pyside_dir)
        for plugin_dir in list(plugins_dir.iterdir()):
            if not plugin_dir.is_dir():
                continue
            if plugin_dir.name not in QT_PLUGIN_DIRS_TO_KEEP and plugin_dir.name != "imageformats":
                shutil.rmtree(plugin_dir)
                print(f"Removed unused Qt plugin directory: {plugin_dir.name}/")
            elif plugin_dir.name == "imageformats":
                for plugin in list(plugin_dir.iterdir()):
                    plugin_base = plugin.stem.split(".")[0]
                    if plugin_base not in QT_IMAGEFORMAT_PLUGINS_TO_KEEP:
                        plugin.unlink()
                        print(f"Removed unused Qt imageformat plugin: {plugin.name}")


def zip_with_symlinks(source_dir: Path, output_zip: Path) -> None:
    """Create a zip archive that preserves symlinks for unix systems"""
    output_zip.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output_zip, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(source_dir):
            for name in dirs + files:
                path = os.path.join(root, name)
                arcname = os.path.relpath(path, source_dir)

                if os.path.islink(path):
                    info = zipfile.ZipInfo(arcname)
                    stat_info = os.lstat(path)
                    # The 2 higher order bytes are used by unix permissions, lower for MS-DOS
                    info.external_attr = (stat_info.st_mode & 0xFFFF) << 16
                    # year, month, day, hour, min, sec
                    info.date_time = time.localtime(stat_info.st_mtime)[:6]
                    zipf.writestr(info, os.readlink(path))
                else:
                    zipf.write(path, arcname)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        default=str(ROOT / "dist" / DEFAULT_OUTPUT_ZIP),
        help=(
            "The name of the file to save the exe zip. By default, "
            f"this will be saved in 'dist/{DEFAULT_OUTPUT_ZIP}' directory in the root of the "
            "DeadlineClient."
        ),
    )
    parser.add_argument(
        "--no-cleanup",
        dest="cleanup",
        action="store_false",
        help=("Leave the build folder produced by pyinstaller. This can be useful for debugging."),
    )
    parser.add_argument(
        "--version-file",
        type=Path,
        required=False,
        help="Path to a file to write package versions used to.",
    )
    args = parser.parse_args()

    output = Path(args.output).absolute()

    make_exe(output, cleanup=args.cleanup, version_file=args.version_file)


if __name__ == "__main__":
    main()
