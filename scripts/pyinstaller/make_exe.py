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
import os
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).absolute().parents[2]
PYINSTALLER_DIR = ROOT / "scripts" / "pyinstaller"

# These are created by PyInstaller: https://pyinstaller.org/en/stable/usage.html
PYINSTALLER_DIST_DIR = PYINSTALLER_DIR / "dist"
PYINSTALLER_BUILD_DIR = PYINSTALLER_DIR / "build"

# Defined in deadline_cli.spec as "name" kwarg to COLLECT
DEADLINE_CLI_SPEC_PATH = PYINSTALLER_DIR / "deadline_cli.spec"
DEADLINE_CLI_DIST_PATH = PYINSTALLER_DIST_DIR / "deadline_cli"

# Defined in deadline.spec as "name" kwarg to COLLECT
DEADLINE_SPEC_PATH = PYINSTALLER_DIR / "deadline.spec"
DEADLINE_DIST_PATH = PYINSTALLER_DIST_DIR / "deadline"

DEFAULT_OUTPUT_ZIP = "deadline-client-exe.zip"


def make_exe(exe_zipfile: Path, cleanup=True) -> None:
    clean_pyinstaller_build_dirs()

    # Create Deadline CLI dist
    pyinstaller(str(DEADLINE_CLI_SPEC_PATH))

    # Create Deadline CLI wrapper dist
    os.environ["PYINSTALLER_DEADLINE_CLI_DIST_PATH"] = str(DEADLINE_CLI_DIST_PATH)
    pyinstaller(str(DEADLINE_SPEC_PATH))

    # Sometimes the files output by pyinstaller have a last modified
    # date of the unix epoch. This causes make_archive to fail.
    # Touch each file in the directory we are archiving to make
    # sure they all have non-epoch modified dates.
    for dirpath, _, filenames in os.walk(DEADLINE_DIST_PATH):
        for filename in filenames:
            (Path(dirpath) / filename).touch(exist_ok=True)

    # Zip up the Deadline CLI wrapper to the final output path
    shutil.make_archive(exe_zipfile.with_suffix(""), "zip", DEADLINE_DIST_PATH)

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
    args = parser.parse_args()

    output = Path(args.output).absolute()

    make_exe(output, cleanup=args.cleanup)


if __name__ == "__main__":
    main()
