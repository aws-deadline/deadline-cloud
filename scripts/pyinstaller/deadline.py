# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Workaround to PyInstaller not allowing an executable to share the same name as a Python module it uses since they
are created in the same directory. This file just calls the PyInstaller executable `deadline_cli` generated from
the actual `deadline` package.

This file should be built with PyInstaller into its own executable which depends on the actual `deadline` package's
PyInstaller executable.
"""
import pathlib
import subprocess
import sys

sys.exit(
    subprocess.call(
        [pathlib.Path(__file__).absolute().parent / "cli" / "deadline_cli"] + sys.argv[1:]
    )
)
