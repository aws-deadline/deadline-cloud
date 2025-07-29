# Pyinstaller Artifact Validation

In order to ensure the consistency of pyinstaller-built artifacts for this package, artifacts must be compared against an allowlist of files.

## Quick Reference

### Prerequisites

- uv (`pip install uv`)

### Steps

#### Generate Attributions Document

See [attributions](../attributions/README.txt)

#### Build Executable

1. `uv venv .venv`
1. `source .venv/bin/activate` (bash/zsh), or `source .venv/bin/activate.fish` (fish), or `.venv/Scripts/activate` (powershell)
1. `uv pip install -e .`
1. `uv pip install -r requirements-pyinstaller-6.txt`
1. `python scripts/pyinstaller/make_exe.py`
1. `deactivate`

When building the pyinstaller executable, the venv used to build it must use the correct Python version for the platform. The python versions used currently are:

|Platform|Version|
|--------|-------|
|Linux   |3.9    |
|Windows |3.10   |
|macOS   |3.12   |

`uv venv` has a `--python` argument which can either take a Python version or a path to a Python interpreter. If a version is specified that is not found on the system, uv will
download that version of Python from the internet and install it into the venv. If a path to a Python interpreter is specified, uv will use that
python interpreter for the venv. The `--no-python-downloads` flag can also be specified to ensure that `uv` does not download Python from the internet
if it is not desired. See `uv venv --help` for more information.

#### Validate Executable

1. `uv venv .venv-validation`
1. `source .venv-validation/bin/activate` (bash/zsh), or `source .venv-validation/bin/activate.fish` (fish), or `.venv-validation/Scripts/activate` (powershell)
1. `uv pip install -r requirements-pyinstaller-6.txt`
1. `python scripts/pyinstaller/validate.py`
1. `deactivate`

The venv used to run the validation script must use Python 3.12 or later.

## Details

`make_exe.py` outputs a zip archive containing the output from pyinstaller. Because we use pyinstaller's one folder mode, this means an executable and a folder with
assets which were not bundled into the executable. The executable is a thin wrapper that calls another deadline_cli executable which contains most of the code.

All the contents of this zip archive must be reconciled against the allow list which specifies which files we are expecting to be bundled. The allowlist itself is
defined recursively because all archives must be extracted and their contents also reconciled with the allow list. This includes the executables themselves and the pyz
archives inside the executables. The pyi-archive_viewer utility that ships with pyinstaller is used to extract the executables and pyz files.

The allowlist itself contains both file paths and globs. Both are relative to the archive root. A file must either be listed under files or match a glob pattern to be
allowlisted. Any file detected that is not allowlisted will result in the validation failing. Additionally individual files can have conditions on them. The available
conditions are "archive_contents" which itself is defined as an allowlist (this is where the recursive definition comes in) and a sha256 hash. If a sha256 hash is specified,
the sha256 sum of the file must match the hash. If "archive_contents" is specified, the file must be either a zip archive, a pyinstaller executable, or a pyz file and
the contents must match the nested allow list.

Nested allow lists do not inherit rules from their parents, however, all levels of allowlist are automatically populated with rules to allow for files based on listed
dependencies and python standard library modules. This is to keep the allowlists themselves manageable. If a file is an archive, an empty "archive_contents" condition
must be put on the file in order for it to be extracted and checked against these automatic allow list rules. This is for performance reasons as we do not want to have
to check if every file is an archive.

Care must be taken when adding entries to the allowlist to make sure that:
- We know the license that applies to the file
- We have attributed and otherwise complied with all conditions of the license
- Allow list glob entries are not so broad that there is a chance they will match files that are unaccounted for
- Archive files are not added to the allow list unless an archive contents condition is added to them

