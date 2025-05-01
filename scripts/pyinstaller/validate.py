# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import argparse
import contextlib
import fnmatch
import hashlib
import itertools
import re
import subprocess
import tempfile
import zipfile

from dataclasses import dataclass
from enum import auto, Enum
from pathlib import Path
from typing import Any, Callable, Generator, Optional

from allowlist import ALLOWLIST, DEPENDENCIES
from stdlib_modules import STANDARD_LIBRARY_MODULES

_BLOCK_SIZE = 65536
_PYI_CONTENTS_HEADER_REGEX = re.compile(r"^Contents of '(.+)' \((.+)\):$")


@contextlib.contextmanager
def temp_zip_contents(zip_path: Path) -> Generator[Path, None, None]:
    """
    Context manager that temporarily extracts the contents of a zip
    file to a temporary directory, yielding the path to the temporary
    directory
    """
    with tempfile.TemporaryDirectory() as td:
        temp = Path(td)
        with zipfile.ZipFile(zip_path, "r") as zip:
            zip.extractall(temp)
        yield temp


class _PYIParseState(Enum):
    Start = auto()
    ContentsSection = auto()
    End = auto()


def _get_pyinstaller_contents_info(archive_path: Path) -> list[str]:
    """
    Uses pyi-archive_viewer to list the contents of a pyi-archive_viewer compatible
    archive such as a pyinstaller generated executable or .pyz file
    """
    archive_list = subprocess.check_output(
        ["pyi-archive_viewer", "--list", "--brief", str(archive_path)], text=True
    )
    # Output will look something like:
    #
    # Options in 'deadline' (PKG/CArchive):
    #  pyi-contents-directory _internal
    # Contents of 'deadline' (PKG/CArchive):
    #  struct
    #  pyimod01_archive
    #  pyimod02_importers
    #  pyimod03_ctypes
    #  pyiboot01_bootstrap
    #  deadline
    #  PYZ.pyz

    state = _PYIParseState.Start
    items = []
    for line in archive_list.splitlines(keepends=False):
        if state == _PYIParseState.Start:
            if _PYI_CONTENTS_HEADER_REGEX.match(line) is not None:
                state = _PYIParseState.ContentsSection
        elif state == _PYIParseState.ContentsSection:
            if item := line.strip():
                items.append(item)
            else:
                state = _PYIParseState.End
        elif state == _PYIParseState.End:
            if line.strip():
                raise RuntimeError(
                    (
                        f"Further data found after contents list for {archive_path}. "
                        "This may mean the format has changed between pyinstaller versions. "
                        f"Use `pyi-archive_viewer -l -b {archive_path}` to check the output to check the output."
                    )
                )
        else:
            raise RuntimeError(
                f"Non-valid parse state {str(state)} reached while parsing contents of {archive_path}"
            )

    return items


@contextlib.contextmanager
def temp_pyinstaller_archive_contents(archive_path: Path) -> Generator[Path, None, None]:
    """
    Context manager to temporarily extract the contents of a pyi-archive_viewer compatible
    archive (such as a pyinstaller generated executable or .pyz file) to a temporary directory
    """
    archive_contents = _get_pyinstaller_contents_info(archive_path)
    with tempfile.TemporaryDirectory() as td:
        temp = Path(td)
        # We have to batch the extractions into multiple
        # invocations of pyi-archive_viewer because if we try
        # to do too many at once on Windows it freezes.
        for batch in itertools.batched(archive_contents, 16):
            # pyi-archive_viewer doesn't seem to have arguments
            # for extracting from the archive, so we need to
            # start it in interactive mode and pipe commands
            # to stdin
            process = subprocess.Popen(
                ["pyi-archive_viewer", archive_path],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            for item in batch:
                # X - Extract
                process.stdin.write(f"X {item}\n")
                process.stdin.flush()
                # Input the path to extract to
                process.stdin.write(f"{str(temp / item)}\n")
                process.stdin.flush()
            # Quit
            process.stdin.write("Q\n")
            process.stdin.flush()
            stdout, stderr = process.communicate()
            if process.returncode != 0:
                print(stdout)
                print(stderr)
                raise RuntimeError(f"Failed to extract {archive_path}")
            # Verify that all files were extracted
            for item in batch:
                if not (temp / item).is_file():
                    raise RuntimeError(f"Failed to extract {item.name} from {archive_path}")
        yield temp


def calculate_sha256(filepath: Path) -> str:
    """
    Calculated the sha256 of a file at a given path
    """
    sha256 = hashlib.sha256()
    with open(filepath, "rb") as f:
        while True:
            data = f.read(_BLOCK_SIZE)
            if not data:
                break
            sha256.update(data)
    return sha256.hexdigest()


def rglob_files_only(basepath: Path, glob: str) -> list[Path]:
    """
    call rglob on a given path, but filter out anything that isn't a file
    """
    return [filepath for filepath in basepath.rglob(glob) if filepath.is_file()]


def _re_raise(e: Exception) -> None:
    raise e


def enumerate_directory(dirpath: Path) -> list[Path]:
    result = []
    for root, dirs, files in dirpath.walk(follow_symlinks=False, on_error=_re_raise):
        for name in files:
            result.append((root / name).relative_to(dirpath))
    return result


def rglob_file_list(files: list[Path], glob: str) -> list[Path]:
    result = []
    for filepath in files:
        if fnmatch.fnmatch(str(filepath), glob):
            result.append(filepath)
    return result


@dataclass
class AllowlistReport:
    allowed_files: list[Path]
    dissallowed_files: list[Path]


@dataclass
class AllowlistCondition:
    """
    If set, the file this condition represents
    must match the given sha256 hash
    """

    sha256: Optional[str]
    """
    If set, the file this condition represets
    must be an archive (currently zip, pyz, and pyinstaller executables are supported)
    and when extracted, the contents must pass the given allowlist being applied to them.
    """
    archive_contents: Optional["Allowlist"]


@dataclass
class Allowlist:
    """
    Allowlist is a recursive datastructure representing a set of rules for what is allowed
    to be in a given archive. All paths are relative to the archive root.
    In order to pass application of the allowlist, all files in the archive must either
    explicitly be listed in `files` or must be matched by a glob in `globs`. Conditions can also
    be applied to specific files. See `AllowlistCondition` for more information.
    """

    files: list[Path]
    globs: list[str]
    conditions: dict[Path, AllowlistCondition]

    @staticmethod
    def from_dict(raw: dict[str, Any]) -> "Allowlist":
        files = []
        globs = []
        for dep in [*DEPENDENCIES, *STANDARD_LIBRARY_MODULES]:
            files.extend(
                [
                    Path(dep),
                    Path(f"{dep}.pyc"),
                ]
            )
            globs.extend(
                [
                    f"{dep}.*",
                    f"{dep}/*",
                    f"**/{dep}.pyd",
                    f"**/_{dep}.pyd",
                    f"_internal/cli/_internal/{dep}-*.dist-info/**/*",
                    f"_internal/cli/_internal/{dep}-*.dist-info/*",
                    f"_internal/cli/_internal/{dep}/**/*",
                    f"_internal/cli/_internal/{dep}/*",
                ]
            )

        files.extend([Path(file) for file in raw.get("files", [])])
        globs.extend(raw.get("globs", []))
        conditions = (
            {
                Path(filepath): AllowlistCondition(
                    sha256=condition.get("sha256", None),
                    archive_contents=(
                        Allowlist.from_dict(condition["archive_contents"])
                        if "archive_contents" in condition
                        else None
                    ),
                )
                for filepath, condition in raw["conditions"].items()
            }
            if "conditions" in raw
            else {}
        )
        return Allowlist(
            files,
            globs,
            conditions,
        )

    def get_allowed_files(self, root: Path) -> AllowlistReport:
        all_files = enumerate_directory(root)
        allowed_files = []
        remaining_files = set(all_files)

        for file in all_files:
            if file in self.files:
                allowed_files.append(file)
                remaining_files.remove(file)

        for glob in self.globs:
            globbed = rglob_file_list(list(remaining_files), glob)
            for file in globbed:
                remaining_files.remove(file)
            allowed_files.extend(globbed)
            if len(remaining_files) == 0:
                break

        return AllowlistReport(
            allowed_files,
            list(remaining_files),
        )


def _prepend_if_not_none(context: Optional[Path], suffix: Path) -> Path:
    if context is None:
        return suffix
    return context / suffix


def _get_extraction_manager(
    archive_path: Path, context: Optional[Path]
) -> Callable[[Path], Generator[Path, None, None]]:
    extension = archive_path.suffix
    if extension == ".zip":
        return temp_zip_contents
    elif extension == ".pyz" or extension == ".exe" or not extension:
        return temp_pyinstaller_archive_contents
    else:
        raise RuntimeError(
            f"Cannot extract from archive of unknown type: {_prepend_if_not_none(context, archive_path.name)}"
        )


def apply_allowlist(archive_path: Path, allowlist: Allowlist, context: Optional[Path]) -> list[str]:
    print(f"Extracting: {archive_path}")
    with _get_extraction_manager(archive_path, context)(archive_path) as temp:
        allowlist_report = allowlist.get_allowed_files(temp)
        failures = [
            f"Found file {_prepend_if_not_none(context, dissallowed)} which was not included in the allowlist"
            for dissallowed in allowlist_report.dissallowed_files
        ]

        for relative in allowlist_report.allowed_files:
            if relative in allowlist.conditions:
                condition = allowlist.conditions[relative]
                if condition.sha256 is not None:
                    calculated = calculate_sha256(temp / relative)
                    if calculated != condition.sha256:
                        failures.append(
                            f"File {_prepend_if_not_none(context, relative)} has sha256 {calculated} which does not match the expected {condition.sha256}"
                        )
                if condition.archive_contents is not None:
                    failures.extend(
                        apply_allowlist(
                            temp / relative,
                            condition.archive_contents,
                            _prepend_if_not_none(context, relative),
                        )
                    )
    return failures


def cli() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--zip-path",
        type=Path,
        required=False,
        help="The path to where the pyinstaller output zip can be found.",
    )
    args = parser.parse_args()

    repository_root = Path(__file__).parent.parent.parent

    if args.zip_path is None:
        zip_path = repository_root / "dist" / "deadline-client-exe.zip"
    else:
        zip_path = args.zip_path

    allowlist = Allowlist.from_dict(ALLOWLIST)
    failures = apply_allowlist(zip_path, allowlist, None)

    for failure in failures:
        print(failure)

    if len(failures) != 0:
        exit(1)
    print("Validation passed")


if __name__ == "__main__":
    cli()
