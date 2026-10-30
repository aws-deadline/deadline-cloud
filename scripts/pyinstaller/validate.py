# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import argparse
import contextlib
import fnmatch
import hashlib
import os
import sys
import tempfile
import zipfile

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Generator, Optional

from allowlist import ALLOWLIST, DEPENDENCIES

_BLOCK_SIZE = 65536


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


@contextlib.contextmanager
def temp_pyinstaller_archive_contents(archive_path: Path) -> Generator[Path, None, None]:
    """Extract archive entries, preserving namespace packages for name auditing."""
    from PyInstaller.archive.readers import CArchiveReader, ZlibArchiveReader
    from PyInstaller.loader.pyimod01_archive import PYZ_ITEM_NSPKG

    is_pyz = archive_path.suffix == ".pyz"
    archive = ZlibArchiveReader(str(archive_path)) if is_pyz else CArchiveReader(str(archive_path))
    with tempfile.TemporaryDirectory() as td:
        temp = Path(td)
        for name, entry in archive.toc.items():
            destination = temp / name
            if not destination.resolve().is_relative_to(temp.resolve()):
                raise RuntimeError(f"Archive entry {name!r} escapes the extraction directory")
            data = archive.extract(name, raw=True) if is_pyz else archive.extract(name)
            if data is None:
                if not is_pyz or entry[0] != PYZ_ITEM_NSPKG:
                    raise RuntimeError(f"Archive entry {name!r} has no data")
                # Namespace packages have no code, but their names still need allowlist checks.
                data = b""
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(data)
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
    for root, dirs, files in os.walk(dirpath, followlinks=False, onerror=_re_raise):
        root_path = Path(root)
        for name in files:
            result.append((root_path / name).relative_to(dirpath))
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
        python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
        python_tag = f"{sys.version_info.major}{sys.version_info.minor}"
        for dep in [*DEPENDENCIES, *sys.stdlib_module_names]:
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
                    f"_internal/{dep}-*.dist-info/**/*",
                    f"_internal/{dep}-*.dist-info/*",
                    f"_internal/{dep}/**/*",
                    f"_internal/{dep}/*",
                    f"_internal/lib-dynload/{dep}.cpython-3*-x86_64-linux-gnu.so",
                    f"_internal/lib-dynload/{dep}.cpython-3*-darwin.so",
                    f"_internal/lib-dynload/_{dep}.cpython-3*-x86_64-linux-gnu.so",
                    f"_internal/lib-dynload/_{dep}.cpython-3*-darwin.so",
                    f"_internal/python{python_version}/lib-dynload/{dep}.cpython-{python_tag}-x86_64-linux-gnu.so",
                    f"_internal/python{python_version}/lib-dynload/{dep}.cpython-{python_tag}-darwin.so",
                    f"_internal/python{python_version}/lib-dynload/_{dep}.cpython-{python_tag}-x86_64-linux-gnu.so",
                    f"_internal/python{python_version}/lib-dynload/_{dep}.cpython-{python_tag}-darwin.so",
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
