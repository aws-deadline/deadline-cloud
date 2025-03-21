# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
import platform
import re
import shutil
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional, Union

import boto3

from common import UnsupportedOSError


_INSTALL_BUILDER_ARCHIVE_FILENAME = {
    "Windows": "VMware-InstallBuilder-Professional-windows.tar.gz",
    "Linux": "VMware-InstallBuilder-Professional-linux.tar.gz",
}


@dataclass
class _InstallBuilderPathSelection:
    path: Path


@dataclass
class _InstallBuilderS3Selection:
    bucket: str
    key: str
    dest_path: Path


@dataclass
class InstallBuilderSelection:
    selection: Optional[Union[_InstallBuilderPathSelection, _InstallBuilderS3Selection]]

    def resolve_install_builder_installation(self, workdir: Path) -> Path:
        if self.selection is None:
            return _get_default_installbuilder_location()
        elif isinstance(self.selection, _InstallBuilderPathSelection):
            return self.selection.path
        elif isinstance(self.selection, _InstallBuilderS3Selection):
            filename = self.selection.key.split("/")[-1]
            s3 = boto3.client("s3")
            dest = self.selection.dest_path / filename
            dest.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(self.selection.bucket, self.selection.key, str(dest))
            unpack_dest = workdir / filename.split(".tar.gz")[0]
            shutil.unpack_archive(dest, unpack_dest)
            dest.unlink()
            return unpack_dest
        else:
            raise ValueError(f"Unknown selection type: {type(self.selection)}")

    @staticmethod
    def from_s3(bucket_name: str, dest_path: Path, key: Optional[str] = None):
        if key is None:
            if platform.system() in _INSTALL_BUILDER_ARCHIVE_FILENAME:
                resolved_key = (
                    f"install_builder/{_INSTALL_BUILDER_ARCHIVE_FILENAME[platform.system()]}"
                )
            else:
                raise UnsupportedOSError(f"Unsupported OS: {platform.system()}")
        else:
            resolved_key = key
        return InstallBuilderSelection(
            _InstallBuilderS3Selection(bucket_name, resolved_key, dest_path)
        )

    @staticmethod
    def from_path(path: Path):
        return InstallBuilderSelection(_InstallBuilderPathSelection(path))

    @staticmethod
    def from_search():
        return InstallBuilderSelection(None)


@dataclass
class _InstallBuilderSearchConfig:
    parent_path: Path
    install_directory_name_regex: str

    def get_install_directory_regex(self) -> re.Pattern:
        return re.compile(self.install_directory_name_regex)


_INSTALL_BUILDER_SEARCH_CONFIGS = {
    "Linux": _InstallBuilderSearchConfig(Path("/opt"), r"^installbuilder-(\d+)\.(\d+)\.(\d+)$"),
    "Darwin": _InstallBuilderSearchConfig(
        Path("/Applications"),
        r"^InstallBuilder (Professional|Enterprise|For (?:Windows|OS X)) (\d+)\.(\d+)\.(\d+)$",
    ),
    "Windows": _InstallBuilderSearchConfig(
        Path("C:\\Program Files"),
        r"^InstallBuilder (Professional|Enterprise|For (?:Windows|OS X)) (\d+)\.(\d+)\.(\d+)$",
    ),
}


class _InstallBuilderEdition(Enum):
    PROFESSIONAL = "Professional"
    ENTERPRISE = "Enterprise"
    WINDOWS = "For Windows"
    OSX = "For OS X"
    LINUX = ""

    @staticmethod
    def from_str(edition: str):
        if edition == "Professional":
            return _InstallBuilderEdition.PROFESSIONAL
        elif edition == "Enterprise":
            return _InstallBuilderEdition.ENTERPRISE
        elif edition == "For Windows":
            return _InstallBuilderEdition.WINDOWS
        elif edition == "For OS X":
            return _InstallBuilderEdition.OSX
        elif edition == "":
            return _InstallBuilderEdition.LINUX
        else:
            raise ValueError(f"Unknown edition: {edition}")


@dataclass
class _Semver:
    major: int
    minor: int
    patch: int


@dataclass
class _InstallBuilderInstallation:
    path: Path
    version: _Semver
    edition: _InstallBuilderEdition

    @staticmethod
    def _sort_key(install: "_InstallBuilderInstallation") -> tuple[int, int, int, int]:
        # Prioritize Enterprise, then Professional, then any other edition
        edition_priority = {
            _InstallBuilderEdition.ENTERPRISE: 0,
            _InstallBuilderEdition.PROFESSIONAL: 1,
        }
        edition_value = edition_priority.get(install.edition, 2)
        return (
            -install.version.major,
            -install.version.minor,
            -install.version.patch,
            edition_value,
        )


def _get_default_installbuilder_location() -> Path:
    """
    Returns the default location where InstallBuilder Professional will be installed depending on the platform.
    """
    if platform.system() not in _INSTALL_BUILDER_SEARCH_CONFIGS:
        raise UnsupportedOSError(f"Unsupported OS for building installer: {platform.system()}")
    config = _INSTALL_BUILDER_SEARCH_CONFIGS[platform.system()]
    candidates = []
    for install_dir in config.parent_path.iterdir():
        if install_dir.is_dir():
            match = config.get_install_directory_regex().match(install_dir.name)
            if match and (install_dir / "bin" / "builder").is_file():
                if platform.system() == "Linux":
                    version_offset = 0
                else:
                    version_offset = 1
                installation = _InstallBuilderInstallation(
                    install_dir,
                    _Semver(
                        int(match.group(1 + version_offset)),
                        int(match.group(2 + version_offset)),
                        int(match.group(3 + version_offset)),
                    ),
                    _InstallBuilderEdition.from_str(
                        match.group(1) if platform.system() != "Linux" else ""
                    ),
                )
                candidates.append(installation)
    candidates.sort(key=_InstallBuilderInstallation._sort_key)
    if not candidates:
        raise FileNotFoundError("Could not find a default InstallBuilder path.")
    return candidates[0].path
