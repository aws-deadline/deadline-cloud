# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import argparse

import difflib
import hashlib
import json
import os
import platform
import re
import subprocess
import sys
import tempfile

from pathlib import Path
from typing import Optional

_PYTHON_VERSION_REGEX = re.compile(r"Python (\d+)\.(\d+)\.(\d+)")

# Used to specify a path relative to package's .dist-info directory
# containing the license text for cases where pip-licenses misses
# it or finds the incorrect file.
# This is especially useful for dual licensed packages where the actual license
# text is in a different file.
_LICENSE_PATH_OVERRIDES = {
    "packaging": "licenses/LICENSE.APACHE",
}

# Same as _LICENSE_PATH_OVERRIDES but for notice files
_NOTICE_PATH_OVERRIDES = {}

# SPDX Identifier Strings
_APACHE_2_0 = "Apache-2.0"
_BSD_2_CLAUSE = "BSD-2-Clause"
_BSD_3_CLAUSE = "BSD-3-Clause"
_MIT = "MIT"
_PSF_2_0 = "PSF-2.0"


# Packages that we're expecting pip-licenses to find with sha256 hashes of the known license
# file. If the package has a notice file as well, the sha256 of that should be included as well.
# SPDX License identifiers are also included for informational purposes.
_ATTRIBUTIONS_ALLOW_LIST = {
    "PyYAML": {
        "license_sha256": "8d3928f9dc4490fd635707cb88eb26bd764102a7282954307d3e5167a577e8a4",
        "spdx": _MIT,
    },
    "QtPy": {
        "license_sha256": "59ec4225bd380e349a82e6482437ff9475eeb1c2e676a2d1185bb53315d45bf9",
        "spdx": _MIT,
    },
    "boto3": {
        "license_sha256": "0d542e0c8804e39aa7f37eb00da5a762149dc682d7829451287e11b938e94594",
        "notice_sha256": "04fb1e61484a7810f1ba09bb42bc01ca58c9af33927d7c5a21556e4c4d1c7fa4",
        "spdx": _APACHE_2_0,
    },
    "botocore": {
        "license_sha256": "0d542e0c8804e39aa7f37eb00da5a762149dc682d7829451287e11b938e94594",
        "notice_sha256": "1d1c5a6f3d68cb11f4fad1afa86a7450d0286f15c176ef0ea048f255b46d95b0",
        "spdx": _APACHE_2_0,
    },
    "click": {
        "license_sha256": "9a8ad106a394e853bfe21f42f4e72d592819a22805d991b5f3275029292b658d",
        "spdx": _BSD_3_CLAUSE,
    },
    "colorama": {
        "license_sha256": "cac35c02686e5d04a5a7140bfb3b36e73aed496656e891102e428886d7930318",
        "spdx": _BSD_3_CLAUSE,
    },
    "jmespath": {
        "license_sha256": "6eefacfa4d71b82d08408c751470ac8d9854538da2142cb27be0287fb13d0ab9",
        "spdx": _MIT,
    },
    "packaging": {
        "license_sha256": "0d542e0c8804e39aa7f37eb00da5a762149dc682d7829451287e11b938e94594",
        "spdx": _APACHE_2_0,
    },
    "psutil": {
        "license_sha256": "b89c063b3786e28e0c0a38f1931db61fed35e69dd2a2966fbecffee0f46c8d10",
        "spdx": _BSD_3_CLAUSE,
    },
    # Parts are BSD-3-Clause and parts are Apache-2.0
    "python-dateutil": {
        "license_sha256": "ba00f51a0d92823b5a1cde27d8b5b9d2321e67ed8da9bc163eff96d5e17e577e",
    },
    "s3transfer": {
        "license_sha256": "cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30",
        "notice_sha256": "d8354e0fa7fb762da7bc054a8a6774a0b310dfbbc78006393ee573a0e57612b5",
        "spdx": _APACHE_2_0,
    },
    "six": {
        "license_sha256": "4375ba20e2b9c6c4e7cad2940a628fd90e95cc3d50ee92aae755715d8ba1fbd0",
        "spdx": _MIT,
    },
    "typing_extensions": {
        "license_sha256": "3b2f81fe21d181c499c59a256c8e1968455d6689d269aa85373bfb6af41da3bf",
        "spdx": _PSF_2_0,
    },
    "urllib3": {
        "license_sha256": "130e3a64d5fdd5d096a752694634a7d9df284469de86e5732100268041e3d686",
        "license_sha256_3.9": "c37bf186e27cf9dbe9619e55edfe3cea7b30091ceb3da63c7dacbe0e6d77907b",
        "spdx": _MIT,
    },
    "xxhash": {
        "license_sha256": "b3c620adc9a8812cc0c3c7fc09567fa9a9930d3546de955dfb955b168cf989ac",
        "spdx": _BSD_2_CLAUSE,
    },
}

# Some packages don't work with pip-licenses and we need to attribute them
# manually.
# "attribution_path" is the path of the license file for the package relative to
# the "additional" directory which is in the same directory as this file.
_ADDITIONAL_ATTRIBUTIONS = [
    {
        "name": "python",
        "attribution_path": "PYTHON_LICENSE.txt",
        "spdx": _PSF_2_0,
        "url": "https://github.com/python/cpython",
    },
    {
        "name": "pyinstaller",
        "attribution_path": "PYINSTALLER_LICENSE.txt",
        # Only the runtime hooks are distributed which are licensed under Apache-2.0
        "spdx": _APACHE_2_0,
        "url": "https://github.com/pyinstaller/pyinstaller",
    },
    {
        "name": "openssl",
        "attribution_path": "OPENSSL_LICENSE.txt",
        "spdx": _APACHE_2_0,
        "url": "https://github.com/openssl/openssl",
    },
    {
        "name": "sqlite",
        "attribution_path": "SQLITE_ACKNOWLEDGEMENT.txt",
        "spdx": "blessing",
        "url": "https://github.com/sqlite/sqlite",
    },
    {
        "name": "VCRedist",
        "attribution_path": "VCREDIST_ACKNOWLEDGEMENT.txt",
        "platforms": ["Windows"],
    },
    {
        "name": "WindowsSDK",
        "attribution_path": "WINDOWS_SDK_ACKNOWLEDGEMENT.txt",
        "platforms": ["Windows"],
    },
    {
        "name": "pywin32",
        "attribution_path": "PYWIN32_LICENSE.txt",
        "spdx": _PSF_2_0,
        "url": "https://github.com/mhammond/pywin32",
        "platforms": ["Windows"],
        "exclude_from_inventory": True,
    },
    {
        "name": "libffi",
        "attribution_path": "LIBFFI_LICENSE.txt",
        "spdx": _MIT,
        "url": "https://github.com/libffi/libffi",
    },
    {
        "name": "ncurses",
        "attribution_path": "NCURSES_LICENSE.txt",
        "spdx": "X11",
        "url": "https://invisible-island.net/ncurses/",
        "platforms": ["Linux", "Darwin"],
    },
    {
        "name": "liblzma",
        "attribution_path": "LZMA_ACKNOWLEDGEMENT.txt",
        "url": "https://tukaani.org/xz/",
    },
    {
        "name": "libmpdec",
        "attribution_path": "MPDEC_LICENSE.txt",
        "spdx": _BSD_2_CLAUSE,
        "url": "https://www.bytereef.org/mpdecimal/index.html",
    },
    {
        "name": "tcl",
        "attribution_path": "TCL_LICENSE.txt",
        "url": "https://github.com/tcltk/tcl",
        "platforms": ["Windows", "Darwin"],
    },
    {
        "name": "tk",
        "attribution_path": "TK_LICENSE.txt",
        "url": "https://github.com/tcltk/tk",
        "platforms": ["Windows", "Darwin"],
    },
    {
        "name": "zlib",
        "attribution_path": "ZLIB_LICENSE.txt",
        "url": "https://github.com/madler/zlib",
        "spdx": "Zlib",
    },
    {
        "name": "libzstd",
        "attribution_path": "LIBZSTD_LICENSE.txt",
        "url": "https://github.com/facebook/zstd/",
        "spdx": _BSD_3_CLAUSE,
        "platforms": ["Linux"],
    },
    {
        "name": "bzip2",
        "attribution_path": "BZIP2_LICENSE.txt",
        "url": "https://gitlab.com/bzip2/bzip2",
        "spdx": "bzip2-1.0.6",
    },
    {
        "name": "libexpat",
        "attribution_path": "LIBEXPAT_LICENSE.txt",
        "url": "https://github.com/libexpat/libexpat",
        "spdx": _MIT,
    },
]

# Some packages specify their license but do not include it in the repository/package
# We attribute these manually using _ADDITIONAL_ATTRIBUTIONS
_EXPECTED_MISSING_LICENSE = {"pywin32"}


def _get_desired_python_version() -> str:
    if platform.system() == "Darwin":
        return "3.13"
    elif platform.system() == "Windows":
        return "3.13"
    elif platform.system() == "Linux":
        return "3.13"
    raise RuntimeError("Platform not supported")


class PythonInstall:
    _interpreter_path: Optional[Path]
    _version: str
    _dev: bool

    def __init__(self, arg: str, version: str, dev: bool):
        """
        Create a python installation based in the passed in --python argument
        If the argument was "mise", query mise for an installed python interpreter of the desired version (only allowed in dev mode)
        If the argument was "uv", let uv install the desired python version (only allowed in dev mode)
        If the argument is "current", use the current Python interpreter
        If the argument is anything else, check to see if it is a path to a file, if it is, assume this is the path to a Python interpreter
        """
        if arg == "mise":
            interpreter_path = PythonInstall._get_mise_interpreter_path(version, dev)
        elif arg == "uv":
            if not dev:
                raise RuntimeError("Cannot use uv for Python interpreter outside of dev mode")
            interpreter_path = None
        elif arg == "current":
            interpreter_path = Path(sys.executable)
        else:
            interpreter_path = Path(arg)

        if interpreter_path is not None:
            if not interpreter_path.is_file():
                raise RuntimeError(
                    "Specified python interpreter path either doesn't exist or is not a file"
                )
            python_version_output = subprocess.check_output(
                [interpreter_path, "--version"], text=True
            )
            version_match = _PYTHON_VERSION_REGEX.match(python_version_output)
            if version_match is None:
                raise RuntimeError(
                    f"Python interpreter candidate at {interpreter_path} is not a Python interpreter"
                )
            interpreter_version = f"{version_match.group(1)}.{version_match.group(2)}"
            if interpreter_version != version:
                raise RuntimeError(
                    f"Python interpreter candidate at {interpreter_path} has version {interpreter_version} which does not match specified version {version}"
                )

        self._interpreter_path = interpreter_path
        self._version = version
        self._dev = dev

    @staticmethod
    def _get_mise_interpreter_path(version: str, dev: bool) -> Path:
        if not dev:
            raise RuntimeError("Cannot use mise for Python interpreter outside of dev mode")

        python_install_path = Path(
            subprocess.check_output(["mise", "where", f"python@{version}"], text=True).strip()
        )
        if not python_install_path.is_dir():
            raise RuntimeError(
                f"mise where python@{version} returned {python_install_path} which is not a directory."
            )
        if platform.system() == "Windows":
            python_exe_name = "python.exe"
        else:
            python_exe_name = "python"
        return python_install_path / "bin" / python_exe_name

    def get_uv_venv_python_args(self) -> list[str]:
        if self._dev:
            if self._interpreter_path is None:
                return ["--python", self._version]
            else:
                return [
                    "--python",
                    str(self._interpreter_path),
                    "--python-preference",
                    "only-system",
                    "--no-python-downloads",
                ]
        else:
            return [
                "--python",
                str(self._interpreter_path),
                "--python-preference",
                "only-system",
                "--no-python-downloads",
            ]


def get_pip_index_url() -> Optional[str]:
    try:
        result = subprocess.check_output(
            ["pip", "config", "get", "global.index-url"], text=True, stderr=subprocess.STDOUT
        )
        return result.strip()
    except subprocess.CalledProcessError as e:
        if "No such key" in e.output:
            return None
        raise


def uv_pip(args: list[str], venv: Path, dev: bool) -> None:
    """
    Convenience function that calls `uv pip [args]` against the virtual envrionment at a given Path
    """
    index_args = []
    pip_index_url = get_pip_index_url()
    if pip_index_url is not None:
        index_args = ["--default-index", pip_index_url]
    else:
        if not dev:
            raise RuntimeError("Expected a pip index url to be configured outside of dev mode.")
    subprocess.check_call(
        ["uv", "pip", *args, *index_args], env={**os.environ, "VIRTUAL_ENV": str(venv)}
    )


def _get_sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf8")).hexdigest()


class _PackageLicenseInfo:
    name: str
    version: str
    license_text: str
    notice_text: Optional[str]
    expect_missing_license: bool
    url: Optional[str]
    license: Optional[str]

    def __init__(self, venv: Path, pip_license_info: dict[str, str]):
        name = pip_license_info["Name"]
        version = pip_license_info["Version"]
        url = pip_license_info["URL"]
        license = pip_license_info["License"]

        if name == "UNKNOWN":
            raise RuntimeError("Package missing name")
        self.name = name

        if version == "UNKNOWN":
            raise RuntimeError(f"Package {name} missing version")
        self.version = version

        if url == "UNKNOWN":
            self.url = None
        else:
            self.url = url

        if name in _ATTRIBUTIONS_ALLOW_LIST and "spdx" in _ATTRIBUTIONS_ALLOW_LIST[name]:
            self.license = _ATTRIBUTIONS_ALLOW_LIST[name]["spdx"]
        elif license == "UNKNOWN":
            self.license = None
        else:
            self.license = license

        discovered_license_text = pip_license_info["LicenseText"]
        discovered_notice_text = pip_license_info["NoticeText"]

        license_text_override = self._get_license_text_override(venv)
        notice_text_override = self._get_notice_text_override(venv)

        if license_text_override is not None:
            self.license_text = license_text_override
        elif discovered_license_text == "UNKNOWN" and name not in _EXPECTED_MISSING_LICENSE:
            raise RuntimeError(f"Package {name} missing license text")
        else:
            self.license_text = discovered_license_text

        if notice_text_override is not None:
            self.notice_text = notice_text_override
        elif discovered_notice_text == "UNKNOWN":
            self.notice_text = None
        else:
            self.notice_text = discovered_notice_text

    def _get_license_text_override(self, venv: Path) -> Optional[str]:
        if self.name not in _LICENSE_PATH_OVERRIDES:
            return None
        dist_info_path = self._get_dist_info_path(venv)
        if not dist_info_path.is_dir():
            raise RuntimeError(f".dist-info path for {self.name} does not exist")
        with open(dist_info_path / _LICENSE_PATH_OVERRIDES[self.name], "r", encoding="utf8") as f:
            return f.read()

    def _get_notice_text_override(self, venv: Path) -> Optional[str]:
        if self.name not in _NOTICE_PATH_OVERRIDES:
            return None
        dist_info_path = self._get_dist_info_path(venv)
        if not dist_info_path.is_dir():
            raise RuntimeError(f".dist-info path for {self.name} does not exist")
        with open(dist_info_path / _NOTICE_PATH_OVERRIDES[self.name], "r", encoding="utf8") as f:
            return f.read()

    def check_against_attributions_allow_list(self) -> None:
        if self.name not in _ATTRIBUTIONS_ALLOW_LIST:
            raise RuntimeError(
                f"Package {self.name} is not in the allow list for the attributions document"
            )

        license_sha256 = self.get_license_sha256()
        license_sha256_version_key = f"license_sha256_{_get_desired_python_version()}"
        if license_sha256_version_key in _ATTRIBUTIONS_ALLOW_LIST[self.name]:
            expected_sha256 = _ATTRIBUTIONS_ALLOW_LIST[self.name][license_sha256_version_key]
        else:
            expected_sha256 = _ATTRIBUTIONS_ALLOW_LIST[self.name]["license_sha256"]
        if license_sha256 != expected_sha256:
            raise RuntimeError(
                f"Package {self.name} has had a change to its license text since added to the allow list. Computed sha256 is {license_sha256}"
            )

        notice_sha256 = self.get_notice_sha256()
        if notice_sha256 is None and "notice_sha256" not in _ATTRIBUTIONS_ALLOW_LIST[self.name]:
            return
        if notice_sha256 is None and "notice_sha256" in _ATTRIBUTIONS_ALLOW_LIST[self.name]:
            raise RuntimeError(
                f"No notice file found for package {self.name} but the allow list has a sha256 for a notice file for {self.name}"
            )
        if notice_sha256 is not None and "notice_sha256" not in _ATTRIBUTIONS_ALLOW_LIST[self.name]:
            raise RuntimeError(
                f"Found notice file with sha256 {notice_sha256} for package {self.name}, but {self.name} does not have a notice file sha256 in teh allow list."
            )
        if notice_sha256 != _ATTRIBUTIONS_ALLOW_LIST[self.name]["notice_sha256"]:
            raise RuntimeError(
                f"Package {self.name} has had a change to its notice text since added to the allow list"
            )

    def get_attribution_text(self) -> str:
        if self.notice_text is None:
            notice_text = "\n"
        else:
            notice_text = f"\n{self.notice_text}\n"

        return f"{self.name}\n\n{self.license_text}{notice_text}"

    def get_license_sha256(self) -> str:
        return _get_sha256(self.license_text)

    def get_notice_sha256(self) -> Optional[str]:
        if self.notice_text is not None:
            return _get_sha256(self.notice_text)
        return None

    def _get_dist_info_path(self, venv: Path) -> Path:
        if platform.system() == "Windows":
            return venv / "Lib" / "site-packages" / f"{self.name}-{self.version}.dist-info"
        else:
            return (
                venv
                / "lib"
                / f"python{_get_desired_python_version()}"
                / "site-packages"
                / f"{self.name}-{self.version}.dist-info"
            )


def _get_license_info(python_interpreter: PythonInstall, dev: bool) -> list[_PackageLicenseInfo]:
    repository_root = Path(__file__).parent.parent.parent
    with tempfile.TemporaryDirectory() as td:
        temp = Path(td)
        venv = temp / ".venv"
        python_args = python_interpreter.get_uv_venv_python_args()
        uv_venv_args = ["uv", "venv", venv, *python_args]
        subprocess.check_call(uv_venv_args)
        uv_pip(["install", repository_root], venv, dev)
        if platform.system() == "Windows":
            python_path = venv / "Scripts" / "python.exe"
        else:
            python_path = venv / "bin" / "python"

        pip_licenses_output = subprocess.check_output(
            [
                "pip-licenses",
                "--from=meta",
                "--with-url",
                "--with-license-file",
                "--with-notice-file",
                "--format=json",
                f"--python={python_path}",
            ],
            text=True,
            encoding="utf-8",
        )
        pip_licenses_parsed = json.loads(pip_licenses_output)

        # Check for Unicode corruption
        for package in pip_licenses_parsed:
            license_text = package["LicenseText"]
            if "�" in license_text:
                raise RuntimeError(
                    f"Corrupted Unicode detected in license text for package {package['Name']}"
                )

        for package in pip_licenses_parsed:
            name = package["Name"]
            license_text = package["LicenseText"]
            notice_text = package["NoticeText"]
            if name in _EXPECTED_MISSING_LICENSE and license_text != "UNKNOWN":
                raise RuntimeError(
                    f"Expected pip-licenses to not find a license for {name} but one was found."
                )
            if license_text == "UNKNOWN" and notice_text != "UNKNOWN":
                raise RuntimeError(
                    f"pip-licenses found a notices file for {name} but no license file. This case is not handled."
                )
            if license_text == "UNKNOWN" and name not in _EXPECTED_MISSING_LICENSE:
                raise RuntimeError(
                    f"pip-licenses did not find a license file for {name} but it was expected to."
                )

        return [
            _PackageLicenseInfo(
                venv,
                pip_license_info,
            )
            for pip_license_info in pip_licenses_parsed
            if pip_license_info["Name"] != "deadline"
        ]


def packages_to_markdown_table(
    packages: list[_PackageLicenseInfo], additional: list[dict[str, str]]
) -> str:
    lines = ["| Name | Version | License | URL |", "| --- | --- | --- | --- |"]
    for pkg in packages:
        url = pkg.url or ""
        license_name = pkg.license or ""
        lines.append(f"| {pkg.name} | {pkg.version} | {license_name} | {url} |")
    for pkg in additional:
        if "platforms" not in pkg or platform.system() in pkg["platforms"]:
            if pkg.get("exclude_from_inventory", False):
                spdx = pkg.get("spdx", "")
                url = pkg.get("url", "")
                version = pkg.get("version", "")
                lines.append(f"| {pkg['name']} | {version} | {spdx} | {url} |")
    return "\n".join(lines)


def generate_attributions_document(
    out_file: Path,
    python_arg: Optional[str],
    dev: bool,
    inventory_file: Optional[Path],
    versions_file: Optional[Path],
) -> None:
    """
    Generate an attributions document for this package and write it to `out_file`
    """
    desired_python_version = _get_desired_python_version()
    python_install = PythonInstall(python_arg, desired_python_version, dev)
    license_info = _get_license_info(python_install, dev)
    attributions = []
    versions: dict[str, str] = {}

    for package in sorted(license_info, key=lambda info: info.name):
        versions[package.name] = package.version
        if package.name not in _EXPECTED_MISSING_LICENSE:
            package.check_against_attributions_allow_list()
            attributions.append(package.get_attribution_text())

    additional_attributions_path = Path(__file__).parent / "additional"
    for attribution in sorted(
        _ADDITIONAL_ATTRIBUTIONS, key=lambda attribution: attribution["name"]
    ):
        if "platforms" not in attribution or platform.system() in attribution["platforms"]:
            with open(
                additional_attributions_path / attribution["attribution_path"], "r", encoding="utf8"
            ) as f:
                attributions.append(f"{attribution['name']}\n\n{f.read()}\n")

    attributions = "".join(attributions)

    if inventory_file is not None:
        inventory = packages_to_markdown_table(license_info, _ADDITIONAL_ATTRIBUTIONS)
        with open(inventory_file, "w", encoding="utf8") as f:
            f.write(inventory)

    if versions_file is not None:
        with open(versions_file, "w", encoding="utf8") as f:
            json.dump(versions, f, indent=2)

    approved_text_path = (
        Path(__file__).parent / "approved_text" / platform.system() / "THIRD_PARTY_LICENSES"
    )

    # Read as bytes first, then decode explicitly
    # because otherwise certain unicode characters
    # are not decoded properly on Windows
    with open(approved_text_path, "rb") as f:
        raw_bytes = f.read()
    approved_text = raw_bytes.decode("utf-8").replace("\r\n", "\n").replace("\r", "\n")

    if approved_text != attributions:
        diff = "".join(
            difflib.unified_diff(
                approved_text.splitlines(keepends=True),
                attributions.replace("\r\n", "\n").replace("\r", "\n").splitlines(keepends=True),
                lineterm="",
            )
        )
        print("ERROR: Attributions generated did not match approved text:", file=sys.stderr)
        print(diff, file=sys.stderr)
        sys.exit(1)

    with open(out_file, "w", encoding="utf8") as f:
        f.write(attributions)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-o",
        "--out-file",
        type=Path,
        required=True,
        help="The path to output the attributions document to",
    )
    parser.add_argument(
        "--python",
        type=str,
        required=True,
        help="Argument to pass to `uv venv --python <arg>` when creating a venv",
    )
    parser.add_argument(
        "--dev",
        action="store_true",
        required=False,
        help="If set, `--python-preference only-system --no-python-downloads` will not be passed to `uv venv` so that uv can download python.",
    )
    parser.add_argument(
        "--inventory-file",
        type=Path,
        required=False,
        help="The path to output the package inventory to",
    )
    parser.add_argument(
        "--versions-file",
        type=Path,
        required=False,
        help="The path to output the versions file to",
    )
    args = parser.parse_args()

    generate_attributions_document(
        args.out_file, args.python, args.dev, args.inventory_file, args.versions_file
    )
