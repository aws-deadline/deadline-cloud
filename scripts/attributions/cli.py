# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import argparse
import hashlib
import json
import os
import platform
import re
import subprocess
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
    "attrs": {
        "license_sha256": "882115c95dfc2af1eeb6714f8ec6d5cbcabf667caff8729f42420da63f714e9f",
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
        "license_sha256": "66b313cce80ed0623fc7db3f24863a0c80fd83eb341a46b57864158ae74faa56",
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
    "pyrsistent": {
        "license_sha256": "3fd3d3d1ab9c733ee453fbf3bbbaa845440d0d8c20d7b5a039d2e46a2ed7fc01",
        "spdx": _MIT,
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
        "license_sha256": "f8e9ef00c78be4d2da526b2c37e4099c920c7fe0b9d943bf0daeb2efc30a3a5b",
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
    },
    {
        "name": "pyinstaller",
        "attribution_path": "PYINSTALLER_LICENSE.txt",
    },
    {
        "name": "openssl",
        "attribution_path": "OPENSSL_LICENSE.txt",
    },
    {
        "name": "sqlite",
        "attribution_path": "SQLITE_ACKNOWLEDGEMENT.txt",
    },
    {
        "name": "VCRedist",
        "attribution_path": "VCREDIST_ACKNOWLEDGEMENT.txt",
    },
    {
        "name": "WindowsSDK",
        "attribution_path": "WINDOWS_SDK_ACKNOWLEDGEMENT.txt",
    },
    {
        "name": "pywin32",
        "attribution_path": "PYWIN32_LICENSE.txt",
    },
    {
        "name": "libffi",
        "attribution_path": "LIBFFI_LICENSE.txt",
    },
]

# Some packages specify their license but do not include it in the repository/package
# We attribute these manually using _ADDITIONAL_ATTRIBUTIONS
_EXPECTED_MISSING_LICENSE = {"pywin32"}


def _get_desired_python_version() -> str:
    if platform.system() == "Darwin":
        return "3.12"
    elif platform.system() == "Windows":
        return "3.10"
    elif platform.system() == "Linux":
        return "3.9"
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
        If the argument is anything else, check to see if it is a path to a file, if it is, assume this is the path to a Python interpreter
        """
        if arg == "mise":
            interpreter_path = PythonInstall._get_mise_interpreter_path(version, dev)
        elif arg == "uv":
            if not dev:
                raise RuntimeError("Cannot use uv for Python interpreter outside of dev mode")
            interpreter_path = None
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


def uv_pip(args: list[str], venv: Path) -> None:
    """
    Convenience function that calls `uv pip [args]` against the virtual envrionment at a given Path
    """
    subprocess.check_call(["uv", "pip", *args], env={**os.environ, "VIRTUAL_ENV": str(venv)})


def _get_sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf8")).hexdigest()


class _PackageLicenseInfo:
    name: str
    version: str
    license_text: str
    notice_text: Optional[str]
    expect_missing_license: bool

    def __init__(self, venv: Path, pip_license_info: dict[str, str]):
        name = pip_license_info["Name"]
        version = pip_license_info["Version"]

        if name == "UNKNOWN":
            raise RuntimeError("Package missing name")
        self.name = name

        if version == "UNKNOWN":
            raise RuntimeError(f"Package {name} missing version")
        self.version = version

        discovered_license_text = pip_license_info["LicenseText"]
        discovered_notice_text = pip_license_info["NoticeText"]

        license_text_override = self._get_license_text_override(venv)
        notice_text_override = self._get_notice_text_override(venv)

        if license_text_override is not None:
            self.license_text = license_text_override
        elif discovered_license_text == "UNKNOWN":
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


def _get_license_info(python_interpreter: PythonInstall) -> list[_PackageLicenseInfo]:
    repository_root = Path(__file__).parent.parent.parent
    with tempfile.TemporaryDirectory() as td:
        temp = Path(td)
        venv = temp / ".venv"
        python_args = python_interpreter.get_uv_venv_python_args()
        uv_venv_args = ["uv", "venv", venv, *python_args]
        subprocess.check_call(uv_venv_args)
        uv_pip(["install", repository_root], venv)
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
            ]
        )
        pip_licenses_parsed = json.loads(pip_licenses_output)
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

        pip_licenses_parsed = [
            package
            for package in pip_licenses_parsed
            if package["Name"] not in _EXPECTED_MISSING_LICENSE
        ]

        return [
            _PackageLicenseInfo(
                venv,
                pip_license_info,
            )
            for pip_license_info in pip_licenses_parsed
            if pip_license_info["Name"] != "deadline"
        ]


def generate_attributions_document(out_file: Path, python_arg: Optional[str], dev: bool) -> None:
    """
    Generate an attributions document for this package and write it to `out_file`
    """
    desired_python_version = _get_desired_python_version()
    python_install = PythonInstall(python_arg, desired_python_version, dev)
    license_info = _get_license_info(python_install)
    attributions = []

    for package in license_info:
        package.check_against_attributions_allow_list()
        attributions.append(package.get_attribution_text())

    additional_attributions_path = Path(__file__).parent / "additional"
    for attribution in _ADDITIONAL_ATTRIBUTIONS:
        with open(
            additional_attributions_path / attribution["attribution_path"], "r", encoding="utf8"
        ) as f:
            attributions.append(f"{attribution['name']}\n\n{f.read()}\n")

    attributions = "".join(attributions)
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
    args = parser.parse_args()

    generate_attributions_document(args.out_file, args.python, args.dev)
