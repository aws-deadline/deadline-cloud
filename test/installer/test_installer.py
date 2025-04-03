# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
import getpass
import glob
import os
import platform
import re
import shutil
import stat
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import DefaultDict, List

import pytest


def _is_admin() -> bool:
    """Platform independent utility to determine if the tests are running with
    elevated privileges"""
    # sys.platform helps mypy type checking ignore other platforms
    if sys.platform != "win32":
        return os.getuid() == 0

    import ctypes

    try:
        return ctypes.windll.shell32.IsUserAnAdmin() == 1
    except Exception:
        return False


@pytest.fixture(scope="session")
def installer_path():
    path = "DeadlineCloudClient-{platform}-installer.{ext}"

    if platform.system() == "Darwin":
        path = os.path.join(
            path.format(platform="osx", ext="app"),
            "Contents",
            "MacOS",
            "installbuilder.sh",
        )
    elif platform.system() == "Windows":
        path = path.format(platform="windows-x64", ext="exe")
    elif platform.system() == "Linux":
        path = path.format(platform="linux-x64", ext="run")

    if not os.path.isfile(path):
        raise FileNotFoundError(f"Installer not found at '{path}'")

    if not os.access(path, os.X_OK) and not platform.system() == "Darwin":
        raise PermissionError(f"Installer at '{path}' is not executable")

    yield Path(path).absolute()


def _run_installer(installer_path, install_scope, installation_path) -> Path:
    # use a path that does not exist
    installation_path = installation_path / "dne"
    args = [
        installer_path,
        "--mode",
        "unattended",
        "--installscope",
        install_scope,
        "--prefix",
        installation_path,
    ]
    subprocess.run(args, check=True)

    return Path(installation_path)


def _validate_files(installation_path: Path) -> None:
    if platform.system() == "Darwin":
        uninstaller = "uninstall.app"
    elif platform.system() == "Windows":
        uninstaller = "uninstall.exe"
    else:
        uninstaller = "uninstall"

    # THEN
    top_level_dir = [f.name for f in installation_path.iterdir()]
    assert "DeadlineClient" in top_level_dir
    assert "installer_version.txt" in top_level_dir
    assert uninstaller in top_level_dir

    # Check main CLI runs
    cli_path = installation_path / "DeadlineClient" / "deadline"
    cli_result = subprocess.run([cli_path, "--version"], capture_output=True, text=True)
    version_regex = r"deadline, version (\d+)\.(\d+)\.(\d+)"
    match = re.search(version_regex, cli_result.stdout)
    assert match is not None, f"--version output is not as expected, got: {cli_result.stdout}"
    assert cli_result.returncode == 0

    # Just check that we have dependencies in this folder
    cli_dir = installation_path / "DeadlineClient" / "cli"
    cli_dir_contents = [f.name for f in (cli_dir).iterdir()]
    assert "deadline" in cli_dir_contents
    assert "xxhash" in cli_dir_contents

    # Check the deadline module is here and there's a version file
    client_dir = [f.name for f in (cli_dir.joinpath("deadline", "client")).iterdir()]
    assert "_version.py" in client_dir


@pytest.fixture(scope="session")
def user_installation(installer_path, tmp_path_factory):
    """Used for tests that just want to assert some facts around the install but do not modify"""
    tmp_path = tmp_path_factory.mktemp("install")
    yield _run_installer(installer_path, "user", tmp_path)


@pytest.fixture(scope="session")
def system_installation(installer_path, tmp_path_factory):
    """Used for tests that just want to assert some facts around the install but do not modify"""
    tmp_path = tmp_path_factory.mktemp("install")
    yield _run_installer(installer_path, "system", tmp_path)


@pytest.fixture(scope="function")
def per_test_user_installation(installer_path, tmp_path):
    """Used for tests that modify the installation"""
    yield _run_installer(installer_path, "user", tmp_path)


@pytest.fixture(scope="function")
def per_test_system_installation(installer_path, tmp_path):
    """Used for tests that modify the installation"""
    yield _run_installer(installer_path, "system", tmp_path)


@pytest.fixture(scope="function")
def uninstaller_path():
    uninstaller_path = Path("uninstall")
    if platform.system() == "Darwin":
        uninstaller_path = Path("uninstall.app", "Contents", "MacOS", "installbuilder.sh")
    elif platform.system() == "Windows":
        uninstaller_path = uninstaller_path.with_suffix(".exe")

    yield uninstaller_path


def test_default_location(installer_path: Path):
    """Ensures that the default output location reported by the installer is accurate.
       The help text will only show it for the default scope (user). Example help output:

    --prefix <prefix>                           Installation Directory
                                                Default: /home/<user>/DeadlineCloudClient
    """
    # GIVEN
    default_install_location = Path("~/DeadlineCloudClient").expanduser()
    default_pattern = r"Default: (.*)"
    location = None

    # WHEN
    text_mode = [] if sys.platform == "win32" else ["--mode", "text"]
    # Since windows doesn't have text mode, it'll pop-up a gui. We use the timeout to ensure it stops
    try:
        help_result = subprocess.run(
            [installer_path, *text_mode, "--help"], capture_output=True, text=True, timeout=5
        )
        assert help_result.returncode == 0, (
            f"Installer exited with non-zero code: {help_result.returncode}"
        )
        assert help_result.stdout is not None, "No stdout from --help"
        help_output = iter(help_result.stdout.splitlines())
    except subprocess.TimeoutExpired as e:
        assert e.stdout is not None, "No stdout from --help"
        # mypy/docs say this should be bytes, but was str?
        assert isinstance(e.stdout, str)
        help_output = iter(e.stdout.splitlines())

    # THEN
    while (line := next(help_output, None)) is not None:
        if line.strip().startswith("--prefix"):
            location = re.match(default_pattern, next(help_output, "").strip(), flags=re.IGNORECASE)
            break

    assert location is not None, (
        f"Could not find default install location in help output:\n{help_result.stdout}"
    )
    if platform.system() != "Windows":
        assert location.group(1) == default_install_location.as_posix()
    else:
        assert str(Path(location.group(1))) == str(default_install_location)


@pytest.mark.skipif(
    os.getenv("CODEBUILD_SRC_DIR") is None or os.getenv("IS_DEV_BUILD", "false").lower() == "true",
    reason="Only installers built with a license will not be evaluation mode",
)
@pytest.mark.skipif(
    sys.platform == "win32", reason="CLI usage for Windows does not make the eval text available"
)
def test_did_not_build_with_evaluation_mode(installer_path: Path, tmp_path: Path):
    """Tests to see if there's an evaluation version header from installbuilder.

    This is done by launching the installer, exiting before it completes, and checking the output
    does NOT contain a specific line entry. Unfortunately this is makes the test pretty fragile, but
    the behaviour has existed for years. If it exists it's expected to be the second line, but we check
    the top few lines that we're guaranteed to have in case it shifts a tiny bit.

    note: tmp_path is leveraged to ensure that the user's install is not messed with if the test does not
    behave correctly."""
    # GIVEN
    eval_text = r"Created with an evaluation version of InstallBuilder"
    output = []

    # WHEN
    proc = subprocess.Popen(
        [installer_path, "--mode", "text", "--prefix", tmp_path],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    try:
        # We want to fail the installer fast so that it doesn't proceed to install with the defaults
        proc.wait(0.5)
    except subprocess.TimeoutExpired:
        # expected behaviour due to the above wait happening before the installer finishes
        pass
    finally:
        proc.terminate()
        proc.kill()

    # THEN
    assert proc.stdout is not None
    # Example header from installbuilder
    """----------------------------------------------------------------------------
    Created with an evaluation version of InstallBuilder

    Welcome to the AWS Deadline Cloud Client Setup Wizard.

    ----------------------------------------------------------------------------
    """
    output = [proc.stdout.readline().strip() for _ in range(6)]
    assert output

    for line in output:
        assert eval_text not in line, (
            "Installer was detected to have been built with Evaluation mode"
        )


@pytest.mark.skipif(platform.system() == "Windows", reason="Only run on Linux and MacOS")
class TestLinuxAndMacOS:
    def test_user_permissions(self, user_installation: Path):
        # GIVEN / WHEN / THEN
        self._validate_posix_permissions(user_installation)

    @pytest.mark.skipif(not _is_admin(), reason="Tests requires admin privileges")
    def test_system_permissions(self, system_installation):
        # GIVEN / WHEN / THEN
        self._validate_posix_permissions(system_installation)

    def _has_group_other_write(self, mode: int) -> bool:
        return bool(mode & (stat.S_IWGRP | stat.S_IWOTH))

    def _has_user_read_write(self, mode: int) -> bool:
        return bool(mode & (stat.S_IRUSR | stat.S_IWUSR))

    def _has_user_group_other_execute(self, mode: int) -> bool:
        return bool(mode & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH))

    def _validate_posix_permissions(self, installation_path: Path):
        # assists mypy type checking to ignore this on Windows
        assert sys.platform != "win32"
        # pwd is not available on Windows
        import pwd

        # GIVEN
        current_user = pwd.getpwuid(os.getuid())[0]  # type: ignore

        # WHEN
        bad_perms: DefaultDict[Path, List[str]] = defaultdict(list)
        for entry in [installation_path, *installation_path.rglob("*")]:
            mode = stat.S_IMODE(entry.stat().st_mode)
            if self._has_group_other_write(mode):
                bad_perms[entry].append("should not have group/other write permissions")
            if not self._has_user_read_write(mode):
                bad_perms[entry].append("should have user read/write permissions")
            if entry.is_dir() and not self._has_user_group_other_execute(mode):
                bad_perms[entry].append("is a directory and should have execute permissions")
            if entry.owner() != current_user:  # type: ignore
                bad_perms[entry].append(f"is not owned by the '{current_user}'")

        error_message = [f"Found {len(bad_perms)} instance(s) of incorrect permissions"]
        for i, entry in enumerate(bad_perms):
            fmted_reasons = "\n  - ".join(reason for reason in bad_perms[entry])
            error_message.append(
                f"{i + 1}. ({stat.S_IMODE(entry.stat().st_mode):o}) '{Path(*entry.parts[len(installation_path.parts) :])!s:>3}'\n"
                f"  - {fmted_reasons}"
            )

        # THEN
        assert len(bad_perms) == 0, "\n".join(error_message)


@pytest.mark.skipif(platform.system() != "Windows", reason="Only run on Windows")
class TestWindows:
    def test_user_permissions(self, user_installation):
        # GIVEN / WHEN / THEN
        self._verify_windows_least_privilege(user_installation)

    @pytest.mark.skipif(not _is_admin(), reason="Tests requires admin privileges")
    def test_system_permissions(self, system_installation):
        # GIVEN / WHEN / THEN
        self._verify_windows_least_privilege(system_installation)

    def _running_in_container(self) -> bool:
        """
        Check to see if the cexecsvc service exists and is running
        to determine if we're running on a container.
        """
        # assists mypy type checking to ignore this on non-Windows
        assert sys.platform == "win32"
        import win32service
        import win32serviceutil

        try:
            service_status = win32serviceutil.QueryServiceStatus("cexecsvc")
            return service_status[1] == win32service.SERVICE_RUNNING
        except win32service.error:
            # Service doesn't exist, not on a container
            return False

    def _verify_windows_least_privilege(self, installation_path: Path):
        # assists mypy type checking to ignore this on non-Windows
        assert sys.platform == "win32"
        import ntsecuritycon
        import win32con
        import win32file
        import win32security

        # GIVEN
        windows_user = getpass.getuser()

        if self._running_in_container():
            # The admin group is different when running
            # in a container.
            admin_group = "ContainerAdministrator"
        else:
            admin_group = "Administrators"

        builtin_admin_group_sid, _, _ = win32security.LookupAccountName(None, admin_group)
        user_sid, _, _ = win32security.LookupAccountName(None, windows_user)

        # WHEN
        bad_perms: DefaultDict[Path, List[str]] = defaultdict(list)
        for path in [installation_path, *installation_path.rglob("*")]:
            sd = win32security.GetFileSecurity(
                str(path),
                win32con.DACL_SECURITY_INFORMATION | win32con.OWNER_SECURITY_INFORMATION,
            )

            # Verify ownership
            owner_sid = sd.GetSecurityDescriptorOwner()
            if _is_admin():
                if builtin_admin_group_sid != owner_sid:
                    bad_perms[path].append(
                        f"Expected to be owned by '{admin_group}' but got '{win32security.LookupAccountSid(None, owner_sid)}'"
                    )
            elif user_sid != owner_sid:
                bad_perms[path].append(
                    f"Expected to be owned by '{win32security.LookupAccountSid(None, user_sid)}' but got '{win32security.LookupAccountSid(None, owner_sid)}'"
                )

            # Verify all ACEs
            dacl = sd.GetSecurityDescriptorDacl()
            if dacl.GetAceCount() != 3:
                bad_perms[path].append(f"Expected 3 ACEs, but was {dacl.GetAceCount()}")

            for ace in [dacl.GetAce(i) for i in range(dacl.GetAceCount())]:
                _ace_info, mask, sid = ace
                ace_type, ace_flags = _ace_info

                if sid not in [builtin_admin_group_sid, user_sid]:
                    continue
                if ace_type != ntsecuritycon.ACCESS_ALLOWED_ACE_TYPE:
                    bad_perms[path].append(
                        f"Expected ACE type {ntsecuritycon.ACCESS_ALLOWED_ACE_TYPE} but got {ace_type}"
                    )
                if (
                    path.is_dir()
                    and ace_flags
                    != ntsecuritycon.OBJECT_INHERIT_ACE | ntsecuritycon.CONTAINER_INHERIT_ACE
                ):
                    bad_perms[path].append(
                        f"Expected inheritance in ACE to be {ntsecuritycon.OBJECT_INHERIT_ACE | ntsecuritycon.CONTAINER_INHERIT_ACE} but got {ace_flags}"
                    )
                if mask != win32file.FILE_ALL_ACCESS:
                    bad_perms[path].append(
                        f"Expected only FILE_ALL_ACCESS ({win32file.FILE_ALL_ACCESS}) ACEs but got {mask}"
                    )

        error_message = [f"Found {len(bad_perms)} instance(s) of incorrect permissions"]
        for i, path in enumerate(bad_perms):
            fmted_reasons = "\n  - ".join(reason for reason in bad_perms[path])
            error_message.append(
                f"{i + 1}. '{Path(*path.parts[len(installation_path.parts) :])!s:>3}'\n"
                f"  - {fmted_reasons}"
            )

        # THEN
        assert len(bad_perms) == 0, "\n".join(error_message)


WINDOWS_RETRY_DELAY = 1
WINDOWS_MAX_RETRIES = 100


class TestUserInstall:
    def test_install(self, user_installation: Path):
        # GIVEN / WHEN / THEN
        _validate_files(user_installation)

    def test_uninstall(self, per_test_user_installation: Path, uninstaller_path: Path):
        # GIVEN / WHEN
        result = subprocess.run(
            [per_test_user_installation / uninstaller_path, "--mode", "unattended"]
        )

        # THEN
        assert result.returncode == 0

        # On Windows, the uninstall process will return before the uninstallation is complete.
        # If necessary, wait for up to 1 minute 40 seconds before timing out.
        if platform.system() == "Windows":
            for _ in range(WINDOWS_MAX_RETRIES):
                if not per_test_user_installation.exists():
                    break
                time.sleep(WINDOWS_RETRY_DELAY)
        assert not per_test_user_installation.exists()


@pytest.mark.skipif(not _is_admin(), reason="Tests requires admin privileges")
class TestSystemInstall:
    def test_install(self, system_installation: Path):
        # GIVEN / WHEN / THEN
        _validate_files(system_installation)

    def test_uninstall(self, per_test_system_installation: Path, uninstaller_path: Path):
        # GIVEN / WHEN
        result = subprocess.run(
            [per_test_system_installation / uninstaller_path, "--mode", "unattended"],
            capture_output=True,
            text=True,
        )

        # THEN
        assert result.returncode == 0

        # On Windows, the uninstall process will return before the uninstallation is complete.
        # If necessary, wait for up to 1 minute 40 seconds before timing out.
        if platform.system() == "Windows":
            for _ in range(WINDOWS_MAX_RETRIES):
                if not per_test_system_installation.exists():
                    break
                time.sleep(WINDOWS_RETRY_DELAY)
        assert not per_test_system_installation.exists()


@pytest.mark.skipif(
    os.getenv("CODEBUILD_SRC_DIR") is None or os.getenv("IS_DEV_BUILD", "false").lower() == "true",
    reason="Only installers built internally will be signed",
)
class TestVerifySigning:
    @pytest.mark.skipif(platform.system() != "Windows", reason="Only run on Windows")
    def test_windows_signing(self, installer_path):
        """Assumes that the Windows SDK is installed so we can find signtool:
            C:/Program Files*/Windows Kits/*/bin/*/x64/signtool.exe
        Example success:

            Index  Algorithm  Timestamp
            ========================================
            0      sha256     Authenticode

            Successfully verified: C:\*.exe

        Example failure:

            Index  Algorithm  Timestamp
            ========================================
            SignTool Error: No signature found.

            Number of errors: 1
        """
        # GIVEN
        # Check PATH, then SDK installation location
        signtool = shutil.which("signtool")
        if not signtool:
            signtool = next(
                glob.iglob("C:/Program Files*/Windows Kits/*/bin/*/x64/signtool.exe"), None
            )
        assert signtool, "signtool not found in expected location"

        # WHEN
        result = subprocess.run(
            [signtool, "verify", "/pa", installer_path], capture_output=True, text=True
        )

        # THEN
        assert "SignTool Error:" not in result.stderr, "signtool did not succeed"
        assert result.returncode == 0
        assert "Successfully verified:" in result.stdout

    @pytest.mark.skipif(platform.system() != "Linux", reason="Only run on Linux")
    def test_linux_signing(self, installer_path):
        """Assumes that gpg is on the PATH, and that the public key has already been imported"""
        # GIVEN
        gpg = shutil.which("gpg")
        assert gpg, "gpg not found in PATH"

        # WHEN
        result = subprocess.run(
            [gpg, "--verify", f"{installer_path}.sig", installer_path],
            capture_output=True,
            text=True,
        )

        # THEN
        assert "Can't check signature: No public key" not in result.stderr, (
            "Missing Public Key in keyring"
        )
        assert result.returncode == 0, "Code signing validation failed"
        # gpg shoves the success message into stderr for some reason
        # This matches what customers are told to do via the public docs:
        #   https://docs.aws.amazon.com/deadline-cloud/latest/userguide/submitter.html#verify-installer
        assert (
            'Good signature from "AWS Deadline Cloud <aws-deadline@amazon.com>"' in result.stderr
        ), "Signing succeeded, but did not match docs instructions"

    @pytest.mark.skipif(platform.system() != "Darwin", reason="Only run on MacOS")
    def test_macos_signing(self, installer_path):
        """Tests that the relevant files are signed on MacOS. Assumes codesign and spectl are on the PATH

        Leverages codesign to check signing, and spctl to verify that MacOS does not complain about running it
        """
        # GIVEN
        codesign = shutil.which("codesign")
        spctl = shutil.which("spctl")
        assert codesign, "codesign not found in PATH"
        assert spctl, "spctl not found in PATH"

        # WHEN / THEN
        codesign_result = subprocess.run(
            [codesign, "--verify", "--deep", "--verbose", installer_path],
            capture_output=True,
            text=True,
        )
        assert "code object is not signed at all" not in codesign_result.stdout, (
            "The file is not signed"
        )
        assert codesign_result.returncode == 0

        # WHEN / THEN
        spctl_result = subprocess.run(
            [spctl, "--verbose", "--assess", "--type", "execute", installer_path],
            capture_output=True,
            text=True,
        )
        assert "rejected" not in spctl_result.stderr, (
            "MacOS will not allow this file to be executed"
        )
        assert spctl_result.returncode == 0
        # success message is sent to stderr
        assert "accepted" in spctl_result.stderr, "File was not accepted by MacOS"
