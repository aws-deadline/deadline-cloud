# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import logging
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
import threading
from signal import SIGTERM
from typing import List, Union, Optional

from .exceptions import (
    Fus3ExecutableMissingError,
    Fus3FailedToMountError,
    Fus3LaunchScriptMissingError,
)

log = logging.getLogger(__name__)

FUS3_PATH_ENV_VAR = "FUS3_PATH"
FUS3_EXECUTABLE = "fus3"
FUS3_DEFAULT_INSTALL_PATH = "/opt/fus3"
FUS3_EXECUTABLE_SCRIPT = "/scripts/production/al2/run_fus3_al2.sh"

DEADLINE_VFS_ENV_VAR = "DEADLINE_VFS_PATH"
DEADLINE_VFS_EXECUTABLE = "deadline_vfs"
DEADLINE_VFS_INSTALL_PATH = "/opt/deadline_vfs"
DEADLINE_VFS_EXECUTABLE_SCRIPT = "/scripts/production/al2/run_deadline_vfs_al2.sh"

EXE_TO_SCRIPT = {
    DEADLINE_VFS_EXECUTABLE: DEADLINE_VFS_EXECUTABLE_SCRIPT,
    FUS3_EXECUTABLE: FUS3_EXECUTABLE_SCRIPT,
}

EXE_TO_INSTALL_PATH = {
    DEADLINE_VFS_EXECUTABLE: DEADLINE_VFS_INSTALL_PATH,
    FUS3_EXECUTABLE: FUS3_DEFAULT_INSTALL_PATH,
}

FUS3_PID_FILE_NAME = "fus3_pids.txt"


class Fus3ProcessManager(object):
    exe_path: Optional[str] = None
    launch_script_path: Optional[str] = None
    library_path: Optional[str] = None
    cwd_path: Optional[str] = None

    _mount_point: str
    _fus3_proc: Optional[subprocess.Popen]
    _fus3_thread: Optional[threading.Thread]
    _mount_temp_directory: Optional[str]
    _run_path: Optional[Union[os.PathLike, str]]
    _asset_bucket: str
    _region: str
    _manifest_path: str
    _queue_id: str
    _os_user: str
    _cas_prefix: Optional[str]

    def __init__(
        self,
        asset_bucket: str,
        region: str,
        manifest_path: str,
        mount_point: str,
        queue_id: str,
        os_user: str,
        cas_prefix: Optional[str] = None,
    ):
        # TODO: Once Windows pathmapping is implemented we can remove this
        if sys.platform == "win32":
            raise NotImplementedError("Windows is not currently supported for Job Attachments")

        self._mount_point = mount_point
        self._fus3_proc = None
        self._fus3_thread = None
        self._mount_temp_directory = None
        self._run_path = None
        self._asset_bucket = asset_bucket
        self._region = region
        self._manifest_path = manifest_path
        self._queue_id = queue_id
        self._os_user = os_user
        self._cas_prefix = cas_prefix

    @classmethod
    def kill_all_processes(cls, session_dir: Path) -> None:
        """
        Kill all existing Fus3 processes when outputs have been uploaded.
        :param session_dir: tmp directory for session
        """
        log.info("Terminating all Fus3 processes.")
        try:
            pid_file_path = (session_dir / FUS3_PID_FILE_NAME).resolve()
            with open(pid_file_path, "r") as file:
                for line in file.readlines():
                    pid = line.strip()
                    log.info(f"Sending SIGTERM to child processes of {pid}.")
                    subprocess.run(["/bin/pkill", "-P", pid])
                    log.info(f"Sending SIGTERM to {pid}.")
                    try:
                        os.kill(int(pid), SIGTERM)
                    except OSError as e:
                        # This is raised when the Fus3 process has already terminated.
                        # This shouldn't happen, but won't cause an error if ignored
                        if e.errno == 3:
                            log.error(f"No process found for {pid}")
            os.remove(pid_file_path)
        except FileNotFoundError:
            log.warning(f"Fus3 pid file not found at {pid_file_path}")

    def wait_for_mount(self, mount_wait_seconds=5) -> bool:
        """
        After we've launched the fus3 subprocess we need to wait
        for the OS to validate that the mount is in place before use
        """
        log.info(f"Testing for mount at {self.get_mount_point()}")
        wait_seconds = mount_wait_seconds
        while wait_seconds >= 0:
            if os.path.ismount(self.get_mount_point()):
                log.info(f"{self.get_mount_point()} is a mount, returning")
                return True
            wait_seconds -= 1
            if wait_seconds >= 0:
                log.info(f"{self.get_mount_point()} not a mount, sleeping...")
                time.sleep(1)
        log.info(f"Failed to find mount at {self.get_mount_point()}")
        Fus3ProcessManager.print_log_end()
        return False

    @classmethod
    def get_logs_folder(cls) -> Union[os.PathLike, str]:
        """
        Find the folder we expect fus3_ logs to be written to
        """
        return os.path.join(os.path.dirname(Fus3ProcessManager.find_fus3()), "..", "logs")

    @classmethod
    def print_log_end(cls, log_file_name="fus3_log.txt", lines=100, log_level=logging.WARNING):
        """
        Print out the end of our VFS Log.  Reads the full log file into memory.  Our VFS logs are size
        capped so this is not an issue for the intended use case.
        :param log_file_name: Name of file within the logs folder to read from.  Defaults to fus3_log.txt which
        is our "most recent" log file.
        :param lines: Maximum number of lines from the end of the log to print
        :param log_level: Level to print logging as
        """
        log_file_path = os.path.join(Fus3ProcessManager.get_logs_folder(), log_file_name)
        log.log(log_level, f"Printing last {lines} lines from {log_file_path}")
        if not os.path.exists(log_file_path):
            log.warning(f"No log file found at {log_file_path}")
            return
        with open(log_file_path, "r") as log_file:
            for this_line in log_file.readlines()[lines * -1 :]:
                log.log(log_level, this_line)

    @classmethod
    def find_fus3_link_dir(cls) -> str:
        """
        Get the path where links to any necessary executables which should be added to the path should live
        :returns: Path to the link folder
        """
        return os.path.join(os.path.dirname(Fus3ProcessManager.find_fus3()), "..", "link")

    def build_launch_command(self, mount_point: Union[os.PathLike, str]) -> List:
        """
        Build command to pass to Popen to launch fus3
        :param mount_point: directory to mount which must be the first parameter seen by our executable
        :return: command
        """
        command = []

        executable = Fus3ProcessManager.find_fus3_launch_script()
        if self._cas_prefix is None:
            command = [
                "%s %s -f --clienttype=deadline --bucket=%s --manifest=%s --region=%s -oallow_other"
                % (executable, mount_point, self._asset_bucket, self._manifest_path, self._region)
            ]
        else:
            command = [
                "%s %s -f --clienttype=deadline --bucket=%s --manifest=%s --region=%s --casprefix=%s -oallow_other"
                % (
                    executable,
                    mount_point,
                    self._asset_bucket,
                    self._manifest_path,
                    self._region,
                    self._cas_prefix,
                )
            ]

        log.info(f"Got launch command {command}")
        return command

    @classmethod
    def find_fus3_launch_script(cls) -> Union[os.PathLike, str]:
        """
        Determine where the fus3 launch script lives so we can build the launch command
        :return: Path to fus3 launch script
        """
        if Fus3ProcessManager.launch_script_path is not None:
            log.info(f"Using saved path {Fus3ProcessManager.launch_script_path} for launch script")
            return Fus3ProcessManager.launch_script_path

        executables = [DEADLINE_VFS_EXECUTABLE, FUS3_EXECUTABLE]
        for exe in executables:
            log.info(f"Searching for {exe} launch script")
            exe_script = EXE_TO_SCRIPT[exe]
            # Look for env var to construct script path
            if DEADLINE_VFS_ENV_VAR in os.environ:
                log.info(f"{DEADLINE_VFS_ENV_VAR} found in environment")
                environ_check = os.environ[DEADLINE_VFS_ENV_VAR] + exe_script
            elif FUS3_PATH_ENV_VAR in os.environ:
                log.info(f"{FUS3_PATH_ENV_VAR} found in environment")
                environ_check = os.environ[FUS3_PATH_ENV_VAR] + exe_script
            else:
                log.warning(
                    f"{FUS3_PATH_ENV_VAR} and {DEADLINE_VFS_ENV_VAR} not found in environment"
                )
                environ_check = EXE_TO_INSTALL_PATH[exe] + exe_script
            # Test if script path exists
            if os.path.exists(environ_check):
                log.info(f"Environ check found {exe} launch script at {environ_check}")
                Fus3ProcessManager.launch_script_path = environ_check
                return environ_check  # type: ignore[return-value]
            else:
                log.error(f"Failed to find {exe} launch script!")

        log.error("Failed to find both executables scripts!")
        raise Fus3LaunchScriptMissingError

    @classmethod
    def find_fus3(cls) -> Union[os.PathLike, str]:
        """
        Determine where the fus3 executable we'll be launching lives so we can
        find the correct relative paths around it for LD_LIBRARY_PATH and config files
        :return: Path to fus3
        """
        if Fus3ProcessManager.exe_path is not None:
            log.info(f"Using saved path {Fus3ProcessManager.exe_path}")
            return Fus3ProcessManager.exe_path

        executables = [DEADLINE_VFS_EXECUTABLE, FUS3_EXECUTABLE]
        for exe in executables:
            # Use "which fus3" by default to find fus3 executable location
            found_path = shutil.which(exe)
            if found_path is None:
                log.info(f"Cwd when finding {exe} is {os.getcwd()}")
                # If fus3 executable isn't on the PATH, check if environment variable is set
                if DEADLINE_VFS_ENV_VAR in os.environ:
                    log.info(f"{DEADLINE_VFS_ENV_VAR} set to {os.environ[DEADLINE_VFS_ENV_VAR]}")
                    environ_check = os.environ[DEADLINE_VFS_ENV_VAR] + f"/bin/{exe}"
                elif FUS3_PATH_ENV_VAR in os.environ:
                    log.info(f"{FUS3_PATH_ENV_VAR} set to {os.environ[FUS3_PATH_ENV_VAR]}")
                    environ_check = os.environ[FUS3_PATH_ENV_VAR] + f"/bin/{exe}"
                else:
                    log.info(f"{FUS3_PATH_ENV_VAR} and {DEADLINE_VFS_ENV_VAR} env vars not set")
                    environ_check = EXE_TO_INSTALL_PATH[exe] + f"/bin/{exe}"
                if os.path.exists(environ_check):
                    log.info(f"Environ check found {exe} at {environ_check}")
                    found_path = environ_check
                else:
                    # Last attempt looks for fus3 in bin
                    bin_check = os.path.join(os.getcwd(), f"bin/{exe}")
                    if os.path.exists(bin_check):
                        log.info(f"Bin check found fus3 at {bin_check}")
                        found_path = bin_check
                    else:
                        log.error(f"Failed to find {exe}!")

            # Run final check to see if exe path was found
            if found_path is not None:
                log.info(f"Found {exe} at {found_path}")
                Fus3ProcessManager.exe_path = found_path
                return found_path  # type: ignore[return-value]

        log.error("Failed to find both executables!")
        raise Fus3ExecutableMissingError

    @classmethod
    def get_library_path(cls) -> Union[os.PathLike, str]:
        """
        Find our library dependencies which should be at ../lib relative to our executable
        """
        if Fus3ProcessManager.library_path is None:
            exe_path = Fus3ProcessManager.find_fus3()
            Fus3ProcessManager.library_path = os.path.normpath(
                os.path.join(os.path.dirname(exe_path), "../lib")
            )
        log.info(f"Using library path {Fus3ProcessManager.library_path}")
        return Fus3ProcessManager.library_path

    def get_file_path(self, relative_file_name: str) -> Union[os.PathLike, str]:
        return os.path.join(self._mount_point, relative_file_name)

    @classmethod
    def create_mount_point(cls, mount_point: Union[os.PathLike, str]) -> None:
        """
        By default fuse won't create our mount folder, create it if it doesn't exist
        """
        if os.path.exists(mount_point) is False:
            log.info(f"Creating mount point at {mount_point}")
            os.makedirs(mount_point, exist_ok=True)
            log.info(f"Modifying permissions of mount point at {mount_point}")
            os.chmod(path=mount_point, mode=0o777)

    @classmethod
    def get_cwd(cls) -> Union[os.PathLike, str]:
        """
        Determine the cwd we should hand to Popen.
        We expect a config/logging.ini file to exist relative to this folder.
        """
        if Fus3ProcessManager.cwd_path is None:
            exe_path = Fus3ProcessManager.find_fus3()
            # Use cwd one folder up from bin
            Fus3ProcessManager.cwd_path = os.path.normpath(
                os.path.join(os.path.dirname(exe_path), "..")
            )
        return Fus3ProcessManager.cwd_path

    def get_launch_environ(self) -> dict:
        """
        Get the environment variables we'll pass to the launch command.
        :returns: dictionary of default environment variables with fus3 changes applied
        """
        my_env = {**os.environ}
        my_env[
            "PATH"
        ] = f"{Fus3ProcessManager.find_fus3_link_dir()}{os.pathsep}{os.environ['PATH']}"
        my_env["LD_LIBRARY_PATH"] = Fus3ProcessManager.get_library_path()  # type: ignore[assignment]

        my_env["AWS_CONFIG_FILE"] = Path(f"~{self._os_user}/.aws/config").expanduser().as_posix()
        my_env["AWS_PROFILE"] = f"deadline-{self._queue_id}"

        return my_env

    def start(self, session_dir: Path) -> None:
        """
        Start our fus3 process
        :return: fus3 process id
        """
        self._run_path = Fus3ProcessManager.get_cwd()
        log.info(f"Using run_path {self._run_path}")
        log.info(f"Using mount_point {self._mount_point}")
        Fus3ProcessManager.create_mount_point(self._mount_point)
        start_command = self.build_launch_command(self._mount_point)
        launch_env = self.get_launch_environ()
        log.info(f"Launching fus3 with command {start_command}")
        log.info(f"Launching with environment {launch_env}")

        try:

            def read_output_thread(pipe, log):
                # Runs in a thread to redirect fus3 output into our log
                try:
                    for line in pipe:
                        log.info(line.decode("utf-8").strip())
                except Exception:
                    log.exception("Error reading fus3 output")

            self._fus3_proc = subprocess.Popen(
                args=start_command,
                stdout=subprocess.PIPE,  # Create a new pipe
                stderr=subprocess.STDOUT,  # Merge stderr into the stdout pipe
                cwd=self._run_path,
                env=launch_env,
                shell=True,
                executable="/bin/bash",
            )

            self._fus3_thread = threading.Thread(
                target=read_output_thread, args=[self._fus3_proc.stdout, log], daemon=True
            )
            self._fus3_thread.start()

        except Exception as e:
            log.exception(f"Exception during launch with command {start_command} exception {e}")
            raise e
        log.info(f"Launched fus3 as pid {self._fus3_proc.pid}")
        if not self.wait_for_mount():
            log.error("Failed to mount, shutting down")
            raise Fus3FailedToMountError

        pid_file_path = (session_dir / FUS3_PID_FILE_NAME).resolve()
        with open(pid_file_path, "a") as file:
            file.write(f"{self._fus3_proc.pid}\n")

    def get_mount_point(self) -> Union[os.PathLike, str]:
        return self._mount_point
