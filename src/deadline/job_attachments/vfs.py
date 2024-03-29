# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import logging
import os
import shutil
import subprocess
import time
from pathlib import Path
import threading
from typing import Dict, Union, Optional

from .exceptions import (
    VFSExecutableMissingError,
    VFSFailedToMountError,
    VFSLaunchScriptMissingError,
    VFSRunPathNotSetError,
)

from .os_file_permission import PosixFileSystemPermissionSettings

log = logging.getLogger(__name__)

DEADLINE_VFS_ENV_VAR = "DEADLINE_VFS_PATH"
DEADLINE_VFS_CACHE_ENV_VAR = "DEADLINE_VFS_CACHE"
DEADLINE_VFS_EXECUTABLE = "deadline_vfs"
DEADLINE_VFS_INSTALL_PATH = "/opt/deadline_vfs"
DEADLINE_VFS_EXECUTABLE_SCRIPT = "/scripts/production/al2/run_deadline_vfs_al2.sh"

DEADLINE_VFS_PID_FILE_NAME = "vfs_pids.txt"
DEADLINE_MANIFEST_GROUP_READ_PERMS = 0o640

VFS_CACHE_REL_PATH_IN_SESSION = ".vfs_object_cache"
VFS_MANIFEST_FOLDER_IN_SESSION = ".vfs_manifests"
VFS_LOGS_FOLDER_IN_SESSION = ".vfs_logs"

VFS_MANIFEST_FOLDER_PERMISSIONS = PosixFileSystemPermissionSettings(
    os_user="",
    os_group="",
    dir_mode=0o31,
    file_mode=0o64,
)


class VFSProcessManager(object):
    exe_path: Optional[str] = None
    launch_script_path: Optional[str] = None
    library_path: Optional[str] = None
    cwd_path: Optional[str] = None

    _mount_point: str
    _vfs_proc: Optional[subprocess.Popen]
    _vfs_thread: Optional[threading.Thread]
    _mount_temp_directory: Optional[str]
    _run_path: Optional[Union[os.PathLike, str]]
    _asset_bucket: str
    _region: str
    _manifest_path: str
    _os_user: str
    _os_env_vars: Dict[str, str]
    _os_group: Optional[str]
    _cas_prefix: Optional[str]
    _asset_cache_path: Optional[str]

    def __init__(
        self,
        asset_bucket: str,
        region: str,
        manifest_path: str,
        mount_point: str,
        os_user: str,
        os_env_vars: Dict[str, str],
        os_group: Optional[str] = None,
        cas_prefix: Optional[str] = None,
        asset_cache_path: Optional[str] = None,
    ):
        self._mount_point = mount_point
        self._vfs_proc = None
        self._vfs_thread = None
        self._mount_temp_directory = None
        self._run_path = None
        self._asset_bucket = asset_bucket
        self._region = region
        self._manifest_path = manifest_path
        self._os_user = os_user
        self._os_group = os_group
        self._os_env_vars = os_env_vars
        self._cas_prefix = cas_prefix
        self._asset_cache_path = asset_cache_path

    @classmethod
    def kill_all_processes(cls, session_dir: Path, os_user: str) -> None:
        """
        Kill all existing VFS processes when outputs have been uploaded.
        :param session_dir: tmp directory for session
        :param os_user: the user running the job.
        """
        log.info("Terminating all VFS processes.")
        try:
            pid_file_path = (session_dir / DEADLINE_VFS_PID_FILE_NAME).resolve()
            with open(pid_file_path, "r") as file:
                for line in file.readlines():
                    line = line.strip()
                    mount_point, _, _ = line.split(":")
                    cls.shutdown_libfuse_mount(mount_point, os_user, session_dir)
            os.remove(pid_file_path)
        except FileNotFoundError:
            log.warning(f"VFS pid file not found at {pid_file_path}")

    @classmethod
    def get_shutdown_args(cls, mount_path: str, os_user: str):
        """
        Return the argument list to provide the subprocess run command to shut down the mount
        :param mount_path: path to mounted folder
        :param os_user: the user running the job.
        """
        fusermount3_path = os.path.join(cls.find_vfs_link_dir(), "fusermount3")
        if not os.path.exists(fusermount3_path):
            log.warn(f"fusermount3 not found at {cls.find_vfs_link_dir()}")
            return None
        return ["sudo", "-u", os_user, fusermount3_path, "-u", mount_path]

    @classmethod
    def shutdown_libfuse_mount(cls, mount_path: str, os_user: str, session_dir: Path) -> bool:
        """
        Shut down the mount at the provided path using the fusermount3 unmount option
        as the provided user
        :param mount_path: path to mounted folder
        """
        log.info(f"Attempting to shut down {mount_path} as {os_user}")
        shutdown_args = cls.get_shutdown_args(mount_path, os_user)
        if not shutdown_args:
            return False
        try:
            run_result = subprocess.run(shutdown_args, check=True)
        except subprocess.CalledProcessError as e:
            log.warn(f"Shutdown failed with error {e}")
            # Don't reraise, check if mount is gone
        log.info(f"Shutdown returns {run_result.returncode}")
        return cls.wait_for_mount(mount_path, session_dir, expected=False)

    @classmethod
    def kill_process_at_mount(cls, session_dir: Path, mount_point: str, os_user: str) -> bool:
        """
        Kill the VFS instance running at the given mount_point and modify the VFS pid tracking
        file to remove the entry.

        :param session_dir: tmp directory for session
        :param mount_point: local directory to search for
        :param os_user: user to attempt shut down as
        """
        if not cls.is_mount(mount_point):
            log.info(f"{mount_point} is not a mount, returning")
            return False
        log.info(f"Terminating deadline_vfs processes at {mount_point}.")
        mount_point_found: bool = False
        try:
            pid_file_path = (session_dir / DEADLINE_VFS_PID_FILE_NAME).resolve()
            with open(pid_file_path, "r") as file:
                lines = file.readlines()
            with open(pid_file_path, "w") as file:
                for line in lines:
                    line = line.strip()
                    if mount_point_found:
                        file.write(line)
                    else:
                        mount_for_pid, _, _ = line.split(":")
                        if mount_for_pid == mount_point:
                            cls.shutdown_libfuse_mount(mount_point, os_user, session_dir)
                            mount_point_found = True
                        else:
                            file.write(line)
        except FileNotFoundError:
            log.warning(f"VFS pid file not found at {pid_file_path}")
            return False

        return mount_point_found

    @classmethod
    def get_manifest_path_for_mount(cls, session_dir: Path, mount_point: str) -> Optional[Path]:
        """
        Given a mount_point this searches the pid file for the associated manifest path.

        :param session_dir: tmp directory for session
        :param mount_point: local directory associated with the desired manifest

        :returns: Path to the manifest file for mount if there is one
        """
        try:
            pid_file_path = (session_dir / DEADLINE_VFS_PID_FILE_NAME).resolve()
            with open(pid_file_path, "r") as file:
                for line in file.readlines():
                    line = line.strip()
                    mount_for_pid, _, manifest_path = line.split(":")
                    if mount_for_pid == mount_point:
                        if os.path.exists(manifest_path):
                            return Path(manifest_path)
                        else:
                            log.warn(f"Expected VFS input manifest at {manifest_path}")
                            return None
        except FileNotFoundError:
            log.warning(f"VFS pid file not found at {pid_file_path}")

        log.warning(f"No manifest found for mount {mount_point}")
        return None

    @classmethod
    def is_mount(cls, path) -> bool:
        """
        os.path.ismount returns false for libfuse mounts owned by "other users",
        use findmnt instead
        """
        return subprocess.run(["findmnt", path]).returncode == 0

    @classmethod
    def wait_for_mount(cls, mount_path, session_dir, mount_wait_seconds=60, expected=True) -> bool:
        """
        After we've launched the VFS subprocess we need to wait
        for the OS to validate that the mount is in place before use
        :param mount_path: Path to mount to watch for
        :param session_dir: Session folder associated with mount
        :param mount_wait_seconds: Duration to wait for mount state
        :param expected: Wait for the mount to exist or no longer exist
        """
        log.info(f"Waiting for is_mount at {mount_path} to return {expected}..")
        wait_seconds = mount_wait_seconds
        while wait_seconds >= 0:
            if cls.is_mount(mount_path) == expected:
                log.info(f"is_mount on {mount_path} returns {expected}, returning")
                return True
            wait_seconds -= 1
            if wait_seconds >= 0:
                log.info(f"is_mount on {mount_path} not {expected}, sleeping...")
                time.sleep(1)
        log.info(f"Failed to find is_mount {expected} at {mount_path} after {mount_wait_seconds}")
        cls.print_log_end(session_dir)
        return False

    @classmethod
    def logs_folder_path(cls, session_dir: Path) -> Union[os.PathLike, str]:
        """
        Find the folder we expect VFS logs to be written to
        """
        return session_dir / VFS_LOGS_FOLDER_IN_SESSION

    def get_logs_folder(self) -> Union[os.PathLike, str]:
        """
        Find the folder we expect VFS logs to be written to
        """
        if self._run_path:
            return self.logs_folder_path(Path(self._run_path))
        raise VFSRunPathNotSetError("Attempted to find logs folder without run path")

    @classmethod
    def print_log_end(
        self, session_dir: Path, log_file_name="vfs_log.txt", lines=100, log_level=logging.WARNING
    ):
        """
        Print out the end of our VFS Log.  Reads the full log file into memory.  Our VFS logs are size
        capped so this is not an issue for the intended use case.
        :param session_dir: Session folder for mount
        :param log_file_name: Name of file within the logs folder to read from.  Defaults to vfs_log.txt which
        is our "most recent" log file.
        :param lines: Maximum number of lines from the end of the log to print
        :param log_level: Level to print logging as
        """
        log_file_path = self.logs_folder_path(session_dir) / log_file_name
        log.log(log_level, f"Printing last {lines} lines from {log_file_path}")
        if not os.path.exists(log_file_path):
            log.warning(f"No log file found at {log_file_path}")
            return
        with open(log_file_path, "r") as log_file:
            for this_line in log_file.readlines()[lines * -1 :]:
                log.log(log_level, this_line)

    @classmethod
    def find_vfs_link_dir(cls) -> str:
        """
        Get the path where links to any necessary executables which should be added to the path should live
        :returns: Path to the link folder
        """
        return os.path.join(os.path.dirname(VFSProcessManager.find_vfs()), "..", "link")

    def build_launch_command(self, mount_point: Union[os.PathLike, str]) -> str:
        """
        Build command to pass to Popen to launch VFS
        :param mount_point: directory to mount which must be the first parameter seen by our executable
        :return: command
        """
        executable = VFSProcessManager.find_vfs_launch_script()

        command = (
            f"sudo -E -u {self._os_user}"
            f" {executable} {mount_point} -f --clienttype=deadline"
            f" --bucket={self._asset_bucket}"
            f" --manifest={self._manifest_path}"
            f" --region={self._region}"
            f" -oallow_other"
        )
        if self._cas_prefix is not None:
            command += f" --casprefix={self._cas_prefix}"
        if self._asset_cache_path is not None:
            command += f" --cachedir={self._asset_cache_path}"

        log.info(f"Got launch command {command}")
        return command

    @classmethod
    def find_vfs_launch_script(cls) -> Union[os.PathLike, str]:
        """
        Determine where the VFS launch script lives so we can build the launch command
        :return: Path to VFS launch script
        """
        if VFSProcessManager.launch_script_path is not None:
            log.info(f"Using saved path {VFSProcessManager.launch_script_path} for launch script")
            return VFSProcessManager.launch_script_path

        exe = DEADLINE_VFS_EXECUTABLE
        # for exe in executables:
        log.info(f"Searching for {exe} launch script")
        exe_script = DEADLINE_VFS_EXECUTABLE_SCRIPT
        # Look for env var to construct script path
        if DEADLINE_VFS_ENV_VAR in os.environ:
            log.info(f"{DEADLINE_VFS_ENV_VAR} found in environment")
            environ_check = os.environ[DEADLINE_VFS_ENV_VAR] + exe_script
        else:
            log.warning(f"{DEADLINE_VFS_ENV_VAR} not found in environment")
            environ_check = DEADLINE_VFS_INSTALL_PATH + exe_script
        # Test if script path exists
        if os.path.exists(environ_check):
            log.info(f"Environ check found {exe} launch script at {environ_check}")
            VFSProcessManager.launch_script_path = environ_check
            return environ_check  # type: ignore[return-value]
        else:
            log.error(f"Failed to find {exe} launch script!")

        log.error("Failed to find both executables scripts!")
        raise VFSLaunchScriptMissingError

    @classmethod
    def find_vfs(cls) -> Union[os.PathLike, str]:
        """
        Determine where the VFS executable we'll be launching lives so we can
        find the correct relative paths around it for LD_LIBRARY_PATH and config files
        :return: Path to VFS executable
        """
        if VFSProcessManager.exe_path is not None:
            log.info(f"Using saved path {VFSProcessManager.exe_path}")
            return VFSProcessManager.exe_path

        exe = DEADLINE_VFS_EXECUTABLE
        # Use "which deadline_vfs" by default to find the executable location
        found_path = shutil.which(exe)
        if found_path is None:
            log.info(f"Cwd when finding {exe} is {os.getcwd()}")
            # If VFS executable isn't on the PATH, check if environment variable is set
            if DEADLINE_VFS_ENV_VAR in os.environ:
                log.info(f"{DEADLINE_VFS_ENV_VAR} set to {os.environ[DEADLINE_VFS_ENV_VAR]}")
                environ_check = os.environ[DEADLINE_VFS_ENV_VAR] + f"/bin/{exe}"
            else:
                log.info(f"{DEADLINE_VFS_ENV_VAR} env var not set")
                environ_check = DEADLINE_VFS_INSTALL_PATH + f"/bin/{exe}"
            if os.path.exists(environ_check):
                log.info(f"Environ check found {exe} at {environ_check}")
                found_path = environ_check
            else:
                # Last attempt looks for deadline_vfs in bin
                bin_check = os.path.join(os.getcwd(), f"bin/{exe}")
                if os.path.exists(bin_check):
                    log.info(f"Bin check found VFS at {bin_check}")
                    found_path = bin_check
                else:
                    log.error(f"Failed to find {exe}!")

        # Run final check to see if exe path was found
        if found_path is not None:
            log.info(f"Found {exe} at {found_path}")
            VFSProcessManager.exe_path = found_path
            return found_path  # type: ignore[return-value]

        log.error("Failed to find both executables!")
        raise VFSExecutableMissingError

    @classmethod
    def get_library_path(cls) -> Union[os.PathLike, str]:
        """
        Find our library dependencies which should be at ../lib relative to our executable
        """
        if VFSProcessManager.library_path is None:
            exe_path = VFSProcessManager.find_vfs()
            VFSProcessManager.library_path = os.path.normpath(
                os.path.join(os.path.dirname(exe_path), "../lib")
            )
        log.info(f"Using library path {VFSProcessManager.library_path}")
        return VFSProcessManager.library_path

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
        if VFSProcessManager.cwd_path is None:
            exe_path = VFSProcessManager.find_vfs()
            # Use cwd one folder up from bin
            VFSProcessManager.cwd_path = os.path.normpath(
                os.path.join(os.path.dirname(exe_path), "..")
            )
        return VFSProcessManager.cwd_path

    def get_launch_environ(self) -> dict:
        """
        Get the environment variables we'll pass to the launch command.
        :returns: dictionary of default environment variables with VFS changes applied
        """
        my_env = {**self._os_env_vars}
        my_env["PATH"] = f"{VFSProcessManager.find_vfs_link_dir()}{os.pathsep}{os.environ['PATH']}"
        my_env["LD_LIBRARY_PATH"] = VFSProcessManager.get_library_path()  # type: ignore[assignment]
        if os.environ.get(DEADLINE_VFS_CACHE_ENV_VAR) is not None:
            my_env[DEADLINE_VFS_CACHE_ENV_VAR] = os.environ.get(DEADLINE_VFS_CACHE_ENV_VAR)  # type: ignore[assignment]

        return my_env

    def set_manifest_owner(self) -> None:
        """
        Set the manifest path to be owned by _os_user
        """
        log.info(
            f"Attempting to set group ownership on {self._manifest_path} for {self._os_user} to {self._os_group}"
        )
        if not os.path.exists(self._manifest_path):
            log.error(f"Manifest not found at {self._manifest_path}")
            return
        if self._os_group is not None:
            try:
                shutil.chown(self._manifest_path, group=self._os_group)
                os.chmod(self._manifest_path, DEADLINE_MANIFEST_GROUP_READ_PERMS)
            except OSError as e:
                log.error(f"Failed to set ownership with error {e}")
                raise

    def start(self, session_dir: Path) -> None:
        """
        Start our VFS process
        :return: VFS process id
        """
        self._run_path = session_dir
        log.info(f"Using run_path {self._run_path}")
        log.info(f"Using mount_point {self._mount_point}")
        self.set_manifest_owner()
        VFSProcessManager.create_mount_point(self._mount_point)
        start_command = self.build_launch_command(self._mount_point)
        launch_env = self.get_launch_environ()
        log.info(f"Launching VFS with command {start_command}")
        log.info(f"Launching with environment {launch_env}")
        log.info(f"Launching as user {self._os_user}")

        try:

            def read_output_thread(pipe, log):
                # Runs in a thread to redirect VFS output into our log
                try:
                    for line in pipe:
                        log.info(line.decode("utf-8").strip())
                except Exception:
                    log.exception("Error reading VFS output")

            self._vfs_proc = subprocess.Popen(
                args=start_command,
                stdout=subprocess.PIPE,  # Create a new pipe
                stderr=subprocess.STDOUT,  # Merge stderr into the stdout pipe
                cwd=str(self._run_path),
                env=launch_env,
                shell=True,
                executable="/bin/bash",
            )

            self._vfs_thread = threading.Thread(
                target=read_output_thread, args=[self._vfs_proc.stdout, log], daemon=True
            )
            self._vfs_thread.start()

        except Exception as e:
            log.exception(f"Exception during launch with command {start_command} exception {e}")
            raise e
        log.info(f"Launched VFS as pid {self._vfs_proc.pid}")
        if not VFSProcessManager.wait_for_mount(self.get_mount_point(), session_dir):
            log.error("Failed to mount, shutting down")
            raise VFSFailedToMountError

        try:
            # if the pid file exists, add the new VFS instance and remove any it replaced
            pid_file_path = (session_dir / DEADLINE_VFS_PID_FILE_NAME).resolve()
            with open(pid_file_path, "r") as file:
                lines = file.readlines()
            with open(pid_file_path, "w") as file:
                file.write(f"{self._mount_point}:{self._vfs_proc.pid}:{self._manifest_path}\n")
                for line in lines:
                    line = line.strip()
                    entry_mount_point, entry_pid, entry_manifest_path = line.split(":")
                    if self._mount_point != entry_mount_point:
                        file.write(f"{line}\n")
                    else:
                        log.warning(f"Pid {entry_pid} entry not removed at {entry_mount_point}")
        except FileNotFoundError:
            # if the pid file doesn't exist, this will create it
            with open(pid_file_path, "a") as file:
                file.write(f"{self._mount_point}:{self._vfs_proc.pid}:{self._manifest_path}")

    def get_mount_point(self) -> Union[os.PathLike, str]:
        return self._mount_point
