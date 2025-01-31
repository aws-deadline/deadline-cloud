# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for the Asset Synching class for task-level attachments."""


import os
import stat
import sys
from pathlib import Path
import subprocess
import threading
from typing import Union
from unittest.mock import Mock, patch, call, MagicMock

import pytest

import deadline
from deadline.job_attachments.asset_sync import AssetSync
from deadline.job_attachments.exceptions import (
    VFSExecutableMissingError,
    VFSLaunchScriptMissingError,
)
from deadline.job_attachments.models import JobAttachmentS3Settings
from deadline.job_attachments.vfs import (
    VFSProcessManager,
    DEADLINE_VFS_ENV_VAR,
    DEADLINE_VFS_CACHE_ENV_VAR,
    DEADLINE_VFS_EXECUTABLE,
    DEADLINE_VFS_EXECUTABLE_SCRIPT,
    DEADLINE_VFS_INSTALL_PATH,
    DEADLINE_VFS_PID_FILE_NAME,
    DEADLINE_MANIFEST_GROUP_READ_PERMS,
    VFS_LOGS_FOLDER_IN_SESSION,
)


# TODO: Remove the skip once we support Windows for AssetSync
@pytest.mark.skipif(sys.platform == "win32", reason="VFS doesn't currently support Windows")
class TestVFSProcessmanager:
    @pytest.fixture(autouse=True)
    def setup_and_teardown(
        self,
        request,
        create_s3_bucket,
        default_job_attachment_s3_settings: JobAttachmentS3Settings,
        default_asset_sync: AssetSync,
    ):
        """
        Setup the default queue and s3 bucket for all asset tests.
        Mark test with `no_setup` if you don't want this setup to run.
        After test completes, reset all static VFSProcessManager fields
        """
        if "no_setup" in request.keywords:
            return

        create_s3_bucket(bucket_name=default_job_attachment_s3_settings.s3BucketName)
        self.default_asset_sync = default_asset_sync
        self.s3_settings = default_job_attachment_s3_settings

        yield

        # reset VFSProcessManager fields
        VFSProcessManager.exe_path = None
        VFSProcessManager.launch_script_path = None
        VFSProcessManager.library_path = None
        VFSProcessManager.cwd_path = None

    def test_build_launch_command(
        self,
        tmp_path: Path,
    ):
        os.environ[DEADLINE_VFS_ENV_VAR] = str((Path(__file__) / "deadline_vfs").resolve())
        session_dir: str = str(tmp_path)
        dest_dir: str = "assetroot-27bggh78dd2b568ab123"
        local_root: str = f"{session_dir}/{dest_dir}"
        manifest_path: str = f"{local_root}/manifest.json"
        test_os_user = "test-user"
        # Create process manager without CAS prefix
        process_manager: VFSProcessManager = VFSProcessManager(
            asset_bucket=self.s3_settings.s3BucketName,
            region=os.environ["AWS_DEFAULT_REGION"],
            manifest_path=manifest_path,
            mount_point=local_root,
            os_user=test_os_user,
            os_env_vars={"AWS_PROFILE": "test-profile"},
        )

        test_executable = os.environ[DEADLINE_VFS_ENV_VAR] + DEADLINE_VFS_EXECUTABLE_SCRIPT

        expected_launch_command = (
            f"sudo -E -u {test_os_user}"
            f" {test_executable} {local_root} -f --clienttype=deadline"
            f" --bucket={self.s3_settings.s3BucketName}"
            f" --manifest={manifest_path}"
            f" --region={os.environ['AWS_DEFAULT_REGION']}"
            f" -oallow_other"
        )
        with patch(
            f"{deadline.__package__}.job_attachments.vfs.os.path.exists",
            return_value=True,
        ):
            assert (
                process_manager.build_launch_command(mount_point=local_root)
                == expected_launch_command
            )

        # Create process manager with CAS prefix
        test_CAS_prefix: str = "test_prefix"
        process_manager = VFSProcessManager(
            asset_bucket=self.s3_settings.s3BucketName,
            region=os.environ["AWS_DEFAULT_REGION"],
            manifest_path=manifest_path,
            mount_point=local_root,
            os_user=test_os_user,
            os_env_vars={"AWS_PROFILE": "test-profile"},
            cas_prefix=test_CAS_prefix,
        )

        # intermediate cleanup
        VFSProcessManager.launch_script_path = None

        expected_launch_command = (
            f"sudo -E -u {test_os_user}"
            f" {test_executable} {local_root} -f --clienttype=deadline"
            f" --bucket={self.s3_settings.s3BucketName}"
            f" --manifest={manifest_path}"
            f" --region={os.environ['AWS_DEFAULT_REGION']}"
            f" -oallow_other"
            f" --casprefix={test_CAS_prefix}"
        )
        with patch(
            f"{deadline.__package__}.job_attachments.vfs.os.path.exists",
            return_value=True,
        ):
            assert (
                process_manager.build_launch_command(mount_point=local_root)
                == expected_launch_command
            )

    def test_find_vfs_with_env_set(
        self,
        tmp_path: Path,
    ):
        session_dir: str = str(tmp_path)
        dest_dir: str = "assetroot-27bggh78dd2b568ab123"
        local_root: str = f"{session_dir}/{dest_dir}"
        manifest_path: str = f"{local_root}/manifest.json"
        os.environ[DEADLINE_VFS_ENV_VAR] = str((Path(__file__) / "deadline_vfs").resolve())

        # Create process manager without CAS prefix
        process_manager: VFSProcessManager = VFSProcessManager(
            asset_bucket=self.s3_settings.s3BucketName,
            region=os.environ["AWS_DEFAULT_REGION"],
            manifest_path=manifest_path,
            mount_point=local_root,
            os_user="test-user",
            os_env_vars={"AWS_PROFILE": "test-profile"},
        )

        # verify which is only called when class path is not set
        with patch(f"{deadline.__package__}.job_attachments.vfs.shutil.which") as mock_which:
            mock_which.return_value = "/test/path"
            test_path: Union[os.PathLike, str] = process_manager.find_vfs()

            assert str(test_path) == "/test/path"
            test_path = process_manager.find_vfs()
            mock_which.assert_called_once()

            # Reset VFS path and remove from PATH so other methods are checked
            mock_which.return_value = None
            VFSProcessManager.exe_path = None

            with patch(
                f"{deadline.__package__}.job_attachments.vfs.os.path.exists"
            ) as mock_path_exists:
                mock_path_exists.return_value = False

                with pytest.raises(VFSExecutableMissingError):
                    process_manager.find_vfs()

                # Verify DEADLINE_VFS_ENV_VAR location is checked
                # Verify bin folder is checked as a last resort
                mock_path_exists.assert_has_calls(
                    [
                        call(os.environ[DEADLINE_VFS_ENV_VAR] + f"/bin/{DEADLINE_VFS_EXECUTABLE}"),
                        call(os.path.join(os.getcwd(), f"bin/{DEADLINE_VFS_EXECUTABLE}")),
                    ]
                )

    def test_find_vfs_with_deadline_env_set(
        self,
        tmp_path: Path,
    ):
        session_dir: str = str(tmp_path)
        dest_dir: str = "assetroot-27bggh78dd2b568ab123"
        local_root: str = f"{session_dir}/{dest_dir}"
        manifest_path: str = f"{local_root}/manifest.json"
        os.environ[DEADLINE_VFS_ENV_VAR] = str((Path(__file__) / "deadline_vfs").resolve())

        # Create process manager without CAS prefix
        process_manager: VFSProcessManager = VFSProcessManager(
            asset_bucket=self.s3_settings.s3BucketName,
            region=os.environ["AWS_DEFAULT_REGION"],
            manifest_path=manifest_path,
            mount_point=local_root,
            os_user="test-user",
            os_env_vars={"AWS_PROFILE": "test-profile"},
        )

        # verify which is only called when class path is not set
        with patch(f"{deadline.__package__}.job_attachments.vfs.shutil.which") as mock_which:
            mock_which.return_value = "/test/path"
            test_path: Union[os.PathLike, str] = process_manager.find_vfs()

            assert str(test_path) == "/test/path"
            test_path = process_manager.find_vfs()
            mock_which.assert_called_once()

            # Reset VFS path and remove from PATH so other methods are checked
            mock_which.return_value = None
            VFSProcessManager.exe_path = None

            with patch(
                f"{deadline.__package__}.job_attachments.vfs.os.path.exists"
            ) as mock_path_exists:
                mock_path_exists.return_value = False

                with pytest.raises(VFSExecutableMissingError):
                    process_manager.find_vfs()

                # Verify DEADLINE_VFS_ENV_VAR location is checked
                # Verify bin folder is checked as a last resort
                mock_path_exists.assert_has_calls(
                    [
                        call(os.environ[DEADLINE_VFS_ENV_VAR] + f"/bin/{DEADLINE_VFS_EXECUTABLE}"),
                        call(os.path.join(os.getcwd(), f"bin/{DEADLINE_VFS_EXECUTABLE}")),
                    ]
                )

    def find_vfs_with_env_not_set(
        self,
        tmp_path: Path,
    ):
        session_dir: str = str(tmp_path)
        dest_dir: str = "assetroot-27bggh78dd2b568ab123"
        local_root: str = f"{session_dir}/{dest_dir}"
        manifest_path: str = f"{local_root}/manifest.json"

        # Create process manager without CAS prefix
        process_manager: VFSProcessManager = VFSProcessManager(
            asset_bucket=self.s3_settings.s3BucketName,
            region=os.environ["AWS_DEFAULT_REGION"],
            manifest_path=manifest_path,
            mount_point=local_root,
            os_user="test-user",
            os_env_vars={"AWS_PROFILE": "test-profile"},
        )

        bin_check = os.path.join(os.getcwd(), f"bin/{DEADLINE_VFS_EXECUTABLE}")

        # verify which is only called when class path is not set
        with patch(
            f"{deadline.__package__}.job_attachments.vfs.shutil.which",
            return_value=None,
        ) as mock_which, patch(
            f"{deadline.__package__}.job_attachments.vfs.os.path.exists",
            side_effect=lambda x: True if x == bin_check else False,
        ) as mock_path_exists:
            test_path: Union[os.PathLike, str] = process_manager.find_vfs()
            assert str(test_path) == bin_check

            test_path = process_manager.find_vfs()
            mock_which.assert_called_once()
            assert mock_path_exists.call_count == 2

    def test_find_library_path(
        self,
        tmp_path: Path,
    ):
        session_dir: str = str(tmp_path)
        dest_dir: str = "assetroot-27bggh78dd2b568ab123"
        local_root: str = f"{session_dir}/{dest_dir}"
        manifest_path: str = f"{local_root}/manifest.json"
        os.environ[DEADLINE_VFS_ENV_VAR] = str((Path(__file__) / "deadline_vfs").resolve())

        # Create process manager without CAS prefix
        process_manager: VFSProcessManager = VFSProcessManager(
            asset_bucket=self.s3_settings.s3BucketName,
            region=os.environ["AWS_DEFAULT_REGION"],
            manifest_path=manifest_path,
            mount_point=local_root,
            os_user="test-user",
            os_env_vars={"AWS_PROFILE": "test-profile"},
        )

        with patch(
            f"{deadline.__package__}.job_attachments.vfs.VFSProcessManager.find_vfs"
        ) as mock_find_vfs:
            mock_find_vfs.return_value = "/test/directory/path"
            library_path: Union[os.PathLike, str] = process_manager.get_library_path()

            assert str(library_path) == "/test/lib"

            process_manager.get_library_path()

            mock_find_vfs.assert_called_once()

    def test_find_vfs_launch_script_with_env_set(
        self,
        tmp_path: Path,
    ):
        session_dir: str = str(tmp_path)
        dest_dir: str = "assetroot-27bggh78dd2b568ab123"
        local_root: str = f"{session_dir}/{dest_dir}"
        manifest_path: str = f"{local_root}/manifest.json"
        vfs_test_path = str((Path(__file__) / "deadline_vfs").resolve())
        os.environ[DEADLINE_VFS_ENV_VAR] = vfs_test_path

        # Create process manager without CAS prefix
        process_manager: VFSProcessManager = VFSProcessManager(
            asset_bucket=self.s3_settings.s3BucketName,
            region=os.environ["AWS_DEFAULT_REGION"],
            manifest_path=manifest_path,
            mount_point=local_root,
            os_user="test-user",
            os_env_vars={"AWS_PROFILE": "test-profile"},
        )

        with patch(
            f"{deadline.__package__}.job_attachments.vfs.os.path.exists"
        ) as mock_os_path_exists:
            mock_os_path_exists.return_value = True
            deadline_vfs_launch_script_path: Union[os.PathLike, str] = (
                process_manager.find_vfs_launch_script()
            )
            assert (
                str(deadline_vfs_launch_script_path)
                == vfs_test_path + DEADLINE_VFS_EXECUTABLE_SCRIPT
            )

            process_manager.find_vfs_launch_script()

            mock_os_path_exists.assert_called_once()

            VFSProcessManager.launch_script_path = None
            mock_os_path_exists.return_value = False

            with pytest.raises(VFSLaunchScriptMissingError):
                process_manager.find_vfs_launch_script()

    def test_find_vfs_launch_script_with_env_not_set(
        self,
        tmp_path: Path,
    ):
        session_dir: str = str(tmp_path)
        dest_dir: str = "assetroot-27bggh78dd2b568ab123"
        local_root: str = f"{session_dir}/{dest_dir}"
        manifest_path: str = f"{local_root}/manifest.json"
        # Note that env variable not set
        if DEADLINE_VFS_ENV_VAR in os.environ:
            os.environ.pop(DEADLINE_VFS_ENV_VAR)

        # Create process manager without CAS prefix
        process_manager: VFSProcessManager = VFSProcessManager(
            asset_bucket=self.s3_settings.s3BucketName,
            region=os.environ["AWS_DEFAULT_REGION"],
            manifest_path=manifest_path,
            mount_point=local_root,
            os_user="test-user",
            os_env_vars={"AWS_PROFILE": "test-profile"},
        )

        with patch(
            f"{deadline.__package__}.job_attachments.vfs.os.path.exists"
        ) as mock_os_path_exists:
            mock_os_path_exists.return_value = True
            deadline_vfs_launch_script_path: Union[os.PathLike, str] = (
                process_manager.find_vfs_launch_script()
            )

            # Will return preset vfs install path with exe script path appended since env is not set
            assert (
                str(deadline_vfs_launch_script_path)
                == DEADLINE_VFS_INSTALL_PATH + DEADLINE_VFS_EXECUTABLE_SCRIPT
            )

            process_manager.find_vfs_launch_script()
            mock_os_path_exists.assert_called_once()

            VFSProcessManager.launch_script_path = None
            mock_os_path_exists.return_value = False

            with pytest.raises(VFSLaunchScriptMissingError):
                process_manager.find_vfs_launch_script()

    def test_create_mount_point(
        self,
        tmp_path: Path,
    ):
        session_dir: str = str(tmp_path)
        dest_dir: str = "assetroot-27bggh78dd2b568ab123"
        local_root: str = f"{session_dir}/{dest_dir}"
        manifest_path: str = f"{local_root}/manifest.json"
        os.environ[DEADLINE_VFS_ENV_VAR] = str((Path(__file__) / "deadline_vfs").resolve())

        # Create process manager without CAS prefix
        process_manager: VFSProcessManager = VFSProcessManager(
            asset_bucket=self.s3_settings.s3BucketName,
            region=os.environ["AWS_DEFAULT_REGION"],
            manifest_path=manifest_path,
            mount_point=local_root,
            os_user="test-user",
            os_env_vars={"AWS_PROFILE": "test-profile"},
        )

        # Verify mount point is created and others have rwx access to it
        process_manager.create_mount_point(local_root)
        assert os.path.exists(local_root)
        assert bool(os.stat(local_root).st_mode & stat.S_IROTH)
        assert bool(os.stat(local_root).st_mode & stat.S_IWOTH)
        assert bool(os.stat(local_root).st_mode & stat.S_IXOTH)

    def test_pids_recorded_and_killed(
        self,
        tmp_path: Path,
    ):
        session_dir: str = str(tmp_path)
        dest_dir1: str = "assetroot-27bggh78dd2b568ab123"
        local_root1: str = f"{session_dir}/{dest_dir1}"
        manifest_path1: str = f"{local_root1}/manifest.json"
        dest_dir2: str = "assetroot-27bggh78dd23131d221"
        local_root2: str = f"{session_dir}/{dest_dir2}"
        manifest_path2: str = f"{local_root2}/manifest.json"
        os.environ[DEADLINE_VFS_ENV_VAR] = str((Path(__file__) / "deadline_vfs").resolve())
        test_pid1 = 12345
        test_pid2 = 67890
        test_os_user = "test-user"
        # Create process managers
        process_manager1: VFSProcessManager = VFSProcessManager(
            asset_bucket=self.s3_settings.s3BucketName,
            region=os.environ["AWS_DEFAULT_REGION"],
            manifest_path=manifest_path1,
            mount_point=local_root1,
            os_user=test_os_user,
            os_env_vars={"AWS_PROFILE": "test-profile"},
        )
        process_manager2: VFSProcessManager = VFSProcessManager(
            asset_bucket=self.s3_settings.s3BucketName,
            region=os.environ["AWS_DEFAULT_REGION"],
            manifest_path=manifest_path2,
            mount_point=local_root2,
            os_user=test_os_user,
            os_env_vars={"AWS_PROFILE": "test-profile"},
        )

        with patch(
            f"{deadline.__package__}.job_attachments.vfs.VFSProcessManager.find_vfs",
            return_value="/test/directory/path",
        ), patch(
            f"{deadline.__package__}.job_attachments.vfs.subprocess.Popen",
        ) as mock_popen, patch(
            f"{deadline.__package__}.job_attachments.vfs.VFSProcessManager.wait_for_mount",
            return_value=True,
        ), patch(
            f"{deadline.__package__}.job_attachments.vfs.os.path.exists",
            return_value=True,
        ), patch(
            f"{deadline.__package__}.job_attachments.vfs.subprocess.run"
        ) as mock_subprocess_run, patch(
            f"{deadline.__package__}.job_attachments.vfs.VFSProcessManager.get_launch_environ",
            return_value=os.environ,
        ):
            # start first mock VFS process
            mock_subprocess = MagicMock()
            mock_subprocess.pid = test_pid1
            mock_popen.return_value = mock_subprocess
            process_manager1.start(tmp_path)

            # start second mock VFS process
            mock_subprocess.pid = test_pid2
            process_manager2.start(tmp_path)

            # verify the pids were written to the correct location
            pid_file_path = (tmp_path / DEADLINE_VFS_PID_FILE_NAME).resolve()
            with open(pid_file_path, "r") as pid_file:
                pid_file_contents = pid_file.readlines()
                assert f"{local_root1}:{test_pid1}:{manifest_path1}\n" in pid_file_contents
                assert f"{local_root2}:{test_pid2}:{manifest_path2}\n" in pid_file_contents

            assert os.path.exists(local_root1)
            assert os.path.exists(local_root2)

            VFSProcessManager.kill_all_processes(tmp_path, os_user=test_os_user)
            # Verify all mounts were killed
            mock_subprocess_run.assert_has_calls(
                [
                    call(
                        VFSProcessManager.get_shutdown_args(local_root1, test_os_user), check=True
                    ),
                    call(
                        VFSProcessManager.get_shutdown_args(local_root2, test_os_user), check=True
                    ),
                ],
                any_order=True,
            )
            with pytest.raises(FileNotFoundError):
                open(pid_file_path, "r")

    def test_process_output_captured(
        self,
        tmp_path: Path,
    ):
        # Test to verify the spawned process output is captured and redirected to log.info

        session_dir: str = str(tmp_path)
        dest_dir1: str = "assetroot-27bggh78dd2b568ab123"
        local_root1: str = f"{session_dir}/{dest_dir1}"
        manifest_path1: str = f"{local_root1}/manifest.json"
        os.environ[DEADLINE_VFS_ENV_VAR] = str((Path(__file__) / "deadline_vfs").resolve())
        test_pid1 = 12345

        # Create process managers
        process_manager1: VFSProcessManager = VFSProcessManager(
            asset_bucket=self.s3_settings.s3BucketName,
            region=os.environ["AWS_DEFAULT_REGION"],
            manifest_path=manifest_path1,
            mount_point=local_root1,
            os_user="test-user",
            os_env_vars={"AWS_PROFILE": "test-profile"},
        )

        with patch(
            f"{deadline.__package__}.job_attachments.vfs.VFSProcessManager.find_vfs",
            return_value="/test/directory/path",
        ), patch(
            f"{deadline.__package__}.job_attachments.vfs.subprocess.Popen",
        ) as mock_popen, patch(
            f"{deadline.__package__}.job_attachments.vfs.VFSProcessManager.wait_for_mount",
            return_value=True,
        ), patch(
            f"{deadline.__package__}.job_attachments.vfs.os.path.exists",
            return_value=True,
        ), patch(
            f"{deadline.__package__}.job_attachments.vfs.log"
        ) as mock_logger, patch(
            f"{deadline.__package__}.job_attachments.vfs.VFSProcessManager.get_launch_environ",
            return_value=os.environ,
        ):
            call_count = 0
            exception_count = 0
            signal = threading.Semaphore(0)

            # Intercept the logging
            def mock_log(str):
                nonlocal call_count
                if str == "a" or str == "b" or str == "c":
                    call_count += 1

            def mock_exception(str):
                nonlocal exception_count
                nonlocal signal
                exception_count += 1
                signal.release()

            # Create a series of mock outputs and signal completion at the end
            def mock_output():
                yield "a".encode("utf-8")
                yield "b".encode("utf-8")
                yield "c".encode("utf-8")
                yield Exception("Test Exception")

            mock_logger.info = Mock(side_effect=mock_log)
            mock_logger.exception = Mock(side_effect=mock_exception)

            mock_subprocess = MagicMock()
            mock_subprocess.pid = test_pid1
            mock_subprocess.stdout = mock_output()
            mock_popen.return_value = mock_subprocess

            # Start the process and wait for our signal to indicate all the outputs have been read
            process_manager1.start(tmp_path)

            # Wait for *up to* 60 seconds at most for the mock outputs to be read by the thread.
            # This should never take that long and failing the timeout should indicate something is wrong.
            assert signal.acquire(blocking=True, timeout=60)

            # Verify all output was logged
            assert call_count == 3
            assert exception_count == 1

    def test_pids_file_behavior(
        self,
        tmp_path: Path,
    ):
        # Test to verify the spawned process output is captured and redirected to log.info

        session_dir: str = str(tmp_path)
        dest_dir1: str = "assetroot-27bggh78dd2b568ab123"
        local_root1: str = f"{session_dir}/{dest_dir1}"
        manifest_path1: str = f"{local_root1}/manifest.json"
        dest_dir2: str = "assetroot-27bggh78dd23131d221"
        local_root2: str = f"{session_dir}/{dest_dir2}"
        manifest_path2: str = f"{local_root2}/manifest.json"
        os.environ[DEADLINE_VFS_ENV_VAR] = str((Path(__file__) / "deadline_vfs").resolve())
        test_pid1 = 12345
        test_pid2 = 67890
        test_os_user = "test-user"

        # Create process managers
        process_manager1: VFSProcessManager = VFSProcessManager(
            asset_bucket=self.s3_settings.s3BucketName,
            region=os.environ["AWS_DEFAULT_REGION"],
            manifest_path=manifest_path1,
            mount_point=local_root1,
            os_user=test_os_user,
            os_env_vars={"AWS_PROFILE": "test-profile"},
        )
        process_manager2: VFSProcessManager = VFSProcessManager(
            asset_bucket=self.s3_settings.s3BucketName,
            region=os.environ["AWS_DEFAULT_REGION"],
            manifest_path=manifest_path2,
            mount_point=local_root2,
            os_user=test_os_user,
            os_env_vars={"AWS_PROFILE": "test-profile"},
        )

        with patch(
            f"{deadline.__package__}.job_attachments.vfs.VFSProcessManager.find_vfs",
            return_value="/test/directory/path",
        ), patch(
            f"{deadline.__package__}.job_attachments.vfs.subprocess.Popen",
        ) as mock_popen, patch(
            f"{deadline.__package__}.job_attachments.vfs.VFSProcessManager.wait_for_mount",
            return_value=True,
        ), patch(
            f"{deadline.__package__}.job_attachments.vfs.os.path.exists",
            return_value=True,
        ), patch(
            f"{deadline.__package__}.job_attachments.vfs.subprocess.run"
        ) as mock_subprocess_run, patch(
            f"{deadline.__package__}.job_attachments.vfs.VFSProcessManager.get_launch_environ",
            return_value=os.environ,
        ), patch(
            f"{deadline.__package__}.job_attachments.vfs.VFSProcessManager.is_mount",
            return_value=True,
        ):
            # start first mock VFS process
            mock_subprocess = MagicMock()
            mock_subprocess.pid = test_pid1
            mock_popen.return_value = mock_subprocess
            process_manager1.start(tmp_path)

            # Verify only the first processes' pid is written
            assert VFSProcessManager.get_manifest_path_for_mount(
                session_dir=tmp_path, mount_point=local_root1
            ) == Path(manifest_path1)
            assert not VFSProcessManager.get_manifest_path_for_mount(
                session_dir=tmp_path, mount_point=local_root2
            )

            # start second mock VFS process
            mock_subprocess.pid = test_pid2
            process_manager2.start(tmp_path)

            # Verify both pids are written
            assert VFSProcessManager.get_manifest_path_for_mount(
                session_dir=tmp_path, mount_point=local_root1
            ) == Path(manifest_path1)
            assert VFSProcessManager.get_manifest_path_for_mount(
                session_dir=tmp_path, mount_point=local_root2
            ) == Path(manifest_path2)

            # Verify killing process 1 removes pid entry
            assert VFSProcessManager.kill_process_at_mount(
                session_dir=tmp_path, mount_point=local_root1, os_user=test_os_user
            )
            mock_subprocess_run.assert_called_with(
                VFSProcessManager.get_shutdown_args(local_root1, test_os_user), check=True
            )
            assert not VFSProcessManager.get_manifest_path_for_mount(
                session_dir=tmp_path, mount_point=local_root1
            )
            assert VFSProcessManager.get_manifest_path_for_mount(
                session_dir=tmp_path, mount_point=local_root2
            ) == Path(manifest_path2)

            VFSProcessManager.kill_all_processes(tmp_path, os_user=test_os_user)

            mock_subprocess_run.assert_has_calls(
                [
                    call(
                        VFSProcessManager.get_shutdown_args(local_root1, test_os_user), check=True
                    ),
                    call(
                        VFSProcessManager.get_shutdown_args(local_root2, test_os_user), check=True
                    ),
                ],
                any_order=True,
            )

    def test_manifest_group_set(
        self,
        tmp_path: Path,
    ):
        # Test to verify group ownership of the manifest is set properly on startup

        session_dir: str = str(tmp_path)
        dest_dir: str = "assetroot-27bggh78dd2b568ab123"
        local_root: str = f"{session_dir}/{dest_dir}"
        manifest_path: str = f"{local_root}/manifest.json"
        os.environ[DEADLINE_VFS_ENV_VAR] = str((Path(__file__) / "deadline_vfs").resolve())
        test_os_user = "test-user"
        test_os_group = "test-group"

        # Create process manager
        process_manager1: VFSProcessManager = VFSProcessManager(
            asset_bucket=self.s3_settings.s3BucketName,
            region=os.environ["AWS_DEFAULT_REGION"],
            manifest_path=manifest_path,
            mount_point=local_root,
            os_user=test_os_user,
            os_group=test_os_group,
            os_env_vars={"AWS_PROFILE": "test-profile"},
        )

        with patch(
            f"{deadline.__package__}.job_attachments.vfs.VFSProcessManager.find_vfs",
            return_value="/test/directory/path",
        ), patch(
            f"{deadline.__package__}.job_attachments.vfs.subprocess.Popen",
        ), patch(
            f"{deadline.__package__}.job_attachments.vfs.VFSProcessManager.wait_for_mount",
            return_value=True,
        ), patch(
            f"{deadline.__package__}.job_attachments.vfs.os.path.exists",
            return_value=True,
        ), patch(
            f"{deadline.__package__}.job_attachments.vfs.shutil.chown",
        ) as mock_chown, patch(
            f"{deadline.__package__}.job_attachments.vfs.os.chmod",
        ) as mock_chmod, patch(
            f"{deadline.__package__}.job_attachments.vfs.subprocess.run"
        ), patch(
            f"{deadline.__package__}.job_attachments.vfs.VFSProcessManager.get_launch_environ",
            return_value=os.environ,
        ), patch(
            f"{deadline.__package__}.job_attachments.vfs.VFSProcessManager.is_mount",
            return_value=True,
        ):
            process_manager1.start(tmp_path)

            mock_chown.assert_called_with(manifest_path, group=test_os_group)

            mock_chmod.assert_called_with(manifest_path, DEADLINE_MANIFEST_GROUP_READ_PERMS)


@pytest.mark.parametrize("vfs_cache_enabled", [True, False])
def test_launch_environment_has_expected_settings(
    tmp_path: Path,
    vfs_cache_enabled: bool,
):
    # Test to verify when retrieving the launch environment it does not contain os.environ variables (Unless passed in),
    # it DOES contain the VFSProcessManager's environment variables, and aws configuration variables aren't modified
    session_dir: str = str(tmp_path)
    test_mount: str = f"{session_dir}/test_mount"
    manifest_path1: str = f"{session_dir}/manifests/some_manifest.json"
    os.environ[DEADLINE_VFS_ENV_VAR] = str((Path(__file__) / "deadline_vfs").resolve())
    if vfs_cache_enabled:
        os.environ[DEADLINE_VFS_CACHE_ENV_VAR] = "V0"
    else:
        os.environ.pop(DEADLINE_VFS_CACHE_ENV_VAR, None)

    provided_vars = {
        "VFS_ENV_VAR": "test-vfs-env-var",
        "AWS_PROFILE": "test-profile",
        "AWS_CONFIG_FILE": "test-config",
        "AWS_SHARED_CREDENTIALS_FILE": "test-credentials",
    }
    # Create process managers
    process_manager: VFSProcessManager = VFSProcessManager(
        asset_bucket="test-bucket",
        region="test-region",
        manifest_path=manifest_path1,
        mount_point=test_mount,
        os_user="test-user",
        os_env_vars=provided_vars,
    )

    # Provided environment variables are passed through
    with patch(
        f"{deadline.__package__}.job_attachments.vfs.VFSProcessManager.find_vfs",
        return_value="/test/directory/path",
    ):
        launch_env = process_manager.get_launch_environ()

    for key, value in provided_vars.items():
        assert launch_env.get(key) == value

    if vfs_cache_enabled:
        assert launch_env.get(DEADLINE_VFS_CACHE_ENV_VAR) == "V0"
    else:
        assert launch_env.get(DEADLINE_VFS_CACHE_ENV_VAR) is None

    # Base environment variables are not passed through
    assert not launch_env.get(DEADLINE_VFS_ENV_VAR)


def test_vfs_launched_in_session_folder(
    tmp_path: Path,
):
    # Test to verify the cwd of launched vfs is the session folder

    session_dir: str = str(tmp_path)
    dest_dir: str = "assetroot-cwdtest"
    local_root: str = f"{session_dir}/{dest_dir}"
    manifest_path: str = f"{local_root}/manifest.json"
    os.environ[DEADLINE_VFS_ENV_VAR] = str((Path(__file__) / "deadline_vfs").resolve())

    # Create process manager
    process_manager: VFSProcessManager = VFSProcessManager(
        asset_bucket="test-bucket",
        region="test-region",
        manifest_path=manifest_path,
        mount_point=local_root,
        os_user="test-user",
        os_env_vars={"AWS_PROFILE": "test-profile"},
    )

    with patch(
        f"{deadline.__package__}.job_attachments.vfs.VFSProcessManager.find_vfs",
        return_value="/test/directory/path",
    ), patch(
        f"{deadline.__package__}.job_attachments.vfs.subprocess.Popen",
    ) as mock_popen, patch(
        f"{deadline.__package__}.job_attachments.vfs.VFSProcessManager.wait_for_mount",
        return_value=True,
    ), patch(
        f"{deadline.__package__}.job_attachments.vfs.os.path.exists",
        return_value=True,
    ), patch(
        f"{deadline.__package__}.job_attachments.vfs.VFSProcessManager.get_launch_environ",
        return_value=os.environ,
    ):
        process_manager.start(tmp_path)

        launch_command = process_manager.build_launch_command(mount_point=local_root)
        launch_env = process_manager.get_launch_environ()

        mock_popen.assert_called_once_with(
            args=launch_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=session_dir,  # Was the session folder used as cwd
            env=launch_env,
            shell=True,
            executable="/bin/bash",
        )


def test_vfs_has_expected_logs_folder(
    tmp_path: Path,
):
    # Test to verify the expected logs folder is returned

    session_dir: str = str(tmp_path)
    dest_dir: str = "assetroot-logsdirtest"
    local_root: str = f"{session_dir}/{dest_dir}"
    manifest_path: str = f"{local_root}/manifest.json"
    os.environ[DEADLINE_VFS_ENV_VAR] = str((Path(__file__) / "deadline_vfs").resolve())
    expected_logs_folder = tmp_path / VFS_LOGS_FOLDER_IN_SESSION

    # Create process manager
    process_manager: VFSProcessManager = VFSProcessManager(
        asset_bucket="test-bucket",
        region="test-region",
        manifest_path=manifest_path,
        mount_point=local_root,
        os_user="test-user",
        os_env_vars={"AWS_PROFILE": "test-profile"},
    )

    assert VFSProcessManager.logs_folder_path(tmp_path) == expected_logs_folder

    with patch(
        f"{deadline.__package__}.job_attachments.vfs.VFSProcessManager.find_vfs",
        return_value="/test/directory/path",
    ), patch(
        f"{deadline.__package__}.job_attachments.vfs.subprocess.Popen",
    ), patch(
        f"{deadline.__package__}.job_attachments.vfs.VFSProcessManager.wait_for_mount",
        return_value=True,
    ), patch(
        f"{deadline.__package__}.job_attachments.vfs.os.path.exists",
        return_value=True,
    ), patch(
        f"{deadline.__package__}.job_attachments.vfs.VFSProcessManager.get_launch_environ",
        return_value=os.environ,
    ):
        process_manager.start(tmp_path)

        assert process_manager.get_logs_folder() == expected_logs_folder
