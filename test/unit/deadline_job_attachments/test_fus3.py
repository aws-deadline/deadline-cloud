# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for the Asset Synching class for task-level attachments."""


import os
import stat
import sys
from pathlib import Path
import threading
from typing import List, Union
from unittest.mock import Mock, patch, call, MagicMock
from signal import SIGTERM

import pytest

import deadline
from deadline.job_attachments.asset_sync import AssetSync
from deadline.job_attachments.exceptions import Fus3ExecutableMissingError
from deadline.job_attachments.models import JobAttachmentS3Settings
from deadline.job_attachments.fus3 import (
    Fus3ProcessManager,
    FUS3_PATH_ENV_VAR,
    FUS3_EXECUTABLE,
    FUS3_EXECUTABLE_SCRIPT,
    FUS3_PID_FILE_NAME,
)


# TODO: Remove the skip once we support Windows for AssetSync
@pytest.mark.skipif(sys.platform == "win32", reason="Fus3 doesn't currently support Windows")
class TestFus3Processmanager:
    @pytest.fixture(autouse=True)
    def before_test(
        self,
        request,
        create_s3_bucket,
        default_job_attachment_s3_settings: JobAttachmentS3Settings,
        default_asset_sync: AssetSync,
    ):
        """
        Setup the default queue and s3 bucket for all asset tests.
        Mark test with `no_setup` if you don't want this setup to run.
        """
        if "no_setup" in request.keywords:
            return

        create_s3_bucket(bucket_name=default_job_attachment_s3_settings.s3BucketName)
        self.default_asset_sync = default_asset_sync
        self.s3_settings = default_job_attachment_s3_settings

    # TODO: Remove this test once we support Windows for Fus3
    @patch("sys.platform", "win32")
    def test_init_fails_on_windows(self) -> None:
        """Asserts an error is raised when trying to create a Fus3ProcessManager
        instance on a Windows OS"""
        with pytest.raises(NotImplementedError):
            Fus3ProcessManager(
                asset_bucket=self.s3_settings.s3BucketName,
                region=os.environ["AWS_DEFAULT_REGION"],
                manifest_path="/test/manifest/path",
                mount_point="/test/mount/point",
            )

    def test_build_launch_command(
        self,
        tmp_path: Path,
    ):
        os.environ[FUS3_PATH_ENV_VAR] = str((Path(__file__) / "fus3").resolve())
        session_dir: str = str(tmp_path)
        dest_dir: str = "assetroot-27bggh78dd2b568ab123"
        local_root: str = f"{session_dir}/{dest_dir}"
        manifest_path: str = f"{local_root}/manifest.json"

        # Create process manager without CAS prefix
        process_manager: Fus3ProcessManager = Fus3ProcessManager(
            asset_bucket=self.s3_settings.s3BucketName,
            region=os.environ["AWS_DEFAULT_REGION"],
            manifest_path=manifest_path,
            mount_point=local_root,
        )

        test_executable = os.environ[FUS3_PATH_ENV_VAR] + FUS3_EXECUTABLE_SCRIPT

        expected_launch_command: List = [
            "%s %s -f --clienttype=deadline --bucket=%s --manifest=%s --region=%s -oallow_other"
            % (
                test_executable,
                local_root,
                self.s3_settings.s3BucketName,
                manifest_path,
                os.environ["AWS_DEFAULT_REGION"],
            )
        ]
        assert (
            process_manager.build_launch_command(mount_point=local_root) == expected_launch_command
        )

        # Create process manager with CAS prefix
        test_CAS_prefix: str = "test_prefix"
        process_manager = Fus3ProcessManager(
            asset_bucket=self.s3_settings.s3BucketName,
            region=os.environ["AWS_DEFAULT_REGION"],
            manifest_path=manifest_path,
            mount_point=local_root,
            cas_prefix=test_CAS_prefix,
        )

        expected_launch_command = [
            "%s %s -f --clienttype=deadline --bucket=%s --manifest=%s --region=%s --casprefix=%s -oallow_other"
            % (
                test_executable,
                local_root,
                self.s3_settings.s3BucketName,
                manifest_path,
                os.environ["AWS_DEFAULT_REGION"],
                test_CAS_prefix,
            )
        ]
        assert (
            process_manager.build_launch_command(mount_point=local_root) == expected_launch_command
        )

    def test_find_fus3(
        self,
        tmp_path: Path,
    ):
        session_dir: str = str(tmp_path)
        dest_dir: str = "assetroot-27bggh78dd2b568ab123"
        local_root: str = f"{session_dir}/{dest_dir}"
        manifest_path: str = f"{local_root}/manifest.json"
        os.environ[FUS3_PATH_ENV_VAR] = str((Path(__file__) / "fus3").resolve())

        # Create process manager without CAS prefix
        process_manager: Fus3ProcessManager = Fus3ProcessManager(
            asset_bucket=self.s3_settings.s3BucketName,
            region=os.environ["AWS_DEFAULT_REGION"],
            manifest_path=manifest_path,
            mount_point=local_root,
        )

        # verify which is only called when class path is not set
        with patch(f"{deadline.__package__}.job_attachments.fus3.shutil.which") as mock_which:
            mock_which.return_value = "/test/path"
            test_path: Union[os.PathLike, str] = process_manager.find_fus3()

            assert str(test_path) == "/test/path"
            test_path = process_manager.find_fus3()
            mock_which.assert_called_once()

            # Reset fus3 path and remove from PATH so other methods are checked
            mock_which.return_value = None
            Fus3ProcessManager.fus3_path = None

            with patch(
                f"{deadline.__package__}.job_attachments.fus3.os.path.exists"
            ) as mock_path_exists:
                mock_path_exists.return_value = False

                with pytest.raises(Fus3ExecutableMissingError):
                    process_manager.find_fus3()

                # Verify FUS3_PATH_ENV_VAR location is checked
                # Verify bin folder is checked as a last resort
                mock_path_exists.assert_has_calls(
                    [
                        call(os.environ[FUS3_PATH_ENV_VAR] + "/bin/fus3"),
                        call(os.path.join(os.getcwd(), f"bin/{FUS3_EXECUTABLE}")),
                    ]
                )

    def test_find_library_path(
        self,
        tmp_path: Path,
    ):
        session_dir: str = str(tmp_path)
        dest_dir: str = "assetroot-27bggh78dd2b568ab123"
        local_root: str = f"{session_dir}/{dest_dir}"
        manifest_path: str = f"{local_root}/manifest.json"
        os.environ[FUS3_PATH_ENV_VAR] = str((Path(__file__) / "fus3").resolve())

        # Create process manager without CAS prefix
        process_manager: Fus3ProcessManager = Fus3ProcessManager(
            asset_bucket=self.s3_settings.s3BucketName,
            region=os.environ["AWS_DEFAULT_REGION"],
            manifest_path=manifest_path,
            mount_point=local_root,
        )

        with patch(
            f"{deadline.__package__}.job_attachments.fus3.Fus3ProcessManager.find_fus3"
        ) as mock_find_fus3:
            mock_find_fus3.return_value = "/test/directory/path"
            library_path: Union[os.PathLike, str] = process_manager.get_library_path()

            assert str(library_path) == "/test/lib"

            process_manager.get_library_path()

            mock_find_fus3.assert_called_once()

    def test_create_mount_point(
        self,
        tmp_path: Path,
    ):
        session_dir: str = str(tmp_path)
        dest_dir: str = "assetroot-27bggh78dd2b568ab123"
        local_root: str = f"{session_dir}/{dest_dir}"
        manifest_path: str = f"{local_root}/manifest.json"
        os.environ[FUS3_PATH_ENV_VAR] = str((Path(__file__) / "fus3").resolve())

        # Create process manager without CAS prefix
        process_manager: Fus3ProcessManager = Fus3ProcessManager(
            asset_bucket=self.s3_settings.s3BucketName,
            region=os.environ["AWS_DEFAULT_REGION"],
            manifest_path=manifest_path,
            mount_point=local_root,
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
        os.environ[FUS3_PATH_ENV_VAR] = str((Path(__file__) / "fus3").resolve())
        test_pid1 = 12345
        test_pid2 = 67890

        # Create process managers
        process_manager1: Fus3ProcessManager = Fus3ProcessManager(
            asset_bucket=self.s3_settings.s3BucketName,
            region=os.environ["AWS_DEFAULT_REGION"],
            manifest_path=manifest_path1,
            mount_point=local_root1,
        )
        process_manager2: Fus3ProcessManager = Fus3ProcessManager(
            asset_bucket=self.s3_settings.s3BucketName,
            region=os.environ["AWS_DEFAULT_REGION"],
            manifest_path=manifest_path2,
            mount_point=local_root2,
        )

        with patch(
            f"{deadline.__package__}.job_attachments.fus3.Fus3ProcessManager.find_fus3",
            return_value="/test/directory/path",
        ), patch(
            f"{deadline.__package__}.job_attachments.fus3.subprocess.Popen",
        ) as mock_popen, patch(
            f"{deadline.__package__}.job_attachments.fus3.Fus3ProcessManager.wait_for_mount",
            return_value=True,
        ), patch(
            f"{deadline.__package__}.job_attachments.fus3.os.kill",
        ) as mock_os_kill, patch(
            f"{deadline.__package__}.job_attachments.fus3.subprocess.run"
        ) as mock_subprocess_run:
            # start first mock fus3 process
            mock_subprocess = MagicMock()
            mock_subprocess.pid = test_pid1
            mock_popen.return_value = mock_subprocess
            process_manager1.start(tmp_path)

            # start second mock fus3 process
            mock_subprocess.pid = test_pid2
            process_manager2.start(tmp_path)

            # verify the pids were written to the correct location
            pid_file_path = (tmp_path / FUS3_PID_FILE_NAME).resolve()
            with open(pid_file_path, "r") as pid_file:
                pid_file_contents = pid_file.readlines()
                assert f"{test_pid1}\n" in pid_file_contents
                assert f"{test_pid2}\n" in pid_file_contents

            assert os.path.exists(local_root1)
            assert os.path.exists(local_root2)

            Fus3ProcessManager.kill_all_processes(tmp_path)
            # Verify all processes in pid file were killed
            mock_os_kill.assert_has_calls(
                [
                    call(test_pid1, SIGTERM),
                    call(test_pid2, SIGTERM),
                ]
            )
            mock_subprocess_run.assert_has_calls(
                [
                    call(["/bin/pkill", "-P", str(test_pid1)]),
                    call(["/bin/pkill", "-P", str(test_pid2)]),
                ]
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
        os.environ[FUS3_PATH_ENV_VAR] = str((Path(__file__) / "fus3").resolve())
        test_pid1 = 12345

        # Create process managers
        process_manager1: Fus3ProcessManager = Fus3ProcessManager(
            asset_bucket=self.s3_settings.s3BucketName,
            region=os.environ["AWS_DEFAULT_REGION"],
            manifest_path=manifest_path1,
            mount_point=local_root1,
        )

        with patch(
            f"{deadline.__package__}.job_attachments.fus3.Fus3ProcessManager.find_fus3",
            return_value="/test/directory/path",
        ), patch(
            f"{deadline.__package__}.job_attachments.fus3.subprocess.Popen",
        ) as mock_popen, patch(
            f"{deadline.__package__}.job_attachments.fus3.Fus3ProcessManager.wait_for_mount",
            return_value=True,
        ), patch(
            f"{deadline.__package__}.job_attachments.fus3.log"
        ) as mock_logger:
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
