# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Integration tests for the incremental download CLI functionality.
"""

import os
import subprocess
import time
from typing import Optional, Tuple

import boto3
import pytest
from pathlib import Path

from deadline.client.api._job_monitoring import wait_for_job_completion
from deadline.job_attachments._utils import _retry
from .job_templates import (
    submit_dep_chain_job,
    submit_dep_data_flow_job,
    submit_make_many_small_files_job,
    submit_make_many_small_files_slow_job,
)
from .test_utils import DeadlineCliTest


class IncrementalDownloadTest:
    """
    Class for Incremental Download Integration Tests.
    """

    def __init__(self, farm_id: str, queue_id: str):
        self.farm_id = farm_id
        self.queue_id = queue_id
        self.deadline_client = boto3.client("deadline")

    def wait_for_job_completion(
        self, job_id: str, timeout: int = 600, poll_interval: int = 5
    ) -> Tuple[bool, str]:
        """Wait for a job to complete. Returns (isSuccess, final_status)."""
        try:
            result = wait_for_job_completion(
                farm_id=self.farm_id,
                queue_id=self.queue_id,
                job_id=job_id,
                timeout=timeout,
                max_poll_interval=poll_interval,
            )
            return result.status == "SUCCEEDED", result.status
        except Exception as e:
            return False, f"TIMEOUT! Received downstream exception: {e}"

    @_retry(tries=60, delay=2, backoff=1.0)
    def wait_for_all_files(
        self,
        tmp_path: Path,
        expected_files: dict,
        test_name: str,
        file_pattern: str = "**/*.out",
        content_check: Optional[str] = None,
        exact_match: bool = False,
        count_only: bool = False,
    ):
        """Generic function to wait for all expected files to be downloaded with correct content."""
        result = self.run_incremental_download_without_storage_profiles(
            str(tmp_path), test_name=test_name
        )
        assert result.returncode == 0, f"Download failed: {result.stderr}"

        downloaded_files = list(tmp_path.glob(file_pattern))

        if count_only:
            # For tests that only care about file count (like make_many_small_files)
            expected_count = expected_files.get("count", 0)
            print(f"[{test_name}] Found {len(downloaded_files)}/{expected_count} files")
            assert len(downloaded_files) == expected_count, (
                f"Expected exactly {expected_count} files, but found {len(downloaded_files)}"
            )
            return

        verified_files = []

        for downloaded_file in downloaded_files:
            filename = downloaded_file.name
            if filename in expected_files:
                expected_marker = expected_files[filename]
                content = downloaded_file.read_text()

                if exact_match:
                    # For exact content matching (like dep_chain)
                    if content.strip() == expected_marker:
                        verified_files.append(filename)
                else:
                    # For content containing markers (like dep_data_flow)
                    content_valid = True
                    if content_check:
                        content_valid = content_check in content

                    if content_valid and expected_marker in content:
                        verified_files.append(filename)

        expected_count = len(expected_files)
        print(f"[{test_name}] Found {len(verified_files)}/{expected_count} files")
        assert len(verified_files) == expected_count, (
            f"Expected exactly {expected_count} verified output files, found {len(verified_files)}: {verified_files}"
        )

    def run_incremental_download_without_storage_profiles(
        self,
        output_dir: str,
        force_bootstrap: bool = False,
        conflict_resolution: Optional[str] = None,
        lookback_window: Optional[int] = None,
        test_name: Optional[str] = None,
    ) -> subprocess.CompletedProcess:
        """Run the incremental download CLI command."""
        # Use test-specific checkpoint directory to avoid conflicts between parallel tests
        checkpoint_suffix = f"_{test_name}" if test_name else ""
        checkpoint_dir = os.path.join(output_dir, f"checkpoints{checkpoint_suffix}")
        os.makedirs(checkpoint_dir, exist_ok=True)

        cmd = [
            "deadline",
            "queue",
            "sync-output",
            "--farm-id",
            self.farm_id,
            "--queue-id",
            self.queue_id,
            "--checkpoint-dir",
            checkpoint_dir,
            "--ignore-storage-profiles",
        ]

        if force_bootstrap:
            cmd.append("--force-bootstrap")

        if conflict_resolution:
            cmd.extend(["--conflict-resolution", conflict_resolution])

        if lookback_window is not None:
            cmd.extend(["--bootstrap-lookback-minutes", str(lookback_window)])

        # Run from the specified output directory so files are downloaded to their manifest paths
        # The CLI will create the necessary directory structure based on job manifests
        result = subprocess.run(cmd, capture_output=True, text=True, check=False, cwd=output_dir)

        # Print CLI output for debugging
        if result.stdout:
            print(f"[sync-output] STDOUT:\n{result.stdout}")
        if result.stderr:
            print(f"[sync-output] STDERR:\n{result.stderr}")
        if result.returncode != 0:
            print(f"[sync-output] Exit code: {result.returncode}")

        return result


@pytest.fixture(scope="session")
def incremental_download_test(deadline_cli_test: DeadlineCliTest):
    """Fixture to get the IncrementalDownloadTest object."""
    return IncrementalDownloadTest(
        farm_id=deadline_cli_test.farm_id, queue_id=deadline_cli_test.queue_id
    )


@pytest.mark.integ
@pytest.mark.timeout(900)  # 15 minutes timeout
@pytest.mark.xfail(
    reason="Soft-fail until edge case is root caused and fixed in sync-output CLI", strict=False
)
def test_incremental_download_many_small_files(incremental_download_test, tmp_path):
    """Test incremental download with many small files (10,000 files total)."""

    files_per_task = 100
    task_count = 100
    total_files = files_per_task * task_count  # 10,000 files

    unique_output_dir = f"{tmp_path}/many_small_files_output"

    print(
        f"[make_many_small_files] Submitting job with {total_files} files ({task_count} tasks × {files_per_task} files)"
    )
    job_id = submit_make_many_small_files_job(
        farm_id=incremental_download_test.farm_id,
        queue_id=incremental_download_test.queue_id,
        files_per_task=files_per_task,
        task_count=task_count,
        output_dir=unique_output_dir,
    )
    print(f"[make_many_small_files] Job submitted with ID: {job_id}")

    # Run incremental download in a loop until job completes
    job_complete = False
    incremental_download_iteration_number = 0
    while not job_complete:
        incremental_download_iteration_number += 1

        job = incremental_download_test.deadline_client.get_job(
            farmId=incremental_download_test.farm_id,
            queueId=incremental_download_test.queue_id,
            jobId=job_id,
        )

        task_run_status = job.get("taskRunStatus")

        # Check if job failed
        if task_run_status == "FAILED":
            print(f"[make_many_small_files] Job {job_id} FAILED - stopping test")
            assert False, f"Job {job_id} failed with status: {task_run_status}"

        incremental_download_test.run_incremental_download_without_storage_profiles(
            str(tmp_path),
            force_bootstrap=(incremental_download_iteration_number == 1),
            lookback_window=2,
            test_name="many_small_files",
        )

        job_complete = task_run_status in ["SUCCEEDED", "FAILED", "CANCELED"]
        # Wait 5 secs if job's still not complete
        if not job_complete:
            time.sleep(5)

    print(
        f"[make_many_small_files] Job completed after {incremental_download_iteration_number} download iterations"
    )

    # Run final incremental download to ensure all files are captured
    incremental_download_test.wait_for_all_files(
        tmp_path=tmp_path,
        expected_files={"count": total_files},
        test_name="make_many_small_files",
        file_pattern="**/*.txt",
        count_only=True,
    )

    print(f"[make_many_small_files] Successfully verified all {total_files} files were downloaded")


@pytest.mark.integ
@pytest.mark.timeout(900)  # 15 minutes timeout
@pytest.mark.xfail(
    reason="Soft-fail until edge case is root caused and fixed in sync-output CLI", strict=False
)
def test_incremental_download_dep_data_flow(incremental_download_test, tmp_path):
    """Test incremental download with dep_data_flow template."""

    # Create data and input directories in tmp_path
    unique_data_dir = tmp_path / "data_dir"
    unique_data_dir.mkdir()

    # Create the required input files for the job template
    create_job_input_content = "1. Input to CreateJob from data_dir\n"

    # Create 2 files in the data directory (template expects exactly 2)
    (unique_data_dir / "create_job_in.txt").write_text(create_job_input_content)
    (unique_data_dir / "initial_data.txt").write_text("Initial data file for job\n")

    # Also create input_dir with 2 required files (template expects exactly 2)
    unique_input_dir = tmp_path / "input_dir"
    unique_input_dir.mkdir()
    (unique_input_dir / "create_job_in.txt").write_text(create_job_input_content)
    (unique_input_dir / "initial_input.txt").write_text("Initial input file for job\n")

    job_id = submit_dep_data_flow_job(
        farm_id=incremental_download_test.farm_id,
        queue_id=incremental_download_test.queue_id,
        data_dir=str(unique_data_dir),
        input_dir=str(unique_input_dir),
    )
    print(f"[dep_data_flow] Job submitted with ID: {job_id}")

    # Run incremental download in a loop until job completes
    job_complete = False
    incremental_download_iteration_number = 0
    force_bootstrap_first = True  # Force bootstrap on first run only
    while not job_complete:
        incremental_download_iteration_number += 1
        print(
            f"[dep_data_flow] Running incremental download iteration {incremental_download_iteration_number}..."
        )

        job = incremental_download_test.deadline_client.get_job(
            farmId=incremental_download_test.farm_id,
            queueId=incremental_download_test.queue_id,
            jobId=job_id,
        )

        task_run_status = job.get("taskRunStatus")

        # Check if job failed
        if task_run_status == "FAILED":
            print(f"[dep_data_flow] Job {job_id} FAILED - stopping test")
            assert False, f"Job {job_id} failed with status: {task_run_status}"

        incremental_download_test.run_incremental_download_without_storage_profiles(
            str(tmp_path),
            force_bootstrap=force_bootstrap_first,
            lookback_window=2,
            test_name="dep_data_flow",
        )
        force_bootstrap_first = False  # Only bootstrap on first iteration

        job_complete = task_run_status in ["SUCCEEDED", "FAILED", "CANCELED"]

        # Wait 5 secs if job's still not complete
        if not job_complete:
            time.sleep(5)

    print(
        f"[dep_data_flow] Job completed after {incremental_download_iteration_number} download iterations"
    )

    # Wait for all output files to be available with incremental download
    expected_files = {
        "Step1.out": "2. Processed in Step1",
        "Step1-2.8.out": "3.8 Processed in Step1-2.8",
        "Step1-2.9.out": "3.9 Processed in Step1-2.9",
        "Step1-2.10.out": "3.10 Processed in Step1-2.10",
        "Step1-2.11.out": "3.11 Processed in Step1-2.11",
        "Step1-2-3.out": "4. Processed in Step1-2-3",
        "Step1-2-4.out": "4. Processed in Step1-2-4",
        "Step1-2-34-5.out": "5. Processed in Step1-2-34-5",
    }

    incremental_download_test.wait_for_all_files(
        tmp_path=unique_data_dir,
        expected_files=expected_files,
        test_name="dep_data_flow",
        file_pattern="**/*.out",
        content_check="1. Input to CreateJob from data_dir",
    )

    print("[dep_data_flow] Successfully verified all 8 expected output files with correct content")


@pytest.mark.integ
@pytest.mark.timeout(900)  # 15 minutes timeout
@pytest.mark.xfail(
    reason="Workaround until scheduler does not pick this job, flaky test", strict=False
)
def test_incremental_download_dependency_chain(incremental_download_test, tmp_path):
    """Test incremental download with dep_chain template."""

    unique_output_dir = f"{tmp_path}/dep_chain_output"

    job_id = submit_dep_chain_job(
        farm_id=incremental_download_test.farm_id,
        queue_id=incremental_download_test.queue_id,
        output_dir=unique_output_dir,
    )

    # Run incremental download in a loop until job completes
    job_complete = False
    force_bootstrap = True
    while not job_complete:
        job = incremental_download_test.deadline_client.get_job(
            farmId=incremental_download_test.farm_id,
            queueId=incremental_download_test.queue_id,
            jobId=job_id,
        )

        task_run_status = job.get("taskRunStatus")

        # Check if job failed
        if task_run_status == "FAILED":
            assert False, f"Job {job_id} failed with status: {task_run_status}"

        incremental_download_test.run_incremental_download_without_storage_profiles(
            str(tmp_path),
            force_bootstrap=force_bootstrap,
            lookback_window=2,
            test_name="dep_chain",
        )
        force_bootstrap = False

        job_complete = task_run_status in ["SUCCEEDED", "FAILED", "CANCELED"]
        if not job_complete:
            time.sleep(5)

    # Run final incremental download to ensure all files are captured
    time.sleep(5)

    # Verify expected output files from dep_chain template
    expected_files = {}
    for i in range(6):  # A through F = 6 files
        step_name = chr(ord("A") + i)
        filename = f"{step_name}.txt"
        expected_content = f"Step {step_name} is correct"
        expected_files[filename] = expected_content

    print(f"[dep_chain] Expected files: {expected_files}")

    incremental_download_test.wait_for_all_files(
        tmp_path=tmp_path,
        expected_files=expected_files,
        test_name="dep_chain",
        file_pattern="**/*.txt",
        exact_match=True,
    )

    print("[dep_chain] Successfully verified all 6 expected chain files with correct content")


@pytest.mark.integ
@pytest.mark.timeout(1200)  # 20 minutes timeout, update step & task add latency
@pytest.mark.xfail(
    reason="Soft-fail until edge case is root caused and fixed in sync-output CLI", strict=False
)
@pytest.mark.parametrize("requeue_level", ["job", "step", "task"])
def test_conflict_resolution_with_requeue(incremental_download_test, requeue_level, tmp_path):
    """Test incremental download with re-queuing at different levels and conflict resolution."""

    files_per_task = 10
    task_count = 2
    expected_initial_files = files_per_task * task_count

    unique_output_dir = tmp_path / f"output_{requeue_level}"
    unique_output_dir.mkdir()

    # Added this to be sure that the unique output directory is not being shared
    assert os.listdir(unique_output_dir) == []

    # Submit and wait for initial job
    job_id = submit_make_many_small_files_slow_job(
        farm_id=incremental_download_test.farm_id,
        queue_id=incremental_download_test.queue_id,
        files_per_task=files_per_task,
        task_count=task_count,
        output_dir=unique_output_dir,
    )

    job_completed, final_status = incremental_download_test.wait_for_job_completion(
        job_id, timeout=600
    )
    assert job_completed, f"Initial job failed with status: {final_status}"

    # Download initial files
    _run_download_until_complete(
        incremental_download_test,
        tmp_path,
        unique_output_dir,
        expected_initial_files,
        requeue_level,
        "initial",
    )

    # Requeue at specified level
    _requeue_at_level(incremental_download_test, job_id, requeue_level)

    # Wait for requeue completion
    requeue_completed, requeue_status = incremental_download_test.wait_for_job_completion(
        job_id, timeout=600
    )
    assert requeue_completed, f"Requeue failed with status: {requeue_status}"

    # Download files after requeue
    expected_final = _get_expected_file_count(requeue_level, expected_initial_files, files_per_task)
    _run_download_until_complete(
        incremental_download_test,
        tmp_path,
        unique_output_dir,
        expected_final,
        requeue_level,
        "requeue",
    )


def _run_download_until_complete(
    test_instance, tmp_path, unique_output_dir, expected_count, level, phase
):
    """Run incremental download until expected file count is reached."""

    # Use the actual unique output directory passed from the test
    test_output_dir = Path(unique_output_dir)

    for iteration in range(1, 11):  # Max 10 iterations
        result = test_instance.run_incremental_download_without_storage_profiles(
            str(tmp_path),
            force_bootstrap=(phase == "initial" and iteration == 1),
            conflict_resolution="create_copy",
            lookback_window=1,
            test_name=f"requeue_{level}",
        )

        if result.returncode != 0 and "had incorrect size 0" not in result.stdout:
            assert False, f"{phase.title()} download failed: {result.stderr}"

        # Only look for files in the test-specific output directory
        files = list(test_output_dir.glob("**/file_*")) if test_output_dir.exists() else []
        if len(files) >= expected_count:
            break
        time.sleep(2)

    assert len(files) == expected_count, (
        f"Expected {expected_count} files after {phase} in {test_output_dir}, got {len(files)}"
    )


def _wait_for_requeue_to_take_effect(test_instance, job_id, timeout=60):
    """Wait for requeue to take effect by checking job status changes from SUCCEEDED."""
    client = test_instance.deadline_client
    farm_id = test_instance.farm_id
    queue_id = test_instance.queue_id

    print(f"Waiting for requeue to take effect for job {job_id}...")

    start_time = time.time()
    while time.time() - start_time < timeout:
        job = client.get_job(farmId=farm_id, queueId=queue_id, jobId=job_id)
        task_run_status = job.get("taskRunStatus")

        print(f"Current job taskRunStatus: {task_run_status}")

        # Job has been re-queued if it's no longer SUCCEEDED
        if task_run_status in ["READY", "ASSIGNED", "STARTING", "SCHEDULED", "RUNNING"]:
            print(f"Requeue took effect - job status is now: {task_run_status}")
            return True

        time.sleep(2)

    # If we get here, requeue didn't take effect within timeout
    job = client.get_job(farmId=farm_id, queueId=queue_id, jobId=job_id)
    current_status = job.get("taskRunStatus")
    raise AssertionError(
        f"Requeue did not take effect within {timeout}s. Job status is still: {current_status}"
    )


def _requeue_at_level(test_instance, job_id, level):
    """Requeue job at the specified level."""
    client = test_instance.deadline_client
    farm_id = test_instance.farm_id
    queue_id = test_instance.queue_id

    if level == "job":
        client.update_job(
            farmId=farm_id, queueId=queue_id, jobId=job_id, targetTaskRunStatus="READY"
        )
    else:
        steps = client.list_steps(farmId=farm_id, queueId=queue_id, jobId=job_id)["steps"]
        assert steps, "No steps found in job"
        step_id = steps[0]["stepId"]

        if level == "step":
            client.update_step(
                farmId=farm_id,
                queueId=queue_id,
                jobId=job_id,
                stepId=step_id,
                targetTaskRunStatus="READY",
            )
        else:  # task
            tasks = client.list_tasks(
                farmId=farm_id, queueId=queue_id, jobId=job_id, stepId=step_id
            )["tasks"]
            assert tasks, "No tasks found in step"
            task_id = tasks[0]["taskId"]
            client.update_task(
                farmId=farm_id,
                queueId=queue_id,
                jobId=job_id,
                stepId=step_id,
                taskId=task_id,
                targetRunStatus="READY",
            )

    print(f"Requeue API call completed for {level} level")

    # Wait for the requeue to actually take effect
    _wait_for_requeue_to_take_effect(test_instance, job_id)


def _get_expected_file_count(level, initial_count, files_per_task):
    """Calculate expected file count after requeue based on level."""
    return initial_count * 2 if level in ["job", "step"] else initial_count + files_per_task
