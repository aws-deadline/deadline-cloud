# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Integration tests for the `deadline bundle submit` CLI command.
"""

import re
import shutil

import boto3
import pytest
from click.testing import CliRunner

from pathlib import Path

from deadline.client.cli import main

from .test_utils import DeadlineCliTest

ECHO_BUNDLE = str(Path(__file__).parent / "job_bundles" / "echo_with_attachment")


@pytest.fixture(scope="module")
def deadline_client():
    """Create a boto3 deadline client for the test module."""
    return boto3.client("deadline")


def _extract_job_id(output: str) -> str:
    """Pull the job-<hex> id that the CLI prints after a successful submit."""
    match = re.search(r"(job-[0-9a-f]{32})", output)
    assert match, f"Could not find job ID in output:\n{output}"
    return match.group(1)


def test_bundle_submit(deadline_cli_test: DeadlineCliTest, deadline_client) -> None:
    """Verify `deadline bundle submit` with --name, --priority, and --target-task-run-status."""
    runner = CliRunner()

    result = runner.invoke(
        main,
        [
            "bundle",
            "submit",
            ECHO_BUNDLE,
            "--farm-id",
            deadline_cli_test.farm_id,
            "--queue-id",
            deadline_cli_test.queue_id,
            "--name",
            "integ-test-bundle-submit",
            "--priority",
            "75",
            "--target-task-run-status",
            "SUSPENDED",
            "--yes",
        ],
    )

    assert result.exit_code == 0, result.output

    job_id = _extract_job_id(result.output)

    deadline_client.get_waiter("job_create_complete").wait(
        farmId=deadline_cli_test.farm_id,
        queueId=deadline_cli_test.queue_id,
        jobId=job_id,
    )

    job = deadline_client.get_job(
        farmId=deadline_cli_test.farm_id,
        queueId=deadline_cli_test.queue_id,
        jobId=job_id,
    )
    assert job["name"] == "integ-test-bundle-submit"
    assert job["priority"] == 75
    assert job["taskRunStatus"] == "SUSPENDED"
    assert "attachments" in job, "Expected job to have attachments"
    assert len(job["attachments"]["manifests"]) == 1, (
        f"Expected exactly 1 attachment manifest, got {len(job['attachments']['manifests'])}"
    )


# ---------------------------------------------------------------------------
# job download-output
#
# Requires a completed job with output attachments. We submit the
# echo_with_attachment bundle as READY, wait for it to succeed, then
# download the output.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def completed_job_with_output(deadline_cli_test: DeadlineCliTest, deadline_client) -> str:
    """
    Submit the echo_with_attachment bundle via CLI, wait for it to complete.

    Returns the job ID of a succeeded job that has downloadable output.
    """
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "bundle",
            "submit",
            ECHO_BUNDLE,
            "--farm-id",
            deadline_cli_test.farm_id,
            "--queue-id",
            deadline_cli_test.queue_id,
            "--name",
            "integ-test-download-output",
            "--target-task-run-status",
            "READY",
            "--yes",
        ],
    )
    assert result.exit_code == 0, result.output
    job_id = _extract_job_id(result.output)

    deadline_client.get_waiter("job_create_complete").wait(
        farmId=deadline_cli_test.farm_id,
        queueId=deadline_cli_test.queue_id,
        jobId=job_id,
    )
    deadline_client.get_waiter("job_succeeded").wait(
        farmId=deadline_cli_test.farm_id,
        queueId=deadline_cli_test.queue_id,
        jobId=job_id,
    )
    return job_id


def test_job_download_output(
    deadline_cli_test: DeadlineCliTest,
    completed_job_with_output: str,
    tmp_path,
) -> None:
    """Verify `deadline job download-output` downloads files from a completed job."""
    runner = CliRunner()

    # The download will place files under the original root path from the manifest,
    # which is the echo_with_attachment bundle directory.
    output_dir = Path(ECHO_BUNDLE) / "output"
    result_file = output_dir / "result.txt"

    try:
        result = runner.invoke(
            main,
            [
                "job",
                "download-output",
                "--farm-id",
                deadline_cli_test.farm_id,
                "--queue-id",
                deadline_cli_test.queue_id,
                "--job-id",
                completed_job_with_output,
                "--conflict-resolution",
                "OVERWRITE",
                "--yes",
            ],
        )

        assert result.exit_code == 0, result.output
        assert "Downloading Outputs" in result.output or "Downloaded" in result.output

        # Verify the output file was downloaded with expected content
        assert result_file.exists(), f"Expected output file at {result_file}"
        content = result_file.read_text()
        assert "Input file contents:" in content
        assert "Hello from integ test attachment" in content
    finally:
        # Clean up downloaded output
        if output_dir.exists():
            shutil.rmtree(output_dir)
