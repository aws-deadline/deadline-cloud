# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for deadline._mcp.tools.job.submit_job, focused on the monitor URL in the
response.
"""

from unittest.mock import patch

from deadline.client import config
from deadline._mcp.tools import job as job_tool

FARM_ID = "farm-1234"
QUEUE_ID = "queue-5678"
JOB_ID = "job-9abc"
JOB_URL = "https://mymonitor.us-east-1.deadlinecloud.amazonaws.com/us-east-1/farms/farm-1234/queues/queue-5678?jobId=job-9abc"


def _run_submit(tmp_path, job_url):
    # submit_job reads/writes a literal "[defaults]" config section. Seed it as a
    # configured workstation would: setting aws_profile_name (which has no
    # profile-scoping dependency) materializes that section in the isolated config.
    config.set_setting("defaults.aws_profile_name", "(default)")

    bundle_dir = str(tmp_path)
    with (
        patch.object(job_tool, "create_job_from_job_bundle", return_value=JOB_ID),
        patch.object(job_tool, "_get_job_monitor_url", return_value=job_url) as mock_url,
    ):
        result = job_tool.submit_job(
            job_bundle_dir=bundle_dir,
            farm_id=FARM_ID,
            queue_id=QUEUE_ID,
        )
    return result, mock_url


def test_submit_job_includes_job_url(fresh_deadline_config, tmp_path):
    """When a monitor URL is available it is returned as job_url."""
    result, mock_url = _run_submit(tmp_path, JOB_URL)

    assert result["status"] == "success"
    assert result["job_id"] == JOB_ID
    assert result["job_url"] == JOB_URL
    # URL helper is called with the resolved farm/queue/job.
    _, kwargs = mock_url.call_args
    assert kwargs["farm_id"] == FARM_ID
    assert kwargs["queue_id"] == QUEUE_ID
    assert kwargs["job_id"] == JOB_ID


def test_submit_job_job_url_none_when_not_monitor(fresh_deadline_config, tmp_path):
    """Non-monitor credentials -> job_url is present but None."""
    result, _ = _run_submit(tmp_path, None)

    assert result["status"] == "success"
    assert result["job_id"] == JOB_ID
    assert result["job_url"] is None
