# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the bundle gui-submit response formatter (_print_response), focused on
the monitor URL (jobUrl / "Job URL:").
"""

import json

from deadline.client.cli._groups.bundle_group import _print_response

JOB_ID = "job-0123456789abcdefabcdefabcdefabcd"
JOB_URL = (
    "https://mymonitor.us-east-1.deadlinecloud.amazonaws.com/us-east-1/"
    "farms/farm-1234/queues/queue-5678?jobId=" + JOB_ID
)


def test_print_response_json_includes_job_url(capsys):
    _print_response(
        output="json",
        job_bundle_dir="/tmp/bundle",
        job_history_bundle_dir="/tmp/history",
        job_id=JOB_ID,
        job_url=JOB_URL,
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "SUBMITTED"
    assert payload["jobId"] == JOB_ID
    assert payload["jobUrl"] == JOB_URL


def test_print_response_json_omits_job_url_when_absent(capsys):
    _print_response(
        output="json",
        job_bundle_dir="/tmp/bundle",
        job_history_bundle_dir="/tmp/history",
        job_id=JOB_ID,
        job_url=None,
    )
    payload = json.loads(capsys.readouterr().out)
    assert "jobUrl" not in payload


def test_print_response_verbose_includes_job_url(capsys):
    _print_response(
        output="verbose",
        job_bundle_dir="/tmp/bundle",
        job_history_bundle_dir="/tmp/history",
        job_id=JOB_ID,
        job_url=JOB_URL,
    )
    out = capsys.readouterr().out
    assert f"Job URL: {JOB_URL}" in out


def test_print_response_verbose_omits_job_url_when_absent(capsys):
    _print_response(
        output="verbose",
        job_bundle_dir="/tmp/bundle",
        job_history_bundle_dir="/tmp/history",
        job_id=JOB_ID,
        job_url=None,
    )
    out = capsys.readouterr().out
    assert "Job URL:" not in out
