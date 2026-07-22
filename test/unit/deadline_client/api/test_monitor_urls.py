# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for deadline.client.api._monitor_urls (build_monitor_url and helpers).
"""

from unittest.mock import MagicMock, patch

import pytest
from botocore.client import BaseClient
from botocore.exceptions import ClientError

from deadline.client import api, config
from deadline.client.api import build_monitor_url
from deadline.client.api._monitor_urls import (
    _get_job_monitor_url,
    _get_monitor_subdomain,
)
from deadline.client.api._session import AwsCredentialsSource

SUBDOMAIN = "iadproductionsandbox"
REGION = "us-east-1"
FARM_ID = "farm-48235bbdf9a8424bbad7e26c6170b074"
QUEUE_ID = "queue-4847e932013148628fbe9ecb7e29a99c"
JOB_ID = "job-4d8a9716356340bba8f825745f52d05c"
STEP_ID = "step-953199a0081448a690c1b039453e11b7"
TASK_ID = "task-953199a0081448a690c1b039453e11b7-2"

HOST = f"https://{SUBDOMAIN}.{REGION}.deadlinecloud.amazonaws.com"


def test_build_monitor_url_all_farms():
    """With no farm, the URL points at the (non-region-scoped) all-farms list."""
    assert build_monitor_url(SUBDOMAIN, REGION) == f"{HOST}/farms"


def test_build_monitor_url_farm():
    assert (
        build_monitor_url(SUBDOMAIN, REGION, farm_id=FARM_ID) == f"{HOST}/{REGION}/farms/{FARM_ID}"
    )


def test_build_monitor_url_queue():
    assert (
        build_monitor_url(SUBDOMAIN, REGION, farm_id=FARM_ID, queue_id=QUEUE_ID)
        == f"{HOST}/{REGION}/farms/{FARM_ID}/queues/{QUEUE_ID}"
    )


def test_build_monitor_url_job():
    assert (
        build_monitor_url(SUBDOMAIN, REGION, farm_id=FARM_ID, queue_id=QUEUE_ID, job_id=JOB_ID)
        == f"{HOST}/{REGION}/farms/{FARM_ID}/queues/{QUEUE_ID}?jobId={JOB_ID}"
    )


def test_build_monitor_url_step():
    assert (
        build_monitor_url(
            SUBDOMAIN, REGION, farm_id=FARM_ID, queue_id=QUEUE_ID, job_id=JOB_ID, step_id=STEP_ID
        )
        == f"{HOST}/{REGION}/farms/{FARM_ID}/queues/{QUEUE_ID}?jobId={JOB_ID}&stepId={STEP_ID}"
    )


def test_build_monitor_url_task():
    """The full task URL matches the example from the feature request."""
    assert build_monitor_url(
        SUBDOMAIN,
        REGION,
        farm_id=FARM_ID,
        queue_id=QUEUE_ID,
        job_id=JOB_ID,
        step_id=STEP_ID,
        task_id=TASK_ID,
    ) == (
        f"{HOST}/{REGION}/farms/{FARM_ID}/queues/{QUEUE_ID}"
        f"?jobId={JOB_ID}&stepId={STEP_ID}&taskId={TASK_ID}"
    )


def test_build_monitor_url_requires_subdomain():
    with pytest.raises(ValueError, match="subdomain"):
        build_monitor_url("", REGION, farm_id=FARM_ID)


@pytest.mark.parametrize(
    "kwargs, missing",
    [
        (dict(queue_id=QUEUE_ID), "queue_id requires farm_id"),
        (dict(farm_id=FARM_ID, job_id=JOB_ID), "job_id requires queue_id"),
        (dict(farm_id=FARM_ID, queue_id=QUEUE_ID, step_id=STEP_ID), "step_id requires job_id"),
        (
            dict(farm_id=FARM_ID, queue_id=QUEUE_ID, job_id=JOB_ID, task_id=TASK_ID),
            "task_id requires step_id",
        ),
    ],
)
def test_build_monitor_url_hierarchy_violations(kwargs, missing):
    """A lower-level id without its parent is a ValueError (e.g. queue with no farm)."""
    with pytest.raises(ValueError, match=missing):
        build_monitor_url(SUBDOMAIN, REGION, **kwargs)


def test_build_monitor_url_region_defaults_from_config(fresh_deadline_config):
    """When region is omitted it resolves from defaults.farm_region, like elsewhere."""
    config.set_setting("defaults.farm_region", "eu-west-1")
    url = build_monitor_url(SUBDOMAIN, farm_id=FARM_ID)
    assert (
        url
        == f"https://{SUBDOMAIN}.eu-west-1.deadlinecloud.amazonaws.com/eu-west-1/farms/{FARM_ID}"
    )


def test_build_monitor_url_requires_region(fresh_deadline_config):
    """With no explicit region and nothing configured, we raise rather than emit a bad URL."""
    with pytest.raises(ValueError, match="region is required"):
        build_monitor_url(SUBDOMAIN, farm_id=FARM_ID)


def test_build_monitor_url_url_encodes_query_values():
    """Query parameter values are URL-encoded."""
    url = build_monitor_url(
        SUBDOMAIN,
        REGION,
        farm_id=FARM_ID,
        queue_id=QUEUE_ID,
        job_id="job with spaces&x=1",
    )
    assert "jobId=job+with+spaces%26x%3D1" in url


def test_build_monitor_url_cross_region_host_and_path_differ():
    """A monitor in one region linking to a farm in another: host uses the
    monitor region, path uses the farm region."""
    url = build_monitor_url(
        SUBDOMAIN,
        "eu-west-1",  # farm/resource region -> path
        farm_id=FARM_ID,
        queue_id=QUEUE_ID,
        monitor_region="us-east-1",  # monitor region -> host
    )
    assert url == (
        f"https://{SUBDOMAIN}.us-east-1.deadlinecloud.amazonaws.com"
        f"/eu-west-1/farms/{FARM_ID}/queues/{QUEUE_ID}"
    )


def test_build_monitor_url_monitor_region_defaults_to_region():
    """When monitor_region is omitted, the host uses the resource region."""
    assert build_monitor_url(SUBDOMAIN, REGION, farm_id=FARM_ID) == (
        f"https://{SUBDOMAIN}.{REGION}.deadlinecloud.amazonaws.com/{REGION}/farms/{FARM_ID}"
    )


def test_build_monitor_url_explicit_host_used_verbatim():
    """An explicit host is used as-is (with any trailing slash stripped), rather than
    reconstructed from the commercial domain -- required for non-commercial partitions
    (aws-us-gov, aws-cn) whose monitor hosts differ."""
    gov_host = f"https://{SUBDOMAIN}.us-gov-west-1.deadlinecloud.amazonaws-us-gov.com"
    assert build_monitor_url(
        SUBDOMAIN, "us-gov-west-1", farm_id=FARM_ID, queue_id=QUEUE_ID, host=gov_host + "/"
    ) == (f"{gov_host}/us-gov-west-1/farms/{FARM_ID}/queues/{QUEUE_ID}")


def test_build_monitor_url_explicit_host_overrides_monitor_region():
    """When host is supplied, monitor_region is ignored for the host."""
    host = f"https://{SUBDOMAIN}.cn-north-1.deadlinecloud.amazonaws.com.cn"
    assert build_monitor_url(
        SUBDOMAIN, "cn-north-1", farm_id=FARM_ID, monitor_region="us-east-1", host=host
    ) == (f"{host}/cn-north-1/farms/{FARM_ID}")


@pytest.mark.parametrize(
    "url, expected",
    [
        (
            f"https://{SUBDOMAIN}.us-east-1.deadlinecloud.amazonaws.com",
            (SUBDOMAIN, "us-east-1"),
        ),
        # Trailing path is ignored.
        (
            f"https://{SUBDOMAIN}.eu-west-1.deadlinecloud.amazonaws.com/eu-west-1/farms",
            (SUBDOMAIN, "eu-west-1"),
        ),
        (None, (None, None)),
        ("", (None, None)),
        ("https://example.com", (None, None)),
        # Missing region label.
        ("https://deadlinecloud.amazonaws.com", (None, None)),
    ],
)
def test_parse_monitor_host(url, expected):
    from deadline.client.api._monitor_urls import _parse_monitor_host

    assert _parse_monitor_host(url) == expected


def test_get_monitor_subdomain_returns_none_for_host_creds(fresh_deadline_config):
    """Host-provided credentials have no monitor, so there is no subdomain."""
    with patch.object(
        api._monitor_urls,
        "get_credentials_source",
        return_value=AwsCredentialsSource.HOST_PROVIDED,
    ):
        assert _get_monitor_subdomain() == (None, None, None)


def test_get_monitor_subdomain_calls_get_monitor(fresh_deadline_config):
    """For DCM credentials, the subdomain, region, and host come from GetMonitor."""
    mock_client = MagicMock()
    mock_client.get_monitor.return_value = {
        "subdomain": SUBDOMAIN,
        "monitorId": "monitor-abc",
        "url": f"https://{SUBDOMAIN}.us-east-1.deadlinecloud.amazonaws.com",
    }
    with (
        patch.object(
            api._monitor_urls,
            "get_credentials_source",
            return_value=AwsCredentialsSource.DEADLINE_CLOUD_MONITOR_LOGIN,
        ),
        patch.object(api._monitor_urls, "get_monitor_id", return_value="monitor-abc"),
        patch.object(api._monitor_urls, "_get_monitor_client", return_value=mock_client),
    ):
        assert _get_monitor_subdomain() == (
            SUBDOMAIN,
            "us-east-1",
            f"https://{SUBDOMAIN}.us-east-1.deadlinecloud.amazonaws.com",
        )
    mock_client.get_monitor.assert_called_once_with(monitorId="monitor-abc")


def _patch_dcm_monitor(mock_client):
    """Patch the module so credentials look like a Deadline Cloud monitor login whose
    GetMonitor call is served by ``mock_client``."""
    return patch.multiple(
        api._monitor_urls,
        get_credentials_source=MagicMock(
            return_value=AwsCredentialsSource.DEADLINE_CLOUD_MONITOR_LOGIN
        ),
        get_monitor_id=MagicMock(return_value="monitor-abc"),
        _get_monitor_client=MagicMock(return_value=mock_client),
    )


def test_get_job_monitor_url_happy_path(fresh_deadline_config):
    config.set_setting("defaults.farm_region", REGION)
    mock_client = MagicMock()
    mock_client.get_monitor.return_value = {
        "subdomain": SUBDOMAIN,
        "url": HOST,
    }
    with _patch_dcm_monitor(mock_client):
        url = _get_job_monitor_url(farm_id=FARM_ID, queue_id=QUEUE_ID, job_id=JOB_ID)
    assert url == f"{HOST}/{REGION}/farms/{FARM_ID}/queues/{QUEUE_ID}?jobId={JOB_ID}"


def test_get_job_monitor_url_cross_region(fresh_deadline_config):
    """Monitor in us-east-1, farm in eu-west-1: host and path regions differ."""
    config.set_setting("defaults.farm_region", "eu-west-1")
    mock_client = MagicMock()
    mock_client.get_monitor.return_value = {
        "subdomain": SUBDOMAIN,
        "url": f"https://{SUBDOMAIN}.us-east-1.deadlinecloud.amazonaws.com",
    }
    with _patch_dcm_monitor(mock_client):
        url = _get_job_monitor_url(farm_id=FARM_ID, queue_id=QUEUE_ID, job_id=JOB_ID)
    assert url == (
        f"https://{SUBDOMAIN}.us-east-1.deadlinecloud.amazonaws.com"
        f"/eu-west-1/farms/{FARM_ID}/queues/{QUEUE_ID}?jobId={JOB_ID}"
    )


def test_get_job_monitor_url_none_without_monitor(fresh_deadline_config):
    """Non-monitor credentials yield None (no URL surfaced)."""
    with patch.object(
        api._monitor_urls,
        "get_credentials_source",
        return_value=AwsCredentialsSource.HOST_PROVIDED,
    ):
        assert _get_job_monitor_url(farm_id=FARM_ID, queue_id=QUEUE_ID, job_id=JOB_ID) is None


def test_get_job_monitor_url_none_when_get_monitor_fails(fresh_deadline_config):
    """If deadline:GetMonitor raises, we omit the URL rather than propagate."""
    config.set_setting("defaults.farm_region", REGION)
    mock_client = MagicMock()
    mock_client.get_monitor.side_effect = RuntimeError("boom")
    with _patch_dcm_monitor(mock_client):
        assert _get_job_monitor_url(farm_id=FARM_ID, queue_id=QUEUE_ID, job_id=JOB_ID) is None


def test_get_job_monitor_url_none_when_no_subdomain(fresh_deadline_config):
    """If GetMonitor returns no subdomain, we omit the URL."""
    config.set_setting("defaults.farm_region", REGION)
    mock_client = MagicMock()
    mock_client.get_monitor.return_value = {"monitorId": "monitor-abc"}  # no subdomain
    with _patch_dcm_monitor(mock_client):
        assert _get_job_monitor_url(farm_id=FARM_ID, queue_id=QUEUE_ID, job_id=JOB_ID) is None


def test_get_job_monitor_url_uses_authoritative_host_in_other_partitions(fresh_deadline_config):
    """In aws-us-gov/aws-cn the monitor host does not end in the commercial domain, so
    the URL must use the authoritative host from GetMonitor rather than reconstruct it."""
    config.set_setting("defaults.farm_region", "us-gov-west-1")
    gov_host = f"https://{SUBDOMAIN}.us-gov-west-1.deadlinecloud.amazonaws-us-gov.com"
    mock_client = MagicMock()
    mock_client.get_monitor.return_value = {"subdomain": SUBDOMAIN, "url": gov_host}
    with _patch_dcm_monitor(mock_client):
        url = _get_job_monitor_url(farm_id=FARM_ID, queue_id=QUEUE_ID, job_id=JOB_ID)
    assert url == f"{gov_host}/us-gov-west-1/farms/{FARM_ID}/queues/{QUEUE_ID}?jobId={JOB_ID}"


def _get_monitor_wire_stub(monitor_region: str, monitor: dict):
    """
    Patch the boto wire so ``deadline:GetMonitor`` behaves like the real service:
    it succeeds only when called through a client scoped to the monitor's home
    region, and raises ``ResourceNotFoundException`` from any other regional
    endpoint (a monitor is a regional resource).
    """

    def _make_api_call(self, operation_name, api_params):
        assert operation_name == "GetMonitor"
        if self.meta.region_name != monitor_region:
            raise ClientError(
                {
                    "Error": {
                        "Code": "ResourceNotFoundException",
                        "Message": f"Monitor not found in {self.meta.region_name}",
                    }
                },
                operation_name,
            )
        return monitor

    return patch.object(BaseClient, "_make_api_call", _make_api_call)


def test_get_monitor_subdomain_uses_profile_region_not_farm_region(
    fresh_deadline_config, aws_config
):
    """The GetMonitor call must go to the monitor's home region -- the region of the
    AWS profile that Deadline Cloud monitor wrote -- not defaults.farm_region. In a
    cross-region setup a farm-region client hits the wrong endpoint and finds no
    monitor."""
    aws_config.write_text("[default]\nregion = us-east-1\n")
    config.set_setting("defaults.farm_region", "eu-west-1")
    with (
        _get_monitor_wire_stub("us-east-1", {"subdomain": SUBDOMAIN, "url": HOST}),
        patch.object(
            api._monitor_urls,
            "get_credentials_source",
            return_value=AwsCredentialsSource.DEADLINE_CLOUD_MONITOR_LOGIN,
        ),
        patch.object(api._monitor_urls, "get_monitor_id", return_value="monitor-abc"),
    ):
        assert _get_monitor_subdomain() == (SUBDOMAIN, REGION, HOST)


def test_get_job_monitor_url_cross_region_without_injected_client(
    fresh_deadline_config, aws_config
):
    """End-to-end cross-region: monitor (and profile) in us-east-1, farm in eu-west-1.
    The URL must still be produced -- GetMonitor must not be attempted in the farm's
    region, where it would raise ResourceNotFoundException and silently yield None."""
    aws_config.write_text("[default]\nregion = us-east-1\n")
    config.set_setting("defaults.farm_region", "eu-west-1")
    with (
        _get_monitor_wire_stub("us-east-1", {"subdomain": SUBDOMAIN, "url": HOST}),
        patch.object(
            api._monitor_urls,
            "get_credentials_source",
            return_value=AwsCredentialsSource.DEADLINE_CLOUD_MONITOR_LOGIN,
        ),
        patch.object(api._monitor_urls, "get_monitor_id", return_value="monitor-abc"),
    ):
        url = _get_job_monitor_url(farm_id=FARM_ID, queue_id=QUEUE_ID, job_id=JOB_ID)
    assert url == (f"{HOST}/eu-west-1/farms/{FARM_ID}/queues/{QUEUE_ID}?jobId={JOB_ID}")


def test_get_job_monitor_url_falls_back_when_monitor_url_malformed(fresh_deadline_config):
    """A subdomain with a missing/garbled monitor url still yields a URL, using the
    farm region for the host (monitor_region falls back to the resource region)."""
    config.set_setting("defaults.farm_region", REGION)
    mock_client = MagicMock()
    mock_client.get_monitor.return_value = {"subdomain": SUBDOMAIN, "url": "not-a-url"}
    with _patch_dcm_monitor(mock_client):
        url = _get_job_monitor_url(farm_id=FARM_ID, queue_id=QUEUE_ID, job_id=JOB_ID)
    assert url == f"{HOST}/{REGION}/farms/{FARM_ID}/queues/{QUEUE_ID}?jobId={JOB_ID}"
