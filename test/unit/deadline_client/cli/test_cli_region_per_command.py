# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Per-command coverage for the ``--region`` CLI flag across the command groups.

The commands fall into two client-construction families, which determines the
assertion each test makes:

  * ``get_boto3_client``-based commands resolve their region from config. ``--region``
    is persisted to ``defaults.farm_region`` by ``_apply_cli_options_to_config`` and then
    ``api.get_boto3_client`` -> ``api._session.get_session_client`` builds a region-scoped
    deadline client. These are exercised in a parametrized table that mocks
    ``api._session.get_session_client`` and asserts ``region="..."`` + ``service_name="deadline"``.
  * Session-based commands (``queue sync-output``, ``manifest download``/``upload``,
    ``trace-schedule``) build the deadline client off an explicit boto3 session; each gets a
    dedicated test asserting the region reaches the session/get_session_client call.

Every test also asserts ``defaults.farm_region`` is persisted (the documented current
behavior, including on read-only commands - see ``test_cli_region_persists_farm_region``).
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from deadline.client import api, config
from deadline.client.cli import main
from deadline.client.cli._groups import queue_group, manifest_group

from ..shared_constants import (
    MOCK_FARM_ID,
    MOCK_QUEUE_ID,
    MOCK_FLEET_ID,
    MOCK_WORKER_ID,
    MOCK_JOB_ID,
    MOCK_GET_QUEUE_RESPONSE,
)

MOCK_REGION = "eu-central-1"


def _region_in_calls(get_session_client_mock, region):
    """True if any get_session_client call built a deadline client for ``region``."""
    return any(
        call.kwargs.get("region") == region and call.kwargs.get("service_name") == "deadline"
        for call in get_session_client_mock.call_args_list
    )


# ---------------------------------------------------------------------------
# A. Homogeneous get_boto3_client-based commands (cases 1, 2, 3, 4, 5, 7, 8,
#    9, 10, 11, 12, 13, 15, 16). Each entry drives a command end-to-end and the
#    test asserts a region-scoped deadline client was built and farm_region persisted.
# ---------------------------------------------------------------------------


def _setup_get_session_client(get_session_client_mock, build_command):
    """Wire a fresh region-scoped client mock, configure its returns, and return the CLI args."""
    client = MagicMock()
    command = build_command(client)
    get_session_client_mock.return_value = client
    return command


def _cmd_farm_get(client):
    client.get_farm.return_value = {"farmId": MOCK_FARM_ID, "displayName": "F"}
    return ["farm", "get", "--farm-id", MOCK_FARM_ID, "--region", MOCK_REGION]


def _cmd_queue_list(client):
    client.list_queues.return_value = {"queues": []}
    return ["queue", "list", "--farm-id", MOCK_FARM_ID, "--region", MOCK_REGION]


def _cmd_queue_get(client):
    client.get_queue.return_value = dict(MOCK_GET_QUEUE_RESPONSE)
    return [
        "queue",
        "get",
        "--farm-id",
        MOCK_FARM_ID,
        "--queue-id",
        MOCK_QUEUE_ID,
        "--region",
        MOCK_REGION,
    ]


def _cmd_queue_paramdefs(client):
    # get_queue_parameter_definitions lists queue environments off the deadline client.
    client.list_queue_environments.return_value = {"environments": []}
    return [
        "queue",
        "paramdefs",
        "--farm-id",
        MOCK_FARM_ID,
        "--queue-id",
        MOCK_QUEUE_ID,
        "--region",
        MOCK_REGION,
    ]


def _cmd_queue_export_credentials(client):
    client.assume_queue_role_for_user.return_value = {
        "credentials": {
            "accessKeyId": "AKIA",
            "secretAccessKey": "secret",
            "sessionToken": "token",
            "expiration": datetime.fromisoformat("2125-01-01T00:00:00+00:00"),
        }
    }
    return [
        "queue",
        "export-credentials",
        "--farm-id",
        MOCK_FARM_ID,
        "--queue-id",
        MOCK_QUEUE_ID,
        "--region",
        MOCK_REGION,
    ]


def _cmd_fleet_get(client):
    client.get_fleet.return_value = {"fleetId": MOCK_FLEET_ID, "farmId": MOCK_FARM_ID}
    return [
        "fleet",
        "get",
        "--farm-id",
        MOCK_FARM_ID,
        "--fleet-id",
        MOCK_FLEET_ID,
        "--region",
        MOCK_REGION,
    ]


def _cmd_job_list(client):
    client.search_jobs.return_value = {"jobs": [], "totalResults": 0}
    return [
        "job",
        "list",
        "--farm-id",
        MOCK_FARM_ID,
        "--queue-id",
        MOCK_QUEUE_ID,
        "--region",
        MOCK_REGION,
    ]


def _cmd_job_get(client):
    client.get_job.return_value = {
        "jobId": MOCK_JOB_ID,
        "name": "Test Job",
        "lifecycleStatus": "CREATE_COMPLETE",
        "lifecycleStatusMessage": "",
        "taskRunStatus": "SUCCEEDED",
        "taskRunStatusCounts": {"SUCCEEDED": 1},
        "createdAt": datetime.fromisoformat("2024-01-01T00:00:00+00:00"),
        "createdBy": "user",
        "priority": 50,
    }
    return [
        "job",
        "get",
        "--farm-id",
        MOCK_FARM_ID,
        "--queue-id",
        MOCK_QUEUE_ID,
        "--job-id",
        MOCK_JOB_ID,
        "--region",
        MOCK_REGION,
    ]


def _cmd_job_cancel(client):
    client.get_job.return_value = {
        "name": "Test Job",
        "jobId": MOCK_JOB_ID,
        "taskRunStatus": "RUNNING",
        "taskRunStatusCounts": {"RUNNING": 1},
        "createdAt": datetime.fromisoformat("2024-01-01T00:00:00+00:00"),
        "createdBy": "user",
    }
    client.update_job.return_value = {}
    return [
        "job",
        "cancel",
        "--farm-id",
        MOCK_FARM_ID,
        "--queue-id",
        MOCK_QUEUE_ID,
        "--job-id",
        MOCK_JOB_ID,
        "--region",
        MOCK_REGION,
        "--yes",
    ]


def _cmd_worker_list(client):
    client.search_workers.return_value = {"workers": [], "totalResults": 0}
    return [
        "worker",
        "list",
        "--farm-id",
        MOCK_FARM_ID,
        "--fleet-id",
        MOCK_FLEET_ID,
        "--region",
        MOCK_REGION,
    ]


def _cmd_worker_get(client):
    client.get_worker.return_value = {"workerId": MOCK_WORKER_ID, "status": "STARTED"}
    return [
        "worker",
        "get",
        "--farm-id",
        MOCK_FARM_ID,
        "--fleet-id",
        MOCK_FLEET_ID,
        "--worker-id",
        MOCK_WORKER_ID,
        "--region",
        MOCK_REGION,
    ]


_GET_BOTO3_CLIENT_COMMANDS = [
    pytest.param(_cmd_farm_get, id="farm-get"),
    pytest.param(_cmd_queue_list, id="queue-list"),
    pytest.param(_cmd_queue_get, id="queue-get"),
    pytest.param(_cmd_queue_paramdefs, id="queue-paramdefs"),
    pytest.param(_cmd_queue_export_credentials, id="queue-export-credentials"),
    pytest.param(_cmd_fleet_get, id="fleet-get"),
    pytest.param(_cmd_job_list, id="job-list"),
    pytest.param(_cmd_job_get, id="job-get"),
    pytest.param(_cmd_job_cancel, id="job-cancel"),
    pytest.param(_cmd_worker_list, id="worker-list"),
    pytest.param(_cmd_worker_get, id="worker-get"),
]


@pytest.mark.parametrize("build_command", _GET_BOTO3_CLIENT_COMMANDS)
def test_cli_region_reaches_get_session_client(
    fresh_deadline_config, mock_telemetry, build_command
):
    """--region scopes the deadline client to that region and persists defaults.farm_region."""
    with (
        patch.object(api._session, "get_boto3_session"),
        patch.object(api._session, "get_session_client") as get_session_client_mock,
    ):
        command = _setup_get_session_client(get_session_client_mock, build_command)
        runner = CliRunner()
        result = runner.invoke(main, command)

    assert result.exit_code == 0, result.output
    assert _region_in_calls(get_session_client_mock, MOCK_REGION)
    assert config.get_setting("defaults.farm_region") == MOCK_REGION


# ---------------------------------------------------------------------------
# job download-output (11) and download-input (12): these build the deadline
# client via get_boto3_client too, but exit early (no attachments / no output)
# before any download. We only need to confirm the region reached the client.
# ---------------------------------------------------------------------------


def test_cli_job_download_output_region(fresh_deadline_config, mock_telemetry):
    """job download-output builds a region-scoped deadline client and persists farm_region."""
    with (
        patch.object(api._session, "get_boto3_session"),
        patch.object(api._session, "get_session_client") as get_session_client_mock,
        patch.object(api, "get_queue_user_boto3_session"),
    ):
        client = MagicMock()
        client.get_job.return_value = {"name": "Test Job", "attachments": None}
        client.get_queue.return_value = dict(MOCK_GET_QUEUE_RESPONSE)
        # OutputDownloader is constructed off the queue session; force "no outputs" by
        # making the job have no attachments, which short-circuits before download.
        get_session_client_mock.return_value = client

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "download-output",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--job-id",
                MOCK_JOB_ID,
                "--region",
                MOCK_REGION,
                "--yes",
            ],
        )

    # The deadline client must have been built for the requested region regardless of how
    # far the download proceeds.
    assert _region_in_calls(get_session_client_mock, MOCK_REGION), result.output
    assert config.get_setting("defaults.farm_region") == MOCK_REGION


def test_cli_job_download_input_region(fresh_deadline_config, mock_telemetry):
    """job download-input builds a region-scoped deadline client and persists farm_region."""
    with (
        patch.object(api._session, "get_boto3_session"),
        patch.object(api._session, "get_session_client") as get_session_client_mock,
        patch.object(api, "get_queue_user_boto3_session"),
    ):
        client = MagicMock()
        # No attachments => command prints "No input attachments" and returns cleanly.
        client.get_job.return_value = {"name": "Test Job", "attachments": None}
        client.get_queue.return_value = dict(MOCK_GET_QUEUE_RESPONSE)
        get_session_client_mock.return_value = client

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "download-input",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--job-id",
                MOCK_JOB_ID,
                "--region",
                MOCK_REGION,
                "--yes",
            ],
        )

    assert result.exit_code == 0, result.output
    assert _region_in_calls(get_session_client_mock, MOCK_REGION)
    assert config.get_setting("defaults.farm_region") == MOCK_REGION


# ---------------------------------------------------------------------------
# job wait (13): builds a region-scoped deadline client for the initial get_job,
# and api.wait_for_job_completion receives the same config (so it resolves the
# same region). We mock wait_for_job_completion to a terminal SUCCEEDED.
# ---------------------------------------------------------------------------


def test_cli_job_wait_region(fresh_deadline_config, mock_telemetry):
    """job wait builds a region-scoped deadline client and persists farm_region."""
    wait_result = MagicMock()
    wait_result.status = "SUCCEEDED"
    wait_result.elapsed_time = 1.0
    wait_result.failed_tasks = []

    with (
        patch.object(api._session, "get_boto3_session"),
        patch.object(api._session, "get_session_client") as get_session_client_mock,
        patch.object(api, "wait_for_job_completion", return_value=wait_result),
    ):
        client = MagicMock()
        client.get_job.return_value = {"name": "Test Job"}
        get_session_client_mock.return_value = client

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "wait",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--job-id",
                MOCK_JOB_ID,
                "--region",
                MOCK_REGION,
            ],
        )

    # A SUCCEEDED job exits 0.
    assert result.exit_code == 0, result.output
    assert _region_in_calls(get_session_client_mock, MOCK_REGION)
    assert config.get_setting("defaults.farm_region") == MOCK_REGION


# ---------------------------------------------------------------------------
# job logs (14): the deadline client AND the CloudWatch LOGS client must both be
# region-scoped to the farm region. For a non-DCM profile both go through
# get_boto3_client -> get_session_client. We assert both service clients were
# built for the requested region.
# ---------------------------------------------------------------------------


def test_cli_job_logs_region_scopes_deadline_and_logs_clients(fresh_deadline_config):
    """job logs scopes both the deadline client and the CloudWatch logs client to the region."""
    from deadline.client.api._job_monitoring import SessionLogResult

    log_result = SessionLogResult(
        events=[],
        count=0,
        next_token=None,
        log_group=f"/aws/deadline/{MOCK_FARM_ID}/{MOCK_QUEUE_ID}",
        log_stream="session-test-session",
    )

    with (
        patch.object(api._session, "get_boto3_session"),
        patch.object(api._session, "get_session_client") as get_session_client_mock,
        patch(
            "deadline.client.api._job_monitoring.get_user_and_identity_store_id",
            return_value=(None, None),
        ),
        patch("deadline.client.api.get_session_logs", return_value=log_result) as mock_get_logs,
    ):
        client = MagicMock()
        client.get_job.return_value = {"name": "Test Job"}
        get_session_client_mock.return_value = client

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "logs",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--job-id",
                MOCK_JOB_ID,
                "--session-id",
                "test-session",
                "--region",
                MOCK_REGION,
            ],
        )

    assert result.exit_code == 0, result.output
    # The CLI's own deadline client (for get_job) was built for the region.
    assert _region_in_calls(get_session_client_mock, MOCK_REGION)
    # get_session_logs receives the same config, so it resolves the same region for both
    # its internal deadline client and the CloudWatch logs client.
    assert mock_get_logs.called
    config_passed = mock_get_logs.call_args.kwargs["config"]
    from deadline.client.config import config_file

    assert config_file.get_setting("defaults.farm_region", config=config_passed) == MOCK_REGION


def test_job_logs_api_logs_client_region_scoped_non_dcm(fresh_deadline_config):
    """
    At the API layer, get_session_logs builds its CloudWatch logs client via
    get_boto3_client("logs", config=config) for a non-DCM profile, so the logs client is
    scoped to defaults.farm_region just like the deadline client.
    """
    from deadline.client.api import _job_monitoring

    config.set_setting("defaults.farm_region", MOCK_REGION)

    captured = []

    def fake_get_boto3_client(service_name, config=None, region=None):
        # get_boto3_client resolves region from config when region is None; emulate that.
        from deadline.client.api._session import _resolve_region

        resolved = _resolve_region(config=config, region=region)
        captured.append((service_name, resolved))
        client = MagicMock()
        client.get_log_events.return_value = {"events": [], "nextForwardToken": None}
        return client

    with (
        patch.object(_job_monitoring, "get_boto3_client", side_effect=fake_get_boto3_client),
        patch.object(_job_monitoring, "get_user_and_identity_store_id", return_value=(None, None)),
    ):
        _job_monitoring.get_session_logs(
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            session_id="session-abc",
            limit=10,
        )

    services = {svc: region for svc, region in captured}
    assert services.get("deadline") == MOCK_REGION
    # The logs client is region-scoped to the farm region (the documented multi-region fix).
    assert services.get("logs") == MOCK_REGION


# ---------------------------------------------------------------------------
# B. Session-based commands. These build the deadline client off an explicit
#    boto3 session (not api.get_boto3_client), so we assert the region reaches the
#    session/get_session_client construction.
# ---------------------------------------------------------------------------


def test_cli_queue_sync_output_region(fresh_deadline_config, tmp_path):
    """
    queue sync-output (6): builds the deadline client via get_session_client(session,
    "deadline", region=...) with the resolved farm region.
    """
    deadline_client = MagicMock()
    deadline_client.get_queue.return_value = dict(MOCK_GET_QUEUE_RESPONSE)

    with (
        patch.object(queue_group.api, "get_boto3_session", return_value=MagicMock()),
        patch.object(
            queue_group, "get_session_client", return_value=deadline_client
        ) as mock_get_session_client,
        patch.object(
            queue_group,
            "_incremental_output_download",
            return_value=(MagicMock(), MagicMock(), {}, {}),
        ),
    ):
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "queue",
                "sync-output",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--region",
                MOCK_REGION,
                "--checkpoint-dir",
                str(tmp_path),
                "--ignore-storage-profiles",
                "--dry-run",
            ],
        )

    assert result.exit_code == 0, result.output
    mock_get_session_client.assert_called_once()
    assert mock_get_session_client.call_args.kwargs["region"] == MOCK_REGION
    assert config.get_setting("defaults.farm_region") == MOCK_REGION


def test_cli_manifest_download_region(fresh_deadline_config, tmp_path):
    """
    manifest download (17): scopes its deadline client to the resolved farm region via
    boto3_session.client("deadline", ..., region_name=region).
    """
    download_dir = tmp_path / "dl"
    download_dir.mkdir()

    import dataclasses as _dc

    @_dc.dataclass
    class _DownloadOutput:
        downloaded = ""

    session_mock = MagicMock()
    deadline_client = session_mock.client.return_value
    deadline_client.get_queue.return_value = dict(MOCK_GET_QUEUE_RESPONSE)

    with (
        patch.object(manifest_group.api, "get_boto3_session", return_value=session_mock),
        patch.object(
            manifest_group, "_get_queue_user_boto3_session", return_value=MagicMock()
        ) as queue_session_mock,
        patch.object(manifest_group, "_manifest_download", return_value=_DownloadOutput()),
    ):
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "manifest",
                "download",
                str(download_dir),
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--job-id",
                MOCK_JOB_ID,
                "--region",
                MOCK_REGION,
            ],
        )

    assert result.exit_code == 0, result.output
    # The deadline client was created with region_name scoped to the requested region.
    assert any(
        call.kwargs.get("region_name") == MOCK_REGION for call in session_mock.client.call_args_list
    ), session_mock.client.call_args_list
    # The queue-user session must also be scoped to the farm's region, not the base session.
    queue_session_mock.assert_called_once()
    assert queue_session_mock.call_args.kwargs.get("region") == MOCK_REGION
    assert config.get_setting("defaults.farm_region") == MOCK_REGION


def test_cli_manifest_upload_region(fresh_deadline_config, tmp_path):
    """
    manifest upload (18): with --farm-id/--queue-id, builds its deadline client via
    api.get_boto3_client which resolves the persisted farm region.
    """
    manifest_file = tmp_path / "abc_manifest"
    manifest_file.write_text("{}")

    with (
        patch.object(manifest_group.api, "get_boto3_session", return_value=MagicMock()),
        patch.object(api._session, "get_session_client") as get_session_client_mock,
        patch.object(manifest_group.api, "get_queue_user_boto3_session", return_value=MagicMock()),
        patch.object(manifest_group, "_manifest_upload", return_value=None),
    ):
        deadline_client = MagicMock()
        deadline_client.get_queue.return_value = dict(MOCK_GET_QUEUE_RESPONSE)
        get_session_client_mock.return_value = deadline_client

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "manifest",
                "upload",
                str(manifest_file),
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--region",
                MOCK_REGION,
            ],
        )

    assert result.exit_code == 0, result.output
    assert _region_in_calls(get_session_client_mock, MOCK_REGION)
    assert config.get_setting("defaults.farm_region") == MOCK_REGION


def test_cli_trace_schedule_region(fresh_deadline_config, mock_telemetry):
    """
    job trace-schedule (19): builds its deadline client via api.get_boto3_client, scoped to
    the persisted farm region. A job that hasn't started exits early after the get_job call.
    """
    with (
        patch.object(api._session, "get_boto3_session"),
        patch.object(api._session, "get_session_client") as get_session_client_mock,
    ):
        client = MagicMock()
        # No startedAt => trace-schedule reports "hasn't started yet" and stops, which is
        # enough to confirm the region-scoped deadline client was built.
        client.get_job.return_value = {"name": "Test Job", "jobId": MOCK_JOB_ID}
        get_session_client_mock.return_value = client

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "trace-schedule",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--job-id",
                MOCK_JOB_ID,
                "--region",
                MOCK_REGION,
            ],
        )

    # "Job hasn't started yet" is surfaced as a DeadlineOperationError (exit 1), but the
    # deadline client was already built for the region by then.
    assert _region_in_calls(get_session_client_mock, MOCK_REGION), result.output
    assert config.get_setting("defaults.farm_region") == MOCK_REGION
