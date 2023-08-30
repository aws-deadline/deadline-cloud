# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
import uuid

from unittest.mock import patch, MagicMock
from dataclasses import asdict
from urllib import request

from deadline.client import api, config
from deadline.client.api._telemetry import TelemetryClient, TelemetryEvent
from deadline.job_attachments.progress_tracker import SummaryStatistics


@pytest.fixture(scope="function", name="telemetry_client")
def fixture_telemetry_client(fresh_deadline_config):
    config.set_setting("defaults.aws_profile_name", "SomeRandomProfileName")
    with patch.object(api.TelemetryClient, "_start_threads"), patch.object(
        api._telemetry, "get_studio_id", side_effect=["studio-id"]
    ), patch.object(
        api._telemetry,
        "get_user_and_identity_store_id",
        side_effect=[("user-id", "identity-store-id")],
    ):
        return TelemetryClient(config=config.config_file.read_config())


def test_opt_out(fresh_deadline_config):
    """Ensures the telemetry client doesn't fully initialize if the opt out config setting is set"""
    # GIVEN
    config.set_setting("defaults.aws_profile_name", "SomeRandomProfileName")
    config.set_setting("telemetry.opt_out", "true")
    # WHEN
    client = TelemetryClient(config=config.config_file.read_config())
    # THEN
    assert not hasattr(client, "endpoint")
    assert not hasattr(client, "session_id")
    assert not hasattr(client, "telemetry_id")
    assert not hasattr(client, "studio_id")
    assert not hasattr(client, "user_id")
    assert not hasattr(client, "env_info")
    assert not hasattr(client, "event_queue")
    assert not hasattr(client, "processing_thread")
    # Ensure nothing blows up if we try recording telemetry after we've opted out
    client.record_hashing_summary(SummaryStatistics(), True)
    client.record_upload_summary(SummaryStatistics(), False)


def test_get_telemetry_identifier(telemetry_client):
    """Ensures that getting the local-user-id handles empty/malformed strings"""
    # Confirm that we generate a new UUID if the setting doesn't exist, and write to config
    uuid.UUID(telemetry_client.telemetry_id, version=4)  # Should not raise ValueError
    assert config.get_setting("telemetry.identifier") == telemetry_client.telemetry_id

    # Confirm we generate a new UUID if the local_user_id is not a valid UUID
    config.set_setting("telemetry.identifier", "bad-id")
    telemetry_id = telemetry_client._get_telemetry_identifier()
    assert telemetry_id != "bad-id"
    uuid.UUID(telemetry_id, version=4)  # Should not raise ValueError

    # Confirm the new user id was saved and is retrieved properly
    assert config.get_setting("telemetry.identifier") == telemetry_id
    assert telemetry_client._get_telemetry_identifier() == telemetry_id


@pytest.mark.timeout(5)  # Timeout in case we don't exit the while loop
def test_process_event_queue_thread(telemetry_client):
    """Test that the queue processing thread function exits cleanly after getting None"""
    # GIVEN
    queue_mock = MagicMock()
    queue_mock.get.side_effect = [TelemetryEvent(), None]
    telemetry_client.event_queue = queue_mock
    # WHEN
    with patch.object(request, "urlopen") as urlopen_mock:
        telemetry_client._process_event_queue_thread()
        urlopen_mock.assert_called_once()
    # THEN
    assert queue_mock.get.call_count == 2


@pytest.mark.timeout(5)  # Timeout in case we don't exit the while loop
def test_process_event_queue_thread_handles_errors(telemetry_client):
    """Test that the thread continues after getting exceptions"""
    # GIVEN
    queue_mock = MagicMock()
    queue_mock.get.side_effect = [TelemetryEvent(), None]
    telemetry_client.event_queue = queue_mock
    # WHEN
    with patch.object(request, "urlopen", side_effect=Exception("Some error")) as urlopen_mock:
        telemetry_client._process_event_queue_thread()
        urlopen_mock.assert_called_once()
    # THEN
    assert queue_mock.get.call_count == 2


def test_record_hashing_summary(telemetry_client):
    """Tests that recording a hashing summary sends the expected TelemetryEvent to the thread queue"""
    # GIVEN
    queue_mock = MagicMock()
    expected_env_info = {"test_env": "test_val"}
    expected_machine_info = {"test_machine": "test_val2"}
    test_summary = SummaryStatistics(total_bytes=123, total_files=12, total_time=12345)

    expected_summary = asdict(test_summary)
    expected_summary["usageMode"] = "CLI"
    expected_summary["userId"] = "user-id"
    expected_summary["studioId"] = "studio-id"
    expected_summary.update(expected_env_info)
    expected_summary.update(expected_machine_info)

    expected_event = TelemetryEvent(
        event_type="com.amazon.rum.job_attachments.hashing_summary", event_body=expected_summary
    )

    telemetry_client.event_queue = queue_mock
    telemetry_client.env_info = expected_env_info
    telemetry_client.system_info = expected_machine_info

    # WHEN
    telemetry_client.record_hashing_summary(test_summary)

    # THEN
    queue_mock.put_nowait.assert_called_once_with(expected_event)


def test_record_upload_summary(telemetry_client):
    """Tests that recording an upload summary sends the expected TelemetryEvent to the thread queue"""
    # GIVEN
    queue_mock = MagicMock()
    expected_env_info = {"test_env": "test_val"}
    expected_machine_info = {"test_machine": "test_val2"}
    test_summary = SummaryStatistics(total_bytes=123, total_files=12, total_time=12345)

    expected_summary = asdict(test_summary)
    expected_summary["usageMode"] = "GUI"
    expected_summary["userId"] = "user-id"
    expected_summary["studioId"] = "studio-id"
    expected_summary.update(expected_env_info)
    expected_summary.update(expected_machine_info)

    expected_event = TelemetryEvent(
        event_type="com.amazon.rum.job_attachments.upload_summary", event_body=expected_summary
    )

    telemetry_client.event_queue = queue_mock
    telemetry_client.env_info = expected_env_info
    telemetry_client.system_info = expected_machine_info

    # WHEN
    telemetry_client.record_upload_summary(test_summary, from_gui=True)

    # THEN
    queue_mock.put_nowait.assert_called_once_with(expected_event)
