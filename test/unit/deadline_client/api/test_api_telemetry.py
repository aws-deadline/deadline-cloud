# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from typing import Any, Dict
import pytest
import uuid
import time

from unittest.mock import patch, MagicMock
from dataclasses import asdict
from urllib import request

from deadline.client import api, config
from deadline.client.api._telemetry import (
    TelemetryClient,
    TelemetryEvent,
    get_deadline_cloud_library_telemetry_client,
    record_success_fail_telemetry_event,
    record_function_latency_telemetry_event,
)
from deadline.job_attachments.progress_tracker import SummaryStatistics


@pytest.fixture(scope="function", name="mock_telemetry_client")
def fixture_telemetry_client(fresh_deadline_config):
    config.set_setting("defaults.aws_profile_name", "SomeRandomProfileName")
    with patch.object(api.TelemetryClient, "_start_threads"), patch.object(
        api._telemetry, "get_monitor_id", side_effect=["monitor-id"]
    ), patch.object(api._telemetry, "get_monitor_id", side_effect=[None]), patch.object(
        api._telemetry,
        "get_user_and_identity_store_id",
        side_effect=[("user-id", "identity-store-id")],
    ), patch.object(
        api._telemetry, "get_deadline_endpoint_url", side_effect=["https://fake-endpoint-url"]
    ):
        client = TelemetryClient(
            package_name="deadline-cloud-library",
            package_ver="0.1.2.1234",
            config=config.config_file.read_config(),
        )
        assert client.is_initialized
        return client


def test_opt_out_config(fresh_deadline_config):
    """Ensures the telemetry client doesn't fully initialize if the opt out config setting is set"""
    # GIVEN
    config.set_setting("defaults.aws_profile_name", "SomeRandomProfileName")
    config.set_setting("telemetry.opt_out", "true")
    # WHEN
    client = TelemetryClient(
        "deadline-cloud-library", "test-version", config=config.config_file.read_config()
    )
    # THEN
    assert not client.is_initialized
    assert not hasattr(client, "endpoint")
    assert not hasattr(client, "event_queue")
    assert not hasattr(client, "processing_thread")
    # Ensure nothing blows up if we try recording telemetry after we've opted out
    client.record_hashing_summary(SummaryStatistics(), from_gui=True)
    client.record_upload_summary(SummaryStatistics(), from_gui=False)
    client.record_error({}, str(type(Exception)))


@pytest.mark.parametrize(
    "env_var_value",
    [
        pytest.param("true"),
        pytest.param("1"),
        pytest.param("yes"),
        pytest.param("on"),
    ],
)
def test_opt_out_env_var(fresh_deadline_config, monkeypatch, env_var_value):
    """Ensures the telemetry client doesn't fully initialize if the opt out env var is set"""
    # GIVEN
    config.set_setting("defaults.aws_profile_name", "SomeRandomProfileName")
    monkeypatch.setenv("DEADLINE_CLOUD_TELEMETRY_OPT_OUT", env_var_value)
    config.set_setting(
        "telemetry.opt_out", "false"
    )  # Ensure we ignore the config file if env var is set
    # WHEN
    client = TelemetryClient(
        "deadline-cloud-library", "test-version", config=config.config_file.read_config()
    )
    # THEN
    assert not client.is_initialized
    assert not hasattr(client, "endpoint")
    assert not hasattr(client, "event_queue")
    assert not hasattr(client, "processing_thread")
    # Ensure nothing blows up if we try recording telemetry after we've opted out
    client.record_hashing_summary(SummaryStatistics(), from_gui=True)
    client.record_upload_summary(SummaryStatistics(), from_gui=False)
    client.record_error({}, str(type(Exception)))


def test_initialize_failure_then_success(fresh_deadline_config):
    """
    Tests that a failure in initializing set keeps the property as false, but trying again
    without an exception initializes everything successfully.
    """
    config.set_setting("defaults.aws_profile_name", "SomeRandomProfileName")
    with patch.object(api.TelemetryClient, "_start_threads"), patch.object(
        api._telemetry, "get_monitor_id", side_effect=["monitor-id"]
    ), patch.object(
        api._telemetry,
        "get_user_and_identity_store_id",
        side_effect=[("user-id", "identity-store-id")],
    ), patch.object(
        api._telemetry,
        "get_deadline_endpoint_url",
        side_effect=[Exception("Boto3 blew up!"), "https://fake-endpoint-url"],
    ):
        client = TelemetryClient(
            package_name="deadline-cloud-library",
            package_ver="0.1.2.1234",
            config=config.config_file.read_config(),
        )

        assert not client.is_initialized
        assert not hasattr(client, "endpoint")
        assert not hasattr(client, "event_queue")
        assert not hasattr(client, "processing_thread")

        client.initialize(config=config.config_file.read_config())
        assert client.is_initialized
        assert client.endpoint == "https://management.fake-endpoint-url/2023-10-12/telemetry"
        assert client._system_metadata["user_id"] == "user-id"
        assert client._system_metadata["monitor_id"] == "monitor-id"


def test_get_telemetry_identifier(mock_telemetry_client):
    """Ensures that getting the local-user-id handles empty/malformed strings"""
    # Confirm that we generate a new UUID if the setting doesn't exist, and write to config
    uuid.UUID(mock_telemetry_client.telemetry_id, version=4)  # Should not raise ValueError
    assert config.get_setting("telemetry.identifier") == mock_telemetry_client.telemetry_id

    # Confirm we generate a new UUID if the local_user_id is not a valid UUID
    config.set_setting("telemetry.identifier", "bad-id")
    telemetry_id = mock_telemetry_client._get_telemetry_identifier()
    assert telemetry_id != "bad-id"
    uuid.UUID(telemetry_id, version=4)  # Should not raise ValueError

    # Confirm the new user id was saved and is retrieved properly
    assert config.get_setting("telemetry.identifier") == telemetry_id
    assert mock_telemetry_client._get_telemetry_identifier() == telemetry_id


@pytest.mark.timeout(5)  # Timeout in case we don't exit the while loop
def test_process_event_queue_thread(mock_telemetry_client):
    """Test that the queue processing thread function exits cleanly after getting None"""
    # GIVEN
    queue_mock = MagicMock()
    queue_mock.get.side_effect = [TelemetryEvent(), None]
    mock_telemetry_client.event_queue = queue_mock
    # WHEN
    with patch.object(request, "urlopen") as urlopen_mock:
        mock_telemetry_client._process_event_queue_thread()
        urlopen_mock.assert_called_once()
    # THEN
    assert queue_mock.get.call_count == 2


@pytest.mark.parametrize(
    "http_code,attempt_count",
    [
        (400, 1),
        (429, TelemetryClient.MAX_RETRY_ATTEMPTS),
        (500, TelemetryClient.MAX_RETRY_ATTEMPTS),
    ],
)
@pytest.mark.timeout(5)  # Timeout in case we don't exit the while loop
def test_process_event_queue_thread_retries_and_exits(
    mock_telemetry_client, http_code, attempt_count
):
    """Test that the thread exits cleanly after getting an unexpected exception"""
    # GIVEN
    http_error = request.HTTPError("http://test.com", http_code, "Http Error", {}, None)  # type: ignore
    queue_mock = MagicMock()
    queue_mock.get.side_effect = [TelemetryEvent(), None]
    mock_telemetry_client.event_queue = queue_mock
    # WHEN
    with patch.object(request, "urlopen", side_effect=http_error) as urlopen_mock, patch.object(
        time, "sleep"
    ) as sleep_mock:
        mock_telemetry_client._process_event_queue_thread()
        urlopen_mock.call_count = attempt_count
        sleep_mock.call_count = attempt_count
    # THEN
    assert queue_mock.get.call_count == 1


@pytest.mark.timeout(5)  # Timeout in case we don't exit the while loop
def test_process_event_queue_thread_handles_unexpected_error(mock_telemetry_client):
    """Test that the thread exits cleanly after getting an unexpected exception"""
    # GIVEN
    queue_mock = MagicMock()
    queue_mock.get.side_effect = [TelemetryEvent(), None]
    mock_telemetry_client.event_queue = queue_mock
    # WHEN
    with patch.object(request, "urlopen", side_effect=Exception("Some error")) as urlopen_mock:
        mock_telemetry_client._process_event_queue_thread()
        urlopen_mock.assert_called_once()
    # THEN
    assert queue_mock.get.call_count == 1


def test_record_hashing_summary(mock_telemetry_client):
    """Tests that recording a hashing summary sends the expected TelemetryEvent to the thread queue"""
    # GIVEN
    queue_mock = MagicMock()
    test_summary = SummaryStatistics(total_bytes=123, total_files=12, total_time=12345)
    expected_summary = asdict(test_summary)
    expected_summary["usage_mode"] = "CLI"
    expected_event = TelemetryEvent(
        event_type="com.amazon.rum.deadline.job_attachments.hashing_summary",
        event_details=expected_summary,
    )
    mock_telemetry_client.event_queue = queue_mock

    # WHEN
    mock_telemetry_client.record_hashing_summary(test_summary)

    # THEN
    queue_mock.put_nowait.assert_called_once_with(expected_event)


def test_record_upload_summary(mock_telemetry_client):
    """Tests that recording an upload summary sends the expected TelemetryEvent to the thread queue"""
    # GIVEN
    queue_mock = MagicMock()
    test_summary = SummaryStatistics(total_bytes=123, total_files=12, total_time=12345)
    expected_summary = asdict(test_summary)
    expected_summary["usage_mode"] = "GUI"
    expected_event = TelemetryEvent(
        event_type="com.amazon.rum.deadline.job_attachments.upload_summary",
        event_details=expected_summary,
    )
    mock_telemetry_client.event_queue = queue_mock

    # WHEN
    mock_telemetry_client.record_upload_summary(test_summary, from_gui=True)

    # THEN
    queue_mock.put_nowait.assert_called_once_with(expected_event)


def test_record_error(mock_telemetry_client):
    """Test that recording an error sends the expected TelemetryEvent to the thread queue"""
    # GIVEN
    queue_mock = MagicMock()
    test_error_details = {"some_field": "some_value"}
    test_exc = Exception("some exception")
    expected_event_details = {
        "some_field": "some_value",
        "exception_type": str(type(test_exc)),
        "usage_mode": "CLI",
    }
    expected_event = TelemetryEvent(
        event_type="com.amazon.rum.deadline.error", event_details=expected_event_details
    )
    mock_telemetry_client.event_queue = queue_mock

    # WHEN
    mock_telemetry_client.record_error(test_error_details, str(type(test_exc)))

    # THEN
    queue_mock.put_nowait.assert_called_once_with(expected_event)


@pytest.mark.parametrize(
    "endpoint,prefix,expected_result",
    [
        pytest.param(
            "test.endpoint.url",
            "",
            "test.endpoint.url",
            id="The endpoint is not prefixed if the prefix is empty.",
        ),
        pytest.param(
            "test.endpoint.url",
            "management.",
            "test.endpoint.url",
            id="The endpoint is not prefixed if the endpoint does not start with 'https://'.",
        ),
        pytest.param(
            "https://test.endpoint.url",
            "management.",
            "https://management.test.endpoint.url",
            id="The prefix is inserted right after 'https://'.",
        ),
    ],
)
def test_get_prefixed_endpoint(
    mock_telemetry_client: TelemetryClient, endpoint: str, prefix: str, expected_result: str
):
    """Test that the _get_prefixed_endpoint function returns the expected prefixed endpoint"""
    assert mock_telemetry_client._get_prefixed_endpoint(endpoint, prefix) == expected_result


def test_record_decorator_success():
    """Tests that recording a decorator successful metric"""
    with patch.object(
        api._telemetry, "get_deadline_endpoint_url", side_effect=["https://fake-endpoint-url"]
    ):
        # GIVEN
        queue_mock = MagicMock()
        expected_summary: Dict[str, Any] = dict()
        expected_summary["is_success"] = True
        expected_summary["usage_mode"] = "CLI"
        expected_event = TelemetryEvent(
            event_type="com.amazon.rum.deadline.successful",
            event_details=expected_summary,
        )
        telemetry_client = get_deadline_cloud_library_telemetry_client()
        telemetry_client.event_queue = queue_mock

        @record_success_fail_telemetry_event()
        def successful():
            return

        # WHEN
        successful()  # type:ignore

        # THEN
        queue_mock.put_nowait.assert_called_once_with(expected_event)


def test_record_decorator_fails():
    """Tests that recording a decorator failed metric"""
    with patch.object(
        api._telemetry, "get_deadline_endpoint_url", side_effect=["https://fake-endpoint-url"]
    ):
        # GIVEN
        queue_mock = MagicMock()
        expected_summary: Dict[str, Any] = dict()
        expected_summary["is_success"] = False
        expected_summary["usage_mode"] = "CLI"
        expected_event = TelemetryEvent(
            event_type="com.amazon.rum.deadline.fails",
            event_details=expected_summary,
        )
        telemetry_client = get_deadline_cloud_library_telemetry_client()
        telemetry_client.event_queue = queue_mock

        @record_success_fail_telemetry_event()
        def fails():
            raise RuntimeError("foobar")

        # WHEN
        with pytest.raises(RuntimeError):
            fails()  # type:ignore

        # THEN
        queue_mock.put_nowait.assert_called_once_with(expected_event)


def test_latency_decorator():
    """Tests that the latency recording decorator works"""
    with patch.object(
        api._telemetry, "get_deadline_endpoint_url", side_effect=["https://fake-endpoint-url"]
    ), patch.object(time, "perf_counter_ns", return_value=0):
        # GIVEN
        queue_mock = MagicMock()
        expected_summary: Dict[str, Any] = dict()
        expected_summary["latency"] = 0
        expected_summary["function_call"] = "test_call"
        expected_summary["usage_mode"] = "CLI"
        expected_event = TelemetryEvent(
            event_type="com.amazon.rum.deadline.latency",
            event_details=expected_summary,
        )
        telemetry_client = get_deadline_cloud_library_telemetry_client()
        telemetry_client.event_queue = queue_mock

        @record_function_latency_telemetry_event()
        def test_call():
            return

        # WHEN
        test_call()  # type:ignore

        # THEN
        queue_mock.put_nowait.assert_called_once_with(expected_event)
