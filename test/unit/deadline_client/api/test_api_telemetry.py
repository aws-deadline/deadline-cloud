# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from typing import Any, Dict
import json
import platform
import pytest
import uuid
import time

from unittest.mock import patch, MagicMock
from dataclasses import asdict
from urllib import request

from deadline.client import api, config
from deadline.client.api._agent_detection import detect_invoking_agent
from deadline.client.api._telemetry import (
    TelemetryClient,
    TelemetryEvent,
    _swallow_exceptions,
    get_deadline_cloud_library_telemetry_client,
    get_telemetry_client,
    record_success_fail_telemetry_event,
    record_function_latency_telemetry_event,
)
from deadline.client.api._stack_trace_sanitizer import (
    _sanitize_path,
    sanitize_exception,
)
from deadline.job_attachments.progress_tracker import SummaryStatistics


@pytest.fixture(scope="function", name="mock_telemetry_client")
def fixture_telemetry_client(fresh_deadline_config):
    config.set_setting("defaults.aws_profile_name", "SomeRandomProfileName")
    with (
        patch.object(api.TelemetryClient, "_start_threads"),
        patch.object(api._telemetry, "get_monitor_id", side_effect=["monitor-id"]),
        patch.object(api._telemetry, "get_monitor_id", side_effect=[None]),
        patch.object(
            api._telemetry,
            "get_user_and_identity_store_id",
            side_effect=[("user-id", "identity-store-id")],
        ),
        patch.object(
            api._telemetry, "get_deadline_endpoint_url", side_effect=["https://fake-endpoint-url"]
        ),
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
    client.record_error_with_trace(RuntimeError("opt-out test"), "test")


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
    client.record_error_with_trace(RuntimeError("opt-out test"), "test")


def test_initialize_failure_then_success(fresh_deadline_config):
    """
    Tests that a failure in initializing set keeps the property as false, but trying again
    without an exception initializes everything successfully.
    """
    config.set_setting("defaults.aws_profile_name", "SomeRandomProfileName")
    with (
        patch.object(api.TelemetryClient, "_start_threads"),
        patch.object(api._telemetry, "get_monitor_id", side_effect=["monitor-id"]),
        patch.object(
            api._telemetry,
            "get_user_and_identity_store_id",
            side_effect=[("user-id", "identity-store-id")],
        ),
        patch.object(
            api._telemetry,
            "get_deadline_endpoint_url",
            side_effect=[Exception("Boto3 blew up!"), "https://fake-endpoint-url"],
        ),
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


def test_get_telemetry_identifier(fresh_deadline_config, mock_telemetry_client):
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
def test_process_event_queue_thread(fresh_deadline_config, mock_telemetry_client):
    """Test that the queue processing thread function exits cleanly after getting None"""
    # GIVEN
    queue_mock = MagicMock()
    queue_mock.get.side_effect = [TelemetryEvent(), None]
    mock_telemetry_client.event_queue = queue_mock
    # WHEN
    with (
        patch.object(request, "urlopen") as urlopen_mock,
        patch.object(TelemetryClient, "get_account_id", return_value=None),
        patch.object(api._telemetry, "get_boto3_session"),
    ):
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
    fresh_deadline_config, mock_telemetry_client, http_code, attempt_count
):
    """Test that the thread exits cleanly after getting an unexpected exception"""
    # GIVEN
    http_error = request.HTTPError("http://test.com", http_code, "Http Error", {}, None)  # type: ignore
    queue_mock = MagicMock()
    queue_mock.get.side_effect = [TelemetryEvent(), None]
    mock_telemetry_client.event_queue = queue_mock
    # WHEN
    with (
        patch.object(request, "urlopen", side_effect=http_error) as urlopen_mock,
        patch.object(time, "sleep") as sleep_mock,
        patch.object(TelemetryClient, "get_account_id", return_value=None),
        patch.object(api._telemetry, "get_boto3_session"),
    ):
        mock_telemetry_client._process_event_queue_thread()
        urlopen_mock.call_count = attempt_count
        sleep_mock.call_count = attempt_count
    # THEN
    assert queue_mock.get.call_count == 1


@pytest.mark.timeout(5)  # Timeout in case we don't exit the while loop
def test_process_event_queue_thread_handles_unexpected_error(
    fresh_deadline_config, mock_telemetry_client
):
    """Test that the thread exits cleanly after getting an unexpected exception"""
    # GIVEN
    queue_mock = MagicMock()
    queue_mock.get.side_effect = [TelemetryEvent(), None]
    mock_telemetry_client.event_queue = queue_mock
    # WHEN
    with (
        patch.object(request, "urlopen", side_effect=Exception("Some error")) as urlopen_mock,
        patch.object(TelemetryClient, "get_account_id", return_value=None),
        patch.object(api._telemetry, "get_boto3_session"),
    ):
        mock_telemetry_client._process_event_queue_thread()
        urlopen_mock.assert_called_once()
    # THEN
    assert queue_mock.get.call_count == 1


def test_record_hashing_summary(fresh_deadline_config, mock_telemetry_client):
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


def test_record_upload_summary(fresh_deadline_config, mock_telemetry_client):
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


def test_record_error(fresh_deadline_config, mock_telemetry_client):
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


def test_record_error_with_trace(fresh_deadline_config, mock_telemetry_client):
    """Test that record_error_with_trace sends a TelemetryEvent with exactly the expected fields."""
    # GIVEN
    queue_mock = MagicMock()
    mock_telemetry_client.event_queue = queue_mock

    try:
        raise ValueError("something broke")
    except ValueError as exc:
        with (
            patch.object(mock_telemetry_client, "get_account_id", return_value="111122223333"),
            patch.object(api._telemetry, "get_boto3_session"),
        ):
            # WHEN
            mock_telemetry_client.record_error_with_trace(exc, "test_scope")

    # THEN
    queue_mock.put_nowait.assert_called_once()
    event: TelemetryEvent = queue_mock.put_nowait.call_args[0][0]
    assert event.event_type == "com.amazon.rum.deadline.error"
    # Pop the dynamic stack_trace and assert on the rest as a full dict so
    # any unexpected key being added would cause the test to fail loudly.
    stack_trace = event.event_details.pop("stack_trace")
    assert event.event_details == {
        "exception_type": "ValueError",
        "exception_scope": "test_scope",
        "usage_mode": "CLI",
    }
    assert "ValueError" in stack_trace
    assert "Traceback (most recent call last):" in stack_trace


def test_record_error_with_trace_extra_details(fresh_deadline_config, mock_telemetry_client):
    """Test that extra_details are merged into the event and no unexpected fields appear."""
    # GIVEN
    queue_mock = MagicMock()
    mock_telemetry_client.event_queue = queue_mock

    try:
        raise RuntimeError("fail")
    except RuntimeError as exc:
        with (
            patch.object(mock_telemetry_client, "get_account_id", return_value="111122223333"),
            patch.object(api._telemetry, "get_boto3_session"),
        ):
            # WHEN
            mock_telemetry_client.record_error_with_trace(
                exc, "cli", extra_details={"command": "bundle submit"}
            )

    # THEN
    event: TelemetryEvent = queue_mock.put_nowait.call_args[0][0]
    stack_trace = event.event_details.pop("stack_trace")
    assert event.event_details == {
        "exception_type": "RuntimeError",
        "exception_scope": "cli",
        "command": "bundle submit",
        "usage_mode": "CLI",
    }
    assert "RuntimeError" in stack_trace


def test_record_error_with_trace_sanitizes_paths(fresh_deadline_config, mock_telemetry_client):
    """Customer paths must be stripped from the stack trace and no extra fields leak."""
    # GIVEN
    queue_mock = MagicMock()
    mock_telemetry_client.event_queue = queue_mock

    try:
        raise TypeError("bad type")
    except TypeError as exc:
        with (
            patch.object(mock_telemetry_client, "get_account_id", return_value="111122223333"),
            patch.object(api._telemetry, "get_boto3_session"),
        ):
            # WHEN
            mock_telemetry_client.record_error_with_trace(exc, "test")

    # THEN
    event: TelemetryEvent = queue_mock.put_nowait.call_args[0][0]
    stack_trace = event.event_details.pop("stack_trace")
    assert event.event_details == {
        "exception_type": "TypeError",
        "exception_scope": "test",
        "usage_mode": "CLI",
    }
    # Every "File ..." line in the trace must reference a sanitized
    # (relative) path, never an absolute filesystem path.
    for line in stack_trace.splitlines():
        if line.strip().startswith('File "'):
            path = line.split('"')[1]
            assert not path.startswith("/"), f"Absolute path leaked: {path}"


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
    fresh_deadline_config,
    mock_telemetry_client: TelemetryClient,
    endpoint: str,
    prefix: str,
    expected_result: str,
):
    """Test that the _get_prefixed_endpoint function returns the expected prefixed endpoint"""
    assert mock_telemetry_client._get_prefixed_endpoint(endpoint, prefix) == expected_result


def test_record_decorator_success(fresh_deadline_config):
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


def test_record_decorator_fails(fresh_deadline_config):
    """Tests that recording a decorator failed metric"""
    with patch.object(
        api._telemetry, "get_deadline_endpoint_url", side_effect=["https://fake-endpoint-url"]
    ):
        # GIVEN
        queue_mock = MagicMock()
        expected_summary: Dict[str, Any] = dict()
        expected_summary["is_success"] = False
        expected_summary["exception_type"] = "RuntimeError"
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


def test_latency_decorator(fresh_deadline_config):
    """Tests that the latency recording decorator works"""
    with (
        patch.object(
            api._telemetry, "get_deadline_endpoint_url", side_effect=["https://fake-endpoint-url"]
        ),
        patch.object(time, "perf_counter_ns", return_value=0),
    ):
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


def test_get_telemetry_client_caches_by_package_name(fresh_deadline_config):
    """
    Verify that get_telemetry_client returns different clients for different package names.
    """
    import deadline.client.api._telemetry as telemetry_mod

    telemetry_mod.__cached_telemetry_clients = {}

    def fake_init(self, **kwargs):
        self._initialized = True
        self.package_name = kwargs["package_name"]

    with patch.object(TelemetryClient, "__init__", fake_init):
        client_a = get_telemetry_client("package-a", "1.0.0")
        client_b = get_telemetry_client("package-b", "2.0.0")
        client_a_again = get_telemetry_client("package-a", "1.0.0")

        assert client_a is not client_b
        assert client_a is client_a_again
        assert client_a.package_name == "package-a"
        assert client_b.package_name == "package-b"

    telemetry_mod.__cached_telemetry_clients = {}


def test_process_start_event_emitted_on_start_threads(fresh_deadline_config):
    """Tests that a process_start event is emitted when _start_threads is called"""
    config.set_setting("defaults.aws_profile_name", "SomeRandomProfileName")
    with (
        patch.object(api._telemetry, "get_monitor_id", side_effect=[None]),
        patch.object(
            api._telemetry,
            "get_user_and_identity_store_id",
            side_effect=[("user-id", "identity-store-id")],
        ),
        patch.object(
            api._telemetry, "get_deadline_endpoint_url", side_effect=["https://fake-endpoint-url"]
        ),
    ):
        client = TelemetryClient(
            package_name="deadline-cloud-library",
            package_ver="0.1.2.1234",
            config=config.config_file.read_config(),
        )
        assert client.is_initialized
        expected_event = TelemetryEvent(
            event_type="com.amazon.rum.deadline.process_start",
            event_details={},
        )
        # Drain the queue to find the session_start event
        events = []
        while not client.event_queue.empty():
            event = client.event_queue.get_nowait()
            if event is not None:
                events.append(event)
        assert expected_event in events


class TestSwallowExceptionsDecorator:
    """Tests for the _swallow_exceptions decorator"""

    def test_returns_value_on_success(self):
        @_swallow_exceptions
        def succeeds():
            return 42

        assert succeeds() == 42

    def test_returns_none_on_exception(self):
        @_swallow_exceptions
        def fails():
            raise RuntimeError("boom")

        assert fails() is None

    def test_logs_exception(self):
        @_swallow_exceptions
        def fails():
            raise RuntimeError("boom")

        with patch("deadline.client.api._telemetry.logger") as mock_logger:
            fails()
            mock_logger.debug.assert_called_once()
            assert "fails" in mock_logger.debug.call_args[0][1]

    def test_preserves_function_name(self):
        @_swallow_exceptions
        def my_func():
            pass

        assert my_func.__name__ == "my_func"


class TestTelemetryClientSwallowExceptions:
    """Tests that decorated TelemetryClient methods don't propagate exceptions"""

    def test_set_opt_out_swallows_exception(self, fresh_deadline_config, mock_telemetry_client):
        with patch.object(config.config_file, "get_setting", side_effect=RuntimeError("boom")):
            mock_telemetry_client.set_opt_out()

    def test_initialize_swallows_exception(self, fresh_deadline_config, mock_telemetry_client):
        mock_telemetry_client._initialized = False
        mock_telemetry_client.telemetry_opted_out = False
        with patch.object(
            api._telemetry, "get_deadline_endpoint_url", side_effect=RuntimeError("boom")
        ):
            mock_telemetry_client.initialize()
        assert not mock_telemetry_client.is_initialized

    def test_record_event_swallows_exception(self, fresh_deadline_config, mock_telemetry_client):
        with patch.object(
            mock_telemetry_client, "_put_telemetry_record", side_effect=RuntimeError("boom")
        ):
            mock_telemetry_client.record_event(
                event_type="com.amazon.rum.deadline.test",
                event_details={},
                from_gui=False,
            )

    def test_exit_cleanly_swallows_exception(self, fresh_deadline_config, mock_telemetry_client):
        mock_telemetry_client.event_queue = MagicMock()
        mock_telemetry_client.event_queue.put_nowait.side_effect = RuntimeError("boom")
        mock_telemetry_client._exit_cleanly()

    def test_init_swallows_get_telemetry_identifier_exception(self, fresh_deadline_config):
        config.set_setting("defaults.aws_profile_name", "SomeRandomProfileName")
        with (
            patch.object(api.TelemetryClient, "_start_threads"),
            patch.object(api._telemetry, "get_monitor_id", side_effect=[None]),
            patch.object(
                api._telemetry,
                "get_user_and_identity_store_id",
                side_effect=[("user-id", "identity-store-id")],
            ),
            patch.object(
                api._telemetry,
                "get_deadline_endpoint_url",
                side_effect=["https://fake-endpoint-url"],
            ),
            patch.object(config.config_file, "get_setting", side_effect=RuntimeError("boom")),
        ):
            client = TelemetryClient(
                package_name="deadline-cloud-library",
                package_ver="0.1.2.1234",
                config=config.config_file.read_config(),
            )
            assert client.telemetry_id is not None

    def test_init_swallows_get_system_metadata_exception(self, fresh_deadline_config):
        config.set_setting("defaults.aws_profile_name", "SomeRandomProfileName")
        with (
            patch.object(api.TelemetryClient, "_start_threads"),
            patch.object(api._telemetry, "get_monitor_id", side_effect=[None]),
            patch.object(
                api._telemetry,
                "get_user_and_identity_store_id",
                side_effect=[("user-id", "identity-store-id")],
            ),
            patch.object(
                api._telemetry,
                "get_deadline_endpoint_url",
                side_effect=["https://fake-endpoint-url"],
            ),
            patch.object(platform, "uname", side_effect=RuntimeError("boom")),
        ):
            client = TelemetryClient(
                package_name="deadline-cloud-library",
                package_ver="0.1.2.1234",
                config=config.config_file.read_config(),
            )
            assert "version" not in client._system_metadata


@pytest.mark.parametrize(
    "filepath, expected",
    [
        pytest.param(
            "/home/customer/secret/venv/lib/python3.11/site-packages/deadline/client/api/_telemetry.py",
            "deadline/client/api/_telemetry.py",
            id="known_package_deadline",
        ),
        pytest.param(
            "/opt/libs/openjd/sessions/runner.py",
            "openjd/sessions/runner.py",
            id="known_package_openjd",
        ),
        pytest.param(
            "/usr/lib/python3/dist-packages/botocore/client.py",
            "botocore/client.py",
            id="known_package_botocore",
        ),
        pytest.param(
            "/home/user/venv/lib/python3.11/site-packages/somelib/core.py",
            "somelib/core.py",
            id="site_packages_unknown_lib",
        ),
        pytest.param(
            "/home/customer/my-bucket-name/scripts/render.py",
            "render.py",
            id="customer_script_returns_filename_only",
        ),
        pytest.param(
            "C:\\Users\\customer\\AppData\\Local\\deadline\\client\\api\\_telemetry.py",
            "deadline/client/api/_telemetry.py",
            id="windows_path",
        ),
        pytest.param(
            "<frozen importlib._bootstrap>",
            "<frozen importlib._bootstrap>",
            id="frozen_module",
        ),
        pytest.param("<string>", "<string>", id="string_input"),
        # A customer directory that happens to share a name with one of our
        # known packages must not anchor the trim — only the *rightmost*
        # match (the actually-installed package) should.
        pytest.param(
            "/home/user/deadline/scripts/venv/lib/python3.11/site-packages/deadline/client/api/_telemetry.py",
            "deadline/client/api/_telemetry.py",
            id="known_package_name_appears_in_customer_dir",
        ),
        pytest.param(
            "/opt/openjd/customer-vendored/openjd/sessions/runner.py",
            "openjd/sessions/runner.py",
            id="known_package_name_appears_twice",
        ),
        # Same right-to-left rule applies to site-packages: a customer
        # directory literally named "site-packages" must not leak the
        # segments between it and the real site-packages dir.
        pytest.param(
            "/home/user/site-packages/scripts/venv/lib/python3.11/site-packages/somelib/core.py",
            "somelib/core.py",
            id="site_packages_name_appears_in_customer_dir",
        ),
    ],
)
def test_sanitize_path(filepath, expected):
    assert _sanitize_path(filepath) == expected


class TestSanitizeException:
    def test_live_exception(self):
        try:
            raise RuntimeError("test error")
        except RuntimeError as e:
            result = sanitize_exception(e)
            assert "RuntimeError" in result
            assert "Traceback (most recent call last):" in result

    def test_no_absolute_paths(self):
        try:
            raise RuntimeError("path test")
        except RuntimeError as e:
            result = sanitize_exception(e)
            for line in result.splitlines():
                if line.strip().startswith('File "'):
                    path = line.split('"')[1]
                    assert not path.startswith("/"), f"Absolute path leaked: {path}"

    def test_no_source_code_context(self):
        """Source code lines are omitted to avoid leaking customer data."""
        try:
            customer_secret = "sensitive"  # noqa: F841
            raise ValueError("fail")
        except ValueError as e:
            result = sanitize_exception(e)
            assert "customer_secret" not in result
            assert "sensitive" not in result

    def test_message_omitted(self):
        """Exception messages are not included — only the type."""
        try:
            raise FileNotFoundError("/home/customer/secret/file.txt")
        except FileNotFoundError as e:
            result = sanitize_exception(e)
            assert "customer" not in result
            assert "secret" not in result
            assert "FileNotFoundError" in result

    def test_chained_exception_cause(self):
        try:
            try:
                raise KeyError("original")
            except KeyError as e:
                raise ValueError("wrapper") from e
        except ValueError as e:
            result = sanitize_exception(e)
            assert "KeyError" in result
            assert "ValueError" in result
            assert "direct cause" in result

    def test_chained_exception_context(self):
        try:
            try:
                raise KeyError("original")
            except KeyError:
                raise ValueError("during handling")
        except ValueError as e:
            result = sanitize_exception(e)
            assert "KeyError" in result
            assert "ValueError" in result
            assert "During handling" in result


class TestGetAccountId:
    """Tests for the background-thread account ID resolution."""

    def test_prefers_credential_account_id(self, fresh_deadline_config, mock_telemetry_client):
        """When credentials expose account_id, it is used directly without any STS call."""
        session_mock = MagicMock()
        session_mock.get_credentials.return_value.account_id = "111122223333"
        # Bypass the @lru_cache so each test gets a fresh call.
        mock_telemetry_client.get_account_id.cache_clear()
        assert mock_telemetry_client.get_account_id(session_mock) == "111122223333"
        session_mock.client.assert_not_called()

    def test_falls_back_to_sts_when_credentials_lack_account_id(
        self, fresh_deadline_config, mock_telemetry_client
    ):
        """When credentials don't expose account_id, a short-timeout STS call is used."""
        session_mock = MagicMock()
        session_mock.get_credentials.return_value.account_id = None
        session_mock.client.return_value.get_caller_identity.return_value = {
            "Account": "444455556666"
        }
        mock_telemetry_client.get_account_id.cache_clear()
        assert mock_telemetry_client.get_account_id(session_mock) == "444455556666"
        # The STS client must be built with a short-timeout Config.
        args, kwargs = session_mock.client.call_args
        assert args[0] == "sts"
        assert kwargs["config"].connect_timeout == 2
        assert kwargs["config"].read_timeout == 2

    def test_returns_none_when_sts_unreachable(self, fresh_deadline_config, mock_telemetry_client):
        """When STS is unreachable, get_account_id silently returns None."""
        session_mock = MagicMock()
        session_mock.get_credentials.return_value.account_id = None
        session_mock.client.return_value.get_caller_identity.side_effect = Exception(
            "STS unreachable"
        )
        mock_telemetry_client.get_account_id.cache_clear()
        assert mock_telemetry_client.get_account_id(session_mock) is None

    def test_returns_none_when_no_credentials(self, fresh_deadline_config, mock_telemetry_client):
        """When there are no credentials at all, returns None without calling STS."""
        session_mock = MagicMock()
        session_mock.get_credentials.return_value = None
        session_mock.client.return_value.get_caller_identity.side_effect = Exception(
            "no creds, no STS"
        )
        mock_telemetry_client.get_account_id.cache_clear()
        assert mock_telemetry_client.get_account_id(session_mock) is None


@pytest.mark.timeout(5)
def test_process_event_queue_thread_attaches_account_id(
    fresh_deadline_config, mock_telemetry_client
):
    """The background thread resolves the account ID once and attaches it to _common_details."""
    queue_mock = MagicMock()
    queue_mock.get.side_effect = [TelemetryEvent(), None]
    mock_telemetry_client.event_queue = queue_mock

    with (
        patch.object(
            TelemetryClient, "get_account_id", return_value="111122223333"
        ) as resolve_mock,
        patch.object(api._telemetry, "get_boto3_session"),
        patch.object(request, "urlopen"),
    ):
        mock_telemetry_client._process_event_queue_thread()

    resolve_mock.assert_called_once()
    assert mock_telemetry_client._common_details.get("accountId") == "111122223333"


@pytest.mark.timeout(5)
def test_process_event_queue_thread_merges_common_details_into_payload(
    fresh_deadline_config, mock_telemetry_client
):
    """Common details (including a late-resolved account ID) are merged into the event
    payload at send time, so even an event that was enqueued before resolution completes
    still includes the resolved accountId in the outgoing request body."""
    queue_mock = MagicMock()
    queue_mock.get.side_effect = [
        TelemetryEvent(
            event_type="com.amazon.rum.deadline.test",
            event_details={"probe": 1, "usage_mode": "CLI"},
        ),
        None,
    ]
    mock_telemetry_client.event_queue = queue_mock

    with (
        patch.object(TelemetryClient, "get_account_id", return_value="111122223333"),
        patch.object(api._telemetry, "get_boto3_session"),
        patch.object(request, "urlopen") as urlopen_mock,
    ):
        mock_telemetry_client._process_event_queue_thread()

    # Inspect the actual HTTP request body sent by urlopen.
    assert urlopen_mock.call_count == 1
    sent_request = urlopen_mock.call_args[0][0]
    body = json.loads(sent_request.data.decode("utf-8"))
    details = json.loads(body["RumEvents"][0]["details"])
    assert details["accountId"] == "111122223333"
    assert details["probe"] == 1
    assert details["usage_mode"] == "CLI"


@pytest.mark.timeout(5)
def test_process_event_queue_thread_skips_account_id_when_resolution_fails(
    fresh_deadline_config, mock_telemetry_client
):
    """When the account ID can't be resolved, no accountId key is added to common details."""
    queue_mock = MagicMock()
    queue_mock.get.side_effect = [TelemetryEvent(), None]
    mock_telemetry_client.event_queue = queue_mock

    with (
        patch.object(TelemetryClient, "get_account_id", return_value=None) as resolve_mock,
        patch.object(api._telemetry, "get_boto3_session"),
        patch.object(request, "urlopen"),
    ):
        mock_telemetry_client._process_event_queue_thread()

    resolve_mock.assert_called_once()
    assert "accountId" not in mock_telemetry_client._common_details


class TestGetMonitorSessionId:
    """Tests for TelemetryClient._get_monitor_session_id"""

    @pytest.fixture
    def make_client(self, fresh_deadline_config):
        """Creates a TelemetryClient with telemetry internals patched out."""

        def _make():
            with (
                patch.object(api.TelemetryClient, "_start_threads"),
                patch.object(api._telemetry, "get_monitor_id", return_value=None),
                patch.object(
                    api._telemetry, "get_user_and_identity_store_id", return_value=(None, None)
                ),
                patch.object(
                    api._telemetry,
                    "get_deadline_endpoint_url",
                    return_value="https://fake-endpoint",
                ),
            ):
                return TelemetryClient(
                    package_name="deadline-cloud-library",
                    package_ver="0.1.2",
                    config=config.config_file.read_config(),
                )

        return _make

    def test_reads_profile_scoped_session_id(self, fresh_deadline_config, make_client):
        """When a profile-scoped monitor session_id exists, it is returned."""
        config.set_setting("defaults.aws_profile_name", "my-profile-us-west-2")
        deadline_config = config.config_file.read_config()
        deadline_config["profile-my-profile-us-west-2 deadline-cloud-monitor"] = {
            "session_id": "abc123"
        }
        config.config_file.write_config(deadline_config)

        client = make_client()
        assert client._common_details.get("monitor_session_id") == "abc123"

    def test_falls_back_to_global_section(self, fresh_deadline_config, make_client):
        """When no profile-scoped section exists, falls back to [deadline-cloud-monitor]."""
        config.set_setting("defaults.aws_profile_name", "my-profile-us-west-2")
        deadline_config = config.config_file.read_config()
        deadline_config["deadline-cloud-monitor"] = {"session_id": "global-session-456"}
        config.config_file.write_config(deadline_config)

        client = make_client()
        assert client._common_details.get("monitor_session_id") == "global-session-456"

    def test_profile_scoped_takes_precedence_over_global(self, fresh_deadline_config, make_client):
        """Profile-scoped session_id is preferred over the global one."""
        config.set_setting("defaults.aws_profile_name", "my-profile-us-west-2")
        deadline_config = config.config_file.read_config()
        deadline_config["deadline-cloud-monitor"] = {"session_id": "global-session"}
        deadline_config["profile-my-profile-us-west-2 deadline-cloud-monitor"] = {
            "session_id": "profile-session"
        }
        config.config_file.write_config(deadline_config)

        client = make_client()
        assert client._common_details.get("monitor_session_id") == "profile-session"

    def test_no_session_id_when_not_configured(self, fresh_deadline_config, make_client):
        """When no monitor session_id is in the config, it is not added to common details."""
        config.set_setting("defaults.aws_profile_name", "my-profile-us-west-2")

        client = make_client()
        assert "monitor_session_id" not in client._common_details

    def test_empty_session_id_treated_as_absent(self, fresh_deadline_config, make_client):
        """An empty session_id value in config is treated as absent."""
        config.set_setting("defaults.aws_profile_name", "my-profile-us-west-2")
        deadline_config = config.config_file.read_config()
        deadline_config["profile-my-profile-us-west-2 deadline-cloud-monitor"] = {"session_id": ""}
        config.config_file.write_config(deadline_config)

        client = make_client()
        assert "monitor_session_id" not in client._common_details


# Every env var that detect_invoking_agent() inspects. Cleared before each agent
# detection test so the host environment (which may itself be an agent, e.g. a
# developer running pytest inside Claude Code) can't leak into the assertions.
_ALL_AGENT_ENV_VARS = (
    "AI_AGENT",
    "AGENT",
    "CLAUDECODE",
    "CLAUDE_CODE",
    "CODEX_SANDBOX",
    "CODEX_THREAD_ID",
    "CURSOR_AGENT",
    "REPL_ID",
    "GEMINI_CLI",
    "OPENCODE",
    "AUGMENT_AGENT",
    "GOOSE_PROVIDER",
    "EDITOR",
    "TERM_PROGRAM",
)


@pytest.fixture(name="clean_agent_env")
def fixture_clean_agent_env(monkeypatch):
    """Removes all agent-detection env vars so tests start from a 'human' baseline."""
    for var in _ALL_AGENT_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    return monkeypatch


def test_detect_invoking_agent_human(clean_agent_env):
    """With no agent markers set, detection returns None (human / direct invocation)."""
    assert detect_invoking_agent() is None


@pytest.mark.parametrize(
    "env_var, expected",
    [
        pytest.param("CLAUDECODE", "claude-code", id="claude-code"),
        pytest.param("CLAUDE_CODE", "claude-code", id="claude-code-alt"),
        pytest.param("CODEX_SANDBOX", "codex", id="codex-sandbox"),
        pytest.param("CODEX_THREAD_ID", "codex", id="codex-thread"),
        pytest.param("CURSOR_AGENT", "cursor", id="cursor"),
        pytest.param("REPL_ID", "replit", id="replit"),
        pytest.param("GEMINI_CLI", "gemini", id="gemini"),
        pytest.param("OPENCODE", "opencode", id="opencode"),
        pytest.param("AUGMENT_AGENT", "auggie", id="auggie"),
        pytest.param("GOOSE_PROVIDER", "goose", id="goose"),
    ],
)
def test_detect_invoking_agent_presence_markers(clean_agent_env, env_var, expected):
    """Presence of a known agent marker env var resolves to its canonical name."""
    clean_agent_env.setenv(env_var, "1")
    assert detect_invoking_agent() == expected


@pytest.mark.parametrize(
    "env_var, value, expected",
    [
        pytest.param("EDITOR", "/usr/local/bin/devin-editor", "devin", id="devin-editor"),
        pytest.param("TERM_PROGRAM", "kiro", "kiro", id="kiro-term"),
        pytest.param("EDITOR", "vim", None, id="non-agent-editor"),
    ],
)
def test_detect_invoking_agent_value_markers(clean_agent_env, env_var, value, expected):
    """Value-substring markers (IDE/editor integrations) match on their value."""
    clean_agent_env.setenv(env_var, value)
    assert detect_invoking_agent() == expected


@pytest.mark.parametrize(
    "value, expected",
    [
        pytest.param("goose", "goose", id="named"),
        pytest.param("claude-code_2-1-177_agent", "claude-code", id="versioned-token"),
        pytest.param("1", "unknown", id="generic-true-numeric"),
        pytest.param("true", "unknown", id="generic-true"),
    ],
)
def test_detect_invoking_agent_generic_agent_var(clean_agent_env, value, expected):
    """The proposed generic AGENT var carries a name, or 1/true -> 'unknown'."""
    clean_agent_env.setenv("AGENT", value)
    assert detect_invoking_agent() == expected


def test_detect_invoking_agent_ai_agent_override(clean_agent_env):
    """AI_AGENT takes priority and is lowercased to its leading token."""
    clean_agent_env.setenv("AI_AGENT", "Claude-Code_1-2-3_agent")
    clean_agent_env.setenv("CURSOR_AGENT", "1")  # would otherwise win
    assert detect_invoking_agent() == "claude-code"


@pytest.mark.parametrize(
    "value, expected",
    [
        # Spaces/slashes/control chars are stripped so the User-Agent token and
        # telemetry payload can't be corrupted by a malformed env value.
        pytest.param("foo bar", "foobar", id="strips-space"),
        pytest.param("a/b\\c", "abc", id="strips-slashes"),
        pytest.param("name\nwith\rnewlines", "namewithnewlines", id="strips-control-chars"),
        pytest.param("keep.this-name_x", "keep.this-name", id="keeps-safe-chars-splits-underscore"),
        pytest.param("MixedCase", "mixedcase", id="lowercased"),
        pytest.param("!@#$%", None, id="all-disallowed-falls-through"),
        pytest.param("x" * 200, "x" * 64, id="length-capped"),
    ],
)
def test_detect_invoking_agent_override_sanitized(clean_agent_env, value, expected):
    """The user-controlled AI_AGENT/AGENT override is constrained to a safe charset/length."""
    clean_agent_env.setenv("AI_AGENT", value)
    assert detect_invoking_agent() == expected


def test_detect_invoking_agent_presence_beats_ide(clean_agent_env):
    """A specific presence marker is detected before a host IDE value marker."""
    clean_agent_env.setenv("TERM_PROGRAM", "kiro")
    clean_agent_env.setenv("CLAUDECODE", "1")
    assert detect_invoking_agent() == "claude-code"


def test_telemetry_client_tags_human(fresh_deadline_config, clean_agent_env):
    """A client created with no agent env tags events invoked_by=HUMAN, no agent_name."""
    # Opt out so __init__ skips network initialize(); _common_details is populated first.
    config.set_setting("telemetry.opt_out", "true")
    client = TelemetryClient(
        "deadline-cloud-library", "test-version", config=config.config_file.read_config()
    )
    assert client._common_details["invoked_by"] == "HUMAN"
    assert "agent_name" not in client._common_details


def test_telemetry_client_tags_agent(fresh_deadline_config, clean_agent_env):
    """A client created under an agent env tags invoked_by=AGENT with the agent_name."""
    config.set_setting("telemetry.opt_out", "true")
    clean_agent_env.setenv("CLAUDECODE", "1")
    client = TelemetryClient(
        "deadline-cloud-library", "test-version", config=config.config_file.read_config()
    )
    assert client._common_details["invoked_by"] == "AGENT"
    assert client._common_details["agent_name"] == "claude-code"
