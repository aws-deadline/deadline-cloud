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
from deadline.client.api._stack_trace_sanitizer import (
    _sanitize_path,
    sanitize_message,
    sanitize_traceback_string,
    sanitize_exception,
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
    try:
        raise RuntimeError("opt-out test")
    except RuntimeError as exc:
        client.record_error_with_trace(exc, "test")


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
    try:
        raise RuntimeError("opt-out test")
    except RuntimeError as exc:
        client.record_error_with_trace(exc, "test")


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
    fresh_deadline_config, mock_telemetry_client, http_code, attempt_count
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
def test_process_event_queue_thread_handles_unexpected_error(
    fresh_deadline_config, mock_telemetry_client
):
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


def test_record_hashing_summary(fresh_deadline_config, mock_telemetry_client):
    """Tests that recording a hashing summary sends the expected TelemetryEvent to the thread queue"""
    # GIVEN
    queue_mock = MagicMock()
    test_summary = SummaryStatistics(total_bytes=123, total_files=12, total_time=12345)
    expected_summary = asdict(test_summary)
    expected_summary["usage_mode"] = "CLI"
    expected_summary["accountId"] = "111122223333"
    expected_event = TelemetryEvent(
        event_type="com.amazon.rum.deadline.job_attachments.hashing_summary",
        event_details=expected_summary,
    )
    mock_telemetry_client.event_queue = queue_mock

    # WHEN
    with patch.object(
        mock_telemetry_client, "get_account_id", return_value="111122223333"
    ), patch.object(api._telemetry, "get_boto3_session"):
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
    expected_summary["accountId"] = "111122223333"
    expected_event = TelemetryEvent(
        event_type="com.amazon.rum.deadline.job_attachments.upload_summary",
        event_details=expected_summary,
    )
    mock_telemetry_client.event_queue = queue_mock

    # WHEN
    with patch.object(
        mock_telemetry_client, "get_account_id", return_value="111122223333"
    ), patch.object(api._telemetry, "get_boto3_session"):
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
        "accountId": "111122223333",
    }
    expected_event = TelemetryEvent(
        event_type="com.amazon.rum.deadline.error", event_details=expected_event_details
    )
    mock_telemetry_client.event_queue = queue_mock

    with patch.object(
        mock_telemetry_client, "get_account_id", return_value="111122223333"
    ), patch.object(api._telemetry, "get_boto3_session"):
        # WHEN
        mock_telemetry_client.record_error(test_error_details, str(type(test_exc)))

    # THEN
    queue_mock.put_nowait.assert_called_once_with(expected_event)


def test_record_error_with_trace(fresh_deadline_config, mock_telemetry_client):
    """Test that record_error_with_trace sends a TelemetryEvent with sanitized stack trace fields"""
    # GIVEN
    queue_mock = MagicMock()
    mock_telemetry_client.event_queue = queue_mock

    try:
        raise ValueError("something broke")
    except ValueError as exc:
        with patch.object(
            mock_telemetry_client, "get_account_id", return_value="111122223333"
        ), patch.object(api._telemetry, "get_boto3_session"):
            # WHEN
            mock_telemetry_client.record_error_with_trace(exc, "test_scope")

    # THEN
    queue_mock.put_nowait.assert_called_once()
    event: TelemetryEvent = queue_mock.put_nowait.call_args[0][0]
    assert event.event_type == "com.amazon.rum.deadline.error"
    assert event.event_details["exception_type"] == "ValueError"
    assert event.event_details["exception_scope"] == "test_scope"
    assert event.event_details["message"] == "something broke"
    assert "ValueError: something broke" in event.event_details["stack_trace"]
    assert event.event_details["usage_mode"] == "CLI"
    assert event.event_details["accountId"] == "111122223333"


def test_record_error_with_trace_extra_details(fresh_deadline_config, mock_telemetry_client):
    """Test that extra_details are merged into the event"""
    # GIVEN
    queue_mock = MagicMock()
    mock_telemetry_client.event_queue = queue_mock

    try:
        raise RuntimeError("fail")
    except RuntimeError as exc:
        with patch.object(
            mock_telemetry_client, "get_account_id", return_value="111122223333"
        ), patch.object(api._telemetry, "get_boto3_session"):
            # WHEN
            mock_telemetry_client.record_error_with_trace(
                exc, "cli", extra_details={"command": "bundle submit"}
            )

    # THEN
    event: TelemetryEvent = queue_mock.put_nowait.call_args[0][0]
    assert event.event_details["command"] == "bundle submit"
    assert event.event_details["exception_type"] == "RuntimeError"


def test_record_error_with_trace_sanitizes_message(fresh_deadline_config, mock_telemetry_client):
    """Test that customer paths in exception messages are sanitized"""
    # GIVEN
    queue_mock = MagicMock()
    mock_telemetry_client.event_queue = queue_mock

    try:
        raise FileNotFoundError(
            "[Errno 2] No such file or directory: '/home/customer/secret/render.py'"
        )
    except FileNotFoundError as exc:
        with patch.object(
            mock_telemetry_client, "get_account_id", return_value="111122223333"
        ), patch.object(api._telemetry, "get_boto3_session"):
            # WHEN
            mock_telemetry_client.record_error_with_trace(exc, "test")

    # THEN
    event: TelemetryEvent = queue_mock.put_nowait.call_args[0][0]
    assert "customer" not in event.event_details["message"]
    assert "secret" not in event.event_details["message"]
    assert "render.py" in event.event_details["message"]


def test_record_error_with_trace_sanitizes_paths(fresh_deadline_config, mock_telemetry_client):
    """Test that customer paths are stripped from the stack trace"""
    # GIVEN
    queue_mock = MagicMock()
    mock_telemetry_client.event_queue = queue_mock

    try:
        raise TypeError("bad type")
    except TypeError as exc:
        with patch.object(
            mock_telemetry_client, "get_account_id", return_value="111122223333"
        ), patch.object(api._telemetry, "get_boto3_session"):
            # WHEN
            mock_telemetry_client.record_error_with_trace(exc, "test")

    # THEN
    event: TelemetryEvent = queue_mock.put_nowait.call_args[0][0]
    stack_trace = event.event_details["stack_trace"]
    # The stack trace should not contain the full absolute path to this test file
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
        expected_summary["accountId"] = "111122223333"
        expected_event = TelemetryEvent(
            event_type="com.amazon.rum.deadline.successful",
            event_details=expected_summary,
        )
        telemetry_client = get_deadline_cloud_library_telemetry_client()
        telemetry_client.event_queue = queue_mock

        with patch.object(
            api.TelemetryClient, "get_account_id", return_value="111122223333"
        ), patch.object(api._telemetry, "get_boto3_session"):

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
        expected_summary["accountId"] = "111122223333"
        expected_event = TelemetryEvent(
            event_type="com.amazon.rum.deadline.fails",
            event_details=expected_summary,
        )
        telemetry_client = get_deadline_cloud_library_telemetry_client()
        telemetry_client.event_queue = queue_mock

        with patch.object(
            api.TelemetryClient, "get_account_id", return_value="111122223333"
        ), patch.object(api._telemetry, "get_boto3_session"):

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
    with patch.object(
        api._telemetry, "get_deadline_endpoint_url", side_effect=["https://fake-endpoint-url"]
    ), patch.object(time, "perf_counter_ns", return_value=0):
        # GIVEN
        queue_mock = MagicMock()
        expected_summary: Dict[str, Any] = dict()
        expected_summary["latency"] = 0
        expected_summary["function_call"] = "test_call"
        expected_summary["usage_mode"] = "CLI"
        expected_summary["accountId"] = "111122223333"
        expected_event = TelemetryEvent(
            event_type="com.amazon.rum.deadline.latency",
            event_details=expected_summary,
        )
        telemetry_client = get_deadline_cloud_library_telemetry_client()
        telemetry_client.event_queue = queue_mock

        with patch.object(
            api.TelemetryClient, "get_account_id", return_value="111122223333"
        ), patch.object(api._telemetry, "get_boto3_session"):

            @record_function_latency_telemetry_event()
            def test_call():
                return

            # WHEN
            test_call()  # type:ignore

        # THEN
        queue_mock.put_nowait.assert_called_once_with(expected_event)


# --- Stack trace sanitizer tests ---


class TestSanitizePath:
    def test_known_package_deadline(self):
        assert (
            _sanitize_path(
                "/home/customer/secret/venv/lib/python3.11/site-packages/deadline/client/api/_telemetry.py"
            )
            == "deadline/client/api/_telemetry.py"
        )

    def test_known_package_openjd(self):
        assert _sanitize_path("/opt/libs/openjd/sessions/runner.py") == "openjd/sessions/runner.py"

    def test_known_package_botocore(self):
        assert (
            _sanitize_path("/usr/lib/python3/dist-packages/botocore/client.py")
            == "botocore/client.py"
        )

    def test_site_packages_unknown_lib(self):
        assert (
            _sanitize_path("/home/user/venv/lib/python3.11/site-packages/somelib/core.py")
            == "somelib/core.py"
        )

    def test_customer_script_returns_filename_only(self):
        assert _sanitize_path("/home/customer/my-bucket-name/scripts/render.py") == "render.py"

    def test_windows_path(self):
        assert (
            _sanitize_path(
                "C:\\Users\\customer\\AppData\\Local\\deadline\\client\\api\\_telemetry.py"
            )
            == "deadline/client/api/_telemetry.py"
        )

    def test_frozen_module(self):
        assert _sanitize_path("<frozen importlib._bootstrap>") == "<frozen importlib._bootstrap>"

    def test_string_input(self):
        assert _sanitize_path("<string>") == "<string>"


SAMPLE_TB = """Traceback (most recent call last):
 File "/home/jsmith/renders/s3-bucket-acme/venv/lib/python3.11/site-packages/deadline/client/api/_telemetry.py", line 42, in record_event
   self._send(event)
 File "/home/jsmith/renders/s3-bucket-acme/custom_scripts/submit.py", line 10, in main
   client.submit_job()
 File "/home/jsmith/renders/s3-bucket-acme/venv/lib/python3.11/site-packages/botocore/client.py", line 530, in _api_call
   return self._make_api_call(operation_name, kwargs)
ValueError: something went wrong"""


class TestSanitizeTracebackString:
    def test_known_packages_preserved(self):
        result = sanitize_traceback_string(SAMPLE_TB)
        assert '"deadline/client/api/_telemetry.py", line 42, in record_event' in result
        assert '"botocore/client.py", line 530, in _api_call' in result

    def test_customer_path_stripped(self):
        result = sanitize_traceback_string(SAMPLE_TB)
        assert '"submit.py", line 10, in main' in result

    def test_no_customer_data_leaked(self):
        result = sanitize_traceback_string(SAMPLE_TB)
        assert "jsmith" not in result
        assert "s3-bucket-acme" not in result
        assert "/home/" not in result

    def test_error_message_preserved(self):
        result = sanitize_traceback_string(SAMPLE_TB)
        assert "ValueError: something went wrong" in result

    def test_structure_preserved(self):
        result = sanitize_traceback_string(SAMPLE_TB)
        assert "Traceback (most recent call last):" in result
        assert "   self._send(event)" in result


class TestSanitizeException:
    def test_live_exception(self):
        try:
            raise RuntimeError("test error")
        except RuntimeError as e:
            result = sanitize_exception(e)
            assert "RuntimeError: test error" in result

    def test_no_absolute_paths(self):
        try:
            raise RuntimeError("path test")
        except RuntimeError as e:
            result = sanitize_exception(e)
            for line in result.splitlines():
                if line.strip().startswith('File "'):
                    path = line.split('"')[1]
                    assert not path.startswith("/"), f"Absolute path leaked: {path}"


class TestSanitizeMessage:
    def test_single_quoted_unix_path(self):
        msg = "[Errno 2] No such file or directory: '/home/customer/secret/render.py'"
        result = sanitize_message(msg)
        assert "customer" not in result
        assert "secret" not in result
        assert "'render.py'" in result

    def test_double_quoted_unix_path(self):
        msg = 'Permission denied: "/mnt/customer-bucket/output.exr"'
        result = sanitize_message(msg)
        assert "customer-bucket" not in result
        assert '"output.exr"' in result

    def test_windows_path(self):
        msg = "Cannot open 'C:\\Users\\customer\\Documents\\scene.blend'"
        result = sanitize_message(msg)
        assert "customer" not in result
        assert "'scene.blend'" in result

    def test_known_package_preserved(self):
        msg = "Error in '/home/user/venv/lib/python3.11/site-packages/deadline/client/api/_telemetry.py'"
        result = sanitize_message(msg)
        assert "user" not in result
        assert "deadline/client/api/_telemetry.py" in result

    def test_no_path_unchanged(self):
        msg = "ValueError: something went wrong"
        assert sanitize_message(msg) == msg

    def test_unquoted_unix_path(self):
        msg = "Failed to process /home/user/job/input.txt"
        result = sanitize_message(msg)
        # Unquoted paths are not sanitized — Python exceptions typically quote paths
        assert result == msg
