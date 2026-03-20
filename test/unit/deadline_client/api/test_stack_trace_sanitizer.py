# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""Tests for _stack_trace_sanitizer."""

from deadline.client.api._stack_trace_sanitizer import (
    _sanitize_path,
    sanitize_message,
    sanitize_traceback_string,
    sanitize_exception,
)


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
