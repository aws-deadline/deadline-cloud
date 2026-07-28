# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
Example-based tests for the stack trace sanitizer.

These complement the property-based fuzz tests in
``test_stack_trace_sanitizer_fuzz.py`` with hand-written traces that pin down
specific behaviors — in particular that a *customer* directory that merely
happens to share a name with one of our framework packages (e.g. a project
tree at ``~/deadline/...``) is NOT mistaken for the installed package and does
not leak customer path segments into telemetry.
"""

import traceback

import pytest

from deadline.client.api._stack_trace_sanitizer import (
    _sanitize_path,
    _sanitize_traceback,
    sanitize_exception,
)


class TestSanitizePathKeepsFrameworkPaths:
    """Genuine installed-package frames must stay recognized (package-relative)."""

    def test_site_packages_deadline_is_kept_relative(self):
        raw = "/home/user/.venv/lib/python3.11/site-packages/deadline/client/api/foo.py"
        assert _sanitize_path(raw) == "deadline/client/api/foo.py"

    def test_site_packages_third_party_is_kept_relative(self):
        raw = "/opt/venv/lib/python3.9/site-packages/botocore/client.py"
        assert _sanitize_path(raw) == "botocore/client.py"

    def test_windows_site_packages_deadline_is_kept_relative(self):
        raw = r"C:\Python311\Lib\site-packages\deadline\client\api\foo.py"
        assert _sanitize_path(raw) == "deadline/client/api/foo.py"

    def test_debian_dist_packages_is_kept_relative(self):
        raw = "/usr/lib/python3/dist-packages/somelib/core.py"
        assert _sanitize_path(raw) == "somelib/core.py"

    def test_windows_user_site_packages_is_kept_relative(self):
        raw = r"C:\Users\bob\AppData\Roaming\Python\Python311\site-packages\somelib\core.py"
        assert _sanitize_path(raw) == "somelib/core.py"

    @pytest.mark.parametrize(
        "raw, expected",
        [
            # System-wide python.org installer (all-users): under Program
            # Files, which contains a space — the anchor must still hold.
            pytest.param(
                r"C:\Program Files\Python311\Lib\site-packages\botocore\client.py",
                "botocore/client.py",
                id="win_system_wide_program_files",
            ),
            pytest.param(
                r"C:\Program Files\Python311\Lib\site-packages\deadline\client\api\foo.py",
                "deadline/client/api/foo.py",
                id="win_system_wide_program_files_deadline",
            ),
            # Microsoft Store Python: WindowsApps install root.
            pytest.param(
                r"C:\Program Files\WindowsApps\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0"
                r"\Lib\site-packages\somelib\core.py",
                "somelib/core.py",
                id="win_ms_store_python",
            ),
            # A customer-named "deadline" directory earlier in the SAME path
            # must not defeat (or extend) the genuine system-wide anchor.
            pytest.param(
                r"D:\deadline\Python311\Lib\site-packages\somelib\core.py",
                "somelib/core.py",
                id="win_customer_deadline_dir_above_real_site_packages",
            ),
        ],
    )
    def test_windows_non_venv_layouts_are_kept_relative(self, raw, expected):
        """Windows system-wide / Store / per-user (non-venv) interpreter
        layouts must keep genuine package frames package-relative while
        never emitting customer path segments above site-packages."""
        sanitized = _sanitize_path(raw)
        assert sanitized == expected
        for marker in ("Program Files", "WindowsApps", "D:", "C:"):
            assert marker not in sanitized


class TestSanitizePathRedactsCustomerDeadlineDir:
    """A customer directory literally named ``deadline`` must not leak."""

    def test_customer_deadline_dir_is_redacted_to_bare_filename(self):
        # Customer's own project tree; the ``deadline`` segment here is a
        # customer directory, NOT the installed package.
        raw = "/home/user/deadline/secret_project/file.py"
        sanitized = _sanitize_path(raw)
        assert "secret_project" not in sanitized
        assert sanitized == "file.py"

    def test_windows_customer_deadline_dir_is_redacted(self):
        raw = r"C:\Users\bob\deadline\secret_project\file.py"
        sanitized = _sanitize_path(raw)
        assert "secret_project" not in sanitized
        assert sanitized == "file.py"

    def test_customer_dir_named_site_packages_is_redacted(self):
        # A customer directory literally named "site-packages" that is NOT
        # under a lib/pythonX.Y parent must not anchor the trim — everything
        # below it is customer content.
        raw = "/home/artist/site-packages/ClientSecretShow/tool.py"
        sanitized = _sanitize_path(raw)
        assert "ClientSecretShow" not in sanitized
        assert sanitized == "tool.py"

    def test_customer_dir_named_dist_packages_is_redacted(self):
        raw = "/home/artist/dist-packages/ClientSecretShow/tool.py"
        sanitized = _sanitize_path(raw)
        assert "ClientSecretShow" not in sanitized
        assert sanitized == "tool.py"

    def test_windows_customer_dir_named_site_packages_is_redacted(self):
        raw = r"C:\Users\bob\site-packages\ClientSecretShow\tool.py"
        sanitized = _sanitize_path(raw)
        assert "ClientSecretShow" not in sanitized
        assert sanitized == "tool.py"

    def test_customer_deadline_dir_in_rendered_traceback_is_redacted(self):
        # End-to-end: fabricate a TracebackException whose frame filename is a
        # customer path with a ``deadline`` directory, and confirm the customer
        # segments do not appear in the sanitized render.
        try:
            raise RuntimeError("boom")
        except RuntimeError as e:
            te = traceback.TracebackException.from_exception(e)

        te.stack[0].filename = "/home/user/deadline/secret_project/file.py"
        rendered = "\n".join(_sanitize_traceback(te))
        assert "secret_project" not in rendered


def test_customer_deadline_dir_via_live_exception_is_redacted():
    """sanitize_exception on a live exception raised from a customer path
    named ``deadline`` must not echo customer-specific path segments."""
    try:
        raise ValueError("boom")
    except ValueError as e:
        te = traceback.TracebackException.from_exception(e)
        # Overwrite the innermost frame's filename to simulate the exception
        # originating from a customer directory named ``deadline``.
        te.stack[-1].filename = "/home/user/deadline/secret_project/customer.py"
        rendered = "\n".join(_sanitize_traceback(te))
    assert "secret_project" not in rendered


def test_sanitize_exception_is_callable():
    """Smoke test that the public entrypoint runs end-to-end without raising."""
    try:
        raise RuntimeError("boom")
    except RuntimeError as e:
        rendered = sanitize_exception(e)
    assert "RuntimeError" in rendered
