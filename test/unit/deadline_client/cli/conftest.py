# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Shared fixtures for the `deadline` CLI unit tests."""

from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def stdout_is_tty(request):
    """
    Forces the CLI to treat stdout as an interactive terminal for all CLI tests.

    Several commands auto-detect their default ``--output`` format from whether
    stdout is a TTY: verbose for an interactive terminal, json otherwise. The click
    ``CliRunner`` used by these tests captures stdout into a non-TTY buffer, which
    would flip the default to json. Most of the existing CLI tests predate that
    auto-detection and assert the human-readable (verbose) output, so this autouse
    fixture pins stdout to a TTY to preserve that baseline.

    This does not weaken coverage of the auto-detection: tests that pass an explicit
    ``--output`` value are unaffected (an explicit value always wins), and the tests
    in ``test_cli_output_format.py`` patch ``_stdout_is_tty`` directly to exercise
    both the TTY and non-TTY default-resolution paths.

    A test can opt out of the patch (e.g. to test ``_stdout_is_tty`` itself) with the
    ``@pytest.mark.real_stdout_isatty`` marker.
    """
    if request.node.get_closest_marker("real_stdout_isatty"):
        yield
        return
    with patch("deadline.client.cli._common._stdout_is_tty", return_value=True):
        yield


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "real_stdout_isatty: do not patch _stdout_is_tty for this test "
        "(lets a test exercise the real TTY-detection helper).",
    )
