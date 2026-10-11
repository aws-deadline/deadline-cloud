# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Root test fixtures shared across the unit and cli_e2e suites."""

import pytest


def _stop_auth_status_poll_timer():
    """Stop the DeadlineAuthenticationStatus singleton's poll timer, if it exists.

    Reaches into the module global directly rather than getInstance() so we never
    *create* the singleton for a test that didn't otherwise need it.
    """
    try:
        import deadline.client.ui.deadline_authentication_status as auth_module
    except ImportError:
        # The UI module imports qtpy/PySide6 at module top; when the optional
        # "gui" extra is not installed there is no singleton and nothing to stop.
        # The GUI test conftest supports this no-PySide6 path, so don't break it.
        return

    status = auth_module._deadline_authentication_status
    if status is not None:
        timer = getattr(status, "_poll_timer", None)
        if timer is not None:
            timer.stop()
        status._poll_subscribers = 0


@pytest.fixture(autouse=True)
def _disable_auth_status_polling():
    """Ensure no test runs with the auth-status poll timer active.

    DeadlineAuthenticationStatus is a process-wide singleton. A live auth-status
    widget starts a QTimer that periodically runs check_authentication_status on
    a background thread (building a boto3 client). Under pytest-xdist (shared
    workers) that timer can outlive the test that started it, and a probe firing
    during a later test builds a boto3 client concurrently with the running test.

    Stopping the timer before and after every test — suite-wide, not just for UI
    tests — keeps the poll from running alongside an unrelated test, independent
    of Qt event-loop timing.
    """
    _stop_auth_status_poll_timer()
    yield
    _stop_auth_status_poll_timer()
