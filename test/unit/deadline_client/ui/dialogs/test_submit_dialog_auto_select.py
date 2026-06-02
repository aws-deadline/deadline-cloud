# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
pytest-qt tests for auto-selection of the default farm/queue in
``SubmitJobToDeadlineDialog``.

Auto-select is a small stateful workflow: a background ``AsyncTaskRunner`` task
lists farms (and then queues), and a main-thread slot applies the result. These
tests cover both the happy paths and the awkward event sequences (stale results,
concurrent triggers, errors) that a threaded workflow can hit.
"""

from configparser import ConfigParser
from unittest.mock import MagicMock, patch

import pytest

try:
    from qtpy.QtWidgets import QWidget
    from deadline.client.ui.controllers._thread_pool import DeadlineThreadPool
    from deadline.client.ui.dataclasses import JobBundleSettings
    from deadline.client.job_bundle.submission import AssetReferences
    from deadline.client.config import get_setting, set_setting
    from deadline.client.config.config_file import (
        _SETTING_FARM_ID as SETTING_FARM_ID,
        _SETTING_QUEUE_ID as SETTING_QUEUE_ID,
    )
except ImportError:
    pytest.importorskip("deadline.client.ui.dialogs.submit_job_to_deadline_dialog")

DIALOG_MODULE = "deadline.client.ui.dialogs.submit_job_to_deadline_dialog"


@pytest.fixture(autouse=True)
def _reset_thread_pool():
    """Reset the shared thread pool around each test for isolation."""
    DeadlineThreadPool.reset()
    yield
    DeadlineThreadPool.shutdown(wait_for_done=True, timeout_ms=2000)
    DeadlineThreadPool.reset()


@pytest.fixture(autouse=True)
def _restore_auth_singleton():
    """Restore the DeadlineAuthenticationStatus singleton after each test.

    _build_dialog swaps in a stub auth status via the module-global; without
    restoring it, the stub leaks into other test files (e.g. config dialog tests
    that expect a real auth status with signals) under random test ordering.
    """
    import deadline.client.ui.deadline_authentication_status as auth_module

    saved = auth_module._deadline_authentication_status
    yield
    auth_module._deadline_authentication_status = saved


class MockJobSettingsWidget(QWidget):
    """A mock job settings widget that is a real QWidget."""

    def __init__(self, initial_settings=None, parent=None):
        super().__init__(parent)
        self.initial_settings = initial_settings
        self.parameter_changed = MagicMock()
        self.parameter_changed.connect = MagicMock()

    def update_settings(self, settings):
        pass


class _AuthStatusStub:
    """Mutable stand-in for DeadlineAuthenticationStatus.

    ``api_availability`` is a writable attribute so a test can build the dialog
    with the API "unavailable" (so construction does not kick off a real
    auto-select against live credentials) and then flip it to ``True``.
    """

    def __init__(self, api_availability):
        self.api_availability = api_availability
        self.creds_source = None
        self.auth_status = None
        self.config = ConfigParser()
        for signal_name in (
            "api_availability_changed",
            "creds_source_changed",
            "auth_status_changed",
        ):
            sig = MagicMock()
            sig.connect = MagicMock()
            setattr(self, signal_name, sig)


def _build_dialog(qtbot, auth_status):
    """Construct a SubmitJobToDeadlineDialog wired to the mock auth status.

    The dialog is always built with the API unavailable so construction does not
    start an auto-select task; tests opt in by setting
    ``auth_status.api_availability = True`` afterwards.

    The ``_auto_select_complete`` signal is disconnected from the heavy
    ``refresh_deadline_settings`` slot and rewired to a spy, so each test
    exercises auto-select in isolation without cascading API calls.
    """
    import deadline.client.ui.deadline_authentication_status as auth_module

    build_availability = auth_status.api_availability
    auth_status.api_availability = None  # quiet construction
    auth_module._deadline_authentication_status = auth_status

    with patch(
        "deadline.client.ui.widgets.deadline_authentication_status_widget.DeadlineAuthenticationStatus.getInstance",
        return_value=auth_status,
    ), patch(
        f"{DIALOG_MODULE}.DeadlineAuthenticationStatus.getInstance",
        return_value=auth_status,
    ):
        from deadline.client.ui.dialogs.submit_job_to_deadline_dialog import (
            SubmitJobToDeadlineDialog,
        )

        dialog = SubmitJobToDeadlineDialog(
            job_setup_widget_type=MockJobSettingsWidget,
            initial_job_settings=JobBundleSettings(browse_enabled=True),
            initial_shared_parameter_values={},
            auto_detected_attachments=AssetReferences(),
            attachments=AssetReferences(),
            on_create_job_bundle_callback=MagicMock(),
        )
    qtbot.addWidget(dialog)

    # Isolate auto-select: drop the refresh wiring and observe the signal directly.
    try:
        dialog._auto_select_complete.disconnect()
    except (TypeError, RuntimeError):
        pass
    complete_spy = MagicMock()
    dialog._auto_select_complete.connect(complete_spy)

    # Restore the availability the test asked for, now that construction is done.
    auth_status.api_availability = build_availability
    return dialog, complete_spy


def _wait_for_auto_select(qtbot, dialog):
    """Wait until the in-flight auto-select task has run and its slots delivered.

    We wait on the thread pool's own active-thread accounting (reliable) rather
    than ``AsyncTaskRunner.is_running`` (whose cleanup is itself a queued slot),
    then flush the Qt event loop so the queued result/error slot runs.
    """
    qtbot.waitUntil(lambda: DeadlineThreadPool.active_thread_count() == 0, timeout=3000)
    qtbot.wait(100)


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------


def test_auto_selects_single_farm_and_queue(qtbot, fresh_deadline_config):
    """One farm + one queue: both are auto-selected end-to-end via the runner."""
    auth = _AuthStatusStub(True)
    dialog, complete_spy = _build_dialog(qtbot, auth)

    with patch(
        f"{DIALOG_MODULE}.api.list_farms", return_value={"farms": [{"farmId": "farm-1"}]}
    ), patch(f"{DIALOG_MODULE}.api.list_queues", return_value={"queues": [{"queueId": "queue-1"}]}):
        dialog._auto_select_defaults()
        _wait_for_auto_select(qtbot, dialog)

    assert get_setting(SETTING_FARM_ID) == "farm-1"
    assert get_setting(SETTING_QUEUE_ID) == "queue-1"
    complete_spy.assert_called_once()


def test_auto_selects_queue_when_farm_already_configured(qtbot, fresh_deadline_config):
    """Farm already set: only the queue is selected, and list_farms is not called."""
    set_setting(SETTING_FARM_ID, "farm-preset")
    auth = _AuthStatusStub(True)
    dialog, complete_spy = _build_dialog(qtbot, auth)

    with patch(f"{DIALOG_MODULE}.api.list_farms") as mock_farms, patch(
        f"{DIALOG_MODULE}.api.list_queues", return_value={"queues": [{"queueId": "queue-9"}]}
    ) as mock_queues:
        dialog._auto_select_defaults()
        _wait_for_auto_select(qtbot, dialog)

    mock_farms.assert_not_called()
    mock_queues.assert_called_once_with(farmId="farm-preset")
    assert get_setting(SETTING_FARM_ID) == "farm-preset"
    assert get_setting(SETTING_QUEUE_ID) == "queue-9"
    complete_spy.assert_called_once()


# ---------------------------------------------------------------------------
# No-op paths
# ---------------------------------------------------------------------------


def test_multiple_farms_does_not_select(qtbot, fresh_deadline_config):
    """More than one farm: nothing is selected and no queue lookup happens."""
    auth = _AuthStatusStub(True)
    dialog, complete_spy = _build_dialog(qtbot, auth)

    with patch(
        f"{DIALOG_MODULE}.api.list_farms",
        return_value={"farms": [{"farmId": "farm-1"}, {"farmId": "farm-2"}]},
    ), patch(f"{DIALOG_MODULE}.api.list_queues") as mock_queues:
        dialog._auto_select_defaults()
        _wait_for_auto_select(qtbot, dialog)

    mock_queues.assert_not_called()
    assert get_setting(SETTING_FARM_ID) == ""
    assert get_setting(SETTING_QUEUE_ID) == ""
    complete_spy.assert_not_called()


def test_multiple_queues_selects_farm_only(qtbot, fresh_deadline_config):
    """One farm but multiple queues: farm is selected, queue is left unset."""
    auth = _AuthStatusStub(True)
    dialog, complete_spy = _build_dialog(qtbot, auth)

    with patch(
        f"{DIALOG_MODULE}.api.list_farms", return_value={"farms": [{"farmId": "farm-1"}]}
    ), patch(
        f"{DIALOG_MODULE}.api.list_queues",
        return_value={"queues": [{"queueId": "queue-1"}, {"queueId": "queue-2"}]},
    ):
        dialog._auto_select_defaults()
        _wait_for_auto_select(qtbot, dialog)

    assert get_setting(SETTING_FARM_ID) == "farm-1"
    assert get_setting(SETTING_QUEUE_ID) == ""
    complete_spy.assert_called_once()


def test_no_farms_does_not_select(qtbot, fresh_deadline_config):
    """Zero farms: nothing selected."""
    auth = _AuthStatusStub(True)
    dialog, complete_spy = _build_dialog(qtbot, auth)

    with patch(f"{DIALOG_MODULE}.api.list_farms", return_value={"farms": []}), patch(
        f"{DIALOG_MODULE}.api.list_queues"
    ) as mock_queues:
        dialog._auto_select_defaults()
        _wait_for_auto_select(qtbot, dialog)

    mock_queues.assert_not_called()
    assert get_setting(SETTING_FARM_ID) == ""
    complete_spy.assert_not_called()


def test_no_api_calls_when_api_unavailable(qtbot, fresh_deadline_config):
    """When the API is not available, auto-select does nothing."""
    auth = _AuthStatusStub(None)
    dialog, complete_spy = _build_dialog(qtbot, auth)

    with patch(f"{DIALOG_MODULE}.api.list_farms") as mock_farms:
        dialog._auto_select_defaults()
        qtbot.wait(50)

    mock_farms.assert_not_called()
    assert not dialog._auto_select_runner.is_running(dialog._AUTO_SELECT_OPERATION_KEY)
    complete_spy.assert_not_called()


def test_no_api_calls_when_already_configured(qtbot, fresh_deadline_config):
    """When farm and queue are both already set, auto-select does nothing."""
    set_setting(SETTING_FARM_ID, "farm-x")
    set_setting(SETTING_QUEUE_ID, "queue-y")
    auth = _AuthStatusStub(True)
    dialog, complete_spy = _build_dialog(qtbot, auth)

    with patch(f"{DIALOG_MODULE}.api.list_farms") as mock_farms, patch(
        f"{DIALOG_MODULE}.api.list_queues"
    ) as mock_queues:
        dialog._auto_select_defaults()
        qtbot.wait(50)

    mock_farms.assert_not_called()
    mock_queues.assert_not_called()
    complete_spy.assert_not_called()


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


def test_api_error_is_handled_gracefully(qtbot, fresh_deadline_config):
    """An API error must not crash the dialog or change any setting."""
    auth = _AuthStatusStub(True)
    dialog, complete_spy = _build_dialog(qtbot, auth)

    with patch(f"{DIALOG_MODULE}.api.list_farms", side_effect=Exception("boom")):
        dialog._auto_select_defaults()
        _wait_for_auto_select(qtbot, dialog)

    assert get_setting(SETTING_FARM_ID) == ""
    assert get_setting(SETTING_QUEUE_ID) == ""
    complete_spy.assert_not_called()


# ---------------------------------------------------------------------------
# Race / unexpected event sequences
# ---------------------------------------------------------------------------


def test_does_not_start_second_run_while_in_flight(qtbot, fresh_deadline_config):
    """A second trigger while a task is in flight must not start another task."""
    auth = _AuthStatusStub(True)
    dialog, _ = _build_dialog(qtbot, auth)

    with patch.object(dialog._auto_select_runner, "is_running", return_value=True), patch.object(
        dialog._auto_select_runner, "run"
    ) as mock_run:
        dialog._auto_select_defaults()

    mock_run.assert_not_called()


def test_stale_farm_result_discarded_when_farm_already_set(qtbot, fresh_deadline_config):
    """A farm resolved while none was set must not overwrite a farm set in the meantime."""
    set_setting(SETTING_FARM_ID, "farm-current")
    auth = _AuthStatusStub(True)
    dialog, complete_spy = _build_dialog(qtbot, auth)

    # Result computed when no farm was configured yet.
    dialog._on_auto_select_resolved(
        {"farm_id": "farm-stale", "queue_id": None, "queue_farm_id": None}
    )

    assert get_setting(SETTING_FARM_ID) == "farm-current"
    complete_spy.assert_not_called()


def test_stale_queue_result_discarded_when_farm_changed(qtbot, fresh_deadline_config):
    """A queue resolved for an old farm must not be applied after the farm changed.

    This is the event sequence behind the reported ResourceNotFoundException: a
    queue belonging to farm A must never be written while farm B is configured.
    """
    set_setting(SETTING_FARM_ID, "farm-B")
    auth = _AuthStatusStub(True)
    dialog, complete_spy = _build_dialog(qtbot, auth)

    # Queue was resolved under farm-A, but farm-B is now configured.
    dialog._on_auto_select_resolved(
        {"farm_id": None, "queue_id": "queue-from-A", "queue_farm_id": "farm-A"}
    )

    assert get_setting(SETTING_QUEUE_ID) == ""
    complete_spy.assert_not_called()


def test_queue_result_applied_when_farm_still_matches(qtbot, fresh_deadline_config):
    """A queue resolved for the still-current farm is applied normally."""
    set_setting(SETTING_FARM_ID, "farm-A")
    auth = _AuthStatusStub(True)
    dialog, complete_spy = _build_dialog(qtbot, auth)

    dialog._on_auto_select_resolved(
        {"farm_id": None, "queue_id": "queue-A", "queue_farm_id": "farm-A"}
    )

    assert get_setting(SETTING_QUEUE_ID) == "queue-A"
    complete_spy.assert_called_once()


def test_queue_not_overwritten_when_already_set(qtbot, fresh_deadline_config):
    """An already-configured queue must not be overwritten by a resolved result."""
    set_setting(SETTING_FARM_ID, "farm-A")
    set_setting(SETTING_QUEUE_ID, "queue-existing")
    auth = _AuthStatusStub(True)
    dialog, complete_spy = _build_dialog(qtbot, auth)

    dialog._on_auto_select_resolved(
        {"farm_id": None, "queue_id": "queue-new", "queue_farm_id": "farm-A"}
    )

    assert get_setting(SETTING_QUEUE_ID) == "queue-existing"
    complete_spy.assert_not_called()
