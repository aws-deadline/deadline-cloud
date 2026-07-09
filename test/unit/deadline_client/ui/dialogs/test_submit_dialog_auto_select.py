# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""pytest-qt tests for the ``SubmitJobToDeadlineDialog`` farm/queue selection flow.

Selection of the default farm/queue is owned by the ``DeadlineUIController`` and
driven by the tab's combo boxes: when a resource list resolves to a single entry
the combo auto-selects it and emits ``user_selected``, which the controller
persists and cascades (farm -> queue -> storage). There is no separate
dialog-level background auto-select; these tests verify the dialog integrates
with that single controller-driven path.
"""

from configparser import ConfigParser
from unittest.mock import MagicMock, patch

import pytest

try:
    from qtpy.QtWidgets import QApplication, QWidget
    from deadline.client.ui.controllers._deadline_controller import DeadlineUIController
    from deadline.client.ui.controllers._thread_pool import DeadlineThreadPool
    from deadline.client.ui.dataclasses import JobBundleSettings
    from deadline.client.job_bundle.submission import AssetReferences
    from deadline.client.config import get_setting
    from deadline.client.config.config_file import (
        _SETTING_FARM_ID as SETTING_FARM_ID,
        _SETTING_QUEUE_ID as SETTING_QUEUE_ID,
    )
except ImportError:
    pytest.importorskip("deadline.client.ui.dialogs.submit_job_to_deadline_dialog")

DIALOG_MODULE = "deadline.client.ui.dialogs.submit_job_to_deadline_dialog"


@pytest.fixture(autouse=True)
def _reset_thread_pool():
    """Reset the shared thread pool and UI controller around each test."""
    DeadlineUIController.resetInstance()
    DeadlineThreadPool.reset()
    yield
    DeadlineUIController.resetInstance()
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
    with the API "unavailable" (so construction does not kick off a real refresh
    against live credentials) and then flip it to ``True``.
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
    kick off list refreshes; tests opt in by setting
    ``auth_status.api_availability = True`` afterwards.
    """
    import deadline.client.ui.deadline_authentication_status as auth_module

    build_availability = auth_status.api_availability
    auth_status.api_availability = None  # quiet construction
    auth_module._deadline_authentication_status = auth_status

    with (
        patch(
            "deadline.client.ui.widgets.deadline_authentication_status_widget.DeadlineAuthenticationStatus.getInstance",
            return_value=auth_status,
        ),
        patch(
            f"{DIALOG_MODULE}.DeadlineAuthenticationStatus.getInstance",
            return_value=auth_status,
        ),
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

    # Restore the availability the test asked for, now that construction is done.
    auth_status.api_availability = build_availability
    return dialog


# ---------------------------------------------------------------------------
# Auto-select now flows combo -> controller. Verify it through the full dialog.
# ---------------------------------------------------------------------------


def test_lone_farm_and_queue_auto_selected_through_dialog(qtbot, fresh_deadline_config):
    """A single farm + single queue are auto-selected and persisted end-to-end.

    refresh_deadline_settings refreshes the combos; the farm combo auto-selects
    the lone farm, the controller persists + cascades to queues, and the lone
    queue is auto-selected and persisted in turn.
    """
    auth = _AuthStatusStub(True)
    dialog = _build_dialog(qtbot, auth)

    def _one_farm_one_region(config=None):
        # Farms load through the multi-region streaming path (_iter_farms_by_region),
        # not a single api.list_farms call, so mock that generator with one region.
        yield (
            "us-west-2",
            [{"displayName": "Only Farm", "farmId": "farm-1", "region": "us-west-2"}],
            None,
        )

    with (
        patch(
            "deadline.client.ui.controllers._deadline_controller._iter_farms_by_region",
            side_effect=_one_farm_one_region,
        ),
        patch(
            "deadline.client.api.list_queues",
            return_value={"queues": [{"displayName": "Only Queue", "queueId": "queue-1"}]},
        ),
        patch(
            "deadline.client.api.list_storage_profiles_for_queue",
            return_value={"storageProfiles": []},
        ),
    ):
        dialog.refresh_deadline_settings()
        # The cascade is multi-hop and async (farm list -> auto-select farm ->
        # queue list -> auto-select queue), so wait until both are persisted rather
        # than for a single signal.
        qtbot.waitUntil(
            lambda: (
                get_setting(SETTING_FARM_ID) == "farm-1"
                and get_setting(SETTING_QUEUE_ID) == "queue-1"
            ),
            timeout=5000,
        )

    assert get_setting(SETTING_FARM_ID) == "farm-1"
    assert get_setting(SETTING_QUEUE_ID) == "queue-1"


def test_multiple_farms_not_auto_selected_through_dialog(qtbot, fresh_deadline_config):
    """With more than one farm, nothing is auto-selected or persisted."""
    auth = _AuthStatusStub(True)
    dialog = _build_dialog(qtbot, auth)
    controller = DeadlineUIController.getInstance()

    def _two_farms_one_region(config=None):
        # Farms load through the multi-region streaming path; two farms in one region
        # means no unambiguous lone resource, so nothing auto-selects.
        yield (
            "us-west-2",
            [
                {"displayName": "Farm A", "farmId": "farm-a", "region": "us-west-2"},
                {"displayName": "Farm B", "farmId": "farm-b", "region": "us-west-2"},
            ],
            None,
        )

    with (
        patch(
            "deadline.client.ui.controllers._deadline_controller._iter_farms_by_region",
            side_effect=_two_farms_one_region,
        ),
        patch("deadline.client.api.list_queues", return_value={"queues": []}),
    ):
        # Wait for the streamed farm load to fully settle (farms_loading -> False), not
        # the initial farms_updated([]) clear, before asserting nothing was selected.
        with qtbot.waitSignal(
            controller.farms_loading,
            timeout=5000,
            check_params_cb=lambda loading: loading is False,
        ):
            dialog.refresh_deadline_settings()
        QApplication.processEvents()

    assert get_setting(SETTING_FARM_ID) == ""
    assert get_setting(SETTING_QUEUE_ID) == ""


def test_no_list_refresh_when_api_unavailable(qtbot, fresh_deadline_config):
    """When the API is not available, no list API calls are made."""
    auth = _AuthStatusStub(None)
    dialog = _build_dialog(qtbot, auth)

    with patch("deadline.client.api.list_farms") as mock_farms:
        dialog.refresh_deadline_settings()
        qtbot.wait(50)

    mock_farms.assert_not_called()
    assert get_setting(SETTING_FARM_ID) == ""


# ---------------------------------------------------------------------------
# A selection on the tab must not re-list the combos.
# ---------------------------------------------------------------------------


def test_tab_selection_change_does_not_refresh_farm_list(qtbot, fresh_deadline_config):
    """Selecting a queue (or farm) on the tab must not re-list the farm combo.

    A selection on the Shared job settings tab only needs to update the Submit
    button and reload queue parameters; re-listing the combos would cause the
    farm list to refresh whenever the user merely changes the queue.
    """
    auth = _AuthStatusStub(True)
    dialog = _build_dialog(qtbot, auth)

    settings_box = dialog.shared_job_settings.deadline_cloud_settings_box
    with (
        patch.object(settings_box.farm_box, "refresh_list") as farm_refresh,
        patch.object(settings_box.queue_box, "refresh_list") as queue_refresh,
        patch.object(dialog.shared_job_settings, "refresh_queue_parameters") as refresh_qp,
    ):
        dialog._on_deadline_cloud_selection_changed()

    farm_refresh.assert_not_called()
    queue_refresh.assert_not_called()
    # The selection change should still reload queue parameters.
    refresh_qp.assert_called_once()
