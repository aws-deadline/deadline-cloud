# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""GUI tests for the editable farm/queue/storage-profile selectors on the Shared
job settings tab (``DeadlineCloudSettingsWidget``) using pytest-qt.

These mirror the controller-driven patterns in ``test_settings_dialogue.py`` but
exercise the new editable selectors on the submit dialog's tab rather than the
Settings dialog. Selections persist immediately to a real (temporary) config file
via ``fresh_deadline_config``, so the round-trip can be asserted with
``config.get_setting``.
"""

import contextlib
import sys
from unittest.mock import MagicMock, patch

import pytest
from qtpy.QtWidgets import QApplication

from deadline.client.config import config_file
from deadline.client.ui.controllers._deadline_controller import DeadlineUIController
from deadline.client.ui.controllers._thread_pool import DeadlineThreadPool
from deadline.client.ui.widgets.shared_job_settings_tab import DeadlineCloudSettingsWidget


@pytest.fixture(autouse=True)
def _reset_singletons():
    """Reset UI singletons before/after each test."""
    DeadlineUIController.resetInstance()
    DeadlineThreadPool.reset()
    yield
    DeadlineUIController.resetInstance()
    DeadlineThreadPool.shutdown(wait_for_done=True, timeout_ms=2000)
    DeadlineThreadPool.reset()


@pytest.fixture
def seeded_backend(mock_deadline_backend):
    """Seed a backend with one farm, one queue, and storage profiles for every OS."""
    backend = mock_deadline_backend
    farm = backend.create_farm(displayName="Test Farm", description="GUI test farm")
    farm_id = farm["farmId"]
    queue = backend.create_queue(farmId=farm_id, displayName="Test Queue")
    queue_id = queue["queueId"]
    for name, os_family in [
        ("Linux Storage Profile", "LINUX"),
        ("Windows Storage Profile", "WINDOWS"),
        ("macOS Storage Profile", "MACOS"),
    ]:
        backend.create_storage_profile(
            farmId=farm_id, queueId=queue_id, displayName=name, osFamily=os_family
        )
    return backend, farm_id, queue_id


# The single region all mock farms are reported under. Farm combo labels are
# region-first ("(us-west-2) Test Farm") per the (region, farm_id) convention.
MOCK_REGION = "us-west-2"


@pytest.fixture
def mock_api(seeded_backend):
    """Patch the list_* API functions the controller calls to use the mock backend."""
    backend, _, _ = seeded_backend
    deadline_mock = MagicMock()
    backend.set_mock_methods(deadline_mock)

    def _strip_config(fn):
        return lambda **kw: fn(**{k: v for k, v in kw.items() if k != "config"})

    def _fake_iter_farms_by_region(config=None):
        # The multi-region streaming path replaces a single api.list_farms call with a
        # per-region fan-out. Mirror it by yielding the backend's farms under one region;
        # each farm dict carries its region per the streaming contract.
        farms = [dict(farm, region=MOCK_REGION) for farm in deadline_mock.list_farms()["farms"]]
        yield (MOCK_REGION, farms, None)

    with contextlib.ExitStack() as stack:
        stack.enter_context(
            patch(
                "deadline.client.ui.controllers._deadline_controller._iter_farms_by_region",
                side_effect=_fake_iter_farms_by_region,
            )
        )
        stack.enter_context(
            patch(
                "deadline.client.api.list_farms",
                side_effect=_strip_config(deadline_mock.list_farms),
            )
        )
        stack.enter_context(
            patch(
                "deadline.client.api.list_queues",
                side_effect=_strip_config(deadline_mock.list_queues),
            )
        )
        stack.enter_context(
            patch(
                "deadline.client.api.list_storage_profiles_for_queue",
                side_effect=_strip_config(deadline_mock.list_storage_profiles_for_queue),
            )
        )
        yield deadline_mock


@pytest.fixture
def widget(qtbot, fresh_deadline_config, mock_api):
    """Build the DeadlineCloudSettingsWidget against the mock backend + temp config."""
    w = DeadlineCloudSettingsWidget()
    qtbot.addWidget(w)
    return w


def _items(combo):
    return [combo.itemText(i) for i in range(combo.count())]


def _has_label_containing(combo, text):
    """True if any combo item label contains *text*.

    Farm labels are region-first ("(us-west-2) Test Farm"), so an exact-match check
    on the bare display name would miss the region prefix.
    """
    return any(text in label for label in _items(combo))


def _wait_for_farm(qtbot, widget, farm_id):
    """Wait until a streamed farm has landed in the farm combo box with its real label.

    Farms stream in per region (``farms_appended``) after ``refresh_farms`` first
    emits ``farms_updated([])`` to clear, so a one-shot wait on ``farms_updated``
    returns on the empty clear before any farm arrives. Poll the combo instead.

    A farm that's the stored default also gets a *raw-id* placeholder row (text == the
    id) inserted by ``refresh_selected_id`` before its region streams in, so require the
    matched row to carry a real label (text != the raw id) — otherwise we'd return on the
    placeholder, before "(region) Display Name" arrives.
    """

    def _real_label_present():
        box = widget.farm_box.box
        index = box.findData(farm_id)
        return index >= 0 and box.itemText(index) != farm_id

    qtbot.waitUntil(_real_label_present, timeout=5000)
    QApplication.processEvents()


def _select_by_data(combo, data):
    """Simulate a user picking the entry whose itemData equals *data*.

    Sets the index AND emits ``activated`` (as a real dropdown pick does), because
    selection persistence is now driven by ``activated`` -> ``user_selected``, not
    by ``currentIndexChanged`` (which also fires on programmatic display-sync).
    """
    index = combo.findData(data)
    assert index >= 0, f"{data!r} not found in {_items(combo)}"
    combo.setCurrentIndex(index)
    combo.activated.emit(index)


def _configure_profile(profile_name, *, farm_id="", queue_id=""):
    """Write farm/queue defaults under *profile_name* (settings are profile-scoped)."""
    config_file.set_setting("defaults.aws_profile_name", profile_name)
    config_file.set_setting("defaults.farm_id", farm_id)
    config_file.set_setting("defaults.queue_id", queue_id)


def _switch_profile_and_refresh(qtbot, widget, profile_name):
    """Mimic the submit dialog's profile-switch: select the profile, then refresh.

    The dialog calls ``refresh_setting_controls`` (via ``refresh_deadline_settings``)
    after a profile switch; this drives the same path and waits for the async farm
    list to settle so the combo reflects the new profile.
    """
    config_file.set_setting("defaults.aws_profile_name", profile_name)
    controller = DeadlineUIController.getInstance()
    # Farms stream in after refresh_farms emits farms_updated([]) to clear, so wait for
    # the streaming load to finish (farms_loading -> False) rather than the empty clear.
    with qtbot.waitSignal(
        controller.farms_loading, timeout=5000, check_params_cb=lambda loading: loading is False
    ):
        widget.refresh_setting_controls(deadline_authorized=True)
    QApplication.processEvents()


class TestSelectorLayout:
    def test_farm_and_queue_selectors_present(self, widget):
        """The tab shows editable farm and queue combo boxes with refresh buttons."""
        assert widget.farm_box.box is not None
        assert widget.queue_box.box is not None
        assert widget.farm_box.refresh_button is not None
        assert widget.queue_box.refresh_button is not None

    def test_storage_profile_row_hidden_initially(self, widget):
        """Storage-profile row starts hidden until a queue with profiles is selected."""
        # Use isHidden() (the explicit hidden flag) rather than isVisible(), which is
        # always False in offscreen tests because the widget is never shown.
        assert widget.storage_profile_box.isHidden()
        assert widget.storage_profile_box_label.isHidden()


class TestDropdownPopulation:
    def test_farm_dropdown_populated_from_backend(self, qtbot, widget, seeded_backend):
        _, farm_id, _ = seeded_backend
        widget.refresh_setting_controls(deadline_authorized=True)
        widget.farm_box.refresh_list()
        _wait_for_farm(qtbot, widget, farm_id)
        assert _has_label_containing(widget.farm_box.box, "Test Farm")

    def test_queue_dropdown_populated_from_backend(self, qtbot, widget, seeded_backend):
        _, farm_id, _ = seeded_backend
        config_file.set_setting("defaults.farm_id", farm_id)
        controller = DeadlineUIController.getInstance()
        widget.refresh_setting_controls(deadline_authorized=True)
        with qtbot.waitSignal(controller.queues_updated, timeout=5000):
            widget.queue_box.refresh_list()
        QApplication.processEvents()
        assert "Test Queue" in _items(widget.queue_box.box)

    def test_storage_profile_dropdown_populated_from_backend(self, qtbot, widget, seeded_backend):
        _, farm_id, queue_id = seeded_backend
        config_file.set_setting("defaults.farm_id", farm_id)
        config_file.set_setting("defaults.queue_id", queue_id)
        controller = DeadlineUIController.getInstance()
        widget.refresh_setting_controls(deadline_authorized=True)
        with qtbot.waitSignal(controller.storage_profiles_updated, timeout=5000):
            widget.storage_profile_box.refresh_list()
        QApplication.processEvents()
        items = _items(widget.storage_profile_box.box)
        if sys.platform.startswith("linux"):
            assert "Linux Storage Profile" in items
        elif sys.platform.startswith("darwin"):
            assert "macOS Storage Profile" in items
        elif sys.platform.startswith("win"):
            assert "Windows Storage Profile" in items


class TestPersistence:
    def test_selecting_farm_persists_and_clears_stale_queue(self, qtbot, widget, seeded_backend):
        """Selecting a farm writes defaults.farm_id and clears the stale queue/storage.

        Uses a second farm that has no queues so the queue list can't auto-select a
        replacement — that isolates the "stale selections are cleared" behavior.
        """
        backend, _, _ = seeded_backend
        empty_farm_id = backend.create_farm(displayName="Empty Farm")["farmId"]
        # Pre-seed a stale queue/storage to prove they get cleared.
        config_file.set_setting("defaults.queue_id", "queue-stale")
        config_file.set_setting("settings.storage_profile_id", "sp-stale")

        widget.refresh_setting_controls(deadline_authorized=True)
        widget.farm_box.refresh_list()
        _wait_for_farm(qtbot, widget, empty_farm_id)

        with qtbot.waitSignal(widget.selection_changed, timeout=5000):
            _select_by_data(widget.farm_box.box, empty_farm_id)
        QApplication.processEvents()

        assert config_file.get_setting("defaults.farm_id") == empty_farm_id
        assert config_file.get_setting("defaults.queue_id") == ""
        assert config_file.get_setting("settings.storage_profile_id") == ""

    def test_selecting_queue_persists(self, qtbot, widget, seeded_backend):
        # Add a second queue so neither is auto-selected and we can pick a specific one.
        backend, farm_id, queue_id = seeded_backend
        queue_id_2 = backend.create_queue(farmId=farm_id, displayName="Second Queue")["queueId"]
        config_file.set_setting("defaults.farm_id", farm_id)

        controller = DeadlineUIController.getInstance()
        widget.refresh_setting_controls(deadline_authorized=True)
        with qtbot.waitSignal(controller.queues_updated, timeout=5000):
            widget.queue_box.refresh_list()
        QApplication.processEvents()

        with qtbot.waitSignal(widget.selection_changed, timeout=5000):
            _select_by_data(widget.queue_box.box, queue_id_2)
        QApplication.processEvents()

        assert config_file.get_setting("defaults.queue_id") == queue_id_2

    def test_selecting_queue_clears_stale_storage_profile(self, qtbot, widget, seeded_backend):
        """Selecting a different queue clears the storage profile from the old queue.

        Storage profiles are queue-scoped, so a profile selected for the previous
        queue must not linger in config (or be re-shown as a raw id) after switching
        queues. Uses a second queue with no storage profiles so nothing can be
        auto-selected back in, isolating the "stale profile is cleared" behavior.
        """
        backend, farm_id, queue_id = seeded_backend
        # A second queue with no storage profiles of its own.
        queue_id_2 = backend.create_queue(farmId=farm_id, displayName="No-SP Queue")["queueId"]
        config_file.set_setting("defaults.farm_id", farm_id)
        # Pre-seed a stale storage profile belonging to the old queue.
        config_file.set_setting("settings.storage_profile_id", "sp-stale")

        controller = DeadlineUIController.getInstance()
        widget.refresh_setting_controls(deadline_authorized=True)
        with qtbot.waitSignal(controller.queues_updated, timeout=5000):
            widget.queue_box.refresh_list()
        QApplication.processEvents()

        with qtbot.waitSignal(widget.selection_changed, timeout=5000):
            _select_by_data(widget.queue_box.box, queue_id_2)
        QApplication.processEvents()

        assert config_file.get_setting("defaults.queue_id") == queue_id_2
        assert config_file.get_setting("settings.storage_profile_id") == ""

    def test_selecting_storage_profile_persists(self, qtbot, widget, seeded_backend):
        backend, farm_id, queue_id = seeded_backend
        config_file.set_setting("defaults.farm_id", farm_id)
        config_file.set_setting("defaults.queue_id", queue_id)

        controller = DeadlineUIController.getInstance()
        widget.refresh_setting_controls(deadline_authorized=True)
        with qtbot.waitSignal(controller.storage_profiles_updated, timeout=5000):
            widget.storage_profile_box.refresh_list()
        QApplication.processEvents()

        # Pick the storage profile for this OS (the only real entry beyond the placeholder).
        sp_combo = widget.storage_profile_box.box
        real = [
            sp_combo.itemData(i)
            for i in range(sp_combo.count())
            if sp_combo.itemData(i)
            and sp_combo.itemText(i) not in ("<refreshing>", "<none selected>")
        ]
        assert real, f"expected a real storage profile, got {_items(sp_combo)}"
        _select_by_data(sp_combo, real[0])
        QApplication.processEvents()

        assert config_file.get_setting("settings.storage_profile_id") == real[0]


class TestStorageProfileVisibility:
    def test_storage_profile_shown_when_queue_has_profiles(self, qtbot, widget, seeded_backend):
        """The storage-profile row appears once a queue with profiles is loaded."""
        _, farm_id, queue_id = seeded_backend
        config_file.set_setting("defaults.farm_id", farm_id)
        config_file.set_setting("defaults.queue_id", queue_id)

        controller = DeadlineUIController.getInstance()
        widget.refresh_setting_controls(deadline_authorized=True)
        with qtbot.waitSignal(controller.storage_profiles_updated, timeout=5000):
            widget.storage_profile_box.refresh_list()
        QApplication.processEvents()

        assert widget._has_real_storage_profiles()
        assert not widget.storage_profile_box.isHidden()
        assert not widget.storage_profile_box_label.isHidden()

    def test_storage_profile_hidden_when_queue_has_no_profiles(self, qtbot, widget, seeded_backend):
        """A queue with no storage profiles keeps the row hidden."""
        backend, farm_id, _ = seeded_backend
        empty_queue = backend.create_queue(farmId=farm_id, displayName="No-SP Queue")
        empty_queue_id = empty_queue["queueId"]
        config_file.set_setting("defaults.farm_id", farm_id)
        config_file.set_setting("defaults.queue_id", empty_queue_id)

        controller = DeadlineUIController.getInstance()
        widget.refresh_setting_controls(deadline_authorized=True)
        with qtbot.waitSignal(controller.storage_profiles_updated, timeout=5000):
            widget.storage_profile_box.refresh_list()
        QApplication.processEvents()

        assert not widget._has_real_storage_profiles()
        assert widget.storage_profile_box.isHidden()


class TestCascade:
    def test_farm_change_triggers_queue_refresh(self, qtbot, widget, seeded_backend):
        """Selecting a farm cascades into reloading the queue list for that farm."""
        _, farm_id, queue_id = seeded_backend

        controller = DeadlineUIController.getInstance()
        widget.refresh_setting_controls(deadline_authorized=True)
        widget.farm_box.refresh_list()
        _wait_for_farm(qtbot, widget, farm_id)

        # Selecting the farm should set the cascade flag and refresh queues, which
        # repopulates the queue combo from the backend.
        with qtbot.waitSignal(controller.queues_updated, timeout=5000):
            _select_by_data(widget.farm_box.box, farm_id)
        QApplication.processEvents()

        assert "Test Queue" in _items(widget.queue_box.box)

    def test_farm_change_loads_the_new_farms_queues(self, qtbot, widget, seeded_backend):
        """Switching from one farm to another reloads queues for the NEWLY selected
        farm, not the previously selected one.

        Each farm has a distinctly-named queue so a stale fetch (re-listing the old
        farm) is observable: the combo would still show the old farm's queue. This
        guards the cascade against reading a stale farm_id from a cached config
        snapshot instead of the freshly persisted selection.
        """
        backend, farm_a, _ = seeded_backend
        # A second farm with its own, distinctly-named queue.
        farm_b = backend.create_farm(displayName="Farm B")["farmId"]
        backend.create_queue(farmId=farm_b, displayName="Queue B only")

        controller = DeadlineUIController.getInstance()
        config_file.set_setting("defaults.farm_id", farm_a)
        widget.refresh_setting_controls(deadline_authorized=True)
        # Load farm A's queues first so there is a stale list to (incorrectly) keep.
        with qtbot.waitSignal(controller.queues_updated, timeout=5000):
            widget.queue_box.refresh_list()
        QApplication.processEvents()
        assert "Test Queue" in _items(widget.queue_box.box)

        # Now select farm B; the cascade must reload queues for farm B.
        _wait_for_farm(qtbot, widget, farm_b)
        with qtbot.waitSignal(controller.queues_updated, timeout=5000):
            _select_by_data(widget.farm_box.box, farm_b)
        QApplication.processEvents()

        queue_items = _items(widget.queue_box.box)
        assert "Queue B only" in queue_items, f"cascade fetched wrong farm's queues: {queue_items}"
        assert "Test Queue" not in queue_items, f"stale farm A queue still present: {queue_items}"


class TestProfileSwitch:
    """Switching AWS profiles re-derives the farm/queue selection for the new profile
    using the unified rule: select the profile's stored default if available, else
    auto-select the lone farm/queue if there's exactly one, else clear to <none>.
    (Settings are profile-scoped, so a stale farm/queue from the old profile must
    never remain selected.)"""

    def test_switch_to_profile_without_default_auto_selects_lone_farm(
        self, qtbot, widget, seeded_backend
    ):
        """A new profile with no default but a single available farm auto-selects it.

        The backend has exactly one farm, so per the unified rule the new profile
        (which has no stored default) auto-selects that lone farm rather than
        clearing to <none>.
        """
        _, farm_id, queue_id = seeded_backend
        # Start on profile A with a default farm + queue selected.
        _configure_profile("profile-A", farm_id=farm_id, queue_id=queue_id)
        widget.refresh_setting_controls(deadline_authorized=True)
        _wait_for_farm(qtbot, widget, farm_id)
        assert widget.farm_box.box.currentData() == farm_id

        # Switch to profile B which has no default farm configured.
        _configure_profile("profile-B", farm_id="", queue_id="")
        _switch_profile_and_refresh(qtbot, widget, "profile-B")
        QApplication.processEvents()

        # Exactly one farm exists -> it is auto-selected (and persisted) for profile B.
        assert widget.farm_box.box.currentData() == farm_id
        assert config_file.get_setting("defaults.farm_id") == farm_id

    def test_switch_to_profile_without_default_clears_when_multiple_farms(
        self, qtbot, widget, seeded_backend
    ):
        """A new profile with no default and multiple farms clears to <none>.

        With more than one farm there's no unambiguous choice, so the selection
        clears rather than guessing.
        """
        backend, farm_id, queue_id = seeded_backend
        # A second farm so the count is > 1 and nothing auto-selects.
        backend.create_farm(displayName="Second Farm")

        _configure_profile("profile-A", farm_id=farm_id, queue_id=queue_id)
        controller = DeadlineUIController.getInstance()
        # Drain BOTH the streamed farm load (farms_loading -> False, fired only after every
        # region has appended) and the queue refresh before switching, so profile A's
        # in-flight farm/queue results can't land after the switch and repopulate under B.
        with qtbot.waitSignals(
            [controller.farms_loading, controller.queues_updated],
            timeout=5000,
            check_params_cbs=[lambda loading: loading is False, lambda *_: True],
        ):
            widget.refresh_setting_controls(deadline_authorized=True)
        QApplication.processEvents()
        assert widget.farm_box.box.currentData() == farm_id

        # Switch to profile B which has no default farm configured.
        _configure_profile("profile-B", farm_id="", queue_id="")
        _switch_profile_and_refresh(qtbot, widget, "profile-B")
        QApplication.processEvents()

        assert widget.farm_box.box.currentData() == ""
        assert widget.queue_box.box.currentData() == ""

    def test_roundtrip_AtoBtoA_preserves_each_profiles_selection(
        self, qtbot, widget, seeded_backend
    ):
        """Regression: switching A -> B -> A must restore each profile's own farm/queue.

        Farm/queue are profile-scoped in config. Returning to a profile must show
        the value it had, never a blank or the other profile's value. This guards
        the reported bug where an auto-select path cleared the stored selection on
        switch.
        """
        backend, farm_a, queue_a = seeded_backend
        farm_b = backend.create_farm(displayName="Farm B")["farmId"]
        queue_b = backend.create_queue(farmId=farm_b, displayName="Queue B")["queueId"]

        _configure_profile("profile-A", farm_id=farm_a, queue_id=queue_a)
        controller = DeadlineUIController.getInstance()
        with qtbot.waitSignal(controller.farms_updated, timeout=5000):
            widget.refresh_setting_controls(deadline_authorized=True)
        QApplication.processEvents()
        assert widget.farm_box.box.currentData() == farm_a

        # A -> B
        _configure_profile("profile-B", farm_id=farm_b, queue_id=queue_b)
        _switch_profile_and_refresh(qtbot, widget, "profile-B")
        assert widget.farm_box.box.currentData() == farm_b
        assert config_file.get_setting("defaults.farm_id") == farm_b
        assert config_file.get_setting("defaults.queue_id") == queue_b

        # B -> A : profile A's stored selection must come back intact.
        _switch_profile_and_refresh(qtbot, widget, "profile-A")
        assert config_file.get_setting("defaults.farm_id") == farm_a, "profile A's farm was cleared"
        assert config_file.get_setting("defaults.queue_id") == queue_a, (
            "profile A's queue was cleared"
        )
        assert widget.farm_box.box.currentData() == farm_a
        QApplication.processEvents()
        assert widget.queue_box.box.currentData() == queue_a

    def test_switch_to_profile_with_default_farm_selects_it(self, qtbot, widget, seeded_backend):
        """A new profile with a default farm/queue must select them."""
        backend, farm_id, queue_id = seeded_backend
        # A second farm/queue that profile B defaults to.
        farm_b = backend.create_farm(displayName="Farm B")["farmId"]
        queue_b = backend.create_queue(farmId=farm_b, displayName="Queue B")["queueId"]

        # Start on profile A.
        _configure_profile("profile-A", farm_id=farm_id, queue_id=queue_id)
        controller = DeadlineUIController.getInstance()
        with qtbot.waitSignal(controller.farms_updated, timeout=5000):
            widget.refresh_setting_controls(deadline_authorized=True)
        QApplication.processEvents()
        assert widget.farm_box.box.currentData() == farm_id

        # Switch to profile B, which defaults to farm_b / queue_b.
        _configure_profile("profile-B", farm_id=farm_b, queue_id=queue_b)
        _switch_profile_and_refresh(qtbot, widget, "profile-B")

        assert widget.farm_box.box.currentData() == farm_b
        # The queue list must also reflect profile B's default queue.
        QApplication.processEvents()
        assert widget.queue_box.box.currentData() == queue_b

    def test_switch_to_not_logged_in_profile_clears_farm_list(self, qtbot, widget, seeded_backend):
        """Switching to a profile that isn't logged in must clear the old farm list.

        When the new profile has no API access we can't list its farms, but the
        previous profile's farms must NOT remain visible in the combo.
        """
        _, farm_id, queue_id = seeded_backend
        # Start on profile A (logged in) with farms loaded.
        _configure_profile("profile-A", farm_id=farm_id, queue_id=queue_id)
        controller = DeadlineUIController.getInstance()
        # refresh_setting_controls kicks off async farm AND queue (and storage)
        # refreshes. Wait for the queues list to land, plus the streamed farms (farms
        # arrive via farms_appended after the initial farms_updated([]) clear), before
        # switching profiles -- otherwise an in-flight profile-A queues_updated can
        # arrive after the offline clear below and repopulate "Test Queue".
        with qtbot.waitSignal(controller.queues_updated, timeout=5000):
            widget.refresh_setting_controls(deadline_authorized=True)
        _wait_for_farm(qtbot, widget, farm_id)
        assert _has_label_containing(widget.farm_box.box, "Test Farm")
        assert "Test Queue" in _items(widget.queue_box.box)

        # Switch to profile B, which is NOT logged in (deadline_authorized=False).
        _configure_profile("profile-B", farm_id="", queue_id="")
        config_file.set_setting("defaults.aws_profile_name", "profile-B")
        widget.refresh_setting_controls(deadline_authorized=False)
        QApplication.processEvents()

        # The old profile's farms must be gone; nothing can be listed without login.
        assert not _has_label_containing(widget.farm_box.box, "Test Farm")
        assert widget.farm_box.box.currentData() == ""
        assert "Test Queue" not in _items(widget.queue_box.box)
        assert widget.queue_box.box.currentData() == ""

    def test_lone_farm_auto_selected_on_login_after_offline_switch(
        self, qtbot, widget, seeded_backend
    ):
        """Switching offline then logging in auto-selects the lone farm.

        Switching to a not-logged-in profile can't list farms, so the selection is
        empty. Once the user logs in to that profile (which has no stored default),
        the unified rule kicks in on the resulting list refresh and the sole farm
        is auto-selected.
        """
        _, farm_id, _ = seeded_backend
        controller = DeadlineUIController.getInstance()

        # Start on profile-A, logged in, with its farm already chosen (so no
        # auto-select fires here to muddy the later assertion). Wait for the farm stream
        # to fully settle (farms_loading -> False) so no in-flight append lands after the
        # offline switch below and repopulates the box.
        _configure_profile("profile-A", farm_id=farm_id, queue_id="")
        with qtbot.waitSignal(
            controller.farms_loading,
            timeout=5000,
            check_params_cb=lambda loading: loading is False,
        ):
            widget.refresh_setting_controls(deadline_authorized=True)
        QApplication.processEvents()

        # Switch to profile-B which is NOT logged in: no API, so nothing is listed
        # or selected.
        _configure_profile("profile-B", farm_id="", queue_id="")
        config_file.set_setting("defaults.aws_profile_name", "profile-B")
        widget.refresh_setting_controls(deadline_authorized=False)
        QApplication.processEvents()
        assert widget.farm_box.box.currentData() == ""

        # Now log in to profile-B. The sole farm is auto-selected for the new
        # profile that has no stored default. Farms stream in after the clear, so wait
        # for the streaming load to finish (farms_loading -> False) where auto-select runs.
        with qtbot.waitSignal(
            controller.farms_loading,
            timeout=5000,
            check_params_cb=lambda loading: loading is False,
        ):
            widget.refresh_setting_controls(deadline_authorized=True)
        QApplication.processEvents()

        assert widget.farm_box.box.currentData() == farm_id
        assert config_file.get_setting("defaults.farm_id") == farm_id
