# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the controller-based Deadline resource list combo boxes.
"""

import pytest
from typing import List
from unittest.mock import patch
from configparser import ConfigParser

pytest.importorskip("deadline.client.ui.widgets._deadline_list_combo_boxes")

from deadline.client.config import config_file  # noqa: E402
from deadline.client.ui.widgets._deadline_list_combo_boxes import (  # noqa: E402
    DeadlineFarmListComboBoxController,
    DeadlineQueueListComboBoxController,
    DeadlineStorageProfileListComboBoxController,
)
from deadline.client.ui.controllers._deadline_controller import DeadlineUIController  # noqa: E402
from deadline.client.ui.controllers._thread_pool import DeadlineThreadPool  # noqa: E402


class TestDeadlineFarmListComboBoxController:
    """Tests for DeadlineFarmListComboBoxController."""

    def setup_method(self):
        """Reset singleton and thread pool before each test."""
        DeadlineUIController.resetInstance()
        DeadlineThreadPool.reset()

    def teardown_method(self):
        """Clean up after each test."""
        DeadlineUIController.resetInstance()
        DeadlineThreadPool.shutdown(wait_for_done=True, timeout_ms=2000)
        DeadlineThreadPool.reset()

    def test_init_creates_widget(self, qtbot):
        """Test that the widget can be instantiated."""
        widget = DeadlineFarmListComboBoxController()
        qtbot.addWidget(widget)

        assert widget.box is not None
        assert widget.refresh_button is not None

    def test_set_config_updates_controller(self, qtbot):
        """Test that set_config updates the controller."""
        widget = DeadlineFarmListComboBoxController()
        qtbot.addWidget(widget)

        config = ConfigParser()
        config["defaults"] = {"aws_profile_name": "test-profile"}

        widget.set_config(config)

        assert widget.config is config

    def test_clear_list_empties_combobox(self, qtbot):
        """Test that clear_list empties the combobox."""
        widget = DeadlineFarmListComboBoxController()
        qtbot.addWidget(widget)

        # Add some items first
        widget.box.addItem("Farm 1", userData="farm-1")
        widget.box.addItem("Farm 2", userData="farm-2")
        assert widget.count() == 2

        widget.clear_list()

        assert widget.count() == 0

    def test_handle_list_update_populates_combobox(self, qtbot):
        """Test that _handle_list_update populates the combobox."""
        widget = DeadlineFarmListComboBoxController()
        qtbot.addWidget(widget)

        config = ConfigParser()
        config["defaults"] = {"farm_id": "farm-a"}  # Match one of the items
        widget.set_config(config)

        # Simulate list update from controller
        items = [["Farm A", "farm-a"], ["Farm B", "farm-b"]]

        with patch("deadline.client.ui.widgets._deadline_list_combo_boxes.config_file") as mock_cf:
            mock_cf.get_setting.return_value = "farm-a"
            widget._handle_list_update(items)

        assert widget.count() == 2
        assert widget.box.itemText(0) == "Farm A"
        assert widget.box.itemData(0) == "farm-a"
        assert widget.box.itemText(1) == "Farm B"
        assert widget.box.itemData(1) == "farm-b"

    def test_handle_loading_state_shows_refreshing(self, qtbot):
        """Test that _handle_loading_state shows refreshing indicator."""
        widget = DeadlineFarmListComboBoxController()
        qtbot.addWidget(widget)

        config = ConfigParser()
        config["defaults"] = {"farm_id": "farm-123"}
        widget.set_config(config)

        with patch("deadline.client.ui.widgets._deadline_list_combo_boxes.config_file") as mock_cf:
            mock_cf.get_setting.return_value = "farm-123"
            widget._handle_loading_state(True)

        assert widget.count() == 1
        assert widget.box.itemText(0) == "<refreshing>"
        assert widget.box.itemData(0) == "farm-123"
        assert widget.refresh_button.isEnabled() is False

    def test_handle_loading_state_enables_button_when_done(self, qtbot):
        """Test that _handle_loading_state enables button when loading completes."""
        widget = DeadlineFarmListComboBoxController()
        qtbot.addWidget(widget)

        widget._handle_loading_state(True)
        assert not widget.refresh_button.isEnabled()

        widget._handle_loading_state(False)
        assert widget.refresh_button.isEnabled()

    @patch.object(DeadlineUIController, "refresh_farms")
    def test_refresh_list_calls_controller(self, mock_refresh, qtbot):
        """Test that refresh_list calls the controller."""
        widget = DeadlineFarmListComboBoxController()
        qtbot.addWidget(widget)

        widget.refresh_list()

        mock_refresh.assert_called_once()

    def test_refresh_selected_id_selects_configured_farm(self, qtbot):
        """Test that refresh_selected_id selects the configured farm."""
        widget = DeadlineFarmListComboBoxController()
        qtbot.addWidget(widget)

        config = ConfigParser()
        config["defaults"] = {"farm_id": "farm-b"}
        widget.set_config(config)

        # Add items
        widget.box.addItem("Farm A", userData="farm-a")
        widget.box.addItem("Farm B", userData="farm-b")

        with patch("deadline.client.ui.widgets._deadline_list_combo_boxes.config_file") as mock_cf:
            mock_cf.get_setting.return_value = "farm-b"
            widget.refresh_selected_id()

        assert widget.box.currentData() == "farm-b"

    def test_refresh_selected_id_adds_none_selected_if_not_found(self, qtbot):
        """Test that refresh_selected_id adds none selected if ID not found."""
        widget = DeadlineFarmListComboBoxController()
        qtbot.addWidget(widget)

        config = ConfigParser()
        config["defaults"] = {"farm_id": "unknown-farm"}
        widget.set_config(config)

        # Add items that don't include the configured ID
        widget.box.addItem("Farm A", userData="farm-a")

        widget.refresh_selected_id()

        # Should add "<none selected>" and select it
        assert widget.box.currentText() == "<none selected>"
        assert widget.box.currentData() == ""

    def test_handle_list_update_auto_selects_single_farm_when_unset(self, qtbot):
        """When no farm is configured and exactly one loads, it is auto-selected.

        This is independent of *why* the list refreshed (open, profile switch,
        sign-in, manual refresh) - the combo selects the lone farm and emits
        ``user_selected`` so the controller persists + cascades. (The display index
        is set under block_signals, so ``currentIndexChanged`` intentionally does
        NOT fire; ``user_selected`` is the explicit "treat as a selection" signal.)
        """
        widget = DeadlineFarmListComboBoxController()
        qtbot.addWidget(widget)

        config = ConfigParser()
        config["defaults"] = {"farm_id": ""}  # nothing configured yet
        widget.set_config(config)

        user_selected: List[str] = []
        widget.user_selected.connect(user_selected.append)

        with patch("deadline.client.ui.widgets._deadline_list_combo_boxes.config_file") as mock_cf:
            mock_cf.get_setting.return_value = ""  # no configured farm
            widget._handle_list_update([["Only Farm", "farm-only"]])

        assert widget.box.currentData() == "farm-only"
        # user_selected must fire so the controller's cascade is driven naturally.
        assert user_selected == ["farm-only"]

    def test_refresh_selected_id_does_not_emit_user_selected(self, qtbot):
        """Display-sync must never look like a user selection.

        ``refresh_selected_id`` only points the combo at the already-persisted
        value; it must not emit ``user_selected`` (which would re-persist + cascade,
        the root cause of the prior clobbering races).
        """
        widget = DeadlineFarmListComboBoxController()
        qtbot.addWidget(widget)

        config = ConfigParser()
        config["defaults"] = {"farm_id": "farm-a"}
        widget.set_config(config)
        widget.box.addItem("Farm A", userData="farm-a")
        widget.box.addItem("Farm B", userData="farm-b")

        user_selected: List[str] = []
        widget.user_selected.connect(user_selected.append)

        with patch("deadline.client.ui.widgets._deadline_list_combo_boxes.config_file") as mock_cf:
            mock_cf.get_setting.return_value = "farm-a"
            widget.refresh_selected_id()

        assert widget.box.currentData() == "farm-a"
        assert user_selected == []

    def test_handle_list_update_no_auto_select_when_multiple(self, qtbot):
        """With more than one farm and nothing configured, no auto-select happens."""
        widget = DeadlineFarmListComboBoxController()
        qtbot.addWidget(widget)

        config = ConfigParser()
        config["defaults"] = {"farm_id": ""}
        widget.set_config(config)

        with patch("deadline.client.ui.widgets._deadline_list_combo_boxes.config_file") as mock_cf:
            mock_cf.get_setting.return_value = ""
            widget._handle_list_update([["Farm A", "farm-a"], ["Farm B", "farm-b"]])

        # Falls back to the "<none selected>" placeholder.
        assert widget.box.currentData() == ""

    def test_handle_list_update_no_auto_select_when_already_configured(self, qtbot):
        """If a farm is already configured, the configured one is kept (no override)."""
        widget = DeadlineFarmListComboBoxController()
        qtbot.addWidget(widget)

        config = ConfigParser()
        config["defaults"] = {"farm_id": "farm-a"}
        widget.set_config(config)

        with patch("deadline.client.ui.widgets._deadline_list_combo_boxes.config_file") as mock_cf:
            mock_cf.get_setting.return_value = "farm-a"
            widget._handle_list_update([["Farm A", "farm-a"]])

        assert widget.box.currentData() == "farm-a"

    def test_handle_list_append_adds_incrementally(self, qtbot):
        """Farm options stream in via append without clearing prior batches."""
        widget = DeadlineFarmListComboBoxController()
        qtbot.addWidget(widget)

        config = ConfigParser()
        config["defaults"] = {"farm_id": "", "farm_region": ""}
        widget.set_config(config)

        with patch("deadline.client.ui.widgets._deadline_list_combo_boxes.config_file") as mock_cf:
            mock_cf.get_setting.return_value = ""
            # First region arrives.
            widget._handle_list_append([("(us-west-2) Farm A", "farm-a", "us-west-2")])
            assert widget.box.findData("farm-a") >= 0
            # Second region arrives; first batch is preserved (not cleared).
            widget._handle_list_append([("(eu-west-1) Farm B", "farm-b", "eu-west-1")])

        assert widget.box.findData("farm-a") >= 0
        assert widget.box.findData("farm-b") >= 0
        # Labels are region-first.
        idx_a = widget.box.findData("farm-a")
        assert widget.box.itemText(idx_a) == "(us-west-2) Farm A"
        # Region is recorded per id for downstream persistence.
        assert widget.region_for_id("farm-a") == "us-west-2"
        assert widget.region_for_id("farm-b") == "eu-west-1"

    def test_handle_list_append_keeps_stable_alphabetical_order(self, qtbot):
        """
        Regardless of the order regions stream in, the list is alphabetized by label.
        Labels are region-first, so this orders by region then name.
        """
        widget = DeadlineFarmListComboBoxController()
        qtbot.addWidget(widget)

        config = ConfigParser()
        config["defaults"] = {"farm_id": "", "farm_region": ""}
        widget.set_config(config)

        with patch("deadline.client.ui.widgets._deadline_list_combo_boxes.config_file") as mock_cf:
            mock_cf.get_setting.return_value = ""
            # Regions arrive out of alphabetical order.
            widget._handle_list_append([("(us-west-2) Zebra", "farm-z", "us-west-2")])
            widget._handle_list_append([("(eu-west-1) Apple", "farm-a", "eu-west-1")])
            widget._handle_list_append(
                [
                    ("(us-west-2) Apple", "farm-wa", "us-west-2"),
                    ("(ap-south-1) Mango", "farm-m", "ap-south-1"),
                ]
            )

        labels = [widget.box.itemText(i) for i in range(widget.box.count())]
        assert labels == [
            "(ap-south-1) Mango",
            "(eu-west-1) Apple",
            "(us-west-2) Apple",
            "(us-west-2) Zebra",
        ]

    def test_handle_list_append_preserves_selection(self, qtbot):
        """Appending more farms must not clobber the user's current selection."""
        widget = DeadlineFarmListComboBoxController()
        qtbot.addWidget(widget)

        config = ConfigParser()
        config["defaults"] = {"farm_id": "", "farm_region": ""}
        widget.set_config(config)

        with patch("deadline.client.ui.widgets._deadline_list_combo_boxes.config_file") as mock_cf:
            mock_cf.get_setting.return_value = ""
            widget._handle_list_append([("(us-west-2) Farm A", "farm-a", "us-west-2")])
            # User selects Farm A.
            widget.box.setCurrentIndex(widget.box.findData("farm-a"))
            assert widget.box.currentData() == "farm-a"
            # A slower region streams in after the selection.
            widget._handle_list_append([("(eu-west-1) Farm B", "farm-b", "eu-west-1")])

        # Selection is preserved across the append.
        assert widget.box.currentData() == "farm-a"

    def test_handle_list_append_replaces_raw_id_placeholder(self, qtbot):
        """A configured-but-unlisted raw id row is replaced by the labeled row."""
        widget = DeadlineFarmListComboBoxController()
        qtbot.addWidget(widget)

        config = ConfigParser()
        config["defaults"] = {"farm_id": "farm-a", "farm_region": ""}
        widget.set_config(config)

        with patch("deadline.client.ui.widgets._deadline_list_combo_boxes.config_file") as mock_cf:
            mock_cf.get_setting.return_value = "farm-a"
            # Simulate the start-of-refresh clear, which inserts the raw configured id.
            widget._handle_list_update([])
            assert widget.box.findData("farm-a") >= 0
            assert widget.box.itemText(widget.box.findData("farm-a")) == "farm-a"
            # The labeled farm streams in for the same id.
            widget._handle_list_append([("(us-west-2) Farm A", "farm-a", "us-west-2")])

        # Only one row for farm-a, now with the labeled text.
        matches = [i for i in range(widget.box.count()) if widget.box.itemData(i) == "farm-a"]
        assert len(matches) == 1
        assert widget.box.itemText(matches[0]) == "(us-west-2) Farm A"
        # And it stays selected.
        assert widget.box.currentData() == "farm-a"

    def test_handle_list_append_dedupes_repeated_same_id(self, qtbot):
        """
        If the same farm id arrives in two separate append batches (e.g. a duplicated or
        retried region response), the combo box must not show two rows for it -- the later
        batch replaces the earlier row rather than adding a duplicate.
        """
        widget = DeadlineFarmListComboBoxController()
        qtbot.addWidget(widget)

        config = ConfigParser()
        config["defaults"] = {"farm_id": "", "farm_region": ""}
        widget.set_config(config)

        with patch("deadline.client.ui.widgets._deadline_list_combo_boxes.config_file") as mock_cf:
            mock_cf.get_setting.return_value = ""
            widget._handle_list_append([("(us-west-2) Farm A", "farm-a", "us-west-2")])
            # The same id streams in again (possibly with an updated label).
            widget._handle_list_append([("(us-west-2) Farm A (updated)", "farm-a", "us-west-2")])

        matches = [i for i in range(widget.box.count()) if widget.box.itemData(i) == "farm-a"]
        assert len(matches) == 1
        # The most recent batch's label wins.
        assert widget.box.itemText(matches[0]) == "(us-west-2) Farm A (updated)"
        assert widget.region_for_id("farm-a") == "us-west-2"

    def test_handle_list_append_dedupes_within_single_batch(self, qtbot):
        """A single append batch containing the same id twice yields only one row."""
        widget = DeadlineFarmListComboBoxController()
        qtbot.addWidget(widget)

        config = ConfigParser()
        config["defaults"] = {"farm_id": "", "farm_region": ""}
        widget.set_config(config)

        with patch("deadline.client.ui.widgets._deadline_list_combo_boxes.config_file") as mock_cf:
            mock_cf.get_setting.return_value = ""
            widget._handle_list_append(
                [
                    ("(us-west-2) Farm A", "farm-a", "us-west-2"),
                    ("(us-west-2) Farm A dup", "farm-a", "us-west-2"),
                ]
            )

        matches = [i for i in range(widget.box.count()) if widget.box.itemData(i) == "farm-a"]
        assert len(matches) == 1

    def test_handle_region_warning_is_non_blocking(self, qtbot):
        """A per-region warning sets a tooltip and does not raise/pop a modal."""
        widget = DeadlineFarmListComboBoxController()
        qtbot.addWidget(widget)

        # Should not raise; just records a tooltip.
        widget._handle_region_warning("us-east-1", Exception("opt-in required"))

        assert "us-east-1" in widget.box.toolTip()

    def test_current_region_reflects_selection(self, qtbot):
        """current_region returns the region of the selected farm."""
        widget = DeadlineFarmListComboBoxController()
        qtbot.addWidget(widget)

        config = ConfigParser()
        config["defaults"] = {"farm_id": "", "farm_region": ""}
        widget.set_config(config)

        with patch("deadline.client.ui.widgets._deadline_list_combo_boxes.config_file") as mock_cf:
            mock_cf.get_setting.return_value = ""
            widget._handle_list_append([("(us-west-2) Farm A", "farm-a", "us-west-2")])
            widget.box.setCurrentIndex(widget.box.findData("farm-a"))

        assert widget.current_region() == "us-west-2"


class TestDeadlineQueueListComboBoxController:
    """Tests for DeadlineQueueListComboBoxController."""

    def setup_method(self):
        """Reset singleton and thread pool before each test."""
        DeadlineUIController.resetInstance()
        DeadlineThreadPool.reset()

    def teardown_method(self):
        """Clean up after each test."""
        DeadlineUIController.resetInstance()
        DeadlineThreadPool.shutdown(wait_for_done=True, timeout_ms=2000)
        DeadlineThreadPool.reset()

    def test_init_creates_widget(self, qtbot):
        """Test that the widget can be instantiated."""
        widget = DeadlineQueueListComboBoxController()
        qtbot.addWidget(widget)

        assert widget.box is not None
        assert widget.resource_name == "Queue"

    @patch("deadline.client.ui.widgets._deadline_list_combo_boxes.config_file")
    @patch.object(DeadlineUIController, "refresh_queues")
    def test_refresh_list_calls_controller_with_farm_id(self, mock_refresh, mock_cf, qtbot):
        """Test that refresh_list calls controller with farm_id."""
        widget = DeadlineQueueListComboBoxController()
        qtbot.addWidget(widget)

        config = ConfigParser()
        config["defaults"] = {"farm_id": "farm-123", "queue_id": ""}
        widget.set_config(config)

        mock_cf.get_setting.return_value = "farm-123"
        widget.refresh_list()

        mock_refresh.assert_called_once_with(farm_id="farm-123")

    def test_handle_list_update_populates_queues(self, qtbot):
        """Test that _handle_list_update populates queues."""
        widget = DeadlineQueueListComboBoxController()
        qtbot.addWidget(widget)

        config = ConfigParser()
        config["defaults"] = {"queue_id": "queue-1"}
        widget.set_config(config)

        items = [["Queue 1", "queue-1"], ["Queue 2", "queue-2"]]

        with patch("deadline.client.ui.widgets._deadline_list_combo_boxes.config_file") as mock_cf:
            mock_cf.get_setting.return_value = "queue-1"
            widget._handle_list_update(items)

        assert widget.count() == 2
        assert widget.box.itemText(0) == "Queue 1"


class TestDeadlineStorageProfileListComboBoxController:
    """Tests for DeadlineStorageProfileListComboBoxController."""

    def setup_method(self):
        """Reset singleton and thread pool before each test."""
        DeadlineUIController.resetInstance()
        DeadlineThreadPool.reset()

    def teardown_method(self):
        """Clean up after each test."""
        DeadlineUIController.resetInstance()
        DeadlineThreadPool.shutdown(wait_for_done=True, timeout_ms=2000)
        DeadlineThreadPool.reset()

    def test_init_creates_widget(self, qtbot):
        """Test that the widget can be instantiated."""
        widget = DeadlineStorageProfileListComboBoxController()
        qtbot.addWidget(widget)

        assert widget.box is not None
        assert widget.resource_name == "Storage profile"

    @patch("deadline.client.ui.widgets._deadline_list_combo_boxes.config_file")
    @patch.object(DeadlineUIController, "refresh_storage_profiles")
    def test_refresh_list_calls_controller_with_ids(self, mock_refresh, mock_cf, qtbot):
        """Test that refresh_list calls controller with farm and queue IDs."""
        widget = DeadlineStorageProfileListComboBoxController()
        qtbot.addWidget(widget)

        config = ConfigParser()
        config["defaults"] = {"farm_id": "farm-123", "queue_id": "queue-456"}
        config["settings"] = {"storage_profile_id": ""}
        widget.set_config(config)

        # Return different values for different setting names
        def get_setting_side_effect(setting_name, config=None):
            if setting_name == "defaults.farm_id":
                return "farm-123"
            elif setting_name == "defaults.queue_id":
                return "queue-456"
            return ""

        mock_cf.get_setting.side_effect = get_setting_side_effect
        widget.refresh_list()

        mock_refresh.assert_called_once_with(farm_id="farm-123", queue_id="queue-456")

    def test_handle_list_update_populates_profiles(self, qtbot):
        """Test that _handle_list_update populates storage profiles."""
        widget = DeadlineStorageProfileListComboBoxController()
        qtbot.addWidget(widget)

        config = ConfigParser()
        config["settings"] = {"storage_profile_id": "profile-1"}
        widget.set_config(config)

        items = [
            ["<none selected>", ""],
            ["Profile 1", "profile-1"],
            ["Profile 2", "profile-2"],
        ]

        with patch("deadline.client.ui.widgets._deadline_list_combo_boxes.config_file") as mock_cf:
            mock_cf.get_setting.return_value = "profile-1"
            widget._handle_list_update(items)

        assert widget.count() == 3
        assert widget.box.itemText(0) == "<none selected>"
        assert widget.box.itemText(1) == "Profile 1"

    def test_handle_list_update_does_not_auto_select_single_profile(self, qtbot):
        """Storage profile is optional, so a lone profile must NOT be auto-selected."""
        widget = DeadlineStorageProfileListComboBoxController()
        qtbot.addWidget(widget)

        config = ConfigParser()
        config["settings"] = {"storage_profile_id": ""}
        widget.set_config(config)

        with patch("deadline.client.ui.widgets._deadline_list_combo_boxes.config_file") as mock_cf:
            mock_cf.get_setting.return_value = ""
            widget._handle_list_update([["Only Profile", "profile-only"]])

        # No configured id and no auto-select -> stays on the "<none selected>" entry.
        assert widget.box.currentData() == ""


class TestStaleConfigOnFarmChange:
    """Regression tests: changing farms must not show stale queue/storage profile IDs.

    When `select_farm` writes to the config file via `set_setting`, the next
    `read_config()` detects the change and creates a fresh ConfigParser, leaving any
    combo that still holds the OLD object in `self.config` reading stale values --
    showing a raw id instead of "<none selected>".

    These tests reproduce that staleness the way it happens in the submit dialog: the
    combo is handed the *live* global config object (exactly what the dialog passes),
    then the cascade's writes force `read_config()` to swap in a fresh parser,
    orphaning the combo's reference. The swap normally hinges on the config file's
    mtime changing, which is filesystem-granularity dependent -- so we force it
    deterministically by making `_should_read_config` always report a change *during
    the cascade only*. (It must NOT be forced while `set_config` runs: `set_config`
    records whether it was handed the live global via an identity check, and forcing a
    swap there would hand it a different object and defeat the check.) The fix
    (`_sync_config`) re-reads the live config on display, so the combo must reflect the
    cleared values. Without the fix, `refresh_selected_id` reads the orphaned parser
    and shows the stale id.
    """

    def setup_method(self):
        DeadlineUIController.resetInstance()
        DeadlineThreadPool.reset()

    def teardown_method(self):
        DeadlineUIController.resetInstance()
        DeadlineThreadPool.shutdown(wait_for_done=True, timeout_ms=2000)
        DeadlineThreadPool.reset()

    @patch("deadline.client.ui.controllers._deadline_controller.api")
    def test_storage_profile_cleared_after_farm_change(
        self, mock_api, qtbot, fresh_deadline_config
    ):
        """After select_farm, storage profile combo must show <none selected>, not stale ID."""
        from deadline.client.config import set_setting

        mock_api.list_queues.return_value = {"queues": []}

        # Persisted state at dialog open: farm-B is selected and still carries the
        # queue/storage profile the user previously used with it.
        set_setting("defaults.farm_id", "farm-B")
        set_setting("defaults.queue_id", "queue-B")
        set_setting("settings.storage_profile_id", "sp-stale-id")

        # The combo is configured with the live global config, like the submit dialog.
        # No swap is forced here so the identity check in set_config sees the global.
        widget = DeadlineStorageProfileListComboBoxController()
        qtbot.addWidget(widget)
        widget.set_config(config_file.read_config())

        # The user re-selects farm-B; the controller clears the now-invalid queue/
        # storage selection. Forcing _should_read_config to True makes every
        # read_config() rebuild the parser, orphaning the combo's reference exactly the
        # way a real mtime change would -- so self.config keeps "sp-stale-id".
        controller = DeadlineUIController.getInstance()
        with patch.object(config_file, "_should_read_config", return_value=True):
            controller.select_farm("farm-B")
            # storage_profiles_updated([]) arrives -> _handle_list_update([]).
            widget._handle_list_update([])

        # Must reflect the cleared value, not the stale "sp-stale-id".
        assert widget.box.currentText() != "sp-stale-id", (
            "Storage profile combo shows stale ID after farm change"
        )
        assert widget.box.currentData() == ""

    @patch("deadline.client.ui.controllers._deadline_controller.api")
    def test_queue_cleared_after_farm_change(self, mock_api, qtbot, fresh_deadline_config):
        """After select_farm, queue combo must show <none selected>, not stale ID."""
        from deadline.client.config import set_setting

        mock_api.list_queues.return_value = {"queues": []}

        # Persisted state at dialog open: farm-B selected with its stored queue.
        set_setting("defaults.farm_id", "farm-B")
        set_setting("defaults.queue_id", "queue-stale-id")

        # Configured with the live global config, like the submit dialog.
        widget = DeadlineQueueListComboBoxController()
        qtbot.addWidget(widget)
        widget.set_config(config_file.read_config())

        # select_farm clears the queue; forcing the swap orphans the combo's reference.
        controller = DeadlineUIController.getInstance()
        with patch.object(config_file, "_should_read_config", return_value=True):
            controller.select_farm("farm-B")
            # queues_updated arrives with the new (empty) list -> _handle_list_update([]).
            widget._handle_list_update([])

        # Must reflect the cleared value, not the stale "queue-stale-id".
        assert widget.box.currentText() != "queue-stale-id", (
            "Queue combo shows stale ID after farm change"
        )
        assert widget.box.currentData() == ""


class TestInjectedConfigPreservedOnSync:
    """`_sync_config` must not clobber a caller-injected config that differs from disk.

    The config dialog (`deadline_config_dialog.refresh`) builds a *copy* of the config
    and layers the user's unsaved `changes` on top before handing it to these combos
    via `set_config`. That copy intentionally differs from the on-disk config. A naive
    `_sync_config` that unconditionally re-reads the live config on every display sync
    would discard those pending edits -- the combo would snap back to the persisted id
    instead of showing what the user just picked.

    The fix gates the re-read on an identity check (`self.config is the live global`),
    so an injected copy is left untouched. These tests assert that: after a list
    update triggers `_sync_config`, the combo still reflects the injected copy's value,
    not the value on disk. Without the identity gate they fail (the pending edit is
    lost); with the previous unconditional-read behavior they would also fail.
    """

    def setup_method(self):
        DeadlineUIController.resetInstance()
        DeadlineThreadPool.reset()

    def teardown_method(self):
        DeadlineUIController.resetInstance()
        DeadlineThreadPool.shutdown(wait_for_done=True, timeout_ms=2000)
        DeadlineThreadPool.reset()

    @staticmethod
    def _config_copy_with_pending_edit(setting_name: str, value: str) -> ConfigParser:
        """Mimic deadline_config_dialog.refresh: copy the live config, layer an edit.

        `set_setting` with an explicit config mutates only that copy (never disk), so
        the returned parser holds an unsaved value that differs from the persisted one.
        """
        copy = ConfigParser()
        copy.read_dict(config_file.read_config())
        config_file.set_setting(setting_name, value, copy)
        return copy

    @patch("deadline.client.ui.controllers._deadline_controller.api")
    def test_pending_storage_profile_edit_survives_list_update(
        self, mock_api, qtbot, fresh_deadline_config
    ):
        """A list update must not revert an unsaved storage-profile pick to the saved id."""
        from deadline.client.config import set_setting

        mock_api.list_queues.return_value = {"queues": []}

        # Persisted (saved) selection on disk.
        set_setting("defaults.farm_id", "farm-A")
        set_setting("settings.storage_profile_id", "sp-saved")

        # The config dialog hands the combo a copy carrying the user's unsaved pick.
        injected = self._config_copy_with_pending_edit("settings.storage_profile_id", "sp-pending")
        widget = DeadlineStorageProfileListComboBoxController()
        qtbot.addWidget(widget)
        widget.set_config(injected)
        assert widget._config_tracks_global is False  # a copy, not the live global

        # A list update arrives (both profiles are selectable) -> _sync_config runs.
        widget._handle_list_update([("Saved SP", "sp-saved"), ("Pending SP", "sp-pending")])

        # The pending edit must win; _sync_config must not have re-read disk's "sp-saved".
        assert widget.box.currentData() == "sp-pending", (
            "Injected pending storage-profile edit was clobbered by _sync_config"
        )
        # And the injected copy itself must be untouched (not swapped for the global).
        assert widget.config is injected

    @patch("deadline.client.ui.controllers._deadline_controller.api")
    def test_pending_queue_edit_survives_loading_state(
        self, mock_api, qtbot, fresh_deadline_config
    ):
        """The loading-state sync must also preserve an injected, unsaved queue pick."""
        from deadline.client.config import set_setting

        mock_api.list_queues.return_value = {"queues": []}

        set_setting("defaults.farm_id", "farm-A")
        set_setting("defaults.queue_id", "queue-saved")

        injected = self._config_copy_with_pending_edit("defaults.queue_id", "queue-pending")
        widget = DeadlineQueueListComboBoxController()
        qtbot.addWidget(widget)
        widget.set_config(injected)

        # _handle_loading_state(True) also calls _sync_config; it must keep the copy.
        widget._handle_loading_state(True)

        assert widget.config is injected, "Injected config was replaced during loading sync"
        assert config_file.get_setting("defaults.queue_id", config=widget.config) == "queue-pending"
