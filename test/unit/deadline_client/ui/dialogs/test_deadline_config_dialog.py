# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from configparser import ConfigParser
from unittest.mock import patch

import pytest

try:
    from deadline.client.ui.dialogs.deadline_config_dialog import _DeadlineResourceListComboBox
except ImportError:
    pytest.importorskip("deadline.client.ui.dialogs.deadline_config_dialog")


class TestDeadlineResourceListComboBox:
    """Tests for _DeadlineResourceListComboBox.refresh_selected_id()"""

    @patch("deadline.client.ui.dialogs.deadline_config_dialog.config_file")
    def test_shows_id_when_not_in_list(self, mock_config_file, qtbot):
        """
        When user has a configured ID but lacks permission to list resources,
        the combobox should display the raw ID instead of '<none selected>'.

        This happens when a user has permission to use a queue but lacks
        permission to call ListFarms.
        """
        mock_config_file.get_setting.return_value = "farm-abc123"

        widget = _DeadlineResourceListComboBox("Farm", "defaults.farm_id")
        qtbot.addWidget(widget)
        widget.set_config(ConfigParser())

        widget.refresh_selected_id()

        assert widget.box.currentText() == "farm-abc123"
        assert widget.box.currentData() == "farm-abc123"

    @patch("deadline.client.ui.dialogs.deadline_config_dialog.config_file")
    def test_shows_none_selected_when_no_id_configured(self, mock_config_file, qtbot):
        """When no ID is configured, should show '<none selected>'."""
        mock_config_file.get_setting.return_value = ""

        widget = _DeadlineResourceListComboBox("Farm", "defaults.farm_id")
        qtbot.addWidget(widget)
        widget.set_config(ConfigParser())

        widget.refresh_selected_id()

        assert widget.box.currentText() == "<none selected>"
