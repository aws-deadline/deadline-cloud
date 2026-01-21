# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from configparser import ConfigParser
from unittest.mock import patch, MagicMock

import pytest

try:
    from deadline.client.ui.widgets._deadline_list_combo_boxes import (
        DeadlineFarmListComboBoxController,
    )
except ImportError:
    pytest.importorskip("deadline.client.ui.widgets._deadline_list_combo_boxes")


class TestDeadlineResourceListComboBoxController:
    """Tests for _DeadlineResourceListComboBoxController.refresh_selected_id()"""

    @patch("deadline.client.ui.widgets._deadline_list_combo_boxes.DeadlineUIController.getInstance")
    @patch("deadline.client.ui.widgets._deadline_list_combo_boxes.config_file")
    def test_shows_id_when_not_in_list(self, mock_config_file, mock_get_instance, qtbot):
        """
        When user has a configured ID but lacks permission to list resources,
        the combobox should display the raw ID instead of '<none selected>'.

        This happens when a user has permission to use a queue but lacks
        permission to call ListFarms.
        """
        mock_controller = MagicMock()
        mock_get_instance.return_value = mock_controller
        mock_config_file.get_setting.return_value = "farm-abc123"

        widget = DeadlineFarmListComboBoxController()
        qtbot.addWidget(widget)
        widget.set_config(ConfigParser())

        widget.refresh_selected_id()

        assert widget.box.currentText() == "farm-abc123"
        assert widget.box.currentData() == "farm-abc123"

    @patch("deadline.client.ui.widgets._deadline_list_combo_boxes.DeadlineUIController.getInstance")
    @patch("deadline.client.ui.widgets._deadline_list_combo_boxes.config_file")
    def test_shows_none_selected_when_no_id_configured(
        self, mock_config_file, mock_get_instance, qtbot
    ):
        """When no ID is configured, should show '<none selected>'."""
        mock_controller = MagicMock()
        mock_get_instance.return_value = mock_controller
        mock_config_file.get_setting.return_value = ""

        widget = DeadlineFarmListComboBoxController()
        qtbot.addWidget(widget)
        widget.set_config(ConfigParser())

        widget.refresh_selected_id()

        assert widget.box.currentText() == "<none selected>"
