# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the _HelpDialog class and its functionality.

These tests verify the about dialog functionality including the
_make_keys_human_readable function and dialog initialization.
"""

import pytest
from unittest.mock import Mock, patch

from deadline.client.dataclasses.submitter_info import SubmitterInfo

try:
    from deadline.client.ui.dialogs._help_dialog import _HelpDialog
    from qtpy.QtWidgets import QLabel  # pylint: disable=import-error

    _make_keys_human_readable = _HelpDialog._make_keys_human_readable
except ImportError:
    # The tests in this file should be skipped if Qt UI related modules cannot be loaded
    pytest.importorskip("deadline.client.ui.dialogs._help_dialog")


@pytest.mark.parametrize(
    "input_data,expected",
    [
        pytest.param(
            {"some_key": "value", "another_key": 123},
            {"some key": "value", "another key": 123},
            id="flat_dict",
        ),
        pytest.param(
            {"outer_key": {"inner_key": "value", "another_inner_key": 456}},
            {"outer key": {"inner key": "value", "another inner key": 456}},
            id="nested_dict",
        ),
        pytest.param(
            {"level_one": {"level_two": {"level_three": {"deep_key": "deep_value"}}}},
            {"level one": {"level two": {"level three": {"deep key": "deep_value"}}}},
            id="deeply_nested_dict",
        ),
        pytest.param(
            {"items_list": [{"item_name": "first"}, {"item_name": "second"}]},
            {"items list": [{"item name": "first"}, {"item name": "second"}]},
            id="list_with_dicts",
        ),
        pytest.param(
            {"my_list": [1, 2, 3, "four_five"]},
            {"my list": [1, 2, 3, "four_five"]},
            id="list_of_scalars",
        ),
        pytest.param(
            {"normalkey": "value", "AnotherKey": 123},
            {"normalkey": "value", "AnotherKey": 123},
            id="keys_without_underscores",
        ),
        pytest.param({}, {}, id="empty_dict"),
        pytest.param([], [], id="empty_list"),
    ],
)
def test_make_keys_human_readable(input_data, expected):
    """Test that _make_keys_human_readable correctly transforms dictionary keys."""
    result = _make_keys_human_readable(input_data)
    assert result == expected


@pytest.mark.parametrize(
    "scalar_value",
    [
        pytest.param("string_value", id="string"),
        pytest.param(123, id="int"),
        pytest.param(45.67, id="float"),
        pytest.param(True, id="bool"),
        pytest.param(None, id="none"),
    ],
)
def test_scalar_values_unchanged(scalar_value):
    """Test that scalar values pass through unchanged."""
    result = _make_keys_human_readable(scalar_value)
    assert result == scalar_value


def test_mixed_nested_structure():
    """Test complex structure with dicts, lists, and scalar values."""
    data = {
        "deadline_dep_versions": {
            "deadline_cloud": "1.0.0",
            "boto3_stubs": "1.2.3",
        },
        "additional_info": {
            "loaded_plugins": [
                {"plugin_name": "Plugin A", "plugin_version": "1.0"},
                {"plugin_name": "Plugin B", "plugin_version": "2.0"},
            ],
            "render_engine": "Cycles",
        },
        "simple_value": "test",
    }

    expected = {
        "deadline dep versions": {
            "deadline cloud": "1.0.0",
            "boto3 stubs": "1.2.3",
        },
        "additional info": {
            "loaded plugins": [
                {"plugin name": "Plugin A", "plugin version": "1.0"},
                {"plugin name": "Plugin B", "plugin version": "2.0"},
            ],
            "render engine": "Cycles",
        },
        "simple value": "test",
    }

    result = _make_keys_human_readable(data)
    assert result == expected


class TestHelpDialog:
    """Test cases for _HelpDialog class."""

    def test_format_version_info_includes_submitter_data(self):
        """Test that _format_version_info includes submitter information."""
        submitter_info = SubmitterInfo(
            submitter_name="TestApp",
            submitter_package_name="test-package",
            submitter_package_version="1.0.0",
        )

        mock_env_info = Mock()

        with patch(
            "deadline.client.ui.dialogs._help_dialog._EnvironmentInfo.collect",
            return_value=mock_env_info,
        ), patch.object(_HelpDialog, "__init__", return_value=None), patch(
            "deadline.client.ui.dialogs._help_dialog.asdict"
        ) as mock_asdict, patch(
            "deadline.client.ui.dialogs._help_dialog.yaml.dump"
        ) as mock_yaml_dump:
            # Mock asdict to return predictable data
            mock_asdict.side_effect = [
                {"env_field": "env_value"},  # environment_info
                {  # submitter_info
                    "submitter_name": "TestApp",
                    "submitter_package_name": "test-package",
                    "submitter_package_version": "1.0.0",
                    "host_application_name": None,
                    "host_application_version": None,
                    "additional_info": None,
                },
            ]
            mock_yaml_dump.return_value = "formatted_yaml"

            # Create dialog instance and manually set attributes
            dialog = _HelpDialog.__new__(_HelpDialog)
            dialog.submitter_info = submitter_info
            dialog.environment_info = mock_env_info

            result = dialog._format_version_info()

            assert result == "formatted_yaml"
            # Verify that asdict was called for both environment and submitter info
            assert mock_asdict.call_count == 2
            # Verify yaml.dump was called with combined info
            mock_yaml_dump.assert_called_once()

    def test_format_for_copy_includes_header(self, qtbot):
        """Test that _format_for_copy includes proper header."""
        submitter_info = SubmitterInfo(submitter_name="TestApp")

        with patch(
            "deadline.client.ui.dialogs._help_dialog._EnvironmentInfo.collect"
        ) as mock_collect:
            from deadline.client.ui.dataclasses._environment_info import _EnvironmentInfo

            mock_env_info = _EnvironmentInfo(
                deadline_dep_versions={"deadline": "1.0.0"},
                os_name="TestOS",
                os_version="1.0",
                os_architecture="x86_64",
                python_version="3.12.0",
                qt_version="6.0.0",
            )
            mock_collect.return_value = mock_env_info

            help_dialog = _HelpDialog(submitter_info)
            qtbot.addWidget(help_dialog)

            assert help_dialog.windowTitle() == "About Deadline Cloud TestApp Submitter"

            result = help_dialog._format_for_copy()
            lines = result.split("\n")

            assert "AWS Deadline Cloud Submitter Information" in lines[0]
            assert "=" * len("AWS Deadline Cloud Submitter Information") in lines[1]
            # Verify that the formatted version info is included
            assert len(lines) > 2

    def test_hard_coded_documentation_links_display(self, qtbot):
        """Test that hard-coded documentation links are displayed correctly in UI."""
        submitter_info = SubmitterInfo(submitter_name="TestApp")

        with patch(
            "deadline.client.ui.dialogs._help_dialog._EnvironmentInfo.collect"
        ) as mock_collect:
            from deadline.client.ui.dataclasses._environment_info import _EnvironmentInfo

            mock_env_info = _EnvironmentInfo(
                deadline_dep_versions={"deadline": "1.0.0"},
                os_name="TestOS",
                os_version="1.0",
                os_architecture="x86_64",
                python_version="3.12.0",
                qt_version="6.0.0",
            )
            mock_collect.return_value = mock_env_info

            help_dialog = _HelpDialog(submitter_info)
            qtbot.addWidget(help_dialog)

            # Get the layout and find all QLabel widgets
            layout = help_dialog.layout()
            labels = []
            if layout is not None:
                for i in range(layout.count()):
                    item = layout.itemAt(i)
                    if item is not None:
                        widget = item.widget()
                        if isinstance(widget, QLabel):
                            labels.append(widget)

            # Filter labels that contain documentation links (have href tags)
            doc_labels = [label for label in labels if "<a href=" in label.text()]

            # Assert we have at least one documentation link label (hard-coded)
            assert len(doc_labels) >= 1

            # Verify the hard-coded documentation link is present
            assert doc_labels[0].text() == "<a href='https://aws-deadline.github.io'>User Guide</a>"

            # Verify tooltip shows the URL
            assert doc_labels[0].toolTip() == "https://aws-deadline.github.io"

            # Verify label has correct properties
            assert doc_labels[0].openExternalLinks() is True
            assert doc_labels[0].wordWrap() is True
