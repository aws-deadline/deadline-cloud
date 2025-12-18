# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the _make_keys_human_readable function in _about_dialog.py.

These tests verify that nested dictionary keys are properly transformed
to be human-readable by replacing underscores with spaces.
"""

import pytest

try:
    from deadline.client.ui.dialogs._about_dialog import _AboutDialog

    _make_keys_human_readable = _AboutDialog._make_keys_human_readable
except ImportError:
    # The tests in this file should be skipped if Qt UI related modules cannot be loaded
    pytest.importorskip("deadline.client.ui.dialogs._about_dialog")


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
