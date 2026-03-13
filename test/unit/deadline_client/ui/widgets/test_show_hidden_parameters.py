# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
import pytest
from typing import List, cast

pytest.importorskip("deadline.client.ui.widgets.openjd_parameters_widget")


from deadline.client.ui.widgets.openjd_parameters_widget import (
    OpenJDParametersWidget,
    _JobTemplateHiddenWidget,
    _JobTemplateLineEditWidget,
    _JobTemplateIntSpinBoxWidget,
    _JobTemplateFloatSpinBoxWidget,
    _JobTemplateDirectoryWidget,
    _JobTemplateInputFileWidget,
    _JobTemplateOutputFileWidget,
    _JobTemplateDropdownListWidget,
    _HIDDEN_PARAMETER_HINT,
)
from deadline.client.job_bundle.parameters import JobParameter, read_job_bundle_parameters


BUNDLE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "test_data",
    "job_bundle_with_hidden_params",
)


@pytest.fixture
def hidden_params_definitions():
    return read_job_bundle_parameters(BUNDLE_DIR)


def _is_in_layout(parent_widget, child_widget):
    """Check whether *child_widget* has been added to *parent_widget*'s layout."""
    layout = parent_widget.layout()
    if layout is None:
        return False
    for i in range(layout.count()):
        item = layout.itemAt(i)
        if item and item.widget() is child_widget:
            return True
    return False


def test_hidden_params_not_visible_by_default(qtbot, hidden_params_definitions):
    """HIDDEN parameters should have controls but not be added to the layout."""
    widget = OpenJDParametersWidget(parameter_definitions=hidden_params_definitions)
    qtbot.addWidget(widget)

    # All 11 parameters should have controls
    assert len(widget.controls) == 11

    # Hidden params should use _JobTemplateHiddenWidget
    assert isinstance(widget.controls["HiddenString"], _JobTemplateHiddenWidget)
    assert isinstance(widget.controls["HiddenInt"], _JobTemplateHiddenWidget)
    assert isinstance(widget.controls["HiddenFloat"], _JobTemplateHiddenWidget)
    assert isinstance(widget.controls["HiddenPath"], _JobTemplateHiddenWidget)
    assert isinstance(widget.controls["HiddenInputFile"], _JobTemplateHiddenWidget)
    assert isinstance(widget.controls["HiddenOutputFile"], _JobTemplateHiddenWidget)
    assert isinstance(widget.controls["HiddenDropdown"], _JobTemplateHiddenWidget)

    # Hidden widgets should not be in the layout
    assert not _is_in_layout(widget, widget.controls["HiddenString"])

    # Visible params should be in the layout
    assert _is_in_layout(widget, widget.controls["VisibleString"])


def test_show_hidden_makes_hidden_params_visible(qtbot, hidden_params_definitions):
    """With show_hidden_parameters=True, HIDDEN parameters should use default widgets and be visible."""
    widget = OpenJDParametersWidget(
        parameter_definitions=hidden_params_definitions, show_hidden_parameters=True
    )
    qtbot.addWidget(widget)

    assert len(widget.controls) == 11

    # Hidden params should now use the default widget for their type
    assert isinstance(widget.controls["HiddenString"], _JobTemplateLineEditWidget)
    assert isinstance(widget.controls["HiddenInt"], _JobTemplateIntSpinBoxWidget)
    assert isinstance(widget.controls["HiddenFloat"], _JobTemplateFloatSpinBoxWidget)
    assert isinstance(widget.controls["HiddenPath"], _JobTemplateDirectoryWidget)
    assert isinstance(widget.controls["HiddenInputFile"], _JobTemplateInputFileWidget)
    assert isinstance(widget.controls["HiddenOutputFile"], _JobTemplateOutputFileWidget)
    assert isinstance(widget.controls["HiddenDropdown"], _JobTemplateDropdownListWidget)

    # All controls should be in the layout
    assert _is_in_layout(widget, widget.controls["HiddenString"])
    assert _is_in_layout(widget, widget.controls["HiddenInt"])
    assert _is_in_layout(widget, widget.controls["HiddenFloat"])
    assert _is_in_layout(widget, widget.controls["HiddenPath"])
    assert _is_in_layout(widget, widget.controls["HiddenInputFile"])
    assert _is_in_layout(widget, widget.controls["HiddenOutputFile"])
    assert _is_in_layout(widget, widget.controls["HiddenDropdown"])

    # Values should be populated from defaults
    assert widget.controls["HiddenString"].value() == "secret-value"
    assert widget.controls["HiddenInt"].value() == 99
    assert widget.controls["HiddenFloat"].value() == pytest.approx(2.718)
    expected_hidden_path = os.path.normpath(os.path.join(BUNDLE_DIR, "hidden"))
    assert widget.controls["HiddenPath"].value() == expected_hidden_path
    assert widget.controls["HiddenDropdown"].value() == "beta"

    # Visible params should still work normally
    assert isinstance(widget.controls["VisibleString"], _JobTemplateLineEditWidget)
    assert widget.controls["VisibleString"].value() == "hello"


def test_get_parameters_round_trip_with_show_hidden(qtbot, hidden_params_definitions):
    """Editing a revealed hidden parameter must update the value returned by get_parameters(), ensuring the widget is fully interactive and not just display-only."""
    widget = OpenJDParametersWidget(
        parameter_definitions=hidden_params_definitions, show_hidden_parameters=True
    )
    qtbot.addWidget(widget)

    # Modify a revealed hidden parameter
    widget.controls["HiddenString"].set_value("modified-value")
    widget.controls["HiddenInt"].set_value(42)

    params = {p["name"]: p for p in widget.get_parameters()}

    # Values should reflect the modifications
    assert params["HiddenString"]["value"] == "modified-value"
    assert params["HiddenInt"]["value"] == 42
    # Unmodified params keep their defaults
    assert params["HiddenFloat"]["value"] == pytest.approx(2.718)
    assert params["HiddenDropdown"]["value"] == "beta"
    # Visible params still work
    assert params["VisibleString"]["value"] == "hello"


def test_show_hidden_with_queue_environment_parameters(qtbot):
    """Queue environment hidden parameters should also be revealed with show_hidden_parameters."""
    queue_params = cast(
        List[JobParameter],
        [
            {
                "name": "QueueEnvVisible",
                "type": "STRING",
                "userInterface": {"control": "LINE_EDIT", "label": "Visible Env Param"},
                "default": "visible",
            },
            {
                "name": "QueueEnvHidden",
                "type": "STRING",
                "userInterface": {"control": "HIDDEN"},
                "default": "internal-config",
            },
        ],
    )
    widget = OpenJDParametersWidget(parameter_definitions=queue_params, show_hidden_parameters=True)
    qtbot.addWidget(widget)

    assert isinstance(widget.controls["QueueEnvVisible"], _JobTemplateLineEditWidget)
    assert isinstance(widget.controls["QueueEnvHidden"], _JobTemplateLineEditWidget)
    assert widget.controls["QueueEnvHidden"].value() == "internal-config"
    assert _is_in_layout(widget, widget.controls["QueueEnvHidden"])


def test_show_hidden_adds_indicator_icon(qtbot, hidden_params_definitions):
    """Revealed hidden parameters should have an info icon with accessible description."""
    from qtpy.QtWidgets import QLabel  # type: ignore

    hidden_hint = _HIDDEN_PARAMETER_HINT

    widget = OpenJDParametersWidget(
        parameter_definitions=hidden_params_definitions, show_hidden_parameters=True
    )
    qtbot.addWidget(widget)

    # A revealed hidden param should have an icon label with the hint
    control = widget.controls["HiddenString"]
    icon_labels = [
        child
        for child in control.findChildren(QLabel)
        if child.accessibleDescription() == hidden_hint
    ]
    assert len(icon_labels) == 1

    # A normally visible param should NOT have the icon
    visible_control = widget.controls["VisibleString"]
    visible_icon_labels = [
        child
        for child in visible_control.findChildren(QLabel)
        if child.accessibleDescription() == hidden_hint
    ]
    assert len(visible_icon_labels) == 0
