# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
from unittest.mock import MagicMock, patch

import pytest

try:
    from deadline.client.ui.dataclasses import JobBundleSettings
    from deadline.client.ui.widgets.job_bundle_settings_tab import JobBundleSettingsWidget
except ImportError:
    pytest.importorskip("deadline.client.ui.widgets.job_bundle_settings_tab")


MINIMAL_TEMPLATE = """
specificationVersion: 'jobtemplate-2023-09'
name: Second Bundle
parameterDefinitions:
- name: Greeting
  type: STRING
  default: hello
steps:
- name: NoOp
  script:
    actions:
      onRun:
        command: "echo hi"
"""


def _write_bundle(directory: str, template: str) -> None:
    with open(os.path.join(directory, "template.yaml"), "w", encoding="utf8") as f:
        f.write(template)


@pytest.fixture
def widget(qtbot, temp_job_bundle_dir):
    _write_bundle(temp_job_bundle_dir, MINIMAL_TEMPLATE.replace("Second Bundle", "First Bundle"))
    initial_settings = JobBundleSettings(
        input_job_bundle_dir=temp_job_bundle_dir, name="First Bundle"
    )
    w = JobBundleSettingsWidget(initial_settings=initial_settings)
    qtbot.addWidget(w)
    return w


def test_on_load_bundle_loads_new_bundle_and_refreshes_dialog(
    widget, qtbot, fresh_deadline_config, tmp_path
):
    """Clicking 'Load a different job bundle' opens a file picker, loads the
    chosen bundle, and pushes its settings into the parent dialog via refresh().
    """
    second_bundle = tmp_path / "second_bundle"
    second_bundle.mkdir()
    _write_bundle(str(second_bundle), MINIMAL_TEMPLATE)

    parent_dialog = MagicMock()

    with (
        patch(
            "deadline.client.ui.widgets.job_bundle_settings_tab.QFileDialog.getExistingDirectory",
            return_value=str(second_bundle),
        ),
        patch.object(widget, "window", return_value=parent_dialog),
    ):
        widget.on_load_bundle()

    assert widget.input_job_bundle_dir == str(second_bundle)
    parent_dialog.refresh.assert_called_once()

    kwargs = parent_dialog.refresh.call_args.kwargs
    assert kwargs["load_new_bundle"] is True
    assert kwargs["job_settings"].input_job_bundle_dir == str(second_bundle)
    assert kwargs["job_settings"].name == "Second Bundle"


def test_on_load_bundle_cancelled_dialog_is_noop(widget, qtbot):
    """If the user cancels the file picker, nothing changes."""
    original_dir = widget.input_job_bundle_dir
    parent_dialog = MagicMock()

    with (
        patch(
            "deadline.client.ui.widgets.job_bundle_settings_tab.QFileDialog.getExistingDirectory",
            return_value="",
        ),
        patch.object(widget, "window", return_value=parent_dialog),
    ):
        widget.on_load_bundle()

    assert widget.input_job_bundle_dir == original_dir
    parent_dialog.refresh.assert_not_called()


def test_on_load_bundle_invalid_bundle_shows_warning(
    widget, qtbot, fresh_deadline_config, tmp_path
):
    """If the chosen directory isn't a valid bundle, show a warning and don't
    refresh the parent dialog."""
    bad_bundle = tmp_path / "not_a_bundle"
    bad_bundle.mkdir()  # empty — no template.yaml

    parent_dialog = MagicMock()

    with (
        patch(
            "deadline.client.ui.widgets.job_bundle_settings_tab.QFileDialog.getExistingDirectory",
            return_value=str(bad_bundle),
        ),
        patch.object(widget, "window", return_value=parent_dialog),
        patch(
            "deadline.client.ui.widgets.job_bundle_settings_tab.QMessageBox.warning"
        ) as mock_warning,
    ):
        widget.on_load_bundle()

    mock_warning.assert_called_once()
    parent_dialog.refresh.assert_not_called()
