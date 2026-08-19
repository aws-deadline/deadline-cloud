# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
from unittest.mock import MagicMock, patch

import pytest

try:
    from deadline.client.ui.dataclasses import JobBundleSettings
    from deadline.client.ui.widgets.job_bundle_settings_tab import JobBundleSettingsWidget
    import deadline.client.ui.dialogs.job_bundle_browser_dialog  # noqa: F401 - preload for patching
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


BROWSER_DIALOG = "deadline.client.ui.dialogs.job_bundle_browser_dialog.JobBundleBrowserDialog"
WIDGET_MODULE = "deadline.client.ui.widgets.job_bundle_settings_tab"


def _patch_browser(selected_path=None, accepted=True):
    """Patch JobBundleBrowserDialog with a mock class that returns a configured instance."""
    mock_instance = MagicMock()
    mock_instance.exec_.return_value = 1 if accepted else 0
    mock_instance.selected_path = selected_path
    mock_instance.selected_is_s3 = False
    mock_instance.selected_is_archive = False
    mock_instance.s3_repo = None
    mock_instance.resolve_selection.return_value = selected_path

    mock_cls = MagicMock(return_value=mock_instance)
    mock_cls.Accepted = 1
    return patch(BROWSER_DIALOG, mock_cls)


def test_on_load_bundle_loads_new_bundle_and_refreshes_dialog(
    widget, qtbot, fresh_deadline_config, tmp_path
):
    """Clicking 'Load a different job bundle' opens the browser dialog, loads the
    chosen bundle, and pushes its settings into the parent dialog via refresh().
    """
    second_bundle = tmp_path / "second_bundle"
    second_bundle.mkdir()
    _write_bundle(str(second_bundle), MINIMAL_TEMPLATE)

    parent_dialog = MagicMock()

    with (
        _patch_browser(selected_path=str(second_bundle)),
        patch.object(widget, "window", return_value=parent_dialog),
    ):
        widget.on_load_bundle(s3_repo=MagicMock())

    assert os.path.realpath(widget.input_job_bundle_dir) == os.path.realpath(str(second_bundle))
    parent_dialog.refresh.assert_called_once()

    kwargs = parent_dialog.refresh.call_args.kwargs
    assert kwargs["load_new_bundle"] is True
    assert kwargs["job_settings"].name == "Second Bundle"


def test_on_load_bundle_cancelled_dialog_is_noop(widget, qtbot):
    """If the user cancels the browser dialog, nothing changes."""
    original_dir = widget.input_job_bundle_dir
    parent_dialog = MagicMock()

    with _patch_browser(accepted=False), patch.object(widget, "window", return_value=parent_dialog):
        widget.on_load_bundle(s3_repo=MagicMock())

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
        _patch_browser(selected_path=str(bad_bundle)),
        patch.object(widget, "window", return_value=parent_dialog),
        patch(
            "deadline.client.ui.widgets.job_bundle_settings_tab.QMessageBox.warning"
        ) as mock_warning,
    ):
        widget.on_load_bundle(s3_repo=MagicMock())

    mock_warning.assert_called_once()
    parent_dialog.refresh.assert_not_called()
