"""Tests for the _gui_entry.py module that the Rust CLI spawns.

These tests verify the Python entry point creates the correct dialogs
and produces the expected JSON output format. They use pytest-qt's
qtbot fixture for headless Qt testing.

Prerequisites: maturin develop, pip install pytest-qt PySide6-essentials
"""

import json
import os
import pytest

# Skip entire module if PySide6 is not installed
pytest.importorskip("PySide6")


class TestGuiSubmitEntry:
    """Tests for gui-submit entry point."""

    def test_run_gui_submit_returns_canceled_on_close(self, qtbot):
        from deadline.client.ui._gui_entry import run_gui_submit

        bundle_dir = os.path.join(
            os.path.dirname(__file__),
            "..",
            "deadline",
            "client",
            "ui",
            "resources",
            "cli_job_bundle",
        )
        result = run_gui_submit(
            job_bundle_dir=bundle_dir,
            output="json",
            auto_close=True,
        )
        parsed = json.loads(result)
        assert parsed == {"status": "CANCELED"}

    def test_run_gui_submit_verbose_canceled(self, qtbot):
        from deadline.client.ui._gui_entry import run_gui_submit

        bundle_dir = os.path.join(
            os.path.dirname(__file__),
            "..",
            "deadline",
            "client",
            "ui",
            "resources",
            "cli_job_bundle",
        )
        result = run_gui_submit(
            job_bundle_dir=bundle_dir,
            output="verbose",
            auto_close=True,
        )
        assert "canceled" in result.lower()


class TestConfigGuiEntry:
    """Tests for config-gui entry point."""

    def test_run_config_gui_opens_without_error(self, qtbot):
        from deadline.client.ui._gui_entry import run_config_gui

        # Just verify it creates the dialog and returns without crashing
        result = run_config_gui(auto_close=True)
        assert result is None or isinstance(result, str)
