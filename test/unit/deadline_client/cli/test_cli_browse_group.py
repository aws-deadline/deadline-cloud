# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""Tests for browse_group.py CLI entry point."""

from __future__ import annotations

from unittest.mock import patch

import click
import pytest

pytest.importorskip("rich", reason="TUI tests require the 'rich' package (deadline[tui])")

from deadline.client.cli._groups.browse_group import _check_tui_installed


class TestCheckTuiInstalled:
    def test_rich_available(self):
        """Should not raise when rich is installed."""
        _check_tui_installed()  # rich is installed in test env

    @patch.dict("sys.modules", {"rich": None})
    def test_rich_missing(self):
        """Should raise ClickException when rich is not importable."""
        with patch("builtins.__import__", side_effect=ImportError):
            with pytest.raises(click.ClickException, match="rich"):
                _check_tui_installed()
