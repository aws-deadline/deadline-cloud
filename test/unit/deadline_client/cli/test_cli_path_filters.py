# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for _validate_and_normalize_include_paths."""

import pytest
import click

from deadline.client.cli._groups.job_group import _validate_and_normalize_include_paths


class TestValidateAndNormalizePathFilters:
    def test_rejects_double_dot(self):
        with pytest.raises(click.BadParameter, match="must not contain '..'"):
            _validate_and_normalize_include_paths(["../../etc/passwd"])

    def test_rejects_double_dot_in_middle(self):
        with pytest.raises(click.BadParameter, match="must not contain '..'"):
            _validate_and_normalize_include_paths(["renders/../secret"])

    def test_converts_backslashes(self):
        result = _validate_and_normalize_include_paths(["renders\\frame_001.exr"])
        assert result == ["renders/frame_001.exr"]

    def test_strips_leading_dot_slash(self):
        result = _validate_and_normalize_include_paths(["./renders/frame.exr"])
        assert result == ["renders/frame.exr"]

    def test_collapses_double_slashes(self):
        result = _validate_and_normalize_include_paths(["renders//frame.exr"])
        assert result == ["renders/frame.exr"]

    def test_empty_filter_removed(self):
        result = _validate_and_normalize_include_paths(["", "a.txt"])
        assert result == ["a.txt"]

    def test_passthrough_normal_paths(self):
        result = _validate_and_normalize_include_paths(
            ["renders/frame_001.exr", "textures/", "scripts/setup.mel"]
        )
        assert result == ["renders/frame_001.exr", "textures/", "scripts/setup.mel"]

    def test_backslash_with_double_dot_rejected(self):
        """Backslash path traversal like 'renders\\..\\..' should be rejected."""
        with pytest.raises(click.BadParameter, match="must not contain '..'"):
            _validate_and_normalize_include_paths(["renders\\..\\secret"])

    def test_multiple_normalizations(self):
        result = _validate_and_normalize_include_paths([".\\renders\\\\frame.exr"])
        # .\ -> ./ after backslash conversion, then ./ stripped, // collapsed
        assert result == ["renders/frame.exr"]


class TestParseFiltersStdin:
    def test_stdin_reads_until_empty_line(self):
        from unittest.mock import patch
        from io import StringIO
        from deadline.client.cli._groups.job_group import _parse_filters_and_config

        stdin_data = "renders/frame_001.exr\nrenders/frame_002.exr\n\nextra_ignored\n"
        with patch("sys.stdin", StringIO(stdin_data)), patch(
            "deadline.client.cli._groups.job_group._apply_cli_options_to_config"
        ), patch("deadline.client.cli._groups.job_group.config_file") as mock_cf:
            mock_cf.get_setting.return_value = "test-id"
            filters, _, _, _, _ = _parse_filters_and_config(
                (), True, {"yes": False, "profile": None}
            )

        assert filters == ["renders/frame_001.exr", "renders/frame_002.exr"]
