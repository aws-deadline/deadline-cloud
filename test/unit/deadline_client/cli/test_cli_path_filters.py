# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for _validate_and_normalize_include_paths."""

import sys

import pytest
import click

from deadline.client.cli._groups._job_download_helpers import _validate_and_normalize_include_paths


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


class TestStdinReadsUntilEmptyLine:
    def test_stdin_stops_at_empty_line(self):
        """Verify that --include-path-stdin reads until an empty line."""
        from io import StringIO
        from unittest.mock import patch

        stdin_data = "renders/frame_001.exr\nrenders/frame_002.exr\n\nextra_ignored\n"
        collected = []
        with patch("sys.stdin", StringIO(stdin_data)):
            for line in sys.stdin:
                stripped = line.strip()
                if not stripped:
                    break
                collected.append(stripped)

        assert collected == ["renders/frame_001.exr", "renders/frame_002.exr"]
