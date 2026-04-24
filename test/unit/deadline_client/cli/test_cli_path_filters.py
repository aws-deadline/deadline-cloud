# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for _normalize_filters and _parse_include_filters."""

from deadline.client.cli._groups._job_download_helpers import (
    _normalize_filters,
    _parse_include_filters,
)


class TestNormalizeFilters:
    def test_converts_backslashes(self):
        result = _normalize_filters(["renders\\frame_001.exr"])
        assert result == ["renders/frame_001.exr"]

    def test_strips_leading_dot_slash(self):
        result = _normalize_filters(["./renders/frame.exr"])
        assert result == ["renders/frame.exr"]

    def test_collapses_double_slashes(self):
        result = _normalize_filters(["renders//frame.exr"])
        assert result == ["renders/frame.exr"]

    def test_empty_filter_removed(self):
        result = _normalize_filters(["", "a.txt"])
        assert result == ["a.txt"]

    def test_passthrough_normal_paths(self):
        result = _normalize_filters(["renders/frame_001.exr", "textures/", "scripts/setup.mel"])
        assert result == ["renders/frame_001.exr", "textures/", "scripts/setup.mel"]

    def test_passthrough_glob_patterns(self):
        result = _normalize_filters(["renders/*.exr", "**/*.png", "textures/wood[0-9].jpg"])
        assert result == ["renders/*.exr", "**/*.png", "textures/wood[0-9].jpg"]

    def test_multiple_normalizations(self):
        result = _normalize_filters([".\\renders\\\\frame.exr"])
        assert result == ["renders/frame.exr"]


class TestParseIncludeFilters:
    def test_include_only(self):
        result = _parse_include_filters(("renders/",))
        assert result == ["renders/"]

    def test_no_filters(self):
        result = _parse_include_filters(())
        assert result is None

    def test_multiple_include_patterns(self):
        result = _parse_include_filters(("*.exr", "*/renders/*.png"))
        assert result == ["*.exr", "*/renders/*.png"]

    def test_relative_paths(self):
        result = _parse_include_filters(("renders/frame_001.exr", "logs/render.log"))
        assert result == ["renders/frame_001.exr", "logs/render.log"]

    def test_mixed_globs_and_relative(self):
        result = _parse_include_filters(("*.exr", "renders/frame_001.exr"))
        assert result == ["*.exr", "renders/frame_001.exr"]
