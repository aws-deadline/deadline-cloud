# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for _normalize_filters."""

from deadline.client.cli._groups._job_download_helpers import (
    _normalize_filters,
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

    def test_empty_input_returns_empty(self):
        assert _normalize_filters([]) == []

    def test_all_empty_returns_empty(self):
        assert _normalize_filters(["", ""]) == []
