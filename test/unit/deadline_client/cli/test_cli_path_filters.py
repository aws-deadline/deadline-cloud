# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for _normalize_filters and _parse_include_config."""

from deadline.client.cli._groups._job_download_helpers import (
    _normalize_filters,
    _parse_include_config,
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


class TestParseIncludeConfig:
    def test_include_only(self):
        result = _parse_include_config(("renders/",), None)
        assert result == ["renders/"]

    def test_no_filters(self):
        result = _parse_include_config((), None)
        assert result is None

    def test_config_json_string(self):
        result = _parse_include_config((), '{"include": ["renders/*.exr"]}')
        assert result == ["renders/*.exr"]

    def test_config_file(self, tmp_path):
        config_file = tmp_path / "filters.json"
        config_file.write_text('{"include": ["**/*.exr"]}')
        result = _parse_include_config((), str(config_file))
        assert result == ["**/*.exr"]

    def test_cli_args_take_precedence_over_config(self):
        result = _parse_include_config(("renders/",), '{"include": ["textures/"]}')
        assert result == ["renders/"]

    def test_multiple_include_patterns(self):
        result = _parse_include_config(("*.exr", "*/renders/*.png"), None)
        assert result == ["*.exr", "*/renders/*.png"]
