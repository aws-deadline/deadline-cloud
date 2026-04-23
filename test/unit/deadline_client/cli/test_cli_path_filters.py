# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for _normalize_filters and _parse_include_exclude."""

from deadline.client.cli._groups._job_download_helpers import (
    _normalize_filters,
    _parse_include_exclude,
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


class TestParseIncludeExclude:
    def test_include_only(self):
        inc, exc = _parse_include_exclude(("renders/",), (), None)
        assert inc == ["renders/"]
        assert exc is None

    def test_exclude_only(self):
        inc, exc = _parse_include_exclude((), ("*.log",), None)
        assert inc == ["*"]
        assert exc == ["*.log"]

    def test_include_and_exclude(self):
        inc, exc = _parse_include_exclude(("renders/",), ("renders/draft/",), None)
        assert inc == ["renders/"]
        assert exc == ["renders/draft/"]

    def test_no_filters(self):
        inc, exc = _parse_include_exclude((), (), None)
        assert inc is None
        assert exc is None

    def test_config_json_string(self, tmp_path):
        inc, exc = _parse_include_exclude(
            (), (), '{"include": ["renders/*.exr"], "exclude": ["renders/draft/"]}'
        )
        assert inc == ["renders/*.exr"]
        assert exc == ["renders/draft/"]

    def test_config_file(self, tmp_path):
        config_file = tmp_path / "filters.json"
        config_file.write_text('{"include": ["**/*.exr"]}')
        inc, exc = _parse_include_exclude((), (), str(config_file))
        assert inc == ["**/*.exr"]
        assert exc is None

    def test_cli_args_take_precedence_over_config(self):
        inc, exc = _parse_include_exclude(("renders/",), (), '{"include": ["textures/"]}')
        assert inc == ["renders/"]
        assert exc is None
