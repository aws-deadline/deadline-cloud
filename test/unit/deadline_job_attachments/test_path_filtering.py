# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for path filtering in download.py"""

from typing import List

from deadline.job_attachments.download import (
    _matches_any_filter,
    _filter_paths,
)
from deadline.job_attachments.models import ManifestPathGroup
from deadline.job_attachments.asset_manifests.hash_algorithms import HashAlgorithm
from deadline.job_attachments.asset_manifests.v2023_03_03 import (
    ManifestPath as ManifestPathv2023_03_03,
)


class TestMatchesAnyFilter:
    def test_exact_match(self):
        assert _matches_any_filter("renders/frame_001.exr", ["renders/frame_001.exr"]) is True

    def test_exact_no_match(self):
        assert _matches_any_filter("renders/frame_002.exr", ["renders/frame_001.exr"]) is False

    def test_directory_prefix_match(self):
        assert _matches_any_filter("renders/frame_001.exr", ["renders/"]) is True

    def test_directory_prefix_no_match(self):
        assert _matches_any_filter("textures/wood.exr", ["renders/"]) is False

    def test_directory_prefix_does_not_match_similar_names(self):
        """'renders/' should NOT match 'renders_v2/file.exr'"""
        assert _matches_any_filter("renders_v2/file.exr", ["renders/"]) is False

    def test_multiple_filters_or(self):
        assert (
            _matches_any_filter("textures/wood.exr", ["renders/frame_001.exr", "textures/"]) is True
        )

    def test_empty_filters(self):
        assert _matches_any_filter("renders/frame_001.exr", []) is False

    def test_nested_directory_prefix(self):
        assert _matches_any_filter("a/b/c/file.txt", ["a/b/"]) is True

    def test_exact_match_no_trailing_slash(self):
        """Exact filter without trailing slash should not match subdirectory files."""
        assert _matches_any_filter("renders/frame_001.exr", ["renders"]) is False


class TestFilterPaths:
    def _make_group(self, paths: List[str]) -> ManifestPathGroup:
        group = ManifestPathGroup()
        group.files_by_hash_alg[HashAlgorithm.XXH128] = [
            ManifestPathv2023_03_03(path=p, hash="abc123", size=100, mtime=1234000000)
            for p in paths
        ]
        group.total_bytes = len(paths) * 100
        return group

    def test_exact_filter(self):
        paths_by_root = {"/root": self._make_group(["a.txt", "b.txt", "c.txt"])}
        result = _filter_paths(paths_by_root, ["b.txt"])
        assert list(result.keys()) == ["/root"]
        assert [f.path for f in result["/root"].files_by_hash_alg[HashAlgorithm.XXH128]] == [
            "b.txt"
        ]
        assert result["/root"].total_bytes == 100

    def test_directory_prefix_filter(self):
        paths_by_root = {
            "/root": self._make_group(["renders/a.exr", "renders/b.exr", "textures/c.png"])
        }
        result = _filter_paths(paths_by_root, ["renders/"])
        files = [f.path for f in result["/root"].files_by_hash_alg[HashAlgorithm.XXH128]]
        assert files == ["renders/a.exr", "renders/b.exr"]

    def test_no_matches_returns_empty(self):
        paths_by_root = {"/root": self._make_group(["a.txt"])}
        result = _filter_paths(paths_by_root, ["nonexistent.txt"])
        assert result == {}

    def test_multiple_asset_roots(self):
        paths_by_root = {
            "/root1": self._make_group(["shared/file.txt", "other.txt"]),
            "/root2": self._make_group(["shared/file.txt", "different.txt"]),
        }
        result = _filter_paths(paths_by_root, ["shared/file.txt"])
        assert "/root1" in result
        assert "/root2" in result

    def test_mixed_filters(self):
        paths_by_root = {
            "/root": self._make_group(
                ["renders/a.exr", "renders/b.exr", "textures/c.png", "scripts/setup.mel"]
            )
        }
        result = _filter_paths(paths_by_root, ["renders/", "scripts/setup.mel"])
        files = [f.path for f in result["/root"].files_by_hash_alg[HashAlgorithm.XXH128]]
        assert set(files) == {"renders/a.exr", "renders/b.exr", "scripts/setup.mel"}

    def test_empty_root_removed(self):
        paths_by_root = {
            "/has_match": self._make_group(["a.txt"]),
            "/no_match": self._make_group(["b.txt"]),
        }
        result = _filter_paths(paths_by_root, ["a.txt"])
        assert "/has_match" in result
        assert "/no_match" not in result
