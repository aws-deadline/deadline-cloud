# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for path filtering in download.py"""

from typing import List

from deadline.job_attachments.download import (
    _matches_any_filter,
    _full_path,
    _filter_paths,
    _filter_manifests,
)
from deadline.job_attachments.models import ManifestPathGroup
from deadline.job_attachments.asset_manifests.base_manifest import (
    BaseAssetManifest,
    BaseManifestPath,
)
from deadline.job_attachments.asset_manifests.hash_algorithms import HashAlgorithm
from deadline.job_attachments.asset_manifests.v2023_03_03 import (
    AssetManifest as AssetManifestv2023_03_03,
    ManifestPath as ManifestPathv2023_03_03,
)


class TestMatchesAnyFilter:
    def test_exact_match(self):
        assert _matches_any_filter("/root/renders/frame_001.exr", ["renders/frame_001.exr"]) is True

    def test_exact_no_match(self):
        assert (
            _matches_any_filter("/root/renders/frame_002.exr", ["renders/frame_001.exr"]) is False
        )

    def test_directory_prefix_match(self):
        assert _matches_any_filter("/root/renders/frame_001.exr", ["/root/renders/"]) is True

    def test_directory_prefix_no_match(self):
        assert _matches_any_filter("/root/textures/wood.exr", ["/root/renders/"]) is False

    def test_directory_prefix_does_not_match_similar_names(self):
        """'renders/' should NOT match 'renders_v2/file.exr'"""
        assert _matches_any_filter("/root/renders_v2/file.exr", ["/root/renders/"]) is False

    def test_multiple_filters_or(self):
        assert (
            _matches_any_filter(
                "/root/textures/wood.exr", ["renders/frame_001.exr", "/root/textures/"]
            )
            is True
        )

    def test_empty_filters(self):
        assert _matches_any_filter("/root/renders/frame_001.exr", []) is False

    def test_nested_directory_prefix(self):
        assert _matches_any_filter("/root/a/b/c/file.txt", ["/root/a/b/"]) is True

    def test_glob_wildcard(self):
        assert _matches_any_filter("/root/renders/frame_001.exr", ["renders/*.exr"]) is True

    def test_glob_wildcard_no_match(self):
        assert _matches_any_filter("/root/renders/frame_001.png", ["renders/*.exr"]) is False

    def test_glob_question_mark(self):
        assert _matches_any_filter("/root/renders/frame_00x.exr", ["renders/frame_00?.exr"]) is True

    def test_glob_recursive(self):
        assert _matches_any_filter("/root/a/b/c.txt", ["*/a/*/c.txt"]) is True

    def test_glob_full_path_wildcard(self):
        """Patterns like '*/renders/*.png' should match against full paths."""
        assert _matches_any_filter("/home/user/renders/frame.png", ["*/renders/*.png"]) is True

    def test_glob_extension_only(self):
        """Simple extension patterns like '*.png' should match full paths."""
        assert _matches_any_filter("/root/renders/frame.png", ["*.png"]) is True

    def test_relative_path_suffix_match(self):
        """Relative paths are auto-prepended with */ and matched via fnmatch."""
        assert _matches_any_filter("/home/user/renders/frame.exr", ["renders/frame.exr"]) is True

    def test_relative_path_no_partial_match(self):
        """Relative path must match complete path segments."""
        assert _matches_any_filter("/home/user/xrenders/frame.exr", ["renders/frame.exr"]) is False

    def test_relative_path_exact_file(self):
        """Single filename matches anywhere under root."""
        assert _matches_any_filter("/root/renders/frame.exr", ["frame.exr"]) is True

    def test_relative_path_no_match(self):
        assert _matches_any_filter("/root/renders/frame.exr", ["other.exr"]) is False

    def test_relative_glob(self):
        """Relative globs like 'renders/*.exr' match without needing a leading */."""
        assert _matches_any_filter("/home/user/renders/frame.exr", ["renders/*.exr"]) is True

    def test_relative_glob_no_match(self):
        assert _matches_any_filter("/home/user/renders/frame.png", ["renders/*.exr"]) is False

    def test_windows_full_path_with_glob(self):
        """Windows full paths (normalized) match glob patterns."""
        assert (
            _matches_any_filter("C:/Users/artist/project/renders/frame.exr", ["*/renders/*.exr"])
            is True
        )

    def test_windows_full_path_with_relative_filter(self):
        """Relative filters match against normalized Windows full paths."""
        assert (
            _matches_any_filter("C:/Users/artist/project/renders/frame.exr", ["renders/frame.exr"])
            is True
        )

    def test_windows_full_path_with_directory_filter(self):
        """Directory filters match against normalized Windows full paths."""
        assert (
            _matches_any_filter(
                "C:/Users/artist/project/renders/frame.exr", ["C:/Users/artist/project/renders/"]
            )
            is True
        )

    def test_windows_full_path_extension_glob(self):
        assert _matches_any_filter("C:/Users/artist/project/frame.exr", ["*.exr"]) is True


class TestFullPath:
    def test_unix_root(self):
        assert _full_path("/home/user", "renders/frame.png") == "/home/user/renders/frame.png"

    def test_windows_root_backslashes(self):
        """Windows root paths with backslashes are normalized to forward slashes."""
        assert (
            _full_path("C:\\Users\\artist\\project", "renders/frame.png")
            == "C:/Users/artist/project/renders/frame.png"
        )

    def test_trailing_slash_root(self):
        assert _full_path("/root/", "file.txt") == "/root/file.txt"

    def test_trailing_backslash_root(self):
        assert _full_path("C:\\root\\", "file.txt") == "C:/root/file.txt"


class TestFilterPaths:
    def _make_group(self, paths: List[str]) -> ManifestPathGroup:
        group = ManifestPathGroup()
        group.files_by_hash_alg[HashAlgorithm.XXH128] = [
            ManifestPathv2023_03_03(path=p, hash="abc123", size=100, mtime=1234000000)
            for p in paths
        ]
        group.total_bytes = len(paths) * 100
        return group

    def test_glob_against_full_path(self):
        """Filters match against root + relative path."""
        paths_by_root = {
            "/home/user/project": self._make_group(["renders/a.exr", "textures/b.png"])
        }
        result = _filter_paths(paths_by_root, ["*/renders/*.exr"])
        files = [
            f.path for f in result["/home/user/project"].files_by_hash_alg[HashAlgorithm.XXH128]
        ]
        assert files == ["renders/a.exr"]

    def test_extension_filter_against_full_path(self):
        """Simple extension patterns match against full paths."""
        paths_by_root = {"/root": self._make_group(["a.exr", "b.png"])}
        result = _filter_paths(paths_by_root, ["*.exr"])
        files = [f.path for f in result["/root"].files_by_hash_alg[HashAlgorithm.XXH128]]
        assert files == ["a.exr"]

    def test_directory_prefix_filter(self):
        paths_by_root = {
            "/root": self._make_group(["renders/a.exr", "renders/b.exr", "textures/c.png"])
        }
        result = _filter_paths(paths_by_root, ["/root/renders/"])
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
        result = _filter_paths(paths_by_root, ["*/shared/file.txt"])
        assert "/root1" in result
        assert "/root2" in result

    def test_mixed_filters(self):
        paths_by_root = {
            "/root": self._make_group(
                ["renders/a.exr", "renders/b.exr", "textures/c.png", "scripts/setup.mel"]
            )
        }
        result = _filter_paths(paths_by_root, ["/root/renders/", "*/setup.mel"])
        files = [f.path for f in result["/root"].files_by_hash_alg[HashAlgorithm.XXH128]]
        assert set(files) == {"renders/a.exr", "renders/b.exr", "scripts/setup.mel"}

    def test_empty_root_removed(self):
        paths_by_root = {
            "/has_match": self._make_group(["a.txt"]),
            "/no_match": self._make_group(["b.txt"]),
        }
        result = _filter_paths(paths_by_root, ["*/a.txt"])
        assert "/has_match" in result
        assert "/no_match" not in result

    def test_glob_pattern(self):
        paths_by_root = {
            "/root": self._make_group(["renders/a.exr", "renders/b.png", "textures/c.exr"])
        }
        result = _filter_paths(paths_by_root, ["*/renders/*.exr"])
        files = [f.path for f in result["/root"].files_by_hash_alg[HashAlgorithm.XXH128]]
        assert files == ["renders/a.exr"]

    def test_windows_root_path(self):
        """Windows backslash roots are normalized so forward-slash patterns match."""
        paths_by_root = {
            "C:\\Users\\artist\\project": self._make_group(["renders/a.exr", "logs/b.log"])
        }
        result = _filter_paths(paths_by_root, ["*/renders/*.exr"])
        files = [
            f.path
            for f in result["C:\\Users\\artist\\project"].files_by_hash_alg[HashAlgorithm.XXH128]
        ]
        assert files == ["renders/a.exr"]

    def test_relative_path_filter(self):
        """Plain relative paths match as suffix against full path."""
        paths_by_root = {"/home/user/project": self._make_group(["renders/a.exr", "logs/b.log"])}
        result = _filter_paths(paths_by_root, ["renders/a.exr"])
        files = [
            f.path for f in result["/home/user/project"].files_by_hash_alg[HashAlgorithm.XXH128]
        ]
        assert files == ["renders/a.exr"]


class TestFilterManifests:
    def _make_manifest(self, paths: List[str]) -> BaseAssetManifest:
        manifest_paths: List[BaseManifestPath] = [
            ManifestPathv2023_03_03(path=p, hash="abc123", size=100, mtime=1234000000)
            for p in paths
        ]
        return AssetManifestv2023_03_03(
            hash_alg=HashAlgorithm.XXH128,
            paths=manifest_paths,
            total_size=len(paths) * 100,
        )

    def test_glob_against_full_path(self):
        """Filters match against root + relative path."""
        manifests_by_root = {
            "/home/user": [self._make_manifest(["renders/a.exr", "textures/b.png"])]
        }
        result = _filter_manifests(manifests_by_root, ["*/renders/*.exr"])
        assert [p.path for p in result["/home/user"][0].paths] == ["renders/a.exr"]

    def test_directory_prefix_filter(self):
        manifests_by_root = {
            "/root": [self._make_manifest(["renders/a.exr", "renders/b.exr", "textures/c.png"])]
        }
        result = _filter_manifests(manifests_by_root, ["/root/renders/"])
        assert [p.path for p in result["/root"][0].paths] == ["renders/a.exr", "renders/b.exr"]

    def test_no_matches_returns_empty(self):
        manifests_by_root = {"/root": [self._make_manifest(["a.txt"])]}
        result = _filter_manifests(manifests_by_root, ["nonexistent.txt"])
        assert result == {}

    def test_empty_root_removed(self):
        manifests_by_root = {
            "/has_match": [self._make_manifest(["a.txt"])],
            "/no_match": [self._make_manifest(["b.txt"])],
        }
        result = _filter_manifests(manifests_by_root, ["*/a.txt"])
        assert "/has_match" in result
        assert "/no_match" not in result

    def test_multiple_manifests_per_root(self):
        manifests_by_root = {
            "/root": [
                self._make_manifest(["a.txt", "b.txt"]),
                self._make_manifest(["c.txt", "d.txt"]),
            ]
        }
        result = _filter_manifests(manifests_by_root, ["*/a.txt", "*/c.txt"])
        assert len(result["/root"]) == 2
        assert [p.path for p in result["/root"][0].paths] == ["a.txt"]
        assert [p.path for p in result["/root"][1].paths] == ["c.txt"]
