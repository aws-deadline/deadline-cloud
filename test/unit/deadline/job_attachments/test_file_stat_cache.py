# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import stat
from pathlib import Path
from unittest.mock import patch, MagicMock

from deadline.job_attachments.upload import _FileStatCache


class TestFileStatCache:
    def test_get_stat_caches_result(self, tmp_path):
        """Test that stat results are cached and not called multiple times"""
        cache = _FileStatCache()
        test_file = tmp_path / "test.txt"
        test_file.write_text("test")

        with patch.object(Path, "stat") as mock_stat:
            mock_stat.return_value = MagicMock()

            # First call should invoke stat
            result1 = cache._get_stat(test_file)
            assert mock_stat.call_count == 1

            # Second call should use cache
            result2 = cache._get_stat(test_file)
            assert mock_stat.call_count == 1
            assert result1 is result2

    def test_get_stat_handles_missing_file(self, tmp_path):
        """Test that missing files return None and are cached"""
        cache = _FileStatCache()
        missing_file = tmp_path / "missing.txt"

        result1 = cache._get_stat(missing_file)
        result2 = cache._get_stat(missing_file)

        assert result1 is None
        assert result2 is None

    def test_exists_with_existing_file(self, tmp_path):
        """Test exists() returns True for existing files"""
        cache = _FileStatCache()
        test_file = tmp_path / "test.txt"
        test_file.write_text("test")

        assert cache.exists(test_file) is True

    def test_exists_with_missing_file(self, tmp_path):
        """Test exists() returns False for missing files"""
        cache = _FileStatCache()
        missing_file = tmp_path / "missing.txt"

        assert cache.exists(missing_file) is False

    def test_is_dir_with_directory(self, tmp_path):
        """Test is_dir() returns True for directories"""
        cache = _FileStatCache()
        test_dir = tmp_path / "testdir"
        test_dir.mkdir()

        assert cache.is_dir(test_dir) is True

    def test_is_dir_with_file(self, tmp_path):
        """Test is_dir() returns False for files"""
        cache = _FileStatCache()
        test_file = tmp_path / "test.txt"
        test_file.write_text("test")

        assert cache.is_dir(test_file) is False

    def test_is_dir_with_missing_path(self, tmp_path):
        """Test is_dir() returns False for missing paths"""
        cache = _FileStatCache()
        missing_path = tmp_path / "missing"

        assert cache.is_dir(missing_path) is False

    def test_get_size_with_file(self, tmp_path):
        """Test get_size() returns correct file size"""
        cache = _FileStatCache()
        test_file = tmp_path / "test.txt"
        content = "test content"
        test_file.write_text(content)

        size = cache.get_size(test_file)
        assert size == len(content.encode())

    def test_get_size_with_missing_file(self, tmp_path, caplog):
        """Test get_size() returns 0 for missing files and emits the expected message"""
        cache = _FileStatCache()
        missing_file = tmp_path / "missing.txt"

        assert cache.get_size(missing_file) == 0
        assert "Skipping file in size calculation" in caplog.text

    def test_cache_reuse_across_methods(self, tmp_path):
        """Test that cache is shared across different methods"""
        cache = _FileStatCache()
        test_file = tmp_path / "test.txt"
        test_file.write_text("test")

        with patch.object(Path, "stat") as mock_stat:
            mock_stat.return_value = MagicMock(st_mode=stat.S_IFREG, st_size=4)

            # Call different methods
            cache.exists(test_file)
            cache.is_dir(test_file)
            cache.get_size(test_file)

            # Should only call stat once
            assert mock_stat.call_count == 1
