# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for MCP bundle sharing tools."""

from unittest.mock import MagicMock, patch

from deadline._mcp.tools.bundles import download_bundle, list_shared_bundles

BUNDLE_GROUP = "deadline.client.cli._groups.bundle_group"


class TestListSharedBundles:
    @patch(f"{BUNDLE_GROUP}._apply_cli_options_to_config")
    @patch(f"{BUNDLE_GROUP}.S3BundleRepository.from_config")
    def test_returns_bundles_as_json(self, mock_from_config, mock_config):
        from deadline.client.job_bundle.repository import BrowseEntry

        mock_repo = MagicMock()
        mock_repo.root_path.return_value = "s3://bucket/DC/job-bundles/"
        mock_repo.list_entries.return_value = [
            BrowseEntry(name="render", path="s3://b/render.ojd", is_bundle=True, is_archive=True),
        ]
        mock_repo.get_hidden_set.return_value = set()
        mock_from_config.return_value = mock_repo

        result = list_shared_bundles()

        assert "bundles" in result
        assert result["bundles"][0]["name"] == "render"

    @patch(f"{BUNDLE_GROUP}._apply_cli_options_to_config")
    @patch(f"{BUNDLE_GROUP}.S3BundleRepository.from_config")
    def test_returns_error_on_failure(self, mock_from_config, mock_config):
        mock_from_config.side_effect = Exception("No credentials")

        result = list_shared_bundles()

        assert "error" in result


class TestDownloadBundle:
    @patch(f"{BUNDLE_GROUP}._apply_cli_options_to_config")
    @patch(f"{BUNDLE_GROUP}.S3BundleRepository.from_config")
    def test_returns_path_on_success(self, mock_from_config, mock_config, tmp_path):
        from deadline.client.job_bundle.repository import BrowseEntry

        mock_repo = MagicMock()
        mock_repo.root_path.return_value = "s3://bucket/DC/job-bundles/"
        mock_repo.list_entries.return_value = [
            BrowseEntry(
                name="my-bundle", path="s3://b/my-bundle.ojd", is_bundle=True, is_archive=True
            ),
        ]
        mock_repo.get_bundle_size.return_value = 100
        cache_dir = tmp_path / "cache" / "my-bundle"
        cache_dir.mkdir(parents=True)
        (cache_dir / "template.yaml").write_text("name: Test\n")
        mock_repo.download_full_bundle.return_value = str(cache_dir)
        mock_from_config.return_value = mock_repo

        result = download_bundle("my-bundle", output_dir=str(tmp_path / "out"))

        assert result["success"] is True
        assert "my-bundle" in result["path"]

    @patch(f"{BUNDLE_GROUP}._apply_cli_options_to_config")
    @patch(f"{BUNDLE_GROUP}.S3BundleRepository.from_config")
    def test_returns_error_when_not_found(self, mock_from_config, mock_config):
        from deadline.client.job_bundle.repository import BrowseEntry

        mock_repo = MagicMock()
        mock_repo.root_path.return_value = "s3://bucket/DC/job-bundles/"
        mock_repo.list_entries.return_value = [
            BrowseEntry(name="other", path="s3://b/other.ojd", is_bundle=True, is_archive=True),
        ]
        mock_from_config.return_value = mock_repo

        result = download_bundle("nonexistent")

        assert result["success"] is False
        assert "not found" in result["error"]
