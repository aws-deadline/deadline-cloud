# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for MCP bundle sharing tools."""

from unittest.mock import MagicMock, patch

from deadline._mcp.tools.bundles import download_bundle, list_shared_bundles, upload_bundle

BUNDLE_GROUP = "deadline.client.cli._groups.bundle_group"
BUNDLES_TOOLS = "deadline._mcp.tools.bundles"


class TestUploadBundleOverwrite:
    """upload_bundle must not silently clobber another user's shared bundle: it
    only passes --yes when the caller explicitly opts in, and never pipes 'y'."""

    def _invoke_args(self, mock_runner_cls):
        result = MagicMock()
        result.exit_code = 0
        result.output = "ok"
        mock_runner_cls.return_value.invoke.return_value = result
        return mock_runner_cls.return_value.invoke

    def test_default_does_not_overwrite_or_pipe_yes(self):
        with patch(f"{BUNDLES_TOOLS}.CliRunner") as runner_cls:
            invoke = self._invoke_args(runner_cls)
            upload_bundle("/some/bundle")
        args, kwargs = invoke.call_args
        assert "--yes" not in args[1]
        assert "input" not in kwargs  # the old auto-"y" answer is gone

    def test_overwrite_passes_yes(self):
        with patch(f"{BUNDLES_TOOLS}.CliRunner") as runner_cls:
            invoke = self._invoke_args(runner_cls)
            upload_bundle("/some/bundle", overwrite=True)
        args, _kwargs = invoke.call_args
        assert "--yes" in args[1]


class TestDownloadBundleOverwrite:
    def test_overwrite_passes_yes(self):
        with patch(f"{BUNDLES_TOOLS}.CliRunner") as runner_cls:
            result = MagicMock()
            result.exit_code = 0
            result.output = '{"path": "/out/b"}'
            result.stdout = '{"path": "/out/b"}'
            runner_cls.return_value.invoke.return_value = result
            download_bundle("b", output_dir="/out", overwrite=True)
        args, _kwargs = runner_cls.return_value.invoke.call_args
        assert "--yes" in args[1]

    def test_default_does_not_pass_yes(self):
        with patch(f"{BUNDLES_TOOLS}.CliRunner") as runner_cls:
            result = MagicMock()
            result.exit_code = 0
            result.output = '{"path": "/out/b"}'
            result.stdout = '{"path": "/out/b"}'
            runner_cls.return_value.invoke.return_value = result
            download_bundle("b", output_dir="/out")
        args, _kwargs = runner_cls.return_value.invoke.call_args
        assert "--yes" not in args[1]


class TestListSharedBundles:
    @patch(f"{BUNDLE_GROUP}._apply_cli_options_to_config")
    @patch(f"{BUNDLE_GROUP}.S3BundleRepository.from_config")
    def test_returns_bundles_as_json(self, mock_from_config, mock_config):
        from deadline.client.job_bundle._repository import BrowseEntry

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
        from deadline.client.job_bundle._repository import BrowseEntry

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
        from deadline.client.job_bundle._repository import BrowseEntry

        mock_repo = MagicMock()
        mock_repo.root_path.return_value = "s3://bucket/DC/job-bundles/"
        mock_repo.list_entries.return_value = [
            BrowseEntry(name="other", path="s3://b/other.ojd", is_bundle=True, is_archive=True),
        ]
        mock_from_config.return_value = mock_repo

        result = download_bundle("nonexistent")

        assert result["success"] is False
        assert "not found" in result["error"]


class TestJsonRobustness:
    """A zero-exit result whose stdout isn't valid JSON (e.g. a stray warning
    line merged in on older click) must be reported, not raise out of the tool."""

    def test_list_reports_non_json_output(self):
        with patch(f"{BUNDLES_TOOLS}.CliRunner") as runner_cls:
            result_obj = MagicMock()
            result_obj.exit_code = 0
            result_obj.output = "WARNING: deprecated\n[not json"
            result_obj.stdout = "[not json"
            runner_cls.return_value.invoke.return_value = result_obj

            result = list_shared_bundles()

        assert result["success"] is False
        assert "not json" in result["error"]

    def test_list_success_key_present_on_happy_path(self):
        from deadline._mcp.tools.bundles import list_shared_bundles as _list

        with patch(f"{BUNDLES_TOOLS}.CliRunner") as runner_cls:
            result_obj = MagicMock()
            result_obj.exit_code = 0
            result_obj.output = '[{"name": "render"}]'
            result_obj.stdout = '[{"name": "render"}]'
            runner_cls.return_value.invoke.return_value = result_obj

            result = _list()

        assert result["success"] is True
        assert result["bundles"][0]["name"] == "render"

    def test_download_reports_non_json_output(self):
        with patch(f"{BUNDLES_TOOLS}.CliRunner") as runner_cls:
            result_obj = MagicMock()
            result_obj.exit_code = 0
            result_obj.output = "Downloading\n{bad"
            result_obj.stdout = "{bad"
            runner_cls.return_value.invoke.return_value = result_obj

            result = download_bundle("b")

        assert result["success"] is False


class TestOverwriteParamRegistered:
    """The MCP-exposed signature is built from the registry's param_names, so
    `overwrite` must be registered or the model can never pass it (and a repeat
    download/upload to an existing destination is unrecoverable)."""

    def test_upload_and_download_expose_overwrite(self):
        from deadline._mcp.registry import TOOL_REGISTRY

        upload_params = TOOL_REGISTRY["upload_bundle"]["param_names"]
        download_params = TOOL_REGISTRY["download_bundle"]["param_names"]
        assert upload_params is not None and "overwrite" in upload_params
        assert download_params is not None and "overwrite" in download_params

    def test_registered_params_match_the_function_signature(self):
        import inspect
        from deadline._mcp.registry import TOOL_REGISTRY

        for name in ("upload_bundle", "download_bundle", "list_shared_bundles"):
            entry = TOOL_REGISTRY[name]
            param_names = entry["param_names"]
            assert param_names is not None
            sig_params = set(inspect.signature(entry["func"]).parameters)
            # Every documented tool parameter must be exposed (no silent drops).
            assert set(param_names) == sig_params, f"{name}: {set(param_names)} != {sig_params}"
