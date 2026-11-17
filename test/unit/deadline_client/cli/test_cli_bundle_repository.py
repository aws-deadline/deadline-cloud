# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for the bundle CLI commands (list, upload, download, cache)."""

import io
import json
import os
import time
import zipfile

import yaml
from botocore.exceptions import ClientError
from click.testing import CliRunner
from unittest.mock import MagicMock, patch

from deadline.client.cli import main
from deadline.client.job_bundle._repository import (
    BrowseEntry,
    LocalBundleRepository,
    METADATA_LIMIT_NAME,
    S3_METADATA_TOTAL_BUDGET,
    S3BundleRepository,
)

BUNDLE_GROUP = "deadline.client.cli._groups.bundle_group"


class TestBundleDownloadOverwrite:
    """`bundle download -o <dir>` must not silently delete an existing directory."""

    def _mock_repo(self, tmp_path):
        # A cache dir that download_full_bundle "resolves" the bundle to.
        cache = tmp_path / "cache" / "my-bundle"
        cache.mkdir(parents=True)
        (cache / "template.yaml").write_text("name: Test\n")
        repo = MagicMock()
        repo.root_path.return_value = "s3://b/DC/job-bundles/"
        repo.list_entries.return_value = [
            BrowseEntry(
                name="my-bundle", path="s3://b/my-bundle.ojd", is_bundle=True, is_archive=True
            ),
        ]
        repo.get_bundle_size.return_value = 10
        repo.download_full_bundle.return_value = str(cache)
        return repo

    @patch(f"{BUNDLE_GROUP}._apply_cli_options_to_config")
    @patch(f"{BUNDLE_GROUP}.S3BundleRepository.from_config")
    def test_refuses_to_overwrite_non_bundle_even_with_yes(
        self, mock_from_config, mock_config, tmp_path
    ):
        """A collision with a folder the tool does not own is never deleted."""
        mock_from_config.return_value = self._mock_repo(tmp_path)
        out = tmp_path / "out"
        collision = out / "my-bundle"  # a user's project dir, NOT a bundle
        collision.mkdir(parents=True)
        (collision / "keep.txt").write_text("precious")

        result = CliRunner().invoke(
            main, ["bundle", "download", "my-bundle", "-o", str(out), "--yes"]
        )

        assert result.exit_code != 0
        assert "not a job bundle" in result.output
        assert (collision / "keep.txt").exists(), "must never delete a non-bundle folder"

    @patch(f"{BUNDLE_GROUP}._apply_cli_options_to_config")
    @patch(f"{BUNDLE_GROUP}.S3BundleRepository.from_config")
    def test_existing_bundle_declined_without_yes(self, mock_from_config, mock_config, tmp_path):
        mock_from_config.return_value = self._mock_repo(tmp_path)
        out = tmp_path / "out"
        existing = out / "my-bundle"
        existing.mkdir(parents=True)
        (existing / "template.yaml").write_text("name: Old\n")  # a real bundle

        # Non-interactive (no input) declines the confirm -> nothing deleted.
        result = CliRunner().invoke(main, ["bundle", "download", "my-bundle", "-o", str(out)])

        assert result.exit_code != 0
        assert (existing / "template.yaml").read_text() == "name: Old\n"

    @patch(f"{BUNDLE_GROUP}._apply_cli_options_to_config")
    @patch(f"{BUNDLE_GROUP}.S3BundleRepository.from_config")
    def test_existing_bundle_overwritten_with_yes(self, mock_from_config, mock_config, tmp_path):
        mock_from_config.return_value = self._mock_repo(tmp_path)
        out = tmp_path / "out"
        existing = out / "my-bundle"
        existing.mkdir(parents=True)
        (existing / "template.yaml").write_text("name: Old\n")
        (existing / "stale.txt").write_text("stale")

        result = CliRunner().invoke(
            main, ["bundle", "download", "my-bundle", "-o", str(out), "--yes"]
        )

        assert result.exit_code == 0, result.output
        assert not (existing / "stale.txt").exists()
        assert (existing / "template.yaml").read_text() == "name: Test\n"

    @patch(f"{BUNDLE_GROUP}._apply_cli_options_to_config")
    @patch(f"{BUNDLE_GROUP}.S3BundleRepository.from_config")
    def test_failed_copy_preserves_existing_bundle(self, mock_from_config, mock_config, tmp_path):
        """A copy failure (permissions/disk full) must leave the existing bundle
        intact — the copy lands in staging and only swaps in on success."""
        mock_from_config.return_value = self._mock_repo(tmp_path)
        out = tmp_path / "out"
        existing = out / "my-bundle"
        existing.mkdir(parents=True)
        (existing / "template.yaml").write_text("name: Old\n")

        with patch(f"{BUNDLE_GROUP}.shutil.copytree", side_effect=OSError("disk full")):
            result = CliRunner().invoke(
                main, ["bundle", "download", "my-bundle", "-o", str(out), "--yes"]
            )

        assert result.exit_code != 0
        # The prior bundle survives the failed copy.
        assert (existing / "template.yaml").read_text() == "name: Old\n"

    @patch(f"{BUNDLE_GROUP}._apply_cli_options_to_config")
    @patch(f"{BUNDLE_GROUP}.S3BundleRepository.from_config")
    def test_invalid_bundle_name_is_clean_error(self, mock_from_config, mock_config, tmp_path):
        mock_from_config.return_value = self._mock_repo(tmp_path)

        result = CliRunner().invoke(main, ["bundle", "download", "..", "-o", str(tmp_path / "o")])

        assert result.exit_code != 0
        assert "not a valid bundle name" in result.output


class TestBundleList:
    def test_list_local_path(self, tmp_path):
        bundle = tmp_path / "my-bundle"
        bundle.mkdir()
        (bundle / "template.yaml").write_text("name: Test\nsteps:\n- name: S1\n")
        (tmp_path / "not-a-bundle").mkdir()

        runner = CliRunner()
        result = runner.invoke(main, ["bundle", "list", str(tmp_path)])
        assert result.exit_code == 0
        assert "my-bundle" in result.output
        assert "not-a-bundle" not in result.output

    def test_list_local_json(self, tmp_path):
        bundle = tmp_path / "render-job"
        bundle.mkdir()
        (bundle / "template.yaml").write_text("name: Render\nsteps: []\n")

        runner = CliRunner()
        result = runner.invoke(main, ["bundle", "list", str(tmp_path), "--output", "json"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert len(data) == 1
        assert data[0]["name"] == "render-job"
        assert data[0]["format"] == "folder"

    def test_list_local_no_archives(self, tmp_path):
        bundle = tmp_path / "dir-bundle"
        bundle.mkdir()
        (bundle / "template.yaml").write_text("name: Dir\nsteps: []\n")

        ojd_path = tmp_path / "archive-bundle.ojd"
        with zipfile.ZipFile(str(ojd_path), "w") as zf:
            zf.writestr("template.yaml", "name: Zipped\nsteps: []\n")

        runner = CliRunner()

        result = runner.invoke(main, ["bundle", "list", str(tmp_path)])
        assert result.exit_code == 0
        assert "dir-bundle" in result.output
        assert "archive-bundle" in result.output

        result = runner.invoke(main, ["bundle", "list", str(tmp_path), "--no-archives"])
        assert result.exit_code == 0
        assert "dir-bundle" in result.output
        assert "archive-bundle" not in result.output

    def test_list_empty_dir(self, tmp_path):
        runner = CliRunner()
        result = runner.invoke(main, ["bundle", "list", str(tmp_path)])
        assert result.exit_code == 0
        assert result.output.strip() == ""


class TestBundleUpload:
    @patch(f"{BUNDLE_GROUP}._apply_cli_options_to_config")
    @patch(f"{BUNDLE_GROUP}._get_queue_s3_settings")
    @patch("boto3.client")
    def test_upload_creates_zip(self, mock_boto3_client, mock_s3_settings, mock_config, tmp_path):
        bundle = tmp_path / "my-bundle"
        bundle.mkdir()
        (bundle / "template.yaml").write_text(
            yaml.dump(
                {
                    "specificationVersion": "jobtemplate-2023-09",
                    "name": "Test Bundle",
                    "steps": [{"name": "Run"}],
                }
            )
        )

        mock_session = MagicMock()
        mock_s3 = MagicMock()
        mock_s3.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")
        mock_session.client.return_value = mock_s3
        mock_s3_settings.return_value = (
            MagicMock(s3BucketName="test-bucket", rootPrefix="DeadlineCloud"),
            mock_session,
        )

        runner = CliRunner()
        result = runner.invoke(main, ["bundle", "upload", str(bundle)])

        assert result.exit_code == 0, result.output
        assert "Uploaded bundle to" in result.output
        mock_s3.upload_fileobj.assert_called_once()

    @patch(f"{BUNDLE_GROUP}._apply_cli_options_to_config")
    @patch(f"{BUNDLE_GROUP}._get_queue_s3_settings")
    def test_upload_not_a_bundle(self, mock_s3_settings, mock_config, tmp_path):
        not_bundle = tmp_path / "empty"
        not_bundle.mkdir()

        mock_s3_settings.return_value = (
            MagicMock(s3BucketName="test-bucket", rootPrefix="DeadlineCloud"),
            MagicMock(),
        )

        runner = CliRunner()
        result = runner.invoke(main, ["bundle", "upload", str(not_bundle)])
        assert result.exit_code == 1
        assert "not appear to be a job bundle" in result.output

    def _bundle_dir(self, tmp_path):
        bundle = tmp_path / "my-bundle"
        bundle.mkdir()
        (bundle / "template.yaml").write_text(
            yaml.dump({"specificationVersion": "jobtemplate-2023-09", "name": "T", "steps": []})
        )
        return bundle

    def _mock_queue(self, mock_s3_settings):
        mock_session = MagicMock()
        mock_s3 = MagicMock()
        mock_s3.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")
        mock_session.client.return_value = mock_s3
        mock_s3_settings.return_value = (
            MagicMock(s3BucketName="test-bucket", rootPrefix="DeadlineCloud"),
            mock_session,
        )
        return mock_s3

    @patch(f"{BUNDLE_GROUP}._apply_cli_options_to_config")
    @patch(f"{BUNDLE_GROUP}._get_queue_s3_settings")
    def test_upload_name_with_slash_is_sanitized(self, mock_s3_settings, mock_config, tmp_path):
        """An embedded '/' in --name is flattened so the object stays in job-bundles/."""
        bundle = self._bundle_dir(tmp_path)
        mock_s3 = self._mock_queue(mock_s3_settings)

        result = CliRunner().invoke(main, ["bundle", "upload", str(bundle), "--name", "a/b"])

        assert result.exit_code == 0, result.output
        s3_key = mock_s3.upload_fileobj.call_args.args[2]
        assert s3_key == "DeadlineCloud/job-bundles/a_b.ojd"

    @patch(f"{BUNDLE_GROUP}._apply_cli_options_to_config")
    @patch(f"{BUNDLE_GROUP}._get_queue_s3_settings")
    def test_upload_name_traversal_stays_within_prefix(
        self, mock_s3_settings, mock_config, tmp_path
    ):
        """A '../..'-style --name cannot escape the job-bundles/ prefix.

        Previously the name was interpolated into the S3 key verbatim, so
        "../../evil" could write to an arbitrary sub-prefix. Routing through
        sanitize_bundle_name flattens the separators, keeping the object inside
        job-bundles/ with no extra path segments.
        """
        bundle = self._bundle_dir(tmp_path)
        mock_s3 = self._mock_queue(mock_s3_settings)

        result = CliRunner().invoke(main, ["bundle", "upload", str(bundle), "--name", "../../evil"])

        assert result.exit_code == 0, result.output
        s3_key = mock_s3.upload_fileobj.call_args.args[2]
        assert s3_key.startswith("DeadlineCloud/job-bundles/")
        # No sub-prefix beyond the fixed job-bundles path (exactly two slashes).
        assert s3_key.count("/") == 2
        assert s3_key == "DeadlineCloud/job-bundles/.._.._evil.ojd"

    @patch(f"{BUNDLE_GROUP}._apply_cli_options_to_config")
    @patch(f"{BUNDLE_GROUP}._get_queue_s3_settings")
    def test_upload_rejects_unsafe_name(self, mock_s3_settings, mock_config, tmp_path):
        """A name that is empty/only-separators after sanitization is rejected."""
        bundle = self._bundle_dir(tmp_path)
        mock_s3 = self._mock_queue(mock_s3_settings)

        for bad_name in ("..", "///"):
            result = CliRunner().invoke(main, ["bundle", "upload", str(bundle), "--name", bad_name])
            assert result.exit_code == 1, (bad_name, result.output)
            assert "empty or invalid" in result.output
        mock_s3.upload_fileobj.assert_not_called()


class TestMetadataTruncation:
    @patch(f"{BUNDLE_GROUP}._apply_cli_options_to_config")
    @patch(f"{BUNDLE_GROUP}._get_queue_s3_settings")
    def test_upload_truncates_metadata_with_warning(
        self, mock_s3_settings, mock_config, tmp_path, capsys
    ):
        bundle = tmp_path / "big-bundle"
        bundle.mkdir()
        (bundle / "template.yaml").write_text(
            yaml.dump(
                {
                    "specificationVersion": "jobtemplate-2023-09",
                    "name": "A" * 300,
                    "description": "D" * 1200,
                    "steps": [{"name": f"Step_{i:03d}_Long"} for i in range(40)],
                    "parameterDefinitions": [
                        {"name": f"Param_{i:03d}_Long", "type": "STRING"} for i in range(50)
                    ],
                }
            )
        )

        mock_session = MagicMock()
        mock_s3 = MagicMock()
        mock_s3.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")
        mock_session.client.return_value = mock_s3
        mock_s3_settings.return_value = (
            MagicMock(s3BucketName="test-bucket", rootPrefix="DC"),
            mock_session,
        )

        runner = CliRunner()
        result = runner.invoke(main, ["bundle", "upload", str(bundle)])
        assert result.exit_code == 0, result.output

        # Verify metadata values respect limits
        call_args = mock_s3.upload_fileobj.call_args
        metadata = call_args[1]["ExtraArgs"]["Metadata"]
        # The object is typed as a zip (courtesy hint; not trusted on download).
        assert call_args[1]["ExtraArgs"]["ContentType"] == "application/zip"
        assert len(metadata["ojd-name"].encode("utf-8")) <= METADATA_LIMIT_NAME
        # Total metadata must stay within S3's 2KB budget
        total = sum(
            12 + len(k.encode("utf-8")) + len(v.encode("utf-8")) for k, v in metadata.items()
        )
        assert total <= S3_METADATA_TOTAL_BUDGET

        # Verify truncated values end with "..."
        assert metadata["ojd-name"].endswith("...")
        assert metadata["ojd-desc"].endswith("...")
        assert metadata["ojd-steps"].endswith("...")
        assert metadata["ojd-params"].endswith("...")

    @patch(f"{BUNDLE_GROUP}._apply_cli_options_to_config")
    @patch(f"{BUNDLE_GROUP}._get_queue_s3_settings")
    def test_upload_no_truncation_when_within_limits(self, mock_s3_settings, mock_config, tmp_path):
        bundle = tmp_path / "small-bundle"
        bundle.mkdir()
        (bundle / "template.yaml").write_text(
            yaml.dump(
                {
                    "specificationVersion": "jobtemplate-2023-09",
                    "name": "Short Name",
                    "description": "Brief",
                    "steps": [{"name": "Render"}],
                    "parameterDefinitions": [{"name": "Frames", "type": "STRING"}],
                }
            )
        )

        mock_session = MagicMock()
        mock_s3 = MagicMock()
        mock_s3.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")
        mock_session.client.return_value = mock_s3
        mock_s3_settings.return_value = (
            MagicMock(s3BucketName="test-bucket", rootPrefix="DC"),
            mock_session,
        )

        runner = CliRunner()
        result = runner.invoke(main, ["bundle", "upload", str(bundle)])
        assert result.exit_code == 0, result.output

        # No warnings
        assert "truncated" not in result.output.lower()

        # Values stored as-is without "..."
        call_args = mock_s3.upload_fileobj.call_args
        metadata = call_args[1]["ExtraArgs"]["Metadata"]
        assert metadata["ojd-name"] == "Short Name"
        assert not metadata["ojd-name"].endswith("...")

    @patch(f"{BUNDLE_GROUP}._apply_cli_options_to_config")
    @patch(f"{BUNDLE_GROUP}._get_queue_s3_settings")
    def test_upload_sanitizes_control_characters_in_name(
        self, mock_s3_settings, mock_config, tmp_path
    ):
        """Control characters in --name are sanitized (replaced with '_'),
        consistent with the GUI export path and sanitize_bundle_name."""
        bundle = tmp_path / "my-bundle"
        bundle.mkdir()
        (bundle / "template.yaml").write_text("name: Test\nsteps:\n- name: S1\n")

        mock_session = MagicMock()
        mock_s3 = MagicMock()
        mock_s3.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")
        mock_session.client.return_value = mock_s3
        mock_s3_settings.return_value = (
            MagicMock(s3BucketName="bucket", rootPrefix="DC"),
            mock_session,
        )

        runner = CliRunner()
        result = runner.invoke(main, ["bundle", "upload", str(bundle), "--name", "bad\x01name"])
        assert result.exit_code == 0, result.output
        s3_key = mock_s3.upload_fileobj.call_args.args[2]
        assert s3_key == "DC/job-bundles/bad_name.ojd"

    @patch(f"{BUNDLE_GROUP}._apply_cli_options_to_config")
    @patch(f"{BUNDLE_GROUP}._get_queue_s3_settings")
    def test_upload_archive_produces_same_metadata_as_directory(
        self, mock_s3_settings, mock_config, tmp_path
    ):
        """Uploading a .ojd archive should produce the same S3 metadata as uploading
        the equivalent directory bundle."""
        template_content = yaml.dump(
            {
                "specificationVersion": "jobtemplate-2023-09",
                "name": "Metadata Test",
                "description": "A bundle for testing metadata parity",
                "steps": [{"name": "StepOne"}, {"name": "StepTwo"}],
                "parameterDefinitions": [
                    {"name": "Frames", "type": "STRING", "default": "1-10"},
                    {"name": "OutputDir", "type": "PATH"},
                ],
            }
        )

        # Create directory bundle
        bundle_dir = tmp_path / "my-bundle"
        bundle_dir.mkdir()
        (bundle_dir / "template.yaml").write_text(template_content)

        # Create .ojd archive with same content
        archive_path = tmp_path / "my-bundle.ojd"
        with zipfile.ZipFile(archive_path, "w") as zf:
            zf.writestr("template.yaml", template_content)

        mock_session = MagicMock()
        mock_s3 = MagicMock()
        mock_s3.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")
        mock_session.client.return_value = mock_s3
        mock_s3_settings.return_value = (
            MagicMock(s3BucketName="test-bucket", rootPrefix="DC"),
            mock_session,
        )

        runner = CliRunner()

        # Upload directory
        result_dir = runner.invoke(main, ["bundle", "upload", str(bundle_dir)])
        assert result_dir.exit_code == 0, result_dir.output
        dir_metadata = mock_s3.upload_fileobj.call_args[1]["ExtraArgs"]["Metadata"]

        mock_s3.reset_mock()
        mock_s3.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")

        # Upload archive
        result_ojd = runner.invoke(main, ["bundle", "upload", str(archive_path)])
        assert result_ojd.exit_code == 0, result_ojd.output
        ojd_metadata = mock_s3.upload_fileobj.call_args[1]["ExtraArgs"]["Metadata"]

        # Metadata should be identical
        assert dir_metadata == ojd_metadata
        assert dir_metadata["ojd-name"] == "Metadata Test"
        assert "ojd-step-count" in dir_metadata
        assert dir_metadata["ojd-step-count"] == "2"
        assert "ojd-param-count" in dir_metadata
        assert dir_metadata["ojd-param-count"] == "2"


class TestBundleUploadOverwrite:
    @patch(f"{BUNDLE_GROUP}._apply_cli_options_to_config")
    @patch(f"{BUNDLE_GROUP}._get_queue_s3_settings")
    def test_upload_prompts_when_bundle_exists_and_user_confirms(
        self, mock_s3_settings, mock_config, tmp_path
    ):
        bundle = tmp_path / "my-bundle"
        bundle.mkdir()
        (bundle / "template.yaml").write_text("name: Test\nsteps:\n- name: S1\n")

        mock_session = MagicMock()
        mock_s3 = MagicMock()
        mock_s3.head_object.return_value = {}  # exists
        mock_session.client.return_value = mock_s3
        mock_s3_settings.return_value = (
            MagicMock(s3BucketName="test-bucket", rootPrefix="DC"),
            mock_session,
        )

        runner = CliRunner()
        result = runner.invoke(main, ["bundle", "upload", str(bundle)], input="y\n")
        assert result.exit_code == 0, result.output
        assert "already exists" in result.output
        mock_s3.upload_fileobj.assert_called_once()

    @patch(f"{BUNDLE_GROUP}._apply_cli_options_to_config")
    @patch(f"{BUNDLE_GROUP}._get_queue_s3_settings")
    def test_upload_aborts_when_bundle_exists_and_user_declines(
        self, mock_s3_settings, mock_config, tmp_path
    ):
        bundle = tmp_path / "my-bundle"
        bundle.mkdir()
        (bundle / "template.yaml").write_text("name: Test\nsteps:\n- name: S1\n")

        mock_session = MagicMock()
        mock_s3 = MagicMock()
        mock_s3.head_object.return_value = {}  # exists
        mock_session.client.return_value = mock_s3
        mock_s3_settings.return_value = (
            MagicMock(s3BucketName="test-bucket", rootPrefix="DC"),
            mock_session,
        )

        runner = CliRunner()
        result = runner.invoke(main, ["bundle", "upload", str(bundle)], input="n\n")
        assert result.exit_code == 0, result.output
        assert "canceled" in result.output.lower()
        mock_s3.upload_fileobj.assert_not_called()


class TestBundleHide:
    def _repo_with(self, mock_from_config, *, hidden=None):
        mock_repo = MagicMock()
        mock_repo.get_hidden_set.return_value = set(hidden or set())
        mock_repo.root_path.return_value = "s3://bucket/prefix/job-bundles/"
        mock_repo.list_entries.return_value = [
            BrowseEntry(
                name="blender-render",
                path="s3://bucket/prefix/job-bundles/blender-render.ojd",
                is_bundle=True,
                is_archive=True,
            ),
        ]
        # Root-level bundles key on their bare name.
        mock_repo.visibility_key.return_value = "blender-render"
        mock_from_config.return_value = mock_repo
        return mock_repo

    @patch(f"{BUNDLE_GROUP}._apply_cli_options_to_config")
    @patch(f"{BUNDLE_GROUP}.S3BundleRepository.from_config")
    def test_hide_bundle(self, mock_from_config, mock_config):
        mock_repo = self._repo_with(mock_from_config)

        runner = CliRunner()
        result = runner.invoke(main, ["bundle", "hide", "blender-render"])

        assert result.exit_code == 0, result.output
        assert "Hidden bundle: blender-render" in result.output
        mock_repo.set_bundle_visibility.assert_called_once_with("blender-render", hidden=True)

    @patch(f"{BUNDLE_GROUP}._apply_cli_options_to_config")
    @patch(f"{BUNDLE_GROUP}.S3BundleRepository.from_config")
    def test_hide_already_hidden(self, mock_from_config, mock_config):
        mock_repo = self._repo_with(mock_from_config, hidden={"blender-render"})

        runner = CliRunner()
        result = runner.invoke(main, ["bundle", "hide", "blender-render"])

        assert result.exit_code == 0, result.output
        assert "already hidden" in result.output
        mock_repo.set_bundle_visibility.assert_not_called()

    @patch(f"{BUNDLE_GROUP}._apply_cli_options_to_config")
    @patch(f"{BUNDLE_GROUP}.S3BundleRepository.from_config")
    def test_hide_unknown_name_errors(self, mock_from_config, mock_config):
        """A typo isn't silently persisted forever — it errors like download/info."""
        mock_repo = self._repo_with(mock_from_config)

        runner = CliRunner()
        result = runner.invoke(main, ["bundle", "hide", "typo-name"])

        assert result.exit_code != 0
        assert "not found" in result.output
        mock_repo.set_bundle_visibility.assert_not_called()


class TestBundleUnhide:
    @patch(f"{BUNDLE_GROUP}._apply_cli_options_to_config")
    @patch(f"{BUNDLE_GROUP}.S3BundleRepository.from_config")
    def test_unhide_bundle(self, mock_from_config, mock_config):
        mock_repo = MagicMock()
        mock_repo.get_hidden_set.return_value = {"blender-render"}
        mock_from_config.return_value = mock_repo

        runner = CliRunner()
        result = runner.invoke(main, ["bundle", "unhide", "blender-render"])

        assert result.exit_code == 0, result.output
        assert "Unhidden bundle: blender-render" in result.output
        mock_repo.set_bundle_visibility.assert_called_once_with("blender-render", hidden=False)

    @patch(f"{BUNDLE_GROUP}._apply_cli_options_to_config")
    @patch(f"{BUNDLE_GROUP}.S3BundleRepository.from_config")
    def test_unhide_not_hidden(self, mock_from_config, mock_config):
        mock_repo = MagicMock()
        mock_repo.get_hidden_set.return_value = set()
        mock_from_config.return_value = mock_repo

        runner = CliRunner()
        result = runner.invoke(main, ["bundle", "unhide", "blender-render"])

        assert result.exit_code == 0, result.output
        assert "not hidden" in result.output
        mock_repo.set_bundle_visibility.assert_not_called()


class TestBundleListShowHidden:
    @patch(f"{BUNDLE_GROUP}._apply_cli_options_to_config")
    @patch(f"{BUNDLE_GROUP}.S3BundleRepository.from_config")
    def test_list_queue_hides_hidden_by_default(self, mock_from_config, mock_config):
        mock_repo = MagicMock()
        mock_repo.get_hidden_set.return_value = {"old-job"}
        mock_repo.root_path.return_value = "s3://bucket/prefix/job-bundles/"
        mock_repo.list_entries.return_value = [
            BrowseEntry(
                name="blender-render",
                path="s3://b/p/blender-render.ojd",
                is_bundle=True,
                is_archive=True,
            ),
            BrowseEntry(
                name="old-job", path="s3://b/p/old-job.ojd", is_bundle=True, is_archive=True
            ),
        ]
        mock_from_config.return_value = mock_repo

        runner = CliRunner()
        result = runner.invoke(main, ["bundle", "list", "--queue"])

        assert result.exit_code == 0, result.output
        assert "blender-render" in result.output
        assert "old-job" not in result.output

    @patch(f"{BUNDLE_GROUP}._apply_cli_options_to_config")
    @patch(f"{BUNDLE_GROUP}.S3BundleRepository.from_config")
    def test_list_queue_show_hidden(self, mock_from_config, mock_config):
        mock_repo = MagicMock()
        mock_repo.get_hidden_set.return_value = {"old-job"}
        mock_repo.root_path.return_value = "s3://bucket/prefix/job-bundles/"
        mock_repo.list_entries.return_value = [
            BrowseEntry(
                name="blender-render",
                path="s3://b/p/blender-render.ojd",
                is_bundle=True,
                is_archive=True,
            ),
            BrowseEntry(
                name="old-job", path="s3://b/p/old-job.ojd", is_bundle=True, is_archive=True
            ),
        ]
        mock_from_config.return_value = mock_repo

        runner = CliRunner()
        result = runner.invoke(main, ["bundle", "list", "--queue", "--show-hidden"])

        assert result.exit_code == 0, result.output
        assert "blender-render" in result.output
        assert "old-job (hidden)" in result.output

    @patch(f"{BUNDLE_GROUP}._apply_cli_options_to_config")
    @patch(f"{BUNDLE_GROUP}.S3BundleRepository.from_config")
    def test_list_queue_show_hidden_json(self, mock_from_config, mock_config):
        mock_repo = MagicMock()
        mock_repo.get_hidden_set.return_value = {"old-job"}
        mock_repo.root_path.return_value = "s3://bucket/prefix/job-bundles/"
        mock_repo.list_entries.return_value = [
            BrowseEntry(
                name="blender-render",
                path="s3://b/p/blender-render.ojd",
                is_bundle=True,
                is_archive=True,
            ),
            BrowseEntry(
                name="old-job", path="s3://b/p/old-job.ojd", is_bundle=True, is_archive=True
            ),
        ]
        mock_from_config.return_value = mock_repo

        runner = CliRunner()
        result = runner.invoke(
            main, ["bundle", "list", "--queue", "--show-hidden", "--output", "json"]
        )

        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        assert len(data) == 2
        hidden_entry = next(e for e in data if e["name"] == "old-job")
        visible_entry = next(e for e in data if e["name"] == "blender-render")
        assert hidden_entry["hidden"] is True
        assert "hidden" not in visible_entry


class TestLocalArchiveCache:
    """Tests for LocalBundleRepository.extract_bundle mtime-based caching."""

    def test_extract_caches_and_reuses(self, fresh_deadline_config, tmp_path):
        """Second extraction of unchanged archive uses the cache."""
        # Create an .ojd archive
        archive = tmp_path / "my-bundle.ojd"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("template.yaml", "name: Test\nsteps:\n- name: S1\n")
            zf.writestr("script.sh", "echo hello")

        repo = LocalBundleRepository(root=str(tmp_path))

        # First extraction
        result1 = repo.extract_bundle(str(archive))
        assert os.path.isfile(os.path.join(result1, "template.yaml"))
        assert os.path.isfile(os.path.join(result1, "script.sh"))

        # Record the extraction time by checking a file's mtime in cache
        template_mtime1 = os.path.getmtime(os.path.join(result1, "template.yaml"))

        # Second extraction — should reuse cache (same path returned)
        result2 = repo.extract_bundle(str(archive))
        assert result2 == result1

        # File mtime should be unchanged (no re-extraction happened)
        template_mtime2 = os.path.getmtime(os.path.join(result2, "template.yaml"))
        assert template_mtime1 == template_mtime2

    def test_extract_invalidates_on_mtime_change(self, fresh_deadline_config, tmp_path):
        """Modified archive triggers re-extraction."""
        # Create initial archive
        archive = tmp_path / "my-bundle.ojd"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("template.yaml", "name: V1\nsteps:\n- name: S1\n")

        repo = LocalBundleRepository(root=str(tmp_path))

        # First extraction
        result1 = repo.extract_bundle(str(archive))
        with open(os.path.join(result1, "template.yaml")) as f:
            assert "V1" in f.read()

        # Modify the archive (ensure different mtime)
        time.sleep(0.05)
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("template.yaml", "name: V2\nsteps:\n- name: S2\n")

        # Second extraction — should detect mtime change and re-extract
        result2 = repo.extract_bundle(str(archive))
        with open(os.path.join(result2, "template.yaml")) as f:
            assert "V2" in f.read()

    def test_extract_handles_wrapper_directory(self, fresh_deadline_config, tmp_path):
        """Archive with a single wrapper dir returns the inner path."""
        archive = tmp_path / "wrapped.ojd"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("inner/template.yaml", "name: Wrapped\nsteps:\n- name: S1\n")

        repo = LocalBundleRepository(root=str(tmp_path))
        result = repo.extract_bundle(str(archive))

        # Should return the inner directory, not the extraction root
        assert os.path.basename(result) == "inner"
        assert os.path.isfile(os.path.join(result, "template.yaml"))


class TestBundleDownload:
    """Tests for `deadline bundle download` — validates the full download + extract + copy flow."""

    @patch(f"{BUNDLE_GROUP}._apply_cli_options_to_config")
    @patch(f"{BUNDLE_GROUP}.S3BundleRepository.from_config")
    def test_download_to_output_dir(
        self, mock_from_config, mock_config, tmp_path, fresh_deadline_config
    ):
        """Download extracts bundle to output dir."""
        # Create a real .ojd archive to serve as the download
        bundle_content = {"template.yaml": "name: DownloadTest\nsteps:\n- name: S1\n"}
        archive_buf = io.BytesIO()
        with zipfile.ZipFile(archive_buf, "w") as zf:
            for name, content in bundle_content.items():
                zf.writestr(name, content)
        archive_bytes = archive_buf.getvalue()

        mock_repo = MagicMock()
        mock_repo.root_path.return_value = "s3://bucket/DC/job-bundles/"
        mock_repo.list_entries.return_value = [
            BrowseEntry(
                name="test-bundle",
                path="s3://bucket/DC/job-bundles/test-bundle.ojd",
                is_bundle=True,
                is_archive=True,
            ),
        ]
        mock_repo.get_bundle_size.return_value = len(archive_bytes)

        # Make download_full_bundle extract to a real temp dir
        extract_dir = tmp_path / "cache" / "test-bundle"
        extract_dir.mkdir(parents=True)
        with zipfile.ZipFile(io.BytesIO(archive_bytes), "r") as zf:
            zf.extractall(str(extract_dir))

        mock_repo.download_full_bundle.return_value = str(extract_dir)
        mock_from_config.return_value = mock_repo

        output_dir = tmp_path / "output"
        runner = CliRunner()
        result = runner.invoke(main, ["bundle", "download", "test-bundle", "-o", str(output_dir)])

        assert result.exit_code == 0, result.output
        assert "Downloaded bundle to:" in result.output
        assert os.path.isfile(str(output_dir / "test-bundle" / "template.yaml"))

    @patch(f"{BUNDLE_GROUP}._apply_cli_options_to_config")
    @patch(f"{BUNDLE_GROUP}.S3BundleRepository.from_config")
    def test_download_no_output_dir_prints_cache_path(
        self, mock_from_config, mock_config, tmp_path, fresh_deadline_config
    ):
        """Without -o, prints the cache path."""
        mock_repo = MagicMock()
        mock_repo.root_path.return_value = "s3://bucket/DC/job-bundles/"
        mock_repo.list_entries.return_value = [
            BrowseEntry(
                name="my-bundle",
                path="s3://bucket/DC/job-bundles/my-bundle.ojd",
                is_bundle=True,
                is_archive=True,
            ),
        ]
        mock_repo.get_bundle_size.return_value = 1024
        cache_path = str(tmp_path / "cached-bundle")
        mock_repo.download_full_bundle.return_value = cache_path
        mock_from_config.return_value = mock_repo

        runner = CliRunner()
        result = runner.invoke(main, ["bundle", "download", "my-bundle"])

        assert result.exit_code == 0, result.output
        assert cache_path in result.output

    @patch(f"{BUNDLE_GROUP}._apply_cli_options_to_config")
    @patch(f"{BUNDLE_GROUP}.S3BundleRepository.from_config")
    def test_download_not_found(self, mock_from_config, mock_config, fresh_deadline_config):
        """Bundle not found shows error with available bundles."""
        mock_repo = MagicMock()
        mock_repo.root_path.return_value = "s3://bucket/DC/job-bundles/"
        mock_repo.list_entries.return_value = [
            BrowseEntry(
                name="other-bundle",
                path="s3://b/k/other-bundle.ojd",
                is_bundle=True,
                is_archive=True,
            ),
        ]
        mock_from_config.return_value = mock_repo

        runner = CliRunner()
        result = runner.invoke(main, ["bundle", "download", "nonexistent"])

        assert result.exit_code == 1
        assert "not found" in result.output
        assert "other-bundle" in result.output


class TestBundleInfo:
    """Tests for `deadline bundle info` — validates local and queue info output."""

    def test_info_local_bundle(self, tmp_path, fresh_deadline_config):
        """Info on a local bundle directory prints template details."""
        bundle = tmp_path / "my-render"
        bundle.mkdir()
        (bundle / "template.yaml").write_text(
            yaml.dump(
                {
                    "specificationVersion": "jobtemplate-2023-09",
                    "name": "Render Job",
                    "description": "Renders frames",
                    "steps": [{"name": "RenderStep"}],
                    "parameterDefinitions": [
                        {"name": "Frames", "type": "STRING", "default": "1-10"}
                    ],
                }
            )
        )

        runner = CliRunner()
        result = runner.invoke(main, ["bundle", "info", str(bundle)])

        assert result.exit_code == 0, result.output
        assert "Render Job" in result.output
        assert "RenderStep" in result.output
        assert "Frames" in result.output

    def test_info_local_json_output(self, tmp_path, fresh_deadline_config):
        """Info with --output json returns structured data."""
        bundle = tmp_path / "json-bundle"
        bundle.mkdir()
        (bundle / "template.yaml").write_text(
            yaml.dump(
                {
                    "specificationVersion": "jobtemplate-2023-09",
                    "name": "JSON Test",
                    "steps": [{"name": "S1"}],
                }
            )
        )

        runner = CliRunner()
        result = runner.invoke(main, ["bundle", "info", str(bundle), "--output", "json"])

        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        assert data["name"] == "JSON Test"
        assert data["steps"] == ["S1"]

    def test_info_not_found(self, tmp_path, fresh_deadline_config):
        """Info on nonexistent path shows error."""
        runner = CliRunner()
        result = runner.invoke(main, ["bundle", "info", str(tmp_path / "nope")])

        assert result.exit_code == 1
        assert "not found" in result.output.lower()


class TestDownloadProgressHeadObjectReuse:
    """Verify that get_bundle_size + download_full_bundle reuses the head_object call."""

    def test_get_bundle_size_caches_head_for_download(self, fresh_deadline_config):
        mock_s3 = MagicMock()
        mock_s3.head_object.return_value = {
            "ETag": '"abc123"',
            "ContentLength": 4096,
            "LastModified": "2026-01-01T00:00:00Z",
        }
        # download_fileobj writes bytes into the buffer
        # Write fake zip-like bytes into the buffer
        mock_s3.download_fileobj.side_effect = lambda Fileobj, **kwargs: Fileobj.write(
            b"PK\x03\x04" + b"\x00" * 100
        )

        repo = S3BundleRepository(bucket_name="bucket", root_prefix="DC", session=MagicMock())
        repo._s3 = mock_s3

        # get_bundle_size does head_object
        size = repo.get_bundle_size("s3://bucket/DC/job-bundles/test.ojd")
        assert size == 4096
        assert mock_s3.head_object.call_count == 1

        # download_full_bundle should reuse the cached head — no additional head_object
        # (It will fail on extraction since our fake data isn't a real zip, but
        # we only care about the head_object count)
        try:
            repo.download_full_bundle("s3://bucket/DC/job-bundles/test.ojd", "/tmp")
        except Exception:
            pass  # Expected — fake zip data

        # Still only 1 head_object call total
        assert mock_s3.head_object.call_count == 1


class TestBundleOutputFormat:
    """The new bundle commands follow the repo-wide --output convention:
    the choices are verbose|json (not the old 'text'), resolved via the shared
    helper. (Auto-detection from TTY state is the helper's own contract and is
    tested there; here we pin the explicit choices, which is deterministic.)"""

    def _local_root(self, tmp_path):
        b = tmp_path / "render-job"
        b.mkdir()
        (b / "template.yaml").write_text("name: Render\nsteps: []\n")
        return tmp_path

    def test_list_json_is_structured(self, tmp_path):
        root = self._local_root(tmp_path)
        result = CliRunner().invoke(main, ["bundle", "list", str(root), "--output", "json"])
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        assert data[0]["name"] == "render-job"

    def test_list_verbose_is_text(self, tmp_path):
        root = self._local_root(tmp_path)
        result = CliRunner().invoke(main, ["bundle", "list", str(root), "--output", "verbose"])
        assert result.exit_code == 0, result.output
        assert result.output.strip() == "render-job"
        assert not result.output.strip().startswith(("{", "["))

    def test_list_rejects_removed_text_choice(self, tmp_path):
        root = self._local_root(tmp_path)
        result = CliRunner().invoke(main, ["bundle", "list", str(root), "--output", "text"])
        # 'text' is no longer a valid choice (verbose|json).
        assert result.exit_code != 0

    def test_info_json_is_structured(self, tmp_path):
        b = tmp_path / "render-job"
        b.mkdir()
        (b / "template.yaml").write_text("name: Render\nsteps: []\n")
        result = CliRunner().invoke(main, ["bundle", "info", str(b), "--output", "json"])
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        assert data["name"] == "Render"


class TestBundleDownloadProgressStream:
    """`--output json` must not interleave progress rendering with the JSON, so
    the bars are skipped in json mode (matching `job download-output`). The MCP
    download tool parses this output."""

    def test_download_json_output_is_clean(self, tmp_path):
        cache = tmp_path / "cache" / "my-bundle"
        cache.mkdir(parents=True)
        (cache / "template.yaml").write_text("name: Test\n")

        called = {"progress": False}

        def _dl(path, progress_callback=None, extract_callback=None, extract_size_callback=None):
            # In json mode the CLI passes None callbacks (bars are skipped).
            if progress_callback or extract_callback or extract_size_callback:
                called["progress"] = True
            return str(cache)

        repo = MagicMock()
        repo.root_path.return_value = "s3://b/DC/job-bundles/"
        repo.list_entries.return_value = [
            BrowseEntry(
                name="my-bundle",
                path="s3://b/my-bundle.ojd",
                is_bundle=True,
                is_archive=True,
            ),
        ]
        repo.get_bundle_size.return_value = 100
        repo.download_full_bundle.side_effect = _dl

        with (
            patch(f"{BUNDLE_GROUP}._apply_cli_options_to_config"),
            patch(f"{BUNDLE_GROUP}.S3BundleRepository.from_config", return_value=repo),
        ):
            result = CliRunner().invoke(
                main, ["bundle", "download", "my-bundle", "--output", "json"]
            )

        assert result.exit_code == 0, result.output
        # No progress callbacks are wired in json mode, and stdout is clean JSON.
        assert called["progress"] is False
        data = json.loads(result.output)
        assert "path" in data
