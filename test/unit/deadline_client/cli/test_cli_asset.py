# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
from unittest.mock import patch, Mock, MagicMock
from click.testing import CliRunner
import os

from deadline.client.cli import main
from deadline.client.cli._groups import asset_group
from deadline.client import api
from deadline.client.api import _submit_job_bundle
from deadline.job_attachments.models import (
    AssetRootGroup,
    # AssetRootManifest,
    JobAttachmentS3Settings,
    Attachments,
)
from deadline.job_attachments.upload import S3AssetManager, S3AssetUploader, FileStatus
from deadline.job_attachments.caches import HashCache
from deadline.job_attachments.asset_manifests.base_manifest import BaseManifestPath
from deadline.job_attachments.asset_manifests.v2023_03_03 import AssetManifest
from deadline.job_attachments.asset_manifests.hash_algorithms import HashAlgorithm

from ..api.test_job_bundle_submission import (
    MOCK_FARM_ID,
    MOCK_QUEUE_ID,
)


@pytest.fixture
def mock_cachedb():
    mock_hash_cache = MagicMock(spec=HashCache)
    mock_hash_cache.__enter__.return_value = mock_hash_cache
    mock_hash_cache.__exit__.return_value = None
    return mock_hash_cache


@pytest.fixture
def mock_prepare_paths_for_upload():
    with patch.object(S3AssetManager, "prepare_paths_for_upload") as mock:
        yield mock


@pytest.fixture
def mock_hash_attachments():
    with patch.object(api, "hash_attachments", return_value=(Mock(), [])) as mock:
        yield mock


@pytest.fixture
def basic_asset_group(tmp_path):
    root_dir = str(tmp_path)
    return AssetRootGroup(
        root_path=root_dir,
        inputs=set(),
        outputs=set(),
        references=set(),
    )


@pytest.fixture
def mock_upload_group(basic_asset_group):
    return Mock(
        asset_groups=[basic_asset_group],
        total_input_files=1,
        total_input_bytes=100,
    )


@pytest.fixture
def basic_asset_manifest():
    return AssetManifest(paths=[], hash_alg=HashAlgorithm("xxh128"), total_size=0)


@pytest.fixture
def mock_attachment_settings():
    return Attachments(manifests=[], fileSystem="").to_dict


@pytest.fixture
def mock_init_objects():
    with patch.object(S3AssetManager, "__init__", lambda self, *args, **kwargs: None), patch.object(
        S3AssetUploader, "__init__", lambda self, *args, **kwargs: None
    ), patch.object(JobAttachmentS3Settings, "__init__", lambda self, *args, **kwargs: None):
        yield


@pytest.fixture
def mock_update_manifest(basic_asset_manifest):
    with patch.object(asset_group, "update_manifest", return_value=basic_asset_manifest) as mock:
        yield mock


@pytest.fixture
def mock_upload_attachments():
    with patch.object(
        _submit_job_bundle.api, "upload_attachments", return_value=MOCK_UPLOAD_ATTACHMENTS_RESPONSE
    ) as mock:
        yield mock


@pytest.fixture
def mock_create_manifest_file():
    def _mock_create_manifest_file(input_paths, root_path, hash_cache):
        return AssetManifest(
            paths=[
                BaseManifestPath(
                    path=os.path.join(root_path, "file1.txt"), hash="mock_hash_1", size=0, mtime=0
                ),
                BaseManifestPath(
                    path=os.path.join(root_path, "subdir1", "file2.txt"),
                    hash="mock_hash_2",
                    size=0,
                    mtime=0,
                ),
                BaseManifestPath(
                    path=os.path.join(root_path, "subdir2", "subdir3", "file3.txt"),
                    hash="mock_hash_3",
                    size=0,
                    mtime=0,
                ),
            ],
            hash_alg=HashAlgorithm("xxh128"),
            total_size=0,
        )

    with patch.object(
        S3AssetManager, "_create_manifest_file", side_effect=_mock_create_manifest_file
    ):
        yield


@pytest.fixture
def mock_read_local_manifest():
    def _mock_read_local_manifest(manifest):
        return AssetManifest(
            paths=[
                BaseManifestPath(path="file1.txt", hash="old_hash_1", size=0, mtime=0),
                BaseManifestPath(path="subdir1/file2.txt", hash="old_hash_2", size=0, mtime=0),
                BaseManifestPath(
                    path="subdir2/subdir3/file3.txt", hash="old_hash_3", size=0, mtime=0
                ),
            ],
            hash_alg=HashAlgorithm("xxh128"),
            total_size=0,
        )

    with patch.object(asset_group, "read_local_manifest", side_effect=_mock_read_local_manifest):
        yield


MOCK_ROOT_DIR = "/path/to/root"
MOCK_MANIFEST_DIR = "/path/to/manifest"
MOCK_MANIFEST_FILE = os.path.join(MOCK_MANIFEST_DIR, "manifest_input")
MOCK_INVALID_DIR = "/nopath/"
MOCK_UPLOAD_ATTACHMENTS_RESPONSE = {"manifests": [{"inputManifestPath": "s3://mock/manifest.json"}]}


class TestSnapshot:

    def test_snapshot_root_directory_only(
        self, tmp_path, mock_prepare_paths_for_upload, mock_hash_attachments, mock_upload_group
    ):
        """
        Tests if CLI snapshot command calls correctly with an exiting directory path at --root-dir
        """
        root_dir = str(tmp_path)

        temp_file = tmp_path / "temp_file.txt"
        temp_file.touch()

        mock_prepare_paths_for_upload.return_value = mock_upload_group

        runner = CliRunner()
        result = runner.invoke(main, ["asset", "snapshot", "--root-dir", root_dir])

        assert result.exit_code == 0
        mock_prepare_paths_for_upload.assert_called_once_with(
            input_paths=[str(temp_file)], output_paths=[root_dir], referenced_paths=[]
        )
        mock_hash_attachments.assert_called_once()

    def test_invalid_root_directory(self, tmp_path):
        """
        Tests if CLI snapshot raises error when called with an invalid --root-dir with non-existing directory path
        """
        invalid_root_dir = str(tmp_path / "invalid_dir")

        runner = CliRunner()
        result = runner.invoke(main, ["asset", "snapshot", "--root-dir", invalid_root_dir])

        assert result.exit_code != 0
        assert f"Specified root directory {invalid_root_dir} does not exist. " in result.output

    def test_valid_manifest_out(
        self, tmp_path, mock_prepare_paths_for_upload, mock_hash_attachments, mock_upload_group
    ):
        """
        Tests if CLI snapshot command correctly calls with --manifest-out arguement
        """
        root_dir = str(tmp_path)
        manifest_out_dir = tmp_path / "manifest_out"
        manifest_out_dir.mkdir()

        temp_file = tmp_path / "temp_file.txt"
        temp_file.touch()

        mock_prepare_paths_for_upload.return_value = mock_upload_group

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "asset",
                "snapshot",
                "--root-dir",
                root_dir,
                "--manifest-out",
                str(manifest_out_dir),
            ],
        )

        assert result.exit_code == 0
        mock_prepare_paths_for_upload.assert_called_once_with(
            input_paths=[str(temp_file)], output_paths=[root_dir], referenced_paths=[]
        )
        mock_hash_attachments.assert_called_once()

    def test_invalid_manifest_out(self, tmp_path):
        """
        Tests if CLI snapshot raises error when called with invalid --manifest-out with non-existing directory path
        """
        root_dir = str(tmp_path)
        invalid_manifest_out = str(tmp_path / "nonexistent_dir")

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["asset", "snapshot", "--root-dir", root_dir, "--manifest-out", invalid_manifest_out],
        )

        assert result.exit_code != 0
        assert (
            f"Specified destination directory {invalid_manifest_out} does not exist. "
            in result.output
        )

    def test_asset_snapshot_recursive(
        self, tmp_path, mock_prepare_paths_for_upload, mock_hash_attachments, mock_upload_group
    ):
        """
        Tests if CLI snapshot --recursive flag is called correctly
        """
        root_dir = str(tmp_path)
        subdir1 = tmp_path / "subdir1"
        subdir2 = tmp_path / "subdir2"
        subdir1.mkdir()
        subdir2.mkdir()
        (subdir1 / "file1.txt").touch()
        (subdir2 / "file2.txt").touch()

        expected_inputs = {str(subdir2 / "file2.txt"), str(subdir1 / "file1.txt")}
        mock_prepare_paths_for_upload.return_value = mock_upload_group

        runner = CliRunner()
        result = runner.invoke(main, ["asset", "snapshot", "--root-dir", root_dir, "--recursive"])

        assert result.exit_code == 0
        actual_inputs = set(mock_prepare_paths_for_upload.call_args[1]["input_paths"])
        assert actual_inputs == expected_inputs
        mock_hash_attachments.assert_called_once()


class TestUpload:

    def test_upload_valid_manifest(
        fresh_deadline_config, mock_init_objects, mock_upload_attachments
    ):
        """
        Test the asset upload command with correct arguments and valid manifest path.
        """
        with patch.object(_submit_job_bundle.api, "get_boto3_client"), patch.object(
            _submit_job_bundle.api, "get_queue_user_boto3_session"
        ), patch.object(os.path, "isdir", side_effect=[True, True]), patch.object(
            os, "listdir", return_value=["manifest_input"]
        ), patch.object(
            asset_group, "read_local_manifest", return_value=basic_asset_manifest
        ), patch.object(
            asset_group, "diff_manifest", return_value=[]
        ), patch.object(
            S3AssetUploader, "_write_local_manifest_s3_mapping"
        ) as mock_write_manifest_mapping:

            runner = CliRunner()
            result = runner.invoke(
                main,
                [
                    "asset",
                    "upload",
                    "--root-dir",
                    MOCK_ROOT_DIR,
                    "--manifest-dir",
                    MOCK_MANIFEST_DIR,
                    "--farm-id",
                    MOCK_FARM_ID,
                    "--queue-id",
                    MOCK_QUEUE_ID,
                ],
            )

        full_manifest_key = MOCK_UPLOAD_ATTACHMENTS_RESPONSE["manifests"][0]["inputManifestPath"]
        manifest_name = os.path.basename(full_manifest_key)
        manifest_dir_name = os.path.basename(MOCK_MANIFEST_DIR)

        mock_write_manifest_mapping.assert_called_once_with(
            manifest_write_dir=MOCK_ROOT_DIR,
            manifest_name=manifest_name,
            full_manifest_key=full_manifest_key,
            manifest_dir_name=manifest_dir_name,
        )
        mock_upload_attachments.assert_called_once()
        assert result.exit_code == 0

    def test_upload_invalid_manifest_dir(fresh_deadline_config):
        """
        Test the asset upload command when the manifest directory is not a valid directory.
        """
        with patch("deadline.client.cli._groups.asset_group.os.path.isdir", return_value=False):

            runner = CliRunner()
            result = runner.invoke(
                main,
                [
                    "asset",
                    "upload",
                    "--root-dir",
                    MOCK_ROOT_DIR,
                    "--manifest-dir",
                    MOCK_INVALID_DIR,
                    "--farm-id",
                    MOCK_FARM_ID,
                    "--queue-id",
                    MOCK_QUEUE_ID,
                ],
            )

        assert f"Specified manifest directory {MOCK_INVALID_DIR} does not exist. "
        assert result.exit_code == 1

    def test_upload_with_update(
        fresh_deadline_config,
        mock_init_objects,
        mock_update_manifest,
        mock_upload_attachments,
        basic_asset_manifest,
    ):
        """
        Test the asset upload command with the --update flag, and manifest has valid updates to find.
        """

        with patch.object(_submit_job_bundle.api, "get_boto3_client"), patch.object(
            _submit_job_bundle.api, "get_queue_user_boto3_session"
        ), patch.object(os.path, "isdir", side_effect=[True, True]), patch.object(
            os, "listdir", return_value=["manifest_input"]
        ), patch.object(
            asset_group, "read_local_manifest", return_value=basic_asset_manifest
        ), patch.object(
            asset_group, "diff_manifest", return_value=["/path/to/modified/file.txt"]
        ), patch.object(
            S3AssetUploader, "_write_local_manifest_s3_mapping"
        ) as mock_write_manifest_mapping:

            runner = CliRunner()
            result = runner.invoke(
                main,
                [
                    "asset",
                    "upload",
                    "--root-dir",
                    MOCK_ROOT_DIR,
                    "--manifest-dir",
                    MOCK_MANIFEST_DIR,
                    "--farm-id",
                    MOCK_FARM_ID,
                    "--queue-id",
                    MOCK_QUEUE_ID,
                    "--update",
                ],
            )

        full_manifest_key = MOCK_UPLOAD_ATTACHMENTS_RESPONSE["manifests"][0]["inputManifestPath"]

        manifest_name = os.path.basename(full_manifest_key)
        manifest_dir_name = os.path.basename(MOCK_MANIFEST_DIR)
        mock_write_manifest_mapping.assert_called_once_with(
            manifest_write_dir=MOCK_ROOT_DIR,
            manifest_name=manifest_name,
            full_manifest_key=full_manifest_key,
            manifest_dir_name=manifest_dir_name,
        )

        mock_update_manifest.assert_called_once_with(
            manifest=MOCK_MANIFEST_DIR, new_or_modified_paths=["/path/to/modified/file.txt"]
        )
        mock_upload_attachments.assert_called_once()
        assert "Manifest information updated:" in result.output
        assert result.exit_code == 0

    def test_upload_with_modified_files_without_update(fresh_deadline_config, mock_init_objects):
        """
        Test the asset upload command when there are modified files, but the --update flag is not provided.
        """
        mock_modified_files = ["/path/to/modified/file1.txt", "/path/to/modified/file2.txt"]

        with patch.object(_submit_job_bundle.api, "get_boto3_client"), patch.object(
            _submit_job_bundle.api, "get_queue_user_boto3_session"
        ), patch.object(os.path, "isdir", side_effect=[True, True]), patch.object(
            os, "listdir", return_value=["manifest_input"]
        ), patch.object(
            asset_group, "read_local_manifest", return_value=basic_asset_manifest
        ), patch.object(
            asset_group, "diff_manifest", return_value=mock_modified_files
        ):

            runner = CliRunner()
            result = runner.invoke(
                main,
                [
                    "asset",
                    "upload",
                    "--root-dir",
                    MOCK_ROOT_DIR,
                    "--manifest-dir",
                    MOCK_MANIFEST_DIR,
                    "--farm-id",
                    MOCK_FARM_ID,
                    "--queue-id",
                    MOCK_QUEUE_ID,
                ],
            )

        assert (
            f"Manifest contents in {MOCK_MANIFEST_DIR} are outdated; versioning does not match local files in {MOCK_ROOT_DIR}. Please run with --update to fix current files. "
            in result.output
        )
        assert result.exit_code == 1

    def test_cli_asset_upload_read_local_manifest_returns_none(
        fresh_deadline_config, mock_init_objects
    ):
        """
        Test the asset upload command when the read_local_manifest function returns None.
        """

        with patch.object(_submit_job_bundle.api, "get_boto3_client"), patch.object(
            _submit_job_bundle.api, "get_queue_user_boto3_session"
        ), patch.object(os.path, "isdir", side_effect=[True, True]), patch.object(
            os, "listdir", return_value=["manifest_input"]
        ), patch.object(
            asset_group, "read_local_manifest", return_value=None
        ):

            runner = CliRunner()
            result = runner.invoke(
                main,
                [
                    "asset",
                    "upload",
                    "--root-dir",
                    MOCK_ROOT_DIR,
                    "--manifest-dir",
                    MOCK_MANIFEST_DIR,
                    "--farm-id",
                    MOCK_FARM_ID,
                    "--queue-id",
                    MOCK_QUEUE_ID,
                ],
            )

            assert (
                f"Specified manifest directory {MOCK_MANIFEST_DIR} does contain valid manifest input file."
                in result.output
            )
            assert result.exit_code == 1


class TestDiff:

    def test_asset_diff_with_format(
        self, mock_create_manifest_file, mock_read_local_manifest, mock_cachedb
    ):
        with patch.object(os.path, "isdir", side_effect=[True, True]), patch.object(
            asset_group, "HashCache", return_value=mock_cachedb
        ), patch.object(
            asset_group,
            "compare_manifest",
            return_value=[
                (
                    FileStatus.MODIFIED,
                    BaseManifestPath(path="file1.txt", hash="mock_hash_1", size=0, mtime=0),
                ),
                (
                    FileStatus.MODIFIED,
                    BaseManifestPath(path="subdir1/file2.txt", hash="mock_hash_2", size=0, mtime=0),
                ),
            ],
        ):
            runner = CliRunner()
            result = runner.invoke(
                main,
                [
                    "asset",
                    "diff",
                    "--root-dir",
                    MOCK_ROOT_DIR,
                    "--manifest",
                    MOCK_MANIFEST_DIR,
                    "--format",
                ],
            )

            assert f"{MOCK_ROOT_DIR}" in result.output
            assert "file1.txt M" in result.output
            assert "file2.txt M" in result.output
            assert result.exit_code == 0

    def test_asset_diff_with_multiple_subdirectories(
        self, mock_create_manifest_file, mock_read_local_manifest, mock_cachedb
    ):
        with patch.object(os.path, "isdir", side_effect=[True, True]), patch.object(
            asset_group, "HashCache", return_value=mock_cachedb
        ), patch.object(
            asset_group,
            "compare_manifest",
            return_value=[
                (
                    FileStatus.MODIFIED,
                    BaseManifestPath(path="file1.txt", hash="mock_hash_1", size=0, mtime=0),
                ),
                (
                    FileStatus.MODIFIED,
                    BaseManifestPath(path="subdir1/file2.txt", hash="mock_hash_2", size=0, mtime=0),
                ),
                (
                    FileStatus.MODIFIED,
                    BaseManifestPath(
                        path="subdir2/subdir3/file3.txt", hash="mock_hash_3", size=0, mtime=0
                    ),
                ),
            ],
        ):

            runner = CliRunner()
            result = runner.invoke(
                main,
                ["asset", "diff", "--root-dir", MOCK_ROOT_DIR, "--manifest", MOCK_MANIFEST_DIR],
            )

            expected_output = "File Diffs: [(<FileStatus.MODIFIED: 2>, BaseManifestPath(path='file1.txt', hash='mock_hash_1', size=0, mtime=0)), (<FileStatus.MODIFIED: 2>, BaseManifestPath(path='subdir1/file2.txt', hash='mock_hash_2', size=0, mtime=0)), (<FileStatus.MODIFIED: 2>, BaseManifestPath(path='subdir2/subdir3/file3.txt', hash='mock_hash_3', size=0, mtime=0))]"
            assert expected_output in result.output
            assert result.exit_code == 0

    def test_asset_diff_without_format(
        self, mock_init_objects, mock_create_manifest_file, mock_read_local_manifest, mock_cachedb
    ):
        with patch.object(os.path, "isdir", side_effect=[True, True]), patch.object(
            asset_group, "HashCache", return_value=mock_cachedb
        ), patch.object(
            asset_group,
            "compare_manifest",
            return_value=[
                (
                    FileStatus.MODIFIED,
                    BaseManifestPath(path="file1.txt", hash="mock_hash_1", size=0, mtime=0),
                ),
                (
                    FileStatus.MODIFIED,
                    BaseManifestPath(path="subdir1/file2.txt", hash="mock_hash_2", size=0, mtime=0),
                ),
            ],
        ):
            runner = CliRunner()
            result = runner.invoke(
                main,
                ["asset", "diff", "--root-dir", MOCK_ROOT_DIR, "--manifest", MOCK_MANIFEST_DIR],
            )

            expected_result = "File Diffs: [(<FileStatus.MODIFIED: 2>, BaseManifestPath(path='file1.txt', hash='mock_hash_1', size=0, mtime=0)), (<FileStatus.MODIFIED: 2>, BaseManifestPath(path='subdir1/file2.txt', hash='mock_hash_2', size=0, mtime=0))]\n"
            assert expected_result in result.output
            assert result.exit_code == 0

    def test_asset_diff_invalid_root_dir(self, tmp_path):
        invalid_root_dir = str(tmp_path / "invalid_dir")
        manifest_dir = tmp_path / "manifests"
        manifest_dir.mkdir()

        runner = CliRunner()
        result = runner.invoke(
            main, ["asset", "diff", "--root-dir", invalid_root_dir, "--manifest", str(manifest_dir)]
        )

        assert result.exit_code == 1
        assert f"Specified root directory {invalid_root_dir} does not exist. " in result.output

    def test_asset_diff_invalid_manifest_dir(self, tmp_path):
        root_dir = str(tmp_path)
        invalid_manifest_dir = str(tmp_path / "invalid_manifests")

        runner = CliRunner()
        result = runner.invoke(
            main, ["asset", "diff", "--root-dir", root_dir, "--manifest", invalid_manifest_dir]
        )

        assert result.exit_code == 1
        assert (
            f"Specified manifest directory {invalid_manifest_dir} does not exist. " in result.output
        )
