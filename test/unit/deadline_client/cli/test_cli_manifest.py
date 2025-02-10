# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
import pytest
from unittest.mock import patch, Mock, MagicMock
from click.testing import CliRunner

from deadline.client.cli import main
from deadline.client.cli._groups import manifest_group
from deadline.client.api import _submit_job_bundle
from deadline.job_attachments.models import (
    AssetRootGroup,
    JobAttachmentS3Settings,
    Attachments,
)
from deadline.job_attachments.upload import S3AssetManager, S3AssetUploader
from deadline.job_attachments.caches import HashCache
from deadline.job_attachments.asset_manifests.base_manifest import BaseManifestPath
from deadline.job_attachments.asset_manifests.v2023_03_03 import AssetManifest
from deadline.job_attachments.asset_manifests.hash_algorithms import HashAlgorithm


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
    with patch(
        "deadline.job_attachments.api.manifest._hash_attachments", return_value=(Mock(), [])
    ) as mock:
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
    with patch.object(manifest_group, "update_manifest", return_value=basic_asset_manifest) as mock:
        yield mock


@pytest.fixture
def mock_upload_attachments():
    with patch.object(
        _submit_job_bundle.api, "_upload_attachments", return_value=MOCK_UPLOAD_ATTACHMENTS_RESPONSE
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

    with patch.object(manifest_group, "read_local_manifest", side_effect=_mock_read_local_manifest):
        yield


MOCK_ROOT_DIR = "/path/to/root"
MOCK_MANIFEST_DIR = "/path/to/manifest"
MOCK_MANIFEST_OUT_DIR = "path/to/out/dir"
MOCK_MANIFEST_FILE = os.path.join(MOCK_MANIFEST_DIR, "manifest_input")
MOCK_INVALID_DIR = "/nopath/"
MOCK_UPLOAD_ATTACHMENTS_RESPONSE = {"manifests": [{"inputManifestPath": "s3://mock/manifest.json"}]}
MOCK_JOB_ATTACHMENTS = {
    "manifests": [
        {
            "inputManifestHash": "mock_input_manifest_hash",
            "inputManifestPath": "mock_input_manifest_path",
            "outputRelativeDirectories": ["mock_output_dir"],
            "rootPath": "mock_root_path",
            "rootPathFormat": "mock_root_path_format",
        }
    ]
}
MOCK_QUEUE = {
    "queueId": "queue-0123456789abcdef0123456789abcdef",
    "displayName": "mock_queue",
    "description": "mock_description",
    "jobAttachmentSettings": {"s3BucketName": "mock_bucket", "rootPrefix": "mock_deadline"},
}


@pytest.mark.skip("Random Failure with no credentials on Github")
class TestSnapshot:
    def test_snapshot_root_directory_only(
        self, tmp_path, mock_prepare_paths_for_upload, mock_hash_attachments, mock_upload_group
    ):
        """
        Tests if CLI snapshot command calls correctly with an exiting directory path at --root
        """
        root_dir = str(tmp_path)

        temp_file = tmp_path / "temp_file.txt"
        temp_file.touch()

        mock_prepare_paths_for_upload.return_value = mock_upload_group

        runner = CliRunner()
        result = runner.invoke(main, ["manifest", "snapshot", "--root", root_dir])

        assert result.exit_code == 0
        mock_prepare_paths_for_upload.assert_called_once_with(
            input_paths=[str(temp_file)], output_paths=[root_dir], referenced_paths=[]
        )
        mock_hash_attachments.assert_called_once()

    def test_invalid_root_directory(self, tmp_path):
        """
        Tests if CLI snapshot raises error when called with an invalid --root with non-existing directory path
        """
        invalid_root_dir = str(tmp_path / "invalid_dir")

        runner = CliRunner()
        result = runner.invoke(main, ["manifest", "snapshot", "--root", invalid_root_dir])

        assert result.exit_code != 0
        assert f"{invalid_root_dir}" in result.output

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
                "manifest",
                "snapshot",
                "--root",
                root_dir,
            ],
        )

        assert result.exit_code == 0
        mock_prepare_paths_for_upload.assert_called_once_with(
            input_paths=[str(temp_file)], output_paths=[root_dir], referenced_paths=[]
        )
        mock_hash_attachments.assert_called_once()

    def test_invalid_manifest_out(self, tmp_path):
        """
        Tests if CLI snapshot raises error when called with invalid --destination with non-existing directory path
        """
        root_dir = str(tmp_path)
        invalid_manifest_out = str(tmp_path / "nonexistent_dir")

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["manifest", "snapshot", "--root", root_dir, "--destination", invalid_manifest_out],
        )

        assert result.exit_code != 0
        assert f"{invalid_manifest_out}" in result.output

    def test_asset_snapshot_recursive(
        self, tmp_path, mock_prepare_paths_for_upload, mock_hash_attachments, mock_upload_group
    ):
        """
        Tests if CLI snapshot works with snapshotting directories with nested data.
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
        result = runner.invoke(main, ["manifest", "snapshot", "--root", root_dir])

        assert result.exit_code == 0
        actual_inputs = set(mock_prepare_paths_for_upload.call_args[1]["input_paths"])
        assert actual_inputs == expected_inputs
        mock_hash_attachments.assert_called_once()
