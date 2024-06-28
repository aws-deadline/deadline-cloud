# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from unittest.mock import patch, Mock
from click.testing import CliRunner

from deadline.client.cli import main
from deadline.client import api
from deadline.job_attachments.upload import S3AssetManager
from deadline.job_attachments.models import AssetRootGroup


class TestSnapshot:

    def test_snapshot_root_directory_only(self, tmp_path):
        """
        Tests if CLI snapshot command calls correctly with the --root-dir arguement
        """
        root_dir = str(tmp_path)

        temp_file = tmp_path / "temp_file.txt"
        temp_file.touch()

        with patch.object(
            S3AssetManager, "prepare_paths_for_upload"
        ) as prepare_paths_mock, patch.object(
            api, "hash_attachments", return_value=(Mock(), [])
        ) as hash_attachments_mock:

            asset_group_mock = AssetRootGroup(
                root_path=root_dir,
                inputs=set(),
                outputs=set(),
                references=set(),
            )
            upload_group_mock = Mock(
                asset_groups=[asset_group_mock],
                total_input_files=1,
                total_input_bytes=100,
            )
            prepare_paths_mock.return_value = upload_group_mock

            runner = CliRunner()
            result = runner.invoke(main, ["asset", "snapshot", "--root-dir", root_dir])

            assert result.exit_code == 0
            prepare_paths_mock.assert_called_once_with(
                input_paths=[str(temp_file)], output_paths=[root_dir], referenced_paths=[]
            )
            hash_attachments_mock.assert_called_once()

    def test_invalid_root_directory(self, tmp_path):
        """
        Tests if CLI snapshot raises error when called with invalid --root-dir
        """
        invalid_root_dir = str(tmp_path / "invalid_dir")

        runner = CliRunner()
        result = runner.invoke(main, ["asset", "snapshot", "--root-dir", invalid_root_dir])

        assert result.exit_code != 0
        assert f"Specified root directory {invalid_root_dir} does not exist. " in result.output

    def test_valid_manifest_out(self, tmp_path):
        """
        Tests if CLI snapshot command correctly calls with --manifest-out arguement
        """
        root_dir = str(tmp_path)
        manifest_out_dir = tmp_path / "manifest_out"
        manifest_out_dir.mkdir()

        temp_file = tmp_path / "temp_file.txt"
        temp_file.touch()

        with patch.object(
            S3AssetManager, "prepare_paths_for_upload"
        ) as prepare_paths_mock, patch.object(
            api, "hash_attachments", return_value=(Mock(), [])
        ) as hash_attachments_mock:

            asset_group_mock = AssetRootGroup(
                root_path=root_dir,
                inputs=set(),
                outputs=set(),
                references=set(),
            )
            upload_group_mock = Mock(
                asset_groups=[asset_group_mock],
                total_input_files=1,
                total_input_bytes=100,
            )
            prepare_paths_mock.return_value = upload_group_mock

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
            prepare_paths_mock.assert_called_once_with(
                input_paths=[str(temp_file)], output_paths=[root_dir], referenced_paths=[]
            )
            hash_attachments_mock.assert_called_once()

    def test_invalid_manifest_out(self, tmp_path):
        """
        Tests if CLI snapshot raises error when called with invalid --manifest-out
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

    def test_asset_snapshot_recursive(self, tmp_path):
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

        with patch.object(
            S3AssetManager, "prepare_paths_for_upload"
        ) as prepare_paths_mock, patch.object(
            api, "hash_attachments", return_value=(Mock(), [])
        ) as hash_attachments_mock:

            expected_inputs = {str(subdir2 / "file2.txt"), str(subdir1 / "file1.txt")}
            asset_group_mock = AssetRootGroup(
                root_path=root_dir,
                inputs=set(),
                outputs=set(),
                references=set(),
            )
            upload_group_mock = Mock(
                asset_groups=[asset_group_mock],
                total_input_files=1,
                total_input_bytes=100,
            )
            prepare_paths_mock.return_value = upload_group_mock

            runner = CliRunner()
            result = runner.invoke(
                main, ["asset", "snapshot", "--root-dir", root_dir, "--recursive"]
            )

        assert result.exit_code == 0
        actual_inputs = set(prepare_paths_mock.call_args[1]["input_paths"])
        assert actual_inputs == expected_inputs
        hash_attachments_mock.assert_called_once()
