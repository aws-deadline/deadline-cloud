# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Integ tests for the CLI attachment commands.
"""

import os
import json
from typing import Tuple
from click.testing import CliRunner
from dataclasses import asdict

import pytest
import tempfile

from deadline_test_fixtures.job_attachment_manager import JobAttachmentManager

from deadline.client.cli import main
from deadline.job_attachments.asset_manifests import (
    HashAlgorithm,
    hash_data,
)
from deadline.job_attachments.models import PathMappingRule

MOCK_MANIFEST_CASE = {
    "TEST_CASE_1": {
        "hashAlg": "xxh128",
        "manifestVersion": "2023-03-03",
        "paths": [
            {
                "hash": "19a71beb47d7cc2d654ac4637e680c88",
                "mtime": 1720199667787520,
                "path": "files/file1.txt",
                "size": 14,
            }
        ],
        "totalSize": 14,
    },
    "TEST_CASE_2": {
        "hashAlg": "xxh128",
        "manifestVersion": "2023-03-03",
        "paths": [
            {
                "hash": "b03f20b08a76635964ab008a10cd20a8",
                "mtime": 1720199667787520,
                "path": "files/file2.txt",
                "size": 16,
            }
        ],
        "totalSize": 16,
    },
}
MOCK_FILE_CASE = {
    "TEST_CASE_1": "This is file 1",
    "TEST_CASE_2": "This is file two",
}


class TestAttachment:
    @pytest.fixture
    def temp_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir_path:
            asset_dir: str = os.path.join(tmpdir_path, "files")
            os.makedirs(asset_dir)
            with open(
                os.path.join(asset_dir, "file1.txt"),
                "w",
                encoding="utf8",
            ) as f:
                f.write(MOCK_FILE_CASE["TEST_CASE_1"])

            with open(
                os.path.join(asset_dir, "file2.txt"),
                "w",
                encoding="utf8",
            ) as f:
                f.write(MOCK_FILE_CASE["TEST_CASE_2"])

            yield tmpdir_path

    @pytest.fixture
    def job_attachment_resources(self, deploy_job_attachment_resources: JobAttachmentManager):
        if deploy_job_attachment_resources.farm_id is None:
            raise TypeError("The Farm ID was not properly retrieved when initializing resources.")
        if (
            deploy_job_attachment_resources.queue is None
            or deploy_job_attachment_resources.queue_with_no_settings is None
        ):
            raise TypeError("The Queues were not properly created when initializing resources.")

        yield deploy_job_attachment_resources

    @pytest.mark.cross_account
    @pytest.mark.integ
    def test_attachment_s3_cross_account_access_denied(self, external_bucket, temp_dir):
        # Given
        file_name: str = f"{hash_data(temp_dir.encode('utf-8'), HashAlgorithm.XXH128)}_output"
        manifest_path: str = os.path.join(temp_dir, file_name)

        with open(
            manifest_path,
            "w",
            encoding="utf8",
        ) as f:
            json.dump(MOCK_MANIFEST_CASE.get("TEST_CASE_1"), f)

        # When - test upload the local asset file
        runner = CliRunner()

        result = runner.invoke(
            main,
            [
                "attachment",
                "upload",
                "--manifests",
                manifest_path,
                "--root-dirs",
                temp_dir,
                "--profile",
                "default",
                "--s3-root-uri",
                f"s3://{external_bucket}/test",
            ],
        )
        assert result.exit_code != 0, (
            f"Expecting cross-account s3 access to fail but not, CLI output {result.output}"
        )
        assert "HTTP Status Code: 403, Access denied." in result.output

        result = runner.invoke(
            main,
            [
                "attachment",
                "download",
                "--manifests",
                manifest_path,
                "--profile",
                "default",
                "--s3-root-uri",
                f"s3://{external_bucket}/test",
            ],
        )
        assert result.exit_code != 0, (
            f"Expecting cross-account s3 access to fail but not, CLI output {result.output}"
        )
        assert "HTTP Status Code: 403, Forbidden or Access denied." in result.output

    def _run_attachment_basic_flow(
        self, temp_dir, job_attachment_resources, manifest_case_key
    ) -> Tuple[str, str, str]:
        """
        Helper function that runs the basic attachment flow and returns file info.
        This is used by both the basic flow test and the file override test.
        """
        # Given
        file_name: str = f"{hash_data(temp_dir.encode('utf-8'), HashAlgorithm.XXH128)}_output"
        manifest_path: str = os.path.join(temp_dir, file_name)

        with open(
            manifest_path,
            "w",
            encoding="utf8",
        ) as f:
            json.dump(MOCK_MANIFEST_CASE[manifest_case_key], f)

        s3_root_uri = f"s3://{job_attachment_resources.bucket_name}/{job_attachment_resources.bucket_root_prefix}"

        runner = CliRunner()

        # When - test upload the local asset file
        result = runner.invoke(
            main,
            [
                "attachment",
                "upload",
                "--manifests",
                manifest_path,
                "--root-dirs",
                temp_dir,
                "--profile",
                "default",
                "--s3-root-uri",
                s3_root_uri,
            ],
        )
        # Then
        assert result.exit_code == 0, f"Non-Zeo exit code, CLI output {result.output}"

        # When - test download the file just uploaded
        result = runner.invoke(
            main,
            [
                "attachment",
                "download",
                "--manifests",
                manifest_path,
                "--profile",
                "default",
                "--s3-root-uri",
                s3_root_uri,
                "--json",
            ],
        )
        # Then
        assert result.exit_code == 0, f"Non-Zeo exit code, CLI output {result.output}"
        assert json.loads(result.output)["processed_bytes"] == len(
            MOCK_FILE_CASE[manifest_case_key]
        )
        assert file_name in os.listdir(os.getcwd()), (
            "Expecting downloaded folder named with data hash created in the working directory with downloaded files but not."
        )
        asset_files = os.listdir(os.path.join(os.getcwd(), file_name, "files"))
        assert len(asset_files) == 1

        return file_name, manifest_path, s3_root_uri

    @pytest.mark.integ
    @pytest.mark.parametrize("manifest_case_key", MOCK_MANIFEST_CASE.keys())
    def test_attachment_basic_flow(self, temp_dir, job_attachment_resources, manifest_case_key):
        """Test the basic attachment upload and download flow."""
        # Run the basic attachment flow and verify the results through assertions
        # (all assertions are done within _run_attachment_basic_flow)
        self._run_attachment_basic_flow(temp_dir, job_attachment_resources, manifest_case_key)

    @pytest.mark.integ
    @pytest.mark.parametrize("manifest_case_key", MOCK_MANIFEST_CASE.keys())
    def test_attachment_path_mapping_flow(
        self, temp_dir, job_attachment_resources, manifest_case_key
    ):
        # Given
        source_path: str = os.path.join(temp_dir, "virtual_source")
        destination_path: str = temp_dir

        file_name: str = f"{hash_data(source_path.encode('utf-8'), HashAlgorithm.XXH128)}_output"
        manifest_path: str = os.path.join(temp_dir, file_name)

        with open(
            manifest_path,
            "w",
            encoding="utf8",
        ) as f:
            json.dump(MOCK_MANIFEST_CASE[manifest_case_key], f)

        path_mapping_file_path: str = os.path.join(temp_dir, "path_mapping")
        with open(path_mapping_file_path, "w", encoding="utf8") as f:
            f.write(
                json.dumps(
                    [
                        asdict(
                            PathMappingRule(
                                source_path_format="posix",
                                source_path=source_path,
                                destination_path=destination_path,
                            )
                        )
                    ]
                )
            )

        s3_root_uri = f"s3://{job_attachment_resources.bucket_name}/{job_attachment_resources.bucket_root_prefix}"

        runner = CliRunner()

        # When - test upload the local asset file with path mapping
        result = runner.invoke(
            main,
            [
                "attachment",
                "upload",
                "--manifests",
                manifest_path,
                "--path-mapping-rules",
                path_mapping_file_path,
                "--profile",
                "default",
                "--s3-root-uri",
                s3_root_uri,
            ],
        )
        # Then
        assert result.exit_code == 0, f"Non-Zeo exit code, CLI output {result.output}"

        # When - test download the file just uploaded with path mapping
        result = runner.invoke(
            main,
            [
                "attachment",
                "download",
                "--manifests",
                manifest_path,
                "--path-mapping-rules",
                path_mapping_file_path,
                "--profile",
                "default",
                "--s3-root-uri",
                s3_root_uri,
                "--json",
            ],
        )
        # Then
        assert result.exit_code == 0, f"Non-Zeo exit code, CLI output {result.output}"
        assert json.loads(result.output)["processed_bytes"] == len(
            MOCK_FILE_CASE[manifest_case_key]
        )

        asset_files = os.listdir(os.path.join(destination_path, "files"))
        assert len(asset_files) == 3, (
            f"Expecting 3 asset files, 2 from upload and 1 from download, but got {len(asset_files)}."
        )

    @pytest.mark.integ
    @pytest.mark.parametrize(
        "override_mode,expected_num_files, expected_files",
        [
            pytest.param("CREATE_COPY", 2, ["file1.txt", "file1 (1).txt"]),
            pytest.param("SKIP", 1, ["file1.txt"]),
            pytest.param("OVERWRITE", 1, ["file1.txt"]),
        ],
    )
    def test_attachment_file_override(
        self, temp_dir, job_attachment_resources, override_mode, expected_num_files, expected_files
    ):
        test_case_key = "TEST_CASE_1"
        file_name, manifest_path, s3_root_uri = self._run_attachment_basic_flow(
            temp_dir=temp_dir,
            job_attachment_resources=job_attachment_resources,
            manifest_case_key=test_case_key,
        )

        # When 2 - test download again with file override mode.
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "attachment",
                "download",
                "--manifests",
                manifest_path,
                "--profile",
                "default",
                "--s3-root-uri",
                s3_root_uri,
                "--conflict-resolution",
                override_mode,
                "--json",
            ],
        )
        # Then
        assert result.exit_code == 0, f"Non-Zeo exit code, CLI output {result.output}"

        # How many bytes downloaded.
        expected_processed_bytes = (
            0 if override_mode == "SKIP" else len(MOCK_FILE_CASE[test_case_key])
        )
        assert json.loads(result.output)["processed_bytes"] == expected_processed_bytes
        assert file_name in os.listdir(os.getcwd()), (
            "Expecting downloaded folder named with data hash created in the working directory with downloaded files but not."
        )
        asset_files = os.listdir(os.path.join(os.getcwd(), file_name, "files"))
        assert len(asset_files) == expected_num_files

        # Make sure the files are named correctly and what we expected to download.
        for file in expected_files:
            assert file in asset_files
