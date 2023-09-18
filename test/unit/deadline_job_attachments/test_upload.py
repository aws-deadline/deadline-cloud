# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests related to the uploading of assets.
"""

import os
import sys
from copy import deepcopy
from datetime import datetime
from io import BytesIO
from logging import DEBUG, WARNING
from pathlib import Path
from typing import Dict, List, Set, Tuple
from unittest.mock import MagicMock, patch

import boto3
import py.path
import pytest
from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import ClientError
from botocore.stub import Stubber
from moto import mock_sts

import deadline
from deadline.job_attachments.asset_manifests import BaseManifestModel, ManifestVersion
from deadline.job_attachments.exceptions import (
    AssetSyncError,
    JobAttachmentsS3ClientError,
    MissingS3BucketError,
    MissingS3RootPrefixError,
)
from deadline.job_attachments.models import (
    AssetRootGroup,
    Attachments,
    FileSystemLocation,
    FileSystemLocationType,
    ManifestProperties,
    HashCacheEntry,
    JobAttachmentS3Settings,
    OperatingSystemFamily,
    StorageProfile,
)
from deadline.job_attachments.progress_tracker import (
    ProgressStatus,
    SummaryStatistics,
)
from deadline.job_attachments.upload import S3AssetManager, S3AssetUploader
from deadline.job_attachments._utils import _human_readable_file_size


class TestUpload:
    """
    Tests for handling uploading assets.
    """

    @pytest.fixture(autouse=True)
    def before_test(
        self, request, create_s3_bucket, default_job_attachment_s3_settings: JobAttachmentS3Settings
    ):
        """
        Setup the default queue and s3 bucket for all asset tests.
        Mark test with `no_setup` if you don't want this setup to run.
        """
        if "no_setup" in request.keywords:
            return

        self.job_attachment_s3_settings = default_job_attachment_s3_settings
        create_s3_bucket(bucket_name=default_job_attachment_s3_settings.s3BucketName)

    @mock_sts
    @pytest.mark.parametrize(
        "manifest_version,expected_manifest",
        [
            (
                ManifestVersion.v2023_03_03,
                '{"hashAlg":"xxh128","manifestVersion":"2023-03-03",'
                '"paths":[{"hash":"d","mtime":1234000000,"path":"meta.txt","size":1},'
                '{"hash":"a","mtime":1234000000,"path":"scene/maya.ma","size":1},'
                '{"hash":"c","mtime":1234000000,"path":"textures/normals/normal.png","size":1},'
                '{"hash":"b","mtime":1234000000,"path":"textures/texture.png","size":1}],"totalSize":4}',
            ),
        ],
    )
    def test_asset_management(
        self,
        tmpdir: py.path.local,
        farm_id,
        queue_id,
        assert_canonical_manifest,
        assert_expected_files_on_s3,
        caplog,
        manifest_version: ManifestVersion,
        expected_manifest: str,
    ):
        """
        Test that the correct files get uploaded to S3 and the asset manifest
        is as expected when there are multiple input and output files.
        """
        # Given
        asset_root = str(tmpdir)

        scene_file = tmpdir.mkdir("scene").join("maya.ma")
        scene_file.write("a")
        os.utime(scene_file, (1234, 1234))

        texture_file = tmpdir.mkdir("textures").join("texture.png")
        texture_file.write("b")
        os.utime(texture_file, (1234, 1234))

        normal_file = tmpdir.join("textures").mkdir("normals").join("normal.png")
        normal_file.write("c")
        os.utime(normal_file, (1234, 1234))

        meta_file = tmpdir.join("meta.txt")
        meta_file.write("d")
        os.utime(meta_file, (1234, 1234))

        cache_dir = tmpdir.mkdir("cache")
        output_dir1 = tmpdir.join("outputs")
        output_dir2 = tmpdir.join("outputs").join("textures")

        expected_total_input_bytes = (
            scene_file.size() + texture_file.size() + normal_file.size() + meta_file.size()
        )

        with patch(
            f"{deadline.__package__}.job_attachments.upload._get_deadline_formatted_os",
            return_value="linux",
        ), patch(
            f"{deadline.__package__}.job_attachments.upload._hash_data",
            side_effect=["e", "manifesthash"],
        ), patch(
            f"{deadline.__package__}.job_attachments.upload._hash_file",
            side_effect={
                str(scene_file): "a",
                str(texture_file): "b",
                str(normal_file): "c",
                str(meta_file): "d",
            }.get,
        ), patch(
            f"{deadline.__package__}.job_attachments.models._generate_random_guid",
            return_value="0000",
        ):
            caplog.set_level(DEBUG)

            mock_on_preparing_to_submit = MagicMock(return_value=True)
            mock_on_uploading_assets = MagicMock(return_value=True)

            asset_manager = S3AssetManager(
                farm_id=farm_id,
                queue_id=queue_id,
                job_attachment_settings=self.job_attachment_s3_settings,
                asset_manifest_version=manifest_version,
            )

            # When
            (
                hash_summary_statstics,
                asset_root_manifests,
            ) = asset_manager.hash_assets_and_create_manifest(
                input_paths=[
                    str(scene_file),
                    str(texture_file),
                    str(normal_file),
                    str(meta_file),
                    str(meta_file),
                    "",
                ],
                output_paths=[
                    str(asset_root),
                    str(output_dir1),
                    str(output_dir2),
                    str(output_dir2),
                    "",
                ],
                referenced_paths=[],
                hash_cache_dir=str(cache_dir),
                on_preparing_to_submit=mock_on_preparing_to_submit,
            )

            (upload_summary_statstics, attachments) = asset_manager.upload_assets(
                manifests=asset_root_manifests,
                on_uploading_assets=mock_on_uploading_assets,
            )

            # Then
            expected_attachments = Attachments(
                manifests=[
                    ManifestProperties(
                        rootPath=asset_root,
                        osType=OperatingSystemFamily.LINUX,
                        inputManifestPath=f"{farm_id}/{queue_id}/Inputs/0000/e_input.xxh128",
                        inputManifestHash="manifesthash",
                        outputRelativeDirectories=[
                            ".",
                            "outputs",
                            os.path.join("outputs", "textures"),
                        ],
                    )
                ],
            )

            assert attachments == expected_attachments
            assert attachments.to_dict() == {  # type: ignore
                "assetLoadingMethod": "PRELOAD",
                "manifests": [
                    {
                        "rootPath": f"{asset_root}",
                        "osType": OperatingSystemFamily("linux").value,
                        "inputManifestPath": f"{farm_id}/{queue_id}/Inputs/0000/e_input.xxh128",
                        "inputManifestHash": "manifesthash",
                        "outputRelativeDirectories": [
                            ".",
                            "outputs",
                            os.path.join("outputs", "textures"),
                        ],
                    }
                ],
            }

            assert (
                f"assetRoot/Manifests/{farm_id}/{queue_id}/Inputs/0000/e_input.xxh128"
                in caplog.text
            )

            assert_progress_report_last_callback(
                num_input_files=4,
                expected_total_input_bytes=expected_total_input_bytes,
                on_preparing_to_submit=mock_on_preparing_to_submit,
                on_uploading_assets=mock_on_uploading_assets,
            )

            assert_progress_report_summary_statistics(
                actual_summary_statstics=hash_summary_statstics,
                processed_files=4,
                processed_bytes=expected_total_input_bytes,
                skipped_files=0,
                skipped_bytes=0,
            )

            assert_progress_report_summary_statistics(
                actual_summary_statstics=upload_summary_statstics,
                processed_files=4,
                processed_bytes=expected_total_input_bytes,
                skipped_files=0,
                skipped_bytes=0,
            )

            s3 = boto3.Session(region_name="us-west-2").resource(
                "s3"
            )  # pylint: disable=invalid-name
            bucket = s3.Bucket(self.job_attachment_s3_settings.s3BucketName)

            assert_expected_files_on_s3(
                bucket,
                expected_files={
                    f"assetRoot/Manifests/{farm_id}/{queue_id}/Inputs/0000/e_input.xxh128",
                    f"{self.job_attachment_s3_settings.full_cas_prefix()}/a",
                    f"{self.job_attachment_s3_settings.full_cas_prefix()}/b",
                    f"{self.job_attachment_s3_settings.full_cas_prefix()}/c",
                    f"{self.job_attachment_s3_settings.full_cas_prefix()}/d",
                },
            )

            assert_canonical_manifest(
                bucket,
                f"assetRoot/Manifests/{farm_id}/{queue_id}/Inputs/0000/e_input.xxh128",
                expected_manifest=expected_manifest,
            )

    @mock_sts
    @pytest.mark.skipif(
        sys.platform != "win32",
        reason="Requires Windows to test resolving paths completely with multiple drives",
    )
    @pytest.mark.parametrize(
        "manifest_version,expected_manifest",
        [
            (
                ManifestVersion.v2023_03_03,
                '{"hashAlg":"xxh128","manifestVersion":"2023-03-03",'
                '"paths":[{"hash":"a","mtime":1234000000,"path":"input.txt","size":1}],"totalSize":1}',
            ),
        ],
    )
    def test_asset_management_windows_multi_root(
        self,
        tmpdir,
        farm_id,
        queue_id,
        assert_canonical_manifest,
        assert_expected_files_on_s3,
        caplog,
        manifest_version,
        expected_manifest,
    ):
        """
        Test that the correct files get uploaded to S3 and the asset manifest
        is as expected when there are multiple input and output files.
        """
        # Given
        root_c = tmpdir.mkdir("c-drive-inputs")
        input_c = root_c.join("input.txt")
        input_c.write("a")
        os.utime(input_c, (1234, 1234))
        input_d = r"D:\my\awesome\input2.txt"  # doesn't exist, shouldn't get included
        output_d = r"D:\my\awesome\outputdir"
        cache_dir = tmpdir.mkdir("cache")

        with patch(
            f"{deadline.__package__}.job_attachments.upload._hash_data",
            side_effect=["b", "manifesthash"],
        ), patch(
            f"{deadline.__package__}.job_attachments.upload._hash_file",
            side_effect={str(input_c): "a"}.get,
        ), patch(
            f"{deadline.__package__}.job_attachments.models._generate_random_guid",
            return_value="0000",
        ):
            caplog.set_level(DEBUG)

            mock_on_preparing_to_submit = MagicMock(return_value=True)
            mock_on_uploading_assets = MagicMock(return_value=True)

            asset_manager = S3AssetManager(
                farm_id=farm_id,
                queue_id=queue_id,
                job_attachment_settings=self.job_attachment_s3_settings,
                asset_manifest_version=manifest_version,
            )

            # When
            (
                hash_summary_statstics,
                asset_root_manifests,
            ) = asset_manager.hash_assets_and_create_manifest(
                input_paths=[input_c, input_d],
                output_paths=[output_d],
                referenced_paths=[],
                hash_cache_dir=cache_dir,
                on_preparing_to_submit=mock_on_preparing_to_submit,
            )

            (upload_summary_statstics, attachments) = asset_manager.upload_assets(
                manifests=asset_root_manifests,
                on_uploading_assets=mock_on_uploading_assets,
            )

            # Then
            expected_attachments = Attachments(
                manifests=[
                    ManifestProperties(
                        rootPath=root_c,
                        osType=OperatingSystemFamily.WINDOWS,
                        inputManifestPath=f"{farm_id}/{queue_id}/Inputs/0000/b_input.xxh128",
                        inputManifestHash="manifesthash",
                        outputRelativeDirectories=[],
                    ),
                    ManifestProperties(
                        rootPath=output_d,
                        outputRelativeDirectories=["."],
                    ),
                ],
            )
            expected_total_input_bytes = input_c.size()

            assert attachments == expected_attachments
            assert attachments.to_dict() == {  # type: ignore
                "assetLoadingMethod": "PRELOAD",
                "manifests": [
                    {
                        "rootPath": f"{root_c}",
                        "osType": OperatingSystemFamily("windows").value,
                        "inputManifestPath": f"{farm_id}/{queue_id}/Inputs/0000/b_input.xxh128",
                        "inputManifestHash": "manifesthash",
                    },
                    {
                        "rootPath": f"{output_d}",
                        "osType": OperatingSystemFamily("windows").value,
                        "outputRelativeDirectories": [
                            ".",
                        ],
                    },
                ],
            }

            assert (
                f"assetRoot/Manifests/{farm_id}/{queue_id}/Inputs/0000/b_input.xxh128"
                in caplog.text
            )

            assert_progress_report_last_callback(
                num_input_files=1,
                expected_total_input_bytes=expected_total_input_bytes,
                on_preparing_to_submit=mock_on_preparing_to_submit,
                on_uploading_assets=mock_on_uploading_assets,
            )

            assert_progress_report_summary_statistics(
                actual_summary_statstics=hash_summary_statstics,
                processed_files=1,
                processed_bytes=expected_total_input_bytes,
                skipped_files=0,
                skipped_bytes=0,
            )

            assert_progress_report_summary_statistics(
                actual_summary_statstics=upload_summary_statstics,
                processed_files=1,
                processed_bytes=expected_total_input_bytes,
                skipped_files=0,
                skipped_bytes=0,
            )

            s3 = boto3.Session(region_name="us-west-2").resource(
                "s3"
            )  # pylint: disable=invalid-name
            bucket = s3.Bucket(self.job_attachment_s3_settings.s3BucketName)

            assert_expected_files_on_s3(
                bucket,
                expected_files={
                    f"{self.job_attachment_s3_settings.rootPrefix}/Manifests/{farm_id}/{queue_id}/Inputs/0000/b_input.xxh128",
                    f"{self.job_attachment_s3_settings.full_cas_prefix()}/a",
                },
            )

            assert_canonical_manifest(
                bucket,
                f"assetRoot/Manifests/{farm_id}/{queue_id}/Inputs/0000/b_input.xxh128",
                expected_manifest=expected_manifest,
            )

    @mock_sts
    @pytest.mark.parametrize(
        "num_input_files",
        [
            deadline.job_attachments.upload.LIST_OBJECT_THRESHOLD + 1,
            deadline.job_attachments.upload.LIST_OBJECT_THRESHOLD + 100,
            deadline.job_attachments.upload.LIST_OBJECT_THRESHOLD + 200,
        ],
    )
    @pytest.mark.parametrize(
        "manifest_version",
        [
            ManifestVersion.v2023_03_03,
        ],
    )
    def test_asset_management_many_inputs(
        self,
        tmpdir,
        farm_id,
        queue_id,
        assert_canonical_manifest,
        assert_expected_files_on_s3,
        caplog,
        manifest_version: ManifestVersion,
        num_input_files: int,
    ):
        """
        Test that the correct files get uploaded to S3 and the asset manifest
        is as expected when there are multiple input and output files.
        """
        # Given
        asset_root = str(tmpdir)

        with patch(
            f"{deadline.__package__}.job_attachments.upload._get_deadline_formatted_os",
            return_value="linux",
        ), patch(
            f"{deadline.__package__}.job_attachments.upload._hash_data",
            side_effect=["c", "manifesthash"],
        ), patch(
            f"{deadline.__package__}.job_attachments.upload._hash_file",
            side_effect=[str(i) for i in range(num_input_files)],
        ), patch(
            f"{deadline.__package__}.job_attachments.models._generate_random_guid",
            return_value="0000",
        ):
            caplog.set_level(DEBUG)

            mock_on_preparing_to_submit = MagicMock(return_value=True)
            mock_on_uploading_assets = MagicMock(return_value=True)

            asset_manager = S3AssetManager(
                farm_id=farm_id,
                queue_id=queue_id,
                job_attachment_settings=self.job_attachment_s3_settings,
                asset_manifest_version=manifest_version,
            )

            input_files = []
            expected_total_input_bytes = 0
            test_dir = tmpdir.mkdir("large_submit")
            for i in range(num_input_files):
                test_file = test_dir.join(f"test{i}.txt")
                test_file.write(f"test {i}")
                expected_total_input_bytes += test_file.size()
                input_files.append(test_file)

            cache_dir = tmpdir.mkdir("cache")

            # When
            (
                hash_summary_statstics,
                asset_root_manifests,
            ) = asset_manager.hash_assets_and_create_manifest(
                input_paths=input_files,
                output_paths=[str(Path(asset_root).joinpath("outputs"))],
                referenced_paths=[],
                hash_cache_dir=cache_dir,
                on_preparing_to_submit=mock_on_preparing_to_submit,
            )

            (upload_summary_statstics, attachments) = asset_manager.upload_assets(
                manifests=asset_root_manifests,
                on_uploading_assets=mock_on_uploading_assets,
            )

            # Then
            expected_attachments = Attachments(
                manifests=[
                    ManifestProperties(
                        rootPath=asset_root,
                        osType=OperatingSystemFamily.LINUX,
                        inputManifestPath=f"{farm_id}/{queue_id}/Inputs/0000/c_input.xxh128",
                        inputManifestHash="manifesthash",
                        outputRelativeDirectories=["outputs"],
                    )
                ],
            )

            assert attachments == expected_attachments
            assert attachments.to_dict() == {  # type: ignore
                "assetLoadingMethod": "PRELOAD",
                "manifests": [
                    {
                        "rootPath": f"{asset_root}",
                        "osType": OperatingSystemFamily("linux").value,
                        "inputManifestPath": f"{farm_id}/{queue_id}/Inputs/0000/c_input.xxh128",
                        "inputManifestHash": "manifesthash",
                        "outputRelativeDirectories": ["outputs"],
                    }
                ],
            }

            assert (
                f"assetRoot/Manifests/{farm_id}/{queue_id}/Inputs/0000/c_input.xxh128"
                in caplog.text
            )

            assert_progress_report_last_callback(
                num_input_files=num_input_files,
                expected_total_input_bytes=expected_total_input_bytes,
                on_preparing_to_submit=mock_on_preparing_to_submit,
                on_uploading_assets=mock_on_uploading_assets,
            )

            assert_progress_report_summary_statistics(
                actual_summary_statstics=hash_summary_statstics,
                processed_files=num_input_files,
                processed_bytes=expected_total_input_bytes,
                skipped_files=0,
                skipped_bytes=0,
            )

            assert_progress_report_summary_statistics(
                actual_summary_statstics=upload_summary_statstics,
                processed_files=num_input_files,
                processed_bytes=expected_total_input_bytes,
                skipped_files=0,
                skipped_bytes=0,
            )

            s3 = boto3.Session(region_name="us-west-2").resource(
                "s3"
            )  # pylint: disable=invalid-name
            bucket = s3.Bucket(self.job_attachment_s3_settings.s3BucketName)

            expected_files = set(
                [
                    f"{self.job_attachment_s3_settings.full_cas_prefix()}/{i}"
                    for i in range(num_input_files)
                ]
            )
            expected_files.add(
                f"assetRoot/Manifests/{farm_id}/{queue_id}/Inputs/0000/c_input.xxh128",
            )
            assert_expected_files_on_s3(bucket, expected_files=expected_files)

    @mock_sts
    @pytest.mark.parametrize(
        "num_input_files",
        [
            deadline.job_attachments.upload.LIST_OBJECT_THRESHOLD + 1,
            deadline.job_attachments.upload.LIST_OBJECT_THRESHOLD + 100,
            deadline.job_attachments.upload.LIST_OBJECT_THRESHOLD + 200,
        ],
    )
    @pytest.mark.parametrize(
        "manifest_version",
        [
            ManifestVersion.v2023_03_03,
        ],
    )
    def test_asset_management_many_inputs_with_same_hash(
        self,
        tmpdir,
        farm_id,
        queue_id,
        manifest_version: ManifestVersion,
        num_input_files: int,
    ):
        """
        Test that the asset management can handle many input files with the same hash.
        If files with different paths have the same content (and thus the same hash),
        they should be counted as skipped files.
        """
        asset_root = str(tmpdir)

        # Given
        with patch(
            f"{deadline.__package__}.job_attachments.upload._get_deadline_formatted_os",
            return_value="linux",
        ), patch(
            f"{deadline.__package__}.job_attachments.upload._hash_data",
            side_effect=["c", "manifesthash"],
        ), patch(
            f"{deadline.__package__}.job_attachments.upload._hash_file",
            side_effect=lambda *args, **kwargs: "samehash",
        ):
            mock_on_preparing_to_submit = MagicMock(return_value=True)
            mock_on_uploading_assets = MagicMock(return_value=True)

            asset_manager = S3AssetManager(
                farm_id=farm_id,
                queue_id=queue_id,
                job_attachment_settings=self.job_attachment_s3_settings,
                asset_manifest_version=manifest_version,
            )

            input_files = []
            expected_total_input_bytes = 0
            test_dir = tmpdir.mkdir("large_submit")
            for i in range(num_input_files):
                test_file = test_dir.join(f"test{i}.txt")
                test_file.write("same content")
                expected_total_input_bytes += test_file.size()
                input_files.append(test_file)
            expected_total_downloaded_bytes = test_file.size()

            cache_dir = tmpdir.mkdir("cache")

            # When
            (
                hash_summary_statstics,
                asset_root_manifests,
            ) = asset_manager.hash_assets_and_create_manifest(
                input_paths=input_files,
                output_paths=[str(Path(asset_root).joinpath("outputs"))],
                referenced_paths=[],
                hash_cache_dir=cache_dir,
                on_preparing_to_submit=mock_on_preparing_to_submit,
            )

            (upload_summary_statstics, _) = asset_manager.upload_assets(
                manifests=asset_root_manifests,
                on_uploading_assets=mock_on_uploading_assets,
            )

            # Then
            assert_progress_report_last_callback(
                num_input_files=num_input_files,
                expected_total_input_bytes=expected_total_input_bytes,
                on_preparing_to_submit=mock_on_preparing_to_submit,
                on_uploading_assets=mock_on_uploading_assets,
            )

            assert_progress_report_summary_statistics(
                actual_summary_statstics=hash_summary_statstics,
                processed_files=num_input_files,
                processed_bytes=expected_total_input_bytes,
                skipped_files=0,
                skipped_bytes=0,
            )

            assert_progress_report_summary_statistics(
                actual_summary_statstics=upload_summary_statstics,
                processed_files=1,
                processed_bytes=expected_total_downloaded_bytes,
                skipped_files=num_input_files - 1,
                skipped_bytes=expected_total_input_bytes - expected_total_downloaded_bytes,
            )

    @mock_sts
    @pytest.mark.parametrize(
        "manifest_version",
        [
            ManifestVersion.v2023_03_03,
        ],
    )
    def test_asset_management_no_outputs_inputs_already_uploaded(
        self,
        tmpdir,
        farm_id,
        queue_id,
        assert_expected_files_on_s3,
        caplog,
        manifest_version: ManifestVersion,
    ):
        """
        Test the input files that have already been uploaded to S3 are skipped.
        """
        already_uploaded_file = tmpdir.mkdir("scene").join("maya_scene.ma")
        already_uploaded_file.write("cool scene with lots of spheres")

        not_yet_uploaded_file = tmpdir.mkdir("textures").join("cool_texture.png")
        not_yet_uploaded_file.write("the best texture you've ever seen")

        expected_total_skipped_bytes = already_uploaded_file.size()
        expected_total_uploaded_bytes = not_yet_uploaded_file.size()
        expected_total_input_bytes = expected_total_skipped_bytes + expected_total_uploaded_bytes

        def mock_hash_file(file_path: str):
            if file_path == already_uploaded_file:
                return "existinghash"
            elif file_path == not_yet_uploaded_file:
                return "somethingnew"

        # Given
        with patch(
            f"{deadline.__package__}.job_attachments.upload._get_deadline_formatted_os",
            return_value="linux",
        ), patch(
            f"{deadline.__package__}.job_attachments.upload._hash_data",
            side_effect=["manifest", "manifesthash"],
        ), patch(
            f"{deadline.__package__}.job_attachments.upload._get_deadline_formatted_os",
            return_value="linux",
        ), patch(
            f"{deadline.__package__}.job_attachments.upload._hash_file", side_effect=mock_hash_file
        ), patch(
            f"{deadline.__package__}.job_attachments.models._generate_random_guid",
            return_value="0000",
        ):
            s3 = boto3.Session(region_name="us-west-2").resource(
                "s3"
            )  # pylint: disable=invalid-name
            bucket = s3.Bucket(self.job_attachment_s3_settings.s3BucketName)

            caplog.set_level(DEBUG)

            mock_on_preparing_to_submit = MagicMock(return_value=True)
            mock_on_uploading_assets = MagicMock(return_value=True)

            asset_manager = S3AssetManager(
                farm_id=farm_id,
                queue_id=queue_id,
                job_attachment_settings=self.job_attachment_s3_settings,
                asset_manifest_version=manifest_version,
            )

            # mock pre-uploading the file
            bucket.put_object(
                Key=f"{self.job_attachment_s3_settings.full_cas_prefix()}/existinghash",
                Body="a",
            )

            cache_dir = tmpdir.mkdir("cache")

            # When
            (
                hash_summary_statstics,
                asset_root_manifests,
            ) = asset_manager.hash_assets_and_create_manifest(
                input_paths=[already_uploaded_file, not_yet_uploaded_file],
                output_paths=[],
                referenced_paths=[],
                hash_cache_dir=cache_dir,
                on_preparing_to_submit=mock_on_preparing_to_submit,
            )

            (upload_summary_statstics, _) = asset_manager.upload_assets(
                manifests=asset_root_manifests,
                on_uploading_assets=mock_on_uploading_assets,
            )

            # Then
            assert "maya_scene.ma because it has already been uploaded to s3" in caplog.text
            assert (
                f"assetRoot/Manifests/{farm_id}/{queue_id}/Inputs/0000/manifest_input.xxh128"
                in caplog.text
            )

            assert_progress_report_last_callback(
                num_input_files=2,
                expected_total_input_bytes=expected_total_input_bytes,
                on_preparing_to_submit=mock_on_preparing_to_submit,
                on_uploading_assets=mock_on_uploading_assets,
            )

            assert_progress_report_summary_statistics(
                actual_summary_statstics=hash_summary_statstics,
                processed_files=2,
                processed_bytes=expected_total_input_bytes,
                skipped_files=0,
                skipped_bytes=0,
            )

            assert_progress_report_summary_statistics(
                actual_summary_statstics=upload_summary_statstics,
                processed_files=1,
                processed_bytes=expected_total_uploaded_bytes,
                skipped_files=1,
                skipped_bytes=expected_total_skipped_bytes,
            )

            assert_expected_files_on_s3(
                bucket,
                expected_files={
                    f"{self.job_attachment_s3_settings.rootPrefix}/Manifests/{farm_id}/{queue_id}/Inputs/0000/manifest_input.xxh128",
                    f"{self.job_attachment_s3_settings.full_cas_prefix()}/existinghash",
                    f"{self.job_attachment_s3_settings.full_cas_prefix()}/somethingnew",
                },
            )

    @mock_sts
    @pytest.mark.parametrize(
        "num_input_files",
        [
            deadline.job_attachments.upload.LIST_OBJECT_THRESHOLD + 1,
            deadline.job_attachments.upload.LIST_OBJECT_THRESHOLD + 100,
            deadline.job_attachments.upload.LIST_OBJECT_THRESHOLD + 200,
        ],
    )
    @pytest.mark.parametrize(
        "manifest_version",
        [
            ManifestVersion.v2023_03_03,
        ],
    )
    def test_asset_management_no_outputs_large_number_of_inputs_already_uploaded(
        self,
        tmpdir,
        farm_id,
        queue_id,
        assert_expected_files_on_s3,
        caplog,
        manifest_version: ManifestVersion,
        num_input_files: int,
    ):
        """
        Test the input files that have already been uploaded to S3 are skipped.
        """
        # Given
        with patch(
            f"{deadline.__package__}.job_attachments.upload._get_deadline_formatted_os",
            return_value="linux",
        ), patch(
            f"{deadline.__package__}.job_attachments.upload._hash_data",
            side_effect=["manifesto", "manifesthash"],
        ), patch(
            f"{deadline.__package__}.job_attachments.upload._get_deadline_formatted_os",
            return_value="linux",
        ), patch(
            f"{deadline.__package__}.job_attachments.upload._hash_file",
            side_effect=[str(i) for i in range(num_input_files)],
        ), patch(
            f"{deadline.__package__}.job_attachments.models._generate_random_guid",
            return_value="0000",
        ):
            s3 = boto3.Session(region_name="us-west-2").resource(
                "s3"
            )  # pylint: disable=invalid-name
            bucket = s3.Bucket(self.job_attachment_s3_settings.s3BucketName)

            caplog.set_level(DEBUG)

            mock_on_preparing_to_submit = MagicMock(return_value=True)
            mock_on_uploading_assets = MagicMock(return_value=True)

            asset_manager = S3AssetManager(
                farm_id=farm_id,
                queue_id=queue_id,
                job_attachment_settings=self.job_attachment_s3_settings,
                asset_manifest_version=manifest_version,
            )

            input_files = []
            expected_total_input_bytes = 0
            test_dir = tmpdir.mkdir("large_submit")
            for i in range(num_input_files):
                test_file = test_dir.join(f"test{i}.txt")
                test_file.write(f"test {i}")
                expected_total_input_bytes += test_file.size()
                input_files.append(test_file)
                # mock pre-uploading the file
                bucket.put_object(
                    Key=f"{self.job_attachment_s3_settings.full_cas_prefix()}/{i}",
                    Body=f"test {i}",
                )

            not_yet_uploaded_file = tmpdir.mkdir("textures").join("texture.png")
            not_yet_uploaded_file.write("b")

            cache_dir = tmpdir.mkdir("cache")

            # When
            (
                hash_summary_statstics,
                asset_root_manifests,
            ) = asset_manager.hash_assets_and_create_manifest(
                input_paths=input_files,
                output_paths=[],
                referenced_paths=[],
                hash_cache_dir=cache_dir,
                on_preparing_to_submit=mock_on_preparing_to_submit,
            )

            (upload_summary_statstics, _) = asset_manager.upload_assets(
                manifests=asset_root_manifests,
                on_uploading_assets=mock_on_uploading_assets,
            )

            # Then
            assert (
                f"assetRoot/Manifests/{farm_id}/{queue_id}/Inputs/0000/manifesto_input.xxh128"
                in caplog.text
            )

            assert_progress_report_last_callback(
                num_input_files=num_input_files,
                expected_total_input_bytes=expected_total_input_bytes,
                on_preparing_to_submit=mock_on_preparing_to_submit,
                on_uploading_assets=mock_on_uploading_assets,
            )

            assert_progress_report_summary_statistics(
                actual_summary_statstics=hash_summary_statstics,
                processed_files=num_input_files,
                processed_bytes=expected_total_input_bytes,
                skipped_files=0,
                skipped_bytes=0,
            )

            assert_progress_report_summary_statistics(
                actual_summary_statstics=upload_summary_statstics,
                processed_files=0,
                processed_bytes=0,
                skipped_files=num_input_files,
                skipped_bytes=expected_total_input_bytes,
            )

            expected_files = set(
                [
                    f"{self.job_attachment_s3_settings.full_cas_prefix()}/{i}"
                    for i in range(num_input_files)
                ]
            )
            expected_files.add(
                f"assetRoot/Manifests/{farm_id}/{queue_id}/Inputs/0000/manifesto_input.xxh128",
            )
            assert_expected_files_on_s3(bucket, expected_files=expected_files)

    @mock_sts
    @pytest.mark.parametrize(
        "manifest_version",
        [
            ManifestVersion.v2023_03_03,
        ],
    )
    def test_asset_management_no_inputs(
        self,
        tmpdir,
        farm_id,
        queue_id,
        assert_canonical_manifest,
        assert_expected_files_on_s3,
        caplog,
        manifest_version: ManifestVersion,
    ):
        """
        Test that only the manifest file gets uploaded to S3 and the asset manifest is as expected
        when there are no input files and multiple output files.
        """
        output_dir = str(tmpdir.join("outputs"))

        # Given
        with patch(
            f"{deadline.__package__}.job_attachments.upload._get_deadline_formatted_os",
            return_value="linux",
        ), patch(
            f"{deadline.__package__}.job_attachments.upload._hash_data",
            side_effect=["a", "manifesthash"],
        ), patch(
            f"{deadline.__package__}.job_attachments.models._generate_random_guid",
            return_value="0000",
        ):
            mock_on_preparing_to_submit = MagicMock(return_value=True)
            mock_on_uploading_assets = MagicMock(return_value=True)

            asset_manager = S3AssetManager(
                farm_id=farm_id,
                queue_id=queue_id,
                job_attachment_settings=self.job_attachment_s3_settings,
                asset_manifest_version=manifest_version,
            )

            cache_dir = tmpdir.mkdir("cache")

            # When
            (
                hash_summary_statstics,
                asset_root_manifests,
            ) = asset_manager.hash_assets_and_create_manifest(
                input_paths=[],
                output_paths=[output_dir],
                referenced_paths=[],
                hash_cache_dir=cache_dir,
                on_preparing_to_submit=mock_on_preparing_to_submit,
            )

            (upload_summary_statstics, attachments) = asset_manager.upload_assets(
                manifests=asset_root_manifests,
                on_uploading_assets=mock_on_uploading_assets,
            )

            # Then
            expected_attachments = Attachments(
                manifests=[
                    ManifestProperties(
                        rootPath=output_dir,
                        osType=OperatingSystemFamily.LINUX,
                        outputRelativeDirectories=["."],
                    )
                ],
            )

            assert attachments == expected_attachments
            assert attachments.to_dict() == {  # type: ignore
                "assetLoadingMethod": "PRELOAD",
                "manifests": [
                    {
                        "rootPath": f"{output_dir}",
                        "osType": OperatingSystemFamily("linux").value,
                        "outputRelativeDirectories": ["."],
                    }
                ],
            }

            assert_progress_report_summary_statistics(
                actual_summary_statstics=hash_summary_statstics,
                processed_files=0,
                processed_bytes=0,
                skipped_files=0,
                skipped_bytes=0,
            )

            assert_progress_report_summary_statistics(
                actual_summary_statstics=upload_summary_statstics,
                processed_files=0,
                processed_bytes=0,
                skipped_files=0,
                skipped_bytes=0,
            )

    @pytest.mark.parametrize(
        "manifest_version",
        [
            ManifestVersion.v2023_03_03,
        ],
    )
    def test_asset_management_no_s3_bucket_set(
        self,
        farm_id,
        queue_id,
        manifest_version: ManifestVersion,
    ):
        """
        Test that the appropriate error is raised when no s3 bucket is provided.
        """
        missing_s3_job_attachment_settings = deepcopy(self.job_attachment_s3_settings)

        del missing_s3_job_attachment_settings.s3BucketName

        with pytest.raises(AttributeError):
            S3AssetManager(
                farm_id=farm_id,
                queue_id=queue_id,
                job_attachment_settings=missing_s3_job_attachment_settings,
                asset_manifest_version=manifest_version,
            )

    @pytest.mark.parametrize(
        "manifest_version",
        [
            ManifestVersion.v2023_03_03,
        ],
    )
    def test_asset_management_with_s3_bucket_empty(
        self,
        farm_id,
        queue_id,
        manifest_version: ManifestVersion,
    ):
        """
        Test that the appropriate error is raised when no s3 bucket is provided.
        """
        s3_job_attachment_settings_with_s3_bucket_empty = deepcopy(self.job_attachment_s3_settings)
        s3_job_attachment_settings_with_s3_bucket_empty.s3BucketName = ""

        with pytest.raises(MissingS3BucketError):
            S3AssetManager(
                farm_id=farm_id,
                queue_id=queue_id,
                job_attachment_settings=s3_job_attachment_settings_with_s3_bucket_empty,
                asset_manifest_version=manifest_version,
            )

    @pytest.mark.parametrize(
        "manifest_version",
        [
            ManifestVersion.v2023_03_03,
        ],
    )
    def test_asset_management_no_s3_root_prefix_set(
        self,
        farm_id,
        queue_id,
        manifest_version: ManifestVersion,
    ):
        """
        Test that the appropriate error is raised when no s3 root prefix is provided.
        """
        missing_s3_job_attachment_settings = deepcopy(self.job_attachment_s3_settings)

        del missing_s3_job_attachment_settings.rootPrefix

        with pytest.raises(AttributeError):
            S3AssetManager(
                farm_id=farm_id,
                queue_id=queue_id,
                job_attachment_settings=missing_s3_job_attachment_settings,
                asset_manifest_version=manifest_version,
            )

    @pytest.mark.parametrize(
        "manifest_version",
        [
            ManifestVersion.v2023_03_03,
        ],
    )
    def test_asset_management_with_root_prefix_empty(
        self,
        farm_id,
        queue_id,
        manifest_version: ManifestVersion,
    ):
        """
        Test that the appropriate error is raised when no s3 bucket is provided.
        """
        s3_job_attachment_settings_with_root_prefix_empty = deepcopy(
            self.job_attachment_s3_settings
        )
        s3_job_attachment_settings_with_root_prefix_empty.rootPrefix = ""

        with pytest.raises(MissingS3RootPrefixError):
            S3AssetManager(
                farm_id=farm_id,
                queue_id=queue_id,
                job_attachment_settings=s3_job_attachment_settings_with_root_prefix_empty,
                asset_manifest_version=manifest_version,
            )

    def test_asset_management_manifest_version_not_implemented(self, farm_id, queue_id, tmpdir):
        """
        Test that the appropriate error is raised when the library doesn't support an asset manifest version.
        """
        with patch(
            f"{deadline.__package__}.job_attachments.upload.ManifestModelRegistry.get_manifest_model",
            return_value=BaseManifestModel,
        ):
            with pytest.raises(
                NotImplementedError,
                match=r"Creation of manifest version (ManifestVersion.)?UNDEFINED is not supported.",
            ):
                asset_manager = S3AssetManager(
                    farm_id=farm_id,
                    queue_id=queue_id,
                    job_attachment_settings=self.job_attachment_s3_settings,
                    asset_manifest_version=ManifestVersion.UNDEFINED,
                )
                cache_dir = tmpdir.mkdir("cache")
                test_file = tmpdir.join("test.txt")
                test_file.write("test")
                asset_manager.hash_assets_and_create_manifest(
                    [test_file], [], [], hash_cache_dir=cache_dir
                )

    @mock_sts
    def test_file_already_uploaded_bucket_in_different_account(self):
        """
        Test that the appropriate error is raised when checking if a file has already been uploaded, but the bucket
        is in an account that is different from the uploader's account.
        """
        s3 = boto3.client("s3")
        stubber = Stubber(s3)
        stubber.add_client_error(
            "head_object",
            service_error_code="AccessDenied",
            service_message="Access Denied",
            http_status_code=403,
        )

        uploader = S3AssetUploader()

        uploader._s3 = s3

        with stubber:
            with pytest.raises(JobAttachmentsS3ClientError) as err:
                uploader.file_already_uploaded(
                    self.job_attachment_s3_settings.s3BucketName, "test_key"
                )
            assert isinstance(err.value.__cause__, ClientError)
            assert (
                err.value.__cause__.response["ResponseMetadata"]["HTTPStatusCode"] == 403  # type: ignore[attr-defined]
            )
            assert (
                "Error checking if object exists in bucket 'test-bucket'. Target key or prefix: 'test_key'. "
                "HTTP Status Code: 403 Access denied. Ensure that the bucket is in the account 123456789012, "
                "and your AWS IAM Role or User has the 's3:ListBucket' permission for this bucket."
            ) in str(err.value)

    @mock_sts
    def test_filter_objects_to_upload_bucket_in_different_account(self):
        """
        Test that the appropriate error is raised when checking if a file has already been uploaded, but the bucket
        is in an account that is different from the uploader's account.
        """
        s3 = boto3.client("s3")
        stubber = Stubber(s3)
        stubber.add_client_error(
            "list_objects_v2",
            service_error_code="AccessDenied",
            service_message="Access Denied",
            http_status_code=403,
        )

        uploader = S3AssetUploader()

        uploader._s3 = s3

        with stubber:
            with pytest.raises(JobAttachmentsS3ClientError) as err:
                uploader.filter_objects_to_upload(
                    self.job_attachment_s3_settings.s3BucketName, "test_prefix", {"test_key"}
                )
            assert isinstance(err.value.__cause__, ClientError)
            assert (
                err.value.__cause__.response["ResponseMetadata"]["HTTPStatusCode"] == 403  # type: ignore[attr-defined]
            )
            assert (
                "Error listing bucket contents in bucket 'test-bucket'. Target key or prefix: 'test_prefix'. "
                "HTTP Status Code: 403 Forbidden or Access denied. "
            ) in str(err.value)

    @mock_sts
    def test_upload_bytes_to_s3_bucket_in_different_account(self):
        """
        Test that the appropriate error is raised when uploading bytes, but the bucket
        is in an account that is different from the uploader's account.
        """
        s3 = boto3.client("s3")
        stubber = Stubber(s3)

        # This is the error that's surfaced when a bucket is in a different account than expected.
        stubber.add_client_error(
            "put_object",
            service_error_code="AccessDenied",
            service_message="Access Denied",
            http_status_code=403,
        )

        uploader = S3AssetUploader()

        uploader._s3 = s3

        with stubber:
            with pytest.raises(JobAttachmentsS3ClientError) as err:
                uploader.upload_bytes_to_s3(
                    BytesIO(), self.job_attachment_s3_settings.s3BucketName, "test_key"
                )
            assert isinstance(err.value.__cause__, ClientError)
            assert (
                err.value.__cause__.response["ResponseMetadata"]["HTTPStatusCode"] == 403  # type: ignore[attr-defined]
            )
            assert (
                "Error uploading binary file in bucket 'test-bucket'. Target key or prefix: 'test_key'. "
                "HTTP Status Code: 403 Forbidden or Access denied. "
            ) in str(err.value)

    @mock_sts
    def test_upload_file_to_s3_bucket_in_different_account(self, tmp_path: Path):
        """
        Test that the appropriate error is raised when uploading files, but the bucket
        is in an account that is different from the uploader's account.
        """
        s3 = boto3.client("s3")
        stubber = Stubber(s3)

        # This is the error that's surfaced when a bucket is in a different account than expected.
        stubber.add_client_error(
            "put_object",
            service_error_code="AccessDenied",
            service_message="Access Denied",
            http_status_code=403,
        )

        uploader = S3AssetUploader()

        uploader._s3 = s3

        file = tmp_path / "test_file"
        file.write_text("")

        with stubber:
            with pytest.raises(AssetSyncError, match=r"Error uploading ") as err:
                uploader.upload_file_to_s3(
                    str(file), self.job_attachment_s3_settings.s3BucketName, "test_key"
                )
            assert isinstance(err.value.__cause__, S3UploadFailedError)

    @pytest.mark.parametrize(
        "manifest_version",
        [
            ManifestVersion.v2023_03_03,
        ],
    )
    def test_process_input_path_cached_file_is_updated(
        self, farm_id, queue_id, tmpdir, manifest_version: ManifestVersion
    ):
        """
        Test that a file that exists in the hash cache, but has been modified, will be hashed again.
        """
        # GIVEN
        root_dir = tmpdir.mkdir("root")
        test_file = root_dir.join("test.txt")
        test_file.write("test")
        file_time = os.stat(test_file).st_mtime
        expected_entry = HashCacheEntry(test_file, "b", str(datetime.fromtimestamp((file_time))))

        # WHEN
        test_entry = HashCacheEntry(test_file, "a", "123.45")
        hash_cache = MagicMock()
        hash_cache.get_entry.return_value = test_entry

        with patch(f"{deadline.__package__}.job_attachments.upload._hash_file", side_effect=["b"]):
            asset_manager = S3AssetManager(
                farm_id=farm_id,
                queue_id=queue_id,
                job_attachment_settings=self.job_attachment_s3_settings,
                asset_manifest_version=manifest_version,
            )

            (is_hashed, _, man_path) = asset_manager._process_input_path(
                Path(test_file), root_dir, hash_cache
            )

            # THEN
            assert is_hashed is True
            assert man_path.path == "test.txt"
            assert man_path.hash == "b"
            hash_cache.put_entry.assert_called_with(expected_entry)

    def test_process_input_path_skip_file_already_in_hash_cache(self, farm_id, queue_id, tmpdir):
        """
        Test the input files that already exists in the hash cache are skipped hashing.
        """
        # GIVEN
        root_dir = tmpdir.mkdir("root")
        test_file = root_dir.join("test.txt")
        test_file.write("test")
        file_time = str(datetime.fromtimestamp(os.stat(test_file).st_mtime))
        file_bytes = test_file.size()

        # WHEN
        test_entry = HashCacheEntry(test_file, "a", file_time)
        hash_cache = MagicMock()
        hash_cache.get_entry.return_value = test_entry

        with patch(f"{deadline.__package__}.job_attachments.upload._hash_file", side_effect=["a"]):
            asset_manager = S3AssetManager(
                farm_id=farm_id,
                queue_id=queue_id,
                job_attachment_settings=self.job_attachment_s3_settings,
                asset_manifest_version=ManifestVersion.v2023_03_03,
            )

            (is_hashed, size, man_path) = asset_manager._process_input_path(
                Path(test_file), root_dir, hash_cache
            )
            _ = asset_manager._create_manifest_file(
                [Path(test_file)], root_dir, hash_cache=hash_cache
            )

            # THEN
            assert is_hashed is False
            assert size == file_bytes
            assert man_path.path == "test.txt"
            assert man_path.hash == "a"
            hash_cache.put_entry.assert_not_called()

    @mock_sts
    def test_asset_management_input_not_exists(self, farm_id, queue_id, tmpdir, caplog):
        """Test the input paths that does not exist are properly skipped"""
        asset_root = str(tmpdir)

        # GIVEN
        scene_file = tmpdir.mkdir("scene").join("maya.ma")
        scene_file.write("a")
        input_not_exist = "/texture/that/doesnt/exist.anywhere"

        cache_dir = tmpdir.mkdir("cache")

        expected_total_input_bytes = scene_file.size()

        with patch(
            f"{deadline.__package__}.job_attachments.upload._get_deadline_formatted_os",
            return_value="linux",
        ), patch(
            f"{deadline.__package__}.job_attachments.upload._hash_data",
            side_effect=["c", "manifesthash"],
        ), patch(
            f"{deadline.__package__}.job_attachments.upload._hash_file", side_effect=["a"]
        ):
            caplog.set_level(WARNING)

            mock_on_preparing_to_submit = MagicMock(return_value=True)
            mock_on_uploading_assets = MagicMock(return_value=True)

            asset_manager = S3AssetManager(
                farm_id=farm_id,
                queue_id=queue_id,
                job_attachment_settings=self.job_attachment_s3_settings,
                asset_manifest_version=ManifestVersion.v2023_03_03,
            )

            # When
            (
                hash_summary_statstics,
                asset_root_manifests,
            ) = asset_manager.hash_assets_and_create_manifest(
                input_paths=[input_not_exist, scene_file],
                output_paths=[str(Path(asset_root).joinpath("outputs"))],
                referenced_paths=[],
                hash_cache_dir=cache_dir,
                on_preparing_to_submit=mock_on_preparing_to_submit,
            )

            (upload_summary_statstics, _) = asset_manager.upload_assets(
                manifests=asset_root_manifests,
                on_uploading_assets=mock_on_uploading_assets,
            )

            # Then
            assert "Skipping uploading input as it doesn't exist: " in caplog.text

            assert_progress_report_last_callback(
                num_input_files=1,
                expected_total_input_bytes=expected_total_input_bytes,
                on_preparing_to_submit=mock_on_preparing_to_submit,
                on_uploading_assets=mock_on_uploading_assets,
            )

            assert_progress_report_summary_statistics(
                actual_summary_statstics=hash_summary_statstics,
                processed_files=1,
                processed_bytes=expected_total_input_bytes,
                skipped_files=0,
                skipped_bytes=0,
            )

            assert_progress_report_summary_statistics(
                actual_summary_statstics=upload_summary_statstics,
                processed_files=1,
                processed_bytes=expected_total_input_bytes,
                skipped_files=0,
                skipped_bytes=0,
            )

    @mock_sts
    @pytest.mark.parametrize(
        "manifest_version,expected_manifest",
        [
            (
                ManifestVersion.v2023_03_03,
                '{"hashAlg":"xxh128","manifestVersion":"2023-03-03",'
                '"paths":[{"hash":"a","mtime":1234000000,"path":"sym_ip_test.txt","size":1}],"totalSize":1}',
            ),
        ],
    )
    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Windows requires Admin to create symlinks, skipping this test.",
    )
    def test_manage_assets_with_symlinks(
        self,
        tmpdir: py.path.local,
        farm_id,
        queue_id,
        assert_canonical_manifest,
        assert_expected_files_on_s3,
        manifest_version: ManifestVersion,
        expected_manifest: str,
    ):
        """
        Test that symlink paths that contain '..' expand the full path without
        resolving the symlink target, but also hash the symlink target and not
        the link.

        /tmp/source_folder/test.txt
            /symlink-folder
        """
        # Given
        test_file = tmpdir.mkdir("source_folder").join("test.txt")
        test_file.write("a")

        expected_total_input_bytes = test_file.size()
        os.utime(test_file, (1234, 1234))

        source_path = Path(tmpdir.join("source_folder"))
        symlink_input_path = Path(
            tmpdir.mkdir("symlink_folder").join("sub_folder").join("..").join("sym_ip_test.txt")
        )
        symlink_input_path.symlink_to(str(test_file))
        symlink_output_path = Path(
            tmpdir.join("symlink_folder").join("sub_folder").join("..").join("sym_op_test_dir")
        )
        symlink_output_path.symlink_to(source_path, target_is_directory=True)

        cache_dir = tmpdir.mkdir("cache")

        # WHEN
        with patch(
            f"{deadline.__package__}.job_attachments.upload._get_deadline_formatted_os",
            return_value="linux",
        ), patch(
            f"{deadline.__package__}.job_attachments.upload._hash_data",
            side_effect=["manifest", "manifesthash"],
        ), patch(
            f"{deadline.__package__}.job_attachments.upload._hash_file", side_effect=["a"]
        ), patch(
            f"{deadline.__package__}.job_attachments.models._generate_random_guid",
            return_value="0000",
        ):
            mock_on_preparing_to_submit = MagicMock(return_value=True)
            mock_on_uploading_assets = MagicMock(return_value=True)

            asset_manager = S3AssetManager(
                farm_id=farm_id,
                queue_id=queue_id,
                job_attachment_settings=self.job_attachment_s3_settings,
                asset_manifest_version=manifest_version,
            )

            (
                hash_summary_statstics,
                asset_root_manifests,
            ) = asset_manager.hash_assets_and_create_manifest(
                input_paths=[str(symlink_input_path)],
                output_paths=[str(symlink_output_path)],
                referenced_paths=[],
                hash_cache_dir=str(cache_dir),
                on_preparing_to_submit=mock_on_preparing_to_submit,
            )

            (upload_summary_statstics, attachments) = asset_manager.upload_assets(
                manifests=asset_root_manifests,
                on_uploading_assets=mock_on_uploading_assets,
            )

            # THEN
            expected_root = str(tmpdir.join("symlink_folder"))
            expected_attachments = Attachments(
                manifests=[
                    ManifestProperties(
                        rootPath=expected_root,
                        osType=OperatingSystemFamily.LINUX,
                        inputManifestPath=f"{farm_id}/{queue_id}/Inputs/0000/manifest_input.xxh128",
                        inputManifestHash="manifesthash",
                        outputRelativeDirectories=["sym_op_test_dir"],
                    )
                ],
            )

            assert attachments == expected_attachments

            assert_progress_report_last_callback(
                num_input_files=1,
                expected_total_input_bytes=expected_total_input_bytes,
                on_preparing_to_submit=mock_on_preparing_to_submit,
                on_uploading_assets=mock_on_uploading_assets,
            )

            assert_progress_report_summary_statistics(
                actual_summary_statstics=hash_summary_statstics,
                processed_files=1,
                processed_bytes=expected_total_input_bytes,
                skipped_files=0,
                skipped_bytes=0,
            )

            assert_progress_report_summary_statistics(
                actual_summary_statstics=upload_summary_statstics,
                processed_files=1,
                processed_bytes=expected_total_input_bytes,
                skipped_files=0,
                skipped_bytes=0,
            )

            s3 = boto3.Session(region_name="us-west-2").resource(
                "s3"
            )  # pylint: disable=invalid-name
            bucket = s3.Bucket(self.job_attachment_s3_settings.s3BucketName)

            assert_expected_files_on_s3(
                bucket,
                expected_files={
                    f"assetRoot/Manifests/{farm_id}/{queue_id}/Inputs/0000/manifest_input.xxh128",
                    f"{self.job_attachment_s3_settings.full_cas_prefix()}/a",
                },
            )

            assert_canonical_manifest(
                bucket,
                f"assetRoot/Manifests/{farm_id}/{queue_id}/Inputs/0000/manifest_input.xxh128",
                expected_manifest=expected_manifest,
            )

    @pytest.mark.parametrize(
        "mock_file_system_locations, expected_result",
        [
            (
                [],
                ({}, {}),
            ),
            (
                [
                    FileSystemLocation(
                        name="location-1",
                        type=FileSystemLocationType.LOCAL,
                        path="C:\\User\\Movie1",
                    ),
                ],
                ({"C:\\User\\Movie1": "location-1"}, {}),
            ),
            (
                [
                    FileSystemLocation(
                        name="location-1",
                        type=FileSystemLocationType.SHARED,
                        path="/mnt/shared/movie1",
                    ),
                ],
                ({}, {"/mnt/shared/movie1": "location-1"}),
            ),
            (
                [
                    FileSystemLocation(
                        name="location-1",
                        type=FileSystemLocationType.LOCAL,
                        path="C:\\User\\Movie1",
                    ),
                    FileSystemLocation(
                        name="location-2",
                        type=FileSystemLocationType.LOCAL,
                        path="/home/user1/movie1",
                    ),
                    FileSystemLocation(
                        name="location-3",
                        type=FileSystemLocationType.SHARED,
                        path="/mnt/shared/movie1",
                    ),
                    FileSystemLocation(
                        name="location-4",
                        type=FileSystemLocationType.SHARED,
                        path="/mnt/shared/etc",
                    ),
                ],
                (
                    {"C:\\User\\Movie1": "location-1", "/home/user1/movie1": "location-2"},
                    {"/mnt/shared/movie1": "location-3", "/mnt/shared/etc": "location-4"},
                ),
            ),
        ],
    )
    def test_get_file_system_locations_by_type(
        self,
        farm_id: str,
        queue_id: str,
        mock_file_system_locations: List[FileSystemLocation],
        expected_result: Tuple[Dict[str, str], Dict[str, str]],
    ):
        mock_storage_profile_for_queue = StorageProfile(
            storageProfileId="sp-0123456789",
            displayName="Storage profile 1",
            osFamily=OperatingSystemFamily.WINDOWS,
            fileSystemLocations=mock_file_system_locations,
        )

        with patch(
            f"{deadline.__package__}.job_attachments.upload.get_storage_profile_for_queue",
            side_effect=[mock_storage_profile_for_queue],
        ):
            asset_manager = S3AssetManager(
                farm_id=farm_id,
                queue_id=queue_id,
                job_attachment_settings=self.job_attachment_s3_settings,
            )

            result = asset_manager._get_file_system_locations_by_type(
                storage_profile_id="sp-0123456789"
            )

            assert result == expected_result

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="This test is for paths in POSIX path format and will be skipped on Windows.",
    )
    @patch.object(Path, "exists", return_value=True)
    @pytest.mark.parametrize(
        "input_paths, output_paths, referenced_paths, local_type_locations, shared_type_locations, expected_result",
        [
            (
                set(),  # input paths
                set(),  # output paths
                set(),  # referenced paths
                {},  # File System Location (LOCAL type)
                {},  # File System Location (SHARED type)
                [],
            ),
            (
                {"/home/username/docs/inputs/input1.txt"},  # input paths
                {"/home/username/docs/outputs"},  # output paths
                set(),  # referenced paths
                {"/home/username/movie1": "Movie 1 - Local"},  # File System Location (LOCAL type)
                {},  # File System Location (SHARED type)
                [
                    AssetRootGroup(
                        root_path="/home/username/docs",
                        inputs={
                            Path("/home/username/docs/inputs/input1.txt"),
                        },
                        outputs={
                            Path("/home/username/docs/outputs"),
                        },
                    ),
                ],
            ),
            (
                {"/home/username/movie1/inputs/input1.txt"},  # input paths
                {"/home/username/movie1/outputs"},  # output paths
                set(),  # referenced paths
                {"/home/username/movie1": "Movie 1 - Local"},  # File System Location (LOCAL type)
                {},  # File System Location (SHARED type)
                [
                    AssetRootGroup(
                        file_system_location_name="Movie 1 - Local",
                        root_path="/home/username/movie1",
                        inputs={
                            Path("/home/username/movie1/inputs/input1.txt"),
                        },
                        outputs={
                            Path("/home/username/movie1/outputs"),
                        },
                    ),
                ],
            ),
            (
                {"/mnt/shared/movie1/something.txt"},  # input paths
                {"/home/username/movie1/outputs"},  # output paths
                set(),  # referenced paths
                {"/home/username/movie1": "Movie 1 - Local"},  # File System Location (LOCAL type)
                {"/mnt/shared/movie1": "Movi 1 - Shared"},  # File System Location (SHARED type)
                [
                    AssetRootGroup(
                        file_system_location_name="Movie 1 - Local",
                        root_path="/home/username/movie1/outputs",
                        inputs=set(),
                        outputs={
                            Path("/home/username/movie1/outputs"),
                        },
                    ),
                ],
            ),
            (
                {
                    "/home/username/movie1/inputs/input1.txt",
                    "/home/username/movie1/inputs/input2.txt",
                    "/home/username/docs/doc1.txt",
                    "/home/username/docs/doc2.txt",
                    "/home/username/extra1.txt",
                    "/mnt/shared/movie1/something.txt",
                },  # input paths
                {
                    "/home/username/movie1/outputs1",
                    "/home/username/movie1/outputs2",
                },  # output paths
                {"/home/username/movie1/outputs1/referenced/path"},  # referenced paths
                {"/home/username/movie1": "Movie 1 - Local"},  # File System Location (LOCAL type)
                {"/mnt/shared/movie1": "Movi 1 - Shared"},  # File System Location (SHARED type)
                [
                    AssetRootGroup(
                        file_system_location_name="Movie 1 - Local",
                        root_path="/home/username/movie1",
                        inputs={
                            Path("/home/username/movie1/inputs/input1.txt"),
                            Path("/home/username/movie1/inputs/input2.txt"),
                        },
                        outputs={
                            Path("/home/username/movie1/outputs1"),
                            Path("/home/username/movie1/outputs2"),
                        },
                        references={Path("/home/username/movie1/outputs1/referenced/path")},
                    ),
                    AssetRootGroup(
                        root_path="/home/username",
                        inputs={
                            Path("/home/username/docs/doc1.txt"),
                            Path("/home/username/docs/doc2.txt"),
                            Path("/home/username/extra1.txt"),
                        },
                        outputs=set(),
                    ),
                ],
            ),
        ],
    )
    def test_get_asset_groups(
        self,
        farm_id: str,
        queue_id: str,
        input_paths: Set[str],
        output_paths: Set[str],
        referenced_paths: Set[str],
        local_type_locations: Dict[str, str],
        shared_type_locations: Dict[str, str],
        expected_result: List[AssetRootGroup],
    ):
        asset_manager = S3AssetManager(
            farm_id=farm_id,
            queue_id=queue_id,
            job_attachment_settings=self.job_attachment_s3_settings,
        )
        result = asset_manager._get_asset_groups(
            input_paths,
            output_paths,
            referenced_paths,
            local_type_locations,
            shared_type_locations,
        )

        sorted_result = sorted(result, key=lambda x: x.root_path)
        sorted_expected_result = sorted(expected_result, key=lambda x: x.root_path)

        assert sorted_result == sorted_expected_result


def assert_progress_report_last_callback(
    num_input_files: int,
    expected_total_input_bytes: int,
    on_preparing_to_submit: MagicMock,
    on_uploading_assets: MagicMock,
):
    """
    Assert that the argument of the last callback (when the progress is 100%) is as expected.
    """
    readable_total_input_bytes = _human_readable_file_size(expected_total_input_bytes)
    actual_args, _ = on_preparing_to_submit.call_args
    actual_last_hashing_progress_report = actual_args[0]
    assert actual_last_hashing_progress_report.status == ProgressStatus.PREPARING_IN_PROGRESS
    assert actual_last_hashing_progress_report.progress == 100.0
    assert (
        f"Processed {readable_total_input_bytes} / {readable_total_input_bytes}"
        f" of {num_input_files} file{'' if num_input_files == 1 else 's'}"
        " (Hashing speed: "
    ) in actual_last_hashing_progress_report.progressMessage

    actual_args, _ = on_uploading_assets.call_args
    actual_last_upload_progress_report = actual_args[0]
    assert actual_last_upload_progress_report.status == ProgressStatus.UPLOAD_IN_PROGRESS
    assert actual_last_upload_progress_report.progress == 100.0
    assert (
        f"Uploaded {readable_total_input_bytes} / {readable_total_input_bytes}"
        f" of {num_input_files} file{'' if num_input_files == 1 else 's'}"
        " (Transfer rate: "
    ) in actual_last_upload_progress_report.progressMessage


def assert_progress_report_summary_statistics(
    actual_summary_statstics: SummaryStatistics,
    processed_files: int,
    processed_bytes: int,
    skipped_files: int,
    skipped_bytes: int,
):
    """
    Assert that the reported summary statistics are as expected.
    """
    expected_summary_statstics = SummaryStatistics(
        total_time=actual_summary_statstics.total_time,
        total_files=processed_files + skipped_files,
        total_bytes=processed_bytes + skipped_bytes,
        processed_files=processed_files,
        processed_bytes=processed_bytes,
        skipped_files=skipped_files,
        skipped_bytes=skipped_bytes,
        transfer_rate=processed_bytes / actual_summary_statstics.total_time,
    )
    assert actual_summary_statstics == expected_summary_statstics
