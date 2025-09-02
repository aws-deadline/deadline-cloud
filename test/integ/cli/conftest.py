# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from unittest.mock import MagicMock
import tempfile
from pathlib import Path
import os

import pytest
from deadline.job_attachments import upload
from deadline.client.config import config_file
from deadline.job_attachments._aws.deadline import get_queue
from deadline.job_attachments.asset_manifests.hash_algorithms import HashAlgorithm, hash_file
from deadline.job_attachments.asset_manifests.versions import ManifestVersion
from .test_utils import JobAttachmentTest, UploadInputFilesOneAssetInCasOutputs, DeadlineCliTest


@pytest.fixture(scope="function", autouse=True)
def fresh_deadline_config():
    """
    Fixture to start with a blank AWS Deadline Cloud config file.

    This fixture is configured for autouse, so that every test is isolated from
    the user's config file.
    """

    # Clear the session cache as part of switching out the config.
    from deadline.client.api._session import invalidate_boto3_session_cache

    invalidate_boto3_session_cache()

    try:
        original_config_file_value = os.environ.get("DEADLINE_CONFIG_FILE_PATH")

        # Create an empty temp file to set as the AWS Deadline Cloud config
        temp_dir = tempfile.TemporaryDirectory()
        temp_dir_path = Path(temp_dir.name)
        temp_file_path = temp_dir_path / "config"
        with open(temp_file_path, "w+t", encoding="utf8") as temp_file:
            temp_file.write("")

        # Use the environment variable to override the path for both the
        # current process and subprocesses
        os.environ["DEADLINE_CONFIG_FILE_PATH"] = str(temp_file_path)

        # Write a telemetry id to force it getting saved to the config file. If we don't, then
        # an ID will get generated and force a save of the config file in the middle of a test.
        # Writing the config file may be undesirable in the middle of a test.
        config_file.set_setting("telemetry.identifier", "00000000-0000-0000-0000-000000000000")

        yield str(temp_file_path)
    finally:
        if original_config_file_value is None:
            del os.environ["DEADLINE_CONFIG_FILE_PATH"]
        else:
            os.environ["DEADLINE_CONFIG_FILE_PATH"] = original_config_file_value
        temp_dir.cleanup()


@pytest.fixture(scope="session")
def deadline_cli_test() -> DeadlineCliTest:
    """
    Fixture to get the sessions DeadlineCliTest object.
    """

    return DeadlineCliTest()


@pytest.fixture(scope="session")
def job_attachment_test(
    tmp_path_factory: pytest.TempPathFactory,
    request: pytest.FixtureRequest,
):
    """
    Fixture to get the session's JobAttachmentTest object.
    """

    return JobAttachmentTest(tmp_path_factory, manifest_version=ManifestVersion.v2023_03_03)


@pytest.fixture(scope="session")
def upload_input_files_assets_not_in_cas(job_attachment_test: JobAttachmentTest):
    """
    When no assets are in the CAS, make sure all files are uploaded.
    """
    # IF

    job_attachment_settings = get_queue(
        farm_id=job_attachment_test.farm_id,
        queue_id=job_attachment_test.queue_id,
        deadline_endpoint_url=job_attachment_test.deadline_endpoint,
    ).jobAttachmentSettings

    if job_attachment_settings is None:
        raise TypeError("Job attachment settings must be set for this test.")

    asset_manager = upload.S3AssetManager(
        farm_id=job_attachment_test.farm_id,
        queue_id=job_attachment_test.queue_id,
        job_attachment_settings=job_attachment_settings,
        asset_manifest_version=job_attachment_test.manifest_version,
    )

    mock_on_preparing_to_submit = MagicMock(return_value=True)
    mock_on_uploading_files = MagicMock(return_value=True)

    # WHEN
    upload_group = asset_manager.prepare_paths_for_upload(
        input_paths=[str(job_attachment_test.SCENE_MA_PATH)],
        output_paths=[str(job_attachment_test.OUTPUT_PATH)],
        referenced_paths=[],
    )
    (_, manifests) = asset_manager.hash_assets_and_create_manifest(
        asset_groups=upload_group.asset_groups,
        total_input_files=upload_group.total_input_files,
        total_input_bytes=upload_group.total_input_bytes,
        hash_cache_dir=str(job_attachment_test.hash_cache_dir),
        on_preparing_to_submit=mock_on_preparing_to_submit,
    )
    asset_manager.upload_assets(
        manifests,
        on_uploading_assets=mock_on_uploading_files,
        s3_check_cache_dir=str(job_attachment_test.s3_cache_dir),
    )

    # THEN
    scene_ma_s3_path = (
        f"{job_attachment_settings.full_cas_prefix()}/{job_attachment_test.SCENE_MA_HASH}.xxh128"
    )

    object_summary_iterator = job_attachment_test.bucket.objects.filter(
        Prefix=scene_ma_s3_path,
    )

    assert list(object_summary_iterator)[0].key == scene_ma_s3_path


@pytest.fixture(scope="session")
def upload_input_files_one_asset_in_cas(
    job_attachment_test: JobAttachmentTest, upload_input_files_assets_not_in_cas: None
) -> UploadInputFilesOneAssetInCasOutputs:
    """
    Test that when one asset is already in the CAS, that every file except for the one in the CAS is uploaded.
    """
    # IF
    job_attachment_settings = get_queue(
        farm_id=job_attachment_test.farm_id,
        queue_id=job_attachment_test.queue_id,
        deadline_endpoint_url=job_attachment_test.deadline_endpoint,
    ).jobAttachmentSettings

    if job_attachment_settings is None:
        raise Exception("Job attachment settings must be set for this test.")

    asset_manager = upload.S3AssetManager(
        farm_id=job_attachment_test.farm_id,
        queue_id=job_attachment_test.queue_id,
        job_attachment_settings=job_attachment_settings,
        asset_manifest_version=job_attachment_test.manifest_version,
    )

    input_paths = [
        str(job_attachment_test.SCENE_MA_PATH),
        str(job_attachment_test.BRICK_PNG_PATH),
        str(job_attachment_test.CLOTH_PNG_PATH),
        str(job_attachment_test.INPUT_IN_OUTPUT_DIR_PATH),
    ]

    scene_ma_s3_path = (
        f"{job_attachment_settings.full_cas_prefix()}/{job_attachment_test.SCENE_MA_HASH}.xxh128"
    )

    # This file has already been uploaded
    scene_ma_upload_time = job_attachment_test.bucket.Object(scene_ma_s3_path).last_modified

    mock_on_preparing_to_submit = MagicMock(return_value=True)
    mock_on_uploading_files = MagicMock(return_value=True)

    # WHEN
    upload_group = asset_manager.prepare_paths_for_upload(
        input_paths=input_paths,
        output_paths=[str(job_attachment_test.OUTPUT_PATH)],
        referenced_paths=[],
    )
    (_, manifests) = asset_manager.hash_assets_and_create_manifest(
        asset_groups=upload_group.asset_groups,
        total_input_files=upload_group.total_input_files,
        total_input_bytes=upload_group.total_input_bytes,
        hash_cache_dir=str(job_attachment_test.hash_cache_dir),
        on_preparing_to_submit=mock_on_preparing_to_submit,
    )

    (_, attachments) = asset_manager.upload_assets(
        manifests,
        on_uploading_assets=mock_on_uploading_files,
        s3_check_cache_dir=str(job_attachment_test.s3_cache_dir),
    )

    # THEN
    brick_png_hash = hash_file(str(job_attachment_test.BRICK_PNG_PATH), HashAlgorithm.XXH128)
    cloth_png_hash = hash_file(str(job_attachment_test.CLOTH_PNG_PATH), HashAlgorithm.XXH128)
    input_in_output_dir_hash = hash_file(
        str(job_attachment_test.INPUT_IN_OUTPUT_DIR_PATH), HashAlgorithm.XXH128
    )

    brick_png_s3_path = f"{job_attachment_settings.full_cas_prefix()}/{brick_png_hash}.xxh128"
    cloth_png_s3_path = f"{job_attachment_settings.full_cas_prefix()}/{cloth_png_hash}.xxh128"
    input_in_output_dir_s3_path = (
        f"{job_attachment_settings.full_cas_prefix()}/{input_in_output_dir_hash}.xxh128"
    )

    object_summary_iterator = job_attachment_test.bucket.objects.filter(
        Prefix=f"{job_attachment_settings.full_cas_prefix()}/",
    )

    s3_objects = {obj.key: obj for obj in object_summary_iterator}

    assert {brick_png_s3_path, cloth_png_s3_path, input_in_output_dir_s3_path} <= set(
        map(lambda x: x.key, object_summary_iterator)
    )

    assert brick_png_s3_path in s3_objects
    assert cloth_png_s3_path in s3_objects
    assert input_in_output_dir_s3_path in s3_objects
    # Make sure that the file hasn't been modified/reuploaded
    assert s3_objects[scene_ma_s3_path].last_modified == scene_ma_upload_time

    return UploadInputFilesOneAssetInCasOutputs(attachments)
