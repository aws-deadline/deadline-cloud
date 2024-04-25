# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Integration tests for Job Attachments."""
import logging
import os
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock

from deadline.job_attachments.models import JobAttachmentS3Settings

import boto3
import pytest
from deadline_test_fixtures.job_attachment_manager import JobAttachmentManager
from pytest import LogCaptureFixture, TempPathFactory

from deadline.job_attachments import asset_sync, download, upload
from deadline.job_attachments.asset_manifests import (
    ManifestVersion,
    HashAlgorithm,
    hash_data,
    hash_file,
)
from deadline.job_attachments._aws.deadline import get_queue
from deadline.job_attachments.exceptions import AssetSyncError, JobAttachmentsS3ClientError
from deadline.job_attachments.models import (
    Attachments,
    ManifestProperties,
    PathFormat,
)
from deadline.job_attachments.progress_tracker import SummaryStatistics
from deadline.job_attachments._utils import (
    _get_unique_dest_dir_name,
)
from .conftest import is_windows_non_admin


def notifier_callback(progress: float, message: str) -> None:
    pass


class JobAttachmentTest:
    """
    Hold information used across all job attachment integration tests.
    """

    ROOT_PREFIX = "root"
    ASSET_ROOT = Path(__file__).parent / "test_data"
    OUTPUT_PATH = ASSET_ROOT / "outputs"
    INPUT_PATH = ASSET_ROOT / "inputs"
    SCENE_MA_PATH = INPUT_PATH / "scene.ma"
    SCENE_MA_HASH = hash_file(str(SCENE_MA_PATH), HashAlgorithm.XXH128)
    BRICK_PNG_PATH = INPUT_PATH / "textures" / "brick.png"
    CLOTH_PNG_PATH = INPUT_PATH / "textures" / "cloth.png"
    INPUT_IN_OUTPUT_DIR_PATH = OUTPUT_PATH / "not_for_sync_outputs.txt"
    FIRST_RENDER_OUTPUT_PATH = Path("outputs/render0000.exr")
    SECOND_RENDER_OUTPUT_PATH = Path("outputs/render0001.exr")
    MOV_FILE_OUTPUT_PATH = Path("outputs/end.mov")

    def __init__(
        self,
        deploy_job_attachment_resources: JobAttachmentManager,
        tmp_path_factory: TempPathFactory,
        manifest_version: ManifestVersion,
    ):
        """
        Sets up resource that these integration tests will need.
        """
        self.job_attachment_resources = deploy_job_attachment_resources

        if self.job_attachment_resources.farm is None:
            raise TypeError("The Farm was not properly created when initializing resources.")
        if (
            self.job_attachment_resources.queue is None
            or self.job_attachment_resources.queue_with_no_settings is None
        ):
            raise TypeError("The Queues were not properly created when initializing resources.")

        self.farm_id = self.job_attachment_resources.farm.id
        self.queue_id = self.job_attachment_resources.queue.id
        self.queue_with_no_settings = self.job_attachment_resources.queue_with_no_settings.id

        self.bucket = boto3.resource("s3").Bucket(self.job_attachment_resources.bucket_name)
        self.deadline_client = self.job_attachment_resources.deadline_client

        self.hash_cache_dir = tmp_path_factory.mktemp("hash_cache")
        self.s3_cache_dir = tmp_path_factory.mktemp("s3_check_cache")
        self.session = boto3.Session()
        self.deadline_endpoint = os.getenv(
            "AWS_ENDPOINT_URL_DEADLINE",
            f"https://deadline.{self.session.region_name}.amazonaws.com",
        )

        self.manifest_version = manifest_version


@pytest.fixture(scope="session", params=[ManifestVersion.v2023_03_03])
def job_attachment_test(
    deploy_job_attachment_resources: JobAttachmentManager,
    tmp_path_factory: TempPathFactory,
    request: pytest.FixtureRequest,
):
    """
    Fixture to get the session's JobAttachmentTest object.
    """

    return JobAttachmentTest(
        deploy_job_attachment_resources, tmp_path_factory, manifest_version=request.param
    )


@pytest.fixture(scope="session")
def upload_input_files_assets_not_in_cas(job_attachment_test: JobAttachmentTest):
    """
    When no assets are in the CAS, make sure all files are uploaded.
    """
    # IF
    job_attachment_test.deadline_client.update_queue(
        queueId=job_attachment_test.queue_id,
        farmId=job_attachment_test.farm_id,
        jobAttachmentSettings={
            "s3BucketName": job_attachment_test.bucket.name,
            "rootPrefix": job_attachment_test.ROOT_PREFIX,
        },
    )

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


@dataclass
class UploadInputFilesOneAssetInCasOutputs:
    attachments: Attachments


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


@pytest.mark.integ
def test_upload_input_files_all_assets_in_cas(
    job_attachment_test: JobAttachmentTest,
    upload_input_files_one_asset_in_cas: UploadInputFilesOneAssetInCasOutputs,
) -> None:
    """
    Test that when all assets are already in the CAS, that no files are uploaded.
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

    # This file has already been uploaded
    asset_upload_time = {
        obj.key: obj.last_modified
        for obj in job_attachment_test.bucket.objects.filter(
            Prefix=f"{job_attachment_settings.full_cas_prefix()}/"
        )
    }

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

    assert attachments.manifests[0].inputManifestPath is not None

    # Confirm nothing was uploaded
    for obj in job_attachment_test.bucket.objects.filter(
        Prefix=f"{job_attachment_settings.full_cas_prefix()}/"
    ):
        if (
            f"{attachments.manifests[0].inputManifestPath}"
            == f"s3://{job_attachment_test.bucket.name}/{obj.key}"
        ):
            # Skip checking the manifest file
            continue

        assert obj.last_modified == asset_upload_time[obj.key]


@dataclass
class SyncInputsOutputs:
    session_dir: Path
    dest_dir: Path
    asset_syncer: asset_sync.AssetSync
    attachments: Attachments
    job_id: str


@pytest.fixture(scope="session")
def sync_inputs(
    job_attachment_test: JobAttachmentTest,
    upload_input_files_one_asset_in_cas: UploadInputFilesOneAssetInCasOutputs,
    tmp_path_factory: TempPathFactory,
    default_job_template: str,
) -> SyncInputsOutputs:
    """
    Test that all of the input files get synced locally.
    """
    # IF
    job_attachment_settings = get_queue(
        farm_id=job_attachment_test.farm_id,
        queue_id=job_attachment_test.queue_id,
        deadline_endpoint_url=job_attachment_test.deadline_endpoint,
    ).jobAttachmentSettings

    job_response = job_attachment_test.deadline_client.create_job(
        farmId=job_attachment_test.farm_id,
        queueId=job_attachment_test.queue_id,
        attachments=upload_input_files_one_asset_in_cas.attachments.to_dict(),  # type: ignore
        targetTaskRunStatus="SUSPENDED",
        template=default_job_template,
        templateType="JSON",
        priority=50,
    )

    syncer = asset_sync.AssetSync(job_attachment_test.farm_id)
    session_dir = tmp_path_factory.mktemp("session_dir")

    def on_downloading_files(*args, **kwargs):
        return True

    # WHEN
    syncer.sync_inputs(
        job_attachment_settings,
        upload_input_files_one_asset_in_cas.attachments,
        job_attachment_test.queue_id,
        job_response["jobId"],
        session_dir,
        on_downloading_files=on_downloading_files,
    )

    dest_dir = _get_unique_dest_dir_name(str(job_attachment_test.ASSET_ROOT))

    # THEN
    assert Path(session_dir / dest_dir / job_attachment_test.SCENE_MA_PATH).exists()
    assert Path(session_dir / dest_dir / job_attachment_test.BRICK_PNG_PATH).exists()
    assert Path(session_dir / dest_dir / job_attachment_test.CLOTH_PNG_PATH).exists()
    assert Path(session_dir / dest_dir / job_attachment_test.INPUT_IN_OUTPUT_DIR_PATH).exists()

    return SyncInputsOutputs(
        session_dir=session_dir,
        dest_dir=Path(dest_dir),
        asset_syncer=syncer,
        attachments=upload_input_files_one_asset_in_cas.attachments,
        job_id=job_response["jobId"],
    )


@dataclass
class SyncInputsNoJobAttachmentS3SettingsOutput:
    job_id: str
    asset_syncer: asset_sync.AssetSync
    session_dir: Path


@pytest.fixture()
def sync_inputs_no_job_attachment_s3_settings(
    job_attachment_test: JobAttachmentTest,
    upload_input_files_one_asset_in_cas: UploadInputFilesOneAssetInCasOutputs,
    tmp_path_factory: TempPathFactory,
    default_job_template_one_task_one_step: str,
    caplog: LogCaptureFixture,
) -> SyncInputsNoJobAttachmentS3SettingsOutput:
    """
    Test that when there are no job attachment settings on a queue, the input sync is skipped.
    """
    # IF
    caplog.set_level(logging.INFO)

    job_response = job_attachment_test.deadline_client.create_job(
        farmId=job_attachment_test.farm_id,
        queueId=job_attachment_test.queue_with_no_settings,
        attachments=upload_input_files_one_asset_in_cas.attachments.to_dict(),  # type: ignore
        targetTaskRunStatus="SUSPENDED",
        template=default_job_template_one_task_one_step,
        templateType="JSON",
        priority=50,
    )

    syncer = asset_sync.AssetSync(
        farm_id=job_attachment_test.farm_id,
        boto3_session=job_attachment_test.session,
        deadline_endpoint_url=job_attachment_test.deadline_endpoint,
    )
    session_dir = tmp_path_factory.mktemp("session_dir")

    def on_downloading_files(*args, **kwargs):
        return True

    # WHEN
    assert syncer.sync_inputs(
        syncer.get_s3_settings(
            farm_id=job_attachment_test.farm_id, queue_id=job_attachment_test.queue_with_no_settings
        ),
        syncer.get_attachments(
            job_attachment_test.farm_id,
            job_attachment_test.queue_with_no_settings,
            job_response["jobId"],
        ),
        job_attachment_test.queue_with_no_settings,
        job_response["jobId"],
        session_dir,
        on_downloading_files=on_downloading_files,
    ) == (SummaryStatistics(), [])

    assert (
        "No Job Attachment settings configured for Queue "
        f"{job_attachment_test.queue_with_no_settings}, no inputs to sync." in caplog.text
    )

    return SyncInputsNoJobAttachmentS3SettingsOutput(
        job_id=job_response["jobId"], asset_syncer=syncer, session_dir=session_dir
    )


@dataclass
class SyncInputsNoJobAttachmentSettingsInJobOutput:
    job_id: str
    asset_syncer: asset_sync.AssetSync
    session_dir: Path


@pytest.fixture()
def sync_inputs_no_job_attachment_settings_in_job(
    job_attachment_test: JobAttachmentTest,
    upload_input_files_one_asset_in_cas: UploadInputFilesOneAssetInCasOutputs,
    tmp_path_factory: TempPathFactory,
    default_job_template_one_task_one_step: str,
    caplog: LogCaptureFixture,
) -> SyncInputsNoJobAttachmentSettingsInJobOutput:
    """
    Test that when there are no job attachment settings on a job, the input sync is skipped.
    """
    # IF
    caplog.set_level(logging.INFO)

    job_response = job_attachment_test.deadline_client.create_job(
        farmId=job_attachment_test.farm_id,
        queueId=job_attachment_test.queue_id,
        targetTaskRunStatus="SUSPENDED",
        template=default_job_template_one_task_one_step,
        templateType="JSON",
        priority=50,
    )

    syncer = asset_sync.AssetSync(
        farm_id=job_attachment_test.farm_id,
        boto3_session=job_attachment_test.session,
        deadline_endpoint_url=job_attachment_test.deadline_endpoint,
    )
    session_dir = tmp_path_factory.mktemp("session_dir")

    def on_downloading_files(*args, **kwargs):
        return True

    # WHEN
    assert syncer.sync_inputs(
        syncer.get_s3_settings(
            farm_id=job_attachment_test.farm_id, queue_id=job_attachment_test.queue_id
        ),
        syncer.get_attachments(
            job_attachment_test.farm_id, job_attachment_test.queue_id, job_response["jobId"]
        ),
        job_attachment_test.queue_id,
        job_response["jobId"],
        session_dir,
        on_downloading_files=on_downloading_files,
    ) == (SummaryStatistics(), [])

    assert (
        f"No attachments configured for Job {job_response['jobId']}, no inputs to sync."
        in caplog.text
    )

    return SyncInputsNoJobAttachmentSettingsInJobOutput(
        job_id=job_response["jobId"], asset_syncer=syncer, session_dir=session_dir
    )


@pytest.mark.integ
def test_sync_outputs_no_job_attachment_settings_in_job(
    job_attachment_test: JobAttachmentTest,
    sync_inputs_no_job_attachment_settings_in_job: SyncInputsNoJobAttachmentSettingsInJobOutput,
    caplog: LogCaptureFixture,
) -> None:
    """
    Test that syncing outputs is skipped when the queue has no job attachment settings.
    """
    # IF
    caplog.set_level(logging.INFO)

    waiter = job_attachment_test.deadline_client.get_waiter("job_create_complete")
    waiter.wait(
        jobId=sync_inputs_no_job_attachment_settings_in_job.job_id,
        queueId=job_attachment_test.queue_id,
        farmId=job_attachment_test.farm_id,
    )

    step_id = job_attachment_test.deadline_client.list_steps(
        farmId=job_attachment_test.farm_id,
        queueId=job_attachment_test.queue_id,
        jobId=sync_inputs_no_job_attachment_settings_in_job.job_id,
    )["steps"][0]["stepId"]

    task_id = job_attachment_test.deadline_client.list_tasks(
        farmId=job_attachment_test.farm_id,
        queueId=job_attachment_test.queue_id,
        jobId=sync_inputs_no_job_attachment_settings_in_job.job_id,
        stepId=step_id,
    )["tasks"][0]["taskId"]

    # WHEN
    sync_inputs_no_job_attachment_settings_in_job.asset_syncer.sync_outputs(
        s3_settings=sync_inputs_no_job_attachment_settings_in_job.asset_syncer.get_s3_settings(
            job_attachment_test.farm_id, job_attachment_test.queue_id
        ),
        attachments=sync_inputs_no_job_attachment_settings_in_job.asset_syncer.get_attachments(
            job_attachment_test.farm_id,
            job_attachment_test.queue_id,
            sync_inputs_no_job_attachment_settings_in_job.job_id,
        ),
        queue_id=job_attachment_test.queue_id,
        job_id=sync_inputs_no_job_attachment_settings_in_job.job_id,
        step_id=step_id,
        task_id=task_id,
        session_action_id="session_action_id",
        start_time=time.time(),
        session_dir=sync_inputs_no_job_attachment_settings_in_job.session_dir,
    )

    # THEN
    assert (
        "No attachments configured for Job "
        f"{sync_inputs_no_job_attachment_settings_in_job.job_id}, no outputs to sync."
        in caplog.text
    )


@pytest.mark.integ
def test_sync_outputs_no_job_attachment_s3_settings(
    job_attachment_test: JobAttachmentTest,
    sync_inputs_no_job_attachment_s3_settings: SyncInputsNoJobAttachmentS3SettingsOutput,
    caplog: LogCaptureFixture,
) -> None:
    """
    Test that syncing outputs is skipped when the job has no job attachment settings.
    """
    # IF
    caplog.set_level(logging.INFO)

    waiter = job_attachment_test.deadline_client.get_waiter("job_create_complete")
    waiter.wait(
        jobId=sync_inputs_no_job_attachment_s3_settings.job_id,
        queueId=job_attachment_test.queue_with_no_settings,
        farmId=job_attachment_test.farm_id,
    )

    step_id = job_attachment_test.deadline_client.list_steps(
        farmId=job_attachment_test.farm_id,
        queueId=job_attachment_test.queue_with_no_settings,
        jobId=sync_inputs_no_job_attachment_s3_settings.job_id,
    )["steps"][0]["stepId"]

    task_id = job_attachment_test.deadline_client.list_tasks(
        farmId=job_attachment_test.farm_id,
        queueId=job_attachment_test.queue_with_no_settings,
        jobId=sync_inputs_no_job_attachment_s3_settings.job_id,
        stepId=step_id,
    )["tasks"][0]["taskId"]

    # WHEN
    sync_inputs_no_job_attachment_s3_settings.asset_syncer.sync_outputs(
        s3_settings=sync_inputs_no_job_attachment_s3_settings.asset_syncer.get_s3_settings(
            job_attachment_test.farm_id, job_attachment_test.queue_with_no_settings
        ),
        attachments=sync_inputs_no_job_attachment_s3_settings.asset_syncer.get_attachments(
            job_attachment_test.farm_id,
            job_attachment_test.queue_with_no_settings,
            sync_inputs_no_job_attachment_s3_settings.job_id,
        ),
        queue_id=job_attachment_test.queue_with_no_settings,
        job_id=sync_inputs_no_job_attachment_s3_settings.job_id,
        step_id=step_id,
        task_id=task_id,
        session_action_id="session_action_id",
        start_time=time.time(),
        session_dir=sync_inputs_no_job_attachment_s3_settings.session_dir,
    )

    # THEN
    assert (
        "No Job Attachment settings configured for Queue "
        f"{job_attachment_test.queue_with_no_settings}, no outputs to sync." in caplog.text
    )


@dataclass
class SyncOutputsOutput:
    step0_task0_id: str
    step0_task1_id: str
    step1_task0_id: str
    step0_id: str
    step1_id: str
    job_id: str
    attachments: Attachments
    step0_task0_output_file: Path
    step0_task1_output_file: Path
    step1_task0_output_file: Path


@pytest.fixture(scope="session")
def sync_outputs(
    job_attachment_test: JobAttachmentTest,
    sync_inputs: SyncInputsOutputs,
) -> SyncOutputsOutput:
    """
    Test that all outputs from the job get synced to the JobAttachment S3 Bucket.
    """
    # IF
    job_attachment_settings = get_queue(
        farm_id=job_attachment_test.farm_id,
        queue_id=job_attachment_test.queue_id,
        deadline_endpoint_url=job_attachment_test.deadline_endpoint,
    ).jobAttachmentSettings

    if job_attachment_settings is None:
        raise Exception("Job attachment settings must be set for this test.")

    waiter = job_attachment_test.deadline_client.get_waiter("job_create_complete")
    waiter.wait(
        jobId=sync_inputs.job_id,
        queueId=job_attachment_test.queue_id,
        farmId=job_attachment_test.farm_id,
    )

    list_steps_response = job_attachment_test.deadline_client.list_steps(
        farmId=job_attachment_test.farm_id,
        queueId=job_attachment_test.queue_id,
        jobId=sync_inputs.job_id,
    )

    step_ids = {step["name"]: step["stepId"] for step in list_steps_response["steps"]}

    step0_id = step_ids["custom-step"]
    step1_id = step_ids["custom-step-2"]

    list_tasks_response = job_attachment_test.deadline_client.list_tasks(
        farmId=job_attachment_test.farm_id,
        queueId=job_attachment_test.queue_id,
        jobId=sync_inputs.job_id,
        stepId=step0_id,
    )

    task_ids = {
        task["parameters"]["frame"]["int"]: task["taskId"] for task in list_tasks_response["tasks"]
    }

    step0_task0_id = task_ids["0"]
    step0_task1_id = task_ids["1"]

    step1_task0_id = list_tasks_response = job_attachment_test.deadline_client.list_tasks(
        farmId=job_attachment_test.farm_id,
        queueId=job_attachment_test.queue_id,
        jobId=sync_inputs.job_id,
        stepId=step1_id,
    )["tasks"][0]["taskId"]

    file_to_be_synced_step0_task0_base = job_attachment_test.FIRST_RENDER_OUTPUT_PATH
    file_to_be_synced_step0_task1_base = job_attachment_test.SECOND_RENDER_OUTPUT_PATH
    file_to_be_synced_step1_task0_base = job_attachment_test.MOV_FILE_OUTPUT_PATH

    file_to_be_synced_step0_task0 = (
        sync_inputs.session_dir / sync_inputs.dest_dir / file_to_be_synced_step0_task0_base
    )
    file_to_be_synced_step0_task1 = (
        sync_inputs.session_dir / sync_inputs.dest_dir / file_to_be_synced_step0_task1_base
    )
    file_to_be_synced_step1_task0 = (
        sync_inputs.session_dir / sync_inputs.dest_dir / file_to_be_synced_step1_task0_base
    )

    render_start_time = time.time()

    # WHEN
    mock_on_uploading_files = MagicMock(return_value=True)

    # First step and task
    # Create files after the render start time in the output dir, these should be synced
    with open(file_to_be_synced_step0_task0, "w") as f:
        f.write("this is the first render")

    summary_stats = sync_inputs.asset_syncer.sync_outputs(
        s3_settings=job_attachment_settings,
        attachments=sync_inputs.attachments,
        queue_id=job_attachment_test.queue_id,
        job_id=sync_inputs.job_id,
        step_id=step0_id,
        task_id=step0_task0_id,
        session_action_id="session_action_id",
        start_time=render_start_time,
        session_dir=sync_inputs.session_dir,
        on_uploading_files=mock_on_uploading_files,
    )
    # There should be one synced output for this task, Step 0 - Task 0
    assert summary_stats.total_files == 1

    render_start_time = time.time()

    # First step and second task
    with open(file_to_be_synced_step0_task1, "w") as f:
        f.write("this is a second render")

    summary_stats = sync_inputs.asset_syncer.sync_outputs(
        s3_settings=job_attachment_settings,
        attachments=sync_inputs.attachments,
        queue_id=job_attachment_test.queue_id,
        job_id=sync_inputs.job_id,
        step_id=step0_id,
        task_id=step0_task1_id,
        session_action_id="session_action_id",
        start_time=render_start_time,
        session_dir=sync_inputs.session_dir,
        on_uploading_files=mock_on_uploading_files,
    )
    # There should be one synced output for this task, Step 0 - Task 1
    assert summary_stats.total_files == 1

    render_start_time = time.time()

    # Second step and first task
    with open(file_to_be_synced_step1_task0, "w") as f:
        f.write("this is a comp")

    summary_stats = sync_inputs.asset_syncer.sync_outputs(
        s3_settings=job_attachment_settings,
        attachments=sync_inputs.attachments,
        queue_id=job_attachment_test.queue_id,
        job_id=sync_inputs.job_id,
        step_id=step1_id,
        task_id=step1_task0_id,
        session_action_id="session_action_id",
        start_time=render_start_time,
        session_dir=sync_inputs.session_dir,
        on_uploading_files=mock_on_uploading_files,
    )
    # There should be one synced output for this task, Step 1 - Task 0
    assert summary_stats.total_files == 1

    # THEN
    object_summary_iterator = job_attachment_test.bucket.objects.filter(
        Prefix=f"{job_attachment_settings.full_cas_prefix()}/",
    )

    object_key_set = set(obj.key for obj in object_summary_iterator)

    assert (
        f"{job_attachment_settings.full_cas_prefix()}/{hash_file(str(file_to_be_synced_step0_task0), HashAlgorithm.XXH128)}.xxh128"
        in object_key_set
    )

    return SyncOutputsOutput(
        step0_id=step0_id,
        step1_id=step1_id,
        step0_task0_id=step0_task0_id,
        step0_task1_id=step0_task1_id,
        step1_task0_id=step1_task0_id,
        job_id=sync_inputs.job_id,
        attachments=sync_inputs.attachments,
        step0_task0_output_file=file_to_be_synced_step0_task0_base,
        step0_task1_output_file=file_to_be_synced_step0_task1_base,
        step1_task0_output_file=file_to_be_synced_step1_task0_base,
    )


@pytest.mark.integ
@pytest.mark.skipif(
    is_windows_non_admin(),
    reason="Windows requires Admin to create symlinks, skipping this test.",
)
def test_sync_outputs_with_symlink(
    job_attachment_test: JobAttachmentTest,
    sync_inputs: SyncInputsOutputs,
    tmp_path,
) -> None:
    """
    Test that a symlink pointing to a file outside the session directory is not synced as output.
    """
    job_attachment_settings = get_queue(
        farm_id=job_attachment_test.farm_id,
        queue_id=job_attachment_test.queue_id,
        deadline_endpoint_url=job_attachment_test.deadline_endpoint,
    ).jobAttachmentSettings

    if job_attachment_settings is None:
        raise Exception("Job attachment settings must be set for this test.")

    waiter = job_attachment_test.deadline_client.get_waiter("job_create_complete")
    waiter.wait(
        jobId=sync_inputs.job_id,
        queueId=job_attachment_test.queue_id,
        farmId=job_attachment_test.farm_id,
    )

    # Get the Step ID
    list_steps_response = job_attachment_test.deadline_client.list_steps(
        farmId=job_attachment_test.farm_id,
        queueId=job_attachment_test.queue_id,
        jobId=sync_inputs.job_id,
    )
    step_ids = {step["name"]: step["stepId"] for step in list_steps_response["steps"]}
    step0_id = step_ids["custom-step"]

    # Get the Task ID
    list_tasks_response = job_attachment_test.deadline_client.list_tasks(
        farmId=job_attachment_test.farm_id,
        queueId=job_attachment_test.queue_id,
        jobId=sync_inputs.job_id,
        stepId=step0_id,
    )
    task_ids = {
        task["parameters"]["frame"]["int"]: task["taskId"] for task in list_tasks_response["tasks"]
    }
    step0_task0_id = task_ids["0"]

    # Create a symlink, in the output directory, pointing to a file located outside the session directory.
    symlink_output = Path("outputs/symlink")
    symlink_path = sync_inputs.session_dir / sync_inputs.dest_dir / symlink_output
    tmp_dir = tmp_path / "tmp_dir"
    tmp_dir.mkdir()
    symlink_target_path = tmp_dir / "symlink_target"
    symlink_target_path.write_text(
        "this is a symlink target, located outside the session directory"
    )
    symlink_path.symlink_to(symlink_target_path)
    assert symlink_path.is_symlink()

    mock_on_uploading_files = MagicMock(return_value=True)

    summary_stats = sync_inputs.asset_syncer.sync_outputs(
        s3_settings=job_attachment_settings,
        attachments=sync_inputs.attachments,
        queue_id=job_attachment_test.queue_id,
        job_id=sync_inputs.job_id,
        step_id=step0_id,
        task_id=step0_task0_id,
        session_action_id="session_action_id",
        start_time=time.time(),
        session_dir=sync_inputs.session_dir,
        on_uploading_files=mock_on_uploading_files,
    )
    # The symlink should not be synced as output.
    assert summary_stats.total_files == 0


@pytest.mark.integ
def test_sync_inputs_with_step_dependencies(
    job_attachment_test: JobAttachmentTest,
    tmp_path_factory: TempPathFactory,
    sync_outputs: SyncOutputsOutput,
):
    """
    Test that sync_inputs() syncs the inputs specified in job settings, and the outputs from other steps
    specified in step dependencies.
    """
    # IF
    job_attachment_settings = get_queue(
        farm_id=job_attachment_test.farm_id,
        queue_id=job_attachment_test.queue_id,
        deadline_endpoint_url=job_attachment_test.deadline_endpoint,
    ).jobAttachmentSettings

    list_steps_response = job_attachment_test.deadline_client.list_steps(
        farmId=job_attachment_test.farm_id,
        queueId=job_attachment_test.queue_id,
        jobId=sync_outputs.job_id,
    )
    step_ids = {step["name"]: step["stepId"] for step in list_steps_response["steps"]}
    step0_id = step_ids["custom-step"]

    session_dir = tmp_path_factory.mktemp("session_dir")

    # WHEN
    syncer = asset_sync.AssetSync(job_attachment_test.farm_id)

    def on_downloading_files(*args, **kwargs):
        return True

    syncer.sync_inputs(
        job_attachment_settings,
        sync_outputs.attachments,
        job_attachment_test.queue_id,
        sync_outputs.job_id,
        session_dir,
        step_dependencies=[step0_id],
        on_downloading_files=on_downloading_files,
    )

    dest_dir = _get_unique_dest_dir_name(str(job_attachment_test.ASSET_ROOT))

    # THEN
    # Check if the inputs specified in job settings were downloaded
    assert Path(session_dir / dest_dir / job_attachment_test.SCENE_MA_PATH).exists()
    assert Path(session_dir / dest_dir / job_attachment_test.BRICK_PNG_PATH).exists()
    assert Path(session_dir / dest_dir / job_attachment_test.CLOTH_PNG_PATH).exists()
    assert Path(session_dir / dest_dir / job_attachment_test.INPUT_IN_OUTPUT_DIR_PATH).exists()
    # Check if the outputs from step0_id ("custom-step") were downloaded
    assert Path(session_dir / dest_dir / job_attachment_test.FIRST_RENDER_OUTPUT_PATH).exists()
    assert Path(session_dir / dest_dir / job_attachment_test.SECOND_RENDER_OUTPUT_PATH).exists()
    # Check if the outputs from the other step ("custom-step-1") were not downloaded
    assert not Path(session_dir / dest_dir / job_attachment_test.MOV_FILE_OUTPUT_PATH).exists()


@pytest.mark.integ
def test_download_outputs_with_job_id_step_id_task_id_and_download_directory(
    job_attachment_test: JobAttachmentTest, tmp_path: Path, sync_outputs: SyncOutputsOutput
):
    """
    Test that outputs for a task are downloaded to the correct location locally
    """
    # GIVEN
    job_attachment_settings = get_queue(
        farm_id=job_attachment_test.farm_id,
        queue_id=job_attachment_test.queue_id,
        deadline_endpoint_url=job_attachment_test.deadline_endpoint,
    ).jobAttachmentSettings

    if job_attachment_settings is None:
        raise TypeError("Job attachment settings must be set for this test.")

    # WHEN
    try:
        job_output_downloader = download.OutputDownloader(
            s3_settings=job_attachment_settings,
            farm_id=job_attachment_test.farm_id,
            queue_id=job_attachment_test.queue_id,
            job_id=sync_outputs.job_id,
            step_id=sync_outputs.step0_id,
            task_id=sync_outputs.step0_task0_id,
        )
        job_output_downloader.download_job_output()

        # THEN
        assert Path(job_attachment_test.ASSET_ROOT / sync_outputs.step0_task0_output_file).exists()
    finally:
        _cleanup_outputs_dir(job_attachment_test)


@pytest.mark.integ
def test_download_outputs_with_job_id_step_id_and_download_directory(
    job_attachment_test: JobAttachmentTest, tmp_path: Path, sync_outputs: SyncOutputsOutput
):
    """
    Test that outputs for a step are downloaded to the correct location locally
    """
    # GIVEN
    job_attachment_settings = get_queue(
        farm_id=job_attachment_test.farm_id,
        queue_id=job_attachment_test.queue_id,
        deadline_endpoint_url=job_attachment_test.deadline_endpoint,
    ).jobAttachmentSettings

    if job_attachment_settings is None:
        raise TypeError("Job attachment settings must be set for this test.")

    # WHEN
    try:
        job_output_downloader = download.OutputDownloader(
            s3_settings=job_attachment_settings,
            farm_id=job_attachment_test.farm_id,
            queue_id=job_attachment_test.queue_id,
            job_id=sync_outputs.job_id,
            step_id=sync_outputs.step0_id,
            task_id=None,
        )
        job_output_downloader.download_job_output()

        # THEN
        assert Path(job_attachment_test.ASSET_ROOT / sync_outputs.step0_task0_output_file).exists()
        assert Path(job_attachment_test.ASSET_ROOT / sync_outputs.step0_task1_output_file).exists()
    finally:
        _cleanup_outputs_dir(job_attachment_test)


@pytest.mark.integ
def test_download_outputs_with_job_id_and_download_directory(
    job_attachment_test: JobAttachmentTest, tmp_path: Path, sync_outputs: SyncOutputsOutput
):
    """
    Test that outputs for a job are downloaded to the correct location locally
    """
    # GIVEN
    job_attachment_settings = get_queue(
        farm_id=job_attachment_test.farm_id,
        queue_id=job_attachment_test.queue_id,
        deadline_endpoint_url=job_attachment_test.deadline_endpoint,
    ).jobAttachmentSettings

    if job_attachment_settings is None:
        raise TypeError("Job attachment settings must be set for this test.")

    # WHEN
    try:
        job_output_downloader = download.OutputDownloader(
            s3_settings=job_attachment_settings,
            farm_id=job_attachment_test.farm_id,
            queue_id=job_attachment_test.queue_id,
            job_id=sync_outputs.job_id,
            step_id=None,
            task_id=None,
        )
        job_output_downloader.download_job_output()

        # THEN
        assert Path(job_attachment_test.ASSET_ROOT / sync_outputs.step0_task0_output_file).exists()
        assert Path(job_attachment_test.ASSET_ROOT / sync_outputs.step0_task1_output_file).exists()
        assert Path(job_attachment_test.ASSET_ROOT / sync_outputs.step1_task0_output_file).exists()
    finally:
        _cleanup_outputs_dir(job_attachment_test)


def _cleanup_outputs_dir(job_attachment_test: JobAttachmentTest) -> None:
    shutil.rmtree(job_attachment_test.OUTPUT_PATH)
    # Revive the INPUT_IN_OUTPUT_DIR_PATH file.
    job_attachment_test.OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
    with open(job_attachment_test.INPUT_IN_OUTPUT_DIR_PATH, "w") as f:
        f.write(
            "Although it is in the output directory, it is actually an input file. It should be"
            " downloaded (to the worker's session working directory) during sync_inputs, and"
            " should not be captured as an output file when sync_outputs."
        )


@dataclass
class UploadInputFilesNoInputPathsOutput:
    attachments: Attachments


@pytest.fixture(scope="session")
def upload_input_files_no_input_paths(
    job_attachment_test: JobAttachmentTest,
) -> UploadInputFilesNoInputPathsOutput:
    """
    Test that the created job settings object doesn't have the requiredAssets field when there are no input files.
    """
    # IF
    job_attachment_test.deadline_client.update_queue(
        queueId=job_attachment_test.queue_id,
        farmId=job_attachment_test.farm_id,
        jobAttachmentSettings={
            "s3BucketName": job_attachment_test.bucket.name,
            "rootPrefix": job_attachment_test.ROOT_PREFIX,
        },
    )

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
        input_paths=[],
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
    mock_host_path_format_name = PathFormat.get_host_path_format_string()
    assert attachments.manifests == [
        ManifestProperties(
            rootPath=str(job_attachment_test.OUTPUT_PATH),
            rootPathFormat=PathFormat(mock_host_path_format_name),
            outputRelativeDirectories=["."],
        )
    ]

    return UploadInputFilesNoInputPathsOutput(attachments=attachments)


@pytest.mark.integ
def test_upload_input_files_no_download_paths(job_attachment_test: JobAttachmentTest) -> None:
    """
    Test that if there are no output directories, when upload_assets is called,
    then the resulting attachments object has no output directories in it.
    """
    # IF
    job_attachment_test.deadline_client.update_queue(
        queueId=job_attachment_test.queue_id,
        farmId=job_attachment_test.farm_id,
        jobAttachmentSettings={
            "s3BucketName": job_attachment_test.bucket.name,
            "rootPrefix": job_attachment_test.ROOT_PREFIX,
        },
    )

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
        output_paths=[],
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
    if manifests[0].asset_manifest is None:
        raise TypeError("Asset manifest must be set for this test.")

    mock_host_path_format_name = PathFormat.get_host_path_format_string()
    asset_root_hash = hash_data(str(job_attachment_test.INPUT_PATH).encode(), HashAlgorithm.XXH128)
    manifest_hash = hash_data(
        bytes(manifests[0].asset_manifest.encode(), "utf-8"), HashAlgorithm.XXH128
    )

    assert len(attachments.manifests) == 1
    assert attachments.manifests[0].fileSystemLocationName is None
    assert attachments.manifests[0].rootPath == str(job_attachment_test.INPUT_PATH)
    assert attachments.manifests[0].rootPathFormat == PathFormat(mock_host_path_format_name)
    assert attachments.manifests[0].outputRelativeDirectories == []
    assert attachments.manifests[0].inputManifestPath is not None
    assert attachments.manifests[0].inputManifestPath.startswith(
        f"{job_attachment_test.farm_id}/{job_attachment_test.queue_id}/Inputs/"
    )
    assert attachments.manifests[0].inputManifestPath.endswith(f"/{asset_root_hash}_input")
    assert attachments.manifests[0].inputManifestHash == manifest_hash


@pytest.mark.integ
def test_sync_inputs_no_inputs(
    job_attachment_test: JobAttachmentTest,
    upload_input_files_no_input_paths: UploadInputFilesNoInputPathsOutput,
    tmp_path: Path,
    default_job_template_one_task_one_step: str,
) -> None:
    """
    Test that all of the input files get synced locally.
    """
    # IF
    job_attachment_settings = get_queue(
        farm_id=job_attachment_test.farm_id,
        queue_id=job_attachment_test.queue_id,
        deadline_endpoint_url=job_attachment_test.deadline_endpoint,
    ).jobAttachmentSettings

    job_response = job_attachment_test.deadline_client.create_job(
        farmId=job_attachment_test.farm_id,
        queueId=job_attachment_test.queue_id,
        attachments=upload_input_files_no_input_paths.attachments.to_dict(),  # type: ignore
        targetTaskRunStatus="SUSPENDED",
        template=default_job_template_one_task_one_step,
        templateType="JSON",
        priority=50,
    )

    syncer = asset_sync.AssetSync(job_attachment_test.farm_id)
    session_dir = tmp_path / "session_dir"
    session_dir.mkdir()

    def on_downloading_files(*args, **kwargs):
        return True

    # WHEN
    syncer.sync_inputs(
        job_attachment_settings,
        upload_input_files_no_input_paths.attachments,
        job_attachment_test.queue_id,
        job_response["jobId"],
        session_dir,
        on_downloading_files=on_downloading_files,
    )

    # THEN
    assert not any(Path(session_dir).iterdir())


@pytest.mark.integ
def test_upload_bucket_wrong_account(external_bucket: str, job_attachment_test: JobAttachmentTest):
    """
    Test that if trying to upload to a bucket that isn't in the farm's AWS account, the correct error is thrown.
    """
    # IF
    job_attachment_settings = JobAttachmentS3Settings(
        s3BucketName=external_bucket,
        rootPrefix=job_attachment_test.ROOT_PREFIX,
    )

    asset_manager = upload.S3AssetManager(
        farm_id=job_attachment_test.farm_id,
        queue_id=job_attachment_test.queue_id,
        job_attachment_settings=job_attachment_settings,
        asset_manifest_version=job_attachment_test.manifest_version,
    )

    mock_on_preparing_to_submit = MagicMock(return_value=True)
    mock_on_uploading_files = MagicMock(return_value=True)

    # WHEN
    with pytest.raises(
        JobAttachmentsS3ClientError, match=".*when calling the PutObject operation: Access Denied"
    ):
        # The attempt to upload the asset manifest should be blocked.
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


@pytest.mark.integ
def test_sync_inputs_bucket_wrong_account(
    external_bucket: str,
    job_attachment_test: JobAttachmentTest,
    upload_input_files_one_asset_in_cas: UploadInputFilesOneAssetInCasOutputs,
    default_job_template: str,
    tmp_path_factory: TempPathFactory,
):
    """
    Test that if trying to sync inputs to a bucket that isn't in the farm's AWS account, the correct error is thrown.
    """
    # IF
    job_attachment_settings = JobAttachmentS3Settings(
        s3BucketName=external_bucket,
        rootPrefix=job_attachment_test.ROOT_PREFIX,
    )

    job_response = job_attachment_test.deadline_client.create_job(
        farmId=job_attachment_test.farm_id,
        queueId=job_attachment_test.queue_id,
        attachments=upload_input_files_one_asset_in_cas.attachments.to_dict(),  # type: ignore
        targetTaskRunStatus="SUSPENDED",
        template=default_job_template,
        templateType="JSON",
        priority=50,
    )

    syncer = asset_sync.AssetSync(job_attachment_test.farm_id)
    session_dir = tmp_path_factory.mktemp(r"An error occurred \(403\)")

    def on_downloading_files(*args, **kwargs):
        return True

    # WHEN
    with pytest.raises(
        JobAttachmentsS3ClientError,
        match=f"Error downloading binary file in bucket '{external_bucket}'",
    ):
        syncer.sync_inputs(
            job_attachment_settings,
            upload_input_files_one_asset_in_cas.attachments,
            job_attachment_test.queue_id,
            job_response["jobId"],
            session_dir,
            on_downloading_files=on_downloading_files,
        )


@pytest.mark.integ
def test_sync_outputs_bucket_wrong_account(
    job_attachment_test: JobAttachmentTest,
    sync_inputs: SyncInputsOutputs,
    external_bucket: str,
) -> None:
    """
    Test that if trying to sync outputs to a bucket that isn't in the farm's AWS account, the correct error is thrown.
    """
    # IF
    job_attachment_settings = JobAttachmentS3Settings(
        s3BucketName=external_bucket,
        rootPrefix=job_attachment_test.ROOT_PREFIX,
    )

    waiter = job_attachment_test.deadline_client.get_waiter("job_create_complete")
    waiter.wait(
        jobId=sync_inputs.job_id,
        queueId=job_attachment_test.queue_id,
        farmId=job_attachment_test.farm_id,
    )

    list_steps_response = job_attachment_test.deadline_client.list_steps(
        farmId=job_attachment_test.farm_id,
        queueId=job_attachment_test.queue_id,
        jobId=sync_inputs.job_id,
    )

    step_ids = {step["name"]: step["stepId"] for step in list_steps_response["steps"]}

    step0_id = step_ids["custom-step"]

    list_tasks_response = job_attachment_test.deadline_client.list_tasks(
        farmId=job_attachment_test.farm_id,
        queueId=job_attachment_test.queue_id,
        jobId=sync_inputs.job_id,
        stepId=step0_id,
    )

    task_ids = {
        task["parameters"]["frame"]["int"]: task["taskId"] for task in list_tasks_response["tasks"]
    }

    step0_task0_id = task_ids["0"]

    Path(sync_inputs.session_dir / sync_inputs.dest_dir / "outputs").mkdir(exist_ok=True)

    file_to_be_synced_step0_task0_base = job_attachment_test.FIRST_RENDER_OUTPUT_PATH

    file_to_be_synced_step0_task0 = (
        sync_inputs.session_dir / sync_inputs.dest_dir / file_to_be_synced_step0_task0_base
    )

    render_start_time = time.time()

    # WHEN

    # First step and task
    # Create files after the render start time in the output dir, these should be synced
    with open(file_to_be_synced_step0_task0, "w") as f:
        f.write("this is the first render")
    mock_on_uploading_files = MagicMock(return_value=True)

    # WHEN
    with pytest.raises(
        AssetSyncError, match=f"Error checking if object exists in bucket '{external_bucket}'"
    ):
        sync_inputs.asset_syncer.sync_outputs(
            s3_settings=job_attachment_settings,
            attachments=sync_inputs.attachments,
            queue_id=job_attachment_test.queue_id,
            job_id=sync_inputs.job_id,
            step_id=step0_id,
            task_id=step0_task0_id,
            session_action_id="session_action_id",
            start_time=render_start_time,
            session_dir=sync_inputs.session_dir,
            on_uploading_files=mock_on_uploading_files,
        )


@pytest.mark.integ
def test_download_outputs_bucket_wrong_account(
    job_attachment_test: JobAttachmentTest,
    tmp_path: Path,
    sync_outputs: SyncOutputsOutput,
    external_bucket: str,
):
    """
    Test that if trying to download outputs to a bucket
    that isn't in the farm's AWS account, the correct error is thrown.
    """
    # GIVEN
    job_attachment_settings = JobAttachmentS3Settings(
        s3BucketName=external_bucket,
        rootPrefix=job_attachment_test.ROOT_PREFIX,
    )

    # WHEN
    with pytest.raises(
        JobAttachmentsS3ClientError,
        match=f"Error listing bucket contents in bucket '{external_bucket}'",
    ):
        job_output_downloader = download.OutputDownloader(
            s3_settings=job_attachment_settings,
            farm_id=job_attachment_test.farm_id,
            queue_id=job_attachment_test.queue_id,
            job_id=sync_outputs.job_id,
            step_id=sync_outputs.step0_id,
            task_id=sync_outputs.step0_task0_id,
        )
        job_output_downloader.download_job_output()
