# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from dataclasses import dataclass
from pathlib import Path
import boto3
from pytest import TempPathFactory
import os

from deadline.job_attachments._aws.deadline import get_queue
from deadline.job_attachments.asset_manifests.hash_algorithms import HashAlgorithm, hash_file
from deadline.job_attachments.asset_manifests.versions import ManifestVersion
from deadline.job_attachments.models import Attachments, JobAttachmentS3Settings
from deadline_test_fixtures.deadline import DeadlineClient


class DeadlineCliTest:
    """
    Hold information used across all Deadline CLI integration tests.
    """

    def __init__(self):
        """
        Sets up resources that the integ tests will need
        """

        self.farm_id: str = os.environ["FARM_ID"]
        self.queue_id: str = os.environ["QUEUE_ID"]


class JobAttachmentTest:
    """
    Hold information used across all job attachment integration tests.
    """

    ASSET_ROOT = Path(__file__).parent / "test_data"
    OUTPUT_PATH = ASSET_ROOT / "outputs"
    INPUT_PATH = ASSET_ROOT / "inputs"
    SCENE_MA_PATH = INPUT_PATH / "scene.ma"
    SCENE_MA_HASH = hash_file(str(SCENE_MA_PATH), HashAlgorithm.XXH128)
    BRICK_PNG_PATH = INPUT_PATH / "textures" / "brick.png"
    CLOTH_PNG_PATH = INPUT_PATH / "textures" / "cloth.png"
    INPUT_IN_OUTPUT_DIR_PATH = OUTPUT_PATH / "not_for_sync_outputs.txt"

    def __init__(
        self,
        tmp_path_factory: TempPathFactory,
        manifest_version: ManifestVersion,
    ):
        """
        Sets up resource that these integration tests will need.
        """

        self.farm_id: str = os.environ.get("FARM_ID", "")
        self.queue_id: str = os.environ.get("QUEUE_ID", "")

        self.bucket = boto3.resource("s3").Bucket(os.environ.get("JOB_ATTACHMENTS_BUCKET", ""))
        self.deadline_client = DeadlineClient(boto3.client("deadline"))

        self.hash_cache_dir = tmp_path_factory.mktemp("hash_cache")
        self.s3_cache_dir = tmp_path_factory.mktemp("s3_check_cache")
        self.session = boto3.Session()
        self.deadline_endpoint = os.getenv(
            "AWS_ENDPOINT_URL_DEADLINE",
            f"https://deadline.{self.session.region_name}.amazonaws.com",
        )

        self.job_attachment_settings: JobAttachmentS3Settings = get_queue(
            farm_id=self.farm_id,
            queue_id=self.queue_id,
            deadline_endpoint_url=self.deadline_endpoint,
        ).jobAttachmentSettings  # type: ignore[union-attr,assignment]

        self.manifest_version = manifest_version


@dataclass
class UploadInputFilesOneAssetInCasOutputs:
    attachments: Attachments
