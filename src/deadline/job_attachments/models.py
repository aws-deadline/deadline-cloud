# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Data classes for AWS objects.
"""
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Set

from dataclasses_json import DataClassJsonMixin, config, dataclass_json

from deadline.job_attachments.asset_manifests.base_manifest import AssetManifest

from deadline.job_attachments.errors import MissingS3RootPrefixError

from .utils import AssetLoadingMethod, OperatingSystemFamily, generate_random_guid, join_s3_paths

S3_DATA_FOLDER_NAME = "Data"
S3_MANIFEST_FOLDER_NAME = "Manifests"
S3_INPUT_MANIFEST_FOLDER_NAME = "Inputs"


@dataclass
class AssetRootManifest:
    """Represents asset manifest and a list of output files grouped under the same root"""

    file_system_location_name: Optional[str] = None
    root_path: str = ""
    asset_manifest: Optional[AssetManifest] = None
    outputs: List[Path] = field(default_factory=list)


@dataclass
class AssetRootGroup:
    """Represents lists of input and output files grouped under the same root"""

    root_path: str = ""
    inputs: Set[Path] = field(default_factory=set)
    outputs: Set[Path] = field(default_factory=set)


@dataclass
class HashCacheEntry(DataClassJsonMixin):
    """Represents an entry in the local hash-cache database"""

    file_path: str
    file_hash: str
    last_modified_time: str


@dataclass
class OutputFile:
    """Files for output"""

    file_size: int  # File size in Bytes
    file_hash: str
    rel_path: str
    full_path: str
    s3_key: str
    in_s3: bool  # If the file already exists in the CAS


@dataclass_json
@dataclass
class ManifestProperties:
    """The assets for a Step under an asset root"""

    # The path assests were relative to on submitting machine
    rootPath: str
    # If submitting machine has a 'Local' Storage Profile and files are relative
    # to any of its 'Asset Roots', the Asset Root Path will be used below.
    # Otherwise, the dynamic Job Attachments root path will be used and this will be empty.
    fileSystemLocationName: Optional[str] = field(default=None, metadata=config(exclude=lambda x: x is None))  # type: ignore
    # Used for path mapping.
    osType: OperatingSystemFamily = OperatingSystemFamily.WINDOWS
    # An S3 (object) key that points to a file manifest location.
    # Optional as we may not need inputs if everything is embedded in the Job Template.
    inputManifestPath: Optional[str] = field(default=None, metadata=config(exclude=lambda x: x is None))  # type: ignore
    # The hash of the manifest, for data provenance
    inputManifestHash: Optional[str] = field(default=None, metadata=config(exclude=lambda x: x is None))  # type: ignore
    # The expected output directories to search for outputs. Relative to the rootPath.
    outputRelativeDirectories: Optional[List[str]] = field(
        default=None, metadata=config(exclude=lambda x: x is None)  # type: ignore
    )


@dataclass_json
@dataclass
class Attachments:
    """An object that holds the job attachments for a Job"""

    # The list of required assests per asset root
    manifests: List[ManifestProperties] = field(default_factory=list)
    # Method to use when loading assets required for a job
    assetLoadingMethod: str = AssetLoadingMethod.PRELOAD.value


@dataclass
class JobAttachmentS3Settings:
    """S3-specific Job Attachment settings, configured at the Queue level."""

    # The S3 bucket all attachments are stored in. (required)
    s3BucketName: str  # pylint: disable=invalid-name
    # The S3 bucket prefix all files are stored relative to. (required)
    rootPrefix: str  # pylint: disable=invalid-name

    def full_cas_prefix(self) -> str:
        self._validate_root_prefix()
        return join_s3_paths(self.rootPrefix, S3_DATA_FOLDER_NAME)

    def full_job_output_prefix(self, farm_id, queue_id, job_id) -> str:
        self._validate_root_prefix()
        return join_s3_paths(self.rootPrefix, S3_MANIFEST_FOLDER_NAME, farm_id, queue_id, job_id)

    def full_step_output_prefix(self, farm_id, queue_id, job_id, step_id) -> str:
        self._validate_root_prefix()
        return join_s3_paths(
            self.rootPrefix, S3_MANIFEST_FOLDER_NAME, farm_id, queue_id, job_id, step_id
        )

    def full_task_output_prefix(self, farm_id, queue_id, job_id, step_id, task_id) -> str:
        self._validate_root_prefix()
        return join_s3_paths(
            self.rootPrefix, S3_MANIFEST_FOLDER_NAME, farm_id, queue_id, job_id, step_id, task_id
        )

    def full_output_prefix(
        self, farm_id, queue_id, job_id, step_id, task_id, session_action_id
    ) -> str:
        self._validate_root_prefix()
        return join_s3_paths(
            self.rootPrefix,
            S3_MANIFEST_FOLDER_NAME,
            farm_id,
            queue_id,
            job_id,
            step_id,
            task_id,
            session_action_id,
        )

    def partial_manifest_prefix(self, farm_id, queue_id) -> str:
        guid = generate_random_guid()
        return join_s3_paths(
            farm_id,
            queue_id,
            S3_INPUT_MANIFEST_FOLDER_NAME,
            guid,
        )

    def add_root_and_manifest_folder_prefix(self, path: str) -> str:
        """
        Adds “{self.rootPrefix}/{S3_MANIFEST_FOLDER_NAME}/” to the beginning
        of the path and returns it.
        """
        self._validate_root_prefix()
        return join_s3_paths(self.rootPrefix, S3_MANIFEST_FOLDER_NAME, path)

    def _validate_root_prefix(self) -> None:
        if not self.rootPrefix:
            raise MissingS3RootPrefixError("Missing S3 root prefix")


@dataclass
class Fleet:
    """DataClass to store fleet objects"""

    fleetId: str  # pylint: disable=invalid-name
    priority: int


@dataclass
class Queue:
    """DataClass to store queue objects"""

    queueId: str  # pylint: disable=invalid-name
    displayName: str
    farmId: str  # pylint: disable=invalid-name
    status: str
    jobAttachmentSettings: Optional[JobAttachmentS3Settings] = None  # pylint: disable=invalid-name


@dataclass_json
@dataclass
class Job:
    """A non-exaustive DataClass to store job objects"""

    jobId: str
    attachments: Optional[Attachments] = None  # pylint: disable=invalid-name
