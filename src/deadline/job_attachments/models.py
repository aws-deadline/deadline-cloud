# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Data classes for AWS objects.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import List, Optional, Set, Any

from deadline.job_attachments.asset_manifests import HashAlgorithm
from deadline.job_attachments.asset_manifests.base_manifest import (
    BaseAssetManifest,
    BaseManifestPath,
)

from deadline.job_attachments.exceptions import MissingS3RootPrefixError

from ._utils import (
    _generate_random_guid,
    _join_s3_paths,
)

S3_DATA_FOLDER_NAME = "Data"
S3_MANIFEST_FOLDER_NAME = "Manifests"
S3_INPUT_MANIFEST_FOLDER_NAME = "Inputs"


@dataclass
class AssetRootManifest:
    """Represents asset manifest and a list of output files grouped under the same root"""

    file_system_location_name: Optional[str] = None
    root_path: str = ""
    asset_manifest: Optional[BaseAssetManifest] = None
    outputs: List[Path] = field(default_factory=list)


@dataclass
class AssetRootGroup:
    """Represents lists of input files, output files and path references grouped under the same root"""

    file_system_location_name: Optional[str] = None
    root_path: str = ""
    inputs: Set[Path] = field(default_factory=set)
    outputs: Set[Path] = field(default_factory=set)
    references: Set[Path] = field(default_factory=set)


@dataclass
class AssetUploadGroup:
    """Represents all of the information needed to prepare to upload assets"""

    asset_groups: List[AssetRootGroup] = field(default_factory=list)
    total_input_files: int = 0
    total_input_bytes: int = 0


@dataclass
class ManifestPathGroup:
    """
    Represents paths combined from multiple manifests under the same root path, organized by hash algorithm.
    """

    total_bytes: int = 0
    files_by_hash_alg: dict[HashAlgorithm, List[BaseManifestPath]] = field(default_factory=dict)

    def add_manifest_to_group(self, manifest: BaseAssetManifest) -> None:
        if manifest.hashAlg not in self.files_by_hash_alg:
            self.files_by_hash_alg[manifest.hashAlg] = manifest.paths
        else:
            self.files_by_hash_alg[manifest.hashAlg].extend(manifest.paths)
        self.total_bytes += manifest.totalSize  # type: ignore[attr-defined]

    def combine_with_group(self, group: ManifestPathGroup) -> None:
        """Adds the content of the given ManifestPathGroup to this ManifestPathGroup"""
        for hash_alg, paths in group.files_by_hash_alg.items():
            if hash_alg not in self.files_by_hash_alg:
                self.files_by_hash_alg[hash_alg] = paths
            else:
                self.files_by_hash_alg[hash_alg].extend(paths)
            self.total_bytes += group.total_bytes

    def get_all_paths(self) -> list[str]:
        """
        Get all paths in this group, regardless of hashing algorithm.
        Note that this may include duplicates if the same path exists for multiple hashing algorithms.

        Returns a sorted list of paths represented as strings.
        """
        path_list: List[str] = []
        for paths in self.files_by_hash_alg.values():
            path_list.extend([path.path for path in paths])
        return sorted(path_list)


@dataclass
class OutputFile:
    """Files for output"""

    # File size in Bytes
    file_size: int
    file_hash: str
    rel_path: str
    full_path: str
    s3_key: str
    # If the file already exists in the CAS
    in_s3: bool
    # The base directory path against which file paths are containment-checked
    base_dir: Optional[str]


class StorageProfileOperatingSystemFamily(str, Enum):
    """Case-insensitive enum for the storage profile operating system family type."""

    WINDOWS = "windows"
    LINUX = "linux"
    MACOS = "macos"

    @classmethod
    def _missing_(cls, value):
        value = value.lower()
        for member in cls:
            if member == value:
                return member
        return None


class PathFormat(str, Enum):
    WINDOWS = "windows"
    POSIX = "posix"

    @classmethod
    def get_host_path_format(cls) -> PathFormat:
        """Get the current path format."""
        if sys.platform.startswith("win"):
            return cls.WINDOWS
        if sys.platform.startswith("darwin") or sys.platform.startswith("linux"):
            return cls.POSIX
        else:
            raise NotImplementedError(f"Operating system {sys.platform} is not supported.")

    @classmethod
    def get_host_path_format_string(cls) -> str:
        """Get a string of the current path format."""
        return cls.get_host_path_format().value


# Behavior to adopt when loading job assets
class JobAttachmentsFileSystem(str, Enum):
    # Load all assets at before execution of the job code
    COPIED = "COPIED"
    # Start job execution immediately and load assets as needed
    VIRTUAL = "VIRTUAL"


@dataclass
class ManifestProperties:
    """The assets for a Step under an asset root"""

    # The path that assets were relative to on submitting machine
    rootPath: str
    # Used for path mapping.
    rootPathFormat: PathFormat
    # If submitting machine has a 'Local' Storage Profile and files are relative
    # to any of its 'Asset Roots', the Asset Root Path will be used below.
    # Otherwise, the dynamic Job Attachments root path will be used and this will be empty.
    fileSystemLocationName: Optional[str] = field(default=None)  # type: ignore
    # An S3 (object) key that points to a file manifest location.
    # Optional as we may not need inputs if everything is embedded in the Job Template.
    inputManifestPath: Optional[str] = field(default=None)  # type: ignore
    # The hash of the manifest, for data provenance
    inputManifestHash: Optional[str] = field(default=None)  # type: ignore
    # The expected output directories to search for outputs. Relative to the rootPath.
    outputRelativeDirectories: Optional[List[str]] = field(default=None)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"rootPath": self.rootPath}
        if self.fileSystemLocationName:
            result["fileSystemLocationName"] = self.fileSystemLocationName
        result["rootPathFormat"] = self.rootPathFormat.value
        if self.inputManifestPath:
            result["inputManifestPath"] = self.inputManifestPath
        if self.inputManifestHash:
            result["inputManifestHash"] = self.inputManifestHash
        if self.outputRelativeDirectories:
            result["outputRelativeDirectories"] = self.outputRelativeDirectories
        return result


@dataclass
class Attachments:
    """An object that holds the job attachments for a Job"""

    # The list of required assets per asset root
    manifests: List[ManifestProperties] = field(default_factory=list)
    # Method to use when loading assets required for a job
    fileSystem: str = JobAttachmentsFileSystem.COPIED.value

    def to_dict(self) -> dict[str, Any]:
        return {
            "manifests": [manifest.to_dict() for manifest in self.manifests],
            "fileSystem": self.fileSystem,
        }


@dataclass
class JobAttachmentS3Settings:
    """S3-specific Job Attachment settings, configured at the Queue level."""

    # The S3 bucket all attachments are stored in. (required)
    s3BucketName: str  # pylint: disable=invalid-name
    # The S3 bucket prefix all files are stored relative to. (required)
    rootPrefix: str  # pylint: disable=invalid-name

    def full_cas_prefix(self) -> str:
        self._validate_root_prefix()
        return _join_s3_paths(self.rootPrefix, S3_DATA_FOLDER_NAME)

    def full_job_output_prefix(self, farm_id, queue_id, job_id) -> str:
        self._validate_root_prefix()
        return _join_s3_paths(self.rootPrefix, S3_MANIFEST_FOLDER_NAME, farm_id, queue_id, job_id)

    def full_step_output_prefix(self, farm_id, queue_id, job_id, step_id) -> str:
        self._validate_root_prefix()
        return _join_s3_paths(
            self.rootPrefix, S3_MANIFEST_FOLDER_NAME, farm_id, queue_id, job_id, step_id
        )

    def full_task_output_prefix(self, farm_id, queue_id, job_id, step_id, task_id) -> str:
        self._validate_root_prefix()
        return _join_s3_paths(
            self.rootPrefix, S3_MANIFEST_FOLDER_NAME, farm_id, queue_id, job_id, step_id, task_id
        )

    def full_output_prefix(
        self, farm_id, queue_id, job_id, step_id, task_id, session_action_id
    ) -> str:
        self._validate_root_prefix()
        return _join_s3_paths(
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
        guid = _generate_random_guid()
        return _join_s3_paths(
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
        return _join_s3_paths(self.rootPrefix, S3_MANIFEST_FOLDER_NAME, path)

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
    defaultBudgetAction: str
    jobAttachmentSettings: Optional[JobAttachmentS3Settings] = None  # pylint: disable=invalid-name


@dataclass
class Job:
    """A non-exhaustive DataClass to store job objects"""

    jobId: str
    attachments: Optional[Attachments] = None  # pylint: disable=invalid-name


@dataclass
class StorageProfile:
    """DataClass to store Storage Profile For Queue objects"""

    storageProfileId: str
    displayName: str
    osFamily: StorageProfileOperatingSystemFamily
    fileSystemLocations: List[FileSystemLocation] = field(default_factory=list)  # type: ignore


@dataclass
class FileSystemLocation:
    """DataClass to store File System Location objects"""

    name: str
    path: str
    type: FileSystemLocationType


class FileSystemLocationType(str, Enum):
    SHARED = "SHARED"
    LOCAL = "LOCAL"


class FileConflictResolution(Enum):
    NOT_SELECTED = 0
    SKIP = 1
    OVERWRITE = 2
    CREATE_COPY = 3
