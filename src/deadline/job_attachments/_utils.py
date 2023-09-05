# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import datetime
import io
import os
import sys
from enum import Enum
from hashlib import shake_256
from pathlib import Path
from typing import Optional, Tuple, Union
import uuid

import xxhash

__all__ = [
    "FileSystemLocationType",
    "OperatingSystemFamily",
    "_hash_file",
    "_hash_data",
    "_join_s3_paths",
    "_generate_random_guid",
    "_float_to_iso_datetime_string",
    "_human_readable_file_size",
    "_get_deadline_formatted_os",
    "_get_unique_dest_dir_name",
    "_get_bucket_and_object_key",
    "_get_default_hash_cache_db_file_dir",
    "_is_relative_to",
    "AssetLoadingMethod",
    "FileConflictResolution",
]

CONFIG_ROOT = ".deadline"
COMPONENT_NAME = "job_attachments"


class FileSystemLocationType(str, Enum):
    SHARED = "SHARED"
    LOCAL = "LOCAL"

    @classmethod
    def get_type(cls, type_string):
        """
        Returns the OperatingSystemFamily enum value from the (case-insensitive) OS string.
        """
        try:
            return cls(type_string.upper())
        except ValueError:
            raise ValueError(f"Invalid type string: {type_string}")


class OperatingSystemFamily(str, Enum):
    WINDOWS = "windows"
    LINUX = "linux"
    MACOS = "macos"

    @classmethod
    def get_os_family(cls, os_string):
        """
        Returns the OperatingSystemFamily enum value from the (case-insensitive) OS string.
        """
        try:
            return cls(os_string.lower())
        except ValueError:
            raise ValueError(f"Invalid OS string: {os_string}")

    def to_path_mapping_os(self) -> str:
        if self == OperatingSystemFamily.WINDOWS:
            return "WINDOWS"
        else:
            return "POSIX"


def _hash_file(file_path: str) -> str:
    with open(file_path, "rb") as file:
        hasher = xxhash.xxh3_128()
        while True:
            chunk = file.read(io.DEFAULT_BUFFER_SIZE)
            if not chunk:
                break
            hasher.update(chunk)
        return hasher.hexdigest()


def _hash_data(data: bytes) -> str:
    hasher = xxhash.xxh3_128()
    hasher.update(data)
    return hasher.hexdigest()


def _join_s3_paths(root: str, *args: str):
    return "/".join([root, *args])


def _generate_random_guid():
    return str(uuid.uuid4()).replace("-", "")


def _float_to_iso_datetime_string(time: float):
    seconds = int(time)
    microseconds = int((time - seconds) * 1000000)

    dt = datetime.datetime.utcfromtimestamp(seconds) + datetime.timedelta(microseconds=microseconds)
    iso_string = dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    return iso_string


def _human_readable_file_size(size_in_bytes: int) -> str:
    """
    Convert a size in bytes to something human readable. For example 1000 bytes will be converted
    to 1 KB. Sizes close enough to a postfix threshold will be rounded up to the next threshold.
    For example 999999 bytes would be output as 1.0 MB and NOT 999.99 MB (or as a consequence of
    Python's round function 1000.0 KB).

    This function is inherently lossy, so it should be used for display purposes only.
    """
    converted_size = float(size_in_bytes)
    rounded: float
    postfixes = ["B", "KB", "MB", "GB", "TB", "PB"]

    for postfix in postfixes:
        rounded = round(converted_size, ndigits=2)

        if rounded < 1000:
            return f"{rounded} {postfix}"

        converted_size /= 1000

    # If we go higher than the provided postfix,
    # then return as a large amount of the highest postfix we've specified.
    return f"{rounded} {postfixes[-1]}"


def _get_deadline_formatted_os() -> str:
    """
    Get a string specifying what the OS is, following the format the Deadline API expects.
    """
    if sys.platform.startswith("linux"):
        return OperatingSystemFamily.LINUX.value

    if sys.platform.startswith("darwin"):
        return OperatingSystemFamily.MACOS.value

    if sys.platform.startswith("win"):
        return OperatingSystemFamily.WINDOWS.value

    return "Unknown"


def _get_unique_dest_dir_name(source_root: str) -> str:
    # Note: this is a quick naive way to attempt to prevent colliding
    # relative paths across manifests without adding too much
    # length to the filepaths. length = 2n where n is the number
    # passed to hexdigest.
    return f"assetroot-{shake_256(source_root.encode()).hexdigest(10)}"


def _get_bucket_and_object_key(s3_path: str) -> Tuple[str, str]:
    """Returns the bucket name and object key from the S3 URI"""
    bucket, key = s3_path.replace("s3://", "").split("/", maxsplit=1)
    return bucket, key


def _get_default_hash_cache_db_file_dir() -> Optional[str]:
    """
    Gets the expected directory for the hash cache database file based on OS environment variables.
    If a directory cannot be found, defaults to the working directory.
    """
    default_path = os.environ.get("HOME")
    if default_path:
        default_path = os.path.join(default_path, CONFIG_ROOT, COMPONENT_NAME)
    return default_path


def _is_relative_to(path1: Union[Path, str], path2: Union[Path, str]) -> bool:
    """
    Determines if path1 is relative to path2. This function is to support
    Python versions (3.7 and 3.8) that do not have the built-in `Path.is_relative_to()` method.
    """
    try:
        Path(path1).relative_to(Path(path2))
        return True
    except ValueError:
        return False


# Behavior to adopt when loading job assets
class AssetLoadingMethod(str, Enum):
    # Load all assets at before execution of the job code
    PRELOAD = "PRELOAD"
    # Start job execution immediately and load assets as needed
    ON_DEMAND = "ON_DEMAND"


class FileConflictResolution(Enum):
    SKIP = 1
    OVERWRITE = 2
    CREATE_COPY = 3

    @classmethod
    def from_index(cls, index: int):
        return cls(index)
