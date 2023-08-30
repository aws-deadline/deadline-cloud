# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import datetime
from functools import wraps
import io
import os
import sys
from enum import Enum
from hashlib import shake_256
from pathlib import Path, PurePath, PurePosixPath, PureWindowsPath
from typing import Callable, Optional, Tuple, Union
import uuid
import yaml

import xxhash

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


def hash_file(file_path: str) -> str:
    with open(file_path, "rb") as file:
        hasher = xxhash.xxh3_128()
        while True:
            chunk = file.read(io.DEFAULT_BUFFER_SIZE)
            if not chunk:
                break
            hasher.update(chunk)
        return hasher.hexdigest()


def hash_data(data: bytes) -> str:
    hasher = xxhash.xxh3_128()
    hasher.update(data)
    return hasher.hexdigest()


def join_s3_paths(root: str, *args: str):
    return "/".join([root, *args])


def generate_random_guid():
    return str(uuid.uuid4()).replace("-", "")


def float_to_iso_string(time: float):
    seconds = int(time)
    microseconds = int((time - seconds) * 1000000)

    dt = datetime.datetime.utcfromtimestamp(seconds) + datetime.timedelta(microseconds=microseconds)
    iso_string = dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    return iso_string


def human_readable_file_size(size_in_bytes: int) -> str:
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


def get_deadline_formatted_os() -> str:
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


def get_unique_dest_dir_name(source_root: str) -> str:
    # Note: this is a quick naive way to attempt to prevent colliding
    # relative paths across manifests without adding too much
    # length to the filepaths. length = 2n where n is the number
    # passed to hexdigest.
    return f"assetroot-{shake_256(source_root.encode()).hexdigest(10)}"


def map_source_path_to_dest_path(source_os: str, dest_os: str, path_str: str) -> PurePath:
    """
    Given a path from a source machine, convert it to an equivalent path on the destination OS.
    """
    path: PurePath
    if source_os.lower() == OperatingSystemFamily.WINDOWS.value:
        path = PureWindowsPath(path_str)
    else:
        path = PurePosixPath(path_str)
    parts = path.parts

    if dest_os.lower() == OperatingSystemFamily.WINDOWS.value:
        return PureWindowsPath(*parts)
    else:
        return PurePosixPath(*parts)


def get_os_pure_path(path: Path) -> PurePath:
    """
    Given a path object, converts it to a pathlib PurePath of the correct OS type.
    """
    if get_deadline_formatted_os() == OperatingSystemFamily.WINDOWS.value:
        return PureWindowsPath(path)
    else:
        return PurePosixPath(path)


def get_bucket_and_object_key(s3_path: str) -> Tuple[str, str]:
    """Returns the bucket name and object key from the S3 URI"""
    bucket, key = s3_path.replace("s3://", "").split("/", maxsplit=1)
    return bucket, key


def get_default_hash_cache_db_file_dir() -> Optional[str]:
    """
    Gets the expected directory for the hash cache database file based on OS environment variables.
    If a directory cannot be found, defaults to the working directory.
    """
    default_path = os.environ.get("HOME")
    if default_path:
        default_path = os.path.join(default_path, CONFIG_ROOT, COMPONENT_NAME)
    return default_path


def is_relative_to(path1: Union[Path, str], path2: Union[Path, str]) -> bool:
    """
    Determines if path1 is relative to path2. This function is to support
    Python versions that do not have the built-in `Path.is_relative_to()` method.
    """
    try:
        Path(path1).relative_to(Path(path2))
        return True
    except ValueError:
        return False


class OJIOToken:
    def __init__(self, token: str) -> None:
        self.token = token

    def __str__(self) -> str:
        return "{{ " + self.token + " }}"


# add OJIOToken to the YAML representer
def ojio_token_representer(dumper: yaml.Dumper, token: OJIOToken) -> yaml.Node:
    return dumper.represent_data(str(token))


yaml.add_representer(OJIOToken, ojio_token_representer)


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


def log_and_reraise_exception(prefix_mssage: Optional[str] = None) -> Callable:
    """
    A decorator for logging exceptions and re-raising them in instance methods.
    This decorator expects the class to have a 'logger' attribute. If not,
    an AttributeError will be raised.

    Usage:
        class MyClass:
            def __init__(self):
                self.logger = ...

            @log_and_reraise_exception("Error in some_methid: ")
            def some_method(self):
                ...

        my_instance = MyClass()
        my_instance.some_method() # Logs the exception and re-raises it.

    Args:
        prefix_mssage: A message to prefix the exception message with.
    """

    def decorator(func: Callable):
        @wraps(func)
        def wrapper(instance, *args, **kwargs):
            if not hasattr(instance, "logger"):
                raise AttributeError(
                    "The class using log_and_reraise_exception must have a 'logger' attribute."
                )
            try:
                return func(instance, *args, **kwargs)
            except Exception as e:
                instance.logger.exception(f"{prefix_mssage} - {str(e)}")
                raise e

        return wrapper

    return decorator
