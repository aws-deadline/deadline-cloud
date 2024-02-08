# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import datetime
from hashlib import shake_256
from pathlib import Path
from typing import Tuple, Union
import uuid


__all__ = [
    "_join_s3_paths",
    "_generate_random_guid",
    "_float_to_iso_datetime_string",
    "_human_readable_file_size",
    "_get_unique_dest_dir_name",
    "_get_bucket_and_object_key",
    "_is_relative_to",
]


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
