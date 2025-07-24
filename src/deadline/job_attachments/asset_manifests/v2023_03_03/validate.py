# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Contains functions validating Asset Manifests version 2023_03_03."""

from __future__ import annotations

from typing import Any, Optional, Tuple

_REQUIRED_FIELDS_2023_03_03: list[str] = [
    "hashAlg",
    "paths",
    "manifestVersion",
    "totalSize",
]

_HASH_ALGS_2023_03_03: set[str] = {"xxh128"}

_PATH_REQUIRED_FIELDS_2023_03_03: list[str] = [
    "path",
    "hash",
    "size",
    "mtime",
]


def _get_missing_fields(obj: dict[str, Any], required: list[str]) -> list[str]:
    missing = []
    for field in required:
        if field not in obj:
            missing.append(field)
    return missing


def _validate_path_2023_03_03(path_object: dict[str, Any]) -> Tuple[bool, Optional[str]]:
    missing = _get_missing_fields(path_object, _PATH_REQUIRED_FIELDS_2023_03_03)
    if len(missing) > 0:
        return False, f"path is missing required field(s) {missing}"

    path = path_object["path"]
    if not isinstance(path, str):
        return False, "path must be a string"

    hash = path_object["hash"]
    if not isinstance(hash, str):
        return False, "hash must be a string"

    size = path_object["size"]
    if not isinstance(size, int):
        return False, "size must be an integer"

    mtime = path_object["mtime"]
    if not isinstance(mtime, int):
        return False, "mtime must be an integer"

    return True, None


def validate_manifest_2023_03_03(manifest: dict[str, Any]) -> Tuple[bool, Optional[str]]:
    missing = _get_missing_fields(manifest, _REQUIRED_FIELDS_2023_03_03)
    if len(missing) > 0:
        return False, f"manifest is missing required field(s) {missing}"

    manifest_version = manifest["manifestVersion"]
    if not isinstance(manifest_version, str) or manifest_version != "2023-03-03":
        return False, 'manifestVersion must be "2023-03-03"'

    hash_alg = manifest["hashAlg"]
    if not isinstance(hash_alg, str) or hash_alg not in _HASH_ALGS_2023_03_03:
        return False, f"hashAlg must be one of {_HASH_ALGS_2023_03_03}"

    total_size = manifest["totalSize"]
    if not isinstance(total_size, int):
        return False, "totalSize must be an integer"

    paths = manifest["paths"]
    if not isinstance(paths, list):
        return False, "paths must be a list"
    elif len(paths) < 1:
        return False, "paths must have a least one item"
    else:
        for path_object in paths:
            ok, message = _validate_path_2023_03_03(path_object)
            if not ok:
                return False, message

    return True, None
