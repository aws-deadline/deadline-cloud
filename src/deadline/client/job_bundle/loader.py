# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

__all__ = [
    "read_yaml_or_json",
    "read_yaml_or_json_object",
    "parse_yaml_or_json_content",
    "validate_directory_symlink_containment",
]

import json
import os
from itertools import chain
from typing import Any, Dict, Optional, Tuple

import yaml

from ..exceptions import DeadlineOperationError


def validate_directory_symlink_containment(job_bundle_dir: str) -> None:
    """
    Validates the integrity of the job bundle, validating that all files
    contained in the job bundle resolve inside of the job bundle directory.
    """
    # The job bundle could itself be a symlink dir
    resolved_root = os.path.realpath(job_bundle_dir)
    if not os.path.isdir(resolved_root):
        raise DeadlineOperationError(
            f"Job bundle path provided is not a directory:\n{job_bundle_dir}"
        )
    for root_dir, dir_names, file_names in os.walk(resolved_root):
        for path in chain(dir_names, file_names):
            norm_path = os.path.normpath(os.path.join(root_dir, path))
            resolved_path = os.path.realpath(norm_path)
            common_path = os.path.commonpath([resolved_root, resolved_path])
            if common_path != resolved_root:
                raise DeadlineOperationError(
                    f"Job bundle cannot contain a path that resolves outside of the resolved bundle directory:\n{resolved_root}\n\nPath in bundle:\n{norm_path}\nResolves to:\n{resolved_path}"
                )


def read_yaml_or_json(job_bundle_dir: str, filename: str, required: bool) -> Tuple[str, str]:
    """
    Checks whether {filename}.json or {filename}.yaml exist in the provided
    job bundle directory, and returns a tuple (<file contents>, "YAML|JSON").

    Args:
        job_bundle_dir (str): The directory containing the job bundle.
        filename (str): The filename, without extension, to look for.
        required (bool): Whether this file is required. If not required and its missing,
                         the function returns ("", "").
    """
    path_prefix = os.path.join(job_bundle_dir, filename)
    has_json = os.path.isfile(path_prefix + ".json")
    has_yaml = os.path.isfile(path_prefix + ".yaml")
    if has_json and has_yaml:
        raise DeadlineOperationError(
            f"Job bundle directory has both {filename}.json and {filename}.yaml, only one is permitted:\n{job_bundle_dir}"
        )
    elif has_json:
        with open(path_prefix + ".json", encoding="utf8") as f:
            return (f.read(), "JSON")
    elif has_yaml:
        with open(path_prefix + ".yaml", encoding="utf8") as f:
            return (f.read(), "YAML")
    else:
        if required:
            raise DeadlineOperationError(
                f"Job bundle directory lacks a {filename}.json or {filename}.yaml:\n{job_bundle_dir}"
            )
        else:
            return ("", "")


def parse_yaml_or_json_content(file_contents: str, file_type: str, bundle_dir: str, filename: str):
    """
    Parses a yaml or json file that was read by the `read_yaml_or_json` function.

    Args:
        file_contents: The contents of the file that was read.
        file_type: The type of the file
        filename: The filename that was read, without extension (for error messages).
        bundle_dir: The bundle directory the file was in (for error messages).
    """
    if file_type == "JSON":
        try:
            return json.loads(file_contents)
        except json.JSONDecodeError as e:
            raise DeadlineOperationError(f"Error loading '{filename}.json':\n{e}")
    elif file_type == "YAML":
        try:
            return yaml.safe_load(file_contents)
        except yaml.MarkedYAMLError as e:
            raise DeadlineOperationError(f"Error loading '{filename}.yaml':\n{e}")
    else:
        raise RuntimeError(f"Unexpected file type '{file_type}' in job bundle:\n{bundle_dir}")


def read_yaml_or_json_object(
    bundle_dir: str, filename: str, required: bool
) -> Optional[Dict[str, Any]]:
    """
    Checks whether {filename}.json or {filename}.yaml exist in the provided
    job bundle directory, and returns the file parsed into an object.

    Args:
        job_bundle_dir (str): The directory containing the job bundle.
        filename (str): The filename, without extension, to look for.
        required (bool): Whether this file is required. If not required and its missing,
                         the function returns ("", "").
    """
    file_contents, file_type = read_yaml_or_json(bundle_dir, filename, required)
    if file_contents and file_type:
        return parse_yaml_or_json_content(file_contents, file_type, bundle_dir, filename)
    else:
        return None
