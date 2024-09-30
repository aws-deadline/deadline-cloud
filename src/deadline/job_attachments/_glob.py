# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import json
from pathlib import Path
from typing import List, Optional
from deadline.client.exceptions import NonValidInputError
from deadline.job_attachments.models import GlobConfig


def _process_glob_inputs(glob_arg_input: str) -> GlobConfig:
    """
    Helper function to process glob inputs.
    glob_input: String, can represent a json, filepath, or general include glob syntax.
    """

    # Default Glob config.
    glob_config = GlobConfig()
    if glob_arg_input is None or len(glob_arg_input) == 0:
        # Not configured, or not passed in.
        return glob_config

    try:
        input_as_path = Path(glob_arg_input)
        if input_as_path.is_file():
            # Read the file so it can be parsed as JSON.
            with open(glob_arg_input) as f:
                glob_arg_input = f.read()
    except Exception:
        # If this cannot be processed as a file, try it as JSON.
        pass

    try:
        # Parse the input as JSON, default to Glob Config defaults.
        input_as_json = json.loads(glob_arg_input)
        glob_config.include_glob = input_as_json.get(GlobConfig.INCLUDE, glob_config.include_glob)
        glob_config.exclude_glob = input_as_json.get(GlobConfig.EXCLUDE, glob_config.exclude_glob)
    except Exception:
        # This is not a JSON blob, bad input.
        raise NonValidInputError(f"Glob input {glob_arg_input} cannot be deserialized as JSON")

    return glob_config


def _glob_paths(
    path: str, include: List[str] = ["**/*"], exclude: Optional[List[str]] = None
) -> List[str]:
    """
    Glob routine that supports Unix style pathname pattern expansion for includes and excludes.
    This function will recursively list all files of path, including all files globbed by include and removing all files marked by exclude.
    path: Root path to glob.
    include: Optional, pattern syntax for files to include.
    exclude: Optional, pattern syntax for files to exclude.
    return: List of files found based on supplied glob patterns.
    """
    include_files: List[Optional[str]] = []
    for input_glob in include:
        include_files.extend(
            [str(path) if path.is_file() else None for path in Path(path).glob(input_glob)]
        )
    include_files = list(filter(None, include_files))

    if exclude:
        exclude_files = []
        for exclude_glob in exclude:
            exclude_files.extend(
                [str(path) if path.is_file() else None for path in Path(path).glob(exclude_glob)]
            )

        # use sets to remove duplicates and remove all excluded file.
        exclude_files = list(filter(None, exclude_files))
        include_files = list(set(include_files) - set(exclude_files))
    return include_files  # type: ignore[return-value]
