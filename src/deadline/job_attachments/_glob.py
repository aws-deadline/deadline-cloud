# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import json
from pathlib import Path
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
