# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import json
import os
from typing import Any
from ._yaml import deadline_yaml_dump


def save_yaml_or_json_to_file(
    bundle_dir: str,
    filename: str,
    file_type: str,
    data: Any,
) -> None:
    """
    Saves data as either a JSON or YAML file depending on the file_type provided. Useful for saving
    job bundle data files which can be in either format. file_type should be either "YAML" or "JSON".
    """
    with open(
        os.path.join(bundle_dir, f"{filename}.{file_type.lower()}"), "w", encoding="utf8"
    ) as f:
        if file_type == "YAML":
            deadline_yaml_dump(data, f)
        elif file_type == "JSON":
            json.dump(data, f, indent=2)
        else:
            raise RuntimeError(f"Unexpected file type '{file_type}' in job bundle:\n{bundle_dir}")
