# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Provides deadline_yaml_dump, which works like pyyaml's safe_dump,
but saves multi-line strings with the "|" style.
"""

import json
import os
from typing import Any
import yaml
from yaml.emitter import Emitter
from yaml.representer import SafeRepresenter
from yaml.resolver import Resolver
from yaml.serializer import Serializer


class DeadlineRepresenter(SafeRepresenter):
    """
    Identical to pyyaml's SafeRepresenter, but uses "|" style for
    multi-line strings.
    """

    def represent_str(self, data):
        if "\n" in data:
            return self.represent_scalar("tag:yaml.org,2002:str", data, style="|")
        else:
            return self.represent_scalar("tag:yaml.org,2002:str", data)


DeadlineRepresenter.add_representer(str, DeadlineRepresenter.represent_str)


class DeadlineDumper(Emitter, Serializer, DeadlineRepresenter, Resolver):
    def __init__(
        self,
        stream,
        default_style=None,
        default_flow_style=False,
        canonical=None,
        indent=None,
        width=None,
        allow_unicode=None,
        line_break=None,
        encoding=None,
        explicit_start=None,
        explicit_end=None,
        version=None,
        tags=None,
        sort_keys=True,
    ):
        Emitter.__init__(
            self,
            stream,
            canonical=canonical,
            indent=indent,
            width=width,
            allow_unicode=allow_unicode,
            line_break=line_break,
        )
        Serializer.__init__(
            self,
            encoding=encoding,
            explicit_start=explicit_start,
            explicit_end=explicit_end,
            version=version,
            tags=tags,
        )
        DeadlineRepresenter.__init__(  # type: ignore[call-arg]
            self,
            default_style=default_style,
            default_flow_style=default_flow_style,
            sort_keys=sort_keys,
        )
        Resolver.__init__(self)


def deadline_yaml_dump(data, stream=None, **kwds):
    """
    Works like pyyaml's safe_dump, but saves multi-line
    strings with the "|" style and defaults to sort_keys=False.
    """
    return yaml.dump_all([data], stream, Dumper=DeadlineDumper, sort_keys=False, **kwds)


def save_yaml_or_json_to_file(
    bundle_dir: str,
    filename: str,
    file_type: str,
    data: Any,
) -> None:
    """
    Saves data as either a JSON or YAML file depending on the file_type provided. Useful for saving
    job bundle data files which can be in either format.
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
