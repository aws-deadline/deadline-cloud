# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

""" Module that defines the second iteration of the asset manifest """
from __future__ import annotations

import dataclasses
import json

from .base_manifest import BaseAssetManifest, BaseManifestPath


def canonical_path_comparator(path: BaseManifestPath):
    """
    Comparator for sorting paths.
    """
    # Sort by UTF-16 values as per the spec
    # https://www.rfc-editor.org/rfc/rfc8785.html#name-sorting-of-object-propertie
    return path.path.encode("utf-16_be")


def manifest_to_canonical_json_string(manifest: BaseAssetManifest) -> str:
    """
    Return a canonicalized JSON string based on the following:
    * The JSON file *MUST* adhere to the JSON canonicalization guidelines
        outlined here (https://www.rfc-editor.org/rfc/rfc8785.html).
        * For now this is a simplification of this spec. Whitespace between JSON tokens are
            not emitted, and the keys are lexographically sorted. However the current implementation doesn't
            serialize Literals, String, Numbers, etc. to the letter of the spec explicitly.
            It implicitly follows the spec as the object keys all fall within the ASCII range of characters
            and this version of the Asset Manifest only serializes strings and integers.
    * The paths array *MUST* be in lexicographical order by path.
    """
    return json.dumps(
        dataclasses.asdict(manifest), sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
