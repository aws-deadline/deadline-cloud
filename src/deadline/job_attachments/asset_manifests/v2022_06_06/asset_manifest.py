# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

""" Module that defines the second iteration of the asset manifest """
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Type

from .._canonical_json import canonical_path_comparator, manifest_to_canonical_json_string
from ..base_manifest import AssetManifest as _BaseAssetManifest
from ..base_manifest import Path as _BasePath
from ..manifest_model import ManifestModel as _ManifestModelBase
from ..versions import ManifestVersion


@dataclass
class Path(_BasePath):
    """
    Data class for paths in the Asset Manifest
    """

    manifest_version = ManifestVersion.v2022_06_06

    def __init__(self, *, path: str, hash: str) -> None:
        super().__init__(path=path, hash=hash)


@dataclass
class AssetManifest(_BaseAssetManifest):
    """
    Data class for the v2022-06-06 version of the Deadline asset manifest.
    """

    def __init__(self, *, hash_alg: str, paths: List[_BasePath]):
        super().__init__(hash_alg=hash_alg, paths=paths)
        self.manifestVersion = ManifestVersion.v2022_06_06

    @classmethod
    def decode(cls, *, manifest_data: dict[str, Any]) -> AssetManifest:
        """
        Return an instance of this class given a manifest dictionary.
        Assumes the manifest has been validated prior to calling.
        """
        return cls(
            hash_alg=manifest_data["hashAlg"],
            paths=[Path(path=path["path"], hash=path["hash"]) for path in manifest_data["paths"]],
        )

    def encode(self) -> str:
        """
        Return a canonicalized JSON string based on the following:
        * The JSON file *MUST* adhere to the JSON canonicalization guidelines
          outlined here (https://www.rfc-editor.org/rfc/rfc8785.html).
            * For now this is a simplification of this spec. Whitespace between JSON tokens are
              not emitted, and the keys are lexographically sorted. However the current implementation doesn't
              serialize Literals, String, Numbers, etc. to the letter of the spec explicitly.
              It implicitly follows the spec as the object keys all fall within the ASCII range of characters
              and this version of the Asset Manifest only serializes strings.
        * The paths array *MUST* be in lexicographical order by path.
        """
        # Sort by UTF-16 values as per the spec
        # https://www.rfc-editor.org/rfc/rfc8785.html#name-sorting-of-object-propertie
        self.paths.sort(key=canonical_path_comparator)
        return manifest_to_canonical_json_string(manifest=self)


class ManifestModel(_ManifestModelBase):
    """
    The asset manifest model for v2022-06-06
    """

    manifest_version: ManifestVersion = ManifestVersion.v2022_06_06
    AssetManifest: Type[AssetManifest] = AssetManifest
    Path: Type[Path] = Path
