# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

""" Module that defines the v2023-03-03 version of the asset manifest """
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Type

from .._canonical_json import canonical_path_comparator, manifest_to_canonical_json_string
from ..base_manifest import AssetManifest as _BaseAssetManifest
from ..base_manifest import Path as _BasePath
from ..manifest_model import ManifestModel as _ManifestModelBase
from ..versions import ManifestVersion


@dataclass
class Path(_BasePath):
    """
    Extension for version v2023-03-03 of the asset manifest.
    """

    size: int
    mtime: int
    manifest_version = ManifestVersion.v2023_03_03

    def __init__(self, *, path: str, hash: str, size: int, mtime: int) -> None:
        super().__init__(path=path, hash=hash)
        self.size = size
        self.mtime = mtime


@dataclass
class AssetManifest(_BaseAssetManifest):
    """Version v2023-03-03 of the asset manifest"""

    totalSize: int  # pyline: disable=invalid-name

    def __init__(self, *, hash_alg: str, paths: list[_BasePath], total_size: int) -> None:
        super().__init__(hash_alg=hash_alg, paths=paths)
        self.totalSize = total_size
        self.manifestVersion = ManifestVersion.v2023_03_03

    @classmethod
    def decode(cls, *, manifest_data: dict[str, Any]) -> AssetManifest:
        """
        Return an instance of this class given a manifest dictionary.
        Assumes the manifest has been validated prior to calling.
        """
        return cls(
            hash_alg=manifest_data["hashAlg"],
            paths=[
                Path(path=path["path"], hash=path["hash"], size=path["size"], mtime=path["mtime"])
                for path in manifest_data["paths"]
            ],
            total_size=manifest_data["totalSize"],
        )

    def encode(self) -> str:
        """
        Return a canonicalized JSON string of the manifest
        """
        self.paths.sort(key=canonical_path_comparator)
        return manifest_to_canonical_json_string(manifest=self)


class ManifestModel(_ManifestModelBase):
    """
    The asset manifest model for v2023-03-03
    """

    manifest_version: ManifestVersion = ManifestVersion.v2023_03_03
    AssetManifest: Type[AssetManifest] = AssetManifest
    Path: Type[Path] = Path
