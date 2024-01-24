# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

""" Module that defines the v2023-03-03 version of the asset manifest """
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Type

from .._canonical_json import canonical_path_comparator, manifest_to_canonical_json_string
from ..base_manifest import BaseAssetManifest, BaseManifestPath
from ..hash_algorithms import HashAlgorithm
from ..manifest_model import BaseManifestModel
from ..versions import ManifestVersion
from ...exceptions import ManifestDecodeValidationError


SUPPORTED_HASH_ALGS: set[HashAlgorithm] = {HashAlgorithm.XXH128}
DEFAULT_HASH_ALG: HashAlgorithm = HashAlgorithm.XXH128


@dataclass
class ManifestPath(BaseManifestPath):
    """
    Extension for version v2023-03-03 of the asset manifest.
    """

    manifest_version = ManifestVersion.v2023_03_03

    def __init__(self, *, path: str, hash: str, size: int, mtime: int) -> None:
        super().__init__(path=path, hash=hash, size=size, mtime=mtime)


@dataclass
class AssetManifest(BaseAssetManifest):
    """Version v2023-03-03 of the asset manifest"""

    totalSize: int  # pyline: disable=invalid-name

    def __init__(
        self, *, hash_alg: HashAlgorithm, paths: list[BaseManifestPath], total_size: int
    ) -> None:
        if hash_alg not in SUPPORTED_HASH_ALGS:
            raise ManifestDecodeValidationError(
                f"Unsupported hashing algorithm: {hash_alg}. Must be one of: {[e.value for e in SUPPORTED_HASH_ALGS]}"
            )

        super().__init__(hash_alg=hash_alg, paths=paths)
        self.totalSize = total_size
        self.manifestVersion = ManifestVersion.v2023_03_03

    @classmethod
    def decode(cls, *, manifest_data: dict[str, Any]) -> AssetManifest:
        """
        Return an instance of this class given a manifest dictionary.
        Assumes the manifest has been validated prior to calling.
        """
        try:
            hash_alg: HashAlgorithm = HashAlgorithm(manifest_data["hashAlg"])
        except ValueError:
            raise ManifestDecodeValidationError(
                f"Unsupported hashing algorithm: {hash_alg}. Must be one of: {[e.value for e in SUPPORTED_HASH_ALGS]}"
            )

        return cls(
            hash_alg=hash_alg,
            paths=[
                ManifestPath(
                    path=path["path"], hash=path["hash"], size=path["size"], mtime=path["mtime"]
                )
                for path in manifest_data["paths"]
            ],
            total_size=manifest_data["totalSize"],
        )

    @classmethod
    def get_default_hash_alg(cls) -> HashAlgorithm:  # pragma: no cover
        """Returns the default hashing algorithm for the Asset Manifest, represented as a string"""
        return DEFAULT_HASH_ALG

    def encode(self) -> str:
        """
        Return a canonicalized JSON string of the manifest
        """
        self.paths.sort(key=canonical_path_comparator)
        return manifest_to_canonical_json_string(manifest=self)


class ManifestModel(BaseManifestModel):
    """
    The asset manifest model for v2023-03-03
    """

    manifest_version: ManifestVersion = ManifestVersion.v2023_03_03
    AssetManifest: Type[AssetManifest] = AssetManifest
    Path: Type[ManifestPath] = ManifestPath
