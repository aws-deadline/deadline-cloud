# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

""" Contains the base asset manifest and entities that are part of the Asset Manifest """
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, fields
from typing import Any, ClassVar

from .hash_algorithms import HashAlgorithm
from .versions import ManifestVersion


@dataclass
class BaseManifestPath(ABC):
    """
    Data class for paths in the Asset Manifest
    """

    path: str
    hash: str
    size: int
    mtime: int
    manifest_version: ClassVar[ManifestVersion]

    def __init__(self, *, path: str, hash: str, size: int, mtime: int) -> None:
        self.path = path
        self.hash = hash
        self.size = size
        self.mtime = mtime

    def __eq__(self, other: object) -> bool:
        """
        By default dataclasses still check ClassVars for equality.
        We only want to compare fields.
        :param other:
        :return: True if all fields are equal, False otherwise.
        """
        if not isinstance(other, BaseManifestPath):
            return NotImplemented
        return fields(self) == fields(other)


@dataclass
class BaseAssetManifest(ABC):
    """Base class for the Asset Manifest."""

    hashAlg: HashAlgorithm
    paths: list[BaseManifestPath]
    manifestVersion: ManifestVersion

    def __init__(
        self,
        *,
        paths: list[BaseManifestPath],
        hash_alg: HashAlgorithm,
    ):
        self.paths = paths
        self.hashAlg = hash_alg

    @classmethod
    @abstractmethod
    def get_default_hash_alg(cls) -> HashAlgorithm:  # pragma: no cover
        """Returns the default hashing algorithm for the Asset Manifest"""
        raise NotImplementedError(
            "Asset Manifest base class does not implement get_default_hash_alg"
        )

    @classmethod
    @abstractmethod
    def decode(cls, *, manifest_data: dict[str, Any]) -> BaseAssetManifest:  # pragma: no cover
        """Turn a dictionary for a manifest into an AssetManifest object"""
        raise NotImplementedError("Asset Manifest base class does not implement decode")

    @abstractmethod
    def encode(self) -> str:  # pragma: no cover
        """
        Recursively encode the Asset Manifest into a string according to
        whatever format the Asset Manifest was written for.
        """
        raise NotImplementedError("Asset Manifest base class does not implement encode")
