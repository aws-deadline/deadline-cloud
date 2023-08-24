# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

""" Contains the base asset manifest and entities that are part of the Asset Manifest """
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, fields
from typing import Any, ClassVar

from .versions import ManifestVersion


@dataclass
class Path(ABC):
    """
    Data class for paths in the Asset Manifest
    """

    path: str
    hash: str
    manifest_version: ClassVar[ManifestVersion]

    def __init__(self, *, path: str, hash: str) -> None:
        self.path = path
        self.hash = hash

    def __eq__(self, other: object) -> bool:
        """
        By default dataclasses still check ClassVars for equality.
        We only want to compare fields.
        :param other:
        :return: True if all fields are equal, False otherwise.
        """
        if not isinstance(other, Path):
            return NotImplemented
        return fields(self) == fields(other)


@dataclass
class AssetManifest(ABC):
    """Base class for the Asset Manifest."""

    hashAlg: str  # pylint: disable=invalid-name
    paths: list[Path]
    manifestVersion: ManifestVersion

    def __init__(
        self,
        *,
        paths: list[Path],
        hash_alg: str,
    ):
        self.paths = paths
        self.hashAlg = hash_alg

    @classmethod
    @abstractmethod
    def decode(cls, *, manifest_data: dict[str, Any]) -> AssetManifest:  # pragma: no cover
        """Turn a dictionary for a manifest into an AssetManifest object"""
        raise NotImplementedError("Asset Manifest base class does not implement decode")

    @abstractmethod
    def encode(self) -> str:  # pragma: no cover
        """
        Recursively encode the Asset Manifest into a string according to
        whatever format the Asset Manifest was written for.
        """
        raise NotImplementedError("Asset Manifest base class does not implement encode")
