# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

""" Module for the base Manifest Model. """
from __future__ import annotations

from typing import Type

from .base_manifest import AssetManifest, Path  # noqa # pylint: disable=unused-import
from .versions import ManifestVersion


class ManifestModel:
    """The base Manifest Model"""

    manifest_version: ManifestVersion = ManifestVersion.UNDEFINED  # pylint: disable=invalid-name
    AssetManifest: Type[AssetManifest]
    Path: Type[Path]


class ManifestModelRegistry:
    _asset_manifest_mapping: dict[ManifestVersion, Type[ManifestModel]] = dict()

    @classmethod
    def register(cls) -> None:
        """
        Register the availble manifest models.
        """
        # Import here to avoid circular dependancies.
        from .v2022_06_06 import ManifestModel as _ManifestModel2022_06_06
        from .v2023_03_03 import ManifestModel as _ManifestModel2023_03_03

        new_manifests = {
            ManifestVersion.v2022_06_06: _ManifestModel2022_06_06,
            ManifestVersion.v2023_03_03: _ManifestModel2023_03_03,
        }
        cls._asset_manifest_mapping = {**cls._asset_manifest_mapping, **new_manifests}

    @classmethod
    def get_manifest_model(cls, *, version: ManifestVersion) -> Type[ManifestModel]:
        """
        Get the manifest model for the specified version.
        """
        manifest_model = cls._asset_manifest_mapping.get(version, None)
        if not manifest_model:
            raise RuntimeError(f"No model for asset manifest version: {version}")
        return manifest_model
