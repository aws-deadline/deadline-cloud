# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

""" Tests for the asset manifest model """

import pytest

from deadline.job_attachments.asset_manifests import (
    BaseManifestModel,
    ManifestModelRegistry,
    v2023_03_03,
)
from deadline.job_attachments.asset_manifests.versions import ManifestVersion


@pytest.mark.parametrize(
    "version,expected_model",
    [
        (ManifestVersion.v2023_03_03, v2023_03_03.ManifestModel),
    ],
)
def test_get_manifest_model(version: ManifestVersion, expected_model: BaseManifestModel):
    """
    Test to ensure that the appropriate manifest model is returned given a manifest version
    """
    model = ManifestModelRegistry.get_manifest_model(version=version)
    assert model == expected_model  # type: ignore[comparison-overlap]


def test_get_manifest_model_no_manifest_for_version():
    """
    Test to ensure the correct error gets raised when there is no asset manifest model for a given version.
    """
    with pytest.raises(
        RuntimeError, match=r"No model for asset manifest version: (ManifestVersion.)?UNDEFINED"
    ):
        ManifestModelRegistry.get_manifest_model(version=ManifestVersion.UNDEFINED)
