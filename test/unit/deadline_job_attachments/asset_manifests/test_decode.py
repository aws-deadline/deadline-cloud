# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

""" Tests for the asset_manifests.decode module """
from __future__ import annotations
from enum import Enum

import json
from dataclasses import dataclass
import re
from typing import Any
from unittest.mock import patch

import pytest

import deadline
from deadline.job_attachments.asset_manifests import decode, versions, HashAlgorithm
from deadline.job_attachments.asset_manifests.v2023_03_03 import (
    AssetManifest as AssetManifest_v2023_03_03,
)
from deadline.job_attachments.asset_manifests.v2023_03_03 import ManifestPath as Path_v2023_03_03
from deadline.job_attachments.exceptions import ManifestDecodeValidationError


@dataclass
class ManifestParam:
    manifest_str: str
    manifest_version: versions.ManifestVersion


@pytest.fixture
def manifest_params(default_manifest_str_v2023_03_03: str) -> list[ManifestParam]:
    return [
        ManifestParam(default_manifest_str_v2023_03_03, versions.ManifestVersion.v2023_03_03),
    ]


def test_validate_manifest(manifest_params: list[ManifestParam]):
    """
    Test the a valid manifest is correctly validated.
    """
    for manifest_param in manifest_params:
        manifest: dict[str, Any] = json.loads(manifest_param.manifest_str)
        assert decode.validate_manifest(manifest, manifest_param.manifest_version) == (True, None)


def test_validate_manifest_manifest_not_valid_manifest(manifest_params: list[ManifestParam]):
    """
    Test that a manifest is returned as not valid with an expected error string if the manifest isn't valid
    """
    for manifest_param in manifest_params:
        manifest: dict[str, Any] = json.loads(manifest_param.manifest_str)
        del manifest["hashAlg"]
        valid, error_str = decode.validate_manifest(manifest, manifest_param.manifest_version)
        assert not valid
        assert error_str is not None
        assert error_str.startswith(
            "'hashAlg' is a required property\n\nFailed validating 'required' in schema:\n"
        )


def test_validate_manifest_not_valid_schema(manifest_params: list[ManifestParam]):
    """
    Test that a manifest is returned as not valid with an expected error string if the schema isn't valid
    """
    for manifest_param in manifest_params:
        manifest: dict[str, Any] = json.loads(manifest_param.manifest_str)

        bad_schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "AWS Deadline Cloud Asset Manifest Schema",
            "type": "bad_type",
            "required": ["hashAlg"],
            "properties": {
                "manifestVersion": {
                    "const": "yes",
                }
            },
        }

        with patch(
            f"{deadline.__package__}.job_attachments.asset_manifests.decode._get_schema",
            return_value=bad_schema,
        ):
            valid, error_str = decode.validate_manifest(manifest, manifest_param.manifest_version)
            assert not valid
            assert error_str is not None
            assert error_str.startswith("'bad_type' is not valid under any of the given schemas")


def test_decode_manifest_v2023_03_03(default_manifest_str_v2023_03_03: str):
    """
    Test that a v2023-03-03 manifest string decodes to an AssetManifest object as expected.
    """
    expected_manifest = AssetManifest_v2023_03_03(
        hash_alg=HashAlgorithm.XXH128,
        total_size=10,
        paths=[
            Path_v2023_03_03(path="\r", hash="CarriageReturn", size=1, mtime=1679079744833848),
            Path_v2023_03_03(path="1", hash="One", size=1, mtime=1679079344833868),
            Path_v2023_03_03(path="another_test_file", hash="c", size=1, mtime=1675079344833848),
            Path_v2023_03_03(path="test_dir/test_file", hash="b", size=1, mtime=1479079344833848),
            Path_v2023_03_03(path="test_file", hash="a", size=1, mtime=167907934333848),
            Path_v2023_03_03(path="\u0080", hash="Control", size=1, mtime=1679079344833348),
            Path_v2023_03_03(
                path="ö", hash="LatinSmallLetterOWithDiaeresis", size=1, mtime=1679079344833848
            ),
            Path_v2023_03_03(path="€", hash="EuroSign", size=1, mtime=1679079344836848),
            Path_v2023_03_03(path="😀", hash="EmojiGrinningFace", size=1, mtime=1679579344833848),
            Path_v2023_03_03(
                path="דּ", hash="HebrewLetterDaletWithDagesh", size=1, mtime=1679039344833848
            ),
        ],
    )
    assert decode.decode_manifest(default_manifest_str_v2023_03_03) == expected_manifest


def test_decode_manifest_version_not_supported():
    """
    Test that a ManifestDecodeValidationError is raised if the manifest passed has a version that isn't valid.
    """
    with pytest.raises(
        ManifestDecodeValidationError,
        match=re.escape(
            "Unknown manifest version: 1900-06-06 (Currently supported Manifest versions: 2023-03-03)"
        ),
    ):
        decode.decode_manifest('{"manifestVersion": "1900-06-06"}')


def test_decode_manifest_version_not_supported_when_multiple_versions_are_supported():
    """
    Test that a ManifestDecodeValidationError is raised with a descriptive error message if the manifest passed
    has a version that isn't valid. In this test, the ManifestVersion class is mocked to simulate having multple
    supported manifest versions.
    """

    class MockManifestVersion(str, Enum):
        UNDEFINED = "UNDEFINED"
        v2023_03_03 = "2023-03-03"
        v2024_04_03 = "2024-04-03"
        v2025_05_03 = "2025-05-03"

    with patch(
        f"{deadline.__package__}.job_attachments.asset_manifests.decode.ManifestVersion",
        new=MockManifestVersion,
    ):
        with pytest.raises(
            ManifestDecodeValidationError,
            match=re.escape(
                "Unknown manifest version: 1900-06-06 "
                "(Currently supported Manifest versions: 2023-03-03, 2024-04-03, 2025-05-03)"
            ),
        ):
            decode.decode_manifest('{"manifestVersion": "1900-06-06"}')


def test_decode_manifest_not_valid_manifest():
    """
    Test that a ManifestDecodeValidationError is raised if the manifest passed in is not valid.
    """
    with pytest.raises(
        ManifestDecodeValidationError, match=r".*Failed validating 'required' in schema:.*"
    ):
        decode.decode_manifest('{"manifestVersion": "2023-03-03"}')


def test_decode_manifest_missing_manifest_version():
    """
    Test that a ManifestDecodeValidationError is raised if the manifest passed in is missing the manifestVersion field.
    """
    with pytest.raises(
        ManifestDecodeValidationError,
        match='Manifest is missing the required "manifestVersion" field',
    ):
        decode.decode_manifest('{"hashAlg": "xxh128"}')


def test_decode_manifest_hash_not_alphanumeric():
    """
    Test that a ManifestDecodeValidationError is raised if the manifest contains non-alphanumeric hashes
    """
    invalid_hashes: list[tuple[str, str]] = [
        ("no_dots", "O.o"),
        ("no_foward_slash", "a/b"),
        ("no_back_slash", "a\\\\b"),
        ("no_tildas", "o~o"),
    ]

    for path, hash in invalid_hashes:
        with pytest.raises(ManifestDecodeValidationError, match=r".*is not alphanumeric"):
            manifest_str = (
                "{"
                '"hashAlg":"xxh128",'
                '"manifestVersion":"2023-03-03",'
                '"paths":['
                f'{{"hash":"{hash}","mtime":1679079744833848,"path":"{path}","size":1}}'
                "],"
                '"totalSize":10'
                "}"
            )
            decode.decode_manifest(manifest_str)
