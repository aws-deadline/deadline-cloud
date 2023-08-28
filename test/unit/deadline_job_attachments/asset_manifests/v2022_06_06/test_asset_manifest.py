# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

""" Tests for the v2022-06-06 version of the manifest file. """
import json

from deadline.job_attachments.asset_manifests.v2022_06_06.asset_manifest import AssetManifest, Path


def test_encode():
    """
    Ensure the expected JSON string is returned from the encode function.
    """
    manifest = AssetManifest(
        hash_alg="xxh128",
        paths=[
            Path(path="test_file", hash="a"),
            Path(path="test_dir/test_file", hash="b"),
            Path(path="another_test_file", hash="c"),
            Path(path="€", hash="Euro Sign"),
            Path(path="\r", hash="Carriage Return"),
            Path(path="דּ", hash="Hebrew Letter Dalet With Dagesh"),
            Path(path="1", hash="One"),
            Path(path="😀", hash="Emoji: Grinning Face"),
            Path(path="\u0080", hash="Control"),
            Path(path="ö", hash="Latin Small Letter O With Diaeresis"),
        ],
    )

    expected = (
        "{"
        '"hashAlg":"xxh128",'
        '"manifestVersion":"2022-06-06",'
        '"paths":['
        r'{"hash":"Carriage Return","path":"\r"},'
        '{"hash":"One","path":"1"},'
        '{"hash":"c","path":"another_test_file"},'
        '{"hash":"b","path":"test_dir/test_file"},'
        '{"hash":"a","path":"test_file"},'
        r'{"hash":"Control","path":"\u0080"},'
        r'{"hash":"Latin Small Letter O With Diaeresis","path":"\u00f6"},'
        r'{"hash":"Euro Sign","path":"\u20ac"},'
        r'{"hash":"Emoji: Grinning Face","path":"\ud83d\ude00"},'
        r'{"hash":"Hebrew Letter Dalet With Dagesh","path":"\ufb33"}'
        "]"
        "}"
    )

    assert manifest.encode() == expected


def test_decode(default_manifest_str_v2022_06_06: str):
    """
    Ensure the expected AssetManifest is returned from the decode function.
    """
    expected = AssetManifest(
        hash_alg="xxh128",
        paths=[
            Path(path="\r", hash="Carriage Return"),
            Path(path="1", hash="One"),
            Path(path="another_test_file", hash="c"),
            Path(path="test_dir/test_file", hash="b"),
            Path(path="test_file", hash="a"),
            Path(path="\u0080", hash="Control"),
            Path(path="ö", hash="Latin Small Letter O With Diaeresis"),
            Path(path="€", hash="Euro Sign"),
            Path(path="😀", hash="Emoji: Grinning Face"),
            Path(path="דּ", hash="Hebrew Letter Dalet With Dagesh"),
        ],
    )
    assert (
        AssetManifest.decode(manifest_data=json.loads(default_manifest_str_v2022_06_06)) == expected
    )
