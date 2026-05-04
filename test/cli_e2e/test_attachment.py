# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""End-to-end tests for `deadline attachment` subcommands."""

from __future__ import annotations

import json

import pytest


def _write_mapping(tmp_path, source: str, destination: str) -> str:
    path = tmp_path / "mapping.json"
    path.write_text(
        json.dumps(
            [
                {
                    "source_path_format": "posix",
                    "source_path": source,
                    "destination_path": destination,
                }
            ]
        )
    )
    return str(path)


def test_cli_attachment_upload_download_roundtrip(seeded_farm_queue, run_cli, s3_client, tmp_path):
    from _constants import BUCKET, ROOT_PREFIX

    _, _, _, env = seeded_farm_queue
    src = tmp_path / "src"
    src.mkdir()
    (src / "a.txt").write_text("alpha")
    (src / "b.txt").write_text("bravo" * 100)
    manifests = tmp_path / "manifests"
    manifests.mkdir()
    r = run_cli(env, "manifest", "snapshot", "--root", str(src), "--destination", str(manifests))
    assert r.returncode == 0, r.stderr
    manifest = next(manifests.glob("*.manifest"))

    r = run_cli(
        env,
        "attachment",
        "upload",
        "--manifests",
        str(manifest),
        "--root-dirs",
        str(src),
    )
    assert r.returncode == 0, r.stderr or r.stdout

    listing = s3_client.list_objects_v2(Bucket=BUCKET, Prefix=f"{ROOT_PREFIX}/Data/")
    assert len(listing.get("Contents", [])) == 2

    dest = tmp_path / "out"
    dest.mkdir()
    r = run_cli(
        env,
        "attachment",
        "download",
        "--manifests",
        str(manifest),
        "--path-mapping-rules",
        _write_mapping(tmp_path, str(src), str(dest)),
        "--conflict-resolution",
        "OVERWRITE",
    )
    assert r.returncode == 0, r.stderr or r.stdout
    assert (dest / "a.txt").read_text() == "alpha"
    assert (dest / "b.txt").read_text() == "bravo" * 100


@pytest.mark.parametrize("resolution", ["OVERWRITE", "SKIP", "CREATE_COPY"])
def test_cli_attachment_download_conflict_resolution(
    seeded_farm_queue, run_cli, tmp_path, resolution
):
    _, _, _, env = seeded_farm_queue
    src = tmp_path / "src"
    src.mkdir()
    (src / "file.txt").write_text("from-s3")
    manifests = tmp_path / "manifests"
    manifests.mkdir()
    r = run_cli(env, "manifest", "snapshot", "--root", str(src), "--destination", str(manifests))
    assert r.returncode == 0, r.stderr
    manifest = next(manifests.glob("*.manifest"))

    r = run_cli(env, "attachment", "upload", "--manifests", str(manifest), "--root-dirs", str(src))
    assert r.returncode == 0, r.stderr or r.stdout

    dest = tmp_path / "dest"
    dest.mkdir()
    (dest / "file.txt").write_text("pre-existing")

    r = run_cli(
        env,
        "attachment",
        "download",
        "--manifests",
        str(manifest),
        "--path-mapping-rules",
        _write_mapping(tmp_path, str(src), str(dest)),
        "--conflict-resolution",
        resolution,
    )
    assert r.returncode == 0, r.stderr or r.stdout

    if resolution == "OVERWRITE":
        assert (dest / "file.txt").read_text() == "from-s3"
    elif resolution == "SKIP":
        assert (dest / "file.txt").read_text() == "pre-existing"
    else:  # CREATE_COPY
        assert (dest / "file.txt").read_text() == "pre-existing"
        copies = [p for p in dest.iterdir() if p.name != "file.txt"]
        assert len(copies) == 1
        assert copies[0].read_text() == "from-s3"


def test_cli_attachment_upload_missing_manifest_fails(seeded_farm_queue, run_cli, tmp_path):
    _, _, _, env = seeded_farm_queue
    r = run_cli(
        env,
        "attachment",
        "upload",
        "--manifests",
        str(tmp_path / "no-such.manifest"),
        "--root-dirs",
        str(tmp_path),
    )
    assert r.returncode != 0


def test_cli_attachment_download_requires_manifests(seeded_farm_queue, run_cli):
    _, _, _, env = seeded_farm_queue
    r = run_cli(env, "attachment", "download")
    assert r.returncode != 0


def test_cli_attachment_help(deadline_env, run_cli):
    _, env = deadline_env
    r = run_cli(env, "attachment", "--help")
    assert r.returncode == 0
    for sub in ("upload", "download"):
        assert sub in r.stdout
