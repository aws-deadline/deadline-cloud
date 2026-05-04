# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""End-to-end tests for `deadline job download-input`."""

from __future__ import annotations

import json
import os
from pathlib import Path

from deadline.job_attachments.asset_manifests.hash_algorithms import HashAlgorithm, hash_data

from _constants import BUCKET, ROOT_PREFIX


# ---- helpers -----------------------------------------------------------------


def _seed_input_job(
    backend,
    s3_client,
    farm_id: str,
    queue_id: str,
    job_id: str,
    asset_root: str,
    files: dict[str, bytes],
) -> None:
    """Seed S3 with CAS objects + input manifest and register the job in the mock backend."""
    manifest_paths = []
    for rel_path, content in files.items():
        file_hash = hash_data(content, HashAlgorithm.XXH128)
        s3_client.put_object(
            Bucket=BUCKET, Key=f"{ROOT_PREFIX}/Data/{file_hash}.xxh128", Body=content
        )
        manifest_paths.append(
            {"hash": file_hash, "mtime": 1234000000, "path": rel_path, "size": len(content)}
        )

    manifest_body = json.dumps(
        {
            "hashAlg": "xxh128",
            "manifestVersion": "2023-03-03",
            "paths": manifest_paths,
            "totalSize": sum(len(c) for c in files.values()),
        }
    ).encode()
    manifest_hash = hash_data(manifest_body, HashAlgorithm.XXH128)
    manifest_key = f"{ROOT_PREFIX}/Manifests/{farm_id}/{queue_id}/Inputs/{manifest_hash}.manifest"
    s3_client.put_object(Bucket=BUCKET, Key=manifest_key, Body=manifest_body)

    backend.jobs[(farm_id, queue_id, job_id)] = {
        "jobId": job_id,
        "name": f"test-input-job-{job_id[-4:]}",
        "lifecycleStatus": "CREATE_COMPLETE",
        "lifecycleStatusMessage": "",
        "priority": 50,
        "createdAt": backend._now(),
        "createdBy": "tester",
        "taskRunStatus": "READY",
        "attachments": {
            "manifests": [
                {
                    "rootPath": asset_root,
                    "rootPathFormat": "windows" if os.name == "nt" else "posix",
                    "inputManifestPath": f"{farm_id}/{queue_id}/Inputs/{manifest_hash}.manifest",
                    "inputManifestHash": manifest_hash,
                }
            ],
            "fileSystem": "COPIED",
        },
    }


# ---- tests ------------------------------------------------------------------


def test_cli_job_download_input(seeded_farm_queue, run_cli, s3_client, tmp_path):
    """Basic download-input downloads all input files."""
    backend, farm_id, queue_id, env = seeded_farm_queue
    job_id = "job-11111111111111111111111111111110"
    asset_root = str(tmp_path / "inputs")
    Path(asset_root).mkdir()

    _seed_input_job(
        backend,
        s3_client,
        farm_id,
        queue_id,
        job_id,
        asset_root,
        {"scene.ma": b"maya-scene-data", "textures/brick.png": b"brick-texture"},
    )

    r = run_cli(
        env,
        "job",
        "download-input",
        "--job-id",
        job_id,
        "--conflict-resolution",
        "OVERWRITE",
        "--yes",
    )
    assert r.returncode == 0, f"download-input failed: {r.stderr}\nstdout: {r.stdout}"
    assert (Path(asset_root) / "scene.ma").read_bytes() == b"maya-scene-data"
    assert (Path(asset_root) / "textures" / "brick.png").read_bytes() == b"brick-texture"


def test_cli_job_download_input_include_glob(seeded_farm_queue, run_cli, s3_client, tmp_path):
    """--include with a glob pattern downloads only matching input files."""
    backend, farm_id, queue_id, env = seeded_farm_queue
    job_id = "job-11111111111111111111111111111111"
    asset_root = str(tmp_path / "filtered_inputs")
    Path(asset_root).mkdir()

    _seed_input_job(
        backend,
        s3_client,
        farm_id,
        queue_id,
        job_id,
        asset_root,
        {"scene.ma": b"maya-scene", "textures/brick.png": b"brick", "textures/cloth.png": b"cloth"},
    )

    r = run_cli(
        env,
        "job",
        "download-input",
        "--job-id",
        job_id,
        "--include",
        "*.png",
        "--conflict-resolution",
        "OVERWRITE",
        "--yes",
    )
    assert r.returncode == 0, f"download-input failed: {r.stderr}\nstdout: {r.stdout}"
    assert (Path(asset_root) / "textures" / "brick.png").read_bytes() == b"brick"
    assert (Path(asset_root) / "textures" / "cloth.png").read_bytes() == b"cloth"
    assert not (Path(asset_root) / "scene.ma").exists()


def test_cli_job_download_input_include_no_match(seeded_farm_queue, run_cli, s3_client, tmp_path):
    """--include with a filter that matches nothing reports no input files."""
    backend, farm_id, queue_id, env = seeded_farm_queue
    job_id = "job-11111111111111111111111111111112"
    asset_root = str(tmp_path / "nomatch_inputs")
    Path(asset_root).mkdir()

    _seed_input_job(
        backend,
        s3_client,
        farm_id,
        queue_id,
        job_id,
        asset_root,
        {"scene.ma": b"maya-scene"},
    )

    r = run_cli(
        env,
        "job",
        "download-input",
        "--job-id",
        job_id,
        "--include",
        "*.nonexistent",
        "--conflict-resolution",
        "OVERWRITE",
        "--yes",
    )
    assert r.returncode == 0, f"download-input failed: {r.stderr}\nstdout: {r.stdout}"
    assert "no input files" in r.stdout.lower()


def test_cli_job_download_input_no_attachments(seeded_farm_queue, run_cli):
    """download-input on a job with no attachments reports a clear message."""
    backend, farm_id, queue_id, env = seeded_farm_queue
    job_id = "job-11111111111111111111111111111113"

    backend.jobs[(farm_id, queue_id, job_id)] = {
        "jobId": job_id,
        "name": "no-attachments-job",
        "lifecycleStatus": "CREATE_COMPLETE",
        "lifecycleStatusMessage": "",
        "priority": 50,
        "createdAt": backend._now(),
        "createdBy": "tester",
        "taskRunStatus": "READY",
    }

    r = run_cli(
        env,
        "job",
        "download-input",
        "--job-id",
        job_id,
        "--yes",
    )
    assert r.returncode == 0, f"download-input failed: {r.stderr}\nstdout: {r.stdout}"
    assert "no input attachments" in r.stdout.lower()
