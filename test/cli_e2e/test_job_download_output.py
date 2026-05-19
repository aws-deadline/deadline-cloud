# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""End-to-end tests for `deadline job download-output` and `queue sync-output`."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

from deadline.job_attachments.asset_manifests.hash_algorithms import HashAlgorithm, hash_data

from _constants import BUCKET, ROOT_PREFIX

# `deadline queue sync-output` requires Python >= 3.9 at the CLI level.
requires_py39 = pytest.mark.skipif(
    sys.version_info < (3, 9),
    reason="`deadline queue sync-output` itself requires Python >= 3.9",
)


# ---- helpers -----------------------------------------------------------------


def _seed_output_job(
    backend,
    s3_client,
    farm_id: str,
    queue_id: str,
    job_id: str,
    asset_root: str,
    files: dict[str, bytes],
    step_id: str = "step-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0",
    task_id: str = "task-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0-0",
) -> None:
    """Seed S3 with CAS objects + output manifest and register the job in the mock backend."""
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
    manifest_key = (
        f"{ROOT_PREFIX}/Manifests/{farm_id}/{queue_id}/{job_id}/{step_id}/{task_id}/"
        f"sessionaction-0/outputmanifestv2023-03-03_output"
    )
    s3_client.put_object(
        Bucket=BUCKET, Key=manifest_key, Body=manifest_body, Metadata={"asset-root": asset_root}
    )

    backend.jobs[(farm_id, queue_id, job_id)] = {
        "jobId": job_id,
        "name": f"test-job-{job_id[-4:]}",
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
                }
            ],
            "fileSystem": "COPIED",
        },
    }


# ---- tests ------------------------------------------------------------------


def test_cli_job_download_output(seeded_farm_queue, run_cli, s3_client, tmp_path):
    backend, farm_id, queue_id, env = seeded_farm_queue
    job_id = "job-fedcba9876543210fedcba9876543210"
    asset_root = str(tmp_path / "outputs")
    Path(asset_root).mkdir()

    _seed_output_job(
        backend,
        s3_client,
        farm_id,
        queue_id,
        job_id,
        asset_root,
        {"result.txt": b"rendered-output"},
        step_id="step-fedcba9876543210fedcba9876543210",
        task_id="task-fedcba9876543210fedcba9876543210-0",
    )

    r = run_cli(
        env,
        "job",
        "download-output",
        "--job-id",
        job_id,
        "--conflict-resolution",
        "OVERWRITE",
        "--yes",
    )
    assert r.returncode == 0, r.stderr or r.stdout
    assert (Path(asset_root) / "result.txt").read_text() == "rendered-output"


def test_cli_job_download_output_include_path(seeded_farm_queue, run_cli, s3_client, tmp_path):
    """`--include` with a glob pattern downloads only matching files."""
    backend, farm_id, queue_id, env = seeded_farm_queue
    job_id = "job-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1"
    asset_root = str(tmp_path / "filtered_outputs")
    Path(asset_root).mkdir()

    _seed_output_job(
        backend,
        s3_client,
        farm_id,
        queue_id,
        job_id,
        asset_root,
        {
            "renders/frame_001.exr": b"frame-one",
            "renders/frame_002.exr": b"frame-two",
            "logs/render.log": b"log-data",
        },
    )

    r = run_cli(
        env,
        "job",
        "download-output",
        "--job-id",
        job_id,
        "--include",
        "*/renders/*",
        "--conflict-resolution",
        "OVERWRITE",
        "--yes",
    )
    assert r.returncode == 0, f"download-output failed: {r.stderr}\nstdout: {r.stdout}"
    assert (Path(asset_root) / "renders" / "frame_001.exr").read_bytes() == b"frame-one"
    assert (Path(asset_root) / "renders" / "frame_002.exr").read_bytes() == b"frame-two"
    assert not (Path(asset_root) / "logs" / "render.log").exists()


def test_cli_job_download_output_include_path_exact_file(
    seeded_farm_queue, run_cli, s3_client, tmp_path
):
    """`--include` with an exact file glob downloads only that single file."""
    backend, farm_id, queue_id, env = seeded_farm_queue
    job_id = "job-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2"
    asset_root = str(tmp_path / "exact_outputs")
    Path(asset_root).mkdir()

    _seed_output_job(
        backend,
        s3_client,
        farm_id,
        queue_id,
        job_id,
        asset_root,
        {"renders/frame_001.exr": b"frame-one", "renders/frame_002.exr": b"frame-two"},
    )

    r = run_cli(
        env,
        "job",
        "download-output",
        "--job-id",
        job_id,
        "--include",
        "*/frame_001.exr",
        "--conflict-resolution",
        "OVERWRITE",
        "--yes",
    )
    assert r.returncode == 0, f"download-output failed: {r.stderr}\nstdout: {r.stdout}"
    assert (Path(asset_root) / "renders" / "frame_001.exr").read_bytes() == b"frame-one"
    assert not (Path(asset_root) / "renders" / "frame_002.exr").exists()


def test_cli_job_download_output_include_path_multiple(
    seeded_farm_queue, run_cli, s3_client, tmp_path
):
    """Multiple --include values are OR'd: files matching any filter are downloaded."""
    backend, farm_id, queue_id, env = seeded_farm_queue
    job_id = "job-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa3"
    asset_root = str(tmp_path / "multi_outputs")
    Path(asset_root).mkdir()

    _seed_output_job(
        backend,
        s3_client,
        farm_id,
        queue_id,
        job_id,
        asset_root,
        {
            "renders/frame_001.exr": b"frame-one",
            "logs/render.log": b"log-data",
            "scripts/setup.mel": b"mel-script",
        },
    )

    r = run_cli(
        env,
        "job",
        "download-output",
        "--job-id",
        job_id,
        "--include",
        "*/frame_001.exr",
        "--include",
        "*/scripts/*",
        "--conflict-resolution",
        "OVERWRITE",
        "--yes",
    )
    assert r.returncode == 0, f"download-output failed: {r.stderr}\nstdout: {r.stdout}"
    assert (Path(asset_root) / "renders" / "frame_001.exr").read_bytes() == b"frame-one"
    assert (Path(asset_root) / "scripts" / "setup.mel").read_bytes() == b"mel-script"
    assert not (Path(asset_root) / "logs" / "render.log").exists()


def test_cli_job_download_output_include_path_no_match(
    seeded_farm_queue, run_cli, s3_client, tmp_path
):
    """--include with a filter that matches nothing reports no output files."""
    backend, farm_id, queue_id, env = seeded_farm_queue
    job_id = "job-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa4"
    asset_root = str(tmp_path / "nomatch_outputs")
    Path(asset_root).mkdir()

    _seed_output_job(
        backend,
        s3_client,
        farm_id,
        queue_id,
        job_id,
        asset_root,
        {"renders/frame_001.exr": b"frame-one"},
    )

    r = run_cli(
        env,
        "job",
        "download-output",
        "--job-id",
        job_id,
        "--include",
        "nonexistent.txt",
        "--conflict-resolution",
        "OVERWRITE",
        "--yes",
    )
    assert r.returncode == 0, f"download-output failed: {r.stderr}\nstdout: {r.stdout}"
    assert "no output files available" in r.stdout.lower()
    assert not (Path(asset_root) / "renders" / "frame_001.exr").exists()


def test_cli_job_download_output_include_matches_full_workstation_path(
    seeded_farm_queue, run_cli, s3_client, tmp_path
):
    """--include patterns match against the full workstation path by default."""
    backend, farm_id, queue_id, env = seeded_farm_queue
    job_id = "job-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa5"
    asset_root = str(tmp_path / "fullpath_outputs")
    Path(asset_root).mkdir()

    _seed_output_job(
        backend,
        s3_client,
        farm_id,
        queue_id,
        job_id,
        asset_root,
        {"renders/frame_001.exr": b"frame-one", "logs/render.log": b"log-data"},
    )

    r = run_cli(
        env,
        "job",
        "download-output",
        "--job-id",
        job_id,
        "--include",
        "*fullpath_outputs/renders/*",
        "--conflict-resolution",
        "OVERWRITE",
        "--yes",
    )
    assert r.returncode == 0, f"download-output failed: {r.stderr}\nstdout: {r.stdout}"
    assert (Path(asset_root) / "renders" / "frame_001.exr").read_bytes() == b"frame-one"
    assert not (Path(asset_root) / "logs" / "render.log").exists()


def test_cli_job_download_output_match_paths_by_job_flag(
    seeded_farm_queue, run_cli, s3_client, tmp_path
):
    """--match-paths-by JOB causes --include to filter against original source paths."""
    backend, farm_id, queue_id, env = seeded_farm_queue
    job_id = "job-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa6"
    asset_root = str(tmp_path / "subpath_outputs")
    Path(asset_root).mkdir()

    _seed_output_job(
        backend,
        s3_client,
        farm_id,
        queue_id,
        job_id,
        asset_root,
        {"renders/frame_001.exr": b"frame-one", "logs/render.log": b"log-data"},
    )

    r = run_cli(
        env,
        "job",
        "download-output",
        "--job-id",
        job_id,
        "--include",
        "*subpath_outputs/renders/*",
        "--match-paths-by",
        "JOB",
        "--conflict-resolution",
        "OVERWRITE",
        "--yes",
    )
    assert r.returncode == 0, f"download-output failed: {r.stderr}\nstdout: {r.stdout}"
    assert (Path(asset_root) / "renders" / "frame_001.exr").read_bytes() == b"frame-one"
    assert not (Path(asset_root) / "logs" / "render.log").exists()


def test_cli_job_download_output_relative_path_filter(
    seeded_farm_queue, run_cli, s3_client, tmp_path
):
    """--include with a plain relative path matches as a suffix (DCM use case)."""
    backend, farm_id, queue_id, env = seeded_farm_queue
    job_id = "job-aaaaaaaaaaaaaaaaaaaaaaaaaaaaab01"
    asset_root = str(tmp_path / "relpath_outputs")
    Path(asset_root).mkdir()

    _seed_output_job(
        backend,
        s3_client,
        farm_id,
        queue_id,
        job_id,
        asset_root,
        {
            "renders/frame_001.exr": b"frame-one",
            "renders/frame_002.exr": b"frame-two",
            "logs/render.log": b"log-data",
        },
    )

    r = run_cli(
        env,
        "job",
        "download-output",
        "--job-id",
        job_id,
        "--include",
        "renders/frame_001.exr",
        "--conflict-resolution",
        "OVERWRITE",
        "--yes",
    )
    assert r.returncode == 0, f"download-output failed: {r.stderr}\nstdout: {r.stdout}"
    assert (Path(asset_root) / "renders" / "frame_001.exr").read_bytes() == b"frame-one"
    assert not (Path(asset_root) / "renders" / "frame_002.exr").exists()
    assert not (Path(asset_root) / "logs" / "render.log").exists()


def test_cli_job_download_output_relative_paths_with_match_paths_by_job(
    seeded_farm_queue, run_cli, s3_client, tmp_path
):
    """--include with relative paths and --match-paths-by JOB filters against job paths."""
    backend, farm_id, queue_id, env = seeded_farm_queue
    job_id = "job-aaaaaaaaaaaaaaaaaaaaaaaaaaaaab02"
    asset_root = str(tmp_path / "relsubmit_outputs")
    Path(asset_root).mkdir()

    _seed_output_job(
        backend,
        s3_client,
        farm_id,
        queue_id,
        job_id,
        asset_root,
        {
            "renders/frame_001.exr": b"frame-one",
            "renders/frame_002.exr": b"frame-two",
            "logs/render.log": b"log-data",
        },
    )

    r = run_cli(
        env,
        "job",
        "download-output",
        "--job-id",
        job_id,
        "--include",
        "renders/frame_001.exr",
        "--include",
        "logs/render.log",
        "--match-paths-by",
        "JOB",
        "--conflict-resolution",
        "OVERWRITE",
        "--yes",
    )
    assert r.returncode == 0, f"download-output failed: {r.stderr}\nstdout: {r.stdout}"
    assert (Path(asset_root) / "renders" / "frame_001.exr").read_bytes() == b"frame-one"
    assert (Path(asset_root) / "logs" / "render.log").read_bytes() == b"log-data"
    assert not (Path(asset_root) / "renders" / "frame_002.exr").exists()


def test_cli_job_download_output_glob_pattern(seeded_farm_queue, run_cli, s3_client, tmp_path):
    """--include with glob patterns (e.g. *.exr) filters using fnmatch against full paths."""
    backend, farm_id, queue_id, env = seeded_farm_queue
    job_id = "job-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa7"
    asset_root = str(tmp_path / "glob_outputs")
    Path(asset_root).mkdir()

    _seed_output_job(
        backend,
        s3_client,
        farm_id,
        queue_id,
        job_id,
        asset_root,
        {
            "renders/frame_001.exr": b"frame-one",
            "renders/frame_002.png": b"frame-two-png",
            "renders/frame_003.exr": b"frame-three",
        },
    )

    r = run_cli(
        env,
        "job",
        "download-output",
        "--job-id",
        job_id,
        "--include",
        "*.exr",
        "--conflict-resolution",
        "OVERWRITE",
        "--yes",
    )
    assert r.returncode == 0, f"download-output failed: {r.stderr}\nstdout: {r.stdout}"
    assert (Path(asset_root) / "renders" / "frame_001.exr").read_bytes() == b"frame-one"
    assert (Path(asset_root) / "renders" / "frame_003.exr").read_bytes() == b"frame-three"
    assert not (Path(asset_root) / "renders" / "frame_002.png").exists()


# ---- queue sync-output tests ------------------------------------------------


@requires_py39
def test_cli_queue_sync_output_requires_storage_profile(seeded_farm_queue, run_cli):
    _, _, _, env = seeded_farm_queue
    r = run_cli(env, "queue", "sync-output")
    # With no --storage-profile-id and no --ignore-storage-profiles, the command errors out.
    assert r.returncode != 0
    combined = r.stdout + r.stderr
    assert "storage profile" in combined.lower()


@requires_py39
def test_cli_queue_sync_output_ignore_storage_profiles(seeded_farm_queue, run_cli, tmp_path):
    _, _, _, env = seeded_farm_queue
    checkpoint_dir = tmp_path / "checkpoint"
    r = run_cli(
        env,
        "queue",
        "sync-output",
        "--ignore-storage-profiles",
        "--checkpoint-dir",
        str(checkpoint_dir),
        "--dry-run",
    )
    # No jobs to process; the command exits cleanly.
    assert r.returncode == 0, r.stderr or r.stdout
