# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
End-to-end tests for `deadline attachment` / `deadline manifest` /
`deadline job download-output` CLI commands.

Runs the real `deadline` CLI as a subprocess with no in-process mocks:

  * MockDeadlineBackend serves the Deadline API over HTTP
    (via AWS_ENDPOINT_URL_DEADLINE).
  * A ThreadedMotoServer serves S3 and STS
    (via AWS_ENDPOINT_URL_S3 / AWS_ENDPOINT_URL_STS).

No internet connection is required.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Iterator

import boto3
import pytest
from moto.server import ThreadedMotoServer

from deadline.job_attachments.asset_manifests.hash_algorithms import (
    HashAlgorithm,
    hash_data,
)

from ..mock_deadline_backend import MockDeadlineBackend, start_server

# Sitecustomize shim shared with test_cli_fleet_worker_subprocess.py: it
# strips the `management.` host prefix from Deadline API calls so the CLI
# can talk directly to our 127.0.0.1 mock server.
_SITECUSTOMIZE = """
import botocore.awsrequest as _ar
_orig = _ar._urljoin
def _urljoin(endpoint_url, url_path, host_prefix):
    return _orig(endpoint_url, url_path, None)
_ar._urljoin = _urljoin
"""

REGION = "us-west-2"
BUCKET = "deadline-job-attachments-e2e"
ROOT_PREFIX = "DeadlineCloud"
ACCESS_KEY = "testing"
SECRET_KEY = "testing"


# ---- fixtures ---------------------------------------------------------------


@pytest.fixture(scope="session")
def s3_server() -> Iterator[str]:
    """moto_server serving S3, running in a thread in this process."""
    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server.get_host_and_port()
    # ThreadedMotoServer binds to 0.0.0.0; clients should hit 127.0.0.1.
    url = f"http://127.0.0.1:{port}" if host == "0.0.0.0" else f"http://{host}:{port}"
    try:
        yield url
    finally:
        server.stop()


@pytest.fixture
def mock_backend() -> Iterator[tuple[MockDeadlineBackend, str]]:
    backend = MockDeadlineBackend()
    server, base_url, _ = start_server(backend)
    try:
        yield backend, base_url
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture
def deadline_setup(
    tmp_path: Path, s3_server: str, mock_backend
) -> tuple[MockDeadlineBackend, str, str, dict]:
    """
    Seed a farm+queue wired to our S3 bucket, create the bucket in moto,
    return (backend, farm_id, queue_id, subprocess_env).
    """
    backend, deadline_url = mock_backend

    # Seed farm + queue with attachment settings pointing at our S3 bucket.
    farm = backend.create_farm(displayName="Test Farm")
    farm_id = farm["farmId"]
    queue = backend.create_queue(
        farmId=farm_id,
        displayName="Test Queue",
        defaultBudgetAction="NONE",
        jobAttachmentSettings={"s3BucketName": BUCKET, "rootPrefix": ROOT_PREFIX},
    )
    queue_id = queue["queueId"]

    # Create the bucket in moto.
    s3 = boto3.client(
        "s3",
        endpoint_url=s3_server,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION,
    )
    try:
        s3.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": REGION})
    except s3.exceptions.BucketAlreadyOwnedByYou:
        pass
    # Wipe any objects left over from a prior test on this xdist worker.
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET):
        if page.get("Contents"):
            s3.delete_objects(
                Bucket=BUCKET,
                Delete={"Objects": [{"Key": o["Key"]} for o in page["Contents"]]},
            )

    config_file = tmp_path / "deadline.config"
    config_file.write_text("")
    shim_dir = tmp_path / "shim"
    shim_dir.mkdir()
    (shim_dir / "sitecustomize.py").write_text(_SITECUSTOMIZE)
    # Each test gets its own HOME so the job-attachments sqlite caches under
    # ~/.deadline/cache don't collide across xdist workers.
    fake_home = tmp_path / "home"
    fake_home.mkdir()

    env = {
        **os.environ,
        "HOME": str(fake_home),
        "AWS_ENDPOINT_URL_DEADLINE": deadline_url,
        "AWS_ENDPOINT_URL_S3": s3_server,
        "AWS_ENDPOINT_URL_STS": s3_server,
        "AWS_ACCESS_KEY_ID": ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": SECRET_KEY,
        "AWS_DEFAULT_REGION": REGION,
        "DEADLINE_CONFIG_FILE_PATH": str(config_file),
        "PYTHONPATH": str(shim_dir) + os.pathsep + os.environ.get("PYTHONPATH", ""),
    }
    return backend, farm_id, queue_id, env


def _run(env: dict, *args: str, cwd=None) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["deadline", *args],
        env=env,
        cwd=str(cwd) if cwd is not None else None,
        capture_output=True,
        text=True,
        timeout=120,
    )


def _configure_defaults(env: dict, farm_id: str, queue_id: str) -> None:
    for k, v in [
        ("defaults.farm_id", farm_id),
        ("defaults.queue_id", queue_id),
        ("telemetry.opt_out", "true"),
    ]:
        r = _run(env, "config", "set", k, v)
        assert r.returncode == 0, f"config set {k} failed: {r.stderr}"


def _write_mapping(tmp_path: Path, source: str, destination: str) -> str:
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


def _s3_client(endpoint_url: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION,
    )


# ---- helpers -----------------------------------------------------------------


def _seed_output_job(
    backend: MockDeadlineBackend,
    s3_endpoint: str,
    farm_id: str,
    queue_id: str,
    job_id: str,
    asset_root: str,
    files: dict[str, bytes],
    step_id: str = "step-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0",
    task_id: str = "task-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0-0",
) -> None:
    """Seed S3 with CAS objects + output manifest and register the job in the mock backend."""
    s3 = _s3_client(s3_endpoint)
    manifest_paths = []
    for rel_path, content in files.items():
        file_hash = hash_data(content, HashAlgorithm.XXH128)
        s3.put_object(Bucket=BUCKET, Key=f"{ROOT_PREFIX}/Data/{file_hash}.xxh128", Body=content)
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
    s3.put_object(
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


def test_cli_manifest_snapshot(deadline_setup, tmp_path):
    """`deadline manifest snapshot` produces a manifest file (no S3 needed)."""
    _, farm_id, queue_id, env = deadline_setup
    _configure_defaults(env, farm_id, queue_id)

    root = tmp_path / "assets"
    root.mkdir()
    (root / "hello.txt").write_text("hello world")
    (root / "sub").mkdir()
    (root / "sub" / "nested.txt").write_text("nested")
    dest = tmp_path / "manifests"
    dest.mkdir()

    r = _run(
        env,
        "manifest",
        "snapshot",
        "--root",
        str(root),
        "--destination",
        str(dest),
        "--name",
        "test",
    )
    assert r.returncode == 0, f"stderr={r.stderr}\nstdout={r.stdout}"
    manifests = list(dest.glob("*.manifest"))
    assert len(manifests) == 1
    paths = {p["path"] for p in json.loads(manifests[0].read_text())["paths"]}
    assert paths == {"hello.txt", "sub/nested.txt"}


def test_cli_manifest_snapshot_include_exclude(deadline_setup, tmp_path):
    """`deadline manifest snapshot` honors --include and --exclude glob filters."""
    _, farm_id, queue_id, env = deadline_setup
    _configure_defaults(env, farm_id, queue_id)

    root = tmp_path / "assets"
    root.mkdir()
    (root / "keep.txt").write_text("keep")
    (root / "drop.log").write_text("drop")
    (root / "also_keep.md").write_text("md")
    dest = tmp_path / "manifests"
    dest.mkdir()

    # Pass filters via --include/--exclude. On Windows, Click's
    # `windows_expand_args=True` (default) would expand `*` and `*.log`
    # relative to the subprocess cwd — so run from an empty directory so
    # those globs match no files and are passed through as-is.
    empty_cwd = tmp_path / "empty_cwd"
    empty_cwd.mkdir()
    r = _run(
        env,
        "manifest",
        "snapshot",
        "--root",
        str(root),
        "--destination",
        str(dest),
        "--include",
        "*",
        "--exclude",
        "*.log",
        cwd=empty_cwd,
    )
    assert r.returncode == 0, f"stderr={r.stderr}\nstdout={r.stdout}"
    manifest = next(dest.glob("*.manifest"))
    paths = {p["path"] for p in json.loads(manifest.read_text())["paths"]}
    assert paths == {"keep.txt", "also_keep.md"}


def test_cli_manifest_diff(deadline_setup, tmp_path):
    """`deadline manifest diff --json` reports new / modified / deleted files."""
    _, farm_id, queue_id, env = deadline_setup
    _configure_defaults(env, farm_id, queue_id)

    root = tmp_path / "assets"
    root.mkdir()
    (root / "unchanged.txt").write_text("same")
    (root / "will_modify.txt").write_text("v1")
    (root / "will_delete.txt").write_text("bye")
    manifests_dir = tmp_path / "manifests"
    manifests_dir.mkdir()
    r = _run(env, "manifest", "snapshot", "--root", str(root), "--destination", str(manifests_dir))
    assert r.returncode == 0, r.stderr
    original_manifest = next(manifests_dir.glob("*.manifest"))

    (root / "will_modify.txt").write_text("v2 changed")
    (root / "will_delete.txt").unlink()
    (root / "brand_new.txt").write_text("new")

    r = _run(
        env,
        "manifest",
        "diff",
        "--root",
        str(root),
        "--manifest",
        str(original_manifest),
        "--json",
    )
    assert r.returncode == 0, f"stderr={r.stderr}\nstdout={r.stdout}"
    diff = json.loads(r.stdout[r.stdout.find("{") :])
    assert diff["new"] == ["brand_new.txt"]
    assert diff["modified"] == ["will_modify.txt"]
    assert diff["deleted"] == ["will_delete.txt"]


def test_cli_manifest_upload_to_s3_cas_uri(deadline_setup, tmp_path):
    """`deadline manifest upload --s3-cas-uri` uploads the manifest to the manifest store."""
    _, farm_id, queue_id, env = deadline_setup
    _configure_defaults(env, farm_id, queue_id)

    root = tmp_path / "src"
    root.mkdir()
    (root / "one.txt").write_text("one")
    manifests = tmp_path / "manifests"
    manifests.mkdir()
    r = _run(env, "manifest", "snapshot", "--root", str(root), "--destination", str(manifests))
    assert r.returncode == 0, r.stderr
    manifest = next(manifests.glob("*.manifest"))

    r = _run(
        env,
        "manifest",
        "upload",
        str(manifest),
        "--s3-cas-uri",
        f"s3://{BUCKET}/{ROOT_PREFIX}",
    )
    assert r.returncode == 0, f"stderr={r.stderr}\nstdout={r.stdout}"

    listing = _s3_client(env["AWS_ENDPOINT_URL_S3"]).list_objects_v2(
        Bucket=BUCKET, Prefix=f"{ROOT_PREFIX}/Manifests/"
    )
    keys = [o["Key"] for o in listing.get("Contents", [])]
    expected_key = f"{ROOT_PREFIX}/Manifests/{manifest.name}"
    assert keys == [expected_key], f"expected [{expected_key}], got {keys}"

    obj = _s3_client(env["AWS_ENDPOINT_URL_S3"]).get_object(Bucket=BUCKET, Key=expected_key)
    assert obj["Body"].read() == manifest.read_bytes()
    assert obj["Metadata"]["file-system-location-name"] == str(manifest)


def test_cli_attachment_upload_download_roundtrip(deadline_setup, tmp_path):
    """snapshot -> attachment upload -> attachment download round-trip."""
    _, farm_id, queue_id, env = deadline_setup
    _configure_defaults(env, farm_id, queue_id)

    src = tmp_path / "src"
    src.mkdir()
    (src / "a.txt").write_text("alpha")
    (src / "b.txt").write_text("bravo" * 100)
    manifests = tmp_path / "manifests"
    manifests.mkdir()
    r = _run(env, "manifest", "snapshot", "--root", str(src), "--destination", str(manifests))
    assert r.returncode == 0, r.stderr
    manifest = next(manifests.glob("*.manifest"))

    r = _run(
        env,
        "attachment",
        "upload",
        "--manifests",
        str(manifest),
        "--root-dirs",
        str(src),
    )
    assert r.returncode == 0, f"upload failed: {r.stderr}\nstdout: {r.stdout}"

    s3 = _s3_client(env["AWS_ENDPOINT_URL_S3"])
    listing = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"{ROOT_PREFIX}/Data/")
    assert len(listing.get("Contents", [])) == 2

    dest = tmp_path / "out"
    dest.mkdir()
    r = _run(
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
    assert r.returncode == 0, f"download failed: {r.stderr}\nstdout: {r.stdout}"
    assert (dest / "a.txt").read_text() == "alpha"
    assert (dest / "b.txt").read_text() == "bravo" * 100


@pytest.mark.parametrize("resolution", ["OVERWRITE", "SKIP", "CREATE_COPY"])
def test_cli_attachment_download_conflict_resolution(deadline_setup, tmp_path, resolution):
    """`deadline attachment download --conflict-resolution` honors each strategy."""
    _, farm_id, queue_id, env = deadline_setup
    _configure_defaults(env, farm_id, queue_id)

    src = tmp_path / "src"
    src.mkdir()
    (src / "file.txt").write_text("from-s3")
    manifests = tmp_path / "manifests"
    manifests.mkdir()
    r = _run(env, "manifest", "snapshot", "--root", str(src), "--destination", str(manifests))
    assert r.returncode == 0, r.stderr
    manifest = next(manifests.glob("*.manifest"))

    r = _run(env, "attachment", "upload", "--manifests", str(manifest), "--root-dirs", str(src))
    assert r.returncode == 0, r.stderr

    dest = tmp_path / "dest"
    dest.mkdir()
    (dest / "file.txt").write_text("pre-existing")

    r = _run(
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
    assert r.returncode == 0, f"stderr={r.stderr}\nstdout={r.stdout}"

    if resolution == "OVERWRITE":
        assert (dest / "file.txt").read_text() == "from-s3"
    elif resolution == "SKIP":
        assert (dest / "file.txt").read_text() == "pre-existing"
    elif resolution == "CREATE_COPY":
        assert (dest / "file.txt").read_text() == "pre-existing"
        copies = [p for p in dest.iterdir() if p.name != "file.txt"]
        assert len(copies) == 1
        assert copies[0].read_text() == "from-s3"


def test_cli_manifest_download(deadline_setup, tmp_path):
    """
    `deadline manifest download` fetches a job's input manifests from S3.
    Seeds a job in the mock backend whose inputManifestPath references a
    manifest we uploaded via `deadline manifest upload`.
    """
    backend, farm_id, queue_id, env = deadline_setup
    _configure_defaults(env, farm_id, queue_id)

    # Snapshot + upload a manifest to a known prefix.
    src = tmp_path / "src"
    src.mkdir()
    (src / "only.txt").write_text("only")
    manifests_dir = tmp_path / "manifests"
    manifests_dir.mkdir()
    r = _run(env, "manifest", "snapshot", "--root", str(src), "--destination", str(manifests_dir))
    assert r.returncode == 0, r.stderr
    local_manifest = next(manifests_dir.glob("*.manifest"))

    # Use a unique prefix rooted at the job we will register below.
    job_id = "job-0123456789abcdefabcdefabcdefabcd"
    r = _run(
        env,
        "manifest",
        "upload",
        str(local_manifest),
        "--s3-cas-uri",
        f"s3://{BUCKET}/{ROOT_PREFIX}",
        "--s3-manifest-prefix",
        f"{farm_id}/{queue_id}/{job_id}",
    )
    assert r.returncode == 0, f"upload failed: {r.stderr}\nstdout: {r.stdout}"

    listing = _s3_client(env["AWS_ENDPOINT_URL_S3"]).list_objects_v2(
        Bucket=BUCKET, Prefix=f"{ROOT_PREFIX}/Manifests/{farm_id}/{queue_id}/{job_id}/"
    )
    keys = [o["Key"] for o in listing["Contents"]]
    input_manifest_path = keys[0][len(f"{ROOT_PREFIX}/Manifests/") :]

    # Seed a job in the mock backend with attachments that reference our manifest.
    backend.jobs[(farm_id, queue_id, job_id)] = {
        "jobId": job_id,
        "name": "mock-job",
        "lifecycleStatus": "CREATE_COMPLETE",
        "lifecycleStatusMessage": "",
        "priority": 50,
        "createdAt": backend._now(),
        "createdBy": "tester",
        "taskRunStatus": "READY",
        "attachments": {
            "manifests": [
                {
                    # Use a POSIX path regardless of host OS: the download
                    # code derives the local filename from this rootPath, and
                    # Windows drive-letter paths produce invalid filenames.
                    "rootPath": "/mock/root",
                    "rootPathFormat": "posix",
                    "inputManifestPath": input_manifest_path,
                    "inputManifestHash": "0",
                }
            ],
            "fileSystem": "COPIED",
        },
    }

    download_dir = tmp_path / "downloaded"
    download_dir.mkdir()
    r = _run(
        env,
        "manifest",
        "download",
        str(download_dir),
        "--job-id",
        job_id,
        "--asset-type",
        "input",
    )
    assert r.returncode == 0, f"download failed: {r.stderr}\nstdout: {r.stdout}"
    downloaded = list(download_dir.rglob("*.manifest"))
    assert downloaded, f"expected downloaded manifest, got {list(download_dir.rglob('*'))}"
    assert {p["path"] for p in json.loads(downloaded[0].read_text())["paths"]} == {"only.txt"}


def test_cli_job_download_output(deadline_setup, tmp_path):
    """
    `deadline job download-output` downloads a job's output files from S3.
    We seed S3 directly with an output manifest + CAS object (there's no
    CLI to produce output manifests; that's normally done by the worker).
    """
    backend, farm_id, queue_id, env = deadline_setup
    _configure_defaults(env, farm_id, queue_id)

    job_id = "job-fedcba9876543210fedcba9876543210"
    step_id = "step-fedcba9876543210fedcba9876543210"
    task_id = "task-fedcba9876543210fedcba9876543210-0"
    asset_root = str(tmp_path / "outputs")
    Path(asset_root).mkdir()

    # Write a CAS object + output manifest with matching `asset-root` metadata.
    content = b"rendered-output"
    file_hash = hash_data(content, HashAlgorithm.XXH128)
    s3 = _s3_client(env["AWS_ENDPOINT_URL_S3"])
    s3.put_object(Bucket=BUCKET, Key=f"{ROOT_PREFIX}/Data/{file_hash}.xxh128", Body=content)

    manifest_body = json.dumps(
        {
            "hashAlg": "xxh128",
            "manifestVersion": "2023-03-03",
            "paths": [
                {"hash": file_hash, "mtime": 1234000000, "path": "result.txt", "size": len(content)}
            ],
            "totalSize": len(content),
        }
    ).encode()
    manifest_key = (
        f"{ROOT_PREFIX}/Manifests/{farm_id}/{queue_id}/{job_id}/{step_id}/{task_id}/"
        f"sessionaction-0/outputmanifestv2023-03-03_output"
    )
    s3.put_object(
        Bucket=BUCKET,
        Key=manifest_key,
        Body=manifest_body,
        Metadata={"asset-root": asset_root},
    )

    # Seed a job pointing at that root so the CLI's download path lines up.
    backend.jobs[(farm_id, queue_id, job_id)] = {
        "jobId": job_id,
        "name": "mock-job",
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
                    # rootPathFormat must match the host OS or the CLI
                    # prompts interactively for a new path.
                    "rootPathFormat": "windows" if os.name == "nt" else "posix",
                }
            ],
            "fileSystem": "COPIED",
        },
    }

    r = _run(
        env,
        "job",
        "download-output",
        "--job-id",
        job_id,
        "--conflict-resolution",
        "OVERWRITE",
        "--yes",
    )
    assert r.returncode == 0, f"download-output failed: {r.stderr}\nstdout: {r.stdout}"
    assert (Path(asset_root) / "result.txt").read_text() == "rendered-output"


def test_cli_job_download_output_include_path(deadline_setup, tmp_path):
    """
    `deadline job download-output --include` with a glob pattern
    downloads only files matching the pattern against the full path.
    """
    backend, farm_id, queue_id, env = deadline_setup
    _configure_defaults(env, farm_id, queue_id)

    job_id = "job-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1"
    asset_root = str(tmp_path / "filtered_outputs")
    Path(asset_root).mkdir()

    files = {
        "renders/frame_001.exr": b"frame-one",
        "renders/frame_002.exr": b"frame-two",
        "logs/render.log": b"log-data",
    }
    _seed_output_job(
        backend, env["AWS_ENDPOINT_URL_S3"], farm_id, queue_id, job_id, asset_root, files
    )

    r = _run(
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


def test_cli_job_download_output_include_path_exact_file(deadline_setup, tmp_path):
    """
    `deadline job download-output --include` with an exact file glob
    downloads only that single file.
    """
    backend, farm_id, queue_id, env = deadline_setup
    _configure_defaults(env, farm_id, queue_id)

    job_id = "job-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2"
    asset_root = str(tmp_path / "exact_outputs")
    Path(asset_root).mkdir()

    files = {
        "renders/frame_001.exr": b"frame-one",
        "renders/frame_002.exr": b"frame-two",
    }
    _seed_output_job(
        backend, env["AWS_ENDPOINT_URL_S3"], farm_id, queue_id, job_id, asset_root, files
    )

    r = _run(
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


def test_cli_job_download_output_include_path_multiple(deadline_setup, tmp_path):
    """
    Multiple --include values are OR'd: files matching any filter are downloaded.
    """
    backend, farm_id, queue_id, env = deadline_setup
    _configure_defaults(env, farm_id, queue_id)

    job_id = "job-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa3"
    asset_root = str(tmp_path / "multi_outputs")
    Path(asset_root).mkdir()

    files = {
        "renders/frame_001.exr": b"frame-one",
        "logs/render.log": b"log-data",
        "scripts/setup.mel": b"mel-script",
    }
    _seed_output_job(
        backend, env["AWS_ENDPOINT_URL_S3"], farm_id, queue_id, job_id, asset_root, files
    )

    r = _run(
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


def test_cli_job_download_output_include_path_no_match(deadline_setup, tmp_path):
    """
    --include-path with a filter that matches nothing reports no output files.
    """
    backend, farm_id, queue_id, env = deadline_setup
    _configure_defaults(env, farm_id, queue_id)

    job_id = "job-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa4"
    asset_root = str(tmp_path / "nomatch_outputs")
    Path(asset_root).mkdir()

    files = {"renders/frame_001.exr": b"frame-one"}
    _seed_output_job(
        backend, env["AWS_ENDPOINT_URL_S3"], farm_id, queue_id, job_id, asset_root, files
    )

    r = _run(
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


def test_cli_job_download_output_include_matches_full_workstation_path(deadline_setup, tmp_path):
    """
    --include patterns match against the full workstation path (root + relative)
    by default, so a pattern containing part of the root directory name works.
    """
    backend, farm_id, queue_id, env = deadline_setup
    _configure_defaults(env, farm_id, queue_id)

    job_id = "job-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa5"
    asset_root = str(tmp_path / "fullpath_outputs")
    Path(asset_root).mkdir()

    files = {
        "renders/frame_001.exr": b"frame-one",
        "logs/render.log": b"log-data",
    }
    _seed_output_job(
        backend, env["AWS_ENDPOINT_URL_S3"], farm_id, queue_id, job_id, asset_root, files
    )

    # Use a pattern that includes the workstation root directory name
    r = _run(
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


def test_cli_job_download_output_submission_path_flag(deadline_setup, tmp_path):
    """
    --submission-path causes --include to filter against the original submission
    paths (asset roots from S3) rather than the workstation paths.
    """
    backend, farm_id, queue_id, env = deadline_setup
    _configure_defaults(env, farm_id, queue_id)

    job_id = "job-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa6"
    asset_root = str(tmp_path / "subpath_outputs")
    Path(asset_root).mkdir()

    files = {
        "renders/frame_001.exr": b"frame-one",
        "logs/render.log": b"log-data",
    }
    _seed_output_job(
        backend, env["AWS_ENDPOINT_URL_S3"], farm_id, queue_id, job_id, asset_root, files
    )

    # Pattern uses the submission root path (same as asset_root in this case)
    r = _run(
        env,
        "job",
        "download-output",
        "--job-id",
        job_id,
        "--include",
        "*subpath_outputs/renders/*",
        "--submission-path",
        "--conflict-resolution",
        "OVERWRITE",
        "--yes",
    )
    assert r.returncode == 0, f"download-output failed: {r.stderr}\nstdout: {r.stdout}"

    assert (Path(asset_root) / "renders" / "frame_001.exr").read_bytes() == b"frame-one"
    assert not (Path(asset_root) / "logs" / "render.log").exists()


def test_cli_job_download_output_relative_path_filter(deadline_setup, tmp_path):
    """
    --include with a plain relative path (no globs) matches as a suffix
    against the full workstation path. This is the DCM use case.
    """
    backend, farm_id, queue_id, env = deadline_setup
    _configure_defaults(env, farm_id, queue_id)

    job_id = "job-aaaaaaaaaaaaaaaaaaaaaaaaaaaaab01"
    asset_root = str(tmp_path / "relpath_outputs")
    Path(asset_root).mkdir()

    files = {
        "renders/frame_001.exr": b"frame-one",
        "renders/frame_002.exr": b"frame-two",
        "logs/render.log": b"log-data",
    }
    _seed_output_job(
        backend, env["AWS_ENDPOINT_URL_S3"], farm_id, queue_id, job_id, asset_root, files
    )

    r = _run(
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


def test_cli_job_download_output_relative_paths_via_include_config(deadline_setup, tmp_path):
    """
    --include-config with relative paths in JSON and --submission-path.
    This is the DCM integration path: relative paths from manifests passed
    as JSON, filtered against submission paths.
    """
    backend, farm_id, queue_id, env = deadline_setup
    _configure_defaults(env, farm_id, queue_id)

    job_id = "job-aaaaaaaaaaaaaaaaaaaaaaaaaaaaab02"
    asset_root = str(tmp_path / "relconfig_outputs")
    Path(asset_root).mkdir()

    files = {
        "renders/frame_001.exr": b"frame-one",
        "renders/frame_002.exr": b"frame-two",
        "logs/render.log": b"log-data",
    }
    _seed_output_job(
        backend, env["AWS_ENDPOINT_URL_S3"], farm_id, queue_id, job_id, asset_root, files
    )

    config_json = json.dumps({"include": ["renders/frame_001.exr", "logs/render.log"]})

    r = _run(
        env,
        "job",
        "download-output",
        "--job-id",
        job_id,
        "--include-config",
        config_json,
        "--submission-path",
        "--conflict-resolution",
        "OVERWRITE",
        "--yes",
    )
    assert r.returncode == 0, f"download-output failed: {r.stderr}\nstdout: {r.stdout}"

    assert (Path(asset_root) / "renders" / "frame_001.exr").read_bytes() == b"frame-one"
    assert (Path(asset_root) / "logs" / "render.log").read_bytes() == b"log-data"
    assert not (Path(asset_root) / "renders" / "frame_002.exr").exists()


def test_cli_job_download_output_glob_pattern(deadline_setup, tmp_path):
    """
    --include with glob patterns (e.g. *.exr) filters using fnmatch against full paths.
    """
    backend, farm_id, queue_id, env = deadline_setup
    _configure_defaults(env, farm_id, queue_id)

    job_id = "job-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa7"
    asset_root = str(tmp_path / "glob_outputs")
    Path(asset_root).mkdir()

    files = {
        "renders/frame_001.exr": b"frame-one",
        "renders/frame_002.png": b"frame-two-png",
        "renders/frame_003.exr": b"frame-three",
    }
    _seed_output_job(
        backend, env["AWS_ENDPOINT_URL_S3"], farm_id, queue_id, job_id, asset_root, files
    )

    r = _run(
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


def test_cli_job_download_output_include_config(deadline_setup, tmp_path):
    """
    --include-config accepts a JSON file with include patterns.
    """
    backend, farm_id, queue_id, env = deadline_setup
    _configure_defaults(env, farm_id, queue_id)

    job_id = "job-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa8"
    asset_root = str(tmp_path / "config_outputs")
    Path(asset_root).mkdir()

    files = {
        "renders/frame_001.exr": b"frame-one",
        "renders/draft/frame_002.exr": b"draft",
        "logs/render.log": b"log-data",
    }
    _seed_output_job(
        backend, env["AWS_ENDPOINT_URL_S3"], farm_id, queue_id, job_id, asset_root, files
    )

    config_file = tmp_path / "filters.json"
    config_file.write_text(json.dumps({"include": ["*/renders/*"]}))

    r = _run(
        env,
        "job",
        "download-output",
        "--job-id",
        job_id,
        "--include-config",
        str(config_file),
        "--submission-path",
        "--conflict-resolution",
        "OVERWRITE",
        "--yes",
    )
    assert r.returncode == 0, f"download-output failed: {r.stderr}\nstdout: {r.stdout}"

    assert (Path(asset_root) / "renders" / "frame_001.exr").read_bytes() == b"frame-one"
    # renders/draft/frame_002.exr matches */renders/* via fnmatch (single * matches path segments)
    assert (Path(asset_root) / "renders" / "draft" / "frame_002.exr").read_bytes() == b"draft"
    assert not (Path(asset_root) / "logs" / "render.log").exists()


def test_cli_job_download_output_include_config_large_inline_json(deadline_setup, tmp_path):
    """
    --include-config with a large inline JSON blob containing many paths.
    This mirrors DCM's usage where the desktop app may pass hundreds of file paths.
    """
    backend, farm_id, queue_id, env = deadline_setup
    _configure_defaults(env, farm_id, queue_id)

    job_id = "job-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa9"
    asset_root = str(tmp_path / "large_config_outputs")
    Path(asset_root).mkdir()

    # Generate 200 files, select 150 of them via include config
    num_total = 200
    num_selected = 150
    files = {f"renders/frame_{i:04d}.exr": f"content-{i}".encode() for i in range(num_total)}
    _seed_output_job(
        backend, env["AWS_ENDPOINT_URL_S3"], farm_id, queue_id, job_id, asset_root, files
    )

    # Build a large inline JSON with 150 exact paths (using glob to match full paths)
    selected_paths = [f"*/renders/frame_{i:04d}.exr" for i in range(num_selected)]
    config_json = json.dumps({"include": selected_paths})

    r = _run(
        env,
        "job",
        "download-output",
        "--job-id",
        job_id,
        "--include-config",
        config_json,
        "--submission-path",
        "--conflict-resolution",
        "OVERWRITE",
        "--yes",
    )
    assert r.returncode == 0, f"download-output failed: {r.stderr}\nstdout: {r.stdout}"

    # Verify exactly the 50 selected files were downloaded
    for i in range(num_selected):
        assert (
            Path(asset_root) / "renders" / f"frame_{i:04d}.exr"
        ).read_bytes() == f"content-{i}".encode()

    # Verify the rest were NOT downloaded
    for i in range(num_selected, num_total):
        assert not (Path(asset_root) / "renders" / f"frame_{i:04d}.exr").exists()


@pytest.mark.skipif(
    sys.version_info < (3, 9),
    reason="MockDeadlineBackend.create_job requires openjd-model, which requires Python >= 3.9",
)
def test_cli_bundle_submit_with_job_attachments(deadline_setup, tmp_path):
    """
    `deadline bundle submit` uploads the bundle's input files as job
    attachments to S3 and creates a job on the farm's queue.
    """
    backend, farm_id, queue_id, env = deadline_setup
    _configure_defaults(env, farm_id, queue_id)

    # Copy the existing test bundle so relative input paths resolve locally.
    bundle_src = Path(__file__).parent / "test_data" / "job_bundle_with_data"
    bundle_dst = tmp_path / "bundle"
    shutil.copytree(bundle_src, bundle_dst)

    r = _run(env, "bundle", "submit", str(bundle_dst), "--yes")
    assert r.returncode == 0, f"submit failed: {r.stderr}\nstdout: {r.stdout}"

    # A job was registered in the mock backend.
    jobs = [j for (f, q, _), j in backend.jobs.items() if f == farm_id and q == queue_id]
    assert len(jobs) == 1
    assert "attachments" in jobs[0], f"expected job to carry attachments, got {jobs[0]}"

    # Attachments were uploaded to the CAS. The bundle's three input files all
    # have unique contents, so we expect three CAS objects.
    listing = _s3_client(env["AWS_ENDPOINT_URL_S3"]).list_objects_v2(
        Bucket=BUCKET, Prefix=f"{ROOT_PREFIX}/Data/"
    )
    keys = [o["Key"] for o in listing.get("Contents", [])]
    assert len(keys) == 3, f"expected 3 input files uploaded to CAS, got {keys}"

    # And an input manifest was written to the Manifests/ store.
    listing = _s3_client(env["AWS_ENDPOINT_URL_S3"]).list_objects_v2(
        Bucket=BUCKET, Prefix=f"{ROOT_PREFIX}/Manifests/"
    )
    manifest_keys = [o["Key"] for o in listing.get("Contents", [])]
    assert manifest_keys, f"expected an input manifest to be uploaded, got {manifest_keys}"
