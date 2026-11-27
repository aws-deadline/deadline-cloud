# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""End-to-end tests for `deadline manifest` subcommands."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterator

import botocore.session
import pytest

from _common.mock_deadline_backend import MockDeadlineBackend, start_server
from _constants import BUCKET, REGION, ROOT_PREFIX


@pytest.fixture
def queue_with_new_settings_field(
    deadline_env, s3_client, tmp_path, monkeypatch
) -> Iterator[tuple[MockDeadlineBackend, str, str, dict]]:
    """
    A queue whose job attachment settings carry a field this client doesn't know about, served
    with a Deadline service model that includes the field, as a newer boto3 would.
    """
    _, env = deadline_env
    loader = botocore.session.get_session().get_component("data_loader")
    api_version = loader.list_api_versions("deadline", "service-2")[-1]
    model = loader.load_service_model("deadline", "service-2", api_version)
    model["shapes"]["JobAttachmentSettings"]["members"]["someNewField"] = {"shape": "String"}
    model_dir = tmp_path / "models" / "deadline" / api_version
    model_dir.mkdir(parents=True)
    (model_dir / "service-2.json").write_text(json.dumps(model))
    # The mock server drops response fields missing from the model loaded in this process, and
    # the CLI subprocess parses responses with the model from its environment: both need the field.
    monkeypatch.setenv("AWS_DATA_PATH", str(tmp_path / "models"))
    env["AWS_DATA_PATH"] = str(tmp_path / "models")

    backend = MockDeadlineBackend()
    server, base_url, _ = start_server(backend)
    env["AWS_ENDPOINT_URL_DEADLINE"] = base_url
    try:
        farm_id = backend.create_farm(displayName="Test Farm")["farmId"]
        queue_id = backend.create_queue(
            farmId=farm_id,
            displayName="Test Queue",
            defaultBudgetAction="NONE",
            jobAttachmentSettings={
                "s3BucketName": BUCKET,
                "rootPrefix": ROOT_PREFIX,
                "someNewField": "some value",
            },
        )["queueId"]
        try:
            s3_client.create_bucket(
                Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": REGION}
            )
        except s3_client.exceptions.BucketAlreadyOwnedByYou:
            pass
        for page in s3_client.get_paginator("list_objects_v2").paginate(Bucket=BUCKET):
            if page.get("Contents"):
                s3_client.delete_objects(
                    Bucket=BUCKET,
                    Delete={"Objects": [{"Key": o["Key"]} for o in page["Contents"]]},
                )
        yield backend, farm_id, queue_id, env
    finally:
        server.shutdown()
        server.server_close()


def test_cli_manifest_snapshot(seeded_farm_queue, run_cli, tmp_path):
    _, _, _, env = seeded_farm_queue
    root = tmp_path / "assets"
    root.mkdir()
    (root / "hello.txt").write_text("hello world")
    (root / "sub").mkdir()
    (root / "sub" / "nested.txt").write_text("nested")
    dest = tmp_path / "manifests"
    dest.mkdir()

    r = run_cli(
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
    assert r.returncode == 0, r.stderr or r.stdout
    manifests = list(dest.glob("*.manifest"))
    assert len(manifests) == 1
    paths = {p["path"] for p in json.loads(manifests[0].read_text())["paths"]}
    assert paths == {"hello.txt", "sub/nested.txt"}


def test_cli_manifest_snapshot_include_exclude(seeded_farm_queue, run_cli, tmp_path):
    _, _, _, env = seeded_farm_queue
    root = tmp_path / "assets"
    root.mkdir()
    (root / "keep.txt").write_text("keep")
    (root / "drop.log").write_text("drop")
    (root / "also_keep.md").write_text("md")
    dest = tmp_path / "manifests"
    dest.mkdir()

    empty_cwd = tmp_path / "empty_cwd"
    empty_cwd.mkdir()
    r = run_cli(
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
    assert r.returncode == 0, r.stderr or r.stdout
    manifest = next(dest.glob("*.manifest"))
    paths = {p["path"] for p in json.loads(manifest.read_text())["paths"]}
    assert paths == {"keep.txt", "also_keep.md"}


def test_cli_manifest_diff(seeded_farm_queue, run_cli, tmp_path):
    _, _, _, env = seeded_farm_queue
    root = tmp_path / "assets"
    root.mkdir()
    (root / "unchanged.txt").write_text("same")
    (root / "will_modify.txt").write_text("v1")
    (root / "will_delete.txt").write_text("bye")
    manifests_dir = tmp_path / "manifests"
    manifests_dir.mkdir()
    r = run_cli(
        env, "manifest", "snapshot", "--root", str(root), "--destination", str(manifests_dir)
    )
    assert r.returncode == 0, r.stderr
    original_manifest = next(manifests_dir.glob("*.manifest"))

    (root / "will_modify.txt").write_text("v2 changed")
    (root / "will_delete.txt").unlink()
    (root / "brand_new.txt").write_text("new")

    r = run_cli(
        env,
        "manifest",
        "diff",
        "--root",
        str(root),
        "--manifest",
        str(original_manifest),
        "--json",
    )
    assert r.returncode == 0, r.stderr or r.stdout
    diff = json.loads(r.stdout[r.stdout.find("{") :])
    assert diff["new"] == ["brand_new.txt"]
    assert diff["modified"] == ["will_modify.txt"]
    assert diff["deleted"] == ["will_delete.txt"]


def test_cli_manifest_upload_to_s3_cas_uri(seeded_farm_queue, run_cli, s3_client, tmp_path):
    from _constants import BUCKET, ROOT_PREFIX

    _, _, _, env = seeded_farm_queue
    root = tmp_path / "src"
    root.mkdir()
    (root / "one.txt").write_text("one")
    manifests = tmp_path / "manifests"
    manifests.mkdir()
    r = run_cli(env, "manifest", "snapshot", "--root", str(root), "--destination", str(manifests))
    assert r.returncode == 0, r.stderr
    manifest = next(manifests.glob("*.manifest"))

    r = run_cli(
        env,
        "manifest",
        "upload",
        str(manifest),
        "--s3-cas-uri",
        f"s3://{BUCKET}/{ROOT_PREFIX}",
    )
    assert r.returncode == 0, r.stderr or r.stdout

    listing = s3_client.list_objects_v2(Bucket=BUCKET, Prefix=f"{ROOT_PREFIX}/Manifests/")
    keys = [o["Key"] for o in listing.get("Contents", [])]
    expected_key = f"{ROOT_PREFIX}/Manifests/{manifest.name}"
    assert keys == [expected_key]

    obj = s3_client.get_object(Bucket=BUCKET, Key=expected_key)
    assert obj["Body"].read() == manifest.read_bytes()
    assert obj["Metadata"]["file-system-location-name"] == str(manifest)


def test_cli_manifest_download(seeded_farm_queue, run_cli, s3_client, tmp_path):
    from _constants import BUCKET, ROOT_PREFIX

    backend, farm_id, queue_id, env = seeded_farm_queue
    src = tmp_path / "src"
    src.mkdir()
    (src / "only.txt").write_text("only")
    manifests_dir = tmp_path / "manifests"
    manifests_dir.mkdir()
    r = run_cli(
        env, "manifest", "snapshot", "--root", str(src), "--destination", str(manifests_dir)
    )
    assert r.returncode == 0, r.stderr
    local_manifest = next(manifests_dir.glob("*.manifest"))

    job_id = "job-0123456789abcdefabcdefabcdefabcd"
    r = run_cli(
        env,
        "manifest",
        "upload",
        str(local_manifest),
        "--s3-cas-uri",
        f"s3://{BUCKET}/{ROOT_PREFIX}",
        "--s3-manifest-prefix",
        f"{farm_id}/{queue_id}/{job_id}",
    )
    assert r.returncode == 0, r.stderr or r.stdout

    listing = s3_client.list_objects_v2(
        Bucket=BUCKET, Prefix=f"{ROOT_PREFIX}/Manifests/{farm_id}/{queue_id}/{job_id}/"
    )
    input_manifest_path = listing["Contents"][0]["Key"][len(f"{ROOT_PREFIX}/Manifests/") :]

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
    r = run_cli(
        env,
        "manifest",
        "download",
        str(download_dir),
        "--job-id",
        job_id,
        "--asset-type",
        "input",
    )
    assert r.returncode == 0, r.stderr or r.stdout
    downloaded = list(Path(download_dir).rglob("*.manifest"))
    assert downloaded
    assert {p["path"] for p in json.loads(downloaded[0].read_text())["paths"]} == {"only.txt"}


def test_cli_manifest_upload_and_download_with_new_queue_settings_field(
    queue_with_new_settings_field, run_cli, configure_cli_defaults, tmp_path
):
    backend, farm_id, queue_id, env = queue_with_new_settings_field
    configure_cli_defaults(env, farm_id=farm_id, queue_id=queue_id)
    r = run_cli(env, "queue", "get")
    assert r.returncode == 0, r.stderr
    assert "someNewField" in r.stdout, "the CLI did not receive the new field"

    src = tmp_path / "src"
    src.mkdir()
    (src / "only.txt").write_text("only")
    manifests_dir = tmp_path / "manifests"
    manifests_dir.mkdir()
    r = run_cli(
        env, "manifest", "snapshot", "--root", str(src), "--destination", str(manifests_dir)
    )
    assert r.returncode == 0, r.stderr
    local_manifest = next(manifests_dir.glob("*.manifest"))

    job_id = "job-0123456789abcdefabcdefabcdefabcd"
    r = run_cli(
        env,
        "manifest",
        "upload",
        str(local_manifest),
        "--farm-id",
        farm_id,
        "--queue-id",
        queue_id,
        "--s3-manifest-prefix",
        f"{farm_id}/{queue_id}/{job_id}",
        "--json",
    )
    assert r.returncode == 0, r.stderr or r.stdout
    input_manifest_path = f"{farm_id}/{queue_id}/{job_id}/{local_manifest.name}"

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
    r = run_cli(
        env, "manifest", "download", str(download_dir), "--job-id", job_id, "--asset-type", "input"
    )
    assert r.returncode == 0, r.stderr or r.stdout
    downloaded = list(download_dir.rglob("*.manifest"))
    assert downloaded
    assert {p["path"] for p in json.loads(downloaded[0].read_text())["paths"]} == {"only.txt"}


def test_cli_manifest_help(deadline_env, run_cli):
    _, env = deadline_env
    r = run_cli(env, "manifest", "--help")
    assert r.returncode == 0
    for sub in ("snapshot", "diff", "upload", "download"):
        assert sub in r.stdout
