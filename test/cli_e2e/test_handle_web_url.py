# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""End-to-end tests for `deadline handle-web-url`."""


def test_cli_handle_web_url_missing_args_fails(deadline_env, run_cli):
    _, env = deadline_env
    r = run_cli(env, "handle-web-url")
    assert r.returncode != 0
    combined = r.stdout + r.stderr
    assert "URL" in combined or "url" in combined


def test_cli_handle_web_url_bad_scheme_fails(deadline_env, run_cli):
    _, env = deadline_env
    r = run_cli(env, "handle-web-url", "https://example.com/download-output")
    assert r.returncode != 0


def test_cli_handle_web_url_install_and_uninstall_cannot_coexist(deadline_env, run_cli):
    _, env = deadline_env
    r = run_cli(env, "handle-web-url", "--install", "--uninstall")
    assert r.returncode != 0


def test_cli_handle_web_url_unsupported_command_fails(deadline_env, run_cli):
    _, env = deadline_env
    r = run_cli(env, "handle-web-url", "deadline://not-a-command?x=1")
    assert r.returncode != 0


def test_cli_handle_web_url_install_with_url_fails(deadline_env, run_cli):
    _, env = deadline_env
    r = run_cli(env, "handle-web-url", "--install", "deadline://download-output?farm-id=x")
    assert r.returncode != 0


def test_cli_handle_web_url_download_output_missing_required_args(deadline_env, run_cli):
    _, env = deadline_env
    # Missing required farm-id / queue-id / job-id.
    r = run_cli(env, "handle-web-url", "deadline://download-output?x=1")
    assert r.returncode != 0


def test_cli_handle_web_url_download_output_end_to_end(
    seeded_farm_queue, run_cli, s3_client, tmp_path
):
    from _constants import BUCKET, ROOT_PREFIX
    from deadline.job_attachments.asset_manifests.hash_algorithms import HashAlgorithm, hash_data
    import json
    import os
    from pathlib import Path

    backend, farm_id, queue_id, env = seeded_farm_queue
    job_id = "job-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    step_id = "step-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    task_id = "task-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-0"
    asset_root = str(tmp_path / "out")
    Path(asset_root).mkdir()

    content = b"x-y-z"
    file_hash = hash_data(content, HashAlgorithm.XXH128)
    s3_client.put_object(Bucket=BUCKET, Key=f"{ROOT_PREFIX}/Data/{file_hash}.xxh128", Body=content)

    manifest_body = json.dumps(
        {
            "hashAlg": "xxh128",
            "manifestVersion": "2023-03-03",
            "paths": [{"hash": file_hash, "mtime": 0, "path": "r.txt", "size": len(content)}],
            "totalSize": len(content),
        }
    ).encode()
    manifest_key = (
        f"{ROOT_PREFIX}/Manifests/{farm_id}/{queue_id}/{job_id}/{step_id}/{task_id}/"
        f"sessionaction-0/outputmanifestv2023-03-03_output"
    )
    s3_client.put_object(
        Bucket=BUCKET,
        Key=manifest_key,
        Body=manifest_body,
        Metadata={"asset-root": asset_root},
    )
    backend.jobs[(farm_id, queue_id, job_id)] = {
        "jobId": job_id,
        "name": "web-url-job",
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

    url = f"deadline://download-output?farm-id={farm_id}&queue-id={queue_id}&job-id={job_id}"
    # handle-web-url doesn't forward --yes; use auto-accept instead.
    run_cli(env, "config", "set", "settings.auto_accept", "true")
    r = run_cli(env, "handle-web-url", url)
    assert r.returncode == 0, r.stderr or r.stdout
    assert (Path(asset_root) / "r.txt").read_text() == "x-y-z"


def test_cli_handle_web_url_help(deadline_env, run_cli):
    _, env = deadline_env
    r = run_cli(env, "handle-web-url", "--help")
    assert r.returncode == 0
