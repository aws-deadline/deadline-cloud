# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""End-to-end tests for `deadline bundle submit`."""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pytest


_BUNDLE_SRC = (
    Path(__file__).resolve().parents[1]
    / "unit"
    / "deadline_client"
    / "cli"
    / "test_data"
    / "job_bundle_with_data"
)

_SIMPLE_TEMPLATE = """\
specificationVersion: 'jobtemplate-2023-09'
name: bundle-submit-no-attachments
steps:
- name: step1
  script:
    actions:
      onRun:
        command: "echo"
        args: ["hi"]
"""


@pytest.mark.skipif(
    sys.version_info < (3, 9),
    reason="MockDeadlineBackend.create_job requires openjd-model (py>=3.9)",
)
def test_cli_bundle_submit_without_attachments(seeded_farm_queue, run_cli, tmp_path):
    backend, farm_id, queue_id, env = seeded_farm_queue
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    (bundle_dir / "template.yaml").write_text(_SIMPLE_TEMPLATE)

    r = run_cli(env, "bundle", "submit", str(bundle_dir), "--yes")
    assert r.returncode == 0, r.stderr or r.stdout

    jobs = [j for (f, q, _), j in backend.jobs.items() if f == farm_id and q == queue_id]
    assert len(jobs) == 1


@pytest.mark.skipif(
    sys.version_info < (3, 9),
    reason="MockDeadlineBackend.create_job requires openjd-model (py>=3.9)",
)
def test_cli_bundle_submit_with_attachments(seeded_farm_queue, run_cli, s3_client, tmp_path):
    from _constants import BUCKET, ROOT_PREFIX

    backend, farm_id, queue_id, env = seeded_farm_queue
    bundle_dst = tmp_path / "bundle"
    shutil.copytree(_BUNDLE_SRC, bundle_dst)

    r = run_cli(env, "bundle", "submit", str(bundle_dst), "--yes")
    assert r.returncode == 0, r.stderr or r.stdout

    jobs = [j for (f, q, _), j in backend.jobs.items() if f == farm_id and q == queue_id]
    assert len(jobs) == 1
    assert "attachments" in jobs[0]

    listing = s3_client.list_objects_v2(Bucket=BUCKET, Prefix=f"{ROOT_PREFIX}/Data/")
    assert len(listing.get("Contents", [])) == 3

    manifests = s3_client.list_objects_v2(Bucket=BUCKET, Prefix=f"{ROOT_PREFIX}/Manifests/")
    assert manifests.get("Contents")


@pytest.mark.skipif(
    sys.version_info < (3, 9),
    reason="MockDeadlineBackend.create_job requires openjd-model (py>=3.9)",
)
def test_cli_bundle_submit_with_priority_flag(seeded_farm_queue, run_cli, tmp_path):
    backend, farm_id, queue_id, env = seeded_farm_queue
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    (bundle_dir / "template.yaml").write_text(_SIMPLE_TEMPLATE)

    r = run_cli(env, "bundle", "submit", str(bundle_dir), "--priority", "75", "--yes")
    assert r.returncode == 0, r.stderr or r.stdout
    jobs = [j for (f, q, _), j in backend.jobs.items() if f == farm_id and q == queue_id]
    assert jobs[0]["priority"] == 75


@pytest.mark.skipif(
    sys.version_info < (3, 9),
    reason="MockDeadlineBackend.create_job requires openjd-model (py>=3.9)",
)
def test_cli_bundle_submit_with_parameter(seeded_farm_queue, run_cli, tmp_path):
    backend, farm_id, queue_id, env = seeded_farm_queue
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    (bundle_dir / "template.yaml").write_text(
        """
specificationVersion: 'jobtemplate-2023-09'
name: param-job
parameterDefinitions:
- name: Greeting
  type: STRING
  default: hello
steps:
- name: step1
  script:
    actions:
      onRun:
        command: "echo"
        args: ["{{Param.Greeting}}"]
""".lstrip()
    )

    r = run_cli(
        env,
        "bundle",
        "submit",
        str(bundle_dir),
        "--parameter",
        "Greeting=hi",
        "--yes",
    )
    assert r.returncode == 0, r.stderr or r.stdout


def test_cli_bundle_submit_bad_parameter_format_fails(seeded_farm_queue, run_cli, tmp_path):
    _, _, _, env = seeded_farm_queue
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    (bundle_dir / "template.yaml").write_text(_SIMPLE_TEMPLATE)
    r = run_cli(
        env,
        "bundle",
        "submit",
        str(bundle_dir),
        "--parameter",
        "badformat",
        "--yes",
    )
    assert r.returncode != 0


def test_cli_bundle_help(deadline_env, run_cli):
    _, env = deadline_env
    r = run_cli(env, "bundle", "--help")
    assert r.returncode == 0
    assert "submit" in r.stdout


def test_cli_bundle_submit_missing_bundle(deadline_env, run_cli, tmp_path):
    _, env = deadline_env
    r = run_cli(env, "bundle", "submit", str(tmp_path / "does-not-exist"))
    assert r.returncode != 0


@pytest.mark.skipif(
    sys.version_info < (3, 9),
    reason="MockDeadlineBackend.create_job requires openjd-model (py>=3.9)",
)
def test_cli_bundle_submit_with_name_override(seeded_farm_queue, run_cli, tmp_path):
    backend, farm_id, queue_id, env = seeded_farm_queue
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    (bundle_dir / "template.yaml").write_text(_SIMPLE_TEMPLATE)

    r = run_cli(env, "bundle", "submit", str(bundle_dir), "--name", "renamed-job", "--yes")
    assert r.returncode == 0, r.stderr or r.stdout
    jobs = [j for (f, q, _), j in backend.jobs.items() if f == farm_id and q == queue_id]
    assert jobs[0]["name"] == "renamed-job"
