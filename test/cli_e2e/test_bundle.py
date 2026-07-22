# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""End-to-end tests for `deadline bundle submit`."""

from __future__ import annotations

import shutil
from pathlib import Path

from _constants import REGION


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


def test_cli_bundle_submit_without_attachments(seeded_farm_queue, run_cli, tmp_path):
    backend, farm_id, queue_id, env = seeded_farm_queue
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    (bundle_dir / "template.yaml").write_text(_SIMPLE_TEMPLATE)

    r = run_cli(env, "bundle", "submit", str(bundle_dir), "--yes")
    assert r.returncode == 0, r.stderr or r.stdout

    jobs = [j for (f, q, _), j in backend.jobs.items() if f == farm_id and q == queue_id]
    assert len(jobs) == 1


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


def test_cli_bundle_submit_with_priority_flag(seeded_farm_queue, run_cli, tmp_path):
    backend, farm_id, queue_id, env = seeded_farm_queue
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    (bundle_dir / "template.yaml").write_text(_SIMPLE_TEMPLATE)

    r = run_cli(env, "bundle", "submit", str(bundle_dir), "--priority", "75", "--yes")
    assert r.returncode == 0, r.stderr or r.stdout
    jobs = [j for (f, q, _), j in backend.jobs.items() if f == farm_id and q == queue_id]
    assert jobs[0]["priority"] == 75


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


def test_cli_bundle_submit_with_name_override(seeded_farm_queue, run_cli, tmp_path):
    backend, farm_id, queue_id, env = seeded_farm_queue
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    (bundle_dir / "template.yaml").write_text(_SIMPLE_TEMPLATE)

    r = run_cli(env, "bundle", "submit", str(bundle_dir), "--name", "renamed-job", "--yes")
    assert r.returncode == 0, r.stderr or r.stdout
    jobs = [j for (f, q, _), j in backend.jobs.items() if f == farm_id and q == queue_id]
    assert jobs[0]["name"] == "renamed-job"


# ---- Monitor URL on submit -------------------------------------------------


def test_cli_bundle_submit_no_monitor_profile_omits_url(seeded_farm_queue, run_cli, tmp_path):
    """With plain (host-provided) credentials, no monitor URL is printed."""
    _, _, _, env = seeded_farm_queue
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    (bundle_dir / "template.yaml").write_text(_SIMPLE_TEMPLATE)

    r = run_cli(env, "bundle", "submit", str(bundle_dir), "--yes")
    assert r.returncode == 0, r.stderr or r.stdout
    assert "Job URL:" not in r.stdout


def test_cli_bundle_submit_with_monitor_profile_prints_url(
    seeded_farm_queue, run_cli, set_cli_monitor_profile, tmp_path
):
    """With a Deadline Cloud monitor profile, the job's monitor URL is printed
    and points at the submitted farm/queue/job."""
    backend, farm_id, queue_id, env = seeded_farm_queue
    monitor = backend.create_monitor(subdomain="mymonitor", region=REGION)
    set_cli_monitor_profile(env, monitor_id=monitor["monitorId"], region=REGION)

    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    (bundle_dir / "template.yaml").write_text(_SIMPLE_TEMPLATE)

    r = run_cli(env, "bundle", "submit", str(bundle_dir), "--yes")
    assert r.returncode == 0, r.stderr or r.stdout

    jobs = {
        j["jobId"]: j for (f, q, _), j in backend.jobs.items() if f == farm_id and q == queue_id
    }
    assert len(jobs) == 1
    job_id = next(iter(jobs))

    expected = (
        f"Job URL: https://mymonitor.{REGION}.deadlinecloud.amazonaws.com"
        f"/{REGION}/farms/{farm_id}/queues/{queue_id}?jobId={job_id}"
    )
    assert expected in r.stdout, r.stdout


def test_cli_bundle_submit_cross_region_monitor_url(
    seeded_farm_queue, run_cli, set_cli_monitor_profile, tmp_path
):
    """A monitor in one region linking to a farm in another: the host uses the
    monitor's region while the path uses the farm's region."""
    monitor_region = "us-east-1"

    backend, farm_id, queue_id, env = seeded_farm_queue
    # Monitor lives in us-east-1; the farm/queue are in REGION (us-west-2).
    monitor = backend.create_monitor(subdomain="mymonitor", region=monitor_region)
    set_cli_monitor_profile(env, monitor_id=monitor["monitorId"], region=monitor_region)
    # farm_id/queue_id (and farm_region) are scoped under the selected AWS profile,
    # so re-assert them now that the monitor profile is active, then pin the farm's
    # region so the URL path uses it (distinct from the monitor's host region).
    for key, value in (
        ("defaults.farm_id", farm_id),
        ("defaults.queue_id", queue_id),
        ("defaults.farm_region", REGION),
    ):
        r = run_cli(env, "config", "set", key, value)
        assert r.returncode == 0, r.stderr

    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    (bundle_dir / "template.yaml").write_text(_SIMPLE_TEMPLATE)

    r = run_cli(env, "bundle", "submit", str(bundle_dir), "--yes")
    assert r.returncode == 0, r.stderr or r.stdout

    jobs = {
        j["jobId"]: j for (f, q, _), j in backend.jobs.items() if f == farm_id and q == queue_id
    }
    job_id = next(iter(jobs))

    expected = (
        f"Job URL: https://mymonitor.{monitor_region}.deadlinecloud.amazonaws.com"
        f"/{REGION}/farms/{farm_id}/queues/{queue_id}?jobId={job_id}"
    )
    assert expected in r.stdout, r.stdout
