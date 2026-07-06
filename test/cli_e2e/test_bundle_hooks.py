# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""End-to-end tests for submission hooks through ``deadline bundle submit``.

These invoke the real ``deadline`` CLI as a subprocess against the in-process mock
Deadline backend + moto S3 (see conftest). Real hook subprocesses execute, so we assert
on their observable effects: a hook writes a sentinel file, a hook-supplied priority
reaches the created job, and disabled hooks do not run.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_TEMPLATE = """\
specificationVersion: 'jobtemplate-2023-09'
name: hook-e2e
parameterDefinitions:
- name: Message
  type: STRING
  default: original_message
steps:
- name: step1
  script:
    actions:
      onRun:
        command: "echo"
        args: ["hi"]
"""

_PARAM_VALUES = "parameterValues:\n- name: Message\n  value: original_message\n"

pytestmark = pytest.mark.skipif(
    sys.version_info < (3, 9),
    reason="MockDeadlineBackend.create_job requires openjd-model (py>=3.9)",
)


def _write_bundle(bundle_dir: Path, hooks_yaml: str, scripts: dict) -> None:
    bundle_dir.mkdir(parents=True, exist_ok=True)
    (bundle_dir / "template.yaml").write_text(_TEMPLATE)
    (bundle_dir / "parameter_values.yaml").write_text(_PARAM_VALUES)
    (bundle_dir / "hooks.yaml").write_text(hooks_yaml)
    for name, contents in scripts.items():
        (bundle_dir / name).write_text(contents)


def _enable_bundle_hooks(env, run_cli):
    assert run_cli(env, "config", "set", "settings.allow_bundle_hooks", "true").returncode == 0
    assert run_cli(env, "config", "set", "settings.auto_accept", "true").returncode == 0


def test_e2e_pre_submission_hook_writes_file(seeded_farm_queue, run_cli, tmp_path):
    """A pre-submission hook runs during a real ``deadline bundle submit`` and writes a
    file, and the job is created on the backend."""
    backend, farm_id, queue_id, env = seeded_farm_queue
    _enable_bundle_hooks(env, run_cli)

    sentinel = tmp_path / "pre_submission_ran.txt"
    bundle = tmp_path / "bundle"
    _write_bundle(
        bundle,
        "version: '1.0'\npreSubmission:\n  - command: python3\n    args: [touch.py]\n",
        {"touch.py": f"open({str(sentinel)!r}, 'w').write('ran')\n"},
    )

    r = run_cli(env, "bundle", "submit", str(bundle), "--yes")

    assert r.returncode == 0, r.stderr or r.stdout
    assert sentinel.exists(), "pre-submission hook did not run"
    jobs = [j for (f, q, _), j in backend.jobs.items() if f == farm_id and q == queue_id]
    assert len(jobs) == 1


def test_e2e_pre_submission_hook_priority_reaches_backend(seeded_farm_queue, run_cli, tmp_path):
    """A pre-submission hook that emits a priority override on stdout changes the priority
    of the job recorded by the backend."""
    backend, farm_id, queue_id, env = seeded_farm_queue
    _enable_bundle_hooks(env, run_cli)

    bundle = tmp_path / "bundle"
    _write_bundle(
        bundle,
        "version: '1.0'\npreSubmission:\n  - command: python3\n    args: [prio.py]\n",
        {"prio.py": "import json; print(json.dumps({'priority': 77}))\n"},
    )

    r = run_cli(env, "bundle", "submit", str(bundle), "--yes")

    assert r.returncode == 0, r.stderr or r.stdout
    jobs = [j for (f, q, _), j in backend.jobs.items() if f == farm_id and q == queue_id]
    assert len(jobs) == 1
    assert jobs[0]["priority"] == 77


def test_e2e_post_submission_hook_runs(seeded_farm_queue, run_cli, tmp_path):
    """A post-submission hook runs after the job is created."""
    backend, farm_id, queue_id, env = seeded_farm_queue
    _enable_bundle_hooks(env, run_cli)

    sentinel = tmp_path / "post_submission_ran.txt"
    bundle = tmp_path / "bundle"
    _write_bundle(
        bundle,
        "version: '1.0'\npostSubmission:\n  - command: python3\n    args: [post.py]\n",
        {"post.py": f"open({str(sentinel)!r}, 'w').write('post')\n"},
    )

    r = run_cli(env, "bundle", "submit", str(bundle), "--yes")

    assert r.returncode == 0, r.stderr or r.stdout
    jobs = [j for (f, q, _), j in backend.jobs.items() if f == farm_id and q == queue_id]
    assert len(jobs) == 1
    assert sentinel.exists(), "post-submission hook did not run"


def test_e2e_bundle_hooks_disabled_by_default(seeded_farm_queue, run_cli, tmp_path):
    """With allow_bundle_hooks unset, a bundle hook does not run (security default)."""
    backend, farm_id, queue_id, env = seeded_farm_queue
    # Only auto_accept; deliberately do NOT enable allow_bundle_hooks.
    assert run_cli(env, "config", "set", "settings.auto_accept", "true").returncode == 0

    sentinel = tmp_path / "should_not_exist.txt"
    bundle = tmp_path / "bundle"
    _write_bundle(
        bundle,
        "version: '1.0'\npreSubmission:\n  - command: python3\n    args: [touch.py]\n",
        {"touch.py": f"open({str(sentinel)!r}, 'w').write('ran')\n"},
    )

    r = run_cli(env, "bundle", "submit", str(bundle), "--yes")

    assert r.returncode == 0, r.stderr or r.stdout
    assert not sentinel.exists(), "bundle hook ran despite allow_bundle_hooks not set"
