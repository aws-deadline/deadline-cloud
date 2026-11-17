# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests that create_job_from_job_bundle saves an as-submitted job bundle that captures the
changes pre-submission hooks make, and that submitting it reproduces the original CreateJob.
"""

import json
import os
from pathlib import Path
from unittest.mock import patch

import yaml

from deadline.client import api, config
from deadline.client.api import _submit_job_bundle
from deadline.job_attachments.models import (
    Attachments,
    AssetRootManifest,
    ManifestProperties,
    PathFormat,
)
from deadline.job_attachments.progress_tracker import SummaryStatistics
from deadline.job_attachments.upload import S3AssetManager

from ..shared_constants import (
    MOCK_CREATE_JOB_RESPONSE,
    MOCK_FARM_ID,
    MOCK_GET_JOB_RESPONSE,
    MOCK_GET_QUEUE_RESPONSE,
    MOCK_QUEUE_ID,
)

_TEMPLATE = """specificationVersion: 'jobtemplate-2023-09'
name: AsSubmittedTest
parameterDefinitions:
- name: Message
  type: STRING
- name: InDir
  type: PATH
  objectType: DIRECTORY
  dataFlow: IN
- name: ScriptsDir
  type: PATH
  objectType: DIRECTORY
  dataFlow: IN
  default: scripts
steps:
- name: S
  script:
    actions:
      onRun:
        command: echo
"""

_HOOK_TEMPLATE = _TEMPLATE.replace("name: AsSubmittedTest", "name: RenamedByHook")


def _write_bundle(bundle: Path, in_dir: Path, studio_dir: Path) -> None:
    bundle.mkdir()
    (bundle / "template.yaml").write_text(_TEMPLATE)
    (bundle / "scripts").mkdir()
    (bundle / "scripts" / "run.sh").write_text("echo run")
    (bundle / "parameter_values.yaml").write_text(
        "parameterValues:\n"
        "- name: Message\n  value: original\n"
        f"- name: InDir\n  value: {in_dir.as_posix()}\n"
    )
    hook_output = {
        "template": _HOOK_TEMPLATE,
        "priority": 80,
        "parameters": {"Message": "from_hook"},
        "attachments": {"assetReferences": {"inputDirectories": [str(studio_dir)]}},
    }
    (bundle / "hook.py").write_text(f"print({json.dumps(json.dumps(hook_output))})\n")
    (bundle / "hooks.yaml").write_text(
        "version: '1.0'\npreSubmission:\n  - command: python3\n    args: [hook.py]\n"
    )


def _submit(bundle_dir, tmp_path, known_asset_paths, **kwargs):
    """Submit the bundle with mocked AWS calls, returning (CreateJob kwargs, hashed inputs).

    There is no confirmation callback, so any input outside the bundle directory and
    known_asset_paths cancels the submission.
    """
    hashed_inputs: set = set()

    def fake_hash(*args, **kwargs):
        for group in kwargs.get("asset_groups", []):
            hashed_inputs.update(str(p) for p in group.inputs)
        return [SummaryStatistics(), AssetRootManifest()]

    with (
        patch.object(_submit_job_bundle.api, "get_boto3_session"),
        patch.object(_submit_job_bundle.api, "get_boto3_client") as client_mock,
        patch.object(_submit_job_bundle.api, "get_queue_user_boto3_session"),
        patch.object(S3AssetManager, "hash_assets_and_create_manifest", side_effect=fake_hash),
        patch.object(S3AssetManager, "upload_assets") as mock_upload_assets,
        patch.object(_submit_job_bundle.api, "get_deadline_cloud_library_telemetry_client"),
    ):
        client_mock().create_job.side_effect = [MOCK_CREATE_JOB_RESPONSE]
        client_mock().get_queue.side_effect = [MOCK_GET_QUEUE_RESPONSE]
        client_mock().get_job.side_effect = [MOCK_GET_JOB_RESPONSE]
        mock_upload_assets.return_value = [
            SummaryStatistics(),
            Attachments(
                [
                    ManifestProperties(
                        rootPath=str(tmp_path),
                        rootPathFormat=PathFormat.POSIX,
                        inputManifestPath="m",
                        inputManifestHash="h",
                        outputRelativeDirectories=["."],
                    )
                ]
            ),
        ]

        job_id = api.create_job_from_job_bundle(
            job_bundle_dir=str(bundle_dir),
            queue_parameter_definitions=[],
            known_asset_paths=[str(p) for p in known_asset_paths],
            **kwargs,
        )

    assert job_id == MOCK_CREATE_JOB_RESPONSE["jobId"]
    client_mock().create_job.assert_called_once()
    return client_mock().create_job.call_args.kwargs, hashed_inputs


def _setup(fresh_deadline_config, tmp_path):
    """Return (bundle, inputs dir, studio defaults dir); only the two dirs are known paths."""
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("settings.allow_bundle_hooks", "true")
    config.set_setting("settings.auto_accept", "true")

    in_dir = tmp_path / "inputs"
    studio_dir = tmp_path / "studio_defaults"
    in_dir.mkdir()
    studio_dir.mkdir()
    (in_dir / "a.txt").write_text("a")
    (studio_dir / "b.txt").write_text("b")

    bundle = tmp_path / "bundle"
    _write_bundle(bundle, in_dir, studio_dir)
    return bundle, in_dir, studio_dir


def test_as_submitted_bundle_captures_hook_changes(fresh_deadline_config, tmp_path):
    bundle, in_dir, studio_dir = _setup(fresh_deadline_config, tmp_path)
    as_submitted = tmp_path / "bundle.as-submitted"

    _submit(bundle, tmp_path, [in_dir, studio_dir], as_submitted_bundle_dir=str(as_submitted))

    assert sorted(os.listdir(as_submitted)) == [
        "asset_references.yaml",
        "hook.py",
        "parameter_values.yaml",
        "scripts",
        "template.yaml",
    ]
    assert (as_submitted / "template.yaml").read_text() == _HOOK_TEMPLATE
    assert (as_submitted / "scripts" / "run.sh").read_text() == "echo run"

    parameter_values = {
        p["name"]: p["value"]
        for p in yaml.safe_load((as_submitted / "parameter_values.yaml").read_text())[
            "parameterValues"
        ]
    }
    assert parameter_values["Message"] == "from_hook"
    assert os.path.normpath(parameter_values["InDir"]) == str(in_dir)
    assert parameter_values["ScriptsDir"] == str(as_submitted / "scripts")
    assert parameter_values["deadline:priority"] == 80

    asset_references = yaml.safe_load((as_submitted / "asset_references.yaml").read_text())
    # The PATH parameter's directory is not duplicated, it comes from the parameter value.
    assert asset_references["assetReferences"]["inputs"]["directories"] == [str(studio_dir)]

    # The original bundle is left as authored.
    assert (bundle / "template.yaml").read_text() == _TEMPLATE


def test_submitting_as_submitted_bundle_reproduces_create_job(fresh_deadline_config, tmp_path):
    bundle, in_dir, studio_dir = _setup(fresh_deadline_config, tmp_path)
    as_submitted = tmp_path / "bundle.as-submitted"

    original_args, original_inputs = _submit(
        bundle, tmp_path, [in_dir, studio_dir], as_submitted_bundle_dir=str(as_submitted)
    )
    # Files from the bundle are not under a known path here, so this would be canceled
    # if the as-submitted bundle still referenced them in the original bundle.
    resubmitted_args, resubmitted_inputs = _submit(as_submitted, tmp_path, [in_dir, studio_dir])

    def in_original_bundle(value):
        # Replace the JSON-escaped forms, since Windows paths contain backslashes.
        escaped_as_submitted = json.dumps(str(as_submitted))[1:-1]
        escaped_bundle = json.dumps(str(bundle))[1:-1]
        return json.loads(json.dumps(value).replace(escaped_as_submitted, escaped_bundle))

    assert in_original_bundle(resubmitted_args) == original_args
    assert in_original_bundle(sorted(resubmitted_inputs)) == sorted(original_inputs)
    assert str(as_submitted / "scripts" / "run.sh") in resubmitted_inputs
    assert str(studio_dir / "b.txt") in resubmitted_inputs


def test_no_as_submitted_bundle_without_pre_submission_hooks(fresh_deadline_config, tmp_path):
    bundle, in_dir, studio_dir = _setup(fresh_deadline_config, tmp_path)
    (bundle / "hooks.yaml").unlink()
    as_submitted = tmp_path / "bundle.as-submitted"

    _submit(bundle, tmp_path, [in_dir, studio_dir], as_submitted_bundle_dir=str(as_submitted))

    assert not as_submitted.exists()


def test_as_submitted_bundle_failure_does_not_fail_submission(fresh_deadline_config, tmp_path):
    bundle, in_dir, studio_dir = _setup(fresh_deadline_config, tmp_path)
    as_submitted = tmp_path / "bundle.as-submitted"
    as_submitted.mkdir()
    (as_submitted / "existing.txt").write_text("keep")

    with patch.object(_submit_job_bundle.logger, "warning") as mock_warning:
        _submit(bundle, tmp_path, [in_dir, studio_dir], as_submitted_bundle_dir=str(as_submitted))

    assert os.listdir(as_submitted) == ["existing.txt"]
    assert "as-submitted" in mock_warning.call_args.args[0]


def test_as_submitted_bundle_inside_job_bundle_is_rejected(fresh_deadline_config, tmp_path):
    bundle, in_dir, studio_dir = _setup(fresh_deadline_config, tmp_path)
    as_submitted = bundle / "as-submitted"

    with patch.object(_submit_job_bundle.logger, "warning") as mock_warning:
        _submit(bundle, tmp_path, [in_dir, studio_dir], as_submitted_bundle_dir=str(as_submitted))

    assert not as_submitted.exists()
    assert "inside the job bundle" in mock_warning.call_args.args[0]
