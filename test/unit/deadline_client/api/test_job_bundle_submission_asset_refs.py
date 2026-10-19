# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests the deadline.client.api functions for submitting Open Job Description job bundles,
where there are PATH parameters that carry assetReference IN/OUT metadata.
"""

import json
import os
import pytest
from unittest.mock import ANY, patch
from pathlib import Path

from deadline.client import api, config
from deadline.client.api import _submit_job_bundle
from deadline.client.exceptions import DeadlineOperationError
from deadline.job_attachments.models import (
    Attachments,
    AssetRootGroup,
    JobAttachmentsFileSystem,
    AssetRootManifest,
    ManifestProperties,
    PathFormat,
)
from deadline.job_attachments.upload import S3AssetManager
from deadline.job_attachments.progress_tracker import SummaryStatistics

from ..shared_constants import (
    MOCK_FARM_ID,
    MOCK_QUEUE_ID,
    MOCK_GET_JOB_RESPONSE,
    MOCK_CREATE_JOB_RESPONSE,
    MOCK_GET_QUEUE_RESPONSE,
)
from ..testing_utilities import write_test_asset_files

# A YAML job template that contains every type of (file, directory) * (none, in, out, inout) asset references
JOB_BUNDLE_RELATIVE_FILE_PATH = "./file/inside/job_bundle.txt"
JOB_BUNDLE_RELATIVE_DIR_PATH = "./dir/inside/job_bundle"
JOB_TEMPLATE_ALL_ASSET_REF_VARIANTS = f"""
specificationVersion: 'jobtemplate-2023-09'
name: Job Template to test all assetReference variants.
parameterDefinitions:
- name: FileNoneDefault
  type: PATH
  objectType: FILE
  description: FILE * NONE (default)
- name: FileNone
  type: PATH
  objectType: FILE
  dataFlow: NONE
  description: FILE * NONE
- name: FileIn
  type: PATH
  objectType: FILE
  dataFlow: IN
  description: FILE * IN
  default: {JOB_BUNDLE_RELATIVE_FILE_PATH}
- name: FileOut
  type: PATH
  objectType: FILE
  dataFlow: OUT
  description: FILE * OUT
- name: FileInout
  type: PATH
  objectType: FILE
  dataFlow: INOUT
  description: FILE * INOUT
- name: DirNoneDefault
  type: PATH
  objectType: DIRECTORY
  description: DIR * NONE
- name: DirNone
  type: PATH
  objectType: DIRECTORY
  dataFlow: NONE
  description: DIR * NONE
- name: DirIn
  type: PATH
  objectType: DIRECTORY
  dataFlow: IN
  description: DIR * IN
  default: {JOB_BUNDLE_RELATIVE_DIR_PATH}
- name: DirOut
  type: PATH
  objectType: DIRECTORY
  dataFlow: OUT
  description: DIR * OUT
- name: DirInout
  type: PATH
  objectType: DIRECTORY
  dataFlow: INOUT
  description: DIR * INOUT
- name: FileInEmpty
  type: PATH
  objectType: FILE
  dataFlow: IN
  description: Empty file
  default: ''
- name: DirInEmpty
  type: PATH
  objectType: DIRECTORY
  dataFlow: IN
  description: Empty dir
  default: ''
steps:
- name: CliScript
  script:
    embeddedFiles:
    - name: runScript
      type: TEXT
      runnable: true
      data: |
        #!/usr/bin/env bash
        echo '
          {{Param.FileNoneDefault}}
          {{Param.FileNone}}
          {{Param.FileIn}}
          {{Param.FileOut}}
          {{Param.FileInout}}
          {{Param.DirNoneDefault}}
          {{Param.DirNone}}
          {{Param.DirIn}}
          {{Param.DirOut}}
          {{Param.DirInout}}
          {{Param.FileInEmpty}}
          {{Param.DirInEmpty}}
        '
    actions:
      onRun:
        command: '{{Task.Attachment.runScript.Path}}'
"""

JOB_TEMPLATE_NOT_VALID_FILE_PATH = """
specificationVersion: 'jobtemplate-2023-09'
name: Job Template to test a not valid file path.
parameterDefinitions:
- name: FileIn
  type: PATH
  objectType: FILE
  dataFlow: IN
  description: FILE * IN
  default: JOB_BUNDLE_NOT_VALID_FILE_PATH
steps:
- name: CliScript
  script:
    embeddedFiles:
    - name: runScript
      type: TEXT
      runnable: true
      data: |
        #!/usr/bin/env bash
        echo '
          {{Param.FileIn}}
        '
    actions:
      onRun:
        command: '{{Task.Attachment.runScript.Path}}'
"""

JOB_TEMPLATE_NOT_VALID_DIR_PATH = """
specificationVersion: 'jobtemplate-2023-09'
name: Job Template to test a not valid directory path.
parameterDefinitions:
- name: DirIn
  type: PATH
  objectType: DIRECTORY
  dataFlow: IN
  description: DIR * IN
  default: JOB_BUNDLE_NOT_VALID_DIR_PATH
steps:
- name: CliScript
  script:
    embeddedFiles:
    - name: runScript
      type: TEXT
      runnable: true
      data: |
        #!/usr/bin/env bash
        echo '
          {{Param.DirIn}}
        '
    actions:
      onRun:
        command: '{{Task.Attachment.runScript.Path}}'
"""


@pytest.mark.parametrize(
    "template, parameter_key, file_path",
    [
        pytest.param(
            JOB_TEMPLATE_NOT_VALID_FILE_PATH,
            "JOB_BUNDLE_NOT_VALID_FILE_PATH",
            "/absolute/absolute.txt",
        ),
        pytest.param(
            JOB_TEMPLATE_NOT_VALID_FILE_PATH,
            "JOB_BUNDLE_NOT_VALID_FILE_PATH",
            "../relative_outside_bundle_dir.txt",
        ),
        pytest.param(
            JOB_TEMPLATE_NOT_VALID_DIR_PATH, "JOB_BUNDLE_NOT_VALID_DIR_PATH", "/absolutedir"
        ),
        pytest.param(
            JOB_TEMPLATE_NOT_VALID_DIR_PATH,
            "JOB_BUNDLE_NOT_VALID_DIR_PATH",
            "../relative_outside_bundle_dir",
        ),
    ],
)
def test_create_job_from_job_bundle_with_not_valid_directory_path(
    fresh_deadline_config, temp_job_bundle_dir, temp_assets_dir, template, parameter_key, file_path
):
    """
    Tests that a job bundle with template that contains either absolute paths or relative paths that resolve
    outside of the Job Bundle directory throws a DeadlineOperationError.
    """
    # Use a temporary directory for the job bundle
    # Define absolute paths for testing within temp_assets_dir
    job_bundle_absolute_dir_path = os.path.normpath(temp_assets_dir + file_path)

    # Insert absolute paths with temp dir into job template
    job_template_replaced = template.replace(parameter_key, job_bundle_absolute_dir_path)

    # Write the YAML template
    with open(os.path.join(temp_job_bundle_dir, "template.yaml"), "w", encoding="utf8") as f:
        f.write(job_template_replaced)

    with (
        patch.object(_submit_job_bundle.api, "get_boto3_session"),
        patch.object(_submit_job_bundle.api, "get_boto3_client") as client_mock,
        patch.object(_submit_job_bundle.api, "get_queue_user_boto3_session"),
    ):
        client_mock().create_job.side_effect = [MOCK_CREATE_JOB_RESPONSE]
        client_mock().get_queue.side_effect = [MOCK_GET_QUEUE_RESPONSE]
        client_mock().get_job.side_effect = [MOCK_GET_JOB_RESPONSE]
        with pytest.raises(
            DeadlineOperationError,
        ):
            # This is the function we're testing
            api.create_job_from_job_bundle(
                temp_job_bundle_dir, job_parameters=[], queue_parameter_definitions=[]
            )


def test_create_job_from_job_bundle_with_all_asset_ref_variants(
    fresh_deadline_config, temp_job_bundle_dir, temp_assets_dir, temp_cwd
):
    """
    Test a job bundle with template from JOB_TEMPLATE_ALL_ASSET_REF_VARIANTS.
    """
    # Use a temporary directory for the job bundle
    with (
        patch.object(_submit_job_bundle.api, "get_boto3_session"),
        patch.object(_submit_job_bundle.api, "get_boto3_client") as client_mock,
        patch.object(_submit_job_bundle.api, "get_queue_user_boto3_session"),
        patch.object(S3AssetManager, "hash_assets_and_create_manifest") as mock_hash_assets,
        patch.object(S3AssetManager, "upload_assets") as mock_upload_assets,
        patch.object(_submit_job_bundle.api, "get_deadline_cloud_library_telemetry_client"),
    ):
        client_mock().create_job.side_effect = [MOCK_CREATE_JOB_RESPONSE]
        client_mock().get_queue.side_effect = [MOCK_GET_QUEUE_RESPONSE]
        client_mock().get_job.side_effect = [MOCK_GET_JOB_RESPONSE]
        mock_hash_assets.return_value = [SummaryStatistics(), AssetRootManifest()]
        mock_upload_assets.return_value = [
            SummaryStatistics(),
            Attachments(
                [
                    ManifestProperties(
                        rootPath="/mnt/root/path1",
                        rootPathFormat=PathFormat.POSIX,
                        inputManifestPath="mock-manifest",
                        inputManifestHash="mock-manifest-hash",
                        outputRelativeDirectories=["."],
                    ),
                ],
            ),
        ]

        config.set_setting("defaults.farm_id", MOCK_FARM_ID)
        config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

        # Write the YAML template
        with open(os.path.join(temp_job_bundle_dir, "template.yaml"), "w", encoding="utf8") as f:
            f.write(JOB_TEMPLATE_ALL_ASSET_REF_VARIANTS)

        job_parameters = [
            {
                "name": "FileNoneDefault",
                "value": os.path.join(temp_assets_dir, "file/inside/asset-dir-filenonedefault.txt"),
            },
            {
                "name": "FileNone",
                "value": os.path.join(temp_assets_dir, "file/inside/asset-dir-filenone.txt"),
            },
            # Leaving out "FileIn" so it gets the default value
            {"name": "FileOut", "value": "./file/inside/cwd.txt"},
            {
                "name": "FileInout",
                "value": os.path.join(temp_assets_dir, "file/inside/asset-dir-fileinout.txt"),
            },
            {
                "name": "DirNoneDefault",
                "value": os.path.join(temp_assets_dir, "./dir/inside/asset-dir-dirnonedefault"),
            },
            {
                "name": "DirNone",
                "value": os.path.join(temp_assets_dir, "./dir/inside/asset-dir-dirnone"),
            },
            # Leaving out "DirIn" so it gets the default value
            {"name": "DirOut", "value": "./dir/inside/cwd-dirout"},
            {
                "name": "DirInout",
                "value": os.path.join(temp_assets_dir, "./dir/inside/asset-dir-dirinout"),
            },
        ]

        # Write file contents to the job bundle dir
        write_test_asset_files(
            temp_job_bundle_dir,
            {
                JOB_BUNDLE_RELATIVE_FILE_PATH: "file in",
                JOB_BUNDLE_RELATIVE_DIR_PATH + "/file1.txt": "dir in file1",
                JOB_BUNDLE_RELATIVE_DIR_PATH + "/subdir/file1.txt": "dir in file2",
            },
        )
        # Write file contents to the temporary assets dir
        write_test_asset_files(
            temp_assets_dir,
            {
                "file/inside/asset-dir-fileinout.txt": "file inout",
                "././dir/inside/asset-dir-dirinout/file_x.txt": "dir inout",
                "././dir/inside/asset-dir-dirinout/subdir/file_y.txt": "dir inout",
            },
        )

        # This is the function we're testing
        api.create_job_from_job_bundle(
            temp_job_bundle_dir,
            job_parameters=job_parameters,
            queue_parameter_definitions=[],
            known_asset_paths=[
                temp_assets_dir,
                os.path.join(os.getcwd(), "dir", "inside"),
                os.path.join(os.getcwd(), "file", "inside"),
            ],
        )

        # The values of input_paths and output_paths are the first
        # thing this test needs to verify, confirming that the
        # bundle dir is used for default parameter values, and the
        # current working directory is used for job parameters.
        mock_hash_assets.assert_called_once_with(
            asset_groups=[
                AssetRootGroup(
                    root_path=os.path.commonpath(
                        [os.getcwd(), temp_job_bundle_dir, temp_assets_dir]
                    ),
                    inputs={
                        Path(temp_job_bundle_dir) / "dir" / "inside" / "job_bundle" / "file1.txt",
                        Path(temp_job_bundle_dir)
                        / "dir"
                        / "inside"
                        / "job_bundle"
                        / "subdir"
                        / "file1.txt",
                        Path(temp_job_bundle_dir) / "file" / "inside" / "job_bundle.txt",
                        Path(temp_assets_dir)
                        / "dir"
                        / "inside"
                        / "asset-dir-dirinout"
                        / "subdir"
                        / "file_y.txt",
                        Path(temp_assets_dir)
                        / "dir"
                        / "inside"
                        / "asset-dir-dirinout"
                        / "file_x.txt",
                        Path(temp_assets_dir) / "file" / "inside" / "asset-dir-fileinout.txt",
                    },
                    outputs={
                        Path(os.path.join(os.getcwd(), "dir", "inside", "cwd-dirout")),
                        Path(os.path.join(os.getcwd(), "file", "inside")),
                        Path(temp_assets_dir) / "file" / "inside",
                        Path(temp_assets_dir) / "dir" / "inside" / "asset-dir-dirinout",
                    },
                    references={
                        Path(temp_assets_dir) / "file" / "inside" / "asset-dir-filenone.txt",
                        Path(temp_assets_dir) / "file" / "inside" / "asset-dir-filenonedefault.txt",
                        Path(temp_assets_dir) / "dir" / "inside" / "asset-dir-dirnone",
                        Path(temp_assets_dir) / "dir" / "inside" / "asset-dir-dirnonedefault",
                    },
                ),
            ],
            total_input_files=6,
            total_input_bytes=59,
            hash_cache_dir=os.path.expanduser(os.path.join("~", ".deadline", "cache")),
            on_preparing_to_submit=ANY,
        )
        client_mock().create_job.assert_called_once_with(
            farmId=MOCK_FARM_ID,
            queueId=MOCK_QUEUE_ID,
            template=ANY,
            templateType="YAML",
            priority=50,
            attachments={
                "manifests": [
                    {
                        "rootPath": "/mnt/root/path1",
                        "rootPathFormat": PathFormat.POSIX,
                        "inputManifestPath": "mock-manifest",
                        "inputManifestHash": "mock-manifest-hash",
                        "outputRelativeDirectories": ["."],
                    },
                ],
                "fileSystem": JobAttachmentsFileSystem.COPIED.value,
            },
            # The job parameter values are the second thing this test needs to verify,
            # confirming that the parameters were processed according to their types.
            parameters={
                "FileNoneDefault": {
                    "path": os.path.join(
                        temp_assets_dir, "file", "inside", "asset-dir-filenonedefault.txt"
                    ),
                },
                "FileNone": {
                    "path": os.path.normpath(
                        os.path.join(temp_assets_dir, "file", "inside", "asset-dir-filenone.txt")
                    )
                },
                "FileOut": {"path": os.path.normpath(os.path.abspath("file/inside/cwd.txt"))},
                "FileIn": {
                    "path": os.path.normpath(temp_job_bundle_dir + "/file/inside/job_bundle.txt")
                },
                "FileInout": {
                    "path": os.path.normpath(
                        temp_assets_dir + "/file/inside/asset-dir-fileinout.txt"
                    )
                },
                "DirNoneDefault": {
                    "path": os.path.join(
                        temp_assets_dir, "dir", "inside", "asset-dir-dirnonedefault"
                    ),
                },
                "DirNone": {
                    "path": os.path.join(temp_assets_dir, "dir", "inside", "asset-dir-dirnone")
                },
                "DirIn": {
                    "path": os.path.normpath(
                        os.path.join(temp_job_bundle_dir, "dir", "inside", "job_bundle")
                    )
                },
                "DirOut": {"path": os.path.normpath(os.path.abspath("dir/inside/cwd-dirout"))},
                "DirInout": {
                    "path": os.path.normpath(
                        os.path.join(temp_assets_dir, "dir", "inside", "asset-dir-dirinout")
                    )
                },
            },
            maxFailedTasksCount=20,
            maxRetriesPerTask=5,
        )


_PATH_REDIRECT_TEMPLATE = """specificationVersion: 'jobtemplate-2023-09'
name: PathRedirect
parameterDefinitions:
- name: InDir
  type: PATH
  objectType: DIRECTORY
  dataFlow: IN
steps:
- name: S
  script:
    actions:
      onRun:
        command: echo
"""


def test_pre_submission_hook_redirecting_path_param_drops_stale_path(
    fresh_deadline_config, tmp_path
):
    """A pre-submission hook that redirects a PATH parameter (by rewriting
    parameter_values.yaml on disk) must upload only the NEW directory. Regression test for
    the parameter re-resolve carrying over the stale first-pass path — previously both the
    old and new directories were hashed/uploaded.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("settings.allow_bundle_hooks", "true")
    config.set_setting("settings.auto_accept", "true")

    old_dir = tmp_path / "old_inputs"
    new_dir = tmp_path / "new_inputs"
    old_dir.mkdir()
    new_dir.mkdir()
    (old_dir / "a.txt").write_text("old")
    (new_dir / "b.txt").write_text("new")

    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "template.yaml").write_text(_PATH_REDIRECT_TEMPLATE)
    # Use POSIX-style paths so backslashes on Windows do not break YAML or the generated
    # hook script's Python string literal.
    (bundle / "parameter_values.yaml").write_text(
        f"parameterValues:\n- name: InDir\n  value: {old_dir.as_posix()}\n"
    )
    # Hook redirects InDir from old_dir to new_dir on disk.
    (bundle / "redirect.py").write_text(
        "import os\n"
        "b = os.environ['DEADLINE_JOB_BUNDLE_DIR']\n"
        "open(os.path.join(b, 'parameter_values.yaml'), 'w').write("
        f"'parameterValues:\\n- name: InDir\\n  value: {new_dir.as_posix()}\\n')\n"
    )
    (bundle / "hooks.yaml").write_text(
        "version: '1.0'\npreSubmission:\n  - command: python3\n    args: [redirect.py]\n"
    )

    captured: dict = {}

    def fake_hash(*args, **kwargs):
        groups = kwargs.get("asset_groups", args[0] if args else [])
        inputs: set = set()
        for group in groups:
            inputs |= {str(p) for p in getattr(group, "inputs", set())}
        captured["inputs"] = inputs
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

        api.create_job_from_job_bundle(
            job_bundle_dir=str(bundle),
            queue_parameter_definitions=[],
            known_asset_paths=[str(tmp_path)],
            require_paths_exist=False,
        )

    inputs = captured.get("inputs", set())
    assert any("new_inputs" in p for p in inputs), "redirected (new) path should be uploaded"
    assert not any("old_inputs" in p for p in inputs), "stale (old) path must be dropped"


def test_pre_submission_hook_redirected_path_stays_known(fresh_deadline_config, tmp_path):
    """A hook that redirects a PATH parameter (which was supplied as a known job parameter)
    must keep the redirected location in known_asset_paths, so it is not flagged as an
    unknown path — which, with no interactive confirmation callback, would raise
    DeadlineOperationCanceled. known_asset_paths must be recomputed after the hook
    re-resolve, including the hook's stdout ``parameters`` override name.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("settings.allow_bundle_hooks", "true")
    config.set_setting("settings.auto_accept", "true")

    old_dir = tmp_path / "old_inputs"
    new_dir = tmp_path / "redirected_inputs"
    old_dir.mkdir()
    new_dir.mkdir()
    (old_dir / "a.txt").write_text("old")
    (new_dir / "b.txt").write_text("new")

    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "template.yaml").write_text(_PATH_REDIRECT_TEMPLATE)
    (bundle / "parameter_values.yaml").write_text(
        f"parameterValues:\n- name: InDir\n  value: {old_dir.as_posix()}\n"
    )
    # Hook redirects InDir to new_dir via a stdout parameters override.
    (bundle / "redirect.py").write_text(
        f"import json\nprint(json.dumps({{'parameters': {{'InDir': {new_dir.as_posix()!r}}}}}))\n"
    )
    (bundle / "hooks.yaml").write_text(
        "version: '1.0'\npreSubmission:\n  - command: python3\n    args: [redirect.py]\n"
    )

    with (
        patch.object(_submit_job_bundle.api, "get_boto3_session"),
        patch.object(_submit_job_bundle.api, "get_boto3_client") as client_mock,
        patch.object(_submit_job_bundle.api, "get_queue_user_boto3_session"),
        patch.object(S3AssetManager, "hash_assets_and_create_manifest") as mock_hash_assets,
        patch.object(S3AssetManager, "upload_assets") as mock_upload_assets,
        patch.object(_submit_job_bundle.api, "get_deadline_cloud_library_telemetry_client"),
    ):
        client_mock().create_job.side_effect = [MOCK_CREATE_JOB_RESPONSE]
        client_mock().get_queue.side_effect = [MOCK_GET_QUEUE_RESPONSE]
        mock_hash_assets.return_value = [SummaryStatistics(), AssetRootManifest()]
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

        # InDir is supplied as a known job parameter (old_dir is marked known). The hook
        # then redirects it to new_dir, which is NOT pre-marked known and has no confirmation
        # callback. If known_asset_paths were not recomputed after the hook, the redirected
        # path would be treated as unknown and raise DeadlineOperationCanceled. It must
        # submit successfully instead.
        api.create_job_from_job_bundle(
            job_bundle_dir=str(bundle),
            job_parameters=[{"name": "InDir", "value": str(old_dir)}],
            known_asset_paths=[str(old_dir)],
            queue_parameter_definitions=[],
            require_paths_exist=False,
        )

    client_mock().create_job.assert_called_once()


_LIST_PATH_TEMPLATE = """specificationVersion: 'jobtemplate-2023-09'
extensions: [EXPR]
name: ListPath
parameterDefinitions:
- name: InFiles
  type: LIST[PATH]
  objectType: FILE
  dataFlow: IN
  default: ["bundle_in.txt"]
- name: InDirs
  type: LIST[PATH]
  objectType: DIRECTORY
  dataFlow: IN
- name: OutFiles
  type: LIST[PATH]
  objectType: FILE
  dataFlow: OUT
- name: Refs
  type: LIST[PATH]
steps:
- name: S
  script:
    actions:
      onRun:
        command: echo
"""


def _mock_upload_result(root: str) -> list:
    return [
        SummaryStatistics(),
        Attachments(
            [
                ManifestProperties(
                    rootPath=root,
                    rootPathFormat=PathFormat.POSIX,
                    inputManifestPath="m",
                    inputManifestHash="h",
                    outputRelativeDirectories=["."],
                )
            ]
        ),
    ]


def test_create_job_from_job_bundle_list_path_asset_references(
    fresh_deadline_config, tmp_path, temp_cwd
):
    """Every item of a LIST[PATH] parameter is attached as if it were a PATH parameter with
    the same objectType and dataFlow: default items resolve against the bundle, items from
    job_parameters resolve against the working directory and count as known paths, and the
    values are sent to CreateJob in the pathList member."""
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "template.yaml").write_text(_LIST_PATH_TEMPLATE)
    (bundle / "bundle_in.txt").write_text("bundle")

    assets = tmp_path / "assets"
    write_test_asset_files(
        str(assets),
        {"a.txt": "a", "b.txt": "bb", "dir1/x.txt": "xxx", "dir2/sub/y.txt": "yyyy"},
    )
    out_dir = tmp_path / "out"

    # A relative item is resolved against the current working directory.
    write_test_asset_files(os.getcwd(), {"cwd_in.txt": "cwd"})

    with (
        patch.object(_submit_job_bundle.api, "get_boto3_session"),
        patch.object(_submit_job_bundle.api, "get_boto3_client") as client_mock,
        patch.object(_submit_job_bundle.api, "get_queue_user_boto3_session"),
        patch.object(S3AssetManager, "hash_assets_and_create_manifest") as mock_hash_assets,
        patch.object(S3AssetManager, "upload_assets") as mock_upload_assets,
        patch.object(_submit_job_bundle.api, "get_deadline_cloud_library_telemetry_client"),
    ):
        client_mock().create_job.side_effect = [MOCK_CREATE_JOB_RESPONSE]
        client_mock().get_queue.side_effect = [MOCK_GET_QUEUE_RESPONSE]
        mock_hash_assets.return_value = [SummaryStatistics(), AssetRootManifest()]
        mock_upload_assets.return_value = _mock_upload_result(str(tmp_path))

        api.create_job_from_job_bundle(
            job_bundle_dir=str(bundle),
            job_parameters=[
                {
                    "name": "InFiles",
                    "value": json.dumps(
                        [str(assets / "a.txt"), "", "cwd_in.txt", str(assets / "b.txt")]
                    ),
                },
                {"name": "InDirs", "value": [str(assets / "dir1"), str(assets / "dir2")]},
                {"name": "OutFiles", "value": [str(out_dir / "o1.exr"), str(out_dir / "o2.exr")]},
                {"name": "Refs", "value": [str(assets / "ref")]},
            ],
            queue_parameter_definitions=[],
            # No known paths are passed: the job_parameters values are known paths, so
            # submission does not stop to ask about them.
            require_paths_exist=False,
        )

    (asset_group,) = mock_hash_assets.call_args.kwargs["asset_groups"]
    assert asset_group.inputs == {
        assets / "a.txt",
        assets / "b.txt",
        Path(os.getcwd()) / "cwd_in.txt",
        assets / "dir1" / "x.txt",
        assets / "dir2" / "sub" / "y.txt",
    }
    assert asset_group.outputs == {out_dir}
    assert asset_group.references == {assets / "ref"}

    parameters = client_mock().create_job.call_args.kwargs["parameters"]
    assert parameters == {
        "InFiles": {
            "pathList": [
                str(assets / "a.txt"),
                "",
                os.path.abspath("cwd_in.txt"),
                str(assets / "b.txt"),
            ]
        },
        "InDirs": {"pathList": [str(assets / "dir1"), str(assets / "dir2")]},
        "OutFiles": {"pathList": [str(out_dir / "o1.exr"), str(out_dir / "o2.exr")]},
        "Refs": {"pathList": [str(assets / "ref")]},
    }


def test_create_job_from_job_bundle_list_path_default_from_bundle(fresh_deadline_config, tmp_path):
    """A LIST[PATH] default item is resolved against the job bundle and uploaded."""
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "template.yaml").write_text(_LIST_PATH_TEMPLATE)
    (bundle / "bundle_in.txt").write_text("bundle")

    with (
        patch.object(_submit_job_bundle.api, "get_boto3_session"),
        patch.object(_submit_job_bundle.api, "get_boto3_client") as client_mock,
        patch.object(_submit_job_bundle.api, "get_queue_user_boto3_session"),
        patch.object(S3AssetManager, "hash_assets_and_create_manifest") as mock_hash_assets,
        patch.object(S3AssetManager, "upload_assets") as mock_upload_assets,
        patch.object(_submit_job_bundle.api, "get_deadline_cloud_library_telemetry_client"),
    ):
        client_mock().create_job.side_effect = [MOCK_CREATE_JOB_RESPONSE]
        client_mock().get_queue.side_effect = [MOCK_GET_QUEUE_RESPONSE]
        mock_hash_assets.return_value = [SummaryStatistics(), AssetRootManifest()]
        mock_upload_assets.return_value = _mock_upload_result(str(tmp_path))

        api.create_job_from_job_bundle(
            job_bundle_dir=str(bundle),
            job_parameters=[
                {"name": "InDirs", "value": []},
                {"name": "OutFiles", "value": []},
                {"name": "Refs", "value": []},
            ],
            queue_parameter_definitions=[],
        )

    (asset_group,) = mock_hash_assets.call_args.kwargs["asset_groups"]
    assert asset_group.inputs == {bundle / "bundle_in.txt"}
    parameters = client_mock().create_job.call_args.kwargs["parameters"]
    assert parameters["InFiles"] == {"pathList": [str(bundle / "bundle_in.txt")]}


_URI_TEMPLATE = """specificationVersion: 'jobtemplate-2023-09'
extensions: [EXPR]
name: Uris
parameterDefinitions:
- name: Scene
  type: PATH
  objectType: FILE
  dataFlow: IN
- name: Textures
  type: LIST[PATH]
  objectType: DIRECTORY
  dataFlow: INOUT
- name: Outputs
  type: LIST[PATH]
  objectType: FILE
  dataFlow: OUT
  default: ["s3://bucket/renders/out.exr"]
steps:
- name: S
  script:
    actions:
      onRun:
        command: echo
"""


def test_create_job_from_job_bundle_uri_path_values(fresh_deadline_config, tmp_path, temp_cwd):
    """With the EXPR extension, URI values of PATH and LIST[PATH] parameters reach CreateJob
    unchanged and are not given to job attachments, while local items still are. A URI is
    not a path that needs confirming, so submission does not stop for it."""
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "template.yaml").write_text(_URI_TEMPLATE)
    textures = tmp_path / "textures"
    write_test_asset_files(str(textures), {"t.txt": "t"})

    with (
        patch.object(_submit_job_bundle.api, "get_boto3_session"),
        patch.object(_submit_job_bundle.api, "get_boto3_client") as client_mock,
        patch.object(_submit_job_bundle.api, "get_queue_user_boto3_session"),
        patch.object(S3AssetManager, "hash_assets_and_create_manifest") as mock_hash_assets,
        patch.object(S3AssetManager, "upload_assets") as mock_upload_assets,
        patch.object(_submit_job_bundle.api, "get_deadline_cloud_library_telemetry_client"),
    ):
        client_mock().create_job.side_effect = [MOCK_CREATE_JOB_RESPONSE]
        client_mock().get_queue.side_effect = [MOCK_GET_QUEUE_RESPONSE]
        mock_hash_assets.return_value = [SummaryStatistics(), AssetRootManifest()]
        mock_upload_assets.return_value = _mock_upload_result(str(tmp_path))

        api.create_job_from_job_bundle(
            job_bundle_dir=str(bundle),
            job_parameters=[
                {"name": "Scene", "value": "s3://bucket/scenes/a.blend"},
                {"name": "Textures", "value": ["https://example.com/tex", str(textures)]},
            ],
            queue_parameter_definitions=[],
        )

    (asset_group,) = mock_hash_assets.call_args.kwargs["asset_groups"]
    assert asset_group.inputs == {textures / "t.txt"}
    assert asset_group.outputs == {textures}
    assert asset_group.references == set()
    assert client_mock().create_job.call_args.kwargs["parameters"] == {
        "Scene": {"path": "s3://bucket/scenes/a.blend"},
        "Textures": {"pathList": ["https://example.com/tex", str(textures)]},
        "Outputs": {"pathList": ["s3://bucket/renders/out.exr"]},
    }


def test_create_job_from_job_bundle_only_uris_skips_job_attachments(
    fresh_deadline_config, tmp_path
):
    """When every path is a URI there is nothing for job attachments to do."""
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "template.yaml").write_text(_URI_TEMPLATE)

    with (
        patch.object(_submit_job_bundle.api, "get_boto3_session"),
        patch.object(_submit_job_bundle.api, "get_boto3_client") as client_mock,
        patch.object(_submit_job_bundle.api, "get_queue_user_boto3_session"),
        patch.object(S3AssetManager, "hash_assets_and_create_manifest") as mock_hash_assets,
        patch.object(_submit_job_bundle.api, "get_deadline_cloud_library_telemetry_client"),
    ):
        client_mock().create_job.side_effect = [MOCK_CREATE_JOB_RESPONSE]
        client_mock().get_queue.side_effect = [MOCK_GET_QUEUE_RESPONSE]

        api.create_job_from_job_bundle(
            job_bundle_dir=str(bundle),
            job_parameters=[
                {"name": "Scene", "value": "s3://bucket/scenes/a.blend"},
                {"name": "Textures", "value": '["s3://bucket/tex/"]'},
            ],
            queue_parameter_definitions=[],
        )

    mock_hash_assets.assert_not_called()
    assert "attachments" not in client_mock().create_job.call_args.kwargs
