# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Integ tests for the CLI manifest download commands.
"""

import json
import tempfile
import time
from typing import List
import pytest
from click.testing import CliRunner

from deadline.client.cli import main
from deadline.job_attachments._utils import _float_to_iso_datetime_string
from deadline.job_attachments.asset_manifests.base_manifest import BaseAssetManifest
from deadline.job_attachments.asset_manifests.decode import decode_manifest
from deadline.job_attachments.asset_manifests.hash_algorithms import HashAlgorithm
from deadline.job_attachments.asset_manifests.v2023_03_03.asset_manifest import (
    AssetManifest,
    ManifestPath,
)
from deadline.job_attachments.asset_sync import AssetSync
from .test_utils import JobAttachmentTest, UploadInputFilesOneAssetInCasOutputs
from typing import Optional
from deadline.job_attachments.models import AssetType


@pytest.mark.integ
class TestManifestDownload:
    @pytest.fixture
    def temp_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir_path:
            yield tmpdir_path

    def _assert_input_mainfests_exist(self, files):
        assert "inputs/textures", "brick.png" in files
        assert "inputs/textures", "cloth.png" in files
        assert "inputs/scene.ma" in files

    def _assert_output_manifests_exist(self, files):
        assert "output_file" in files
        assert "output/nested_output_file" in files

    def _assert_dependent_step_output_exist(self, files):
        assert "dependent_step_output_file" in files
        assert "dependent_step_output/nested_output_file" in files

    def check_json_mode_contains_files(self, result):
        # If JSON mode was specified, make sure the output is JSON and contains the downloaded manifest file.
        download = json.loads(result.output)
        assert download is not None
        assert len(download["downloaded"]) == 1
        return download

    def validate_result_exit_code_0(self, job_attachment_test, result):
        # Then
        assert result.exit_code == 0, (
            f"{result.output}, {job_attachment_test.farm_id}, {job_attachment_test.queue_id}"
        )

    def run_cli_with_params(
        self, asset_type, job_attachment_test, job_id, json_output, step_id, temp_dir
    ):
        runner = CliRunner()
        # Download for farm, queue, job to temp dir.
        args = [
            "manifest",
            "download",
            "--farm-id",
            job_attachment_test.farm_id,
            "--queue-id",
            job_attachment_test.queue_id,
            "--job-id",
            job_id,
            "--asset-type",
            asset_type,
            temp_dir,
        ]

        if json_output:
            args.append("--json")

        if step_id:
            args.append("--step-id")
            args.append(step_id)

        result = runner.invoke(main, args)
        return result

    def _setup_create_job(
        self,
        upload_input_files_one_asset_in_cas: UploadInputFilesOneAssetInCasOutputs,
        job_template: str,
        job_attachment_test: JobAttachmentTest,
    ) -> str:
        """
        Create a job with the provided template and wait for the job to be created.
        """
        farm_id: str = job_attachment_test.farm_id
        queue_id: str = job_attachment_test.queue_id

        # Setup failure for the test.
        assert farm_id
        assert queue_id

        # Create a job w/ CAS data already created.
        job_response = job_attachment_test.deadline_client.create_job(
            farmId=farm_id,
            queueId=queue_id,
            attachments=upload_input_files_one_asset_in_cas.attachments.to_dict(),  # type: ignore
            targetTaskRunStatus="SUSPENDED",
            template=job_template,
            templateType="JSON",
            priority=50,
        )

        job_id: str = job_response["jobId"]

        # Wait for the job to be created.
        waiter = job_attachment_test.deadline_client.get_waiter("job_create_complete")
        waiter.wait(
            jobId=job_id,
            queueId=job_attachment_test.queue_id,
            farmId=job_attachment_test.farm_id,
        )

        # Return the created Job ID.
        return job_id

    def validate_manifest_is_not_None(self, manifest_file):
        manifest: BaseAssetManifest = decode_manifest(manifest_file.read())
        assert manifest is not None
        return manifest

    def _sync_mock_output_file(
        self,
        job_attachment_test: JobAttachmentTest,
        job_id: str,
        first_step_name: str,
        second_step_name: Optional[str],
        asset_root_path: str,
        output_file_path: str,
        nested_output_file_path: str,
    ) -> str:
        """
        Create a fake manifest file, upload it as a step output and return the step ID that is dependent.
        job_attachment_test: JobAttachmentTest test harness
        job_id: str, self-explanatory.
        first_step_name: str, independent step.
        second_step_name: Optional[str], dependent on first step.
        asset_root_path: Asset root to upload an output file.
        """
        list_steps_response = job_attachment_test.deadline_client.list_steps(
            farmId=job_attachment_test.farm_id,
            queueId=job_attachment_test.queue_id,
            jobId=job_id,
        )

        # Find the IDs of the steps:
        step_ids = {step["name"]: step["stepId"] for step in list_steps_response["steps"]}
        first_step_id = step_ids[first_step_name]
        second_step_id = step_ids[second_step_name] if second_step_name is not None else None

        # Get the task of the first step so we can upload a fake manifest.
        first_step_first_task_id = job_attachment_test.deadline_client.list_tasks(
            farmId=job_attachment_test.farm_id,
            queueId=job_attachment_test.queue_id,
            jobId=job_id,
            stepId=first_step_id,
        )["tasks"][0]["taskId"]

        assert first_step_first_task_id is not None

        # Create a fake manifest as output and upload it to S3.
        asset_sync = AssetSync(job_attachment_test.farm_id)

        output_manifest = AssetManifest(
            hash_alg=HashAlgorithm("xxh128"),
            total_size=10,
            paths=[
                ManifestPath(path=output_file_path, hash="a", size=1, mtime=167907934333848),
                ManifestPath(
                    path=nested_output_file_path, hash="b", size=1, mtime=1479079344833848
                ),
            ],
        )

        session_action_id_with_time_stamp = (
            f"{_float_to_iso_datetime_string(time.time())}_session-86231a00283449158900410c7d58051e"
        )
        full_output_prefix = job_attachment_test.job_attachment_settings.full_output_prefix(
            farm_id=job_attachment_test.farm_id,
            queue_id=job_attachment_test.queue_id,
            job_id=job_id,
            step_id=first_step_id,
            task_id=first_step_first_task_id,
            session_action_id=session_action_id_with_time_stamp,
        )
        asset_sync._upload_output_manifest_to_s3(
            s3_settings=job_attachment_test.job_attachment_settings,
            output_manifest=output_manifest,
            full_output_prefix=full_output_prefix,
            root_path=asset_root_path,
        )
        return second_step_id if second_step_id is not None else first_step_id

    @pytest.mark.parametrize(
        "json_output",
        [
            pytest.param(True),
            pytest.param(False),
        ],
    )
    @pytest.mark.parametrize(
        "asset_type", [AssetType.INPUT.value, AssetType.OUTPUT.value, AssetType.ALL.value]
    )
    def test_manifest_download_job_asset_type_with_no_step_dependency(
        self,
        temp_dir: str,
        json_output: bool,
        asset_type: str,
        upload_input_files_one_asset_in_cas: UploadInputFilesOneAssetInCasOutputs,
        default_job_template: str,
        job_attachment_test: JobAttachmentTest,
    ):
        # Given:
        # Create a job
        job_id: str = self._setup_create_job(
            upload_input_files_one_asset_in_cas, default_job_template, job_attachment_test
        )

        # Upload the task output manifest for the step
        asset_root_path: str = upload_input_files_one_asset_in_cas.attachments.manifests[0].rootPath
        step_id: str = self._sync_mock_output_file(
            job_attachment_test,
            job_id,
            "custom-step-2",
            None,
            asset_root_path,
            "output_file",
            "output/nested_output_file",
        )

        # When
        result = self.run_cli_with_params(
            asset_type=asset_type,
            job_attachment_test=job_attachment_test,
            job_id=job_id,
            json_output=json_output,
            temp_dir=temp_dir,
            step_id=step_id,
        )

        self.validate_result_exit_code_0(job_attachment_test, result)
        if json_output:
            download = self.check_json_mode_contains_files(result)

            # With JSON mode, we can also check the manifest file itself.
            with open(download["downloaded"][0]["local_manifest_path"]) as manifest_file:
                manifest = self.validate_manifest_is_not_None(manifest_file)

                # Create a list of files we know should be in the input paths.
                files: List[str] = [path.path for path in manifest.paths]
                if asset_type == AssetType.INPUT:
                    self._assert_input_mainfests_exist(files)

                if asset_type == AssetType.OUTPUT:
                    assert "inputs/textures", "brick.png" not in files
                    self._assert_output_manifests_exist(files)

                if asset_type == AssetType.ALL:
                    self._assert_input_mainfests_exist(files)
                    self._assert_output_manifests_exist(files)

    @pytest.mark.parametrize(
        "json_output",
        [
            pytest.param(True),
            pytest.param(False),
        ],
    )
    @pytest.mark.parametrize(
        "asset_type", [AssetType.INPUT.value, AssetType.OUTPUT.value, AssetType.ALL.value, None]
    )
    def test_manifest_download_asset_type_with_job_step_dependency(
        self,
        temp_dir: str,
        asset_type: str,
        json_output: bool,
        upload_input_files_one_asset_in_cas: UploadInputFilesOneAssetInCasOutputs,
        default_job_template_step_step_dependency: str,
        job_attachment_test: JobAttachmentTest,
    ):
        # Create a job, with step step dependency.
        job_id: str = self._setup_create_job(
            upload_input_files_one_asset_in_cas,
            default_job_template_step_step_dependency,
            job_attachment_test,
        )

        # Upload a dependent task output manifest.
        asset_root_path: str = upload_input_files_one_asset_in_cas.attachments.manifests[0].rootPath
        self._sync_mock_output_file(
            job_attachment_test,
            job_id,
            "custom-step",
            "custom-step-2",
            asset_root_path,
            "dependent_step_output_file",
            "dependent_step_output/nested_output_file",
        )

        # Upload the task output manifest
        self._sync_mock_output_file(
            job_attachment_test,
            job_id,
            "custom-step-2",
            None,
            asset_root_path,
            "output_file",
            "output/nested_output_file",
        )

        result = self.run_cli_with_params(
            asset_type=asset_type,
            job_attachment_test=job_attachment_test,
            job_id=job_id,
            json_output=json_output,
            temp_dir=temp_dir,
            step_id=None,
        )

        self.validate_result_exit_code_0(job_attachment_test, result)
        if json_output:
            download = self.check_json_mode_contains_files(result)

            # With JSON mode, we can also check the manifest file itself.
            with open(download["downloaded"][0]["local_manifest_path"]) as manifest_file:
                manifest = self.validate_manifest_is_not_None(manifest_file)

                # Create a list of files we know should be in the input paths.
                files: List[str] = [path.path for path in manifest.paths]
                if asset_type == AssetType.INPUT:
                    self._assert_input_mainfests_exist(files)
                    # No step id in request hence no step dependencies outputs
                    assert "dependent_step_output_file" not in files

                if asset_type == AssetType.OUTPUT:
                    assert "inputs/textures", "brick.png" not in files
                    self._assert_output_manifests_exist(files)
                    self._assert_dependent_step_output_exist(files)

                if asset_type == AssetType.ALL or asset_type is None:
                    self._assert_input_mainfests_exist(files)
                    self._assert_dependent_step_output_exist(files)
                    self._assert_output_manifests_exist(files)

    @pytest.mark.parametrize(
        "json_output",
        [
            pytest.param(True),
            pytest.param(False),
        ],
    )
    def test_manifest_download_output_only_for_one_step(
        self,
        temp_dir: str,
        json_output: bool,
        upload_input_files_one_asset_in_cas: UploadInputFilesOneAssetInCasOutputs,
        default_job_template_step_step_dependency: str,
        job_attachment_test: JobAttachmentTest,
    ):
        # Create a job, with step-step dependency.
        job_id: str = self._setup_create_job(
            upload_input_files_one_asset_in_cas,
            default_job_template_step_step_dependency,
            job_attachment_test,
        )

        # Upload a dependent task output manifest.
        asset_root_path: str = upload_input_files_one_asset_in_cas.attachments.manifests[0].rootPath
        second_step_id: str = self._sync_mock_output_file(
            job_attachment_test,
            job_id,
            "custom-step",
            "custom-step-2",
            asset_root_path,
            "dependent_step_output_file",
            "dependent_step_output/nested_output_file",
        )

        # Upload the task output manifest for the step
        self._sync_mock_output_file(
            job_attachment_test,
            job_id,
            "custom-step-2",
            None,
            asset_root_path,
            "output_file",
            "output/nested_output_file",
        )

        # When
        result = self.run_cli_with_params(
            asset_type="output",
            job_attachment_test=job_attachment_test,
            job_id=job_id,
            json_output=json_output,
            temp_dir=temp_dir,
            step_id=second_step_id,
        )

        self.validate_result_exit_code_0(job_attachment_test, result)
        if json_output:
            download = self.check_json_mode_contains_files(result)

            # With JSON mode, we can also check the manifest file itself.
            with open(download["downloaded"][0]["local_manifest_path"]) as manifest_file:
                manifest = self.validate_manifest_is_not_None(manifest_file)

                # Create a list of files we know should be in the input paths.
                files: List[str] = [path.path for path in manifest.paths]
                assert "inputs/textures", "brick.png" not in files
                assert "dependent_step_output_file" not in files
                self._assert_output_manifests_exist(files)

    @pytest.mark.parametrize(
        "json_output",
        [
            pytest.param(True),
            pytest.param(False),
        ],
    )
    def test_manifest_download_job_input_with_given_step_and_step_dependency(
        self,
        temp_dir: str,
        json_output: bool,
        upload_input_files_one_asset_in_cas: UploadInputFilesOneAssetInCasOutputs,
        default_job_template_step_step_dependency: str,
        job_attachment_test: JobAttachmentTest,
    ):
        # Given:
        # Create a job, with step step dependency.
        job_id: str = self._setup_create_job(
            upload_input_files_one_asset_in_cas,
            default_job_template_step_step_dependency,
            job_attachment_test,
        )

        # Upload the task output manifest for the step
        asset_root_path: str = upload_input_files_one_asset_in_cas.attachments.manifests[0].rootPath
        self._sync_mock_output_file(
            job_attachment_test,
            job_id,
            "custom-step",
            "custom-step-2",
            asset_root_path,
            "dependent_step_output_file",
            "dependent_step_output/nested_output_file",
        )

        # Upload the task output manifest for the step
        second_step_id: str = self._sync_mock_output_file(
            job_attachment_test,
            job_id,
            "custom-step-2",
            None,
            asset_root_path,
            "output_file",
            "output/nested_output_file",
        )

        # When
        result = self.run_cli_with_params(
            asset_type="input",
            job_attachment_test=job_attachment_test,
            job_id=job_id,
            json_output=json_output,
            temp_dir=temp_dir,
            step_id=second_step_id,
        )

        self.validate_result_exit_code_0(job_attachment_test, result)
        if json_output:
            download = self.check_json_mode_contains_files(result)

            # With JSON mode, we can also check the manifest file itself.
            with open(download["downloaded"][0]["local_manifest_path"]) as manifest_file:
                manifest = self.validate_manifest_is_not_None(manifest_file)

                # Create a list of files we know should be in the input paths.
                files: List[str] = [path.path for path in manifest.paths]
                self._assert_input_mainfests_exist(files)
                self._assert_dependent_step_output_exist(files)
