# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
A set of utilities developed for this set of tests.
"""

import contextlib
import os
import tempfile
from typing import Dict, Optional, Tuple
from unittest.mock import patch, Mock
import boto3

from deadline.client.api import _submit_job_bundle
from deadline.job_attachments.models import (
    Attachments,
    ManifestProperties,
    PathFormat,
)
from deadline.job_attachments.upload import S3AssetManager
from deadline.job_attachments.progress_tracker import SummaryStatistics

from .shared_constants import (
    MOCK_CREATE_JOB_RESPONSE,
    MOCK_GET_JOB_RESPONSE,
    MOCK_GET_QUEUE_ENVIRONMENT_RESPONSES,
    MOCK_LIST_QUEUE_ENVIRONMENTS_RESPONSE,
)

if os.name == "nt":

    def _batfile_quote(s: str) -> str:
        """Quotes the string so it can be echoed in a batfile script."""
        for replacement in [
            ("%", "%%"),
            ('"', '""'),
            ("^", "^^"),
            ("&", "^&"),
            ("<", "^<"),
            (">", "^>"),
            ("|", "^|"),
        ]:
            s = s.replace(*replacement)
        return f"{s}"

    _file_extension = ".bat"
    _header = "@echo off\n"

    def _format_sleep(seconds: float) -> str:
        return f'powershell -nop -c "{{sleep {seconds}}}" > nul\n'

    def _format_line(line: str) -> str:
        return f"echo.{line}\n"

    def _format_exit(exit_code: int) -> str:
        return f"exit {exit_code}\n"

    def _format_args_check(args: Tuple[str], args_index: int) -> str:
        result = ["set MATCHED=YES\n"]
        for i, arg in enumerate(args, start=1):
            result.append(f'if not "%{i}" == "{_batfile_quote(arg)}" set MATCHED=NO\n')
        result.append(f'if not "%{len(args) + 1}" == "" set MATCHED=NO\n')
        result.append(f"if %MATCHED% == NO goto no_match_{args_index}\n")
        return "".join(result)

    def _format_end_args_check(args_index: int) -> str:
        return f":no_match_{args_index}\n"

else:
    import shlex

    _file_extension = ".sh"
    _header = "#!/bin/sh\n"

    def _format_sleep(seconds: float) -> str:
        return f"sleep {seconds}\n"

    def _format_line(line: str) -> str:
        return f"echo {shlex.quote(line)}\n"

    def _format_exit(exit_code: int) -> str:
        return f"exit {exit_code}\n"

    def _format_args_check(args: Tuple[str], args_index: int) -> str:
        result = [f"if [ $# == {len(args)} ] "]
        for i, arg in enumerate(args, start=1):
            result.append(f'&& [ "${i}" == {shlex.quote(arg)} ] ')
        result.append("; then ")  # lack of \n here puts the next commend with the `then`
        return "".join(result)

    def _format_end_args_check(args_index: int) -> str:
        return "fi\n"


def _format_output_and_exit(program_output: str, exit_code: int) -> str:
    result = []
    for line in program_output.splitlines():
        result.append(_format_line(line))
    result.append(_format_exit(exit_code))
    return "".join(result)


@contextlib.contextmanager
def program_that_prints_output(
    program_output: str,
    exit_code: int,
    *,
    sleep_seconds=0.1,
    conditional_outputs: Optional[Dict[Tuple[str], Tuple[str, int]]] = None,
):
    """
    This context manager creates a program that prints the specified output, then returns
    the specified exit code.

    By default, the program sleeps for 0.1 seconds, so tests that check for a thread or process
    immediately after launching can do so.

    If conditional outputs are provided, they change the output and exit code for the specified args.
    """
    try:
        with tempfile.NamedTemporaryFile(
            mode="w+t", suffix=_file_extension, encoding="utf8", delete=False
        ) as temp:
            temp.write(_header)
            if sleep_seconds > 0:
                temp.write(_format_sleep(sleep_seconds))
            # Handle each conditional output
            if conditional_outputs:
                for args_index, (
                    args,
                    (conditional_program_output, conditional_exit_code),
                ) in enumerate(conditional_outputs.items()):
                    temp.write(_format_args_check(args, args_index))
                    temp.write(
                        _format_output_and_exit(conditional_program_output, conditional_exit_code)
                    )
                    temp.write(_format_end_args_check(args_index))
            # Handle the default output
            temp.write(_format_output_and_exit(program_output, exit_code))
            temp.flush()
            if os.name != "nt":
                os.chmod(temp.name, 0o500)

        yield temp.name
    finally:
        os.remove(temp.name)


def write_test_asset_files(assets_dir: str, asset_contents: Dict[str, str]):
    """
    Write a set of asset contents files to the provided assets directory.
    Each key of asset_contents is a relative path from assets_dir, and
    each value is what to write to the file.
    """
    for rel_path, contents in asset_contents.items():
        path = os.path.join(assets_dir, rel_path)
        if not os.path.isdir(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))
        if isinstance(contents, str):
            with open(path, "w", encoding="utf8") as f:
                f.write(contents)
        elif isinstance(contents, bytes):
            with open(path, "wb") as f:
                f.write(contents)
        else:
            raise ValueError("The contents provided in asset_contents must be either str or bytes.")


@contextlib.contextmanager
def patch_calls_for_create_job_from_job_bundle(
    *,
    create_job_return=MOCK_CREATE_JOB_RESPONSE,
    get_job_return=MOCK_GET_JOB_RESPONSE,
    get_queue_return={
        "displayName": "Test Queue",
        "jobAttachmentSettings": {"s3BucketName": "mock", "rootPrefix": "root"},
    },
    queue_paramdefs=[],
    upload_assets_return=[
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
    ],
):
    """This is a context manager to help test deadline.client.api.create_job_from_job_bundle.

    It patches a bunch of functions that might call to the internet, or need to be wrapped to test JA effectively.
    See the assignments in this function implementation to access the mocked values."""
    with patch.object(
        _submit_job_bundle.api, "get_deadline_cloud_library_telemetry_client"
    ) as mock_get_deadline_cloud_library_telemetry_client, patch.object(
        _submit_job_bundle.api, "get_queue_parameter_definitions", return_value=queue_paramdefs
    ) as mock_get_queue_parameter_definitions, patch.object(
        _submit_job_bundle.api,
        "create_job_from_job_bundle",
        wraps=_submit_job_bundle.create_job_from_job_bundle,
    ) as mock_create_job_from_job_bundle, patch.object(
        _submit_job_bundle,
        "_generate_message_for_asset_paths",
        wraps=_submit_job_bundle._generate_message_for_asset_paths,
    ) as mock_generate_message_for_asset_paths, patch.object(
        _submit_job_bundle, "_hash_attachments", wraps=_submit_job_bundle._hash_attachments
    ) as mock_hash_attachments, patch(
        "deadline.job_attachments.upload.S3AssetUploader"
    ), patch.object(
        S3AssetManager, "upload_assets", return_value=upload_assets_return
    ) as mock_upload_assets, patch.object(
        _submit_job_bundle.api, "get_queue_user_boto3_session"
    ) as mock_get_queue_user_boto3_session, patch.object(boto3, "Session") as boto3_session_mock:
        mock = Mock()
        # Read these assignments to see what to access
        mock.get_boto3_client = boto3_session_mock().client
        mock.get_deadline_cloud_library_telemetry_client = (
            mock_get_deadline_cloud_library_telemetry_client
        )
        mock.create_job_from_job_bundle = mock_create_job_from_job_bundle
        mock.get_queue_parameter_definitions = mock_get_queue_parameter_definitions
        mock.generate_message_for_asset_paths = mock_generate_message_for_asset_paths
        mock.hash_attachments = mock_hash_attachments
        mock.upload_assets = mock_upload_assets
        mock.get_queue_user_boto3_session = mock_get_queue_user_boto3_session
        mock.Session = boto3_session_mock

        mock.get_boto3_client().create_job.return_value = create_job_return
        mock.get_boto3_client().get_job.return_value = get_job_return
        mock.get_boto3_client().get_queue.return_value = get_queue_return
        mock.get_boto3_client().list_queue_environments.return_value = (
            MOCK_LIST_QUEUE_ENVIRONMENTS_RESPONSE
        )
        mock.get_boto3_client().get_queue_environment.side_effect = (
            MOCK_GET_QUEUE_ENVIRONMENT_RESPONSES
        )

        yield mock
