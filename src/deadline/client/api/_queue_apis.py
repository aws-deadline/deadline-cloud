# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

__all__ = ["_incremental_output_download"]

from .. import api
from typing import Optional, Callable
import boto3
from ...job_attachments.incremental_downloads.incremental_download_state import (
    IncrementalDownloadState,
)
import datetime

import os


@api.record_function_latency_telemetry_event()
def _incremental_output_download(
    farm_id: str,
    queue_id: str,
    boto3_session: boto3.Session,
    download_state: IncrementalDownloadState,
    path_mapping_rules: Optional[str] = None,
    print_function_callback: Callable[[str], None] = lambda msg: None,
) -> IncrementalDownloadState:
    """
    Download Job Output data incrementally for all jobs running on a queue as session actions finish.
    The command bootstraps once using a bootstrap lookback specified in minutes and
    continues downloading from the last saved progress thereafter until bootstrap is forced

    :param farm_id: farm id for the output download
    :param queue_id: queue for scoping output download
    since these many minutes at bootstrap. Default value is 0 minutes.
    :param download_state: Download state for starting the incremental download
    :param path_mapping_rules: path mapping rules for cross OS path mapping
    :param boto3_session: boto3 session
    :param print_function_callback: Callback to print messages produced in this function.
                Used in the CLI to print to stdout using click.echo. By default, ignores messages.
    :return: updated downloaded state
    """
    # Download outputs for ongoing jobs using current download progress
    # Right now it is set to no change in progress except setting the last lookback time to now
    updated_downloaded_state: IncrementalDownloadState = download_state
    updated_downloaded_state.last_lookback_time = datetime.datetime.now(datetime.timezone.utc)

    return updated_downloaded_state


def _validate_file_inputs_for_incremental_output_download(
    saved_progress_checkpoint_location: str, path_mapping_rules: Optional[str] = None
) -> bool:
    """
    Validate inputs for incremental output download
    :param saved_progress_checkpoint_location: location of the download progress file
    :param path_mapping_rules: path mapping rules for cross OS path mapping
    :return:
    """

    # Check if download progress location is a valid directory on the os
    if not os.path.isdir(saved_progress_checkpoint_location):
        raise RuntimeError(
            f"Download progress location {saved_progress_checkpoint_location} is not a valid directory"
        )

    # Check that download progress location is writable
    if not os.access(saved_progress_checkpoint_location, os.W_OK):
        raise RuntimeError(
            f"Download progress location {saved_progress_checkpoint_location} exists but is not writable, please provide write permissions"
        )

    # Check that the path mapping rules file exists on the os
    if path_mapping_rules is not None and not os.path.isfile(path_mapping_rules):
        raise RuntimeError(f"Path mapping rules file {path_mapping_rules} does not exist")

    # Check that the path mapping rules file is readable
    elif path_mapping_rules is not None and not os.access(path_mapping_rules, os.R_OK):
        raise RuntimeError(
            f"Path mapping rules file {path_mapping_rules} exists but is not readable, please provide read permissions"
        )

    return True
