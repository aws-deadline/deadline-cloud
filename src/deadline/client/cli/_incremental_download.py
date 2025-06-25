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
    This function downloads all the task run outputs from the specified queue, that have become
    available since the last time the function was called. The download_state object
    keeps track of all state needed to keep track of what needs to be downloaded.

    :param farm_id: farm id for the output download
    :param queue_id: queue for scoping output download
    :param download_state: Download state for starting the incremental download
    :param path_mapping_rules: path mapping rules for cross OS path mapping
    :param boto3_session: boto3 session
    :param print_function_callback: Callback to print messages produced in this function.
                Used in the CLI to print to stdout using click.echo. By default, ignores messages.
    :return: updated downloaded state
    """
    # When this function is done, we will be confident that downloads are complete up to
    # this timestamp. We subtract a duration from now() that gives a generous amount of
    # time for the deadline:SearchJobs API's eventual consistency to converge.
    new_completed_timestamp = (
        datetime.datetime.now(datetime.timezone.utc)
        - download_state.eventual_consistency_max_duration
    )

    # TODO the rest of the incremental output download

    # Update the timestamp in the state object to reflect the downloads that were completed
    download_state.downloads_completed_timestamp = new_completed_timestamp

    return download_state
