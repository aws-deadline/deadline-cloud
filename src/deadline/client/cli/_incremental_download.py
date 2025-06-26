# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

__all__ = ["_incremental_output_download"]

from datetime import datetime, timedelta, timezone
import difflib
import textwrap

from .. import api
from typing import Any, Callable
import boto3
from ..api._list_jobs_by_filter_expression import _list_jobs_by_filter_expression
from ...job_attachments.incremental_downloads.incremental_download_state import (
    IncrementalDownloadState,
    IncrementalDownloadJob,
    _datetimes_to_str,
)
from ._common import _cli_object_repr


def _get_download_candidate_jobs(
    boto3_session: boto3.Session, farm_id: str, queue_id: str, starting_timestamp: datetime
) -> dict[str, dict[str, Any]]:
    """
    Uses deadline:SearchJobs queries to get a dict {job_id: job} of download candidates for the queue.
    This is a superset of all the jobs that have produced any output for download since
    the provided starting_timestamp.

    Args:
        boto3_session: The boto3 session for calling AWS APIs.
        farm_id: The farm ID.
        queue_id: The queue ID in the farm.
        starting_timestamp: The point in time from which to look for new download outputs.

    Returns:
        A dictionary mapping job id to the job as returned by the deadline.search_jobs API.
    """
    # Construct the full set of jobs that may have new available downloads.
    # - Any active job (job with taskRunStatus in READY, ASSIGNED,
    #   STARTING, SCHEDULED, or RUNNING), that has at least one SUCCEEDED task.
    download_candidate_jobs_dict = {
        job["jobId"]: job
        for job in _list_jobs_by_filter_expression(
            boto3_session,
            farm_id,
            queue_id,
            filter_expression={
                "filters": [
                    {
                        "stringFilter": {
                            "name": "TASK_RUN_STATUS",
                            "operator": "EQUAL",
                            "value": status_value,
                        },
                    }
                    # Maximum of 3 filters are permitted, so the 5 statuses are split
                    for status_value in ["READY", "ASSIGNED", "STARTING"]
                ],
                "operator": "OR",
            },
        )
    }
    download_candidate_jobs_dict.update(
        {
            job["jobId"]: job
            for job in _list_jobs_by_filter_expression(
                boto3_session,
                farm_id,
                queue_id,
                filter_expression={
                    "filters": [
                        {
                            "stringFilter": {
                                "name": "TASK_RUN_STATUS",
                                "operator": "EQUAL",
                                "value": status_value,
                            },
                        }
                        for status_value in ["SCHEDULED", "RUNNING"]
                    ],
                    "operator": "OR",
                },
            )
        }
    )
    print(f"DEBUG: Got {len(download_candidate_jobs_dict)} active jobs")
    download_candidate_jobs_dict = {
        job_id: _datetimes_to_str(job)
        for job_id, job in download_candidate_jobs_dict.items()
        if job["taskRunStatusCounts"]["SUCCEEDED"] > 0
    }
    print(
        f"DEBUG: Filtered down to {len(download_candidate_jobs_dict)} active jobs based on SUCCEEDED task filter"
    )

    # - Any recently ended job (job went from active to terminal with a taskRunStatus
    #   in SUSPENDED, CANCELED, FAILED, SUCCEEDED, NOT_COMPATIBLE), that has at least
    #   one SUCCEEDED task. The endedAt timestamp field gets updated when that occurs.
    # TODO: Enable this when filtering by ENDED_AT works.
    # download_candidate_jobs_dict.update(
    #     {
    #         job["jobId"]: job
    #         for job in _list_jobs_by_filter_expression(
    #             boto3_session,
    #             farm_id,
    #             queue_id,
    #             filter_expression={
    #                 "filters": [
    #                     {
    #                         "dateTimeFilter": {
    #                             "name": "ENDED_AT",
    #                             "dateTime": starting_timestamp,
    #                             "operator": "GREATER_THAN_EQUAL_TO",
    #                         }
    #                     }
    #                 ],
    #                 "operator": "AND",
    #             },
    #         )
    #     }
    # )
    # WORKAROUND: Get all jobs with a SUCCEEDED or SUSPENDED task run status, and filter by endedAt client-side.
    #             We want to download everything that is succeeded or suspended, but not
    #             FAILED, CANCELED, or NOT_COMPATIBLE.
    recently_ended_jobs = _list_jobs_by_filter_expression(
        boto3_session,
        farm_id,
        queue_id,
        filter_expression={
            "filters": [
                {
                    "stringFilter": {
                        "name": "TASK_RUN_STATUS",
                        "operator": "EQUAL",
                        "value": status_value,
                    },
                }
                for status_value in ["SUCCEEDED", "SUSPENDED"]
            ],
            "operator": "OR",
        },
    )
    print(f"DEBUG: Got {len(recently_ended_jobs)} succeeded/suspended jobs")
    # Jobs that are submitted with a SUSPENDED status will have no "endedAt" field
    # Filter to jobs that:
    # 1. Have an endedAt field. (jobs submitted as SUSPENDED will not have one)
    # 2. Timestamp endedAt is after the timestamp threshold.
    # 3. The count of SUCCEEDED tasks is positive.
    recently_ended_jobs = [
        job
        for job in recently_ended_jobs
        if "endedAt" in job
        and job["endedAt"] >= starting_timestamp
        and job["taskRunStatusCounts"]["SUCCEEDED"] > 0
    ]
    print(
        f"DEBUG: Filtered down to {len(recently_ended_jobs)} succeeded/suspended jobs based on endedAt timestamp threshold and SUCCEEDED task filter"
    )
    download_candidate_jobs_dict.update(
        {job["jobId"]: _datetimes_to_str(job) for job in recently_ended_jobs}
    )

    return download_candidate_jobs_dict


@api.record_function_latency_telemetry_event()
def _incremental_output_download(
    farm_id: str,
    queue_id: str,
    boto3_session: boto3.Session,
    download_state: IncrementalDownloadState,
    print_function_callback: Callable[[str], None] = lambda msg: None,
) -> IncrementalDownloadState:
    """
    This function downloads all the task run outputs from the specified queue, that have become
    available since the last time the function was called. The download_state object
    keeps track of all state needed to keep track of what needs to be downloaded.

    :param farm_id: farm id for the output download
    :param queue_id: queue for scoping output download
    :param download_state: Download state for starting the incremental download
    :param boto3_session: boto3 session
    :param print_function_callback: Callback to print messages produced in this function.
                Used in the CLI to print to stdout using click.echo. By default, ignores messages.
    :return: updated downloaded state
    """
    # When this function is done, we will be confident that downloads are complete up to
    # this timestamp. We subtract a duration from now() that gives a generous amount of
    # time for the deadline:SearchJobs API's eventual consistency to converge.
    new_completed_timestamp = datetime.now(timezone.utc) - timedelta(
        seconds=download_state.eventual_consistency_max_seconds
    )

    print_function_callback(
        f"Checkpoint state tracks {len(download_state.jobs)} jobs. Retrieving updated data from Deadline Cloud..."
    )

    deadline = boto3_session.client("deadline")

    download_candidate_jobs = _get_download_candidate_jobs(
        boto3_session, farm_id, queue_id, download_state.downloads_completed_timestamp
    )
    download_candidate_job_ids = set(download_candidate_jobs.keys())

    in_progress_jobs = {job.job_id: job.job for job in download_state.jobs}
    in_progress_job_ids = set(in_progress_jobs.keys())

    print_function_callback(
        f"Comparing with {len(download_candidate_jobs)} download candidate jobs..."
    )

    updated_in_progress_jobs = []

    dropped_job_ids = in_progress_job_ids.difference(download_candidate_job_ids)
    updated_job_ids = in_progress_job_ids.intersection(download_candidate_job_ids)
    new_job_ids = download_candidate_job_ids.difference(in_progress_job_ids)
    # The following sets get populated while analyzing the jobs
    unchanged_job_ids = set()
    attachments_free_job_ids = set()

    # Copy the job attachments manifest data from the checkpoint to the new job objects. This data is not returned
    # by deadline:SearchJobs, so we need to call deadline:GetJob on every job to retrieve it. The manifests on a job
    # don't change, so after the call to deadline:GetJob we can cache it indefinitely.
    for job_id in updated_job_ids:
        ip_job = in_progress_jobs[job_id]
        dc_job = download_candidate_jobs[job_id]

        if ip_job["attachments"] is None:
            # Carry over the minimal placeholder identifying the job as not using job attachments
            download_candidate_jobs[job_id] = ip_job
            attachments_free_job_ids.add(job_id)
            updated_in_progress_jobs.append(ip_job)
        else:
            # Carry over the attachments manifest metadata
            dc_job["attachments"] = ip_job["attachments"]
    updated_job_ids.difference_update(attachments_free_job_ids)

    # Prune jobs that we are certain have no changes by looking at its task status counts. A job is unchanged if both of these are true:
    # 1. job["taskRunStatusCounts"]["SUCCEEDED"] stayed the same. Except for when a task is requeued, this count will always increase
    #    when new output is available to download. If a task is requeued, this value could drop and then return to the same value
    #    when new output is generated.
    # 2. job["updatedAt"] stayed the same. If a task is requeued, this timestamp will be updated, so this catches anything missed
    #    by the first check. This timestamp can also change for other reasons, and the later checks that look at session actions
    #    directly will find those cases.
    for job_id in updated_job_ids:
        ip_job = in_progress_jobs[job_id]
        dc_job = download_candidate_jobs[job_id]

        if ip_job["taskRunStatusCounts"]["SUCCEEDED"] == dc_job["taskRunStatusCounts"][
            "SUCCEEDED"
        ] and ip_job.get("updatedAt") == dc_job.get("updatedAt"):
            print_function_callback(f"UNCHANGED Job: {dc_job['name']} ({job_id})")
            unchanged_job_ids.add(job_id)
            updated_in_progress_jobs.append(dc_job)
    updated_job_ids.difference_update(unchanged_job_ids)

    # First make note of any jobs that were dropped, for example if they were canceled or they failed
    for job_id in dropped_job_ids:
        ip_job = in_progress_jobs[job_id]

        print_function_callback(f"DROPPED Job: {ip_job['name']} ({job_id})")
        if ip_job["attachments"] is None:
            print_function_callback("  Job without job attachments no longer needs tracking")
        else:
            print_function_callback(
                "   Job is not a download candidate anymore (likely canceled or failed)"
            )

    # Process all the jobs that have updates
    for job_id in updated_job_ids:
        ip_job = in_progress_jobs[job_id]
        dc_job = download_candidate_jobs[job_id]

        print_function_callback(f"EXISTING Job: {ip_job['name']} ({job_id})")
        print_function_callback(
            f"  Succeeded tasks (before): {ip_job['taskRunStatusCounts']['SUCCEEDED']} / {sum(value for _, value in ip_job['taskRunStatusCounts'].items())}"
        )
        print_function_callback(
            f"  Succeeded tasks (now)   : {dc_job['taskRunStatusCounts']['SUCCEEDED']} / {sum(value for _, value in dc_job['taskRunStatusCounts'].items())}"
        )

        # Use the CLI output format to produce a diff of the changes
        ip_job_repr: list[str] = _cli_object_repr(ip_job).splitlines()
        dc_job_repr: list[str] = _cli_object_repr(dc_job).splitlines()

        for line in difflib.unified_diff(
            ip_job_repr,
            dc_job_repr,
            fromfile="Previous update",
            tofile="Current update",
            lineterm="",
        ):
            print_function_callback(f"  {line}")

        updated_in_progress_jobs.append(dc_job)

    # Process all the jobs that are new
    for job_id in new_job_ids:
        dc_job = download_candidate_jobs[job_id]

        # Call deadline:GetJob to retrieve attachments manifest information
        job = deadline.get_job(jobId=job_id, queueId=queue_id, farmId=farm_id)
        dc_job["attachments"] = job.get("attachments")

        print_function_callback(f"NEW Job: {dc_job['name']} ({job_id})")
        print_function_callback(
            f"  Succeeded tasks: {dc_job['taskRunStatusCounts']['SUCCEEDED']} / {sum(value for _, value in dc_job['taskRunStatusCounts'].items())}"
        )
        if dc_job["attachments"] is None:
            # If the job does not use job attachments, save a minimal placeholder to avoid
            # repeatedly calling deadline:GetJob.
            download_candidate_jobs[job_id] = dc_job = {
                "jobId": job_id,
                "name": dc_job["name"],
                "attachments": None,
            }
            attachments_free_job_ids.add(job_id)
            print_function_callback("  Job does not use job attachments.")
        else:
            print_function_callback(textwrap.indent(_cli_object_repr(dc_job["attachments"]), "  "))

        updated_in_progress_jobs.append(dc_job)
    new_job_ids.difference_update(attachments_free_job_ids)

    download_state.jobs = [IncrementalDownloadJob(job) for job in updated_in_progress_jobs]

    print_function_callback("")
    print_function_callback(f"Identified {len(download_state.jobs)}")

    # TODO the rest of the incremental output download

    # Update the timestamp in the state object to reflect the downloads that were completed
    download_state.downloads_completed_timestamp = max(
        download_state.downloads_started_timestamp, new_completed_timestamp
    )

    print_function_callback("")
    print_function_callback("Summary of incremental output download:")
    print_function_callback(f"  Jobs without job attachments: {len(attachments_free_job_ids)}")
    print_function_callback(f"  Jobs unchanged: {len(unchanged_job_ids)}")
    print_function_callback(f"  Jobs added: {len(new_job_ids)}")
    print_function_callback(f"  Jobs updated: {len(updated_job_ids)}")
    print_function_callback(f"  Jobs dropped: {len(dropped_job_ids)}")

    return download_state
