# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
The ``deadline job trace-schedule`` command.

Uses the Deadline Cloud batch-get APIs (BatchGetStep, BatchGetTask) via the
``_batch_get`` helper to avoid a per-task / per-step round-trip for large
jobs.
"""

from __future__ import annotations

import datetime
import json
import sys
from typing import Any

import click

from ... import api
from ...config import config_file
from ...exceptions import DeadlineOperationError
from .._common import _apply_cli_options_to_config, _cli_object_repr, _handle_error
from .._main import ContextTrackingCommand
from ._batch_get import batch_get


@click.command(name="trace-schedule", cls=ContextTrackingCommand)
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The farm to use.")
@click.option("--queue-id", help="The queue to use.")
@click.option("--job-id", help="The job to trace.")
@click.option("-v", "--verbose", is_flag=True, help="Output verbose trace details.")
@click.option(
    "--trace-format",
    type=click.Choice(["chrome"], case_sensitive=False),
    help="The tracing format to write.",
)
@click.option("--trace-file", help="The tracing file to write.")
@_handle_error
def cli_job_trace_schedule(verbose, trace_format, trace_file, **args):
    """
    EXPERIMENTAL - Generate statistics from a job with a trace that you can view and explore interactively.

    To visualize the trace output file when providing the options
    "--trace-format chrome --trace-file output.json", open
    https://ui.perfetto.dev in a browser and choose "Open trace file".
    """
    if sys.version_info < (3, 9):
        raise DeadlineOperationError(
            "The trace-schedule command requires Python version 3.9 or later"
        )

    config = _apply_cli_options_to_config(
        required_options={"farm_id", "queue_id", "job_id"}, **args
    )

    farm_id = config_file.get_setting("defaults.farm_id", config=config)
    queue_id = config_file.get_setting("defaults.queue_id", config=config)
    job_id = config_file.get_setting("defaults.job_id", config=config)

    if trace_file and not trace_format:
        raise DeadlineOperationError("Error: Must provide --trace-format with --trace-file.")

    deadline = api.get_boto3_client("deadline", config=config)
    trace_end_utc = datetime.datetime.now(datetime.timezone.utc)

    click.echo("Getting the job...")
    job = deadline.get_job(farmId=farm_id, queueId=queue_id, jobId=job_id)
    job.pop("ResponseMetadata", None)

    if "startedAt" not in job:
        raise DeadlineOperationError("No trace available - Job hasn't started yet, exiting")
    started_at = job["startedAt"]

    sessions = _get_all_sessions(deadline, farm_id, queue_id, job_id)

    click.echo("Getting all the session actions for the job...")
    for session in sessions:
        session["actions"] = _get_all_session_actions(
            deadline, farm_id, queue_id, job_id, session["sessionId"]
        )

    steps, tasks = _fetch_steps_and_tasks(deadline, farm_id, queue_id, job_id, sessions)

    _attach_steps_and_tasks(sessions, steps, tasks)

    worker_ids = {session["workerId"] for session in sessions}
    workers = {worker_id: index for index, worker_id in enumerate(worker_ids)}

    click.echo("Processing the trace data...")
    trace_events, accumulators = _build_trace_events(sessions, workers, started_at, trace_end_utc)

    if verbose:
        click.echo(" ==== TRACE DATA ====")
        click.echo(_cli_object_repr(job))
        click.echo("")
        click.echo(_cli_object_repr(sessions))

    _print_summary(accumulators)

    if trace_file:
        _write_trace_file(trace_file, trace_events, accumulators, job, farm_id, queue_id, job_id)


def _get_all_sessions(deadline, farm_id, queue_id, job_id):
    response = deadline.list_sessions(farmId=farm_id, queueId=queue_id, jobId=job_id)
    sessions = list(response.get("sessions", []))
    while "nextToken" in response:
        response = deadline.list_sessions(
            farmId=farm_id,
            queueId=queue_id,
            jobId=job_id,
            nextToken=response["nextToken"],
        )
        sessions.extend(response.get("sessions", []))
    return sorted(sessions, key=lambda session: session["startedAt"])


def _get_all_session_actions(deadline, farm_id, queue_id, job_id, session_id):
    response = deadline.list_session_actions(
        farmId=farm_id, queueId=queue_id, jobId=job_id, sessionId=session_id
    )
    actions = list(response.get("sessionActions", []))
    while "nextToken" in response:
        response = deadline.list_session_actions(
            farmId=farm_id,
            queueId=queue_id,
            jobId=job_id,
            sessionId=session_id,
            nextToken=response["nextToken"],
        )
        actions.extend(response.get("sessionActions", []))
    return actions


def _fetch_steps_and_tasks(deadline, farm_id, queue_id, job_id, sessions):
    """Collect unique (stepId, taskId) pairs from all sessions and fetch them
    via BatchGetStep / BatchGetTask."""
    step_ids: set[str] = set()
    task_refs: set[tuple[str, str]] = set()
    for session in sessions:
        for action in session.get("actions", []):
            task_run = action.get("definition", {}).get("taskRun")
            if not task_run:
                continue
            step_id = task_run.get("stepId")
            task_id = task_run.get("taskId")
            if step_id and task_id:
                step_ids.add(step_id)
                task_refs.add((step_id, task_id))

    click.echo(f"Getting {len(step_ids)} step(s) via BatchGetStep...")
    steps, step_errors = batch_get(
        deadline=deadline,
        operation="batch_get_step",
        identifiers=[
            {"farmId": farm_id, "queueId": queue_id, "jobId": job_id, "stepId": s} for s in step_ids
        ],
        key_fn=lambda item: item["stepId"],
        items_field="steps",
        id_fields=("farmId", "queueId", "jobId", "stepId"),
    )
    _warn_on_errors("step", step_errors)

    click.echo(f"Getting {len(task_refs)} task(s) via BatchGetTask...")
    tasks, task_errors = batch_get(
        deadline=deadline,
        operation="batch_get_task",
        identifiers=[
            {
                "farmId": farm_id,
                "queueId": queue_id,
                "jobId": job_id,
                "stepId": step_id,
                "taskId": task_id,
            }
            for step_id, task_id in task_refs
        ],
        key_fn=lambda item: item["taskId"],
        items_field="tasks",
        id_fields=("farmId", "queueId", "jobId", "stepId", "taskId"),
    )
    _warn_on_errors("task", task_errors)

    return steps, tasks


def _warn_on_errors(resource_type: str, errors: list[dict]) -> None:
    if not errors:
        return
    click.echo(
        f"Warning: could not retrieve {len(errors)} {resource_type}(s); "
        "the trace will exclude their details.",
        err=True,
    )
    for err in errors[:5]:
        click.echo(f"  {err.get('code')}: {err.get('message', '')}", err=True)
    if len(errors) > 5:
        click.echo(f"  ... and {len(errors) - 5} more.", err=True)


def _attach_steps_and_tasks(sessions, steps, tasks):
    """Attach step/task records to sessions and actions."""
    for index, session in enumerate(sessions):
        session["index"] = index
        for action in session.get("actions", []):
            task_run = action.get("definition", {}).get("taskRun", {})
            step_id = task_run.get("stepId")
            task_id = task_run.get("taskId")
            if not (step_id and task_id):
                continue
            step = steps.get(step_id)
            if step is None:
                continue
            if "step" not in session:
                session["step"] = step
            elif session["step"]["stepId"] != step_id:
                raise DeadlineOperationError(
                    f"Session {session['sessionId']} ran more than one step! When this code was"
                    " written that wasn't possible."
                )
            task = tasks.get(task_id)
            if task is not None:
                action["task"] = task


def _build_trace_events(sessions, workers, started_at, trace_end_utc):
    trace_events: list[dict] = []

    def time_int(timestamp: datetime.datetime):
        return int((timestamp - started_at) / datetime.timedelta(microseconds=1))

    def duration_of(resource):
        try:
            return time_int(resource.get("endedAt", trace_end_utc)) - time_int(
                resource["startedAt"]
            )
        except KeyError:
            return 0

    accumulators = {
        "sessionCount": 0,
        "sessionActionCount": 0,
        "taskRunCount": 0,
        "envActionCount": 0,
        "syncJobAttachmentsCount": 0,
        "sessionDuration": 0,
        "sessionActionDuration": 0,
        "taskRunDuration": 0,
        "envActionDuration": 0,
        "syncJobAttachmentsDuration": 0,
    }

    for session in sessions:
        accumulators["sessionCount"] += 1
        accumulators["sessionDuration"] += duration_of(session)

        pid = workers[session["workerId"]]
        session_event_name = f"{session['step']['name']} - {session['index']}"
        if "endedAt" not in session:
            session_event_name = f"{session_event_name} - In Progress"
        trace_events.append(
            {
                "name": session_event_name,
                "cat": "SESSION",
                "ph": "B",
                "ts": time_int(session["startedAt"]),
                "pid": pid,
                "tid": 0,
                "args": {
                    "sessionId": session["sessionId"],
                    "workerId": session["workerId"],
                    "fleetId": session["fleetId"],
                    "lifecycleStatus": session["lifecycleStatus"],
                },
            }
        )

        for action in session["actions"]:
            accumulators["sessionActionCount"] += 1
            accumulators["sessionActionDuration"] += duration_of(action)

            name = action["sessionActionId"]
            action_type = list(action["definition"].keys())[0]
            if action_type == "taskRun":
                accumulators["taskRunCount"] += 1
                accumulators["taskRunDuration"] += duration_of(action)

                task = action.get("task", {})
                parameters = task.get("parameters", {})
                name = ",".join(
                    f"{param}={list(parameters[param].values())[0]}" for param in parameters
                )
                if not name:
                    name = "<No Task Params>"
            elif action_type in ("envEnter", "envExit"):
                accumulators["envActionCount"] += 1
                accumulators["envActionDuration"] += duration_of(action)

                name = action["definition"][action_type]["environmentId"].split(":")[-1]
            elif action_type == "syncInputJobAttachments":
                accumulators["syncJobAttachmentsCount"] += 1
                accumulators["syncJobAttachmentsDuration"] += duration_of(action)

                if "stepId" in action["definition"][action_type]:
                    name = "Sync Job Attchmnt (Dependencies)"
                else:
                    name = "Sync Job Attchmnt (Submitted)"
            if "endedAt" not in action:
                name = f"{name} - In Progress"
            if "startedAt" in action:
                trace_events.append(
                    {
                        "name": name,
                        "cat": action_type,
                        "ph": "X",
                        "ts": time_int(action["startedAt"]),
                        "dur": duration_of(action),
                        "pid": pid,
                        "tid": 0,
                        "args": {
                            "sessionActionId": action["sessionActionId"],
                            "status": action["status"],
                            "stepName": session["step"]["name"],
                        },
                    }
                )

        trace_events.append(
            {
                "name": session_event_name,
                "cat": "SESSION",
                "ph": "E",
                "ts": time_int(session.get("endedAt", trace_end_utc)),
                "pid": pid,
                "tid": 0,
            }
        )

    return trace_events, accumulators


def _print_summary(accumulators):
    click.echo("")
    click.echo(" ==== SUMMARY ====")
    click.echo("")
    click.echo(f"Session Count: {accumulators['sessionCount']}")
    session_total_duration = accumulators["sessionDuration"]
    click.echo(f"Session Total Duration: {datetime.timedelta(microseconds=session_total_duration)}")
    click.echo(f"Session Action Count: {accumulators['sessionActionCount']}")
    click.echo(
        f"Session Action Total Duration: "
        f"{datetime.timedelta(microseconds=accumulators['sessionActionDuration'])}"
    )
    click.echo(f"Task Run Count: {accumulators['taskRunCount']}")
    task_run_total_duration = accumulators["taskRunDuration"]
    click.echo(
        f"Task Run Total Duration: {datetime.timedelta(microseconds=task_run_total_duration)} "
        f"({100 * task_run_total_duration / session_total_duration:.1f}%)"
    )
    click.echo(
        f"Non-Task Run Count: {accumulators['sessionActionCount'] - accumulators['taskRunCount']}"
    )
    non_task_run_total_duration = (
        accumulators["sessionActionDuration"] - accumulators["taskRunDuration"]
    )
    click.echo(
        f"Non-Task Run Total Duration: "
        f"{datetime.timedelta(microseconds=non_task_run_total_duration)} "
        f"({100 * non_task_run_total_duration / session_total_duration:.1f}%)"
    )
    click.echo(f"Sync Job Attachments Count: {accumulators['syncJobAttachmentsCount']}")
    sync_job_attachments_total_duration = accumulators["syncJobAttachmentsDuration"]
    click.echo(
        f"Sync Job Attachments Total Duration: "
        f"{datetime.timedelta(microseconds=sync_job_attachments_total_duration)} "
        f"({100 * sync_job_attachments_total_duration / session_total_duration:.1f}%)"
    )
    click.echo(f"Env Action Count: {accumulators['envActionCount']}")
    env_action_total_duration = accumulators["envActionDuration"]
    click.echo(
        f"Env Action Total Duration: "
        f"{datetime.timedelta(microseconds=env_action_total_duration)} "
        f"({100 * env_action_total_duration / session_total_duration:.1f}%)"
    )
    click.echo("")
    within_session_overhead_duration = (
        accumulators["sessionDuration"] - accumulators["sessionActionDuration"]
    )
    click.echo(
        f"Within-session Overhead Duration: "
        f"{datetime.timedelta(microseconds=within_session_overhead_duration)} "
        f"({100 * within_session_overhead_duration / session_total_duration:.1f}%)"
    )
    click.echo(
        f"Within-session Overhead Duration Per Action: "
        f"{datetime.timedelta(microseconds=within_session_overhead_duration / accumulators['sessionActionCount'])}"
    )


def _write_trace_file(trace_file, trace_events, accumulators, job, farm_id, queue_id, job_id):
    tracing_data: dict[str, Any] = {
        "traceEvents": trace_events,
        "otherData": {
            "farmId": farm_id,
            "queueId": queue_id,
            "jobId": job_id,
            "jobName": job["name"],
            "startedAt": job["startedAt"].isoformat(sep="T"),
        },
    }
    if "endedAt" in job:
        tracing_data["otherData"]["endedAt"] = job["endedAt"].isoformat(sep="T")
    tracing_data["otherData"].update(accumulators)

    with open(trace_file, "w", encoding="utf8") as f:
        json.dump(tracing_data, f, indent=1)
