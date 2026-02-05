# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""CLI entry point for the interactive job attachments browser.

The TUI rendering code lives in ``_browse_tui.py`` and requires the ``rich``
package (install via ``pip install 'deadline[tui]'``).  This module only
registers the Click command and does the conditional import at invocation time
so the rest of the CLI works without ``rich`` installed.
"""

import sys
from configparser import ConfigParser
from typing import Optional

import click

from deadline.client import api
from deadline.client.config import config_file
from deadline.job_attachments.models import JobAttachmentS3Settings

from .._common import _apply_cli_options_to_config, _handle_error
from .._main import deadline as main


def _check_tui_installed() -> None:
    """Raise a clear ClickException if the ``rich`` package is missing."""
    try:
        import rich  # noqa: F401
    except ImportError:
        raise click.ClickException(
            "The TUI requires the 'rich' package. Install it with: pip install 'deadline[tui]'"
        )


@main.command(name="browse")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The AWS Deadline Cloud Farm to use.")
@click.option("--queue-id", help="The AWS Deadline Cloud Queue to use.")
@click.option(
    "--job-id", help="The AWS Deadline Cloud Job to browse. If omitted, shows job selector."
)
@_handle_error
def cli_browse(**args):
    """
    Interactively browse input and output files of a Deadline Cloud job.

    If --job-id is not provided, shows a job selection screen.
    Navigate with arrow keys, Enter to select, 'd' to download, 'q' to quit.

    Requires the [tui] extra: pip install 'deadline[tui]'
    """
    _check_tui_installed()
    from ._browse_tui import JobBrowserTUI, JobSelectorTUI

    config: Optional[ConfigParser] = _apply_cli_options_to_config(
        required_options={"farm_id", "queue_id"}, **args
    )
    farm_id = config_file.get_setting("defaults.farm_id", config=config)
    queue_id = config_file.get_setting("defaults.queue_id", config=config)
    job_id = args.get("job_id") or config_file.get_setting("defaults.job_id", config=config)

    if not sys.stdin.isatty():
        raise click.ClickException("This command requires an interactive terminal")

    boto3_session = api.get_boto3_session(config=config)
    deadline = api.get_boto3_client("deadline", config=config)

    # If no job_id, show job selector
    if not job_id:
        selector = JobSelectorTUI(farm_id, queue_id, deadline)
        job_id = selector.run()
        if not job_id:
            return

    job = deadline.get_job(farmId=farm_id, queueId=queue_id, jobId=job_id)
    job_name = job["name"]
    job_status = job.get("taskRunStatus", "UNKNOWN")

    queue = deadline.get_queue(farmId=farm_id, queueId=queue_id)
    if "jobAttachmentSettings" not in queue:
        raise click.ClickException("Queue does not have job attachments configured")

    s3_settings = JobAttachmentS3Settings(**queue["jobAttachmentSettings"])
    queue_role_session = api.get_queue_user_boto3_session(
        deadline=deadline,
        config=config,
        farm_id=farm_id,
        queue_id=queue_id,
        queue_display_name=queue["displayName"],
    )

    browser = JobBrowserTUI(
        farm_id=farm_id,
        queue_id=queue_id,
        job_id=job_id,
        job_name=job_name,
        job_status=job_status,
        boto3_session=boto3_session,
        queue_role_session=queue_role_session,
        s3_settings=s3_settings,
    )
    browser.run()
