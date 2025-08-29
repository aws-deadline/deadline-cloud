# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
The AWS Deadline Cloud CLI interface.
"""

import logging
from logging import getLogger
import sys

import click

from .. import version
from ..config import get_setting, get_setting_default
from ._common import _PROMPT_WHEN_COMPLETE
from ._groups.bundle_group import cli_bundle
from ._groups.config_group import cli_config
from ._groups.auth_group import cli_auth
from ._groups.farm_group import cli_farm
from ._groups.fleet_group import cli_fleet
from ._groups.handle_web_url_command import cli_handle_web_url
from ._groups.job_group import cli_job
from ._groups.queue_group import cli_queue
from ._groups.worker_group import cli_worker
from ._groups.attachment_group import cli_attachment
from ._groups.manifest_group import cli_manifest

logger = getLogger(__name__)

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])
_DEADLINE_LOG_LEVELS = [
    "ERROR",
    "WARNING",
    "INFO",
    "DEBUG",
]  # Log Levels AWS Deadline Cloud Allows

# Set the default log level based on the setting, must do here so we can pass into the click option
_SETTING_LOG_LEVEL = get_setting("settings.log_level").upper()
_DEFAULT_LOG_LEVEL = get_setting_default("settings.log_level")
_CLI_DEFAULT_LOG_LEVEL = _DEFAULT_LOG_LEVEL
if _SETTING_LOG_LEVEL not in _DEADLINE_LOG_LEVELS:
    logger.warning(
        f"Log Level '{_SETTING_LOG_LEVEL}' not in {_DEADLINE_LOG_LEVELS}. Defaulting to {_DEFAULT_LOG_LEVEL}"
    )
else:
    _CLI_DEFAULT_LOG_LEVEL = _SETTING_LOG_LEVEL


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(version=version, prog_name="deadline")
@click.option(
    "--log-level",
    type=click.Choice(_DEADLINE_LOG_LEVELS, case_sensitive=False),
    default=_CLI_DEFAULT_LOG_LEVEL,
    help="Set the logging level.",
)
@click.option(
    "--redirect-output",
    help="Redirects stdout and stderr messages to append to the specified file. "
    "Useful for the 'deadlinew' command which does not produce terminal output by default on Windows.",
)
@click.option(
    "--redirect-mode",
    type=click.Choice(["append", "replace"], case_sensitive=False),
    default="append",
    help="When using the --redirect-output option, controls whether to append to or replace the output file.",
)
@click.pass_context
def main(ctx: click.Context, log_level: str, redirect_output: str, redirect_mode: str):
    """
    The AWS Deadline Cloud CLI provides functionality to interact with the AWS Deadline Cloud
    service.
    """
    if redirect_output:
        # Set both stdout and stderr to write to the specified file, writing in line buffering mode
        if redirect_mode == "append":
            open_mode = "a"
        else:
            open_mode = "w"
        sys.stdout = sys.stderr = open(redirect_output, open_mode, encoding="utf-8", buffering=1)
    logging.basicConfig(level=log_level)
    if log_level == "DEBUG":
        logger.debug("Debug logging is on")

    ctx.ensure_object(dict)
    # By default don't prompt when the operation is complete
    ctx.obj[_PROMPT_WHEN_COMPLETE] = False


main.add_command(cli_bundle)
main.add_command(cli_config)
main.add_command(cli_auth)
main.add_command(cli_farm)
main.add_command(cli_fleet)
main.add_command(cli_handle_web_url)
main.add_command(cli_job)
main.add_command(cli_queue)
main.add_command(cli_worker)

main.add_command(cli_attachment)
main.add_command(cli_manifest)
