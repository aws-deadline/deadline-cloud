# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
The Amazon Deadline Cloud CLI interface.
"""

import logging
from logging import getLogger

import click

from .. import version
from ..config import get_setting, get_setting_default
from ._common import PROMPT_WHEN_COMPLETE
from .groups.bundle_group import cli_bundle
from .groups.config_group import cli_config
from .groups.farm_group import cli_farm
from .groups.fleet_group import cli_fleet
from .groups.handle_web_url_command import cli_handle_web_url
from .groups.job_group import cli_job
from .groups.loginout_commands import cli_login, cli_logout
from .groups.queue_group import cli_queue
from .groups.worker_group import cli_worker

logger = getLogger(__name__)

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])
_DEADLINE_LOG_LEVELS = [
    "ERROR",
    "WARNING",
    "INFO",
    "DEBUG",
]  # Log Levels Amazon Deadline Cloud Allows

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
@click.version_option(version)
@click.option(
    "--log-level",
    type=click.Choice(_DEADLINE_LOG_LEVELS, case_sensitive=False),
    default=_CLI_DEFAULT_LOG_LEVEL,
    help="Set the logging level.",
)
@click.pass_context
def cli(ctx: click.Context, log_level: str):
    """
    The Amazon Deadline Cloud CLI provides functionality to work with the Amazon Amazon Deadline Cloud
    closed beta service.
    """
    logging.basicConfig(level=logging.getLevelName(log_level))
    if log_level == "DEBUG":
        logger.debug("Debug logging is on")

    ctx.ensure_object(dict)
    # By default don't prompt when the operation is complete
    ctx.obj[PROMPT_WHEN_COMPLETE] = False


cli.add_command(cli_bundle)
cli.add_command(cli_config)
cli.add_command(cli_farm)
cli.add_command(cli_fleet)
cli.add_command(cli_handle_web_url)
cli.add_command(cli_job)
cli.add_command(cli_login)
cli.add_command(cli_logout)
cli.add_command(cli_queue)
cli.add_command(cli_worker)
