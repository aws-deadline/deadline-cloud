# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
The AWS Deadline Cloud CLI interface.
"""

import logging
import sys
from logging import getLogger
from typing import Optional

import click

from .. import version
from ..api._session import session_context
from ..config import get_setting, get_setting_default
from ._common import _PROMPT_WHEN_COMPLETE

logger = getLogger(__name__)

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])
_DEADLINE_LOG_LEVELS = [
    "ERROR",
    "WARNING",
    "INFO",
    "DEBUG",
]  # Log Levels AWS Deadline Cloud Allows


def _get_default_log_level() -> str:
    """
    Get the default log level from the config file.
    """
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
    return _CLI_DEFAULT_LOG_LEVEL


class ContextTrackingCommand(click.Command):
    """
    Adds the current CLI command name to User Agent headers in boto requests
    """

    def invoke(self, ctx: click.Context):
        # This is a global variable used to modify User Agent header in the default boto config
        session_context["cli-command-name"] = ctx.command_path.replace(" ", ".")
        return super().invoke(ctx)


class ContextTrackingGroup(click.Group):
    """
    Adds the current CLI command name to User Agent headers in boto requests
    """

    # Special value documented in Click to make this group class the default
    # See https://click.palletsprojects.com/en/stable/api/#click.Group.group_class
    group_class = type

    # Special value documented in Click to make this command class the default
    # See https://click.palletsprojects.com/en/stable/api/#click.Group.command_class
    command_class = ContextTrackingCommand


@click.group(cls=ContextTrackingGroup, context_settings=CONTEXT_SETTINGS)
@click.version_option(version=version, prog_name="deadline")
@click.option(
    "--log-level",
    type=click.Choice(_DEADLINE_LOG_LEVELS, case_sensitive=False),
    default=None,
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
def main(ctx: click.Context, log_level: Optional[str], redirect_output: str, redirect_mode: str):
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
    if log_level is None:
        log_level = _get_default_log_level()

    logging.basicConfig(level=log_level)
    if log_level == "DEBUG":
        logger.debug("Debug logging is on")

    ctx.ensure_object(dict)
    # By default don't prompt when the operation is complete
    ctx.obj[_PROMPT_WHEN_COMPLETE] = False
