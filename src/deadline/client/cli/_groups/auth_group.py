# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
All the `deadline auth` commands:
    * login
    * logout
    * status
"""

import click
import json
import logging

from ... import api
from .._main import deadline as main
from ...api._session import _modified_logging_level, AwsCredentialsSource
from ...config import config_file, get_setting
from .._common import (
    _OUTPUT_FORMAT_HELP,
    _apply_cli_options_to_config,
    _handle_error,
    _resolve_output_format,
)

JSON_FIELD_PROFILE_NAME = "profile_name"
JSON_FIELD_AUTH_STATUS = "status"
JSON_FIELD_CREDS_SOURCE = "source"
JSON_FIELD_AUTH_API_AVAILABLE = "api_availability"


def _cli_on_pending_authorization(**kwargs):
    """
    Callback for `login`, to tell the user which login flow is opening
    """

    if kwargs["credentials_source"] == AwsCredentialsSource.DEADLINE_CLOUD_MONITOR_LOGIN:
        click.echo("Opening Deadline Cloud monitor. Please log in and then return here.")
    elif kwargs["credentials_source"] == AwsCredentialsSource.AWS_CONSOLE_LOGIN:
        # Deadline Cloud monitor opens the browser for the console sign-in itself.
        click.echo(
            "Opening Deadline Cloud monitor to sign in with the AWS Console. "
            "Please sign in and then return here."
        )


@main.group(name="auth")
@_handle_error
def cli_auth():
    """
    Manage authentication for Deadline Cloud. Log in via Deadline Cloud
    monitor, log out, or check the status of your current AWS credentials.

    \b
    Learn more about [Deadline Cloud monitor](https://docs.aws.amazon.com/deadline-cloud/latest/userguide/working-with-deadline-monitor.html)
    """


@cli_auth.command(name="login")
@_handle_error
def auth_login():
    """
    Opens Deadline Cloud monitor to log in to your farm. Supports profiles
    created by Deadline Cloud monitor or by AWS Console sign-in.
    """
    click.echo(
        f"Logging into AWS Profile {config_file.get_setting('defaults.aws_profile_name')!r} for AWS Deadline Cloud"
    )

    message = api.login(
        on_pending_authorization=_cli_on_pending_authorization, on_cancellation_check=None
    )

    click.echo(f"\nSuccessfully logged in: {message}\n")


@cli_auth.command(name="logout")
@_handle_error
def auth_logout():
    """
    Logs out of the configured AWS profile, if it was created by Deadline Cloud monitor
    or by AWS Console sign-in.
    """
    # Echo what logout actually did. The message names the profile type, so hardcoding one
    # here would misreport the other. The monitor path returns the subprocess's stdout,
    # which can be empty, so fall back to a generic confirmation.
    message = api.logout().strip()
    click.echo(message or "Successfully logged out")


@cli_auth.command(name="status")
@click.option("--profile", help="The AWS profile to use.")
@click.option(
    "--output",
    type=click.Choice(
        ["verbose", "json"],
        case_sensitive=False,
    ),
    default=None,
    help=_OUTPUT_FORMAT_HELP,
)
@_handle_error
def auth_status(output, **args):
    """Gets the status of the selected AWS profile, including its name, whether it was created by
    Deadline Cloud monitor, and whether Deadline Cloud APIs are accessible.
    """
    output = _resolve_output_format(output)
    # Get a temporary config object with the standard options handled
    config = _apply_cli_options_to_config(**args)
    profile_name = get_setting("defaults.aws_profile_name", config=config)
    is_json_format = True if output == "json" else False

    with _modified_logging_level(logging.getLogger("deadline.client.api"), logging.CRITICAL):
        # always returns enum in AwsCredentialsSource
        creds_source = api.get_credentials_source(config=config)
        creds_source_result = creds_source.name

        # always returns enum in AwsAuthenticationStatus
        auth_status = api.check_authentication_status(config=config)
        auth_status_results = auth_status.name

        # API availability is equivalent to being AUTHENTICATED: both rely on the
        # same deadline:ListFarms probe. Derive it from the single status check
        # above rather than making a second, redundant ListFarms call.
        api_availability_result = auth_status == api.AwsAuthenticationStatus.AUTHENTICATED

    if not is_json_format:
        width = 17
        click.echo(f"{'Profile Name:': >{width}} {profile_name}")
        click.echo(f"{'Source:': >{width}} {creds_source_result}")
        click.echo(f"{'Status:': >{width}} {auth_status_results}")
        click.echo(f"{'API Availability:': >{width}} {api_availability_result}")
    else:
        json_output = {
            JSON_FIELD_PROFILE_NAME: profile_name,
            JSON_FIELD_CREDS_SOURCE: creds_source_result,
            JSON_FIELD_AUTH_STATUS: auth_status_results,
            JSON_FIELD_AUTH_API_AVAILABLE: api_availability_result,
        }
        click.echo(json.dumps(json_output, ensure_ascii=True))
