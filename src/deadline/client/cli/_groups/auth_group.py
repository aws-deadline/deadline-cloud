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
from ...api._session import _modified_logging_level, AwsCredentialsSource
from ...config import config_file, get_setting
from .._common import _apply_cli_options_to_config, _handle_error

JSON_FIELD_PROFILE_NAME = "profile_name"
JSON_FIELD_AUTH_STATUS = "status"
JSON_FIELD_CREDS_SOURCE = "source"
JSON_FIELD_AUTH_API_AVAILABLE = "api_availability"


def _cli_on_pending_authorization(**kwargs):
    """
    Callback for `login`, to tell the user that Deadline Cloud monitor is opening
    """

    if kwargs["credentials_source"] == AwsCredentialsSource.DEADLINE_CLOUD_MONITOR_LOGIN:
        click.echo("Opening Deadline Cloud monitor. Please log in and then return here.")


@click.group(name="auth")
@_handle_error
def cli_auth():
    """
    Commands to handle AWS Deadline Cloud authentication.
    """


@cli_auth.command(name="login")
@_handle_error
def auth_login():
    """
    Logs in to the AWS Deadline Cloud configured AWS profile.

    This is for any profile type that AWS Deadline Cloud knows how to login to
    Currently only supports Deadline Cloud monitor
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
    Logs out of the Deadline Cloud monitor configured AWS profile.
    """
    api.logout()

    click.echo("Successfully logged out of all Deadline Cloud monitor AWS profiles")


@cli_auth.command(name="status")
@click.option("--profile", help="The AWS profile to use.")
@click.option(
    "--output",
    type=click.Choice(
        ["verbose", "json"],
        case_sensitive=False,
    ),
    default="verbose",
    help="Specifies the output format of the messages printed to stdout.\n"
    "VERBOSE: Displays messages in a human-readable text format.\n"
    "JSON: Displays messages in JSON line format, so that the info can be easily "
    "parsed/consumed by custom scripts.",
)
@_handle_error
def auth_status(output, **args):
    """Gets the authentication status for the given AWS profile"""
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

        # always returns True/False
        api_availability_result = api.check_deadline_api_available(config=config)

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
