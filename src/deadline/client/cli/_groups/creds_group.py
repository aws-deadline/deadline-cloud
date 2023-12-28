# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
All the `deadline creds` commands:
    * login
    * logout
    * status
"""
import click
import json
import logging

from ... import api
from ...api._session import _modified_logging_level, AwsCredentialsType
from ...config import config_file, get_setting
from .._common import _apply_cli_options_to_config, _handle_error

JSON_FIELD_PROFILE_NAME = "profile_name"
JSON_FIELD_CRED_STATUS = "status"
JSON_FIELD_CRED_TYPE = "type"
JSON_FIELD_CRED_API_AVAILABLE = "api_availability"


def _cli_on_pending_authorization(**kwargs):
    """
    Callback for `login`, to tell the user that Deadline Cloud Monitor is opening
    """

    if kwargs["credential_type"] == AwsCredentialsType.DEADLINE_CLOUD_MONITOR_LOGIN:
        click.echo("Opening Deadline Cloud Monitor. Please login and then return here.")


@click.group(name="creds")
@_handle_error
def cli_creds():
    """
    Commands to work with Amazon Deadline Cloud credentials.
    """


@cli_creds.command(name="login")
@_handle_error
def creds_login():
    """
    Logs in to the Amazon Deadline Cloud configured AWS profile.

    This is for any profile type that Amazon Deadline Cloud knows how to login to
    Currently only supports Deadline Cloud Monitor
    """
    click.echo(
        f"Logging into AWS Profile {config_file.get_setting('defaults.aws_profile_name')!r} for Amazon Deadline Cloud"
    )

    message = api.login(
        on_pending_authorization=_cli_on_pending_authorization, on_cancellation_check=None
    )

    click.echo(f"\nSuccessfully logged in: {message}\n")


@cli_creds.command(name="logout")
@_handle_error
def creds_logout():
    """
    Logs out of the Deadline Cloud Monitor configured AWS profile.
    """
    api.logout()

    click.echo("Successfully logged out of all Deadline Cloud Monitor AWS profiles")


@cli_creds.command(name="status")
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
def creds_status(output, **args):
    """EXPERIMENTAL - Gets the status of the credentials for the given AWS profile"""
    # Get a temporary config object with the standard options handled
    config = _apply_cli_options_to_config(**args)
    profile_name = get_setting("defaults.aws_profile_name", config=config)
    is_json_format = True if output == "json" else False

    with _modified_logging_level(logging.getLogger("deadline.client.api"), logging.CRITICAL):
        # always returns enum in AwsCredentialsType
        creds_type = api.get_credentials_type(config=config)
        creds_type_result = creds_type.name

        # always returns enum in AwsCredentialsStatus
        creds_status = api.check_credentials_status(config=config)
        creds_status_results = creds_status.name

        # always returns True/False
        api_availability_result = api.check_deadline_api_available(config=config)

    if not is_json_format:
        width = 17
        click.echo(f"{'Profile Name:': >{width}} {profile_name}")
        click.echo(f"{'Type:': >{width}} {creds_type_result}")
        click.echo(f"{'Status:': >{width}} {creds_status_results}")
        click.echo(f"{'API Availability:': >{width}} {api_availability_result}")
    else:
        json_output = {
            JSON_FIELD_PROFILE_NAME: profile_name,
            JSON_FIELD_CRED_TYPE: creds_type_result,
            JSON_FIELD_CRED_STATUS: creds_status_results,
            JSON_FIELD_CRED_API_AVAILABLE: api_availability_result,
        }
        click.echo(json.dumps(json_output, ensure_ascii=True))
