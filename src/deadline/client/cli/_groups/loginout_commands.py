# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
The `deadline login` and `deadline logout` commands.
"""


import click

from ... import api
from ...api._session import AwsCredentialsType
from ...config import config_file
from .._common import _handle_error


def _cli_on_pending_authorization(**kwargs):
    """
    Callback for `login`, to tell the user that Deadline Cloud Monitor is opening
    """

    if kwargs["credential_type"] == AwsCredentialsType.DEADLINE_CLOUD_MONITOR_LOGIN:
        click.echo("Opening Deadline Cloud Monitor. Please login and then return here.")


@click.command(name="login")
@_handle_error
def cli_login():
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


@click.command(name="logout")
@_handle_error
def cli_logout():
    """
    Logs out of the Deadline Cloud Monitor configured AWS profile.
    """
    api.logout()

    click.echo("Successfully logged out of all Deadline Cloud Monitor AWS profiles")
