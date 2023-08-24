# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
The `deadline login` and `deadline logout` commands.
"""


import click

from ... import api
from ...api._session import AwsCredentialsType
from ...config import config_file
from .._common import handle_error


def _cli_on_pending_authorization(**kwargs):
    """
    Callback for `login`, to tell the user that Cloud Companion is opening
    """

    if kwargs["credential_type"] == AwsCredentialsType.CLOUD_COMPANION_LOGIN:
        click.echo("Opening Cloud Companion. Please login before returning here.")


@click.command(name="login")
@handle_error
def cli_login():
    """
    Logs in to the Amazon Deadline Cloud configured AWS profile.

    This is for any profile type that Amazon Deadline Cloud knows how to login to
    Currently only supports Nimble Cloud Companion
    """
    click.echo(
        f"Logging into AWS Profile {config_file.get_setting('defaults.aws_profile_name')!r} for Amazon Deadline Cloud"
    )

    message = api.login(
        on_pending_authorization=_cli_on_pending_authorization, on_cancellation_check=None
    )

    click.echo(f"\nSuccessfully logged in: {message}\n")


@click.command(name="logout")
@handle_error
def cli_logout():
    """
    Logs out of the Amazon Deadline Cloud configured AWS profile.
    """
    api.logout()

    click.echo("Successfully logged out of all AWS sessions")
