# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
All the `deadline config` commands.
"""

import click
import json
import textwrap

from ...config import config_file
from .._common import _handle_error
from .._main import deadline as main


@main.group(name="config")
@_handle_error
def cli_config():
    """
    Commands to show and update Deadline's workstation configuration.

    Deadline's workstation configuration is organized in a shallow hierarchy.
    Most of the settings are set to depend on the selected AWS profile, so that
    you can store different configurations like farm and queue for different profiles
    and switch between them by selecting the profile.
    """


@cli_config.command(name="show")
@click.option(
    "--output",
    type=click.Choice(["verbose", "json"], case_sensitive=False),
    default="verbose",
    help="Output format of the command",
)
@_handle_error
def config_show(output):
    """
    Show all workstation configuration settings and current values.
    """
    settings_json = {}
    if output == "verbose":
        click.echo(
            f"AWS Deadline Cloud configuration file:\n   {config_file.get_config_file_path()}"
        )
        click.echo()
        for setting_name in config_file.SETTINGS.keys():
            setting_value = config_file.get_setting(setting_name)
            setting_default = config_file.get_setting_default(setting_name)

            # Wrap and indent the descriptions to 80 characters because they may be multiline.
            setting_description: str = config_file.SETTINGS[setting_name].get("description", "")
            setting_description = "\n".join(
                f"   {line}" for line in textwrap.wrap(setting_description, width=77)
            )
            click.echo(
                f"{setting_name}: {setting_value} {'(default)' if setting_value == setting_default else ''}"
            )
            click.echo(setting_description)
            click.echo()
    else:
        settings_json["settings.config_file_path"] = str(config_file.get_config_file_path())
        for setting_name in config_file.SETTINGS.keys():
            setting_value = config_file.get_setting(setting_name)
            settings_json[setting_name] = setting_value

        click.echo(json.dumps(settings_json))


@cli_config.command(name="gui")
@click.option(
    "--install-gui",
    is_flag=True,
    help="Installs GUI dependencies if they are not installed already",
)
@_handle_error
def config_gui(install_gui: bool):
    """
    Open the workstation configuration settings GUI to view or edit setting values.
    """
    from ...ui import gui_context_for_cli

    with gui_context_for_cli(automatically_install_dependencies=install_gui):
        from ...ui.dialogs.deadline_config_dialog import DeadlineConfigDialog

        DeadlineConfigDialog.configure_settings()


@cli_config.command(name="set")
@click.argument("setting_name")
@click.argument("value")
@_handle_error
def config_set(setting_name, value):
    """
    Sets a workstation configuration setting.

    \b
    Example:
        `deadline config set defaults.farm_id <farm-id>`
    """
    config_file.set_setting(setting_name, value)


@cli_config.command(name="clear")
@click.argument("setting_name")
@_handle_error
def config_clear(setting_name):
    """
    Clears a workstation configuration setting to restore its default value.

    \b
    Example:
        `deadline config clear defaults.farm_id`
    """
    config_file.clear_setting(setting_name)


@cli_config.command(name="get")
@click.argument("setting_name")
@_handle_error
def config_get(setting_name):
    """
    Prints the value of a workstation configuration setting. You can use this command
    to call the [AWS CLI][aws-cli] with settings from the Deadline workstation configuration.

    [aws-cli]: https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-welcome.html

    \b
    Example:
        `aws deadline get-queue --profile $(deadline config get defaults.aws_profile_name) --farm-id $(deadline config get defaults.farm_id) --queue-id $(deadline config get defaults.queue_id)`
    """
    click.echo(config_file.get_setting(setting_name))
