# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
All the `deadline config` commands.
"""

import click

from ...config import config_file
from .._common import _handle_error


@click.group(name="config")
@_handle_error
def cli_config():
    """
    Manage AWS Deadline Cloud's workstation configuration. Config options are organized
    and documented in config_file.SETTINGS.
    """


@cli_config.command(name="show")
@_handle_error
def config_show():
    """
    Show AWS Deadline Cloud's current workstation configuration.
    """
    click.echo(f"AWS Deadline Cloud configuration file:\n   {config_file.get_config_file_path()}")
    click.echo()

    for setting_name in config_file.SETTINGS.keys():
        setting_value = config_file.get_setting(setting_name)
        setting_default = config_file.get_setting_default(setting_name)
        if setting_value == setting_default:
            click.echo(f"{setting_name}: (default)\n   {setting_value}")
        else:
            click.echo(f"{setting_name}:\n   {setting_value}")
        click.echo()


@cli_config.command(name="gui")
@click.option(
    "--install-gui",
    is_flag=True,
    help="Installs GUI dependencies if they are not installed already",
)
@_handle_error
def config_gui(install_gui: bool):
    """
    Open the workstation configuration settings GUI.
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
    Sets an AWS Deadline Cloud workstation configuration setting.

    For example `deadline config set defaults.farm_id <farm-id>`.
    Run `deadline config --help` to show available settings.
    """
    config_file.set_setting(setting_name, value)


@cli_config.command(name="get")
@click.argument("setting_name")
@_handle_error
def config_get(setting_name):
    """
    Gets an AWS Deadline Cloud workstation configuration setting.

    For example `deadline config get defaults.farm_id`.
    Run `deadline config --help` to show available settings.
    """
    click.echo(config_file.get_setting(setting_name))
