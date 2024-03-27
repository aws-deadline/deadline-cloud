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
    Manage AWS Deadline Cloud's workstation configuration.

    The available AWS Deadline Cloud settings are:

    defaults.aws_profile_name:

        The default AWS profile to use for AWS Deadline Cloud commands. Set to '' to use the default credentials. Other settings are saved with the profile.

    defaults.farm_id:

        The default farm ID to use for job submissions or CLI operations.

    defaults.queue_id:

        The default queue ID to use for job submissions or CLI operations.

    settings.storage_profile_id:

        The storage profile that this workstation conforms to. It specifies
        where shared file systems are mounted, and where named job attachments
        should go.

    defaults.job_id:

        The Job ID to use by default. This gets updated by job submission so is normally the most recently submitted job.

    settings.job_history_dir:

        The directory in which to create new job bundles for submitting to AWS Deadline Cloud, to produce a history of job submissions.

    settings.auto_accept:

        Flag to automatically confirm any confirmation prompts.

    settings.log_level:

        Setting to change the log level. Must be one of ["ERROR", "WARNING", "INFO", "DEBUG"]

    defaults.job_attachments_file_system:

        Setting to determine if attachments are copied on to workers before a job executes
        or fetched in realtime. Must be one of ["COPIED", "VIRTUAL"]

    telemetry.opt_out:

        Flag to specify opting out of telemetry data collection.
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
@_handle_error
def config_gui():
    """
    Open the workstation configuration settings GUI.
    """
    from ...ui import gui_context_for_cli

    with gui_context_for_cli():
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
