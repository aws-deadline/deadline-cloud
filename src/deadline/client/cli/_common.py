# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Functionality common to all the CLI groups.
"""
__all__ = [
    "PROMPT_WHEN_COMPLETE",
    "prompt_at_completion",
    "handle_error",
    "apply_cli_options_to_config",
    "cli_object_repr",
]

import sys
from configparser import ConfigParser
from typing import Any, Callable, Optional, Set

import click

from ..config import config_file
from ..exceptions import DeadlineOperationError
from ..job_bundle import deadline_yaml_dump

PROMPT_WHEN_COMPLETE = "PROMPT_WHEN_COMPLETE"


def prompt_at_completion(ctx: click.Context):
    """
    If the click context has PROMPT_WHEN_COMPLETE set to True,
    prints out a prompt and waits for keyboard input.
    """
    if ctx.obj[PROMPT_WHEN_COMPLETE]:
        click.prompt(
            "Press Enter To Exit", prompt_suffix="", show_default=False, hide_input=True, default=""
        )


def handle_error(func: Callable) -> Callable:
    """
    Decorator that catches any exceptions raised in the passed in function,
    and handles their default printout.
    """

    @click.pass_context
    def wraps(ctx: click.Context, *args, **kwargs):
        try:
            func(*args, **kwargs)
        except DeadlineOperationError as e:
            # The message from DeadlineOperationError is printed
            # out verbatim.
            click.echo(str(e))
            prompt_at_completion(ctx)
            sys.exit(1)
        except click.ClickException:
            # Let click exceptions fall through
            raise
        except Exception as e:
            # Log and print out unfamiliar exceptions with additional
            # messaging.
            click.echo(f"The Amazon Deadline Cloud CLI encountered the following exception:\n{e}")
            import traceback

            traceback.print_exc()
            prompt_at_completion(ctx)
            sys.exit(1)

    wraps.__doc__ = func.__doc__
    return wraps


def apply_cli_options_to_config(
    *, config: Optional[ConfigParser] = None, required_options: Set[str] = set(), **args
) -> Optional[ConfigParser]:
    """
    Modifies a Amazon Deadline Cloud config object to apply standard option names to it, such as
    the AWS profile, Amazon Deadline Cloud Farm, or Amazon Deadline Cloud Queue to use.

    Args:
        config (ConfigParser, optional): A Amazon Deadline Cloud config, read by config_file.read_config().
                If not provided, loads the config from disk.
    """
    # Only work with a custom config if there are standard options provided
    if any(value is not None for value in args.values()):
        if config is None:
            config = config_file.read_config()

        aws_profile_name = args.pop("profile", None)
        if aws_profile_name:
            config_file.set_setting("defaults.aws_profile_name", aws_profile_name, config=config)

        farm_id = args.pop("farm_id", None)
        if farm_id:
            config_file.set_setting("defaults.farm_id", farm_id, config=config)

        queue_id = args.pop("queue_id", None)
        if queue_id:
            config_file.set_setting("defaults.queue_id", queue_id, config=config)

        auto_accept = args.pop("yes", None)
        if auto_accept:
            config_file.set_setting("settings.auto_accept", "true", config=config)
    else:
        # Remove the standard option names from the args list
        for name in ["profile", "farm_id", "queue_id"]:
            args.pop(name, None)

    # Check that the required options have values
    if "farm_id" in required_options:
        required_options.remove("farm_id")
        if not config_file.get_setting("defaults.farm_id", config=config):
            raise click.UsageError("Missing '--farm-id' or default Farm ID configuration")

    if "queue_id" in required_options:
        required_options.remove("queue_id")
        if not config_file.get_setting("defaults.queue_id", config=config):
            raise click.UsageError("Missing '--queue-id' or default Queue ID configuration")

    if required_options:
        raise RuntimeError(
            f"Unexpected required Amazon Deadline Cloud CLI options: {required_options}"
        )

    if args:
        raise RuntimeError(
            f"Option names {args.keys()} are not standard Amazon Deadline Cloud CLI options, they need special handling"
        )

    return config


def _fix_multiline_strings(obj: Any) -> Any:
    """
    Fixes the multi-line strings in `obj` to end with "\n".
    Returns a new object that has been modified.
    """
    if isinstance(obj, str):
        if "\n" in obj and not obj.endswith("\n"):
            return obj + "\n"
        else:
            return obj
    elif isinstance(obj, list):
        return [_fix_multiline_strings(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(_fix_multiline_strings(item) for item in obj)
    elif isinstance(obj, dict):
        return {key: _fix_multiline_strings(value) for key, value in obj.items()}
    elif isinstance(obj, set):
        return {_fix_multiline_strings(item) for item in obj}
    else:
        return obj


def cli_object_repr(obj: Any):
    """
    Transforms an API response object into a string, for printing as
    CLI output. This formats the output as YAML, using the "|"-style
    for multi-line strings.
    """
    # If a multi-line string does not end with an "\n", the formatting
    # will not use the "|"-style yaml. We fix that up be modifying such
    # strings to end with "\n".
    obj = _fix_multiline_strings(obj)
    return deadline_yaml_dump(obj)
