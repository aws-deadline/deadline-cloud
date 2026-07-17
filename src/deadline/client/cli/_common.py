# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Functionality common to all the CLI groups.
"""

from __future__ import annotations

__all__ = [
    "_OUTPUT_FORMAT_HELP",
    "_PROMPT_WHEN_COMPLETE",
    "_ProgressBarCallbackManager",
    "_apply_cli_options_to_config",
    "_cli_object_repr",
    "_handle_error",
    "_parse_file_parameter",
    "_parse_multi_format_parameters",
    "_prompt_at_completion",
    "_resolve_output_format",
    "_suggest_resources_on_client_error",
]

import sys
from configparser import ConfigParser
from typing import Any, Callable, Optional, Set
import logging
import traceback
import json
import re
from pathlib import Path

import click
import yaml
from contextlib import ExitStack
from deadline.job_attachments.progress_tracker import ProgressReportMetadata

from .. import api as _api
from ..config import config_file
from ..config.config_file import _SETTING_FARM_ID as SETTING_FARM_ID
from ..config.config_file import _SETTING_QUEUE_ID as SETTING_QUEUE_ID
from ..exceptions import DeadlineOperationError
from ..job_bundle import deadline_yaml_dump
from ._groups._sigint_handler import SigIntHandler

logger = logging.getLogger("deadline.client.cli")

_PROMPT_WHEN_COMPLETE = "PROMPT_WHEN_COMPLETE"

# Shared help text for the `--output` option, documenting the TTY-aware default.
_OUTPUT_FORMAT_HELP = (
    "Specifies the output format of the messages printed to stdout.\n"
    "VERBOSE: Displays messages in a human-readable text format.\n"
    "JSON: Displays messages in JSON line format, so that the info can be easily "
    "parsed/consumed by custom scripts.\n"
    "When this option is not specified, the format is chosen automatically: "
    "VERBOSE when stdout is an interactive terminal, and JSON otherwise (for example "
    "when the output is piped, redirected, or run without a TTY such as in CI or by an agent)."
)


def _stdout_is_tty() -> bool:
    """
    Returns whether stdout is connected to an interactive terminal.

    A missing or non-TTY stdout (e.g. a closed or redirected stream) is treated as
    non-interactive. ``isatty()`` can raise on unusual streams, so this is defensive.
    """
    try:
        return bool(sys.stdout.isatty())
    except (AttributeError, ValueError):
        return False


def _resolve_output_format(output: Optional[str]) -> str:
    """
    Resolves the effective ``--output`` format for a CLI command.

    An explicit value always wins. When ``output`` is ``None`` (the option was not
    provided), the format is auto-detected from whether stdout is an interactive
    terminal: ``"verbose"`` for a TTY (a human reader) and ``"json"`` otherwise
    (pipes, redirection, CI, or agents), matching the behavior of tools like the
    AWS CLI and kubectl.

    Args:
        output (Optional[str]): The value of the ``--output`` option, or ``None`` if
            it was not specified. Comparison is case-insensitive.

    Returns:
        str: The resolved format, either ``"verbose"`` or ``"json"`` (lowercased).
    """
    if output is not None:
        return output.lower()

    return "verbose" if _stdout_is_tty() else "json"


# Set up the signal handler for handling Ctrl + C interruptions.
sigint_handler = SigIntHandler()


def _prompt_at_completion(ctx: click.Context):
    """
    If the click context has PROMPT_WHEN_COMPLETE set to True,
    prints out a prompt and waits for keyboard input.
    """
    if ctx.obj[_PROMPT_WHEN_COMPLETE]:
        click.prompt(
            "Press Enter To Exit", prompt_suffix="", show_default=False, hide_input=True, default=""
        )


def _handle_error(func: Callable) -> Callable:
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
            _prompt_at_completion(ctx)
            sys.exit(1)
        except click.ClickException:
            # Let click exceptions fall through
            raise
        except Exception:
            # Log and print out unfamiliar exceptions with additional
            # messaging.
            click.echo("The AWS Deadline Cloud CLI encountered the following exception:")
            click.echo(traceback.format_exc())
            _prompt_at_completion(ctx)
            sys.exit(1)

    wraps.__doc__ = func.__doc__
    return wraps


def _auto_select_farm(config: Optional[ConfigParser] = None) -> Optional[str]:
    """Auto-select farm ID if exactly one farm is available."""
    try:
        farms = _api.list_farms(config=config).get("farms", [])
        if len(farms) == 1:
            return farms[0]["farmId"]
    except Exception:
        pass
    return None


def _auto_select_queue(config: Optional[ConfigParser] = None) -> Optional[str]:
    """Auto-select queue ID if exactly one queue is available in the current farm."""
    try:
        farm_id = config_file.get_setting(SETTING_FARM_ID, config=config)
        if not farm_id:
            return None
        queues = _api.list_queues(farmId=farm_id, config=config).get("queues", [])
        if len(queues) == 1:
            return queues[0]["queueId"]
    except Exception:
        pass
    return None


def _apply_cli_options_to_config(
    *, config: Optional[ConfigParser] = None, required_options: Set[str] = set(), **args
) -> Optional[ConfigParser]:
    """
    Modifies an AWS Deadline Cloud config object to apply standard option names to it, such as
    the AWS profile, AWS Deadline Cloud Farm, or AWS Deadline Cloud Queue to use.

    Args:
        config (ConfigParser, optional): an AWS Deadline Cloud config, read by config_file.read_config().
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
            config_file.set_setting(SETTING_FARM_ID, farm_id, config=config)

        region = args.pop("region", None)
        if region:
            config_file.set_setting("defaults.farm_region", region, config=config)

        queue_id = args.pop("queue_id", None)
        if queue_id:
            config_file.set_setting(SETTING_QUEUE_ID, queue_id, config=config)

        storage_profile_id = args.pop("storage_profile_id", None)
        if storage_profile_id:
            config_file.set_setting(
                "settings.storage_profile_id", storage_profile_id, config=config
            )

        job_id = args.pop("job_id", None)
        if job_id:
            config_file.set_setting("defaults.job_id", job_id, config=config)

        auto_accept = args.pop("yes", None)
        if auto_accept:
            config_file.set_setting("settings.auto_accept", "true", config=config)

        conflict_resolution = args.pop("conflict_resolution", None)
        if conflict_resolution:
            config_file.set_setting(
                "settings.conflict_resolution", conflict_resolution, config=config
            )
    else:
        # Remove the standard option names from the args list
        for name in ["profile", "farm_id", "region", "queue_id", "job_id", "storage_profile_id"]:
            args.pop(name, None)

    # Check that the required options have values, auto-selecting if only one exists
    if "farm_id" in required_options:
        required_options.remove("farm_id")
        if not config_file.get_setting(SETTING_FARM_ID, config=config):
            farm_id = _auto_select_farm(config)
            if farm_id:
                if config is None:
                    config = config_file.read_config()
                config_file.set_setting(SETTING_FARM_ID, farm_id, config=config)
            else:
                raise click.UsageError("Missing '--farm-id' or default Farm ID configuration")

    if "queue_id" in required_options:
        required_options.remove("queue_id")
        if not config_file.get_setting(SETTING_QUEUE_ID, config=config):
            queue_id = _auto_select_queue(config)
            if queue_id:
                if config is None:
                    config = config_file.read_config()
                config_file.set_setting(SETTING_QUEUE_ID, queue_id, config=config)
            else:
                raise click.UsageError("Missing '--queue-id' or default Queue ID configuration")

    if "job_id" in required_options:
        required_options.remove("job_id")
        if not config_file.get_setting("defaults.job_id", config=config):
            raise click.UsageError("Missing '--job-id' or default Job ID configuration")

    if required_options:
        raise RuntimeError(
            f"Unexpected required AWS Deadline Cloud CLI options: {required_options}"
        )

    if args:
        raise RuntimeError(
            f"Option names {tuple(args.keys())} are not standard AWS Deadline Cloud CLI options, they need special handling"
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


def _cli_object_repr(obj: Any) -> str:
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


def _parse_file_parameter(file_path: Path) -> dict:
    """
    Parse a file parameter (with file:// prefix) into a dictionary.

    Supports JSON and YAML files. Unknown extensions default to YAML.

    Args:
        file_param: File path string with 'file://' prefix

    Returns:
        Dictionary containing the parsed file contents

    Raises:
        click.BadParameter: If file doesn't exist, isn't readable, or contains invalid data
    """
    file_path = file_path.expanduser()

    if not file_path.exists():
        raise click.BadParameter(f"Provided file '{file_path}' does not exist.")
    if not file_path.is_file():
        raise click.BadParameter(f"Provided file '{file_path}' is not a file.")

    try:
        content = file_path.read_text()
    except OSError as e:
        raise click.BadParameter(f"Could not open file '{file_path}': {e}") from e

    try:
        if file_path.suffix.lower() == ".json":
            data = json.loads(content)
        else:  # Default to YAML for other extensions
            data = yaml.safe_load(content)
    except (yaml.YAMLError, json.JSONDecodeError) as e:
        raise click.BadParameter(f"File '{file_path}' is formatted incorrectly: {e}") from e

    if not isinstance(data, dict):
        raise click.BadParameter(f"File '{file_path}' should contain a dictionary.")

    return data


def _parse_multi_format_parameters(params: list[str]) -> dict:
    """
    Parse a list of parameters that can be in multiple formats.

    Supports three formats that can be mixed:
    - Key=value pairs: "key=value"
    - Inline JSON strings: '{"key": "value"}'
    - File paths: "file://path/to/file.json" or "file://path/to/file.yaml"

    Later values for the same key override earlier ones.

    Args:
        params: List of parameter strings in various formats

    Returns:
        Dictionary with all parsed key-value pairs merged

    Raises:
        click.BadParameter: If any argument is malformed or files can't be read
    """
    result_dict = {}

    for param in params:
        param = param.strip()

        # Case 1: File path
        prefix = "file://"
        if param.startswith(prefix):
            # retain support for Python 3.8 which does not support str.removeprefix.
            file_path = Path(param[len(prefix) :])
            data = _parse_file_parameter(file_path)
            result_dict.update(data)

        # Case 2: Inline JSON string
        elif re.match(r"^{.*}$", param):
            try:
                data = json.loads(param)
            except (json.JSONDecodeError, TypeError) as e:
                raise click.BadParameter(
                    f"Parameter ('{param}') not formatted correctly. It must be key=value pairs, "
                    f"inline JSON, or a path to a JSON or YAML document prefixed with 'file://': {e}"
                )
            if not isinstance(data, dict):
                raise click.BadParameter(
                    f"Argument ('{param}') must contain a dictionary mapping keys to their values."
                )
            result_dict.update(data)

        # Case 3: Key=value string
        elif match := re.match(r"^([^=]+)=(.*)$", param):
            key, val = match.groups()
            result_dict[key] = val

        else:
            raise click.BadParameter(
                f"Parameter ('{param}') not formatted correctly. It must be key=value pairs, "
                "inline JSON, or a path to a JSON or YAML document prefixed with 'file://'."
            )

    return result_dict


class _ProgressBarCallbackManager:
    """
    Manages creation, update, and deletion of a progress bar. On first call of the callback, the progress bar is created. The progress bar is closed
    on the final call (100% completion)
    """

    BAR_NOT_CREATED = 0
    BAR_CREATED = 1
    BAR_CLOSED = 2

    def __init__(self, length: int, label: str):
        self._length = length
        self._label = label
        self._bar_status = self.BAR_NOT_CREATED
        self._exit_stack = ExitStack()

    def callback(self, upload_metadata: ProgressReportMetadata) -> bool:
        if self._bar_status == self.BAR_CLOSED:
            # from multithreaded execution this can be called after completion somtimes.
            return sigint_handler.continue_operation
        elif self._bar_status == self.BAR_NOT_CREATED:
            # Note: click doesn't export the return type of progressbar(), so we suppress mypy warnings for
            # not annotating the type of hashing_progress.
            self._upload_progress = click.progressbar(length=self._length, label=self._label)  # type: ignore[var-annotated]
            self._exit_stack.enter_context(self._upload_progress)
            self._bar_status = self.BAR_CREATED

        total_progress = int(upload_metadata.progress)
        new_progress = total_progress - self._upload_progress.pos
        if new_progress > 0:
            self._upload_progress.update(new_progress)

        if total_progress == self._length or not sigint_handler.continue_operation:
            self._bar_status = self.BAR_CLOSED
            self._exit_stack.close()

        return sigint_handler.continue_operation


# Re-export from _suggest_resources for backward compatibility
from ._suggest_resources import _suggest_resources_on_client_error  # noqa: E402,F401
