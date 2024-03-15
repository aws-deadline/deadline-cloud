# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
import re
import sys
import urllib.parse
from logging import getLogger
from typing import Any, Dict, List

import click

from ..exceptions import DeadlineOperationError

logger = getLogger(__name__)

__all__ = [
    "parse_query_string",
    "install_deadline_web_url_handler",
    "uninstall_deadline_web_url_handler",
    "DEADLINE_URL_SCHEME_NAME",
]

DEADLINE_URL_SCHEME_NAME = "deadline"
DEADLINE_ID_PATTERN = re.compile(r"^[0-9a-f]{32}$")
DEADLINE_TASK_ID_PATTERN = re.compile(r"^[0-9a-f]{32}-(0|([1-9][0-9]{0,9}))$")
VALID_RESOURCE_NAMES_IN_ID = ["farm", "queue", "job", "step", "task"]


def parse_query_string(
    query_string: str, parameter_names: List[str], required_parameter_names: List[str]
) -> Dict[str, str]:
    """
    Parses the URL query string into {"parameter": "value"} form.

    Any "-" in the parameter names are switched to "_" in the result.

    Args:
        query_string (str): The query string from a parsed web URL.
    """
    result: dict = {}

    if query_string:
        parsed_qs = urllib.parse.parse_qs(query_string, strict_parsing=True)
    else:
        parsed_qs = {}

    # Ensure the required parameters are provided
    missing_required_parameters = set(required_parameter_names) - set(parsed_qs.keys())
    if missing_required_parameters:
        raise DeadlineOperationError(
            f"The URL query did not contain the required parameter(s) {list(missing_required_parameters)}"
        )

    # Process all the valid parameter names
    for name in parameter_names:
        values = parsed_qs.pop(name, None)
        if values:
            if len(values) > 1:
                raise DeadlineOperationError(
                    f"The URL query parameter {name} was provided multiple times, it may only be provided once."
                )
            result[name.replace("-", "_")] = values[0]

    # If there are any left, they are not valid
    if parsed_qs:
        raise DeadlineOperationError(
            f"The URL query contained unsupported parameter names {parsed_qs.keys()}"
        )

    return result


def validate_resource_ids(ids: Dict[str, str]) -> None:
    """
    Validates that the resource IDs are all valid.
    i.e. "<name of resource>-<a hexadecimal string of length 32>"
    for Task ID: "task-<a hexadecimal string of length 32>-<0 or a number up to 10 digits long>"

    Args:
        ids (Dict[str, str]): The resource IDs to validate.
        Expected to be {"<resource type>_id": "<full id string>"} form.
    """
    for id_name, id_str in ids.items():
        resource_type = id_str.split("-")[0]
        if not id_name.startswith(resource_type) or not validate_id_format(resource_type, id_str):
            raise DeadlineOperationError(
                f'The given resource ID "{id_name}": "{id_str}" has invalid format.'
            )


def validate_id_format(resource_type: str, full_id_str: str) -> bool:
    """
    Validates if the ID is in correct format. The ID must
    - start with one of the resource names, and followed by a hyphen ("-"), and
    - the string that follows must be a hexadecimal string of length 32.
    - Additional for "task": followed by another hyphen ("-"), and ends with
      a string of either 0 or a number up to 10 digits long.

    Args:
        full_id_str (str): The ID to validate.
        resource_type (str): "farm", "queue", "job", etc.
    """
    if resource_type not in VALID_RESOURCE_NAMES_IN_ID:
        return False

    prefix = f"{resource_type}-"
    if not full_id_str.startswith(prefix):
        return False

    id_str = full_id_str[len(prefix) :]

    if resource_type == "task":
        return bool(DEADLINE_TASK_ID_PATTERN.fullmatch(id_str))
    else:
        if len(id_str) != 32:
            return False
        return bool(DEADLINE_ID_PATTERN.fullmatch(id_str))


def install_deadline_web_url_handler(all_users: bool) -> None:
    """
    Installs the called AWS Deadline Cloud CLI command as the deadline:// web URL handler.
    """
    if sys.platform == "win32":
        import winreg

        # Get the CLI program path, either an .exe or a .py with the Python interpreter
        deadline_cli_program = os.path.abspath(sys.argv[0])
        if deadline_cli_program.endswith(".py"):
            deadline_cli_prefix = f'"{sys.executable}" "{deadline_cli_program}"'
        else:
            deadline_cli_program = deadline_cli_program + ".exe"
            deadline_cli_prefix = f'"{deadline_cli_program}"'

        if not os.path.isfile(deadline_cli_program):
            raise DeadlineOperationError(
                f"Error determining the AWS Deadline Cloud CLI program, {deadline_cli_program} does not exist."
            )

        logger.info(
            f'Installing "{deadline_cli_program}" as the handler for {DEADLINE_URL_SCHEME_NAME} URLs'
        )

        try:
            hkey: Any = None
            hkey_command: Any = None
            if all_users:
                hkey = winreg.CreateKeyEx(winreg.HKEY_CLASSES_ROOT, DEADLINE_URL_SCHEME_NAME)
            else:
                hkey = winreg.CreateKeyEx(
                    winreg.HKEY_CURRENT_USER, f"Software\\Classes\\{DEADLINE_URL_SCHEME_NAME}"
                )
            winreg.SetValueEx(hkey, None, 0, winreg.REG_SZ, "URL:AWS Deadline Cloud Protocol")
            winreg.SetValueEx(hkey, "URL Protocol", 0, winreg.REG_SZ, "")
            hkey_command = winreg.CreateKeyEx(hkey, "shell\\open\\command")
            winreg.SetValueEx(
                hkey_command,
                None,
                0,
                winreg.REG_SZ,
                f'{deadline_cli_prefix} handle-web-url "%1" --prompt-when-complete',
            )
        except OSError as e:
            if all_users and e.winerror == 5:  # Access denied
                raise DeadlineOperationError(
                    f"Administrator access is required to install the {DEADLINE_URL_SCHEME_NAME} URL handler for all users:\n{e}"
                )
            else:
                raise DeadlineOperationError(
                    f"Failed to install the handler for {DEADLINE_URL_SCHEME_NAME} URLs:\n{e}"
                )
        finally:
            if hkey_command:
                winreg.CloseKey(hkey_command)
            if hkey:
                winreg.CloseKey(hkey)

    elif sys.platform == "linux":
        import subprocess
        import shutil

        if shutil.which("update-desktop-database") is None:
            raise DeadlineOperationError(
                f"Failed to install the handler for {DEADLINE_URL_SCHEME_NAME} URLs: update-desktop-database is not installed."
            )

        # Get the CLI program path
        deadline_cli_program = os.path.abspath(sys.argv[0])

        if all_users:
            entry_dir = "/usr/share/applications"
            mimeapps_list_file_path = "/usr/share/applications/mimeapps.list"
        else:
            entry_dir = os.path.expanduser("~/.local/share/applications")
            mimeapps_list_file_path = os.path.expanduser("~/.config/mimeapps.list")
        try:
            os.makedirs(entry_dir, exist_ok=True)
        except OSError as e:
            raise DeadlineOperationError(f"Failed to create a directory: {e}")

        desktop_file_path = os.path.join(entry_dir, "deadline.desktop")

        desktop_file_content = f"""[Desktop Entry]
Type=Application
Name={DEADLINE_URL_SCHEME_NAME}
Exec={deadline_cli_program} handle-web-url %u
Type=Application
Terminal=true
MimeType=x-scheme-handler/{DEADLINE_URL_SCHEME_NAME}
"""

        mimeapps_file_content = f"""[Default Applications]
x-scheme-handler/{DEADLINE_URL_SCHEME_NAME}={DEADLINE_URL_SCHEME_NAME}.desktop;
"""
        with open(desktop_file_path, "w") as desktop_file:
            desktop_file.write(desktop_file_content)
        with open(mimeapps_list_file_path, "w") as mimeapps_list_file:
            mimeapps_list_file.write(mimeapps_file_content)

        try:
            subprocess.run(["update-desktop-database", entry_dir], check=True)
        except subprocess.CalledProcessError as e:
            raise DeadlineOperationError(
                f"Failed to install the handler for {DEADLINE_URL_SCHEME_NAME} URLs:\n{e}"
            ) from e

    else:
        raise DeadlineOperationError(
            f"Installing the web URL handler is not supported on OS {sys.platform}"
        )


def uninstall_deadline_web_url_handler(all_users: bool) -> None:
    """
    Uninstalls the called AWS Deadline Cloud CLI command as the deadline:// web URL handler.
    """
    if sys.platform == "win32":
        import winreg

        logger.info(f"Unstalling the handler for {DEADLINE_URL_SCHEME_NAME} URLs")

        try:
            hkey: Any = None
            if all_users:
                hkey = winreg.HKEY_CLASSES_ROOT
            else:
                hkey = winreg.OpenKeyEx(winreg.HKEY_CURRENT_USER, "Software\\Classes")
            winreg.DeleteKeyEx(hkey, f"{DEADLINE_URL_SCHEME_NAME}\\shell\\open\\command")
            winreg.DeleteKeyEx(hkey, f"{DEADLINE_URL_SCHEME_NAME}\\shell\\open")
            winreg.DeleteKeyEx(hkey, f"{DEADLINE_URL_SCHEME_NAME}\\shell")
            winreg.DeleteKeyEx(hkey, DEADLINE_URL_SCHEME_NAME)
        except OSError as e:
            if e.winerror == 2:  # Cannot find the specified key
                click.echo(
                    f"Nothing to uninstall, no handler for {DEADLINE_URL_SCHEME_NAME} URLs was installed"
                )
            else:
                raise DeadlineOperationError(
                    f"Failed to uninstall handler for {DEADLINE_URL_SCHEME_NAME} URLs:\n{e}"
                )
        finally:
            if hkey and hkey != winreg.HKEY_CLASSES_ROOT:
                winreg.CloseKey(hkey)

    elif sys.platform == "linux":
        import subprocess
        import shutil

        if shutil.which("update-desktop-database") is None:
            raise DeadlineOperationError(
                f"Failed to uninstall the handler for {DEADLINE_URL_SCHEME_NAME} URLs: update-desktop-database is not installed."
            )

        logger.info(f"Unstalling the handler for {DEADLINE_URL_SCHEME_NAME} URLs")

        if all_users:
            entry_dir = "/usr/share/applications"
        else:
            entry_dir = os.path.expanduser("~/.local/share/applications")
        desktop_file_path = f"{entry_dir}/{DEADLINE_URL_SCHEME_NAME}.desktop"

        try:
            os.remove(desktop_file_path)
            print(f"Removed {desktop_file_path}")
        except FileNotFoundError:
            print(f"{desktop_file_path} not found, nothing to remove")

        try:
            subprocess.run(["update-desktop-database", entry_dir], check=True)
        except subprocess.CalledProcessError as e:
            raise DeadlineOperationError(
                f"Failed to uninstall the handler for {DEADLINE_URL_SCHEME_NAME} URLs:\n{e}"
            ) from e

    else:
        raise DeadlineOperationError(
            f"Uninstalling the web URL handler is not supported on OS {sys.platform}"
        )
