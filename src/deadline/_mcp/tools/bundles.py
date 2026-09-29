# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Deadline Cloud Bundle sharing tools for MCP.
"""

import json
from typing import Any, Dict, Optional

from click.testing import CliRunner

from ...client.cli import main


def list_shared_bundles(
    farm_id: Optional[str] = None,
    queue_id: Optional[str] = None,
    show_hidden: bool = False,
) -> Dict[str, Any]:
    """List job bundles shared on the queue.

    Returns a list of bundles with their name and format.
    Hidden bundles are excluded by default unless show_hidden is True.
    """
    args = ["bundle", "list", "--queue", "--output", "json"]
    if farm_id:
        args.extend(["--farm-id", farm_id])
    if queue_id:
        args.extend(["--queue-id", queue_id])
    if show_hidden:
        args.append("--show-hidden")

    runner = CliRunner()
    result = runner.invoke(main, args)

    if result.exit_code != 0:
        return {"success": False, "error": result.output.strip()}
    # Parse the stdout stream specifically (not the combined output, which also
    # carries stderr): a warning/log line on stderr must not corrupt the JSON.
    # The guard reports a malformed body instead of raising out of the tool.
    try:
        bundles = json.loads(result.stdout)
    except json.JSONDecodeError:
        return {"success": False, "error": result.output.strip()}
    return {"success": True, "bundles": bundles}


def upload_bundle(
    job_bundle: str,
    name: Optional[str] = None,
    farm_id: Optional[str] = None,
    queue_id: Optional[str] = None,
    overwrite: bool = False,
) -> Dict[str, Any]:
    """Upload a local job bundle to the queue as a shared .ojd archive.

    Args:
        job_bundle: Path to a job bundle directory or .ojd archive file.
        name: Override the bundle name (defaults to directory/file name).
        farm_id: The farm ID (uses default if not specified).
        queue_id: The queue ID (uses default if not specified).
        overwrite: Overwrite an existing shared bundle of the same name. Defaults
            to False so an existing bundle (possibly another user's) is never
            clobbered unless the caller explicitly opts in.
    """
    args = ["bundle", "upload", job_bundle]
    if name:
        args.extend(["--name", name])
    if farm_id:
        args.extend(["--farm-id", farm_id])
    if queue_id:
        args.extend(["--queue-id", queue_id])
    if overwrite:
        args.append("--yes")

    runner = CliRunner()
    result = runner.invoke(main, args)

    if result.exit_code != 0:
        return {"success": False, "error": result.output.strip()}
    return {"success": True, "message": result.output.strip()}


def download_bundle(
    bundle_name: str,
    output_dir: Optional[str] = None,
    farm_id: Optional[str] = None,
    queue_id: Optional[str] = None,
    overwrite: bool = False,
) -> Dict[str, Any]:
    """Download a shared bundle from the queue to a local directory.

    Args:
        bundle_name: Name of the bundle to download.
        output_dir: Local directory to download to (uses cache if not specified).
        farm_id: The farm ID (uses default if not specified).
        queue_id: The queue ID (uses default if not specified).
        overwrite: Overwrite ``<output_dir>/<bundle_name>`` if it already exists.
            Defaults to False so existing local data is never deleted unless the
            caller explicitly opts in.
    """
    args = ["bundle", "download", bundle_name, "--output", "json"]
    if output_dir:
        args.extend(["-o", output_dir])
    if farm_id:
        args.extend(["--farm-id", farm_id])
    if queue_id:
        args.extend(["--queue-id", queue_id])
    if overwrite:
        args.append("--yes")

    runner = CliRunner()
    result = runner.invoke(main, args)

    if result.exit_code != 0:
        return {"success": False, "error": result.output.strip()}

    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        return {"success": False, "error": result.output.strip()}
    return {"success": True, "path": data["path"]}
