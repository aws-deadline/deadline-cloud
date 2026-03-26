# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Helper functions for job download-output storage profile support.

These are single-responsibility functions that handle storage profile resolution,
validation, and path mapping for the `deadline job download-output` command.
"""

from __future__ import annotations

import ntpath
import posixpath
from configparser import ConfigParser
from dataclasses import dataclass
from typing import Any, Optional

import click
from botocore.client import BaseClient  # type: ignore[import]

from ... import api
from ...config import config_file
from ....job_attachments._path_mapping import (
    _PathMappingRuleApplier,
)
from ....job_attachments.models import (
    PathMappingRule,
    StorageProfile,
    StorageProfileOperatingSystemFamily,
)


@dataclass
class ResolvedStorageProfiles:
    """The result of resolving storage profiles for a download operation."""

    job_profile: StorageProfile  # profile the job was submitted with (source paths)
    local_profile: StorageProfile  # profile on this machine (destination paths)


def _resolve_storage_profiles(
    config: Optional[ConfigParser],
    deadline: BaseClient,
    farm_id: str,
    queue_id: str,
    job: dict[str, Any],
    ignore_storage_profiles: bool,
) -> Optional[ResolvedStorageProfiles]:
    """Resolve the storage profiles needed to map a job's output paths to local paths.

    The job_profile is where paths came from (the submitting machine).
    The local_profile is where paths should go (this machine).

    Returns:
        ResolvedStorageProfiles if path mapping is needed, None otherwise.
    """
    if ignore_storage_profiles:
        return None

    local_storage_profile_id = config_file.get_setting("settings.storage_profile_id", config=config)
    job_storage_profile_id = job.get("storageProfileId")

    if not local_storage_profile_id and not job_storage_profile_id:
        # Same-machine case: no profiles on either side
        return None

    if not local_storage_profile_id and job_storage_profile_id:
        click.echo(
            "Warning: The job was submitted with a storage profile but no local storage "
            "profile is configured. Path mapping will be skipped.\n\n"
            "Options:\n"
            "  1. Configure a storage profile: deadline config set "
            "settings.storage_profile_id <id>\n"
            "  2. Skip path mapping (same-machine only): --ignore-storage-profiles\n\n"
            "See https://docs.aws.amazon.com/deadline-cloud/latest/developerguide/"
            "modeling-your-shared-filesystem-locations-with-storage-profiles.html"
        )
        return None

    if local_storage_profile_id and not job_storage_profile_id:
        click.echo(
            "Warning: A local storage profile is configured but the job was submitted "
            "without one. Path mapping will be skipped."
        )
        return None

    # Both profiles exist — fetch them
    assert local_storage_profile_id is not None  # narrowing for mypy
    assert job_storage_profile_id is not None  # narrowing for mypy
    local_profile = api.get_storage_profile_for_queue(
        farm_id, queue_id, local_storage_profile_id, deadline, config=config
    )
    job_profile = api.get_storage_profile_for_queue(
        farm_id, queue_id, job_storage_profile_id, deadline, config=config
    )

    return ResolvedStorageProfiles(job_profile=job_profile, local_profile=local_profile)


def _transform_manifests_to_absolute_paths(
    manifests_by_root: dict[str, list[Any]],
    rules: list[PathMappingRule],
    source_os_family: StorageProfileOperatingSystemFamily,
) -> dict[str, Any]:
    """Transform manifest paths using absolute-path mapping, matching sync-output behavior.

    Joins each root + relative path, applies path mapping rules to the full absolute path,
    and returns a dict suitable for download_files_from_manifests(). This correctly handles
    rules that match at any depth (e.g., nested file system locations), unlike root-only
    transformation.

    Args:
        manifests_by_root: dict from asset root to list of BaseAssetManifest objects
            (as returned by get_output_manifests_by_asset_root).
        rules: path mapping rules from _generate_path_mapping_rules().
        source_os_family: the OS family of the submitting machine (determines path joining).

    Returns:
        dict mapping "" to a merged BaseAssetManifest with absolute local paths.
        The empty-string key means download_files_from_manifests() will use the
        absolute paths directly (Path("").joinpath("/abs/path") == Path("/abs/path")).
    """
    applier = _PathMappingRuleApplier(rules)

    if source_os_family == StorageProfileOperatingSystemFamily.WINDOWS:
        source_os_path: Any = ntpath
    else:
        source_os_path = posixpath

    unmapped_count = 0
    mapped_count = 0

    for root_path, manifest_list in manifests_by_root.items():
        for manifest in manifest_list:
            new_paths = []
            for manifest_path in manifest.paths:
                abs_path = source_os_path.normpath(
                    source_os_path.join(root_path, manifest_path.path)
                )
                try:
                    manifest_path.path = str(applier.strict_transform(abs_path))
                    new_paths.append(manifest_path)
                    mapped_count += 1
                except ValueError:
                    unmapped_count += 1
            manifest.paths = new_paths

    if unmapped_count > 0:
        click.echo(
            f"Warning: {unmapped_count} output file(s) could not be mapped and will be skipped."
        )

    if mapped_count > 0:
        click.echo(f"  Mapped {mapped_count} output file(s) to local paths.")

    # Collect all manifests with remaining paths into a flat dict keyed by "".
    # Using "" as the key means Path("").joinpath(absolute_path) == absolute_path.
    result: dict[str, Any] = {}
    all_manifests = [m for ml in manifests_by_root.values() for m in ml if m.paths]
    if all_manifests:
        # Use the first manifest as the base and merge others into it
        merged = all_manifests[0]
        for extra in all_manifests[1:]:
            merged.paths.extend(extra.paths)
        result[""] = merged

    return result
