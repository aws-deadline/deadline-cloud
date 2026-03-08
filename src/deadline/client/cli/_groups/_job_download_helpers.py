# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Helper functions for job download-output storage profile support.

These are single-responsibility functions that handle storage profile resolution,
validation, and path mapping for the `deadline job download-output` command.
"""

from __future__ import annotations

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
from ....job_attachments.download import OutputDownloader
from ....job_attachments.models import (
    PathMappingRule,
    StorageProfile,
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


def _apply_path_mappings_to_roots(
    job_output_downloader: OutputDownloader,
    output_paths_by_root: dict[str, list[str]],
    rules: list[PathMappingRule],
) -> None:
    """Apply path mapping rules to remap output root directories.

    Modifies the downloader in-place via set_root_path().
    """
    if not rules:
        return

    applier = _PathMappingRuleApplier(rules)
    for original_root in list(output_paths_by_root.keys()):
        mapped_root = applier.transform(original_root)
        if str(mapped_root) != original_root:
            click.echo(f"  Mapping root: {original_root} -> {mapped_root}")
            job_output_downloader.set_root_path(original_root, str(mapped_root))
