# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the deadline-cloud -> deadline-cloud-v2 Conda channel migration applied to the
CondaChannels queue parameter (see deadline.client.api._queue_parameters).
"""

from __future__ import annotations

import pytest

from deadline.client.api._queue_parameters import (
    _apply_deadline_cloud_v2_channel_migration,
    _prepend_v2_channel,
)
from deadline.client.job_bundle.parameters import JobParameter


@pytest.mark.parametrize(
    "channels, expected",
    [
        # The default queue value: v2 is prepended, v1 kept as fallback.
        ("deadline-cloud", "deadline-cloud-v2 deadline-cloud"),
        # Trailing channels keep their order, v2 lands just before v1.
        ("deadline-cloud conda-forge", "deadline-cloud-v2 deadline-cloud conda-forge"),
        # Leading channels are preserved; v2 is inserted at the v1 position, not the front.
        ("conda-forge deadline-cloud", "conda-forge deadline-cloud-v2 deadline-cloud"),
        # Idempotent: v2 already present means no change, even alongside v1.
        ("deadline-cloud-v2 deadline-cloud", "deadline-cloud-v2 deadline-cloud"),
        ("deadline-cloud-v2", "deadline-cloud-v2"),
        # No deadline-cloud channel: customized channels are left untouched.
        ("my-private-channel", "my-private-channel"),
        ("conda-forge defaults", "conda-forge defaults"),
        # Substring safety: only the exact "deadline-cloud" token migrates.
        ("deadline-cloud-extra", "deadline-cloud-extra"),
        (
            "deadline-cloud-extra deadline-cloud",
            "deadline-cloud-extra deadline-cloud-v2 deadline-cloud",
        ),
        # Empty value is a no-op.
        ("", ""),
    ],
)
def test_prepend_v2_channel(channels: str, expected: str):
    assert _prepend_v2_channel(channels) == expected


def test_prepend_v2_channel_is_idempotent():
    """Applying the prepend twice yields the same result as applying it once."""
    once = _prepend_v2_channel("deadline-cloud conda-forge")
    twice = _prepend_v2_channel(once)
    assert once == "deadline-cloud-v2 deadline-cloud conda-forge"
    assert twice == once


def test_migration_rewrites_default_and_value():
    """Both the default and value fields of the CondaChannels parameter are migrated."""
    params: list[JobParameter] = [
        {
            "name": "CondaChannels",
            "default": "deadline-cloud",
            "value": "deadline-cloud conda-forge",
        },
    ]
    _apply_deadline_cloud_v2_channel_migration(params)
    assert params[0]["default"] == "deadline-cloud-v2 deadline-cloud"
    assert params[0]["value"] == "deadline-cloud-v2 deadline-cloud conda-forge"


def test_migration_only_touches_conda_channels_parameter():
    """Parameters other than CondaChannels are left untouched."""
    params: list[JobParameter] = [
        {"name": "CondaPackages", "value": "cinema4d=2026 deadline-cloud-for-cinema4d"},
        {"name": "CondaChannels", "value": "deadline-cloud"},
        {"name": "MyQueueParameter", "value": "deadline-cloud"},
    ]
    _apply_deadline_cloud_v2_channel_migration(params)
    # CondaPackages happens to contain the substring but must not be rewritten.
    assert params[0]["value"] == "cinema4d=2026 deadline-cloud-for-cinema4d"
    assert params[1]["value"] == "deadline-cloud-v2 deadline-cloud"
    # A different parameter that happens to hold the same value is not the conda channels list.
    assert params[2]["value"] == "deadline-cloud"


def test_migration_handles_missing_conda_channels_parameter():
    """No CondaChannels parameter present is a no-op and does not raise."""
    params: list[JobParameter] = [{"name": "CondaPackages", "value": "cinema4d=2026"}]
    _apply_deadline_cloud_v2_channel_migration(params)
    assert params == [{"name": "CondaPackages", "value": "cinema4d=2026"}]


def test_migration_handles_parameter_without_value_or_default():
    """A CondaChannels parameter missing both string fields is skipped cleanly."""
    params: list[JobParameter] = [{"name": "CondaChannels"}]
    _apply_deadline_cloud_v2_channel_migration(params)
    assert params == [{"name": "CondaChannels"}]


def test_migration_is_idempotent_across_repeated_calls():
    """Re-running the migration (as happens on queue/farm reloads) does not duplicate v2."""
    params: list[JobParameter] = [{"name": "CondaChannels", "value": "deadline-cloud"}]
    _apply_deadline_cloud_v2_channel_migration(params)
    _apply_deadline_cloud_v2_channel_migration(params)
    assert params[0]["value"] == "deadline-cloud-v2 deadline-cloud"
