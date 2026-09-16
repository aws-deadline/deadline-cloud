# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the deadline-cloud -> deadline-cloud-v2 Conda channel migration applied to the
CondaChannels queue parameter (see deadline.client.api._queue_parameters).
"""

from __future__ import annotations

import logging

import pytest

from deadline.client.api._queue_parameters import (
    _apply_deadline_cloud_v2_channel_migration,
    _prepend_v2_channel,
)
from deadline.client.job_bundle.parameters import JobParameter

_MIGRATION_LOGGER = "deadline.client.api._queue_parameters"


@pytest.mark.parametrize(
    "channels, expected, reason",
    [
        ("deadline-cloud", "deadline-cloud-v2 deadline-cloud", "v2 prepended, v1 kept as fallback"),
        (
            "deadline-cloud conda-forge",
            "deadline-cloud-v2 deadline-cloud conda-forge",
            "trailing channels keep order, v2 lands just before v1",
        ),
        (
            "conda-forge deadline-cloud",
            "conda-forge deadline-cloud-v2 deadline-cloud",
            "leading channels preserved, v2 inserted at the v1 position",
        ),
        (
            "deadline-cloud-v2 deadline-cloud",
            "deadline-cloud-v2 deadline-cloud",
            "idempotent: v2 already present alongside v1",
        ),
        ("deadline-cloud-v2", "deadline-cloud-v2", "v2 present, no v1 to prepend before"),
        ("my-private-channel", "my-private-channel", "no deadline-cloud token: left untouched"),
        ("conda-forge defaults", "conda-forge defaults", "no deadline-cloud token: left untouched"),
        ("deadline-cloud-extra", "deadline-cloud-extra", "substring safety: not the exact token"),
        (
            "deadline-cloud-extra deadline-cloud",
            "deadline-cloud-extra deadline-cloud-v2 deadline-cloud",
            "only the exact deadline-cloud token migrates",
        ),
        ("", "", "empty value is a no-op"),
    ],
)
def test_prepend_v2_channel(channels: str, expected: str, reason: str):
    assert _prepend_v2_channel(channels) == expected, reason


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


def test_migration_logs_when_no_conda_channels_parameter(caplog):
    """When the flag is set but the queue has no CondaChannels parameter, the silent no-op is
    explained at debug level so the flag doing nothing is diagnosable."""
    params: list[JobParameter] = [{"name": "CondaPackages", "value": "cinema4d=2026"}]
    with caplog.at_level(logging.DEBUG, logger=_MIGRATION_LOGGER):
        _apply_deadline_cloud_v2_channel_migration(params)
    assert "no CondaChannels parameter" in caplog.text


def test_migration_logs_when_channels_already_migrated(caplog):
    """When CondaChannels already lists v2 (or lacks deadline-cloud), the no-op is logged."""
    params: list[JobParameter] = [{"name": "CondaChannels", "value": "deadline-cloud-v2"}]
    with caplog.at_level(logging.DEBUG, logger=_MIGRATION_LOGGER):
        _apply_deadline_cloud_v2_channel_migration(params)
    assert "leaving Conda channels unchanged" in caplog.text


def test_migration_does_not_log_no_op_when_channels_change(caplog):
    """A real prepend must not emit the no-op debug message."""
    params: list[JobParameter] = [{"name": "CondaChannels", "value": "deadline-cloud"}]
    with caplog.at_level(logging.DEBUG, logger=_MIGRATION_LOGGER):
        _apply_deadline_cloud_v2_channel_migration(params)
    assert "leaving Conda channels unchanged" not in caplog.text


def test_migration_is_atomic_across_default_and_value(caplog):
    """When maxLength admits the migrated default but not the (longer) migrated value, neither is
    written — otherwise the widget, which resolves value over default, would submit the unmigrated
    value while default silently carried v2. The skip is still logged."""
    params: list[JobParameter] = [
        {
            "name": "CondaChannels",
            "type": "STRING",
            "default": "deadline-cloud",
            "value": "deadline-cloud conda-forge",
            "maxLength": len("deadline-cloud-v2 deadline-cloud"),
        }
    ]
    with caplog.at_level(logging.DEBUG, logger=_MIGRATION_LOGGER):
        _apply_deadline_cloud_v2_channel_migration(params)
    assert params[0]["default"] == "deadline-cloud"
    assert params[0]["value"] == "deadline-cloud conda-forge"
    assert "leaving Conda channels unchanged" in caplog.text


@pytest.mark.parametrize(
    "parameter, expected_default, expect_unchanged_log, reason",
    [
        (
            {
                "name": "CondaChannels",
                "type": "STRING",
                "default": "deadline-cloud",
                "allowedValues": ["deadline-cloud", "conda-forge"],
            },
            "deadline-cloud",
            True,
            "migrated value not in allowedValues; the dropdown would drop it and submit "
            "allowedValues[0], so the field is left unchanged",
        ),
        (
            {
                "name": "CondaChannels",
                "type": "STRING",
                "default": "deadline-cloud",
                "allowedValues": ["deadline-cloud", "deadline-cloud-v2 deadline-cloud"],
            },
            "deadline-cloud-v2 deadline-cloud",
            False,
            "migrated value is itself in allowedValues, so the migration applies",
        ),
        (
            {
                "name": "CondaChannels",
                "type": "STRING",
                "default": "deadline-cloud",
                "maxLength": len("deadline-cloud"),
            },
            "deadline-cloud",
            True,
            "migrated value exceeds maxLength; the service would reject it, so leave unchanged",
        ),
        (
            {
                "name": "CondaChannels",
                "type": "STRING",
                "default": "deadline-cloud",
                # Only a single bare token allowed: the space-separated migrated value cannot match.
                # allowedPattern is a valid queue field but not a JobParameter TypedDict key, and
                # validate_job_parameter_value never checks it (QLineEdit.setText also bypasses the
                # widget validator), so the explicit re.match guard is what catches it here.
                "allowedPattern": r"^[a-z-]+$",  # type: ignore[typeddict-unknown-key]
            },
            "deadline-cloud",
            True,
            "migrated value fails allowedPattern, so leave unchanged rather than fail at CreateJob",
        ),
        (
            {"name": "CondaChannels", "default": "deadline-cloud"},
            "deadline-cloud-v2 deadline-cloud",
            False,
            "no type: a malformed/partial definition degrades to a plain migration instead of "
            "raising a KeyError out of validate_job_parameter_value and the Qt slot",
        ),
    ],
)
def test_migration_respects_constraints(
    parameter, expected_default, expect_unchanged_log, reason, caplog
):
    """A constrained CondaChannels is migrated only when the migrated value satisfies the
    constraint (allowedValues / maxLength / allowedPattern); otherwise the field is left unchanged
    and the skip is logged. A definition lacking "type" degrades to a plain migration."""
    params: list[JobParameter] = [parameter]
    with caplog.at_level(logging.DEBUG, logger=_MIGRATION_LOGGER):
        _apply_deadline_cloud_v2_channel_migration(params)
    assert params[0]["default"] == expected_default, reason
    assert ("leaving Conda channels unchanged" in caplog.text) is expect_unchanged_log, reason


def test_migration_empty_queue_parameters_is_silent(caplog):
    """An empty list means queue parameters have not loaded yet (fired on dialog open, failed
    fetch, or farm switch), so it must be a clean no-op with no misleading log."""
    params: list[JobParameter] = []
    with caplog.at_level(logging.DEBUG, logger=_MIGRATION_LOGGER):
        _apply_deadline_cloud_v2_channel_migration(params)
    assert params == []
    assert caplog.text == ""
