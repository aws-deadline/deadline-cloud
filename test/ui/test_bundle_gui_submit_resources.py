# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""UI tests for ``deadline bundle gui-submit`` — the editable farm/queue/storage
selectors on the Shared job settings tab.

Drives the real submitter GUI through the accessibility tree (xa11y) against an
in-process mock backend. Resource selection is configured via the config file the
subprocess reads (the macOS-safe pattern — combo-box popups can't be reliably
activated through the a11y API), and the GUI is asserted to reflect it.
"""

from __future__ import annotations

import json
import sys

import pytest
from helpers import SAMPLE_TEMPLATE, SubmitterDialog


def _os_family() -> str:
    """The storage-profile osFamily the running OS matches, per the controller."""
    if sys.platform.startswith("linux"):
        return "LINUX"
    if sys.platform.startswith("darwin"):
        return "MACOS"
    if sys.platform.startswith("win"):
        return "WINDOWS"
    return "LINUX"


def _write_config(env: dict, farm_id: str, queue_id: str, job_history_dir: str) -> None:
    with open(env["DEADLINE_CONFIG_FILE_PATH"], "w") as f:
        f.write(
            "[defaults]\n"
            "aws_profile_name = (default)\n"
            "\n"
            "[profile-(default) defaults]\n"
            f"farm_id = {farm_id}\n"
            "\n"
            f"[profile-(default) {farm_id} defaults]\n"
            f"queue_id = {queue_id}\n"
            "\n"
            "[profile-(default) settings]\n"
            f"job_history_dir = {job_history_dir}\n"
        )


@pytest.fixture
def bundle_dir(tmp_path) -> str:
    d = tmp_path / "bundle"
    d.mkdir()
    (d / "template.json").write_text(json.dumps(SAMPLE_TEMPLATE))
    return str(d)


@pytest.fixture
def env_with_storage_profile(deadline_env, tmp_path) -> dict:
    """Seed a farm + queue + an OS-matched storage profile, and point config at them."""
    backend, env = deadline_env
    farm = backend.create_farm(displayName="TestFarm", description="")
    queue = backend.create_queue(farmId=farm["farmId"], displayName="TestQueue", description="")
    backend.create_storage_profile(
        farmId=farm["farmId"],
        queueId=queue["queueId"],
        displayName="TestStorageProfile",
        osFamily=_os_family(),
    )
    _write_config(env, farm["farmId"], queue["queueId"], str(tmp_path / "job_history"))
    return env


class TestSharedJobSettingsResourceSelectors:
    """The Shared job settings tab exposes editable farm and queue selectors."""

    def test_farm_and_queue_labels_present(self, bundle_dir, submitter_env) -> None:
        with SubmitterDialog.open(bundle_dir, env=submitter_env) as app:
            app.wait_farm_resolved()
            assert app.has_label("Farm"), "Farm selector label missing from the tab"
            assert app.has_label("Queue"), "Queue selector label missing from the tab"

    def test_configured_farm_and_queue_names_shown(self, bundle_dir, submitter_env) -> None:
        """The combo boxes resolve and display the configured farm/queue names."""
        with SubmitterDialog.open(bundle_dir, env=submitter_env) as app:
            app.wait_farm_resolved()
            assert app.wait_text("TestFarm"), "Configured farm name not shown in the selector"
            assert app.wait_text("TestQueue"), "Configured queue name not shown in the selector"


class TestStorageProfileVisibility:
    """The storage-profile row appears only when the queue has matching profiles."""

    def test_storage_profile_hidden_without_profiles(self, bundle_dir, submitter_env) -> None:
        """The default seeded queue has no storage profiles, so the row stays hidden."""
        with SubmitterDialog.open(bundle_dir, env=submitter_env) as app:
            app.wait_farm_resolved()
            assert not app.has_label("Default storage profile"), (
                "Storage-profile row should be hidden when the queue has no profiles"
            )

    def test_storage_profile_shown_with_profiles(
        self, bundle_dir, env_with_storage_profile
    ) -> None:
        """A queue with an OS-matched storage profile reveals the storage-profile row."""
        with SubmitterDialog.open(bundle_dir, env=env_with_storage_profile) as app:
            app.wait_farm_resolved()
            # The row reveals once the queue's storage profiles load. The combo itself
            # sits on "<none selected>" until the user picks one (none is configured),
            # so we assert on the row (label) appearing rather than a profile name.
            assert app.wait_text("Default storage profile"), (
                "Storage-profile row should be visible when the queue has profiles"
            )
