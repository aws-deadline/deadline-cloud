# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests that preGUI bundle hooks are gated solely by allow_bundle_hooks."""

import logging
import os
from unittest.mock import patch

import pytest

from deadline.client.ui.job_bundle_submitter import show_job_bundle_submitter

MODULE = "deadline.client.ui.job_bundle_submitter"


@pytest.fixture
def _patch_submitter_deps(tmp_path):
    """Set up a bundle with a real preGUI hook and patch only the Qt-heavy dependencies of
    show_job_bundle_submitter. The real pre-GUI source-selection and hook execution run, so
    a hook that fires writes a sentinel file; the tests assert on its presence."""
    bundle_dir = str(tmp_path / "bundle")
    os.makedirs(bundle_dir)
    with open(os.path.join(bundle_dir, "template.yaml"), "w") as f:
        f.write("name: Test\nsteps: []\n")

    # A real preGUI hook that writes a sentinel when it runs.
    sentinel = str(tmp_path / "pre_gui_ran.txt")
    with open(os.path.join(bundle_dir, "prefill.py"), "w") as f:
        f.write(f"open({sentinel!r}, 'w').write('ran')\n")
    with open(os.path.join(bundle_dir, "hooks.yaml"), "w") as f:
        f.write("version: '1.0'\npreGUI:\n  - command: python3\n    args: [prefill.py]\n")

    patches = {
        "validate_directory_symlink_containment": patch(
            f"{MODULE}.validate_directory_symlink_containment"
        ),
        "read_yaml_or_json_object": patch(
            f"{MODULE}.read_yaml_or_json_object",
            side_effect=lambda _dir, name, *args, **kwargs: (
                {"name": "Test", "steps": []} if name == "template" else None
            ),
        ),
        "read_job_bundle_parameters": patch(
            f"{MODULE}.read_job_bundle_parameters", return_value=[]
        ),
        "SubmitJobToDeadlineDialog": patch(f"{MODULE}.SubmitJobToDeadlineDialog"),
        "QApplication": patch(f"{MODULE}.QApplication"),
        "QMessageBox": patch(f"{MODULE}.QMessageBox"),
    }

    started = {}
    for name, p in patches.items():
        started[name] = p.start()

    yield {
        "bundle_dir": bundle_dir,
        "sentinel": sentinel,
        **started,
    }

    for p in patches.values():
        p.stop()


def _call_submitter(bundle_dir, settings_map):
    """Call show_job_bundle_submitter with a config that returns values from settings_map."""

    def fake_get_setting(name, config=None):
        return settings_map.get(name, "false")

    with (
        patch(f"{MODULE}._get_setting", side_effect=fake_get_setting),
        patch(f"{MODULE}._config_file") as mock_config_file,
    ):
        mock_config_file.str2bool.side_effect = lambda v: v.lower() == "true"
        show_job_bundle_submitter(input_job_bundle_dir=bundle_dir)


class TestPreGuiHooksPermissionGating:
    """Verify that preGUI hooks execute only when allow_bundle_hooks is true."""

    def test_hooks_blocked_when_bundle_hooks_disabled(self, _patch_submitter_deps, caplog):
        """preGUI hooks must NOT run when allow_bundle_hooks is false."""
        ctx = _patch_submitter_deps
        with caplog.at_level(logging.WARNING):
            _call_submitter(
                ctx["bundle_dir"],
                {
                    "settings.allow_bundle_hooks": "false",
                    "settings.allow_environment_hooks": "false",
                },
            )

        assert not os.path.exists(ctx["sentinel"]), "preGUI hook ran despite being disabled"
        assert "bundle hooks are disabled" in caplog.text

    def test_hooks_blocked_even_when_env_hooks_enabled(self, _patch_submitter_deps, caplog):
        """preGUI bundle hooks must NOT run when only allow_environment_hooks is true.

        This is the core security fix: allow_environment_hooks must not bypass
        the allow_bundle_hooks gate for preGUI bundle hooks.
        """
        ctx = _patch_submitter_deps
        with caplog.at_level(logging.WARNING):
            _call_submitter(
                ctx["bundle_dir"],
                {
                    "settings.allow_bundle_hooks": "false",
                    "settings.allow_environment_hooks": "true",
                },
            )

        assert not os.path.exists(ctx["sentinel"]), "preGUI bundle hook ran despite being disabled"
        assert "bundle hooks are disabled" in caplog.text

    def test_hooks_execute_when_bundle_hooks_enabled(self, _patch_submitter_deps, caplog):
        """preGUI hooks must run when allow_bundle_hooks is true (with auto_accept)."""
        ctx = _patch_submitter_deps
        with caplog.at_level(logging.WARNING):
            _call_submitter(
                ctx["bundle_dir"],
                {
                    "settings.allow_bundle_hooks": "true",
                    "settings.auto_accept": "true",
                },
            )

        assert os.path.exists(ctx["sentinel"]), "preGUI hook did not run when enabled"
        assert "bundle hooks are disabled" not in caplog.text

    def test_hooks_execute_without_env_hooks(self, _patch_submitter_deps, caplog):
        """preGUI hooks run with allow_bundle_hooks=true even when
        allow_environment_hooks is false — env hooks setting is irrelevant."""
        ctx = _patch_submitter_deps
        with caplog.at_level(logging.WARNING):
            _call_submitter(
                ctx["bundle_dir"],
                {
                    "settings.allow_bundle_hooks": "true",
                    "settings.allow_environment_hooks": "false",
                    "settings.auto_accept": "true",
                },
            )

        assert os.path.exists(ctx["sentinel"]), "preGUI hook did not run when enabled"
        assert "bundle hooks are disabled" not in caplog.text
