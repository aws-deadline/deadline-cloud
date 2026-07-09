# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests that preGUI bundle hooks are gated solely by allow_bundle_hooks."""

import logging
from unittest.mock import MagicMock, patch

import pytest

from deadline.client.job_bundle._hooks import HookConfiguration, HookDefinition
from deadline.client.ui.job_bundle_submitter import show_job_bundle_submitter

# show_job_bundle_submitter lives here (dialog construction, auto_accept check)...
MODULE = "deadline.client.ui.job_bundle_submitter"
# ...but the pre-GUI hook sourcing + gating it delegates to lives in this module, so the
# HookManager seam and the allow_*_hooks config reads must be patched here.
HOOKS_MODULE = "deadline.client.ui.pre_gui_hooks"


@pytest.fixture
def hooks_with_pre_gui():
    """A HookConfiguration that has a preGUI hook defined."""
    return HookConfiguration(
        version="1.0",
        pre_gui=[HookDefinition(command="python", args=["prefill.py"])],
        pre_submission=[],
        post_submission=[],
    )


@pytest.fixture
def _patch_submitter_deps(tmp_path, hooks_with_pre_gui):
    """Patch all heavy dependencies of show_job_bundle_submitter so we can
    exercise the hooks permission logic without Qt or real file I/O."""
    bundle_dir = str(tmp_path / "bundle")
    (tmp_path / "bundle").mkdir()
    (tmp_path / "bundle" / "template.yaml").write_text("name: Test\nsteps: []\n")

    mock_hook_manager = MagicMock()
    mock_hook_manager.load_hooks.return_value = hooks_with_pre_gui
    mock_hook_manager.execute_pre_gui_hooks.return_value = {}

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
        "HookManager": patch(f"{HOOKS_MODULE}.HookManager", return_value=mock_hook_manager),
        "SubmitJobToDeadlineDialog": patch(f"{MODULE}.SubmitJobToDeadlineDialog"),
        "QApplication": patch(f"{MODULE}.QApplication"),
        "QMessageBox": patch(f"{MODULE}.QMessageBox"),
    }

    started = {}
    for name, p in patches.items():
        started[name] = p.start()

    yield {
        "bundle_dir": bundle_dir,
        "hook_manager": mock_hook_manager,
        **started,
    }

    for p in patches.values():
        p.stop()


def _call_submitter(bundle_dir, settings_map):
    """Call show_job_bundle_submitter with a config that returns values from settings_map."""

    def fake_get_setting(name, config=None):
        return settings_map.get(name, "false")

    # _get_setting / _config_file are read by both the submitter (auto_accept) and the
    # pre_gui_hooks module (allow_bundle_hooks / allow_environment_hooks gates) — patch both.
    with (
        patch(f"{MODULE}._get_setting", side_effect=fake_get_setting),
        patch(f"{MODULE}._config_file") as mock_config_file,
        patch(f"{HOOKS_MODULE}._get_setting", side_effect=fake_get_setting),
        patch(f"{HOOKS_MODULE}._config_file") as hooks_config_file,
    ):
        mock_config_file.str2bool.side_effect = lambda v: v.lower() == "true"
        hooks_config_file.str2bool.side_effect = lambda v: v.lower() == "true"
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

        ctx["hook_manager"].execute_pre_gui_hooks.assert_not_called()
        assert "bundle hooks are disabled" in caplog.text

    def test_hooks_blocked_even_when_env_hooks_enabled(self, _patch_submitter_deps, caplog):
        """preGUI hooks must NOT run when only allow_environment_hooks is true.

        This is the core security fix: allow_environment_hooks must not bypass
        the allow_bundle_hooks gate for preGUI hooks.
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

        ctx["hook_manager"].execute_pre_gui_hooks.assert_not_called()
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

        ctx["hook_manager"].execute_pre_gui_hooks.assert_called_once()
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

        ctx["hook_manager"].execute_pre_gui_hooks.assert_called_once()
        assert "bundle hooks are disabled" not in caplog.text
