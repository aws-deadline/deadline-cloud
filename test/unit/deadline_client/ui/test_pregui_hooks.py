# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests that preGUI submission hooks work as expected on the gui-submit path.

These exercise ``show_job_bundle_submitter`` with the Qt-heavy dependencies patched (no
GUI binding required), focusing on hook *behavior*: environment (DEADLINE_HOOKS_DIR) hooks
are loaded in addition to bundle hooks, and a preGUI hook's output (name, description,
parameters) is applied to the dialog's initial settings. The gating rules (allow_bundle_hooks)
are covered separately in ``test_pregui_hooks_permission.py``.
"""

import os
from unittest.mock import MagicMock, patch

from deadline.client.job_bundle._hooks import HookConfiguration, HookDefinition
from deadline.client.ui.job_bundle_submitter import show_job_bundle_submitter

MODULE = "deadline.client.ui.job_bundle_submitter"


def _pre_gui_config(pre_gui=True):
    return HookConfiguration(
        version="1.0",
        pre_gui=[HookDefinition(command="python", args=["prefill.py"])] if pre_gui else [],
        pre_submission=[],
        post_submission=[],
    )


def _run_submitter(bundle_dir, settings_map, hook_manager, *, env=None):
    """Drive show_job_bundle_submitter with Qt/deps patched, returning the constructed
    SubmitJobToDeadlineDialog mock so callers can inspect the initial settings passed to it."""

    def fake_get_setting(name, config=None):
        return settings_map.get(name, "false")

    template = {"name": "Bundle Job", "steps": []}
    with (
        patch(f"{MODULE}.validate_directory_symlink_containment"),
        patch(
            f"{MODULE}.read_yaml_or_json_object",
            side_effect=lambda _dir, name, *a, **k: template if name == "template" else None,
        ),
        patch(f"{MODULE}.read_job_bundle_parameters", return_value=[]),
        patch(f"{MODULE}._HookManager", return_value=hook_manager),
        patch(f"{MODULE}.SubmitJobToDeadlineDialog") as dialog_cls,
        patch(f"{MODULE}.QApplication"),
        patch(f"{MODULE}.QMessageBox"),
        patch(f"{MODULE}._get_setting", side_effect=fake_get_setting),
        patch(f"{MODULE}._config_file") as mock_config_file,
        patch.dict(os.environ, env or {}, clear=False),
    ):
        mock_config_file.str2bool.side_effect = lambda v: str(v).lower() == "true"
        show_job_bundle_submitter(input_job_bundle_dir=bundle_dir)
    return dialog_cls


def _make_bundle(tmp_path):
    bundle_dir = str(tmp_path / "bundle")
    os.makedirs(bundle_dir)
    with open(os.path.join(bundle_dir, "template.yaml"), "w") as f:
        f.write("name: Bundle Job\nsteps: []\n")
    return bundle_dir


class TestPreGuiHookOutputApplied:
    """A preGUI hook's output is applied to the dialog's initial settings."""

    def test_hook_name_applied_to_initial_settings(self, tmp_path):
        bundle_dir = _make_bundle(tmp_path)
        hook_manager = MagicMock()
        hook_manager.hooks = _pre_gui_config()
        hook_manager.load_hooks.return_value = hook_manager.hooks
        hook_manager._original_bundle_dir = bundle_dir
        hook_manager.execute_pre_gui_hooks.return_value = {"name": "PREGUI_RAN"}

        dialog_cls = _run_submitter(
            bundle_dir,
            {"settings.allow_bundle_hooks": "true", "settings.auto_accept": "true"},
            hook_manager,
        )

        hook_manager.execute_pre_gui_hooks.assert_called_once()
        initial_settings = dialog_cls.call_args.kwargs["initial_job_settings"]
        assert initial_settings.name == "PREGUI_RAN"

    def test_hook_description_applied_to_initial_settings(self, tmp_path):
        bundle_dir = _make_bundle(tmp_path)
        hook_manager = MagicMock()
        hook_manager.hooks = _pre_gui_config()
        hook_manager.load_hooks.return_value = hook_manager.hooks
        hook_manager._original_bundle_dir = bundle_dir
        hook_manager.execute_pre_gui_hooks.return_value = {"description": "from pipeline"}

        dialog_cls = _run_submitter(
            bundle_dir,
            {"settings.allow_bundle_hooks": "true", "settings.auto_accept": "true"},
            hook_manager,
        )

        initial_settings = dialog_cls.call_args.kwargs["initial_job_settings"]
        assert initial_settings.description == "from pipeline"

    def test_hook_shared_parameter_applied(self, tmp_path):
        """A deadline: shared job property from the hook flows into the initial shared
        parameter values passed to the dialog."""
        bundle_dir = _make_bundle(tmp_path)
        hook_manager = MagicMock()
        hook_manager.hooks = _pre_gui_config()
        hook_manager.load_hooks.return_value = hook_manager.hooks
        hook_manager._original_bundle_dir = bundle_dir
        hook_manager.execute_pre_gui_hooks.return_value = {"parameters": {"deadline:priority": 90}}

        dialog_cls = _run_submitter(
            bundle_dir,
            {"settings.allow_bundle_hooks": "true", "settings.auto_accept": "true"},
            hook_manager,
        )

        shared = dialog_cls.call_args.kwargs["initial_shared_parameter_values"]
        assert shared.get("deadline:priority") == 90

    def test_no_pre_gui_hooks_no_execution(self, tmp_path):
        """A bundle without preGUI hooks does not invoke execute_pre_gui_hooks."""
        bundle_dir = _make_bundle(tmp_path)
        hook_manager = MagicMock()
        hook_manager.hooks = _pre_gui_config(pre_gui=False)
        hook_manager.load_hooks.return_value = hook_manager.hooks
        hook_manager._original_bundle_dir = bundle_dir

        _run_submitter(
            bundle_dir,
            {"settings.allow_bundle_hooks": "true", "settings.auto_accept": "true"},
            hook_manager,
        )

        hook_manager.execute_pre_gui_hooks.assert_not_called()


class TestPreGuiEnvironmentHooksLoaded:
    """PreGUI hooks are loaded from DEADLINE_HOOKS_DIR, not just the job bundle."""

    def test_env_hooks_dir_is_used_as_a_source(self, tmp_path):
        """With DEADLINE_HOOKS_DIR set and environment hooks enabled, a _HookManager is
        constructed for the env dir (the #2 fix — the loader must consult it)."""
        bundle_dir = _make_bundle(tmp_path)
        studio = str(tmp_path / "studio")
        os.makedirs(studio)

        constructed_with = []

        def factory(job_bundle_dir, *a, **k):
            constructed_with.append(job_bundle_dir)
            m = MagicMock()
            m.hooks = _pre_gui_config()
            m.load_hooks.return_value = m.hooks
            m._original_bundle_dir = job_bundle_dir
            m.execute_pre_gui_hooks.return_value = {}
            return m

        def fake_get_setting(name, config=None):
            return {
                "settings.allow_bundle_hooks": "true",
                "settings.allow_environment_hooks": "true",
                "settings.auto_accept": "true",
            }.get(name, "false")

        template = {"name": "Bundle Job", "steps": []}
        with (
            patch(f"{MODULE}.validate_directory_symlink_containment"),
            patch(
                f"{MODULE}.read_yaml_or_json_object",
                side_effect=lambda _dir, name, *a, **k: template if name == "template" else None,
            ),
            patch(f"{MODULE}.read_job_bundle_parameters", return_value=[]),
            patch(f"{MODULE}._HookManager", side_effect=factory),
            patch(f"{MODULE}.SubmitJobToDeadlineDialog"),
            patch(f"{MODULE}.QApplication"),
            patch(f"{MODULE}.QMessageBox"),
            patch(f"{MODULE}._get_setting", side_effect=fake_get_setting),
            patch(f"{MODULE}._config_file") as mock_config_file,
            patch.dict(os.environ, {"DEADLINE_HOOKS_DIR": studio}, clear=False),
        ):
            mock_config_file.str2bool.side_effect = lambda v: str(v).lower() == "true"
            show_job_bundle_submitter(input_job_bundle_dir=bundle_dir)

        assert studio in constructed_with  # env dir was consulted
        assert bundle_dir in constructed_with

    def test_env_hooks_dir_ignored_when_env_hooks_disabled(self, tmp_path):
        """A preGUI hook in DEADLINE_HOOKS_DIR does not run when environment hooks are
        disabled (only the bundle source, which here has none, is considered)."""
        bundle_dir = _make_bundle(tmp_path)
        studio = str(tmp_path / "studio")
        os.makedirs(studio)

        executed = []

        def factory(job_bundle_dir, *a, **k):
            m = MagicMock()
            # Only the env dir has preGUI hooks; the bundle has none.
            m.hooks = _pre_gui_config(pre_gui=(job_bundle_dir == studio))
            m.load_hooks.return_value = m.hooks
            m._original_bundle_dir = job_bundle_dir

            def _exec(_meta):
                executed.append(job_bundle_dir)
                return {}

            m.execute_pre_gui_hooks.side_effect = _exec
            return m

        def fake_get_setting(name, config=None):
            return {
                "settings.allow_bundle_hooks": "true",
                "settings.allow_environment_hooks": "false",
                "settings.auto_accept": "true",
            }.get(name, "false")

        template = {"name": "Bundle Job", "steps": []}
        with (
            patch(f"{MODULE}.validate_directory_symlink_containment"),
            patch(
                f"{MODULE}.read_yaml_or_json_object",
                side_effect=lambda _dir, name, *a, **k: template if name == "template" else None,
            ),
            patch(f"{MODULE}.read_job_bundle_parameters", return_value=[]),
            patch(f"{MODULE}._HookManager", side_effect=factory),
            patch(f"{MODULE}.SubmitJobToDeadlineDialog"),
            patch(f"{MODULE}.QApplication"),
            patch(f"{MODULE}.QMessageBox"),
            patch(f"{MODULE}._get_setting", side_effect=fake_get_setting),
            patch(f"{MODULE}._config_file") as mock_config_file,
            patch.dict(os.environ, {"DEADLINE_HOOKS_DIR": studio}, clear=False),
        ):
            mock_config_file.str2bool.side_effect = lambda v: str(v).lower() == "true"
            show_job_bundle_submitter(input_job_bundle_dir=bundle_dir)

        assert executed == []  # env preGUI hook did not run (env hooks disabled)


class TestPreGuiHookCliPrecedence:
    """CLI --parameter values take precedence over preGUI hook parameters, for both template
    and shared parameters. Regression for a bug where template CLI params were popped out of
    the working dict before the hook merge ran, letting a hook silently override them."""

    def _run(self, bundle_dir, hook_manager, bundle_parameters, job_parameters):
        def fake_get_setting(name, config=None):
            return {
                "settings.allow_bundle_hooks": "true",
                "settings.auto_accept": "true",
            }.get(name, "false")

        template = {"name": "Bundle Job", "steps": []}
        with (
            patch(f"{MODULE}.validate_directory_symlink_containment"),
            patch(
                f"{MODULE}.read_yaml_or_json_object",
                side_effect=lambda _dir, name, *a, **k: template if name == "template" else None,
            ),
            patch(f"{MODULE}.read_job_bundle_parameters", return_value=bundle_parameters),
            patch(f"{MODULE}._HookManager", return_value=hook_manager),
            patch(f"{MODULE}.SubmitJobToDeadlineDialog") as dialog_cls,
            patch(f"{MODULE}.QApplication"),
            patch(f"{MODULE}.QMessageBox"),
            patch(f"{MODULE}._get_setting", side_effect=fake_get_setting),
            patch(f"{MODULE}._config_file") as mock_config_file,
        ):
            mock_config_file.str2bool.side_effect = lambda v: str(v).lower() == "true"
            show_job_bundle_submitter(
                input_job_bundle_dir=bundle_dir, job_parameters=job_parameters
            )
        return dialog_cls

    def test_cli_template_parameter_wins_over_hook(self, tmp_path):
        """A CLI --parameter value for a *template* parameter is not overridden by a preGUI
        hook emitting the same parameter name."""
        bundle_dir = _make_bundle(tmp_path)
        hook_manager = MagicMock()
        hook_manager.hooks = _pre_gui_config()
        hook_manager.load_hooks.return_value = hook_manager.hooks
        hook_manager._original_bundle_dir = bundle_dir
        hook_manager.execute_pre_gui_hooks.return_value = {"parameters": {"Foo": "hook_value"}}

        dialog_cls = self._run(
            bundle_dir,
            hook_manager,
            bundle_parameters=[{"name": "Foo", "type": "STRING", "default": "bundle_value"}],
            job_parameters=[{"name": "Foo", "value": "cli_value"}],
        )

        initial_settings = dialog_cls.call_args.kwargs["initial_job_settings"]
        foo = next(p for p in initial_settings.parameters if p["name"] == "Foo")
        assert foo["value"] == "cli_value"

    def test_hook_template_parameter_applied_when_no_cli_value(self, tmp_path):
        """Without a CLI value, the preGUI hook's template parameter is applied (the guard
        does not block hook values for parameters the CLI did not supply)."""
        bundle_dir = _make_bundle(tmp_path)
        hook_manager = MagicMock()
        hook_manager.hooks = _pre_gui_config()
        hook_manager.load_hooks.return_value = hook_manager.hooks
        hook_manager._original_bundle_dir = bundle_dir
        hook_manager.execute_pre_gui_hooks.return_value = {"parameters": {"Foo": "hook_value"}}

        dialog_cls = self._run(
            bundle_dir,
            hook_manager,
            bundle_parameters=[{"name": "Foo", "type": "STRING", "default": "bundle_value"}],
            job_parameters=[],
        )

        initial_settings = dialog_cls.call_args.kwargs["initial_job_settings"]
        foo = next(p for p in initial_settings.parameters if p["name"] == "Foo")
        assert foo["value"] == "hook_value"
