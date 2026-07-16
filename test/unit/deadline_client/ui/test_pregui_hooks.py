# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests that preGUI submission hooks work as expected.

Two layers are covered:

* ``show_job_bundle_submitter`` integration (the gui-submit path) with the Qt-heavy
  dependencies patched (no GUI binding required), focusing on hook *behavior*: environment
  (DEADLINE_HOOKS_DIR) hooks are loaded in addition to bundle hooks, and a preGUI hook's
  output (name, description, parameters) is applied to the dialog's initial settings.
* ``run_pre_gui_hooks`` / ``apply_pre_gui_output`` called directly (the way DCC submitters
  call them), headless — no dialog, no Qt.

The pre-GUI logic lives in ``deadline.client.ui.pre_gui_hooks`` (Qt-free); the confirmation
prompt and the ``show_job_bundle_submitter`` call site live in ``job_bundle_submitter``. The
gating rules (allow_bundle_hooks) are covered separately in ``test_pregui_hooks_permission.py``.
"""

import os
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from deadline.client.exceptions import DeadlineOperationCanceled
from deadline.client.job_bundle._hooks import HookConfiguration, HookDefinition
from deadline.client.ui.dataclasses import JobBundleSettings
from deadline.client.ui.job_bundle_submitter import show_job_bundle_submitter
from deadline.client.ui.pre_gui_hooks import (
    PreGuiHookContext,
    apply_pre_gui_output,
    run_pre_gui_hooks,
)

# Seams in the submitter module (dialog construction, bundle loading, auto_accept check).
MODULE = "deadline.client.ui.job_bundle_submitter"
# Seams in the pre-GUI hooks module (source selection + config gates that run_pre_gui_hooks
# resolves). HookManager lives here now; _get_setting / _config_file are imported by BOTH
# modules, so integration tests patch them in both places.
HOOKS_MODULE = "deadline.client.ui.pre_gui_hooks"


def _pre_gui_config(pre_gui=True):
    return HookConfiguration(
        version="1.0",
        pre_gui=[HookDefinition(command="python", args=["prefill.py"])] if pre_gui else [],
        pre_submission=[],
        post_submission=[],
    )


def _run_submitter(bundle_dir, settings_map, hook_manager, *, env=None, job_parameters=None):
    """Drive show_job_bundle_submitter with Qt/deps patched, returning the constructed
    SubmitJobToDeadlineDialog mock so callers can inspect the initial settings passed to it."""

    return _run_submitter_with_factory(
        bundle_dir,
        settings_map,
        hook_factory=lambda *a, **k: hook_manager,
        env=env,
        job_parameters=job_parameters,
    )


def _run_submitter_with_factory(
    bundle_dir, settings_map, hook_factory, *, bundle_parameters=None, env=None, job_parameters=None
):
    """Like _run_submitter but constructs a fresh HookManager per source via hook_factory,
    so tests can distinguish the env-dir manager from the bundle-dir manager."""

    def fake_get_setting(name, config=None):
        return settings_map.get(name, "false")

    template = {"name": "Bundle Job", "steps": []}
    with (
        patch(f"{MODULE}.validate_directory_symlink_containment"),
        patch(
            f"{MODULE}.read_yaml_or_json_object",
            side_effect=lambda _dir, name, *a, **k: template if name == "template" else None,
        ),
        patch(f"{MODULE}.read_job_bundle_parameters", return_value=bundle_parameters or []),
        patch(f"{HOOKS_MODULE}.HookManager", side_effect=hook_factory),
        patch(f"{MODULE}.SubmitJobToDeadlineDialog") as dialog_cls,
        patch(f"{MODULE}.QApplication"),
        patch(f"{MODULE}.QMessageBox"),
        # _get_setting / _config_file are read by both the submitter (auto_accept) and the
        # hooks module (allow_* gates + farm/queue defaults) — patch both.
        patch(f"{MODULE}._get_setting", side_effect=fake_get_setting),
        patch(f"{HOOKS_MODULE}._get_setting", side_effect=fake_get_setting),
        patch(f"{MODULE}._config_file") as mock_config_file,
        patch(f"{HOOKS_MODULE}._config_file") as hooks_config_file,
        patch.dict(os.environ, env or {}, clear=False),
    ):
        mock_config_file.str2bool.side_effect = lambda v: str(v).lower() == "true"
        hooks_config_file.str2bool.side_effect = lambda v: str(v).lower() == "true"
        show_job_bundle_submitter(input_job_bundle_dir=bundle_dir, job_parameters=job_parameters)
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
        """With DEADLINE_HOOKS_DIR set and environment hooks enabled, a HookManager is
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

        _run_submitter_with_factory(
            bundle_dir,
            {
                "settings.allow_bundle_hooks": "true",
                "settings.allow_environment_hooks": "true",
                "settings.auto_accept": "true",
            },
            hook_factory=factory,
            env={"DEADLINE_HOOKS_DIR": studio},
        )

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

        _run_submitter_with_factory(
            bundle_dir,
            {
                "settings.allow_bundle_hooks": "true",
                "settings.allow_environment_hooks": "false",
                "settings.auto_accept": "true",
            },
            hook_factory=factory,
            env={"DEADLINE_HOOKS_DIR": studio},
        )

        assert executed == []  # env preGUI hook did not run (env hooks disabled)


class TestPreGuiHookCliPrecedence:
    """CLI --parameter values take precedence over preGUI hook parameters, for both template
    and shared parameters. Regression for a bug where template CLI params were popped out of
    the working dict before the hook merge ran, letting a hook silently override them."""

    def test_cli_template_parameter_wins_over_hook(self, tmp_path):
        """A CLI --parameter value for a *template* parameter is not overridden by a preGUI
        hook emitting the same parameter name."""
        bundle_dir = _make_bundle(tmp_path)
        hook_manager = MagicMock()
        hook_manager.hooks = _pre_gui_config()
        hook_manager.load_hooks.return_value = hook_manager.hooks
        hook_manager._original_bundle_dir = bundle_dir
        hook_manager.execute_pre_gui_hooks.return_value = {"parameters": {"Foo": "hook_value"}}

        dialog_cls = _run_submitter_with_factory(
            bundle_dir,
            {"settings.allow_bundle_hooks": "true", "settings.auto_accept": "true"},
            hook_factory=lambda *a, **k: hook_manager,
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

        dialog_cls = _run_submitter_with_factory(
            bundle_dir,
            {"settings.allow_bundle_hooks": "true", "settings.auto_accept": "true"},
            hook_factory=lambda *a, **k: hook_manager,
            bundle_parameters=[{"name": "Foo", "type": "STRING", "default": "bundle_value"}],
            job_parameters=[],
        )

        initial_settings = dialog_cls.call_args.kwargs["initial_job_settings"]
        foo = next(p for p in initial_settings.parameters if p["name"] == "Foo")
        assert foo["value"] == "hook_value"


def _make_env_hook_manager(hooks_dir):
    """A HookManager stand-in whose preGUI hook echoes the metadata it received back to the
    caller, so tests can assert on what run_pre_gui_hooks passed in."""
    m = MagicMock()
    m.hooks = _pre_gui_config()
    m.load_hooks.return_value = m.hooks
    m._original_bundle_dir = hooks_dir
    return m


@contextmanager
def _headless_run(settings_map, hook_manager, env=None):
    """Patch the hooks-package seam + config so run_pre_gui_hooks runs headless (no Qt, no
    dialog). Yields nothing — callers just invoke run_pre_gui_hooks inside the block."""

    def fake_get_setting(name, config=None):
        return settings_map.get(name, "false")

    with (
        patch(f"{HOOKS_MODULE}.HookManager", return_value=hook_manager),
        patch(f"{HOOKS_MODULE}._get_setting", side_effect=fake_get_setting),
        patch(f"{HOOKS_MODULE}._config_file") as cfg,
        patch.dict(os.environ, env or {}, clear=False),
    ):
        cfg.str2bool.side_effect = lambda v: str(v).lower() == "true"
        yield


class TestRunPreGuiHooksHeadless:
    """run_pre_gui_hooks is Qt-free and callable directly by DCC submitters. These tests do
    NOT construct any dialog — they exercise the public function the way Maya/Nuke will."""

    def test_dcc_env_only_source_runs_without_a_bundle(self, tmp_path):
        """The DCC scenario: bundle_dir=None, hooks come only from DEADLINE_HOOKS_DIR. The
        env hook runs and its output is returned — no dialog, no Qt, no bundle on disk."""
        studio = str(tmp_path / "studio")
        os.makedirs(studio)
        hook_manager = _make_env_hook_manager(studio)
        hook_manager.execute_pre_gui_hooks.return_value = {"name": "PREGUI_RAN"}

        with _headless_run(
            {
                "settings.allow_environment_hooks": "true",
                "defaults.farm_id": "farm-123",
                "defaults.queue_id": "queue-456",
            },
            hook_manager,
            env={"DEADLINE_HOOKS_DIR": studio},
        ):
            output = run_pre_gui_hooks(
                PreGuiHookContext(bundle_dir=None, job_name="Initial", submitter_name="maya")
            )

        assert output == {"name": "PREGUI_RAN"}
        hook_manager.execute_pre_gui_hooks.assert_called_once()
        # The metadata handed to the hook carries the DCC context and an empty bundle dir.
        metadata = hook_manager.execute_pre_gui_hooks.call_args.args[0]
        assert metadata.submitter_name == "maya"
        assert metadata.job_bundle_dir == ""
        assert metadata.farm_id == "farm-123"
        assert metadata.queue_id == "queue-456"

    def test_no_sources_returns_empty_and_skips_confirmation(self, tmp_path):
        """With no runnable preGUI hooks, the function returns {} and never calls the
        confirm callback (nothing to confirm)."""
        studio = str(tmp_path / "studio")
        os.makedirs(studio)
        hook_manager = _make_env_hook_manager(studio)
        hook_manager.hooks = _pre_gui_config(pre_gui=False)
        hook_manager.load_hooks.return_value = hook_manager.hooks

        confirm = MagicMock(return_value=True)
        with _headless_run(
            {"settings.allow_environment_hooks": "true"},
            hook_manager,
            env={"DEADLINE_HOOKS_DIR": studio},
        ):
            output = run_pre_gui_hooks(PreGuiHookContext(bundle_dir=None), confirm_callback=confirm)

        assert output == {}
        confirm.assert_not_called()
        hook_manager.execute_pre_gui_hooks.assert_not_called()

    def test_confirm_callback_declined_raises_cancel(self, tmp_path):
        """A confirm_callback returning False cancels the submission and no hook runs."""
        studio = str(tmp_path / "studio")
        os.makedirs(studio)
        hook_manager = _make_env_hook_manager(studio)

        with _headless_run(
            {"settings.allow_environment_hooks": "true"},
            hook_manager,
            env={"DEADLINE_HOOKS_DIR": studio},
        ):
            with pytest.raises(DeadlineOperationCanceled):
                run_pre_gui_hooks(
                    PreGuiHookContext(bundle_dir=None), confirm_callback=lambda _sources: False
                )

        hook_manager.execute_pre_gui_hooks.assert_not_called()

    def test_confirm_callback_accepted_runs_hook(self, tmp_path):
        """A confirm_callback returning True lets the hook run; it receives the source list."""
        studio = str(tmp_path / "studio")
        os.makedirs(studio)
        hook_manager = _make_env_hook_manager(studio)
        hook_manager.execute_pre_gui_hooks.return_value = {"description": "ok"}

        seen = {}

        def confirm(sources):
            seen["count"] = len(sources)
            return True

        with _headless_run(
            {"settings.allow_environment_hooks": "true"},
            hook_manager,
            env={"DEADLINE_HOOKS_DIR": studio},
        ):
            output = run_pre_gui_hooks(PreGuiHookContext(bundle_dir=None), confirm_callback=confirm)

        assert output == {"description": "ok"}
        assert seen["count"] == 1
        hook_manager.execute_pre_gui_hooks.assert_called_once()

    def test_none_confirm_callback_runs_without_prompting(self, tmp_path):
        """confirm_callback=None (e.g. auto_accept at the call site) runs hooks silently."""
        studio = str(tmp_path / "studio")
        os.makedirs(studio)
        hook_manager = _make_env_hook_manager(studio)
        hook_manager.execute_pre_gui_hooks.return_value = {"name": "N"}

        with _headless_run(
            {"settings.allow_environment_hooks": "true"},
            hook_manager,
            env={"DEADLINE_HOOKS_DIR": studio},
        ):
            output = run_pre_gui_hooks(PreGuiHookContext(bundle_dir=None), confirm_callback=None)

        assert output == {"name": "N"}
        hook_manager.execute_pre_gui_hooks.assert_called_once()


class _DccSettings:
    """A minimal DCC-style settings object: assignable name/description, NO parameters list.

    Stands in for Maya's RenderSubmitterUISettings / Nuke's SubmitterUISettings, which have no
    template-parameter list — so apply_pre_gui_output must route every hook parameter to the
    shared values dict rather than onto the settings object."""

    def __init__(self):
        self.name = "Original"
        self.description = ""


class TestApplyPreGuiOutput:
    """apply_pre_gui_output routes merged hook output onto a settings object + shared dict.

    It is generic: JobBundleSettings (with a .parameters template list) gets template params
    routed in place, while DCC settings (no .parameters) send every param to the shared dict.
    """

    def _settings(self, parameters=None):
        s = JobBundleSettings(input_job_bundle_dir="/bundle", name="Original")
        s.parameters = parameters or []
        return s

    def test_name_and_description_applied(self):
        settings = self._settings()
        shared: dict = {}
        apply_pre_gui_output({"name": "NEW", "description": "desc"}, settings, shared)
        assert settings.name == "NEW"
        assert settings.description == "desc"

    def test_template_parameter_updated_in_place(self):
        settings = self._settings(parameters=[{"name": "Foo", "type": "STRING", "default": "orig"}])
        shared: dict = {}
        apply_pre_gui_output({"parameters": {"Foo": "from_hook"}}, settings, shared)
        foo = next(p for p in settings.parameters if p["name"] == "Foo")
        assert foo["value"] == "from_hook"
        assert "Foo" not in shared  # template param stays on settings, not shared

    def test_non_template_parameter_lands_in_shared(self):
        settings = self._settings()
        shared: dict = {}
        apply_pre_gui_output({"parameters": {"deadline:priority": 90}}, settings, shared)
        assert shared["deadline:priority"] == 90

    def test_cli_provided_name_blocks_hook_override(self):
        settings = self._settings(
            parameters=[{"name": "Foo", "type": "STRING", "value": "cli_value"}]
        )
        shared: dict = {}
        apply_pre_gui_output(
            {"parameters": {"Foo": "hook_value"}},
            settings,
            shared,
            cli_provided_param_names={"Foo"},
        )
        foo = next(p for p in settings.parameters if p["name"] == "Foo")
        assert foo["value"] == "cli_value"  # CLI wins

    def test_empty_output_is_a_noop(self):
        settings = self._settings()
        shared: dict = {}
        apply_pre_gui_output({}, settings, shared)
        assert settings.name == "Original"
        assert shared == {}

    def test_dcc_settings_name_and_description_applied(self):
        """A DCC settings object (no .parameters) still gets name/description applied."""
        settings = _DccSettings()
        shared: dict = {}
        apply_pre_gui_output({"name": "NEW", "description": "desc"}, settings, shared)
        assert settings.name == "NEW"
        assert settings.description == "desc"

    def test_dcc_settings_all_parameters_go_to_shared(self):
        """With no template-parameter list, every hook parameter lands in the shared values —
        the generic behavior DCC submitters (Maya, Nuke) rely on."""
        settings = _DccSettings()
        shared: dict = {"RezPackages": "mayaIO-2024 deadline_cloud_for_maya"}
        apply_pre_gui_output(
            {"parameters": {"deadline:priority": 90, "RezPackages": "mayaIO-2024 custom_pkg"}},
            settings,
            shared,
        )
        assert shared["deadline:priority"] == 90
        assert shared["RezPackages"] == "mayaIO-2024 custom_pkg"  # overrides default


class TestPreGuiHookContext:
    """PreGuiHookContext bundles hook inputs so run_pre_gui_hooks' signature stays stable."""

    def test_defaults_match_env_only_dcc_usage(self):
        """A bare context is the DCC default: no bundle, JobBundle submitter identity."""
        ctx = PreGuiHookContext()
        assert ctx.bundle_dir is None
        assert ctx.parameters == {}
        assert ctx.submitter_name == "JobBundle"
        assert ctx.priority == 50

    def test_parameters_default_is_not_shared_between_instances(self):
        """The mutable ``parameters`` default is per-instance (field(default_factory=dict))."""
        a = PreGuiHookContext()
        a.parameters["x"] = 1
        b = PreGuiHookContext()
        assert b.parameters == {}
