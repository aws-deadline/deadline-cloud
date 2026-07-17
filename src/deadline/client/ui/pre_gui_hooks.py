# coding: utf-8
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""Pre-GUI submission hooks.

This module is the entry point DCC submitters (Maya, Nuke, etc.) and the standalone
``gui-submit`` command call to run pre-GUI hooks before building
``SubmitJobToDeadlineDialog``, so studios can pre-populate dialog fields.

It is deliberately kept out of ``job_bundle_submitter.py`` — which imports ``qtpy`` at module
top — so that ``run_pre_gui_hooks`` can be imported and unit-tested **headless** (no Qt
bindings). The one Qt-coupled helper, :func:`qt_hook_confirmation`, imports Qt lazily inside
its body, so importing this module never requires Qt.
"""

from __future__ import annotations
import os
from dataclasses import dataclass, field
from logging import getLogger
from typing import Any, Callable, Dict, List, Optional

from ..config import config_file as _config_file
from ..config.config_file import get_setting as _get_setting
from ..exceptions import DeadlineOperationCanceled as _DeadlineOperationCanceled
from ..job_bundle._hooks import (
    HookMetadata as _HookMetadata,
    _generate_hooks_confirmation_message,
    collect_pre_gui_hook_sources as _collect_pre_gui_hook_sources,
)
from ..job_bundle.hooks import HookManager
from ._utils import tr

logger = getLogger(__name__)

__all__ = [
    "PreGuiHookContext",
    "apply_pre_gui_output",
    "qt_hook_confirmation",
    "run_pre_gui_hooks",
]


@dataclass
class PreGuiHookContext:
    """The inputs passed to pre-GUI hooks, bundled into one object.

    Collecting these into a dataclass (rather than passing each as a keyword argument to
    :func:`run_pre_gui_hooks`) keeps that function's signature stable as the pre-GUI hook
    metadata grows: a new field is added here — with a default, so it stays backward
    compatible — instead of adding another parameter to every call site.

    Attributes:
        bundle_dir: The job bundle directory, or ``None`` for env-only (DCC) sourcing. DCC
            submitters have no on-disk bundle at pre-GUI time and leave this ``None``, so
            their only hook source is ``DEADLINE_HOOKS_DIR``.
        job_name: Initial job name passed to hooks as ``jobName``.
        parameters: Current parameter values passed to hooks (name -> value).
        submitter_name: The submitter identity passed to hooks (e.g. "maya", "nuke").
        priority: Initial priority passed to hooks.
        farm_id / queue_id / storage_profile_id: Passed to hooks; when ``None`` the
            farm/queue default from config (and the storage-profile setting) is used.
    """

    bundle_dir: Optional[str] = None
    job_name: str = ""
    parameters: Dict[str, Any] = field(default_factory=dict)
    submitter_name: str = "JobBundle"
    priority: int = 50
    farm_id: Optional[str] = None
    queue_id: Optional[str] = None
    storage_profile_id: Optional[str] = None


def run_pre_gui_hooks(
    context: PreGuiHookContext,
    *,
    confirm_callback: Optional[Callable[[List[HookManager]], bool]] = None,
) -> dict[str, Any]:
    """Run all allowed pre-GUI hooks and return their merged output.

    This is the entry point DCC submitters (Maya, Nuke, etc.) call before building
    ``SubmitJobToDeadlineDialog`` so studios can pre-populate dialog fields. It is
    deliberately free of any Qt import so it can be called (and unit-tested) headless;
    the confirmation prompt is injected via ``confirm_callback``.

    Pre-GUI hooks may come from the directory named by ``DEADLINE_HOOKS_DIR`` (gated by
    ``settings.allow_environment_hooks``) and, when ``context.bundle_dir`` is given, the job
    bundle (gated by ``settings.allow_bundle_hooks``). Environment hooks run first, then bundle
    hooks. DCC submitters pass ``bundle_dir=None``, so their only source is ``DEADLINE_HOOKS_DIR``.

    Args:
        context: The :class:`PreGuiHookContext` carrying the data passed to hooks. Callers
            build one object rather than passing many keyword arguments, so this signature
            stays stable as the hook metadata grows.
        confirm_callback: Called once with the list of ``HookManager`` sources whose
            preGUI hooks will run; returns ``True`` to proceed or ``False`` to cancel
            (raising ``DeadlineOperationCanceled``). When ``None``, hooks run without
            prompting — callers that want the standard prompt pass ``qt_hook_confirmation``.

    Returns:
        The merged pre-GUI output (any of ``name``, ``description``, ``parameters``), or an
        empty dict if no pre-GUI hooks ran.
    """
    # Source selection (which bundle/env HookManagers have runnable preGUI hooks) is
    # Qt-free logic in the hooks package so it can be unit-tested without a GUI.
    sources = _collect_pre_gui_hook_sources(
        bundle_dir=context.bundle_dir or "",
        env_hooks_dir=os.environ.get("DEADLINE_HOOKS_DIR"),
        allow_bundle_hooks=_config_file.str2bool(_get_setting("settings.allow_bundle_hooks")),
        allow_environment_hooks=_config_file.str2bool(
            _get_setting("settings.allow_environment_hooks")
        ),
        # Hook execution messages ("Running pre-GUI hook…") log at info; "hooks present but
        # disabled" guidance logs at warning (its pre-refactor severity).
        print_callback=logger.info,
        warning_callback=logger.warning,
        # Pass our reference so tests can patch `{MODULE}.HookManager` as a behavior seam.
        hook_manager_cls=HookManager,
    )

    if not sources:
        return {}

    # Confirmation is injected rather than built-in because:
    # 1. Building it in would require importing Qt (QMessageBox) here, making this function
    #    impossible to call or unit-test without Qt bindings — but DCC callers need it headless.
    # 2. The auto_accept policy differs by call site (gui-submit checks settings; a DCC may
    #    always prompt; a CI job may never prompt). Injecting keeps this function policy-free.
    # Callers pass qt_hook_confirmation(parent) for the standard dialog, or None to skip.
    if confirm_callback is not None and not confirm_callback(sources):
        raise _DeadlineOperationCanceled("Job submission canceled (user declined hooks).")

    resolved_farm_id = (
        context.farm_id if context.farm_id is not None else (_get_setting("defaults.farm_id") or "")
    )
    resolved_queue_id = (
        context.queue_id
        if context.queue_id is not None
        else (_get_setting("defaults.queue_id") or "")
    )
    resolved_storage_profile_id = (
        context.storage_profile_id
        if context.storage_profile_id is not None
        else (_get_setting("settings.storage_profile_id") or None)
    )

    merged: dict[str, Any] = {}
    for manager in sources:
        # Every hook — bundle or environment — receives the job bundle being submitted as
        # its job_bundle_dir, matching the pre/post-submission contract. (The manager still
        # resolves relative hook script paths against its own directory.) DCC callers pass
        # bundle_dir=None, so hooks see an empty jobBundleDir at pre-GUI time.
        metadata = _HookMetadata(
            job_name=context.job_name,
            priority=context.priority,
            farm_id=resolved_farm_id,
            queue_id=resolved_queue_id,
            job_bundle_dir=context.bundle_dir or "",
            parameters=dict(context.parameters or {}),
            submitter_name=context.submitter_name,
            asset_references={},
            submission_payload={},
            storage_profile_id=resolved_storage_profile_id,
        )
        output = manager.execute_pre_gui_hooks(metadata)
        # Later sources (bundle) override earlier (env) for scalars; parameters merge.
        params = merged.pop("parameters", {})
        params.update(output.pop("parameters", {}))
        merged.update(output)
        if params:
            merged["parameters"] = params
    return merged


def qt_hook_confirmation(parent: Any) -> Callable[[List[HookManager]], bool]:
    """Return a ``confirm_callback`` for :func:`run_pre_gui_hooks` that shows the standard
    Qt confirmation dialog listing every hook that will run.

    This is the only Qt-coupled part of the pre-GUI flow. Qt is imported lazily inside the
    returned callable — not at module top — so importing this module (and calling
    ``run_pre_gui_hooks``) never requires Qt bindings. Returns a callable that returns
    ``True`` when the user accepts and ``False`` otherwise.
    """
    from qtpy.QtWidgets import QMessageBox  # pylint: disable=import-error

    def _confirm(sources: List[HookManager]) -> bool:
        confirmation_msg = (
            "".join(
                _generate_hooks_confirmation_message(
                    m.hooks, m._original_bundle_dir, m.source_label
                )
                for m in sources
                if m.hooks
            )
            + "Do you want to run these hooks?"
        )
        reply = QMessageBox.question(
            parent,
            tr("Job Submission Confirmation"),
            confirmation_msg,
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        return reply == QMessageBox.Yes

    return _confirm


def apply_pre_gui_output(
    pre_gui_output: dict[str, Any],
    initial_settings: Any,
    initial_shared_parameter_values: dict[str, Any],
    cli_provided_param_names: Optional[set[str]] = None,
) -> None:
    """Apply merged pre-GUI hook output onto a submitter's settings and its shared values.

    ``name`` / ``description`` overwrite the corresponding fields on ``initial_settings``.
    For ``parameters``: any name that matches a job-template parameter (an entry in
    ``initial_settings.parameters``) updates that entry in place; every other name lands in
    ``initial_shared_parameter_values`` (queue parameters, ``deadline:`` job properties, etc.).
    CLI-supplied parameter names (``cli_provided_param_names``) always win over hook values.

    This is generic across submitters:

    * The standalone job-bundle submitter passes a ``JobBundleSettings`` whose ``parameters``
      is a list of template-parameter dicts, so template params are routed onto it in place.
    * DCC submitters (Maya, Nuke, etc.) pass their own settings dataclass, which has no
      ``parameters`` list. With no template-parameter list, every hook parameter is treated
      as a shared value — matching how a DCC seeds ``initial_shared_parameter_values``.

    ``initial_settings`` only needs assignable ``name`` / ``description`` attributes and,
    optionally, a ``parameters`` list of ``{"name": ..., "value": ...}`` dicts.
    """
    if not pre_gui_output:
        return

    cli_provided_param_names = cli_provided_param_names or set()
    hook_params = pre_gui_output.get("parameters", {})
    # DCC settings have no template-parameter list; treat their absence as "no template
    # params", so every hook parameter flows to the shared values dict below.
    template_parameters = getattr(initial_settings, "parameters", None) or []
    template_param_names = {p["name"] for p in template_parameters}
    for param_name, param_value in hook_params.items():
        # CLI --parameter values take precedence over hook values.
        if param_name in cli_provided_param_names:
            continue
        if param_name in template_param_names:
            # Job template parameter — update initial_settings.parameters in-place
            for p in template_parameters:
                if p["name"] == param_name:
                    p["value"] = param_value
                    break
        else:
            # Shared job property (deadline: keys, queue parameters, etc.)
            initial_shared_parameter_values[param_name] = param_value
    if "name" in pre_gui_output:
        initial_settings.name = pre_gui_output["name"]
    if "description" in pre_gui_output:
        initial_settings.description = pre_gui_output["description"]
