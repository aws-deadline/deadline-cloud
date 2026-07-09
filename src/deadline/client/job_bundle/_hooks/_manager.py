# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Hook manager for orchestrating hook execution."""

from __future__ import annotations

import json as _json
import logging as _logging
import os as _os
from typing import (
    Any as _Any,
    Callable as _Callable,
    Dict as _Dict,
    List as _List,
    Optional as _Optional,
)

from deadline.client.exceptions import DeadlineOperationError as _DeadlineOperationError
from deadline.client.job_bundle.loader import read_yaml_or_json_object as _read_yaml_or_json_object

from ._executor import HookExecutor as _HookExecutor
from ._merger import merge_payload as _merge_payload
from ._models import HookConfiguration as _HookConfiguration
from ._models import HookMetadata as _HookMetadata
from ._validator import validate_pre_gui_output as _validate_pre_gui_output
from ._validator import validate_configuration as _validate_configuration
from ._validator import validate_modified_payload as _validate_modified_payload

_logger = _logging.getLogger(__name__)


def _generate_hooks_confirmation_message(hooks: _HookConfiguration, bundle_dir: str) -> str:
    """Generate a confirmation message listing hooks that will execute."""
    lines = ["This job bundle contains submission hooks that will execute on your machine:\n"]

    if hooks.pre_gui:
        lines.append("  Pre-GUI hooks:")
        for i, hook in enumerate(hooks.pre_gui):
            cmd = f"{hook.command} {' '.join(hook.args)}".strip()
            lines.append(f"    [{i + 1}] {cmd}")
        lines.append("")

    if hooks.pre_submission:
        lines.append("  Pre-submission hooks:")
        for i, hook in enumerate(hooks.pre_submission):
            cmd = f"{hook.command} {' '.join(hook.args)}".strip()
            lines.append(f"    [{i + 1}] {cmd}")
        lines.append("")

    if hooks.post_submission:
        lines.append("  Post-submission hooks:")
        for i, hook in enumerate(hooks.post_submission):
            cmd = f"{hook.command} {' '.join(hook.args)}".strip()
            lines.append(f"    [{i + 1}] {cmd}")
        lines.append("")

    lines.append(f"  Bundle: {bundle_dir}\n")
    return "\n".join(lines)


def collect_pre_gui_hook_sources(
    bundle_dir: str,
    env_hooks_dir: _Optional[str],
    allow_bundle_hooks: bool,
    allow_environment_hooks: bool,
    print_callback: _Callable[[str], None],
    warning_callback: _Optional[_Callable[[str], None]] = None,
    hook_manager_cls: _Optional[type] = None,
) -> _List["HookManager"]:
    """Return the HookManagers whose preGUI hooks should run, in execution order.

    PreGUI hooks may come from the directory named by ``DEADLINE_HOOKS_DIR`` (gated by
    ``allow_environment_hooks``) and/or the job bundle (gated by ``allow_bundle_hooks``).
    Environment hooks run before bundle hooks. Sources without preGUI hooks are omitted.

    ``print_callback`` is attached to each returned HookManager for its execution-time
    messages. ``warning_callback`` (defaults to ``print_callback``) receives guidance
    notes — a hooks source that is present but disabled, or an invalid DEADLINE_HOOKS_DIR —
    so callers can log those at a higher severity than routine execution output.
    ``hook_manager_cls`` (defaults to ``HookManager``) is the class used to construct each
    source; callers may pass their own reference so it remains a patchable seam.

    This is deliberately Qt-free so it can be unit-tested without a GUI binding; the caller
    (the submitter) handles the confirmation prompt and execution.
    """
    warn = warning_callback or print_callback
    manager_cls = hook_manager_cls or HookManager
    sources: _List["HookManager"] = []

    # If DEADLINE_HOOKS_DIR resolves to the job bundle directory, the two sources are the
    # same hooks.yaml. Treat it as the bundle source only (skip the env source) so hooks are
    # not loaded and run twice — matching the pre/post-submission path's dedup.
    env_is_bundle = (
        bool(env_hooks_dir)
        and bool(bundle_dir)
        and (_os.path.realpath(env_hooks_dir or "") == _os.path.realpath(bundle_dir or ""))
    )

    # Environment hooks first.
    if env_hooks_dir and not env_is_bundle:
        if not _os.path.isdir(env_hooks_dir):
            warn(f"Warning: DEADLINE_HOOKS_DIR '{env_hooks_dir}' is not a valid directory")
        else:
            env_manager = manager_cls(env_hooks_dir, print_callback)
            env_hooks = env_manager.load_hooks()
            if env_hooks and env_hooks.pre_gui:
                if allow_environment_hooks:
                    sources.append(env_manager)
                else:
                    warn(
                        "Note: DEADLINE_HOOKS_DIR contains preGUI hooks but environment "
                        "hooks are disabled.\n"
                        "Enable with: deadline config set settings.allow_environment_hooks true"
                    )

    # Bundle hooks second. When env_is_bundle, this single source covers both; it is gated
    # by allow_bundle_hooks OR allow_environment_hooks (either grant permits the shared dir).
    #
    # Only consult the bundle source when there IS a bundle. DCC submitters have no on-disk
    # bundle at pre-GUI time and pass bundle_dir="" — an empty dir would make HookManager
    # resolve hooks.yaml/.json relative to the process CWD (os.path.join("", "hooks") →
    # "hooks"), so a stray hooks file in the launch directory could be loaded and, with
    # bundle hooks enabled studio-wide, executed for a submission that has no bundle. Skip
    # the bundle source entirely when bundle_dir is falsy to avoid that CWD footgun.
    if bundle_dir:
        bundle_manager = manager_cls(bundle_dir, print_callback)
        bundle_hooks = bundle_manager.load_hooks()
        if bundle_hooks and bundle_hooks.pre_gui:
            if allow_bundle_hooks or (env_is_bundle and allow_environment_hooks):
                sources.append(bundle_manager)
            else:
                warn(
                    "Note: Job bundle contains preGUI hooks but bundle hooks are disabled.\n"
                    "Enable with: deadline config set settings.allow_bundle_hooks true"
                )

    return sources


class HookManager:
    """Manages loading and execution of submission hooks."""

    def __init__(
        self,
        job_bundle_dir: str,
        print_callback: _Callable[[str], None],
    ):
        self.job_bundle_dir = job_bundle_dir
        self.print_callback = print_callback
        self.hooks: _Optional[_HookConfiguration] = None
        self._executor = _HookExecutor(job_bundle_dir, print_callback)
        # Use original bundle path for metadata if available (GUI submit case)
        self._original_bundle_dir = self._executor._script_resolve_dir

    def load_hooks(self) -> _Optional[_HookConfiguration]:
        """Load hook configuration from hooks.yaml or hooks.json."""
        config_data = _read_yaml_or_json_object(self.job_bundle_dir, "hooks", required=False)
        if config_data is None:
            return None

        _validate_configuration(config_data)
        self.hooks = _HookConfiguration.from_dict(config_data)
        return self.hooks

    def execute_pre_gui_hooks(
        self,
        metadata: _HookMetadata,
    ) -> _Dict[str, _Any]:
        """Execute all pre-GUI hooks and return merged initial parameter overrides.

        Runs before the submission dialog opens. Hooks may output JSON to pre-populate
        dialog fields: parameters, priority, farmId, queueId, name, description.
        Failures block the dialog from opening.

        The caller sets ``metadata.job_bundle_dir`` to the job bundle being submitted; it is
        respected as-is so environment hooks receive the real bundle dir (not the
        environment hooks directory), matching the pre/post-submission contract. Relative
        hook *script* paths are still resolved against this manager's own directory.
        """
        if not self.hooks or not self.hooks.pre_gui:
            return {}

        merged: _Dict[str, _Any] = {}
        for i, hook in enumerate(self.hooks.pre_gui):
            hook_name = f"{hook.command} {' '.join(hook.args)}".strip()
            self.print_callback(f"Running pre-GUI hook [{i + 1}]: {hook_name}")

            result = self._executor.execute(hook, metadata, "pre-GUI", i + 1)

            if result.timed_out:
                self._report_failure(hook, result, i + 1, "pre-GUI")
                raise _DeadlineOperationError(
                    f"Pre-GUI hook [{i + 1}] timed out after {hook.timeout}s: {hook_name}"
                )

            if not result.is_success():
                self._report_failure(hook, result, i + 1, "pre-GUI")
                raise _DeadlineOperationError(
                    f"Pre-GUI hook [{i + 1}] failed with exit code {result.exit_code}: {hook_name}"
                )

            if result.stdout.strip():
                try:
                    output = _json.loads(result.stdout)
                    _validate_pre_gui_output(output, hook_name)
                    # Later hooks override earlier ones for scalar fields; parameters are merged.
                    params = merged.pop("parameters", {})
                    params.update(output.pop("parameters", {}))
                    merged.update(output)
                    if params:
                        merged["parameters"] = params
                    _logger.debug(f"Pre-GUI hook [{i + 1}] modified initial settings")
                except _json.JSONDecodeError as e:
                    raise _DeadlineOperationError(
                        f"Pre-GUI hook [{i + 1}] produced invalid JSON: {e}"
                    )

            # stderr was already streamed to the user line-by-line during execution
            # (see HookExecutor); keep a full-blob copy only in the debug log.
            if result.stderr:
                _logger.debug(f"Pre-GUI hook [{i + 1}] stderr: {result.stderr}")

            _logger.debug(f"Pre-GUI hook [{i + 1}] completed in {result.execution_time:.2f}s")

        return merged

    def execute_pre_submission_hooks(
        self,
        metadata: _HookMetadata,
        payload: _Dict[str, _Any],
    ) -> _Dict[str, _Any]:
        """Execute all pre-submission hooks in sequence."""
        if not self.hooks or not self.hooks.pre_submission:
            return payload

        current_payload = payload
        for i, hook in enumerate(self.hooks.pre_submission):
            hook_name = f"{hook.command} {' '.join(hook.args)}".strip()
            self.print_callback(f"Running pre-submission hook [{i + 1}]: {hook_name}")

            # Update metadata with current payload
            metadata.submission_payload = current_payload
            result = self._executor.execute(hook, metadata, "pre-submission", i + 1)

            if result.timed_out:
                self._report_failure(hook, result, i + 1, "pre-submission")
                raise _DeadlineOperationError(
                    f"Pre-submission hook [{i + 1}] timed out after {hook.timeout}s: {hook_name}"
                )

            if not result.is_success():
                self._report_failure(hook, result, i + 1, "pre-submission")
                raise _DeadlineOperationError(
                    f"Pre-submission hook [{i + 1}] failed with exit code {result.exit_code}: {hook_name}"
                )

            # Process output
            if result.stdout.strip():
                try:
                    modified = _json.loads(result.stdout)
                    _validate_modified_payload(modified, hook_name)
                    current_payload = _merge_payload(current_payload, modified)
                    _logger.debug(f"Hook [{i + 1}] modified payload")
                except _json.JSONDecodeError as e:
                    raise _DeadlineOperationError(
                        f"Pre-submission hook [{i + 1}] produced invalid JSON: {e}"
                    )

            # stderr was already streamed to the user line-by-line during execution
            # (see HookExecutor); keep a full-blob copy only in the debug log.
            if result.stderr:
                _logger.debug(f"Hook [{i + 1}] stderr: {result.stderr}")

            _logger.debug(f"Hook [{i + 1}] completed in {result.execution_time:.2f}s")

        return current_payload

    def execute_post_submission_hooks(self, metadata: _HookMetadata) -> None:
        """Execute all post-submission hooks in sequence. Failures only log warnings."""
        if not self.hooks or not self.hooks.post_submission:
            return

        for i, hook in enumerate(self.hooks.post_submission):
            hook_name = f"{hook.command} {' '.join(hook.args)}".strip()
            self.print_callback(f"Running post-submission hook [{i + 1}]: {hook_name}")

            try:
                result = self._executor.execute(hook, metadata, "post-submission", i + 1)

                if result.timed_out:
                    _logger.warning(
                        f"Post-submission hook [{i + 1}] timed out after {hook.timeout}s: {hook_name}"
                    )
                    continue

                if not result.is_success():
                    _logger.warning(
                        f"Post-submission hook [{i + 1}] failed with exit code {result.exit_code}: {hook_name}"
                    )
                    # stderr was already streamed live during execution (see HookExecutor).
                    if result.stderr:
                        _logger.debug(f"stderr: {result.stderr}")
                    continue

                if result.stdout:
                    _logger.info(f"Hook [{i + 1}] output: {result.stdout}")

                _logger.debug(f"Hook [{i + 1}] completed in {result.execution_time:.2f}s")

            except _DeadlineOperationError as e:
                _logger.warning(f"Post-submission hook [{i + 1}] error: {e}")

    def _report_failure(self, hook, result, index: int, hook_type: str) -> None:
        """Report hook failure details.

        The hook's stderr was already streamed to the user line-by-line while it ran (see
        HookExecutor), so it is not repeated here. stdout is reserved for the hook's JSON
        contract and is not streamed, so it is surfaced here to aid debugging a failure.
        """
        hook_name = f"{hook.command} {' '.join(hook.args)}".strip()
        self.print_callback(f"\n{hook_type.title()} hook [{index}] failed: {hook_name}")
        self.print_callback(f"Exit code: {result.exit_code}")
        if result.timed_out:
            self.print_callback(f"Timed out after {hook.timeout}s")
        if result.stdout:
            self.print_callback(f"stdout:\n{result.stdout}")
