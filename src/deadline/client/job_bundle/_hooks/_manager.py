# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Hook manager for orchestrating hook execution."""

from __future__ import annotations

import json as _json
import logging as _logging
from typing import Any as _Any, Callable as _Callable, Dict as _Dict, Optional as _Optional

from deadline.client.exceptions import DeadlineOperationError as _DeadlineOperationError
from deadline.client.job_bundle.loader import read_yaml_or_json_object as _read_yaml_or_json_object

from ._executor import HookExecutor as _HookExecutor
from ._merger import merge_payload as _merge_payload
from ._models import HookConfiguration as _HookConfiguration
from ._models import HookMetadata as _HookMetadata
from ._validator import validate_configuration as _validate_configuration
from ._validator import validate_modified_payload as _validate_modified_payload

_logger = _logging.getLogger(__name__)


def _generate_hooks_confirmation_message(hooks: _HookConfiguration, bundle_dir: str) -> str:
    """Generate a confirmation message listing hooks that will execute."""
    lines = ["This job bundle contains submission hooks that will execute on your machine:\n"]

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

    def execute_pre_submission_hooks(
        self,
        metadata: _HookMetadata,
        payload: _Dict[str, _Any],
    ) -> _Dict[str, _Any]:
        """Execute all pre-submission hooks in sequence."""
        if not self.hooks or not self.hooks.pre_submission:
            return payload

        # Use original bundle dir for hooks to find files
        metadata.job_bundle_dir = self._original_bundle_dir

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

            if result.stderr:
                _logger.warning(f"Hook [{i + 1}] stderr: {result.stderr}")

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
                    if result.stderr:
                        _logger.warning(f"stderr: {result.stderr}")
                    continue

                if result.stdout:
                    _logger.info(f"Hook [{i + 1}] output: {result.stdout}")

                _logger.debug(f"Hook [{i + 1}] completed in {result.execution_time:.2f}s")

            except _DeadlineOperationError as e:
                _logger.warning(f"Post-submission hook [{i + 1}] error: {e}")

    def _report_failure(self, hook, result, index: int, hook_type: str) -> None:
        """Report hook failure details."""
        hook_name = f"{hook.command} {' '.join(hook.args)}".strip()
        self.print_callback(f"\n{hook_type.title()} hook [{index}] failed: {hook_name}")
        self.print_callback(f"Exit code: {result.exit_code}")
        if result.timed_out:
            self.print_callback(f"Timed out after {hook.timeout}s")
        if result.stdout:
            self.print_callback(f"stdout:\n{result.stdout}")
        if result.stderr:
            self.print_callback(f"stderr:\n{result.stderr}")
