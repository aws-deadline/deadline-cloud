# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Hook execution via subprocess."""

from __future__ import annotations

import logging as _logging
import os as _os
import shutil as _shutil
import subprocess as _subprocess
import time as _time
from typing import Callable as _Callable, Dict as _Dict, List as _List

from deadline.client.exceptions import DeadlineOperationError as _DeadlineOperationError

from ._models import HookDefinition as _HookDefinition
from ._models import HookMetadata as _HookMetadata
from ._models import HookResult as _HookResult

_logger = _logging.getLogger(__name__)


class HookExecutor:
    """Executes individual hook scripts as subprocesses."""

    def __init__(
        self,
        job_bundle_dir: str,
        print_callback: _Callable[[str], None],
    ):
        self.job_bundle_dir = job_bundle_dir
        self.print_callback = print_callback
        # Check for original bundle path (set by GUI when using job history bundle)
        self._script_resolve_dir = self._get_script_resolve_dir()

    def _get_script_resolve_dir(self) -> str:
        """Get the directory to use for resolving relative script paths."""
        hooks_origin_file = _os.path.join(self.job_bundle_dir, ".hooks_origin")
        if _os.path.isfile(hooks_origin_file):
            try:
                with open(hooks_origin_file) as f:
                    origin_dir = f.read().strip()
                if _os.path.isdir(origin_dir):
                    return origin_dir
            except Exception:
                # Fall through to default if .hooks_origin is unreadable or malformed
                pass
        return self.job_bundle_dir

    def execute(
        self,
        hook: _HookDefinition,
        metadata: _HookMetadata,
        hook_type: str,
        hook_index: int,
    ) -> _HookResult:
        """Execute a single hook."""
        command = self._resolve_command_path(hook.command)
        env = self._build_environment(hook, metadata)
        args = self._resolve_args(hook.args)

        _logger.debug(f"Executing {hook_type} hook {hook_index}: {command} {args}")

        start_time = _time.time()
        try:
            process = _subprocess.Popen(
                [command] + args,
                stdin=_subprocess.PIPE,
                stdout=_subprocess.PIPE,
                stderr=_subprocess.PIPE,
                env=env,
                text=True,
            )
            try:
                stdout, stderr = process.communicate(input=metadata.to_json(), timeout=hook.timeout)
                timed_out = False
            except _subprocess.TimeoutExpired:
                process.kill()
                stdout, stderr = process.communicate()
                timed_out = True

            execution_time = _time.time() - start_time
            return _HookResult(
                exit_code=process.returncode,
                stdout=stdout,
                stderr=stderr,
                execution_time=execution_time,
                timed_out=timed_out,
            )
        except FileNotFoundError as e:
            raise _DeadlineOperationError(f"Hook command not found: {hook.command}\n{e}")
        except PermissionError as e:
            raise _DeadlineOperationError(f"Permission denied executing hook: {hook.command}\n{e}")
        except Exception as e:
            raise _DeadlineOperationError(f"Failed to execute hook: {hook.command}\n{e}")

    def _build_environment(self, hook: _HookDefinition, metadata: _HookMetadata) -> _Dict[str, str]:
        """Build environment variables for hook execution."""
        env = _os.environ.copy()
        env.update(metadata.to_environment_variables())
        env.update({k: str(v) for k, v in hook.env.items()})
        return env

    def _resolve_command_path(self, command: str) -> str:
        """Resolve the command to an executable path."""
        if _os.path.isabs(command):
            if _os.path.isfile(command):
                return command
            raise _DeadlineOperationError(f"Hook command not found: {command}")

        # Try relative to script resolve dir (original bundle for GUI)
        relative_path = _os.path.join(self._script_resolve_dir, command)
        if _os.path.isfile(relative_path):
            return _os.path.abspath(relative_path)

        # Try PATH lookup
        resolved = _shutil.which(command)
        if resolved:
            return resolved

        raise _DeadlineOperationError(f"Hook command not found: {command}")

    def _resolve_args(self, args: _List[str]) -> _List[str]:
        """Resolve arguments, handling relative paths."""
        resolved = []
        for arg in args:
            if not _os.path.isabs(arg):
                relative_path = _os.path.join(self._script_resolve_dir, arg)
                if _os.path.exists(relative_path):
                    resolved.append(_os.path.abspath(relative_path))
                    continue
            resolved.append(arg)
        return resolved
