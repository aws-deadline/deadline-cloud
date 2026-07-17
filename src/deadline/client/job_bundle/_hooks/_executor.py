# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Hook execution via subprocess."""

from __future__ import annotations

import logging as _logging
import os as _os
import shutil as _shutil
import subprocess as _subprocess
import threading as _threading
import time as _time
from typing import Callable as _Callable, Dict as _Dict, List as _List, Tuple as _Tuple

from deadline.client.exceptions import DeadlineOperationError as _DeadlineOperationError

from ._models import HookDefinition as _HookDefinition
from ._models import HookMetadata as _HookMetadata
from ._models import HookResult as _HookResult

_logger = _logging.getLogger(__name__)


class HookExecutor:
    """Executes individual hook scripts as subprocesses."""

    # How long to wait for the stdout/stderr reader threads to drain after the hook process
    # exits (or is killed). A lingering child that inherited the pipe fds can hold the write
    # end open past the process's own exit, so this bounds that wait rather than joining the
    # readers forever.
    _READER_JOIN_GRACE_SECONDS = 5.0

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
        except FileNotFoundError as e:
            raise _DeadlineOperationError(f"Hook command not found: {hook.command}\n{e}")
        except PermissionError as e:
            raise _DeadlineOperationError(f"Permission denied executing hook: {hook.command}\n{e}")
        except Exception as e:
            raise _DeadlineOperationError(f"Failed to execute hook: {hook.command}\n{e}")

        stdout, stderr, timed_out = self._communicate_streaming(
            process, metadata.to_json(), hook.timeout, hook_type, hook_index
        )

        execution_time = _time.time() - start_time
        return _HookResult(
            exit_code=process.returncode,
            stdout=stdout,
            stderr=stderr,
            execution_time=execution_time,
            timed_out=timed_out,
        )

    def _communicate_streaming(
        self,
        process: "_subprocess.Popen",
        stdin_data: str,
        timeout: int,
        hook_type: str,
        hook_index: int,
    ) -> _Tuple[str, str, bool]:
        """Feed ``stdin_data`` to the hook and collect its output, streaming stderr live.

        stdout is reserved for the hook's JSON contract, so it is captured whole and
        returned. stderr is where hooks are expected to write human-readable progress, so
        each line is forwarded to ``print_callback`` as it arrives — giving the user live
        feedback while a slow hook (for example, generating auth tokens for several
        services) runs, instead of nothing until it finishes. stderr is also accumulated and
        returned so failure reporting keeps the full text.

        Reading each pipe on its own thread avoids the deadlock that a single-threaded
        write-then-read would hit when a hook fills one pipe's buffer before we drain it.
        Returns ``(stdout, stderr, timed_out)``.

        The shared output buffers and the ``print_callback`` are guarded by a lock, and an
        ``abandoned`` flag lets a reader we stop waiting on (the lingering-child timeout
        path) bow out cleanly: without this, a daemon reader could still be ``append``-ing to
        a buffer while the main thread joins it via ``"".join(...)`` — a data race that can
        corrupt output or crash the interpreter — and could keep calling ``print_callback``
        after this method returns, racing the next hook's output.
        """
        stdout_chunks: _List[str] = []
        stderr_chunks: _List[str] = []
        output_lock = _threading.Lock()
        # Set once we give up waiting on the readers; tells any leaked reader thread to stop
        # touching the shared buffers and the callback.
        abandoned = _threading.Event()

        def _write_stdin() -> None:
            if process.stdin is None:
                return
            try:
                process.stdin.write(stdin_data)
                process.stdin.close()
            except (BrokenPipeError, OSError):
                # The hook may exit (or be killed on timeout) before reading all of stdin.
                pass

        def _drain_stdout() -> None:
            if process.stdout is None:
                return
            data = process.stdout.read()
            with output_lock:
                if not abandoned.is_set():
                    stdout_chunks.append(data)

        def _drain_stderr() -> None:
            if process.stderr is None:
                return
            # readline (rather than iterating the file) yields each line without read-ahead
            # buffering, so progress lines reach the user as soon as the hook emits them.
            for line in iter(process.stderr.readline, ""):
                with output_lock:
                    if abandoned.is_set():
                        # We've stopped waiting on this hook; don't append or print past the
                        # point execute() returned, to avoid racing the next hook's output.
                        break
                    stderr_chunks.append(line)
                    message = line.rstrip("\n")
                    self.print_callback(f"  [{hook_type} hook {hook_index}] {message}")

        threads = [
            _threading.Thread(target=target, daemon=True)
            for target in (_write_stdin, _drain_stdout, _drain_stderr)
        ]
        for thread in threads:
            thread.start()

        timed_out = False
        try:
            process.wait(timeout=timeout)
        except _subprocess.TimeoutExpired:
            timed_out = True
            process.kill()
            process.wait()

        # Killing the process closes its ends of the pipes, so the reader threads normally
        # reach EOF and finish promptly. Join with a bound rather than unconditionally,
        # though: if the hook left a child that inherited the pipe fds and still holds the
        # write end open, the reads would never see EOF and an unbounded join() would hang
        # submission forever — defeating the timeout the old communicate(timeout=...)
        # enforced. Treat readers still blocked after the grace period as a timeout; they are
        # daemon threads, so any leaked output is abandoned rather than blocking exit.
        deadline = _time.monotonic() + self._READER_JOIN_GRACE_SECONDS
        for thread in threads:
            thread.join(timeout=max(0.0, deadline - _time.monotonic()))
        if any(thread.is_alive() for thread in threads):
            timed_out = True
            if process.poll() is None:
                process.kill()

        # Snapshot under the lock: set ``abandoned`` first so any still-alive reader stops
        # mutating the buffers, then read a consistent copy. Joined readers have already
        # exited, so in the normal path this is uncontended.
        with output_lock:
            abandoned.set()
            return "".join(stdout_chunks), "".join(stderr_chunks), timed_out

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
