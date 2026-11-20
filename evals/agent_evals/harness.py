# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Run Claude Code headless and capture telemetry from its stream-json events.

The working directory is not a sandbox: the agent runs with bypassPermissions as
the operator, with their filesystem and AWS credentials. Treat eval files and
materials as trusted input.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# --max-turns doesn't bound a hung network or auth call.
DEFAULT_TIMEOUT_S = 1200


class HarnessError(RuntimeError):
    """Raised when the claude subprocess cannot be launched or times out."""


@dataclass
class RunResult:
    """One headless agent run: outcome, telemetry, and the raw event log."""

    success: bool
    subtype: Optional[str]  # e.g. "success", "error_max_turns"
    tool_calls: list = field(default_factory=list)
    num_turns: int = 0
    total_cost_usd: float = 0.0
    duration_ms: int = 0
    final_text: str = ""
    raw_events: list = field(default_factory=list)

    @property
    def tool_call_count(self) -> int:
        return len(self.tool_calls)

    def telemetry_dict(self) -> dict:
        """Serializable telemetry, without the raw event log."""
        return {
            "success": self.success,
            "subtype": self.subtype,
            "tool_calls": self.tool_calls,
            "tool_call_count": self.tool_call_count,
            "num_turns": self.num_turns,
            "total_cost_usd": self.total_cost_usd,
            "duration_ms": self.duration_ms,
            "final_text": self.final_text,
        }


def claude_cmd(
    prompt: str,
    *,
    tools: list,
    max_turns: int,
    output_format: str = "stream-json",
    model: Optional[str] = None,
    claude_bin: str = "claude",
) -> list:
    """A headless `claude -p` command limited to exactly `tools` (none if empty).

    `--tools` limits the tool set; `--allowedTools` would only pre-approve tools,
    which bypassPermissions already does. No MCP servers, so the operator's config
    can't add tools.
    """
    cmd = [
        claude_bin,
        "-p",
        prompt,
        "--output-format",
        output_format,
        "--permission-mode",
        "bypassPermissions",
        "--max-turns",
        str(max_turns),
        "--tools",
        ",".join(tools),
        "--strict-mcp-config",
    ]
    if output_format == "stream-json":
        cmd.append("--verbose")  # required for per-event output
    if model:
        cmd += ["--model", model]
    return cmd


def run_claude(
    cmd: list, cwd: Path, timeout_s: int, env: Optional[dict] = None
) -> subprocess.CompletedProcess:
    """Run a claude command, raising HarnessError on launch failure or timeout."""
    try:
        return subprocess.run(
            cmd,
            cwd=str(cwd),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            # With -p the CLI still reads stdin and can fail on a closed pipe.
            stdin=subprocess.DEVNULL,
            timeout=timeout_s,
            env=env,
        )
    except OSError as e:
        raise HarnessError(f"could not launch {cmd[0]}: {e}") from e
    except subprocess.TimeoutExpired as e:
        raise HarnessError(f"{cmd[0]} exceeded {timeout_s}s wall-clock timeout") from e


def run_agent(
    prompt: str,
    workdir: Path,
    tools: list,
    *,
    max_turns: int = 20,
    model: Optional[str] = None,
    claude_bin: str = "claude",
    timeout_s: int = DEFAULT_TIMEOUT_S,
    env: Optional[dict] = None,
) -> RunResult:
    """Run Claude Code headless in `workdir` with only `tools` available."""
    cmd = claude_cmd(prompt, tools=tools, max_turns=max_turns, model=model, claude_bin=claude_bin)
    proc = run_claude(cmd, workdir, timeout_s, env)
    events = []
    for line in proc.stdout.splitlines():
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return _parse_events(events, proc.returncode)


def _parse_events(events: list, returncode: int) -> RunResult:
    tool_calls = []
    final = None
    for ev in events:
        if ev.get("type") == "assistant":
            for blk in ev.get("message", {}).get("content", []):
                if blk.get("type") == "tool_use":
                    tool_calls.append(blk["name"])
        elif ev.get("type") == "result":
            final = ev

    if final is None:
        return RunResult(
            success=False, subtype="no_result_event", tool_calls=tool_calls, raw_events=events
        )

    result = final.get("result")
    return RunResult(
        success=not final.get("is_error", False) and returncode == 0,
        subtype=final.get("subtype"),
        tool_calls=tool_calls,
        num_turns=final.get("num_turns", 0),
        total_cost_usd=final.get("total_cost_usd", 0.0),
        duration_ms=final.get("duration_ms", 0),
        final_text=result if isinstance(result, str) else "",
        raw_events=events,
    )
