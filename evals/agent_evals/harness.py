# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Run Claude Code headless against a goal and capture telemetry.

The tested agent runs as an ISOLATED subprocess (`claude -p` with stream-json
output) in a sandbox directory. The JSON event stream carries both per-tool-call
events and a final result event with token/cost telemetry, so no extra
instrumentation is needed.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class RunResult:
    """One headless agent run: outcome + telemetry, plus the raw event log."""

    success: bool  # did the CLI complete without error
    subtype: Optional[str]  # result subtype, e.g. "success" / "error_max_turns"
    tool_calls: list = field(default_factory=list)
    num_turns: int = 0
    total_cost_usd: float = 0.0
    duration_ms: int = 0
    final_text: str = ""  # the agent's final response text
    workdir: Optional[Path] = None
    raw_events: list = field(default_factory=list)

    @property
    def tool_call_count(self) -> int:
        return len(self.tool_calls)

    def telemetry_dict(self) -> dict:
        """Serializable telemetry (excludes the bulky raw event log)."""
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


def run_agent(
    prompt: str,
    workdir: Path,
    allowed_tools: list,
    *,
    max_turns: int = 20,
    model: Optional[str] = None,
    claude_bin: str = "claude",
) -> RunResult:
    """Run Claude Code headless in `workdir`, restricted to `allowed_tools`."""
    cmd = [
        claude_bin,
        "-p",
        prompt,
        "--output-format",
        "stream-json",
        "--verbose",  # required for stream-json event detail
        "--permission-mode",
        "bypassPermissions",
        "--max-turns",
        str(max_turns),
    ]
    if allowed_tools:
        cmd += ["--allowedTools", *allowed_tools]
    if model:
        cmd += ["--model", model]

    # stdin=DEVNULL: with -p the CLI still waits on stdin and can exit non-zero on a
    # closed pipe; DEVNULL makes the call cleanly non-interactive.
    proc = subprocess.run(
        cmd, cwd=str(workdir), capture_output=True, text=True, stdin=subprocess.DEVNULL
    )

    events = []
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue  # non-JSON lines (rare) are ignored

    return _parse_events(events, proc.returncode, workdir)


def _parse_events(events: list, returncode: int, workdir: Path) -> RunResult:
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
        # CLI died before emitting a result event.
        return RunResult(
            success=False,
            subtype="no_result_event",
            tool_calls=tool_calls,
            workdir=workdir,
            raw_events=events,
        )

    return RunResult(
        success=(not final.get("is_error", False)) and returncode == 0,
        subtype=final.get("subtype"),
        tool_calls=tool_calls,
        num_turns=final.get("num_turns", 0),
        total_cost_usd=final.get("total_cost_usd", 0.0),
        duration_ms=final.get("duration_ms", 0),
        final_text=final.get("result", "") if isinstance(final.get("result"), str) else "",
        workdir=workdir,
        raw_events=events,
    )
