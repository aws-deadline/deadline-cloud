# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Harness command building, launch errors, and event parsing (no real agent calls)."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from agent_evals import harness
from agent_evals.harness import HarnessError, claude_cmd, run_agent


def _flag(cmd: list, name: str) -> str:
    return cmd[cmd.index(name) + 1]


def test_run_agent_raises_harness_error_on_missing_binary(tmp_path: Path) -> None:
    with pytest.raises(HarnessError, match="could not launch"):
        run_agent("hello", tmp_path, ["Bash"], claude_bin="definitely-not-a-real-binary-xyz")


@pytest.mark.parametrize(
    "tools, expected", [(["Read"], "Read"), (["Bash", "Read"], "Bash,Read"), ([], "")]
)
def test_claude_cmd_limits_tools(tools, expected) -> None:
    cmd = claude_cmd("p", tools=tools, max_turns=3)
    assert _flag(cmd, "--tools") == expected
    assert "--allowedTools" not in cmd  # pre-approval only; doesn't limit anything
    assert "--strict-mcp-config" in cmd


def test_claude_cmd_verbose_only_for_stream_json() -> None:
    assert "--verbose" in claude_cmd("p", tools=[], max_turns=1)
    assert "--verbose" not in claude_cmd("p", tools=[], max_turns=1, output_format="json")


def test_run_claude_decodes_utf8(monkeypatch, tmp_path: Path) -> None:
    seen = {}

    def _run(cmd, **kwargs):
        seen.update(kwargs)
        return subprocess.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr(harness.subprocess, "run", _run)
    harness.run_claude(["claude"], tmp_path, 1)
    assert seen["encoding"] == "utf-8" and seen["errors"] == "replace"


def test_run_claude_timeout_raises_harness_error(monkeypatch, tmp_path: Path) -> None:
    def _run(cmd, **kwargs):
        raise subprocess.TimeoutExpired(cmd, kwargs["timeout"])

    monkeypatch.setattr(harness.subprocess, "run", _run)
    with pytest.raises(HarnessError, match="timeout"):
        harness.run_claude(["claude"], tmp_path, 1)


_RESULT = {"type": "result", "subtype": "success", "num_turns": 2, "result": "done"}
_TOOL_USE = {"type": "assistant", "message": {"content": [{"type": "tool_use", "name": "Bash"}]}}


def test_parse_events_success() -> None:
    r = harness._parse_events([_TOOL_USE, _RESULT], 0)
    assert r.success and r.final_text == "done" and r.tool_calls == ["Bash"] and r.num_turns == 2


@pytest.mark.parametrize(
    "events, returncode",
    [([{**_RESULT, "is_error": True}], 0), ([_RESULT], 1), ([_TOOL_USE], 0)],
    ids=["is_error", "nonzero_exit", "no_result_event"],
)
def test_parse_events_failure(events, returncode) -> None:
    assert not harness._parse_events(events, returncode).success
