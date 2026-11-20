# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Propose an improvement to a subject from a run where the agent struggled.

An agent edits the subject from the run's transcript and the edit is committed to
a scratch ref; `runner run --revised-ref <ref>` then measures whether it helped.
"""

from __future__ import annotations

import json
import secrets
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .harness import HarnessError, run_agent
from .subject import Subject, SubjectError

DEFAULT_MAX_TURNS = 40
DEFAULT_TIMEOUT_S = 1200
_TRANSCRIPT_MAX_PARTS = 60
_REVISER_TOOLS = ["Read", "Edit", "Write", "Grep", "Glob"]


class ReviseError(RuntimeError):
    """Raised when the reviser cannot run or produces no change to the subject."""


_REVISE_PROMPT = """\
You are improving the material an AI agent relies on (source code, documentation,
or a guide) so the agent can achieve a goal more reliably. The material is in this
directory. Below is the goal and a transcript of an agent that used the CURRENT
material and struggled (took too many steps, went down wrong paths, or failed).

Edit the material to fix what tripped the agent up:
- add a missing prerequisite, command, or step the agent had to guess at,
- clarify anything ambiguous the agent misread,
- if you change code, it MUST fully work end to end -- documenting behavior that
  isn't implemented is worse than no change.

Make additive, minimal edits; do not remove existing behavior or guidance. When
done, STOP -- do not run tests or git.

=== THE GOAL THE AGENT WAS GIVEN ===
{goal}

=== AGENT TRANSCRIPT (struggled) ===
{transcript}
"""


def _transcript_text(events_path: Path) -> str:
    """Assistant text and tool calls from a run's events, compacted for the prompt."""
    parts = []
    for line in events_path.read_text(encoding="utf-8", errors="replace").splitlines():
        try:
            ev = json.loads(line)
        except json.JSONDecodeError:
            continue
        if ev.get("type") != "assistant":
            continue
        for blk in ev.get("message", {}).get("content", []):
            if blk.get("type") == "text":
                parts.append("ASSISTANT: " + blk["text"][:500])
            elif blk.get("type") == "tool_use":
                inp = blk.get("input", {})
                detail = inp.get("command") or inp.get("file_path") or json.dumps(inp)[:200]
                parts.append(f"TOOL[{blk.get('name')}]: {str(detail)[:300]}")
    return "\n".join(parts[:_TRANSCRIPT_MAX_PARTS])


def _new_scratch_ref() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"eval-revise-{stamp}-{secrets.token_hex(3)}"


def revise(
    subj: Subject,
    run_dir: Path,
    *,
    goal: str,
    base_ref: str,
    ref: Optional[str] = None,
    claude_bin: str = "claude",
    max_turns: int = DEFAULT_MAX_TURNS,
    timeout_s: int = DEFAULT_TIMEOUT_S,
) -> str:
    """Edit the subject from `run_dir`'s transcript and commit to scratch branch `ref`
    (default: a new unique name). Returns the ref.

    Raises ReviseError if `ref` is unusable, the agent fails, or it makes no edits.
    """
    ref = ref or _new_scratch_ref()
    # Fail before the paid run, not after it.
    if subj._git("check-ref-format", "--branch", ref).returncode != 0:
        raise ReviseError(f"invalid branch name: {ref!r}")
    if subj._git("rev-parse", "--verify", "--quiet", f"refs/heads/{ref}").returncode == 0:
        raise ReviseError(f"branch {ref!r} already exists")

    prompt = _REVISE_PROMPT.format(goal=goal, transcript=_transcript_text(run_dir / "events.jsonl"))
    try:
        with subj.scratch_worktree(base_ref) as scratch:
            try:
                result = run_agent(
                    prompt,
                    scratch.root,
                    _REVISER_TOOLS,
                    max_turns=max_turns,
                    claude_bin=claude_bin,
                    timeout_s=timeout_s,
                )
            except HarnessError as e:
                raise ReviseError(f"reviser agent: {e}") from e
            if not result.success:
                raise ReviseError(f"reviser agent failed: {result.subtype}")
            if not scratch.capture_diff().strip():
                raise ReviseError("reviser agent made no edits to the subject.")
            return scratch.commit_scratch(
                ref, "agent-evals: proposed improvement from eval transcript"
            )
    except SubjectError as e:
        raise ReviseError(str(e)) from e
