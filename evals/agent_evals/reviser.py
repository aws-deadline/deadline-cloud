# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Propose improvements to a subject from a run where the agent struggled.

Given a struggling run's transcript and a Subject (the repo source, repo docs, or
a seeded corpus), drive an isolated agent to edit the subject so the NEXT agent
succeeds more reliably, and commit the edits to a scratch git ref. Re-running the
eval with --revised-ref <scratch ref> then A/B-proves whether the edit helped, and
the diff is the PR-ready proposal.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from .subject import Subject


# Bounds for the reviser agent. It drives a full edit session, so it needs both a
# turn ceiling and a wall-clock ceiling -- otherwise a runaway or hung session has
# no limit at all.
DEFAULT_MAX_TURNS = 40
DEFAULT_TIMEOUT_S = 1200


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


def transcript_text(events_path: Path, max_parts: int = 60) -> str:
    """A compact transcript of assistant text + tool calls, for the revise prompt."""
    parts = []
    for line in events_path.read_text().splitlines():
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
    return "\n".join(parts[:max_parts])


def revise(
    subj: Subject,
    run_dir: Path,
    *,
    goal: str,
    base_ref: str,
    claude_bin: str = "claude",
    max_turns: int = DEFAULT_MAX_TURNS,
    timeout_s: int = DEFAULT_TIMEOUT_S,
) -> str:
    """Edit the subject from a struggling run's transcript; commit to a scratch ref.

    Returns the scratch ref name. Raises ReviseError if the agent can't be launched,
    exceeds `timeout_s`, or made no edits -- a candidate identical to baseline would
    make any measured delta pure noise.
    """
    prompt = _REVISE_PROMPT.format(goal=goal, transcript=transcript_text(run_dir / "events.jsonl"))

    subj.checkout(base_ref)  # edit from a clean baseline
    try:
        proc = subprocess.run(
            [
                claude_bin,
                "-p",
                prompt,
                "--permission-mode",
                "bypassPermissions",
                "--max-turns",
                str(max_turns),
                "--allowedTools",
                "Read",
                "Edit",
                "Write",
                "Grep",
                "Glob",
            ],
            cwd=str(subj.root),
            capture_output=True,
            text=True,
            stdin=subprocess.DEVNULL,
            timeout=timeout_s,
        )
    except OSError as e:
        raise ReviseError(f"could not launch {claude_bin}: {e}") from e
    except subprocess.TimeoutExpired as e:
        raise ReviseError(f"reviser agent exceeded {timeout_s}s wall-clock timeout") from e
    if proc.returncode != 0:
        raise ReviseError(f"reviser agent exited {proc.returncode}: {proc.stderr.strip()[:200]}")

    if not subj.capture_diff().strip():
        raise ReviseError("reviser agent made no edits to the subject.")

    ref = f"eval-revise-{run_dir.parent.parent.name}"
    return subj.commit_scratch(ref, "agent-evals: proposed improvement from eval transcript")
