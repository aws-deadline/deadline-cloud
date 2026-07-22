# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Grade an agent's answer against a natural-language rubric (LLM-as-judge).

The judge is a SCORER, not an agent: a headless, tool-less, single-turn model call.
It sees only the rubric, the task prompt, and the agent's final answer, so its
verdict is reproducible from the run artifact alone. The rubric being a plain
string is what makes new eval domains code-free: describing what "success" means
is the only per-eval authoring step.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from typing import Optional


class JudgeError(RuntimeError):
    """Raised when the judge call fails to produce a usable verdict."""


@dataclass
class Verdict:
    passed: bool
    reasoning: str


_JUDGE_PROMPT = """\
You are grading an AI agent's answer to a task. Decide ONLY whether the answer
satisfies the rubric below. Judge the substance of the answer, not its wording or
format. A correct conclusion stated in unexpected phrasing still passes; a
confident answer that is wrong or unsupported fails. If the rubric asks the agent
to reach a conclusion the available evidence does not support, then correctly
declining to invent one SATISFIES the rubric.

You have NO tools. Do not attempt to read any file or run any command -- files the
answer mentions are not available to you. Grade purely from the text below.

=== RUBRIC (what a passing answer must do) ===
{rubric}

=== THE TASK THE AGENT WAS GIVEN ===
{prompt}

=== THE AGENT'S FINAL ANSWER ===
{answer}

Respond with ONLY a JSON object on a single line, no prose, no code fence:
{{"passed": true or false, "reasoning": "one or two sentences citing the rubric"}}
"""


def judge_answer(
    rubric: str,
    prompt: str,
    answer: str,
    *,
    model: Optional[str] = None,
    claude_bin: str = "claude",
) -> Verdict:
    """Grade `answer` against `rubric` with a headless, tool-less model call."""
    if not (answer or "").strip():
        return Verdict(passed=False, reasoning="agent produced no final answer")

    full_prompt = _JUDGE_PROMPT.format(rubric=rubric, prompt=prompt or "(none)", answer=answer)
    cmd = [
        claude_bin,
        "-p",
        full_prompt,
        "--output-format",
        "json",
        "--permission-mode",
        "bypassPermissions",
        # The judge must answer from the given text alone, so all tools are denied.
        # A denied tool attempt still consumes a turn, so leave headroom for the
        # model to recover and answer instead of dying on error_max_turns.
        "--max-turns",
        "5",
        "--disallowedTools",
        "Bash",
        "Read",
        "Write",
        "Edit",
        "Grep",
        "Glob",
        "WebFetch",
        "WebSearch",
        "Agent",
        "TodoWrite",
        "NotebookEdit",
    ]
    if model:
        cmd += ["--model", model]

    try:
        # stdin=DEVNULL: with -p the CLI still waits on stdin and can exit non-zero
        # on a closed pipe.
        proc = subprocess.run(cmd, capture_output=True, text=True, stdin=subprocess.DEVNULL)
    except OSError as e:
        raise JudgeError(f"could not launch {claude_bin}: {e}") from e

    # --output-format json wraps the reply in an envelope whose `result` field is
    # the text we asked for.
    reply = proc.stdout
    try:
        envelope = json.loads(proc.stdout)
        if isinstance(envelope, dict) and isinstance(envelope.get("result"), str):
            reply = envelope["result"]
    except json.JSONDecodeError:
        # stdout wasn't the JSON envelope (e.g. plain-text or truncated output);
        # fall through and try to grade the raw stdout instead.
        pass

    # Parse the verdict from stdout FIRST: the CLI sometimes exits non-zero after
    # emitting a valid result (e.g. a model-availability warning). Only surface the
    # exit code when stdout carried nothing gradable.
    try:
        return _extract_verdict(reply)
    except JudgeError:
        if proc.returncode != 0:
            raise JudgeError(
                f"judge exited {proc.returncode} with no usable verdict: "
                f"{(proc.stderr or reply).strip()[:200]}"
            ) from None
        raise


def _extract_verdict(text: str) -> Verdict:
    """Pull the JSON verdict out of the judge's reply, tolerating code fences or
    stray prose around it (scan first '{' to last '}')."""
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end < start:
        raise JudgeError(f"judge response had no JSON object: {text[:200]!r}")
    try:
        obj = json.loads(text[start : end + 1])
    except json.JSONDecodeError as e:
        raise JudgeError(f"judge JSON did not parse: {e}") from e
    if "passed" not in obj:
        raise JudgeError(f"judge JSON missing 'passed': {obj!r}")
    return Verdict(passed=bool(obj["passed"]), reasoning=str(obj.get("reasoning", "")))
