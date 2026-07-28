# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Judge verdict parsing + runner aggregation (no live model calls)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from agent_evals import judge, runner  # noqa: E402
from agent_evals.subject import CORPUS_DOC, corpus_subject  # noqa: E402


def test_extract_verdict_plain_json() -> None:
    v = judge._extract_verdict('{"passed": true, "reasoning": "meets the rubric"}')
    assert v.passed is True and "rubric" in v.reasoning


def test_extract_verdict_tolerates_code_fence() -> None:
    v = judge._extract_verdict('```json\n{"passed": false, "reasoning": "missing"}\n```')
    assert v.passed is False


def test_extract_verdict_rejects_no_json() -> None:
    with pytest.raises(judge.JudgeError):
        judge._extract_verdict("I think it passes.")


def test_extract_verdict_requires_passed_key() -> None:
    with pytest.raises(judge.JudgeError):
        judge._extract_verdict('{"reasoning": "no verdict"}')


def test_extract_verdict_string_false_is_fail() -> None:
    # bool("false") is True in Python — the coercion must handle string forms safely.
    v = judge._extract_verdict('{"passed": "false", "reasoning": "did not meet rubric"}')
    assert v.passed is False


def test_extract_verdict_string_true_is_pass() -> None:
    v = judge._extract_verdict('{"passed": "true", "reasoning": "ok"}')
    assert v.passed is True


def test_extract_verdict_ambiguous_value_raises() -> None:
    with pytest.raises(judge.JudgeError, match="ambiguous"):
        judge._extract_verdict('{"passed": "maybe", "reasoning": "unsure"}')


def test_empty_answer_fails_without_model_call() -> None:
    v = judge.judge_answer("rubric", "prompt", "   ")
    assert v.passed is False


def test_aggregate_medians_and_pass_rate() -> None:
    runs = [
        {"passed": True, "num_turns": 4, "total_cost_usd": 0.10},
        {"passed": False, "num_turns": 10, "total_cost_usd": 0.30},
        {"passed": True, "num_turns": 6, "total_cost_usd": 0.20},
    ]
    agg = runner._aggregate(runs)
    assert agg["pass_rate"] == pytest.approx(2 / 3)
    assert agg["median_turns"] == 6.0
    assert agg["median_cost_usd"] == pytest.approx(0.20)
    assert agg["n"] == 3


def test_aggregate_empty() -> None:
    assert runner._aggregate([]) == {
        "pass_rate": 0.0,
        "median_turns": 0.0,
        "median_cost_usd": 0.0,
        "n": 0,
    }


def test_proposal_emitted_when_improved_and_none_regressed() -> None:
    summaries = [{"verdict": "improved"}, {"verdict": "no_change"}]
    assert runner._should_emit_proposal(summaries, "diff --git a b\n") is True


def test_proposal_suppressed_on_mixed_verdicts() -> None:
    # The patch is the whole base..revised diff -- a change that regressed ANY
    # eval must never be surfaced as PR-ready, even if it improved another.
    summaries = [{"verdict": "improved"}, {"verdict": "regressed"}]
    assert runner._should_emit_proposal(summaries, "diff --git a b\n") is False


def test_proposal_suppressed_without_improvement() -> None:
    summaries = [{"verdict": "no_change"}, {"verdict": "no_change"}]
    assert runner._should_emit_proposal(summaries, "diff --git a b\n") is False


def test_proposal_suppressed_on_empty_diff() -> None:
    assert runner._should_emit_proposal([{"verdict": "improved"}], "  \n") is False


def test_subject_files_reflects_checked_out_ref(tmp_path) -> None:
    # The seeded files must track the CURRENT ref, so baseline and revised runs get
    # different content -- otherwise a docs A/B compares identical sandboxes.
    subj = corpus_subject("# Guide\nbaseline text\n", tmp_path / "corpus")
    base_files = runner._subject_files(subj)
    assert base_files[CORPUS_DOC] == "# Guide\nbaseline text\n"

    (subj.root / CORPUS_DOC).write_text("# Guide\nrevised text\n")
    revised_ref = subj.commit_scratch("revised", "revise")
    subj.checkout(revised_ref)
    assert runner._subject_files(subj)[CORPUS_DOC] == "# Guide\nrevised text\n"


def test_subject_files_scoped_to_pathspec(tmp_path) -> None:
    # corpus_subject owns *.md; a non-markdown file must not be seeded.
    subj = corpus_subject("# Guide\n", tmp_path / "corpus")
    (subj.root / "notes.txt").write_text("not markdown")
    subj._git("add", "-A")
    subj._git("commit", "-q", "-m", "add non-md")
    files = runner._subject_files(subj)
    assert CORPUS_DOC in files
    assert "notes.txt" not in files
