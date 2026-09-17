# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Judge verdict parsing + runner aggregation (no live model calls)."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from agent_evals import judge, reviser, runner  # noqa: E402
from agent_evals.subject import BASE_REF, CORPUS_DOC, corpus_subject  # noqa: E402


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


def test_source_diff_found_past_skipped_case() -> None:
    # A skipped real_aws case carries no source_diff. Picking summaries[0] blindly
    # would yield "" and silently suppress a proposal a later case earned.
    summaries = [
        {"case_id": "real_aws", "skipped": "requires --allow-real-aws"},
        {"case_id": "docs", "verdict": "improved", "source_diff": "diff --git a b\n"},
    ]
    diff = next((s["source_diff"] for s in summaries if s.get("source_diff")), "")
    assert diff == "diff --git a b\n"
    assert runner._should_emit_proposal(summaries, diff) is True


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


def test_safe_write_creates_nested_dirs(tmp_path) -> None:
    # A material/subject key with a path separator must create parent dirs, not
    # raise FileNotFoundError.
    runner._safe_write(tmp_path / "materials", "docs/guide.md", "content")
    assert (tmp_path / "materials" / "docs" / "guide.md").read_text() == "content"


def test_safe_write_rejects_parent_traversal(tmp_path) -> None:
    with pytest.raises(ValueError, match="unsafe"):
        runner._safe_write(tmp_path / "materials", "../escape.md", "x")


def test_safe_write_rejects_absolute_path(tmp_path) -> None:
    with pytest.raises(ValueError, match="unsafe"):
        runner._safe_write(tmp_path / "materials", "/etc/evil", "x")


def test_real_aws_context_requires_env(monkeypatch) -> None:
    monkeypatch.delenv(runner.ENV_FARM_ID, raising=False)
    monkeypatch.delenv(runner.ENV_QUEUE_ID, raising=False)
    with pytest.raises(runner.RealAwsConfigError, match="FARM_ID"):
        runner._real_aws_context()


def test_real_aws_context_reads_env(monkeypatch) -> None:
    monkeypatch.setenv(runner.ENV_FARM_ID, "farm-x")
    monkeypatch.setenv(runner.ENV_QUEUE_ID, "queue-y")
    monkeypatch.setenv(runner.ENV_REGION, "eu-central-1")
    ctx = runner._real_aws_context()
    assert ctx == {"farm_id": "farm-x", "queue_id": "queue-y", "region": "eu-central-1"}


def test_real_aws_skipped_without_optin(monkeypatch) -> None:
    monkeypatch.setenv(runner.ENV_FARM_ID, "farm-x")
    monkeypatch.setenv(runner.ENV_QUEUE_ID, "queue-y")
    reason = runner._real_aws_skip_reason(allow_real_aws=False)
    assert reason and "allow-real-aws" in reason


def test_real_aws_skip_reason_flags_missing_env(monkeypatch) -> None:
    monkeypatch.delenv(runner.ENV_FARM_ID, raising=False)
    monkeypatch.delenv(runner.ENV_QUEUE_ID, raising=False)
    reason = runner._real_aws_skip_reason(allow_real_aws=True)
    assert reason and "FARM_ID" in reason


def test_provenance_survives_binary_launcher(tmp_path, monkeypatch) -> None:
    # A native launcher (deadline.exe on Windows) isn't decodable UTF-8. Reading it
    # raises UnicodeDecodeError (a ValueError, NOT an OSError) -- that must not abort
    # the whole run before any eval executes.
    launcher = tmp_path / "deadline"
    launcher.write_bytes(b"\x7fELF\x02\x01\x01\x00\xff\xfe\xfd")
    monkeypatch.setattr(runner.shutil, "which", lambda _: str(launcher))
    monkeypatch.setattr(
        runner.subprocess,
        "run",
        lambda *a, **k: subprocess.CompletedProcess(a[0], 0, stdout="1.2.3", stderr=""),
    )
    line = runner._deadline_provenance()
    assert "1.2.3" in line and "EDITABLE" not in line


def test_judge_timeout_raises_judge_error(monkeypatch) -> None:
    # A hung judge must fail its own run, not hang the batch -- the harness timeout
    # can't cover the judge call.
    def _timeout(*a, **k):
        raise subprocess.TimeoutExpired(cmd="claude", timeout=k.get("timeout", 0))

    monkeypatch.setattr(judge.subprocess, "run", _timeout)
    with pytest.raises(judge.JudgeError, match="timeout"):
        judge.judge_answer("rubric", "prompt", "an answer", timeout_s=1)


def test_revise_timeout_raises_revise_error(tmp_path, monkeypatch) -> None:
    # The reviser drives a full edit session; a hung one must fail loudly instead of
    # blocking with no ceiling.
    subj = corpus_subject("# Guide\n", tmp_path / "corpus")
    run_dir = tmp_path / "out" / "case" / "run-1"
    run_dir.mkdir(parents=True)
    (run_dir / "events.jsonl").write_text("")

    real_run = subprocess.run

    def _timeout(cmd, *a, **k):
        # Only the agent launch times out; git calls must still work.
        if "claude" in cmd[0]:
            raise subprocess.TimeoutExpired(cmd=cmd[0], timeout=k.get("timeout", 0))
        return real_run(cmd, *a, **k)

    monkeypatch.setattr(reviser.subprocess, "run", _timeout)
    with pytest.raises(reviser.ReviseError, match="timeout"):
        reviser.revise(subj, run_dir, goal="g", base_ref=BASE_REF, timeout_s=1)


def test_revise_missing_binary_raises_revise_error(tmp_path) -> None:
    subj = corpus_subject("# Guide\n", tmp_path / "corpus")
    run_dir = tmp_path / "out" / "case" / "run-1"
    run_dir.mkdir(parents=True)
    (run_dir / "events.jsonl").write_text("")
    with pytest.raises(reviser.ReviseError, match="could not launch"):
        reviser.revise(
            subj, run_dir, goal="g", base_ref=BASE_REF, claude_bin="definitely-not-real-xyz"
        )
