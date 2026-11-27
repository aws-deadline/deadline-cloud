# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Judge parsing, runner logic, and reviser flow (no real agent calls)."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

import pytest

from agent_evals import judge, reviser, runner
from agent_evals.harness import RunResult
from agent_evals.subject import BASE_REF, CORPUS_DOC, corpus_subject

_REAL_RUN = subprocess.run
_DIFF = "diff --git a b\n"


def _fake_claude(monkeypatch, handler) -> None:
    """Route `claude` launches to `handler(cmd, kwargs)`; everything else (git) runs."""

    def _run(cmd, *args, **kwargs):
        if Path(cmd[0]).name == "claude":
            return handler(cmd, kwargs)
        return _REAL_RUN(cmd, *args, **kwargs)

    monkeypatch.setattr(subprocess, "run", _run)


# --- judge -------------------------------------------------------------------


@pytest.mark.parametrize(
    "reply, passed",
    [
        ('{"passed": true, "reasoning": "ok"}', True),
        ('```json\n{"passed": false, "reasoning": "no"}\n```', False),
        ('{"passed": "false", "reasoning": "no"}', False),
        ('{"passed": "true", "reasoning": "ok"}', True),
    ],
    ids=["json", "code_fence", "string_false", "string_true"],
)
def test_extract_verdict(reply, passed) -> None:
    assert judge._extract_verdict(reply).passed is passed


@pytest.mark.parametrize(
    "reply",
    ["I think it passes.", '{"reasoning": "no verdict"}', '{"passed": "maybe"}'],
    ids=["no_json", "no_passed_key", "ambiguous"],
)
def test_extract_verdict_rejects(reply) -> None:
    with pytest.raises(judge.JudgeError):
        judge._extract_verdict(reply)


def test_empty_answer_fails_without_model_call() -> None:
    assert judge.judge_answer("rubric", "prompt", "   ").passed is False


def _judge_call(monkeypatch, answer: str) -> dict:
    seen: dict = {}

    def _handler(cmd, kwargs):
        seen.update(prompt=cmd[cmd.index("-p") + 1], cmd=cmd, cwd=kwargs["cwd"])
        return subprocess.CompletedProcess(cmd, 0, '{"passed": false, "reasoning": "r"}', "")

    _fake_claude(monkeypatch, _handler)
    judge.judge_answer("rubric", "task", answer)
    return seen


def test_judge_is_toolless_in_an_empty_dir(monkeypatch) -> None:
    seen = _judge_call(monkeypatch, "my answer")
    assert seen["cmd"][seen["cmd"].index("--tools") + 1] == ""
    assert Path(seen["cwd"]) != Path.cwd()


def test_judge_wraps_answer_in_unpredictable_markers(monkeypatch) -> None:
    first = _judge_call(monkeypatch, "my answer")["prompt"]
    second = _judge_call(monkeypatch, "my answer")["prompt"]
    marker = first.split("<answer-", 1)[1].split(">", 1)[0]
    assert f"<answer-{marker}>\nmy answer\n</answer-{marker}>" in first
    assert f"<answer-{marker}>" not in second


def test_judge_truncates_long_answer(monkeypatch) -> None:
    prompt = _judge_call(monkeypatch, "x" * (judge.MAX_ANSWER_CHARS + 500))["prompt"]
    assert "[answer truncated]" in prompt
    assert "x" * (judge.MAX_ANSWER_CHARS + 1) not in prompt


def test_judge_timeout_raises_judge_error(monkeypatch) -> None:
    def _handler(cmd, kwargs):
        raise subprocess.TimeoutExpired(cmd, kwargs["timeout"])

    _fake_claude(monkeypatch, _handler)
    with pytest.raises(judge.JudgeError, match="timeout"):
        judge.judge_answer("rubric", "prompt", "an answer", timeout_s=1)


# --- runner: scoring ------------------------------------------------------------


def test_aggregate() -> None:
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
    assert runner._aggregate([])["n"] == 0


def _agg(pass_rate, turns, n=3):
    return {"pass_rate": pass_rate, "median_turns": turns, "median_cost_usd": 0.0, "n": n}


@pytest.mark.parametrize(
    "b, c, verdict",
    [
        (_agg(0.3, 5), _agg(0.6, 9), "improved"),
        (_agg(0.6, 9), _agg(0.3, 5), "regressed"),
        (_agg(1.0, 10), _agg(1.0, 6), "improved"),
        (_agg(0.0, 10), _agg(0.0, 6), "no_change"),
        (_agg(1.0, 10, n=1), _agg(1.0, 6, n=1), "no_change"),
    ],
    ids=["pass_up", "pass_down", "fewer_turns", "fails_faster", "turns_at_k1"],
)
def test_ab_verdict(b, c, verdict) -> None:
    assert runner._ab_verdict(b, c) == verdict


def _summary(verdict, k=3):
    agg = _agg(1.0, 1, n=k)
    return {
        "verdict": verdict,
        "variants": {"baseline": {"aggregate": agg}, "revised": {"aggregate": agg}},
    }


@pytest.mark.parametrize(
    "summaries, diff, emit",
    [
        ([_summary("improved"), _summary("no_change")], _DIFF, True),
        ([_summary("improved"), _summary("regressed")], _DIFF, False),
        ([_summary("no_change")], _DIFF, False),
        ([_summary("improved")], "  \n", False),
        ([_summary("improved", k=1)], _DIFF, False),
        ([{"skipped": "x"}, _summary("improved")], _DIFF, True),
    ],
    ids=["improved", "any_regression", "no_improvement", "empty_diff", "k1", "with_skipped"],
)
def test_should_emit_proposal(summaries, diff, emit) -> None:
    assert runner._should_emit_proposal(summaries, diff) is emit


def test_proposal_diff_skips_cases_without_one() -> None:
    summaries = [{"case_id": "aws", "skipped": "x"}, {"case_id": "docs", "source_diff": _DIFF}]
    assert runner._proposal_diff(summaries) == _DIFF


# --- runner: inputs and environment ------------------------------------------------


def test_safe_write_creates_nested_dirs(tmp_path) -> None:
    runner._safe_write(tmp_path / "materials", "docs/guide.md", "content")
    assert (tmp_path / "materials" / "docs" / "guide.md").read_text() == "content"


_UNSAFE = ["../escape.md", "/etc/evil"] + (["C:evil", "C:/evil"] if sys.platform == "win32" else [])


@pytest.mark.parametrize("rel", _UNSAFE)
def test_safe_write_rejects_escapes(tmp_path, rel) -> None:
    with pytest.raises(ValueError, match="unsafe"):
        runner._safe_write(tmp_path / "materials", rel, "x")


def test_fill_placeholders_leaves_other_braces() -> None:
    text = 'farm {farm_id}; OpenJD {{Task.File.run}}; json {"a": 1}; {unknown}'
    out = runner._fill_placeholders(text, {"farm_id": "farm-x"})
    assert out == 'farm farm-x; OpenJD {{Task.File.run}}; json {"a": 1}; {unknown}'


def test_agent_env_exports_region(monkeypatch) -> None:
    monkeypatch.setenv("KEEP_ME", "1")
    env = runner._agent_env({"region": "eu-west-1"})
    assert env["AWS_REGION"] == env["AWS_DEFAULT_REGION"] == "eu-west-1"
    assert env["KEEP_ME"] == "1"
    assert runner._agent_env({"region": ""}) is None
    assert runner._agent_env(None) is None


@pytest.fixture
def aws_env(monkeypatch):
    monkeypatch.setenv(runner.ENV_FARM_ID, "farm-x")
    monkeypatch.setenv(runner.ENV_QUEUE_ID, "queue-y")
    monkeypatch.setenv(runner.ENV_REGION, "eu-central-1")
    monkeypatch.setattr(runner, "_aws_authenticated", lambda: True)
    return monkeypatch


def test_real_aws_setup_ready(aws_env) -> None:
    ctx, skip = runner._real_aws_setup(allow_real_aws=True)
    assert skip is None
    assert ctx == {"farm_id": "farm-x", "queue_id": "queue-y", "region": "eu-central-1"}


def test_real_aws_setup_needs_opt_in(aws_env) -> None:
    assert "allow-real-aws" in runner._real_aws_setup(allow_real_aws=False)[1]


def test_real_aws_setup_needs_env(aws_env) -> None:
    aws_env.delenv(runner.ENV_FARM_ID)
    assert runner.ENV_FARM_ID in runner._real_aws_setup(allow_real_aws=True)[1]


def test_real_aws_setup_needs_auth(aws_env) -> None:
    aws_env.setattr(runner, "_aws_authenticated", lambda: False)
    assert "auth" in runner._real_aws_setup(allow_real_aws=True)[1]


@pytest.mark.parametrize(
    "cases",
    [{"id": "x"}, [{"id": "has space"}], [{"id": "a/b"}], [{}]],
    ids=["not_list", "space", "slash", "missing"],
)
def test_load_cases_rejects(tmp_path, cases) -> None:
    path = tmp_path / "eval.json"
    path.write_text(json.dumps(cases))
    with pytest.raises(ValueError):
        runner._load_cases(str(path))


def test_provenance_survives_binary_launcher(tmp_path, monkeypatch) -> None:
    launcher = tmp_path / "deadline"
    launcher.write_bytes(b"\x7fELF\x02\x01\x01\x00\xff\xfe\xfd")
    calls = []

    def _run(cmd, **kwargs):
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, stdout="1.2.3", stderr="")

    monkeypatch.setattr(runner.shutil, "which", lambda _: str(launcher))
    monkeypatch.setattr(runner.subprocess, "run", _run)
    assert runner._deadline_provenance() == f"deadline: 1.2.3 at {launcher}"
    assert calls == [[str(launcher), "--version"]]  # never reached pip


# --- runner: run flow ---------------------------------------------------------------


_CASE = {"id": "c", "prompt": "p", "rubric": "r"}


def test_failed_agent_run_is_not_judged(tmp_path, monkeypatch) -> None:
    def _no_judge(*a, **k):
        raise AssertionError("judge called on a failed run")

    failed = RunResult(success=False, subtype="error_max_turns", final_text="ok")
    monkeypatch.setattr(runner, "run_agent", lambda *a, **k: failed)
    monkeypatch.setattr(runner.judge, "judge_answer", _no_judge)
    record = runner._run_case(_CASE, tmp_path / "run-1", None)
    assert record["passed"] is False and "error_max_turns" in record["detail"]
    assert json.loads((tmp_path / "run-1" / "result.json").read_text())["passed"] is False


def test_summary_written_when_a_case_raises(tmp_path, monkeypatch) -> None:
    eval_file = tmp_path / "e.json"
    eval_file.write_text(json.dumps([{**_CASE, "id": "ok"}, {**_CASE, "id": "boom"}]))
    record = {"passed": True, "num_turns": 1, "total_cost_usd": 0.0}

    def _run_case(case, *a):
        if case["id"] == "boom":
            raise RuntimeError("boom")
        return record

    monkeypatch.setattr(runner, "OUTPUT_ROOT", tmp_path / "out")
    monkeypatch.setattr(runner, "_run_case", _run_case)
    monkeypatch.setattr(runner, "_deadline_provenance", lambda: "deadline: test")
    args = argparse.Namespace(
        eval_file=str(eval_file),
        k=1,
        model=None,
        revised_ref=None,
        base_ref=None,
        pathspec="src",
        seed_subject=False,
        allow_real_aws=False,
    )
    with pytest.raises(RuntimeError, match="boom"):
        runner._cmd_run(args)
    (summary,) = (tmp_path / "out" / "e").glob("*/summary.json")
    assert [s["case_id"] for s in json.loads(summary.read_text())] == ["ok"]


# --- reviser --------------------------------------------------------------------------


@pytest.fixture
def run_dir(tmp_path) -> Path:
    path = tmp_path / "out" / "case" / "baseline" / "run-1"
    path.mkdir(parents=True)
    (path / "events.jsonl").write_text("")
    return path


@pytest.fixture
def corpus(tmp_path):
    return corpus_subject("# Guide\noriginal\n", tmp_path / "corpus")


def _agent_result(cmd, is_error=False):
    event = {"type": "result", "subtype": "success", "is_error": is_error, "result": "done"}
    return subprocess.CompletedProcess(cmd, 0, json.dumps(event) + "\n", "")


def _timeout(cmd, kwargs):
    raise subprocess.TimeoutExpired(cmd, 1)


def test_revise_commits_without_touching_operator_checkout(corpus, run_dir, monkeypatch) -> None:
    (corpus.root / CORPUS_DOC).write_text("# Guide\noperator wip\n")

    def _edit(cmd, kwargs):
        doc = Path(kwargs["cwd"]) / CORPUS_DOC
        doc.write_text(doc.read_text() + "added step\n")
        return _agent_result(cmd)

    _fake_claude(monkeypatch, _edit)
    ref = reviser.revise(corpus, run_dir, goal="g", base_ref=BASE_REF)

    assert ref.startswith("eval-revise-")
    assert "added step" in corpus.diff_refs(BASE_REF, ref)
    assert (corpus.root / CORPUS_DOC).read_text() == "# Guide\noperator wip\n"
    assert corpus._git_checked("rev-parse", "--abbrev-ref", "HEAD").stdout.strip() == BASE_REF


@pytest.mark.parametrize("ref, match", [("has space", "invalid"), (BASE_REF, "exists")])
def test_revise_rejects_unusable_ref_before_running(
    corpus, run_dir, monkeypatch, ref, match
) -> None:
    _fake_claude(monkeypatch, lambda cmd, kwargs: pytest.fail("agent launched"))
    with pytest.raises(reviser.ReviseError, match=match):
        reviser.revise(corpus, run_dir, goal="g", base_ref=BASE_REF, ref=ref)


@pytest.mark.parametrize(
    "handler, match",
    [
        (lambda cmd, kwargs: _agent_result(cmd), "no edits"),
        (lambda cmd, kwargs: _agent_result(cmd, is_error=True), "failed"),
        (_timeout, "timeout"),
    ],
    ids=["no_edits", "agent_error", "timeout"],
)
def test_revise_errors(corpus, run_dir, monkeypatch, handler, match) -> None:
    _fake_claude(monkeypatch, handler)
    with pytest.raises(reviser.ReviseError, match=match):
        reviser.revise(corpus, run_dir, goal="g", base_ref=BASE_REF, timeout_s=1)


def test_revise_missing_binary(corpus, run_dir) -> None:
    with pytest.raises(reviser.ReviseError, match="could not launch"):
        reviser.revise(
            corpus, run_dir, goal="g", base_ref=BASE_REF, claude_bin="definitely-not-real-xyz"
        )
