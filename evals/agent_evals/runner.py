# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Run agent evals: before-and-after comparisons over any material.

An eval file is a JSON array of cases. Each case gives an agent a goal and grades
the outcome against a rubric:

    [
      {
        "id": "list_farms",
        "prompt": "List the Deadline Cloud farms and report how many there are.",
        "tools": ["Bash"],
        "rubric": "The answer states the number of farms.",
        "materials": {"guide.md": "...optional reference text..."},
        "max_turns": 20,
        "k": 3
      }
    ]

Each run launches an ISOLATED headless agent in a fresh sandbox; an LLM judge
grades the final answer against the rubric. With --revised-ref, every case runs
twice -- baseline at the current subject ref, candidate at the revised ref -- and
the summary reports the paired delta plus the subject diff (the PR-ready patch).

Usage:
    python -m agent_evals.runner run examples/cli_basics.json
    python -m agent_evals.runner run my_eval.json --k 3 --revised-ref my-branch
"""

from __future__ import annotations

import argparse
import json
import shutil
import statistics
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from . import judge, subject as subject_mod
from .harness import run_agent

OUTPUT_ROOT = Path(__file__).resolve().parents[1] / "output"

DEFAULT_TOOLS = ["Bash", "Read", "Write", "Edit"]


def _subject_files(subj) -> dict:
    """The subject's owned files (per its pathspec) at the CURRENTLY checked-out ref,
    as {relative_path: content}. Used to seed read-material subjects into the
    sandbox so a ref swap actually changes what the agent sees."""
    listing = subj._git_checked("ls-files", "-z", "--", subj.diff_pathspec).stdout
    files = {}
    for rel in filter(None, listing.split("\0")):
        p = subj.root / rel
        if p.is_file():
            files[rel] = p.read_text(errors="replace")
    return files


def _run_case(case: dict, run_dir: Path, model: Optional[str], subject_files=None) -> dict:
    """One agent run + judge verdict; artifacts under run_dir."""
    prompt = case["prompt"]
    materials = case.get("materials", {})
    if materials:
        listing = ", ".join(f"materials/{name}" for name in materials)
        prompt = f"{prompt}\n\nReference material is available in this directory: {listing}"
    if subject_files:
        prompt = f"{prompt}\n\nThe material under evaluation is available under subject/ in this directory."

    workdir = Path(tempfile.mkdtemp(prefix=f"eval-{case['id']}-"))
    try:
        if materials:
            mdir = workdir / "materials"
            mdir.mkdir()
            for name, content in materials.items():
                (mdir / name).write_text(content)
        if subject_files:
            # Seed the subject's owned files (docs, etc.) into the sandbox. Without
            # this, a read-material A/B would compare two identical sandboxes: the
            # ref swap happens in the repo checkout the sandboxed agent can't see.
            for rel, content in subject_files.items():
                dest = workdir / "subject" / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_text(content)

        result = run_agent(
            prompt,
            workdir,
            case.get("tools", DEFAULT_TOOLS),
            max_turns=case.get("max_turns", 20),
            model=model,
        )
    finally:
        shutil.rmtree(workdir, ignore_errors=True)

    try:
        verdict = judge.judge_answer(case["rubric"], prompt, result.final_text, model=model)
        passed, reasoning = verdict.passed, verdict.reasoning
    except judge.JudgeError as e:
        # A judge that can't render a verdict must not silently pass a run.
        passed, reasoning = False, f"judge error: {e}"

    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "result.json").write_text(
        json.dumps({**result.telemetry_dict(), "passed": passed, "detail": reasoning}, indent=2)
    )
    with (run_dir / "events.jsonl").open("w") as f:
        for ev in result.raw_events:
            f.write(json.dumps(ev) + "\n")

    mark = "PASS" if passed else "FAIL"
    print(
        f"    run: {mark} (tools={result.tool_call_count}, turns={result.num_turns}, "
        f"cost=${result.total_cost_usd:.4f}) -- {reasoning}"
    )
    return {**result.telemetry_dict(), "passed": passed, "detail": reasoning}


def _aggregate(runs: list) -> dict:
    if not runs:
        return {"pass_rate": 0.0, "median_turns": 0.0, "median_cost_usd": 0.0, "n": 0}
    return {
        "pass_rate": sum(1 for r in runs if r["passed"]) / len(runs),
        "median_turns": statistics.median(float(r["num_turns"]) for r in runs),
        "median_cost_usd": statistics.median(r["total_cost_usd"] for r in runs),
        "n": len(runs),
    }


def _cmd_run(args: argparse.Namespace) -> int:
    cases = json.loads(Path(args.eval_file).read_text())
    if not isinstance(cases, list):
        print("ERROR: eval file must be a JSON array of cases.")
        return 2

    if args.seed_subject and not args.revised_ref:
        print("ERROR: --seed-subject only applies in A/B mode; pass --revised-ref too.")
        return 2

    subj = None
    base_ref = None
    if args.revised_ref:
        subj = subject_mod.repo_subject(args.pathspec)
        base = subj._git("rev-parse", "--abbrev-ref", "HEAD").stdout.strip()
        base_ref = args.base_ref or base
        # Fail fast, BEFORE any (costly) agent runs: both refs must resolve, and the
        # operator's working tree must be clean -- checkout() discards changes under
        # the pathspec, and that must never eat uncommitted work.
        try:
            subj.assert_clean()
            for ref in (base_ref, args.revised_ref):
                subj._git_checked("rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}")
        except subject_mod.SubjectError as e:
            print(f"ERROR: {e}")
            return 2
        print(f"[subject] {subj.root} (pathspec={subj.diff_pathspec})")
        print(f"[variants] baseline={base_ref}  revised={args.revised_ref}")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_root = OUTPUT_ROOT / Path(args.eval_file).stem / timestamp

    summaries = []
    exit_code = 0
    try:
        for case in cases:
            k = case.get("k", args.k)
            print(f"\n=== {case['id']} (k={k}) ===")
            variants = {}

            refs = (
                {"baseline": base_ref, "revised": args.revised_ref} if subj else {"baseline": None}
            )
            for variant, ref in refs.items():
                subject_files = None
                if subj and ref:
                    subj.checkout(ref)
                    print(f"  [{variant}] subject at {ref}")
                    if args.seed_subject:
                        # Read-material subject (docs): the sandboxed agent can't see
                        # the repo checkout, so seed the owned files AT THIS REF into
                        # the sandbox -- otherwise both variants get identical input
                        # and the A/B silently reports no_change.
                        subject_files = _subject_files(subj)
                runs = [
                    _run_case(
                        case,
                        out_root / case["id"] / variant / f"run-{i}",
                        args.model,
                        subject_files,
                    )
                    for i in range(1, k + 1)
                ]
                variants[variant] = {"aggregate": _aggregate(runs), "runs": runs}

            summary = {"case_id": case["id"], "variants": variants}
            if subj and args.revised_ref:
                summary["source_diff"] = subj.diff_refs(base_ref, args.revised_ref)
                b, c = variants["baseline"]["aggregate"], variants["revised"]["aggregate"]
                improved = c["pass_rate"] > b["pass_rate"] or (
                    c["pass_rate"] == b["pass_rate"] and c["median_turns"] < b["median_turns"]
                )
                regressed = c["pass_rate"] < b["pass_rate"]
                summary["verdict"] = (
                    "improved" if improved else "regressed" if regressed else "no_change"
                )
                print(
                    f"  [A/B] pass {b['pass_rate']:.0%} -> {c['pass_rate']:.0%}, "
                    f"turns {b['median_turns']:g} -> {c['median_turns']:g}: {summary['verdict']}"
                )
                if regressed:
                    exit_code = 4
            summaries.append(summary)
    finally:
        if subj and base_ref:
            subj.checkout(base_ref)  # always leave the checkout on baseline

    out_root.mkdir(parents=True, exist_ok=True)
    (out_root / "summary.json").write_text(json.dumps(summaries, indent=2))
    print(f"\nsummary -> {out_root / 'summary.json'}")

    if subj and args.revised_ref:
        diff = summaries[0].get("source_diff", "") if summaries else ""
        if _should_emit_proposal(summaries, diff):
            (out_root / "proposal.patch").write_text(diff)
            print(f"proposal -> {out_root / 'proposal.patch'}")
        elif any(s.get("verdict") == "regressed" for s in summaries):
            print("[proposal] skipped: the change regressed at least one eval.")
    return exit_code


def _should_emit_proposal(summaries: list, diff: str) -> bool:
    """Emit the PR-ready patch only when the change measurably improved at least
    one eval AND regressed none. The patch is the whole base..revised diff, so a
    change that breaks any eval must never be surfaced as ready to ship."""
    if not diff.strip():
        return False
    verdicts = [s.get("verdict") for s in summaries]
    return "improved" in verdicts and "regressed" not in verdicts


def main(argv: Optional[list] = None) -> int:
    ap = argparse.ArgumentParser(prog="agent-evals")
    sub = ap.add_subparsers(dest="cmd")

    run = sub.add_parser("run", help="run an eval file")
    run.add_argument("eval_file", help="path to a JSON eval file")
    run.add_argument("--k", type=int, default=1, help="runs per case per variant")
    run.add_argument("--model", help="model for the agent AND the judge")
    run.add_argument(
        "--revised-ref",
        help="A/B mode: also run with this repo at the given git ref and compare",
    )
    run.add_argument("--base-ref", help="baseline ref for A/B (default: current branch)")
    run.add_argument(
        "--pathspec",
        default="src",
        help="repo paths the A/B owns, e.g. 'src' or ':(glob)docs/**/*.md'",
    )
    run.add_argument(
        "--seed-subject",
        action="store_true",
        help="copy the subject's owned files into each run's sandbox (under subject/) "
        "so the agent READS them -- required for docs/prose A/B, where the agent has "
        "no path to the repo checkout. Not needed for the CLI-source case (pip install "
        "-e makes the ref swap take effect through the installed `deadline`).",
    )

    args = ap.parse_args(argv)
    if args.cmd == "run":
        return _cmd_run(args)
    ap.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
