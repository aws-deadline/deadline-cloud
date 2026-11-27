# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Run an eval file, optionally as an A/B between two refs of this checkout.

See evals/README.md for the eval-file schema and flags.

Usage:
    python -m agent_evals.runner run examples/deadline_cli.json --k 3
    python -m agent_evals.runner run my_eval.json --k 3 --revised-ref my-branch
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

from . import judge, subject as subject_mod
from .harness import HarnessError, RunResult, run_agent

OUTPUT_ROOT = Path(__file__).resolve().parents[1] / "output"

DEFAULT_TOOLS = ["Bash", "Read", "Write", "Edit"]

# Case ids become directory names and temp-dir prefixes.
_CASE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")

# One run per variant is a single sample, too noisy to ship a patch on.
MIN_PROPOSAL_K = 2

REAL_AWS_ENV = "real_aws"
ENV_FARM_ID = "DEADLINE_EVAL_FARM_ID"
ENV_QUEUE_ID = "DEADLINE_EVAL_QUEUE_ID"
ENV_REGION = "DEADLINE_EVAL_REGION"

_TEXT = {"encoding": "utf-8", "errors": "replace"}


def _real_aws_setup(allow_real_aws: bool) -> Tuple[Optional[dict], Optional[str]]:
    """(farm/queue/region context, None) when real_aws cases can run, else
    (None, reason to skip). Skipped rather than failed, so a default run passes."""
    if not allow_real_aws:
        return None, "requires --allow-real-aws (submits real, billable jobs)"
    farm = os.environ.get(ENV_FARM_ID, "").strip()
    queue = os.environ.get(ENV_QUEUE_ID, "").strip()
    missing = [n for n, v in ((ENV_FARM_ID, farm), (ENV_QUEUE_ID, queue)) if not v]
    if missing:
        return None, (
            f"needs {' and '.join(missing)} set to a non-production sandbox "
            "farm/queue you own (these submit real, billable jobs)"
        )
    if not _aws_authenticated():
        return None, "deadline auth status is not authenticated; run `deadline auth login`"
    region = os.environ.get(ENV_REGION, "").strip()
    return {"farm_id": farm, "queue_id": queue, "region": region}, None


def _aws_authenticated() -> bool:
    try:
        out = subprocess.run(
            ["deadline", "auth", "status"], capture_output=True, text=True, timeout=30, **_TEXT
        ).stdout
    except (OSError, subprocess.TimeoutExpired):
        return False
    return '"api_availability": true' in out or "API Availability: True" in out


def _fill_placeholders(text: str, ctx: dict) -> str:
    """Replace only the known {name} placeholders; str.format would choke on, or
    unescape, every other brace (e.g. OpenJD `{{...}}`)."""
    for name, value in ctx.items():
        text = text.replace("{" + name + "}", value)
    return text


def _agent_env(aws_ctx: Optional[dict]) -> Optional[dict]:
    """The operator's environment plus the eval region, if one is set."""
    region = (aws_ctx or {}).get("region")
    if not region:
        return None
    return {**os.environ, "AWS_REGION": region, "AWS_DEFAULT_REGION": region}


def _safe_write(base: Path, rel: str, content: str) -> None:
    """Write base/rel, refusing paths that would land outside `base`."""
    rel_path = Path(rel)
    # `anchor`, not is_absolute(): on Windows "/x" and "C:x" aren't absolute but
    # still escape `base`.
    if rel_path.anchor or ".." in rel_path.parts:
        raise ValueError(f"unsafe sandbox path: {rel!r}")
    dest = base / rel_path
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(content, encoding="utf-8")


def _write_result(run_dir: Path, record: dict, events: list = ()) -> dict:
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "result.json").write_text(json.dumps(record, indent=2), encoding="utf-8")
    with (run_dir / "events.jsonl").open("w", encoding="utf-8") as f:
        for ev in events:
            f.write(json.dumps(ev) + "\n")
    return record


def _run_case(
    case: dict, run_dir: Path, model: Optional[str], subject_files=None, aws_ctx=None
) -> dict:
    """One agent run plus judge verdict, with artifacts under run_dir."""
    prompt = _fill_placeholders(case["prompt"], aws_ctx or {})
    rubric = _fill_placeholders(case["rubric"], aws_ctx or {})
    materials = case.get("materials", {})
    if materials:
        listing = ", ".join(f"materials/{name}" for name in materials)
        prompt = f"{prompt}\n\nReference material is available in this directory: {listing}"
    if subject_files:
        prompt = f"{prompt}\n\nThe material under evaluation is available under subject/ in this directory."

    workdir = Path(tempfile.mkdtemp(prefix=f"eval-{case['id']}-"))
    try:
        for name, content in materials.items():
            _safe_write(workdir / "materials", name, content)
        for rel, content in (subject_files or {}).items():
            _safe_write(workdir / "subject", rel, content)
        result = run_agent(
            prompt,
            workdir,
            case.get("tools", DEFAULT_TOOLS),
            max_turns=case.get("max_turns", 20),
            model=model,
            env=_agent_env(aws_ctx),
        )
    except HarnessError as e:
        print(f"    run: FAIL (harness error) -- {e}")
        failed = RunResult(success=False, subtype="harness_error").telemetry_dict()
        return _write_result(run_dir, {**failed, "passed": False, "detail": f"harness error: {e}"})
    finally:
        shutil.rmtree(workdir, ignore_errors=True)

    if not result.success:
        passed, reasoning = False, f"agent run failed ({result.subtype})"
    else:
        try:
            verdict = judge.judge_answer(rubric, prompt, result.final_text, model=model)
            passed, reasoning = verdict.passed, verdict.reasoning
        except judge.JudgeError as e:
            passed, reasoning = False, f"judge error: {e}"

    print(
        f"    run: {'PASS' if passed else 'FAIL'} (tools={result.tool_call_count}, "
        f"turns={result.num_turns}, cost=${result.total_cost_usd:.4f}) -- {reasoning}"
    )
    record = {**result.telemetry_dict(), "passed": passed, "detail": reasoning}
    return _write_result(run_dir, record, result.raw_events)


def _deadline_provenance() -> str:
    """Which `deadline` the agent will drive: path, version, and editable target."""
    path = shutil.which("deadline")
    if not path:
        return "deadline: NOT ON PATH (agent runs that need it will fail)"
    try:
        version = subprocess.run(
            [path, "--version"], capture_output=True, text=True, timeout=30, **_TEXT
        ).stdout.strip()
    except (OSError, subprocess.TimeoutExpired):
        version = "version unknown"
    editable = ""
    try:
        # The console-script shebang names the interpreter to ask pip with. A
        # native launcher (deadline.exe) raises UnicodeDecodeError here.
        interp = Path(path).read_text(encoding="utf-8").splitlines()[0].lstrip("#!").strip()
        show = subprocess.run(
            [interp, "-m", "pip", "show", "deadline"],
            capture_output=True,
            text=True,
            timeout=30,
            **_TEXT,
        ).stdout
        for line in show.splitlines():
            if line.lower().startswith("editable project location"):
                editable = f" [EDITABLE -> {line.split(':', 1)[1].strip()}]"
    except (OSError, subprocess.TimeoutExpired, IndexError, ValueError):
        pass  # diagnostic only; report without the editable target
    return f"deadline: {version} at {path}{editable}"


def _aggregate(runs: list) -> dict:
    if not runs:
        return {"pass_rate": 0.0, "median_turns": 0.0, "median_cost_usd": 0.0, "n": 0}
    return {
        "pass_rate": sum(1 for r in runs if r["passed"]) / len(runs),
        "median_turns": statistics.median(float(r["num_turns"]) for r in runs),
        "median_cost_usd": statistics.median(r["total_cost_usd"] for r in runs),
        "n": len(runs),
    }


def _ab_verdict(b: dict, c: dict) -> str:
    """improved / regressed / no_change for baseline `b` vs revised `c`.

    Fewer turns breaks a pass-rate tie only if the variants pass and have k > 1:
    at 0% it means giving up faster, and at k=1 it's one noisy sample.
    """
    if c["pass_rate"] > b["pass_rate"]:
        return "improved"
    if c["pass_rate"] < b["pass_rate"]:
        return "regressed"
    turns_comparable = b["pass_rate"] > 0 and min(b["n"], c["n"]) > 1
    if turns_comparable and c["median_turns"] < b["median_turns"]:
        return "improved"
    return "no_change"


def _min_k(summary: dict) -> int:
    return min(v["aggregate"]["n"] for v in summary["variants"].values())


def _proposal_diff(summaries: list) -> str:
    """The base..revised diff; every A/B case carries the same one, skipped ones none."""
    return next((s["source_diff"] for s in summaries if s.get("source_diff")), "")


def _should_emit_proposal(summaries: list, diff: str) -> bool:
    """Emit the patch only if some eval improved at k >= MIN_PROPOSAL_K and none
    regressed; the patch is the whole diff, so one regression disqualifies it."""
    if not diff.strip() or any(s.get("verdict") == "regressed" for s in summaries):
        return False
    return any(s.get("verdict") == "improved" and _min_k(s) >= MIN_PROPOSAL_K for s in summaries)


def _load_cases(path: str) -> list:
    cases = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(cases, list):
        raise ValueError("eval file must be a JSON array of cases")
    for case in cases:
        if not _CASE_ID.match(str(case.get("id", ""))):
            raise ValueError(f"case id {case.get('id')!r} must match {_CASE_ID.pattern}")
    return cases


def _cmd_run(args: argparse.Namespace) -> int:
    try:
        cases = _load_cases(args.eval_file)
    except ValueError as e:
        print(f"ERROR: {e}")
        return 2
    if args.seed_subject and not args.revised_ref:
        print("ERROR: --seed-subject only applies in A/B mode; pass --revised-ref too.")
        return 2

    print(f"[env] {_deadline_provenance()}")

    subj = None
    base_ref = None
    if args.revised_ref:
        subj = subject_mod.repo_subject(args.pathspec)
        # Check before any paid agent runs.
        try:
            subj.assert_clean()
            base_ref = subj.resolve(args.base_ref or "HEAD")
            subj.resolve(args.revised_ref)
        except subject_mod.SubjectError as e:
            print(f"ERROR: {e}")
            return 2
        print(f"[subject] {subj.root} (pathspec={subj.pathspec})")
        print(f"[variants] baseline={base_ref}  revised={args.revised_ref}")

    aws_ctx, aws_skip = None, None
    if any(c.get("env") == REAL_AWS_ENV for c in cases):
        aws_ctx, aws_skip = _real_aws_setup(args.allow_real_aws)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_root = OUTPUT_ROOT / Path(args.eval_file).stem / timestamp
    refs = {"baseline": base_ref, "revised": args.revised_ref} if subj else {"baseline": None}

    summaries = []
    exit_code = 0
    try:
        for case in cases:
            real_aws = case.get("env") == REAL_AWS_ENV
            if real_aws and aws_skip:
                print(f"\n=== {case['id']} === SKIPPED (real_aws): {aws_skip}")
                summaries.append({"case_id": case["id"], "skipped": aws_skip})
                continue

            k = case.get("k", args.k)
            print(f"\n=== {case['id']} (k={k}) ===")
            variants = {}
            for variant, ref in refs.items():
                subject_files = None
                if subj:
                    subj.checkout(ref)
                    print(f"  [{variant}] subject at {ref}")
                    if args.seed_subject:
                        subject_files = subj.owned_files()
                runs = [
                    _run_case(
                        case,
                        out_root / case["id"] / variant / f"run-{i}",
                        args.model,
                        subject_files,
                        aws_ctx if real_aws else None,
                    )
                    for i in range(1, k + 1)
                ]
                variants[variant] = {"aggregate": _aggregate(runs), "runs": runs}

            summary = {"case_id": case["id"], "variants": variants}
            if subj:
                summary["source_diff"] = subj.diff_refs(base_ref, args.revised_ref)
                b, c = variants["baseline"]["aggregate"], variants["revised"]["aggregate"]
                summary["verdict"] = _ab_verdict(b, c)
                print(
                    f"  [A/B] pass {b['pass_rate']:.0%} -> {c['pass_rate']:.0%}, "
                    f"turns {b['median_turns']:g} -> {c['median_turns']:g}: {summary['verdict']}"
                )
                if summary["verdict"] == "regressed":
                    exit_code = 4
            summaries.append(summary)
    finally:
        # Keep finished runs even if a later case raised.
        out_root.mkdir(parents=True, exist_ok=True)
        (out_root / "summary.json").write_text(json.dumps(summaries, indent=2), encoding="utf-8")
        print(f"\nsummary -> {out_root / 'summary.json'}")
        if subj:
            # Must not replace the exception that got us here.
            try:
                subj.reset_clean()
            except subject_mod.SubjectError as e:
                print(f"WARNING: could not restore {subj.pathspec} to HEAD: {e}")

    if subj:
        diff = _proposal_diff(summaries)
        if _should_emit_proposal(summaries, diff):
            (out_root / "proposal.patch").write_text(diff, encoding="utf-8")
            print(f"proposal -> {out_root / 'proposal.patch'}")
        elif any(s.get("verdict") == "regressed" for s in summaries):
            print("[proposal] skipped: the change regressed at least one eval.")
        elif any(s.get("verdict") == "improved" for s in summaries):
            print(f"[proposal] skipped: improvements need k >= {MIN_PROPOSAL_K} to count.")
    return exit_code


def main(argv: Optional[list] = None) -> int:
    ap = argparse.ArgumentParser(prog="agent-evals")
    sub = ap.add_subparsers(dest="cmd")

    run = sub.add_parser("run", help="run an eval file")
    run.add_argument("eval_file", help="path to a JSON eval file")
    run.add_argument("--k", type=int, default=1, help="runs per case per variant")
    run.add_argument("--model", help="model for the agent and the judge")
    run.add_argument("--revised-ref", help="A/B mode: also run with this git ref and compare")
    run.add_argument("--base-ref", help="baseline ref for A/B (default: HEAD)")
    run.add_argument(
        "--pathspec",
        default="src",
        help="repo paths the A/B owns, e.g. 'src' or ':(glob)docs/**/*.md'",
    )
    run.add_argument(
        "--seed-subject",
        action="store_true",
        help="copy the owned files into each run's directory under subject/ (docs A/B)",
    )
    run.add_argument(
        "--allow-real-aws",
        action="store_true",
        help=f'run "env": "{REAL_AWS_ENV}" cases, which submit real, billable jobs; '
        f"needs {ENV_FARM_ID} and {ENV_QUEUE_ID}",
    )

    args = ap.parse_args(argv)
    if args.cmd == "run":
        return _cmd_run(args)
    ap.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
