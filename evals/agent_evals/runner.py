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
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from . import judge, subject as subject_mod
from .harness import HarnessError, run_agent

OUTPUT_ROOT = Path(__file__).resolve().parents[1] / "output"

DEFAULT_TOOLS = ["Bash", "Read", "Write", "Edit"]

# A case with "env": "real_aws" submits real, billable jobs to a real farm. Such
# cases are SKIPPED unless the operator passes --allow-real-aws, and the farm/queue
# come from these env vars -- never hardcoded, so a public eval file names no
# account. Prompts may reference {farm_id}/{queue_id}/{region}, filled from here.
REAL_AWS_ENV = "real_aws"
ENV_FARM_ID = "DEADLINE_EVAL_FARM_ID"
ENV_QUEUE_ID = "DEADLINE_EVAL_QUEUE_ID"
ENV_REGION = "DEADLINE_EVAL_REGION"


class RealAwsConfigError(RuntimeError):
    """Raised when a real_aws case is requested but the environment isn't ready."""


def _real_aws_context() -> dict:
    """Farm/queue/region for real_aws cases, from env vars. Raises if the required
    ones are unset -- we never fall back to a hardcoded or ambient default farm."""
    farm = os.environ.get(ENV_FARM_ID, "").strip()
    queue = os.environ.get(ENV_QUEUE_ID, "").strip()
    missing = [n for n, v in ((ENV_FARM_ID, farm), (ENV_QUEUE_ID, queue)) if not v]
    if missing:
        raise RealAwsConfigError(
            f"real_aws case needs {' and '.join(missing)} set to a NON-PRODUCTION "
            "sandbox farm/queue you own (these submit real, billable jobs)."
        )
    return {"farm_id": farm, "queue_id": queue, "region": os.environ.get(ENV_REGION, "").strip()}


def _aws_authenticated() -> bool:
    """True when `deadline auth status` reports the API reachable."""
    try:
        out = subprocess.run(
            ["deadline", "auth", "status"], capture_output=True, text=True, timeout=30
        ).stdout
    except (OSError, subprocess.TimeoutExpired):
        return False
    return '"api_availability": true' in out or "API Availability: True" in out


def _real_aws_skip_reason(allow_real_aws: bool) -> Optional[str]:
    """Why a real_aws case should be skipped, or None if it's clear to run.

    Skipping (rather than failing) keeps a default `run` green: real_aws cases are
    opt-in, submit billable jobs, and need live auth + a configured sandbox farm.
    """
    if not allow_real_aws:
        return "requires --allow-real-aws (submits real, billable jobs)"
    try:
        _real_aws_context()
    except RealAwsConfigError as e:
        return str(e)
    if not _aws_authenticated():
        return "deadline auth status is not authenticated; run `deadline auth login`"
    return None


# Telemetry shape for a run that never produced a result (harness error). Mirrors
# RunResult.telemetry_dict so aggregation treats it like any other failed run.
_EMPTY_TELEMETRY = {
    "success": False,
    "subtype": "harness_error",
    "tool_calls": [],
    "tool_call_count": 0,
    "num_turns": 0,
    "total_cost_usd": 0.0,
    "duration_ms": 0,
    "final_text": "",
}


def _write_failed_run(run_dir: Path, detail: str) -> None:
    """Persist a harness-error run so its artifact exists alongside the others."""
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "result.json").write_text(
        json.dumps({**_EMPTY_TELEMETRY, "passed": False, "detail": detail}, indent=2)
    )


def _safe_write(base: Path, rel: str, content: str) -> None:
    """Write content to base/rel, creating parent dirs. Rejects absolute paths and
    '..' segments so a material/subject key can never write outside the sandbox."""
    rel_path = Path(rel)
    if rel_path.is_absolute() or ".." in rel_path.parts:
        raise ValueError(f"unsafe sandbox path: {rel!r}")
    dest = base / rel_path
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(content)


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


def _run_case(
    case: dict, run_dir: Path, model: Optional[str], subject_files=None, aws_ctx=None
) -> dict:
    """One agent run + judge verdict; artifacts under run_dir.

    `aws_ctx` (farm_id/queue_id/region) is filled into {placeholders} in the prompt
    and rubric for real_aws cases; None for offline/mock cases.
    """
    fmt = dict(aws_ctx or {})
    prompt = case["prompt"].format(**fmt) if fmt else case["prompt"]
    rubric = case["rubric"].format(**fmt) if fmt else case["rubric"]
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
        if subject_files:
            # Seed the subject's owned files (docs, etc.) into the sandbox. Without
            # this, a read-material A/B would compare two identical sandboxes: the
            # ref swap happens in the repo checkout the sandboxed agent can't see.
            for rel, content in subject_files.items():
                _safe_write(workdir / "subject", rel, content)

        try:
            result = run_agent(
                prompt,
                workdir,
                case.get("tools", DEFAULT_TOOLS),
                max_turns=case.get("max_turns", 20),
                model=model,
            )
        except HarnessError as e:
            # A run that could not launch or timed out fails just this run -- it must
            # not abort the batch, and must not be scored as a pass.
            _write_failed_run(run_dir, f"harness error: {e}")
            print(f"    run: FAIL (harness error) -- {e}")
            return {**_EMPTY_TELEMETRY, "passed": False, "detail": f"harness error: {e}"}
    finally:
        shutil.rmtree(workdir, ignore_errors=True)

    try:
        verdict = judge.judge_answer(rubric, prompt, result.final_text, model=model)
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


def _deadline_provenance() -> str:
    """A one-line description of the `deadline` the agent will drive: path, version,
    and whether it's an editable install (points at a source checkout). Surfaced at
    startup so an operator never unknowingly evaluates a stale or fork-shadowed CLI
    -- the 'editable install shadows your real deadline' footgun."""
    path = shutil.which("deadline")
    if not path:
        return "deadline: NOT ON PATH (agent runs that need it will fail)"
    try:
        version = subprocess.run(
            [path, "--version"], capture_output=True, text=True, timeout=30
        ).stdout.strip()
    except (OSError, subprocess.TimeoutExpired):
        version = "version unknown"
    editable = ""
    try:
        # A console-script shebang names the interpreter; ask pip where the package
        # lives and whether it's an editable (source-checkout) install.
        interp = Path(path).read_text().splitlines()[0].lstrip("#!").strip()
        show = subprocess.run(
            [interp, "-m", "pip", "show", "deadline"], capture_output=True, text=True, timeout=30
        ).stdout
        for line in show.splitlines():
            if line.lower().startswith("editable project location"):
                editable = f" [EDITABLE -> {line.split(':', 1)[1].strip()}]"
    except (OSError, subprocess.TimeoutExpired, IndexError, ValueError):
        # Provenance is a diagnostic, never a gate: if the launcher isn't a readable
        # text stub (a native `deadline.exe` on Windows raises UnicodeDecodeError, a
        # ValueError subclass) or pip can't be reached, report without the editable
        # detail rather than aborting the run before any eval executes.
        pass
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


def _cmd_run(args: argparse.Namespace) -> int:
    cases = json.loads(Path(args.eval_file).read_text())
    if not isinstance(cases, list):
        print("ERROR: eval file must be a JSON array of cases.")
        return 2

    if args.seed_subject and not args.revised_ref:
        print("ERROR: --seed-subject only applies in A/B mode; pass --revised-ref too.")
        return 2

    # Always report which deadline CLI the agent will drive -- guards against
    # silently evaluating a stale or fork-shadowed editable install.
    print(f"[env] {_deadline_provenance()}")

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

            # real_aws cases submit real, billable jobs: skip (don't fail) unless the
            # operator opted in, the farm/queue env vars are set, and auth is live.
            # Skipping keeps a plain `run` green in CI while these are opt-in only.
            aws_ctx = None
            if case.get("env") == REAL_AWS_ENV:
                skip = _real_aws_skip_reason(args.allow_real_aws)
                if skip:
                    print(f"\n=== {case['id']} === SKIPPED (real_aws): {skip}")
                    summaries.append({"case_id": case["id"], "skipped": skip})
                    continue
                aws_ctx = _real_aws_context()

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
                        aws_ctx,
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
        # The diff is identical across A/B cases, so take it from any case that has
        # one -- summaries[0] may be a skipped real_aws case with no source_diff,
        # which would silently suppress a genuinely earned proposal.
        diff = next((s["source_diff"] for s in summaries if s.get("source_diff")), "")
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
    run.add_argument(
        "--allow-real-aws",
        action="store_true",
        help=f'opt in to running cases with "env": "{REAL_AWS_ENV}", which submit '
        f"REAL, BILLABLE jobs. Requires {ENV_FARM_ID}/{ENV_QUEUE_ID} (optionally "
        f"{ENV_REGION}) set to a non-production sandbox you own, and `deadline auth "
        "login`. Without this flag such cases are skipped.",
    )

    args = ap.parse_args(argv)
    if args.cmd == "run":
        return _cmd_run(args)
    ap.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
