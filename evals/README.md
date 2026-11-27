# Agent evals

Measure how well an AI agent achieves goals with Deadline Cloud tooling and
docs — and prove, with a before-and-after comparison, that a change to the CLI,
the docs, or any reference material actually helps.

## How it works

1. An **eval file** (JSON) gives an agent a goal, the tools it may use, optional
   reference material, and a **rubric** describing what a passing answer must do.
2. The **runner** launches a separate headless agent (`claude -p`) per run in a
   throwaway working directory, limited to the case's tools, and captures
   telemetry (tool calls, turns, cost).
3. An **LLM judge** grades the agent's final answer against the rubric.
4. In **A/B mode**, every case runs twice — with this repo at the current ref
   (baseline) and at `--revised-ref` (candidate) — and the summary reports the
   paired delta plus the diff, which is the PR-ready proposed change.

The material under test can be almost anything the agent relies on:

| Data source | How |
| --- | --- |
| `deadline` CLI | `pip install -e .` this repo; A/B two git refs of `src/` |
| AWS CLI usage | goal + rubric only — no subject needed |
| AWS documentation / blog / web page | fetch it to markdown and pass it as `materials` |
| This repo's docs | A/B with `--pathspec ':(glob)docs/**/*.md' --seed-subject` |
| Real Deadline Cloud (submit a job) | `"env": "real_aws"` case + `--allow-real-aws` and sandbox env vars (see below) |

## Setup

```bash
pip install -e .          # the agent's `deadline` is this checkout (A/B needs this)
which claude              # Claude Code must be on PATH and authenticated
```

The evals use only the Python standard library.

## Run

```bash
cd evals

# score how an agent does today (baseline only)
python -m agent_evals.runner run examples/deadline_cli.json --k 3

# A/B a change: HEAD vs a revision of the CLI source
python -m agent_evals.runner run examples/deadline_cli.json --k 3 --revised-ref my-improvement

# A/B a docs change. The agent works in its own directory, so --seed-subject copies
# the owned files into each run's directory; without it both variants match.
python -m agent_evals.runner run my_docs_eval.json --revised-ref docs-fix \
    --pathspec ':(glob)docs/**/*.md' --seed-subject
```

Flags:

| Flag | Effect |
| --- | --- |
| `--k N` | runs per case per variant (default 1); a case's own `k` overrides it |
| `--model ALIAS` | model for both the agent and the judge |
| `--revised-ref REF` | enable A/B mode: also run with the repo at this git ref |
| `--base-ref REF` | baseline ref for A/B (default: HEAD) |
| `--pathspec SPEC` | repo paths the A/B owns, e.g. `src` or `:(glob)docs/**/*.md` |
| `--seed-subject` | copy the owned files into each run's directory under `subject/` (docs A/B) |
| `--allow-real-aws` | opt in to `real_aws` cases, which submit real jobs (see below) |

Artifacts land under `evals/output/<eval>/<timestamp>/`: per-run telemetry and
transcripts, `summary.json`, and `proposal.patch` when the revision improved at
least one eval at `k >= 2` and regressed none. Exit code 4 means it regressed.
A run counts as a pass only if the agent finished without error and the judge
passed its answer.

## Real-AWS evals (submit real jobs)

A case with `"env": "real_aws"` (see `examples/real_aws_submit.json`) has the agent
submit a real job bundle to a real farm and confirm it reaches SUCCEEDED — a true
end-to-end check. Because these submit **real, billable jobs**, they are opt-in and
never name an account in the eval file:

- They are **skipped** (not failed) unless you pass `--allow-real-aws`, so a plain
  `run` stays green in CI.
- The farm/queue come from environment variables — set them to a **non-production
  sandbox you own**:

```bash
export DEADLINE_EVAL_FARM_ID=farm-...
export DEADLINE_EVAL_QUEUE_ID=queue-...
export DEADLINE_EVAL_REGION=us-west-2        # optional
deadline auth login                          # the runner prechecks auth
python -m agent_evals.runner run examples/real_aws_submit.json --allow-real-aws
```

`prompt` and `rubric` may use `{farm_id}`, `{queue_id}`, and `{region}`, filled from
those variables. `DEADLINE_EVAL_REGION` is also exported to the
agent as `AWS_REGION`/`AWS_DEFAULT_REGION`; a `defaults.farm_region` in the agent's
deadline config still takes precedence. Missing vars or expired auth skip the case
with a clear reason rather than submitting to the wrong place.

## Write an eval

```json
[
  {
    "id": "my_case",
    "prompt": "The goal, stated imperatively and self-contained.",
    "tools": ["Bash", "Read"],
    "rubric": "What a passing answer must do, in plain language.",
    "materials": {"guide.md": "optional reference text the agent can read"},
    "max_turns": 20,
    "k": 3,
    "env": "real_aws"
  }
]
```

`id`, `prompt`, and `rubric` are required. `id` may use letters, digits, `_`, `.`,
and `-`.

| Field | Meaning |
| --- | --- |
| `tools` | the only Claude Code tools the agent gets; `[]` for none (default: `Bash`, `Read`, `Write`, `Edit`) |
| `materials` | `{path: content}` written under `materials/` and named in the prompt; keys may contain `/` but not `..` or absolute paths |
| `max_turns` | agent turn cap (default 20) |
| `k` | runs per variant for this case (overrides the `--k` flag) |
| `env` | `real_aws` for a case that submits real jobs (see above) |

The rubric is the only per-eval authoring step that matters: it should state the
*material's own* success criterion (for a docs page, what the page promises the
reader can do), including what a correct answer looks like when the evidence is
incomplete — a good judge passes an agent that refuses to invent missing details.

## Propose a revision

`reviser.revise()` gives a struggling run's transcript to an agent that edits this
checkout's owned paths in a throwaway `git worktree` and commits the edit to a new
`eval-revise-*` branch. Pass that branch to `--revised-ref` to measure it:

```python
from pathlib import Path

from agent_evals import reviser, subject

run_dir = Path("output/deadline_cli/<timestamp>/<case>/baseline/run-1")
ref = reviser.revise(subject.repo_subject(), run_dir, goal="...", base_ref="mainline")
# python -m agent_evals.runner run examples/deadline_cli.json --k 3 --revised-ref <ref>
```

## Notes

- The tested agent always runs as its own process; the orchestrating session must
  never do the goal itself, or the telemetry measures the wrong thing.
- The run directory is not a sandbox. Agents run with `bypassPermissions` as your
  user, with your filesystem and AWS credentials, so eval files and materials are
  trusted input. Run fetched material you don't trust inside a container or VM.
- A/B mode swaps only the files under `--pathspec` and restores them to HEAD at the
  end, so it never touches your branch or work outside the pathspec.
- Unit tests in `evals/tests/` run with the repo suite (`hatch run test`).
- The judge is a single vote per run; for gate-quality decisions increase `k`.
