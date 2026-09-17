# Agent evals

Measure how well an AI agent achieves goals with Deadline Cloud tooling and
docs — and prove, with a before-and-after comparison, that a change to the CLI,
the docs, or any reference material actually helps.

## How it works

1. An **eval file** (JSON) gives an agent a goal, the tools it may use, optional
   reference material, and a **rubric** describing what a passing answer must do.
2. The **runner** launches an isolated headless agent (`claude -p`) per run in a
   throwaway sandbox and captures telemetry (tool calls, turns, cost).
3. An **LLM judge** grades the agent's final answer against the rubric.
4. In **A/B mode**, every case runs twice — with this repo at the current ref
   (baseline) and at `--revised-ref` (candidate) — and the summary reports the
   paired delta plus the diff, which is the PR-ready proposed change.

The material under test can be almost anything the agent relies on:

| Data source | How |
| --- | --- |
| `deadline` CLI | `pip install -e .` this repo; A/B two git refs of `src/` |
| AWS CLI usage | goal + rubric only — no subject needed |
| AWS documentation / blog / web page | fetch it to markdown, pass as `materials`, seed a corpus to revise |
| This repo's docs | A/B with `--pathspec ':(glob)docs/**/*.md' --seed-subject` |
| Real Deadline Cloud (submit a job) | `"env": "real_aws"` case + `--allow-real-aws` and sandbox env vars (see below) |

## Setup

```bash
pip install -e .          # the agent's `deadline` is this checkout (A/B needs this)
which claude              # Claude Code must be on PATH and authenticated
```

That's it — no extra dependencies; the evals use only the Python standard library.

## Run

```bash
cd evals

# score how an agent does today (baseline only)
python -m agent_evals.runner run examples/deadline_cli.json --k 3

# A/B a change: current branch vs a revision of the CLI source
python -m agent_evals.runner run examples/deadline_cli.json --k 3 --revised-ref my-improvement

# A/B a docs change instead of code
# --seed-subject copies the owned files into each run's sandbox (under subject/) so
# the agent actually reads them; without it a docs A/B compares identical sandboxes.
python -m agent_evals.runner run my_docs_eval.json --revised-ref docs-fix \
    --pathspec ':(glob)docs/**/*.md' --seed-subject
```

Flags:

| Flag | Effect |
| --- | --- |
| `--k N` | runs per case per variant (default 1); a case's own `k` overrides it |
| `--model ALIAS` | model for both the agent and the judge |
| `--revised-ref REF` | enable A/B mode: also run with the repo at this git ref |
| `--base-ref REF` | baseline ref for A/B (default: current branch) |
| `--pathspec SPEC` | repo paths the A/B owns, e.g. `src` or `:(glob)docs/**/*.md` |
| `--seed-subject` | copy the subject's owned files into each sandbox (docs A/B — see above) |
| `--allow-real-aws` | opt in to `real_aws` cases, which submit real jobs (see below) |

Artifacts land under `evals/output/<eval>/<timestamp>/`: per-run telemetry and
transcripts, `summary.json`, and — when a revision measurably improved an eval —
`proposal.patch`. Exit code 4 means the revision regressed.

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

The case's `prompt`/`rubric` may reference `{farm_id}`, `{queue_id}`, and `{region}`,
which are filled from those env vars. Missing vars or expired auth skip the case
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

Fields: `id`, `prompt`, and `rubric` are required; the rest are optional.

| Field | Meaning |
| --- | --- |
| `tools` | Claude Code tools the agent may use (default: `Bash`, `Read`, `Write`, `Edit`) |
| `materials` | `{path: content}` written under `materials/` in the sandbox and named in the prompt; keys may contain `/` but not `..` or absolute paths |
| `max_turns` | agent turn cap (default 20) |
| `k` | runs per variant for this case (overrides the `--k` flag) |
| `env` | set to `real_aws` to mark a case that submits real jobs — see above; omit for ordinary offline cases |

The rubric is the only per-eval authoring step that matters: it should state the
*material's own* success criterion (for a docs page, what the page promises the
reader can do), including what a correct answer looks like when the evidence is
incomplete — a good judge passes an agent that refuses to invent missing details.
For `real_aws` cases, `prompt` and `rubric` may use `{farm_id}` / `{queue_id}` /
`{region}` placeholders, filled from the environment variables above.

## Close the loop automatically

`reviser.revise()` hands a struggling run's transcript to an agent that edits the
subject (code or docs), commits to a scratch ref, and returns it — feed that ref
back to `--revised-ref` to A/B-prove the improvement:

```python
from agent_evals import reviser, subject

subj = subject.repo_subject()                       # or corpus_subject(markdown)
ref = reviser.revise(subj, run_dir, goal="...", base_ref="mainline")
# python -m agent_evals.runner run my_eval.json --revised-ref <ref>
```

## Notes

- The tested agent always runs isolated — the orchestrating session must never do
  the goal itself, or the telemetry measures the wrong thing.
- Runs that talk to real AWS use whatever credentials/config the environment has;
  point them at a non-production sandbox account.
- The judge is a single vote per run; for gate-quality decisions increase `k`.
