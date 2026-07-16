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
| This repo's docs | A/B with `--pathspec ':(glob)docs/**/*.md'` |

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
python -m agent_evals.runner run my_docs_eval.json --revised-ref docs-fix --pathspec ':(glob)docs/**/*.md'
```

Artifacts land under `evals/output/<eval>/<timestamp>/`: per-run telemetry and
transcripts, `summary.json`, and — when a revision measurably improved an eval —
`proposal.patch`. Exit code 4 means the revision regressed.

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
    "k": 3
  }
]
```

The rubric is the only per-eval authoring step that matters: it should state the
*material's own* success criterion (for a docs page, what the page promises the
reader can do), including what a correct answer looks like when the evidence is
incomplete — a good judge passes an agent that refuses to invent missing details.

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
