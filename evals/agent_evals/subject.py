# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""The subject under test: the material an eval A/B-compares and improves.

A Subject is any git checkout whose revisions can be A/B-tested: baseline = the
subject at one ref, candidate = the subject at another (usually an agent's edits
committed to a scratch ref). The diff between them, scoped to the paths the
subject owns, is the PR-ready proposed improvement.

Two ready-made subjects cover the common cases:
  - repo_subject(): this deadline-cloud checkout itself -- A/B changes to the CLI
    source or the repo docs. Requires an editable install (`pip install -e .`) so
    the `deadline` the agent drives IS this checkout.
  - corpus_subject(markdown): any fetched material (an AWS docs page, a blog post,
    a web-search result) seeded into a throwaway git repo so it can be diffed and
    revised like everything else.
"""

from __future__ import annotations

import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

BASE_REF = "eval-base"

# The doc filename used when seeding fetched material into a corpus.
CORPUS_DOC = "source.md"


class SubjectError(RuntimeError):
    """Raised when a git operation on the subject fails or would lose work."""


@dataclass
class Subject:
    """A git checkout + the pathspec its evals own (scopes diffs and cleanup)."""

    root: Path
    diff_pathspec: str = "."

    def _git(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(["git", "-C", str(self.root), *args], capture_output=True, text=True)

    def _git_checked(self, *args: str) -> subprocess.CompletedProcess:
        """Like _git, but raises on failure. Used where a silent git failure would
        corrupt the experiment -- e.g. a checkout that never happened leaves the
        'revised' variant running baseline source and reporting a bogus no-delta."""
        proc = self._git(*args)
        if proc.returncode != 0:
            raise SubjectError(
                f"git {' '.join(args)} failed in {self.root}: {proc.stderr.strip()[:300]}"
            )
        return proc

    def assert_clean(self) -> None:
        """Refuse to operate on a checkout with uncommitted work under the owned paths.

        checkout()/reset_clean() discard working-tree changes under the pathspec by
        design (one variant must not bleed into the next) -- but the operator's own
        uncommitted work must never be what gets discarded. Callers invoke this once
        before the first destructive operation; a dirty tree is the operator's to
        stash or commit, not ours to delete.
        """
        status = self._git_checked("status", "--porcelain", "--", self.diff_pathspec).stdout.strip()
        if status:
            raise SubjectError(
                f"the checkout at {self.root} has uncommitted changes under "
                f"'{self.diff_pathspec}':\n{status[:500]}\n"
                "Commit or stash them first -- running evals would discard them."
            )

    def checkout(self, ref: str) -> None:
        """Put the checkout on `ref`, discarding working-tree edits first."""
        self.reset_clean()
        self._git_checked("checkout", "-q", ref)

    def reset_clean(self) -> None:
        """Discard uncommitted edits so one variant never bleeds into the next."""
        self._git("reset", "--hard", "HEAD")
        self._git("clean", "-fd", self.diff_pathspec)

    def capture_diff(self) -> str:
        """Uncommitted edits to the owned paths (including new files) as a unified
        diff -- the candidate's proposed change."""
        self._git("add", "-N", self.diff_pathspec)
        return self._git("diff", "--", self.diff_pathspec).stdout

    def diff_refs(self, base: str, revised: str) -> str:
        """The committed change a candidate ref is testing, as a unified diff."""
        return self._git_checked("diff", f"{base}..{revised}", "--", self.diff_pathspec).stdout

    def commit_scratch(self, ref: str, message: str) -> str:
        """Commit ONLY the owned paths to a fresh branch `ref` (recreated if it
        exists) and return the ref. Scoped so stray edits elsewhere never land in a
        proposal."""
        self._git_checked("checkout", "-B", ref)
        self._git_checked("add", self.diff_pathspec)
        self._git_checked("commit", "-m", message)
        return ref


def repo_subject(pathspec: str = "src") -> Subject:
    """This deadline-cloud checkout as the subject (default: the CLI source).

    Pass pathspec=":(glob)docs/**/*.md" to A/B the repo docs instead.
    """
    root = Path(__file__).resolve().parents[2]
    return Subject(root=root, diff_pathspec=pathspec)


def corpus_subject(markdown: str, dest: "Path | None" = None) -> Subject:
    """Seed fetched material (docs page, blog post, search result) into a git repo
    so it can be A/B-tested and revised. Returns the ready Subject on BASE_REF."""
    root = Path(dest) if dest else Path(tempfile.mkdtemp(prefix="eval-corpus-"))
    root.mkdir(parents=True, exist_ok=True)
    if any(root.iterdir()) and not (root / ".git").exists():
        raise RuntimeError(f"{root} is non-empty and not a git repo; refusing to seed over it.")

    subj = Subject(root=root, diff_pathspec=":(glob)**/*.md")
    if not (root / ".git").exists():
        subj._git("init", "-q")
        # Local identity so the commit works without global git config.
        subj._git("config", "user.email", "agent-evals@amazon.com")
        subj._git("config", "user.name", "agent-evals")

    subj._git("checkout", "-q", "-B", BASE_REF)
    (root / CORPUS_DOC).write_text(markdown)
    subj._git("add", CORPUS_DOC)
    subj._git("commit", "-q", "-m", "seed corpus from fetched material")
    return subj
