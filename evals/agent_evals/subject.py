# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""The subject under test: a git checkout plus the pathspec an A/B owns.

Baseline and candidate are the owned paths at two refs; their diff is the
proposed change.
  - repo_subject(): this checkout (CLI source or repo docs). Needs `pip install -e .`
    so the agent's `deadline` is this checkout.
  - corpus_subject(markdown): fetched material seeded into a throwaway repo, for
    revising with reviser.revise().
"""

from __future__ import annotations

import shutil
import subprocess
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional

BASE_REF = "eval-base"
CORPUS_DOC = "source.md"

# Scratch commits must not depend on, or run, the operator's git identity,
# hooks, or signing setup.
_SCRATCH_COMMIT = (
    "-c",
    "user.name=agent-evals",
    "-c",
    "user.email=agent-evals@amazon.com",
    "commit",
    "-q",
    "--no-verify",
    "--no-gpg-sign",
)


class SubjectError(RuntimeError):
    """Raised when a git operation on the subject fails or would lose work."""


@dataclass
class Subject:
    root: Path
    pathspec: str = "."

    def _git(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["git", "-C", str(self.root), *args],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )

    def _git_checked(self, *args: str) -> subprocess.CompletedProcess:
        proc = self._git(*args)
        if proc.returncode != 0:
            raise SubjectError(
                f"git {' '.join(args)} failed in {self.root}: {proc.stderr.strip()[:300]}"
            )
        return proc

    def resolve(self, ref: str) -> str:
        """The commit SHA for `ref`."""
        return self._git_checked(
            "rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}"
        ).stdout.strip()

    def assert_clean(self) -> None:
        """Raise if the owned paths have uncommitted changes, which checkout() would discard."""
        status = self._git_checked("status", "--porcelain", "--", self.pathspec).stdout.strip()
        if status:
            raise SubjectError(
                f"{self.root} has uncommitted changes under '{self.pathspec}':\n"
                f"{status[:500]}\nCommit or stash them first."
            )

    def checkout(self, ref: str) -> None:
        """Make the owned paths match `ref`, leaving HEAD and other paths alone."""
        self.reset_clean()
        self._restore(ref)

    def reset_clean(self) -> None:
        """Return the owned paths to HEAD, removing untracked files under them."""
        self._restore("HEAD")
        self._git_checked("clean", "-fdq", "--", self.pathspec)

    def _restore(self, ref: str) -> None:
        self._git_checked(
            "restore", f"--source={ref}", "--staged", "--worktree", "--", self.pathspec
        )

    def owned_files(self) -> dict:
        """{relative_path: content} for the owned files as currently checked out."""
        listing = self._git_checked("ls-files", "-z", "--", self.pathspec).stdout
        files = {}
        for rel in filter(None, listing.split("\0")):
            path = self.root / rel
            if path.is_file():
                files[rel] = path.read_text(encoding="utf-8", errors="replace")
        return files

    def capture_diff(self) -> str:
        """Uncommitted edits to the owned paths, new files included."""
        self._git("add", "-N", "--", self.pathspec)
        return self._git("diff", "--", self.pathspec).stdout

    def diff_refs(self, base: str, revised: str) -> str:
        return self._git_checked("diff", f"{base}..{revised}", "--", self.pathspec).stdout

    def commit_scratch(self, ref: str, message: str) -> str:
        """Commit only the owned paths to a new branch `ref` and return it."""
        self._git_checked("checkout", "-q", "-b", ref)
        self._git_checked("add", "--", self.pathspec)
        self._git_checked(*_SCRATCH_COMMIT, "-m", message)
        return ref

    @contextmanager
    def scratch_worktree(self, ref: str) -> Iterator["Subject"]:
        """A throwaway detached worktree at `ref`, so an editing agent never touches
        the operator's checkout."""
        path = Path(tempfile.mkdtemp(prefix="eval-worktree-"))
        self._git_checked("worktree", "add", "-q", "--detach", str(path), ref)
        try:
            yield Subject(root=path, pathspec=self.pathspec)
        finally:
            self._git("worktree", "remove", "--force", str(path))
            shutil.rmtree(path, ignore_errors=True)


def repo_subject(pathspec: str = "src") -> Subject:
    """This checkout as the subject. Use ":(glob)docs/**/*.md" for the repo docs."""
    return Subject(root=Path(__file__).resolve().parents[2], pathspec=pathspec)


def corpus_subject(markdown: str, dest: Optional[Path] = None) -> Subject:
    """Seed fetched material into a git repo on BASE_REF."""
    root = Path(dest) if dest else Path(tempfile.mkdtemp(prefix="eval-corpus-"))
    root.mkdir(parents=True, exist_ok=True)
    if any(root.iterdir()) and not (root / ".git").exists():
        raise SubjectError(f"{root} is non-empty and not a git repo; refusing to seed over it.")

    subj = Subject(root=root, pathspec=":(glob)**/*.md")
    if not (root / ".git").exists():
        subj._git_checked("init", "-q")
    subj._git_checked("checkout", "-q", "-B", BASE_REF)
    (root / CORPUS_DOC).write_text(markdown, encoding="utf-8")
    subj._git_checked("add", CORPUS_DOC)
    subj._git_checked(*_SCRATCH_COMMIT, "-m", "seed corpus from fetched material")
    return subj
