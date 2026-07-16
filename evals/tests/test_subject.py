# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Subject git mechanics on real temp repos (no agent calls)."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from agent_evals.subject import BASE_REF, CORPUS_DOC, Subject, corpus_subject  # noqa: E402


def _git_out(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, text=True, check=True
    ).stdout


def test_corpus_subject_seeds_committed_repo(tmp_path: Path) -> None:
    subj = corpus_subject("# Guide\nStep 1.\n", tmp_path / "corpus")
    assert (subj.root / CORPUS_DOC).read_text().startswith("# Guide")
    assert _git_out(subj.root, "rev-parse", "--abbrev-ref", "HEAD").strip() == BASE_REF


def test_corpus_subject_refuses_non_git_dir(tmp_path: Path) -> None:
    dest = tmp_path / "corpus"
    dest.mkdir()
    (dest / "keep.txt").write_text("do not clobber")
    with pytest.raises(RuntimeError):
        corpus_subject("# New\n", dest)


def test_capture_diff_scopes_to_pathspec(tmp_path: Path) -> None:
    subj = corpus_subject("# Guide\noriginal\n", tmp_path / "corpus")
    (subj.root / CORPUS_DOC).write_text("# Guide\nimproved\n")
    (subj.root / "notes.txt").write_text("not markdown")
    diff = subj.capture_diff()
    assert "improved" in diff
    assert "notes.txt" not in diff  # pathspec limits proposals to *.md


def test_scratch_commit_and_diff_refs(tmp_path: Path) -> None:
    subj = corpus_subject("# Guide\noriginal\n", tmp_path / "corpus")
    (subj.root / CORPUS_DOC).write_text("# Guide\nrevised\n")
    ref = subj.commit_scratch("eval-revise-test", "test edit")
    subj.checkout(BASE_REF)
    assert "revised" in subj.diff_refs(BASE_REF, ref)
    assert "revised" not in (subj.root / CORPUS_DOC).read_text()  # back on baseline


def test_reset_clean_restores(tmp_path: Path) -> None:
    subj = corpus_subject("# Guide\noriginal\n", tmp_path / "corpus")
    (subj.root / CORPUS_DOC).write_text("broken edit")
    subj.reset_clean()
    assert (subj.root / CORPUS_DOC).read_text() == "# Guide\noriginal\n"
    assert subj.capture_diff() == ""


def test_subject_dataclass_on_plain_repo(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "-C", str(repo), "init", "-q"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.email", "t@example.com"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.name", "t"], check=True)
    (repo / "a.md").write_text("hello\n")
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-q", "-m", "init"], check=True)

    subj = Subject(root=repo, diff_pathspec=".")
    (repo / "a.md").write_text("hello world\n")
    assert "world" in subj.capture_diff()
