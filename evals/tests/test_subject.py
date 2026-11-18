# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Subject git mechanics on real temp repos (no agent calls)."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from agent_evals.subject import BASE_REF, CORPUS_DOC, Subject, SubjectError, corpus_subject


def _git_out(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, text=True, check=True
    ).stdout


@pytest.fixture
def corpus(tmp_path: Path) -> Subject:
    return corpus_subject("# Guide\noriginal\n", tmp_path / "corpus")


@pytest.fixture
def two_dir_repo(tmp_path: Path) -> Path:
    """A committed repo with src/ and docs/, for pathspec scoping."""
    repo = tmp_path / "repo"
    (repo / "src").mkdir(parents=True)
    (repo / "docs").mkdir()
    (repo / "src" / "a.py").write_text("orig code\n")
    (repo / "docs" / "b.md").write_text("orig doc\n")
    git = ["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@example.com"]
    subprocess.run([*git, "init", "-q"], check=True)
    subprocess.run([*git, "add", "-A"], check=True)
    subprocess.run([*git, "commit", "-q", "-m", "init"], check=True)
    return repo


def test_corpus_subject_seeds_committed_repo(corpus: Subject) -> None:
    assert (corpus.root / CORPUS_DOC).read_text().startswith("# Guide")
    assert _git_out(corpus.root, "rev-parse", "--abbrev-ref", "HEAD").strip() == BASE_REF


def test_corpus_subject_refuses_non_git_dir(tmp_path: Path) -> None:
    dest = tmp_path / "corpus"
    dest.mkdir()
    (dest / "keep.txt").write_text("do not clobber")
    with pytest.raises(SubjectError):
        corpus_subject("# New\n", dest)


def test_capture_diff_scopes_to_pathspec(corpus: Subject) -> None:
    (corpus.root / CORPUS_DOC).write_text("# Guide\nimproved\n")
    (corpus.root / "notes.txt").write_text("not markdown")
    diff = corpus.capture_diff()
    assert "improved" in diff
    assert "notes.txt" not in diff


def test_scratch_commit_and_diff_refs(corpus: Subject) -> None:
    (corpus.root / CORPUS_DOC).write_text("# Guide\nrevised\n")
    ref = corpus.commit_scratch("eval-revise-test", "test edit")
    corpus.checkout(BASE_REF)
    assert "revised" in corpus.diff_refs(BASE_REF, ref)


def test_commit_scratch_refuses_existing_branch(corpus: Subject) -> None:
    corpus._git_checked("branch", "taken")
    (corpus.root / CORPUS_DOC).write_text("edit\n")
    with pytest.raises(SubjectError):
        corpus.commit_scratch("taken", "would overwrite")


def test_scratch_commit_ignores_operator_git_setup(corpus: Subject, monkeypatch) -> None:
    hooks = corpus.root / "hooks"
    hooks.mkdir()
    (hooks / "pre-commit").write_text("#!/bin/sh\nexit 1\n")
    (hooks / "pre-commit").chmod(0o755)
    corpus._git_checked("config", "core.hooksPath", str(hooks))
    corpus._git_checked("config", "commit.gpgsign", "true")
    corpus._git_checked("config", "gpg.program", "definitely-not-gpg")
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(corpus.root / "no-global-config"))
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")

    (corpus.root / CORPUS_DOC).write_text("edit\n")
    corpus.commit_scratch("eval-revise-test", "scratch")
    assert _git_out(corpus.root, "log", "-1", "--format=%an").strip() == "agent-evals"


def test_checkout_is_scoped_to_pathspec(corpus: Subject) -> None:
    (corpus.root / CORPUS_DOC).write_text("# Guide\nrevised\n")
    (corpus.root / "added.md").write_text("new page\n")
    ref = corpus.commit_scratch("eval-revise-test", "test edit")
    corpus._git_checked("checkout", "-q", BASE_REF)
    (corpus.root / "notes.txt").write_text("operator work")

    corpus.checkout(ref)
    assert "revised" in (corpus.root / CORPUS_DOC).read_text()
    assert (corpus.root / "added.md").exists()
    assert _git_out(corpus.root, "rev-parse", "--abbrev-ref", "HEAD").strip() == BASE_REF
    assert (corpus.root / "notes.txt").read_text() == "operator work"

    corpus.reset_clean()
    assert "original" in (corpus.root / CORPUS_DOC).read_text()
    assert not (corpus.root / "added.md").exists()
    assert _git_out(corpus.root, "status", "--porcelain", "--", corpus.pathspec) == ""


def test_reset_clean_reverts_pathspec_and_keeps_other_work(two_dir_repo: Path) -> None:
    subj = Subject(root=two_dir_repo, pathspec="src")
    (two_dir_repo / "src" / "a.py").write_text("staged edit\n")
    subprocess.run(["git", "-C", str(two_dir_repo), "add", "src/a.py"], check=True)
    (two_dir_repo / "src" / "extra.py").write_text("untracked\n")
    (two_dir_repo / "docs" / "b.md").write_text("operator wip\n")

    subj.reset_clean()

    assert (two_dir_repo / "src" / "a.py").read_text() == "orig code\n"
    assert not (two_dir_repo / "src" / "extra.py").exists()
    assert _git_out(two_dir_repo, "diff", "--cached", "--name-only").strip() == ""
    assert (two_dir_repo / "docs" / "b.md").read_text() == "operator wip\n"


def test_checkout_raises_on_missing_ref(corpus: Subject) -> None:
    with pytest.raises(SubjectError, match="no-such-ref"):
        corpus.checkout("no-such-ref")


def test_diff_refs_raises_on_missing_ref(corpus: Subject) -> None:
    with pytest.raises(SubjectError):
        corpus.diff_refs(BASE_REF, "no-such-ref")


def test_resolve(corpus: Subject) -> None:
    assert corpus.resolve(BASE_REF) == _git_out(corpus.root, "rev-parse", "HEAD").strip()
    with pytest.raises(SubjectError):
        corpus.resolve("no-such-ref")


@pytest.mark.parametrize(
    "path, content, dirty",
    [
        (CORPUS_DOC, "uncommitted edit\n", True),
        ("wip_notes.md", "untracked\n", True),
        ("scratch.txt", "outside the pathspec\n", False),
        (None, None, False),
    ],
    ids=["tracked_edit", "untracked_file", "outside_pathspec", "clean"],
)
def test_assert_clean(corpus: Subject, path, content, dirty) -> None:
    if path:
        (corpus.root / path).write_text(content)
    if dirty:
        with pytest.raises(SubjectError, match="uncommitted"):
            corpus.assert_clean()
    else:
        corpus.assert_clean()


def test_owned_files_track_checked_out_ref(corpus: Subject) -> None:
    (corpus.root / CORPUS_DOC).write_text("# Guide\nrevised\n")
    (corpus.root / "notes.txt").write_text("not markdown")
    ref = corpus.commit_scratch("revised", "revise")
    corpus._git_checked("checkout", "-q", BASE_REF)
    assert corpus.owned_files() == {CORPUS_DOC: "# Guide\noriginal\n"}

    corpus.checkout(ref)
    assert corpus.owned_files() == {CORPUS_DOC: "# Guide\nrevised\n"}


def test_scratch_worktree_is_removed(corpus: Subject) -> None:
    with corpus.scratch_worktree(BASE_REF) as scratch:
        assert scratch.root != corpus.root
        assert (scratch.root / CORPUS_DOC).exists()
        path = scratch.root
    assert not path.exists()
    assert str(path) not in _git_out(corpus.root, "worktree", "list")
