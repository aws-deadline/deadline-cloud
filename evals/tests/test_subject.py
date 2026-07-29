# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Subject git mechanics on real temp repos (no agent calls)."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from agent_evals.subject import (  # noqa: E402
    BASE_REF,
    CORPUS_DOC,
    Subject,
    SubjectError,
    corpus_subject,
)


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


def _two_dir_repo(tmp_path: Path) -> Path:
    """A committed repo with src/ and docs/ so pathspec scoping can be exercised."""
    repo = tmp_path / "repo"
    (repo / "src").mkdir(parents=True)
    (repo / "docs").mkdir()
    (repo / "src" / "a.py").write_text("orig code\n")
    (repo / "docs" / "b.md").write_text("orig doc\n")
    subprocess.run(["git", "-C", str(repo), "init", "-q"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.email", "t@example.com"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.name", "t"], check=True)
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-q", "-m", "init"], check=True)
    return repo


def test_reset_clean_preserves_work_outside_pathspec(tmp_path: Path) -> None:
    # reset_clean is scoped: an operator A/Bing `src` while carrying uncommitted
    # edits to docs/ must NOT have those edits destroyed (the bug an unscoped
    # `git reset --hard HEAD` would cause).
    repo = _two_dir_repo(tmp_path)
    subj = Subject(root=repo, diff_pathspec="src")
    (repo / "src" / "a.py").write_text("variant edit\n")
    (repo / "src" / "extra.py").write_text("untracked variant file\n")
    (repo / "docs" / "b.md").write_text("OPERATOR WIP\n")

    subj.reset_clean()

    assert (repo / "src" / "a.py").read_text() == "orig code\n"  # tracked edit reverted
    assert not (repo / "src" / "extra.py").exists()  # untracked removed
    assert (repo / "docs" / "b.md").read_text() == "OPERATOR WIP\n"  # out-of-pathspec preserved


def test_reset_clean_reverts_staged_edits_in_pathspec(tmp_path: Path) -> None:
    # A staged (git add'd) edit under the pathspec must also be reverted.
    repo = _two_dir_repo(tmp_path)
    subj = Subject(root=repo, diff_pathspec="src")
    (repo / "src" / "a.py").write_text("staged edit\n")
    subprocess.run(["git", "-C", str(repo), "add", "src/a.py"], check=True)

    subj.reset_clean()

    assert (repo / "src" / "a.py").read_text() == "orig code\n"
    staged = subprocess.run(
        ["git", "-C", str(repo), "diff", "--cached", "--name-only"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    assert staged == ""


def test_checkout_raises_on_missing_ref(tmp_path: Path) -> None:
    # A checkout that silently fails would leave the "revised" variant running
    # baseline source, producing a plausible-looking but meaningless A/B result.
    subj = corpus_subject("# Guide\n", tmp_path / "corpus")
    with pytest.raises(SubjectError, match="checkout"):
        subj.checkout("no-such-ref")


def test_diff_refs_raises_on_missing_ref(tmp_path: Path) -> None:
    subj = corpus_subject("# Guide\n", tmp_path / "corpus")
    with pytest.raises(SubjectError):
        subj.diff_refs(BASE_REF, "no-such-ref")


def test_assert_clean_refuses_dirty_tree(tmp_path: Path) -> None:
    # Uncommitted operator work under the pathspec must never be discarded.
    subj = corpus_subject("# Guide\noriginal\n", tmp_path / "corpus")
    (subj.root / CORPUS_DOC).write_text("uncommitted operator edit\n")
    with pytest.raises(SubjectError, match="uncommitted"):
        subj.assert_clean()


def test_assert_clean_flags_untracked_files(tmp_path: Path) -> None:
    # git clean -fd would delete untracked files with no prompt; assert_clean must
    # catch them, not just tracked modifications.
    subj = corpus_subject("# Guide\n", tmp_path / "corpus")
    (subj.root / "wip_notes.md").write_text("not yet committed\n")
    with pytest.raises(SubjectError, match="uncommitted"):
        subj.assert_clean()


def test_assert_clean_passes_on_clean_tree(tmp_path: Path) -> None:
    subj = corpus_subject("# Guide\n", tmp_path / "corpus")
    subj.assert_clean()  # must not raise


def test_assert_clean_ignores_dirt_outside_pathspec(tmp_path: Path) -> None:
    # Dirt outside the owned paths is none of our business -- the destructive
    # clean is scoped to diff_pathspec, so the guard is too.
    subj = corpus_subject("# Guide\n", tmp_path / "corpus")  # owns only *.md
    (subj.root / "scratch.txt").write_text("non-markdown dirt\n")
    subj.assert_clean()  # must not raise
