# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import importlib.util
import sys
from pathlib import Path

from PyInstaller.archive.writers import CArchiveWriter, ZlibArchiveWriter
import pytest


pytestmark = pytest.mark.skipif(
    sys.version_info < (3, 10), reason="Installer allowlist requires sys.stdlib_module_names"
)


@pytest.fixture
def validator(monkeypatch):
    directory = Path(__file__).absolute().parents[2] / "scripts/pyinstaller"
    monkeypatch.syspath_prepend(str(directory))
    spec = importlib.util.spec_from_file_location("_installer_validator", directory / "validate.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    return module


def _make_pyz(tmp_path, namespace="deadline"):
    archive = tmp_path / "test.pyz"
    code = compile("raise RuntimeError('archive code must not execute')", "client.py", "exec")
    ZlibArchiveWriter(
        str(archive),
        [(namespace, None, "PYMODULE"), ("deadline.client", "client.py", "PYMODULE")],
        {"deadline.client": code},
    )
    return archive


def test_namespace_and_module_entries_are_both_audited(validator, tmp_path):
    archive = _make_pyz(tmp_path)

    with validator.temp_pyinstaller_archive_contents(archive) as extracted:
        assert {p.name for p in extracted.iterdir()} == {"deadline", "deadline.client"}
        assert (extracted / "deadline").read_bytes() == b""
        assert (extracted / "deadline.client").stat().st_size > 0

    assert validator.apply_allowlist(archive, validator.Allowlist.from_dict({}), None) == []


def test_unapproved_namespace_is_rejected_even_without_code(validator, tmp_path):
    archive = _make_pyz(tmp_path, "unapproved_namespace")

    failures = validator.apply_allowlist(archive, validator.Allowlist.from_dict({}), None)

    assert any("unapproved_namespace" in failure for failure in failures)


def test_nested_executable_archive_is_audited(validator, tmp_path):
    pyz = _make_pyz(tmp_path, "unapproved_namespace")
    executable = tmp_path / "deadline"
    CArchiveWriter(str(executable), [("PYZ.pyz", str(pyz), False, "z")], "python314.dll")
    allowlist = validator.Allowlist.from_dict(
        {"files": ["PYZ.pyz"], "conditions": {"PYZ.pyz": {"archive_contents": {}}}}
    )

    failures = validator.apply_allowlist(executable, allowlist, None)

    assert any("PYZ.pyz" in failure and "unapproved_namespace" in failure for failure in failures)


def test_archive_path_traversal_is_rejected(validator, tmp_path):
    archive = _make_pyz(tmp_path, "../escaped")

    with pytest.raises(RuntimeError, match="escapes the extraction directory"):
        with validator.temp_pyinstaller_archive_contents(archive):
            pass


@pytest.mark.parametrize("platform", ["darwin", "x86_64-linux-gnu"])
@pytest.mark.parametrize("versioned_directory", [False, True])
def test_known_stdlib_extensions_allowed_and_unknown_extensions_rejected(
    validator, tmp_path, platform, versioned_directory
):
    internal = tmp_path / "_internal"
    if versioned_directory:
        internal /= f"python{sys.version_info.major}.{sys.version_info.minor}"
    dynload = internal / "lib-dynload"
    dynload.mkdir(parents=True)
    suffix = f".cpython-{sys.version_info.major}{sys.version_info.minor}-{platform}.so"
    allowed = [dynload / f"_ssl{suffix}", dynload / f"xxsubtype{suffix}"]
    rejected = dynload / f"unapproved_extension{suffix}"
    for path in [*allowed, rejected]:
        path.touch()

    report = validator.Allowlist.from_dict(validator.ALLOWLIST).get_allowed_files(tmp_path)

    assert {path.relative_to(tmp_path) for path in allowed} <= set(report.allowed_files)
    assert report.dissallowed_files == [rejected.relative_to(tmp_path)]


def test_zstd_allowlist_rejects_other_library_names(validator, tmp_path):
    internal = tmp_path / "_internal"
    internal.mkdir()
    (internal / "libzstd.1.dylib").touch()
    (internal / "libzstd.unapproved.dylib").touch()

    report = validator.Allowlist.from_dict(validator.ALLOWLIST).get_allowed_files(tmp_path)

    assert report.allowed_files == [Path("_internal/libzstd.1.dylib")]
    assert report.dissallowed_files == [Path("_internal/libzstd.unapproved.dylib")]
