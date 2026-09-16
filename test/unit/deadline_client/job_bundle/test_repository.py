# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for the job bundle repository module."""

from __future__ import annotations

import io
import json
import math
import ntpath
import os
import zipfile
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError

from deadline.client._path_utils import is_path_contained
from deadline.client.exceptions import DeadlineOperationError
from deadline.client.job_bundle import _repository
from deadline.client.job_bundle._repository import (
    LocalBundleRepository,
    MAX_ARCHIVE_ENTRIES,
    METADATA_KEY_NAME,
    METADATA_LIMIT_NAME,
    PREVIEW_MAX_DESC_LEN,
    PREVIEW_MAX_NAME_LEN,
    PREVIEW_MAX_PARAM_VALUE_LEN,
    PREVIEW_MAX_PARAMS,
    PREVIEW_MAX_STEPS,
    S3BundleRepository,
    S3_METADATA_TOTAL_BUDGET,
    PREVIEW_PREFETCH_MAX_WORKERS,
    _make_s3_client,
    _bundle_info_from_s3_metadata,
    _check_archive_extraction_safety,
    _decode_s3_value,
    _DOWNLOAD_SPOOL_THRESHOLD,
    _encode_s3_value,
    _extract_archive,
    _extract_archive_from_fileobj,
    _is_archive,
    _open_download_sink,
    _parse_template,
    _cache_key,
    _local_cache_key,
    _safe_zip_extract,
    _strip_archive_ext,
    _truncate_s3_value,
    archive_bundle_dir,
    build_bundle_metadata,
    extract_bundle_info,
    VISIBILITY_VERSION,
    get_bundle_cache_dir,
    get_bundle_dir_size,
    read_template_from_archive,
    sanitize_bundle_name,
)


class TestParseTemplate:
    def test_parse_yaml(self):
        raw = "name: Test\nsteps:\n- name: Step1\n"
        result = _parse_template(raw, "template.yaml")
        assert result == {"name": "Test", "steps": [{"name": "Step1"}]}

    def test_parse_json(self):
        raw = json.dumps({"name": "Test", "steps": [{"name": "Step1"}]})
        result = _parse_template(raw, "template.json")
        assert result == {"name": "Test", "steps": [{"name": "Step1"}]}

    def test_parse_invalid_yaml(self):
        result = _parse_template("{{invalid", "template.yaml")
        assert result is None

    def test_parse_invalid_json(self):
        result = _parse_template("{invalid", "template.json")
        assert result is None


class TestExtractBundleInfoCaps:
    """extract_bundle_info must cap oversized fields and tolerate malformed
    templates so a maliciously constructed bundle cannot DoS or crash the
    preview. Caps are at (never above) the OpenJD spec maxima; steps have no
    spec bound so we cap them ourselves."""

    def test_oversized_name_and_description_are_capped(self):
        template = {
            "name": "A" * 10_000,
            "description": "D" * 10_000,
            "steps": [{"name": "S"}],
        }
        info = extract_bundle_info(template, "/path")
        assert len(info.name) == PREVIEW_MAX_NAME_LEN
        assert len(info.description) == PREVIEW_MAX_DESC_LEN

    def test_huge_step_count_is_capped_with_true_total(self):
        template = {"name": "J", "steps": [{"name": f"S{i}"} for i in range(50_000)]}
        info = extract_bundle_info(template, "/path")
        assert len(info.step_names) == PREVIEW_MAX_STEPS
        # The real total is preserved so the preview can show "… N more".
        assert info.total_steps == 50_000

    def test_huge_parameter_count_is_capped_with_true_total(self):
        template = {
            "name": "J",
            "steps": [],
            "parameterDefinitions": [{"name": f"P{i}", "type": "STRING"} for i in range(10_000)],
        }
        info = extract_bundle_info(template, "/path")
        assert len(info.parameters) == PREVIEW_MAX_PARAMS
        assert info.total_parameters == 10_000

    def test_oversized_parameter_value_is_capped(self):
        template = {
            "name": "J",
            "steps": [],
            "parameterDefinitions": [
                {"name": "P", "type": "STRING", "default": "x" * 10_000},
            ],
        }
        info = extract_bundle_info(template, "/path")
        assert len(info.parameters[0]["_display_value"]) == PREVIEW_MAX_PARAM_VALUE_LEN

    def test_within_limits_reports_no_truncation(self):
        template = {
            "name": "J",
            "steps": [{"name": "S1"}, {"name": "S2"}],
            "parameterDefinitions": [{"name": "P", "type": "STRING"}],
        }
        info = extract_bundle_info(template, "/path")
        assert info.total_steps is None
        assert info.total_parameters is None

    def test_malformed_template_does_not_crash(self):
        # steps/parameterDefinitions not lists, non-dict entries, non-string name.
        template = {
            "name": 12345,
            "description": ["not", "a", "string"],
            "steps": "not-a-list",
            "parameterDefinitions": [{"name": "ok", "type": "STRING"}, "garbage", 42],
        }
        info = extract_bundle_info(template, "/path/to/bundle")
        assert info.name == "bundle"  # falls back to basename
        assert info.description == ""
        assert info.step_names == []
        # Only the well-formed parameter dict survives.
        assert [p["name"] for p in info.parameters] == ["ok"]

    def test_unhashable_param_name_does_not_crash(self):
        """A hostile template can make a parameter name a dict/list; using it as a
        dict key (name in pv_map) must not raise TypeError: unhashable type."""
        template = {
            "name": "J",
            "steps": [],
            "parameterDefinitions": [
                {"name": {"a": 1}, "type": "STRING"},  # dict name
                {"name": ["b"], "type": "PATH"},  # list name
                {"name": "Frames", "type": "STRING", "default": "1-10"},
            ],
        }
        parameter_values = {
            "parameterValues": [
                {"name": {"a": 1}, "value": "x"},  # hostile non-string pv name
                {"name": "Frames", "value": "1-100"},
            ]
        }
        info = extract_bundle_info(template, "/path/to/bundle", parameter_values)
        # Names are coerced to strings; nothing crashes.
        names = [p["name"] for p in info.parameters]
        assert "Frames" in names
        # The well-formed param still resolves its value from parameter_values.
        frames = next(p for p in info.parameters if p["name"] == "Frames")
        assert frames["_display_value"] == "1-100"

    def test_does_not_mutate_caller_template(self):
        template = {
            "name": "J",
            "steps": [],
            "parameterDefinitions": [{"name": "P", "type": "STRING", "default": "1"}],
        }
        extract_bundle_info(template, "/path")
        # The caller's parameter dict must not gain a _display_value key.
        assert "_display_value" not in template["parameterDefinitions"][0]


class TestExtractBundleInfo:
    def test_full_template(self):
        template = {
            "name": "My Job",
            "description": "A test job",
            "steps": [{"name": "Step1"}, {"name": "Step2"}],
            "parameterDefinitions": [
                {"name": "Param1", "type": "STRING"},
                {"name": "Param2", "type": "PATH"},
            ],
        }
        info = extract_bundle_info(template, "/path/to/bundle")
        assert info.name == "My Job"
        assert info.description == "A test job"
        assert info.step_names == ["Step1", "Step2"]
        assert len(info.parameters) == 2

    def test_minimal_template(self):
        template = {"steps": [{"name": "OnlyStep"}]}
        info = extract_bundle_info(template, "/path/to/bundle")
        assert info.name == "bundle"
        assert info.description == ""
        assert info.step_names == ["OnlyStep"]
        assert info.parameters == []

    def test_parameter_values_from_file(self):
        template = {
            "name": "Job",
            "steps": [],
            "parameterDefinitions": [
                {"name": "Frames", "type": "STRING", "default": "1-10"},
                {"name": "Output", "type": "PATH"},
            ],
        }
        pv = {"parameterValues": [{"name": "Frames", "value": "1-50"}]}
        info = extract_bundle_info(template, "/path", pv)
        frames = next(p for p in info.parameters if p["name"] == "Frames")
        output = next(p for p in info.parameters if p["name"] == "Output")
        assert frames["_display_value"] == "1-50"  # from parameter_values
        assert "_display_value" not in output  # no value or default

    def test_parameter_default_used_when_no_value(self):
        template = {
            "name": "Job",
            "steps": [],
            "parameterDefinitions": [
                {"name": "Frames", "type": "STRING", "default": "1-10"},
            ],
        }
        info = extract_bundle_info(template, "/path")
        frames = info.parameters[0]
        assert frames["_display_value"] == "1-10"

    def test_name_with_param_reference(self):
        template = {
            "name": "Render {{Param.SceneName}}",
            "steps": [],
            "parameterDefinitions": [
                {"name": "SceneName", "type": "STRING", "default": "my_scene"},
            ],
        }
        info = extract_bundle_info(template, "/path")
        assert info.name == "Render {{Param.SceneName}}"

    def test_name_not_resolved_with_parameter_values(self):
        template = {
            "name": "{{Param.JobName}}",
            "steps": [],
            "parameterDefinitions": [
                {"name": "JobName", "type": "STRING", "default": "Default Name"},
            ],
        }
        pv = {"parameterValues": [{"name": "JobName", "value": "Custom Name"}]}
        info = extract_bundle_info(template, "/path", pv)
        assert info.name == "{{Param.JobName}}"

    def test_name_unresolved_param(self):
        template = {
            "name": "{{Param.Missing}}",
            "steps": [],
            "parameterDefinitions": [],
        }
        info = extract_bundle_info(template, "/path")
        assert info.name == "{{Param.Missing}}"

    def test_name_not_resolved_from_pv(self):
        """Parameter values don't affect the displayed name."""
        template = {
            "name": "{{Param.JobName}}",
            "steps": [],
            "parameterDefinitions": [],
        }
        pv = {"parameterValues": [{"name": "JobName", "value": "From PV"}]}
        info = extract_bundle_info(template, "/path", pv)
        assert info.name == "{{Param.JobName}}"


class TestBundleInfoFromS3Metadata:
    def test_full_metadata(self):
        metadata = {
            "ojd-name": "My Bundle",
            "ojd-desc": "A description",
            "ojd-steps": "Step1,Step2",
            "ojd-params": "Frames:STRING,Output:PATH",
        }
        info = _bundle_info_from_s3_metadata(metadata, "s3://bucket/key")
        assert info is not None
        assert info.name == "My Bundle"
        assert info.description == "A description"
        assert info.step_names == ["Step1", "Step2"]
        assert len(info.parameters) == 2
        assert info.parameters[0] == {"name": "Frames", "type": "STRING", "_from_metadata": True}
        assert info.parameters[1] == {"name": "Output", "type": "PATH", "_from_metadata": True}

    def test_missing_name_returns_none(self):
        info = _bundle_info_from_s3_metadata({}, "s3://bucket/key")
        assert info is None

    def test_name_only(self):
        info = _bundle_info_from_s3_metadata({"ojd-name": "Simple"}, "s3://bucket/key")
        assert info is not None
        assert info.name == "Simple"
        assert info.step_names == []
        assert info.parameters == []

    def test_oversized_metadata_values_are_capped(self):
        """Attacker-influenced S3 metadata must be capped to the PREVIEW_MAX_* limits,
        matching the template preview path, so a crafted object can't bloat the preview."""
        metadata = {
            "ojd-name": "N" * (PREVIEW_MAX_NAME_LEN + 500),
            "ojd-desc": "D" * (PREVIEW_MAX_DESC_LEN + 500),
            "ojd-steps": ",".join(f"step{i}" for i in range(PREVIEW_MAX_STEPS + 50)),
            "ojd-params": ",".join(f"p{i}:STRING" for i in range(PREVIEW_MAX_PARAMS + 50)),
        }
        info = _bundle_info_from_s3_metadata(metadata, "s3://bucket/key")
        assert info is not None
        assert len(info.name) == PREVIEW_MAX_NAME_LEN
        assert len(info.description) == PREVIEW_MAX_DESC_LEN
        assert len(info.step_names) == PREVIEW_MAX_STEPS
        assert len(info.parameters) == PREVIEW_MAX_PARAMS

    def test_oversized_param_name_and_type_are_capped(self):
        metadata = {
            "ojd-name": "Bundle",
            "ojd-params": f"{'x' * 5000}:{'y' * 5000}",
        }
        info = _bundle_info_from_s3_metadata(metadata, "s3://bucket/key")
        assert info is not None
        assert len(info.parameters) == 1
        assert len(info.parameters[0]["name"]) <= PREVIEW_MAX_NAME_LEN
        assert len(info.parameters[0]["type"]) <= PREVIEW_MAX_NAME_LEN


class TestArchiveHelpers:
    def test_is_archive(self):
        assert _is_archive("bundle.ojd")
        assert not _is_archive("bundle.zip")
        assert not _is_archive("bundle.tar.gz")
        assert not _is_archive("bundle.tgz")
        assert not _is_archive("bundle.tar.bz2")
        assert not _is_archive("bundle.tar.xz")
        assert not _is_archive("bundle.tar")
        assert not _is_archive("bundle")
        assert not _is_archive("template.yaml")

    def test_strip_archive_ext(self):
        assert _strip_archive_ext("bundle.ojd") == "bundle"
        assert _strip_archive_ext("my-job.ojd") == "my-job"
        assert _strip_archive_ext("noext") == "noext"


class TestReadTemplateFromArchive:
    def _make_ojd(self, tmp_path, contents: dict[str, str]) -> str:
        ojd_path = str(tmp_path / "bundle.ojd")
        with zipfile.ZipFile(ojd_path, "w") as zf:
            for name, data in contents.items():
                zf.writestr(name, data)
        return ojd_path

    def test_ojd_root_template(self, tmp_path):
        path = self._make_ojd(tmp_path, {"template.yaml": "name: OjdBundle\nsteps: []\n"})
        result = read_template_from_archive(path)
        assert result is not None
        raw, fname = result
        assert "OjdBundle" in raw
        assert fname == "template.yaml"

    def test_ojd_wrapped_template(self, tmp_path):
        path = self._make_ojd(tmp_path, {"my-bundle/template.yaml": "name: Wrapped\nsteps: []\n"})
        result = read_template_from_archive(path)
        assert result is not None
        raw, fname = result
        assert "Wrapped" in raw

    def test_ojd_json_template(self, tmp_path):
        path = self._make_ojd(
            tmp_path,
            {"template.json": json.dumps({"name": "JSONBundle", "steps": []})},
        )
        result = read_template_from_archive(path)
        assert result is not None
        raw, fname = result
        assert fname == "template.json"

    def test_ojd_no_template(self, tmp_path):
        path = self._make_ojd(tmp_path, {"readme.txt": "no template here"})
        result = read_template_from_archive(path)
        assert result is None

    def test_non_zip_ojd_returns_none(self, tmp_path):
        """A .ojd that isn't actually a zip (corrupt/renamed/malicious) must be
        tolerated on the read/preview path, not raise."""
        path = str(tmp_path / "bogus.ojd")
        with open(path, "wb") as f:
            f.write(b"this is definitely not a zip archive")
        assert read_template_from_archive(path) is None

    def test_truncated_zip_ojd_returns_none(self, tmp_path):
        """A file that starts with the zip magic but is truncated/garbage must also
        be handled gracefully."""
        path = str(tmp_path / "truncated.ojd")
        with open(path, "wb") as f:
            f.write(b"PK\x03\x04" + b"\x00" * 8)  # zip local-file magic, then garbage
        assert read_template_from_archive(path) is None


class TestExtractNonZipArchive:
    """Extraction must convert a raw zipfile.BadZipFile into a clear ValueError so
    a corrupt/renamed/non-zip .ojd surfaces a friendly, catchable error."""

    def test_extract_archive_rejects_non_zip(self, tmp_path):
        path = str(tmp_path / "bogus.ojd")
        with open(path, "wb") as f:
            f.write(b"not a zip file at all")
        dest = str(tmp_path / "out")
        with pytest.raises(ValueError, match="not a valid .ojd archive"):
            _extract_archive(path, dest)

    def test_extract_archive_from_fileobj_rejects_non_zip(self, tmp_path):
        buf = io.BytesIO(b"still not a zip")
        dest = str(tmp_path / "out")
        with pytest.raises(ValueError, match="not a valid .ojd archive"):
            _extract_archive_from_fileobj(buf, dest)


class TestLocalBundleRepository:
    def test_root_path_default(self):
        repo = LocalBundleRepository()
        assert repo.root_path() == os.path.expanduser("~")

    def test_root_path_custom(self, tmp_path):
        repo = LocalBundleRepository(root=str(tmp_path))
        assert repo.root_path() == str(tmp_path)

    def test_list_entries_empty(self, tmp_path):
        repo = LocalBundleRepository(root=str(tmp_path))
        entries = repo.list_entries(str(tmp_path))
        assert entries == []

    def test_list_entries_with_bundles_and_dirs(self, tmp_path):
        bundle_dir = tmp_path / "my-bundle"
        bundle_dir.mkdir()
        (bundle_dir / "template.yaml").write_text("name: Test Bundle\nsteps:\n- name: Step1\n")

        regular_dir = tmp_path / "regular-dir"
        regular_dir.mkdir()

        (tmp_path / "some-file.txt").write_text("not a dir")

        repo = LocalBundleRepository(root=str(tmp_path))
        entries = repo.list_entries(str(tmp_path))

        assert len(entries) == 2
        names = {e.name for e in entries}
        assert "my-bundle" in names
        assert "regular-dir" in names

        bundle_entry = next(e for e in entries if e.name == "my-bundle")
        assert bundle_entry.is_bundle is True
        assert bundle_entry.is_archive is False

        dir_entry = next(e for e in entries if e.name == "regular-dir")
        assert dir_entry.is_bundle is False

    def test_list_entries_with_valid_archive(self, tmp_path):
        ojd_path = tmp_path / "render-job.ojd"
        with zipfile.ZipFile(str(ojd_path), "w") as zf:
            zf.writestr("template.yaml", "name: Render\nsteps:\n- name: S1\n")

        repo = LocalBundleRepository(root=str(tmp_path))
        entries = repo.list_entries(str(tmp_path))

        archive_entries = [e for e in entries if e.is_archive]
        assert len(archive_entries) == 1
        assert archive_entries[0].name == "render-job"
        assert archive_entries[0].is_bundle is True

    def test_list_entries_lists_ojd_by_extension(self, tmp_path):
        """.ojd files are listed by extension without opening them.

        Listing must not open/decompress every archive (it runs on the Qt main
        thread with the home dir as the default root), so even an .ojd that has
        no template is listed; it surfaces an empty/error preview on selection
        rather than being silently hidden.
        """
        ojd_path = tmp_path / "random.ojd"
        with zipfile.ZipFile(str(ojd_path), "w") as zf:
            zf.writestr("readme.txt", "not a bundle")

        repo = LocalBundleRepository(root=str(tmp_path))
        entries = repo.list_entries(str(tmp_path))
        assert [e.name for e in entries] == ["random"]
        assert entries[0].is_archive and entries[0].is_bundle

    def test_list_entries_include_archives_false(self, tmp_path):
        """With include_archives=False, archives are skipped entirely."""
        ojd_path = tmp_path / "bundle.ojd"
        with zipfile.ZipFile(str(ojd_path), "w") as zf:
            zf.writestr("template.yaml", "name: Zipped\nsteps: []\n")

        bundle_dir = tmp_path / "dir-bundle"
        bundle_dir.mkdir()
        (bundle_dir / "template.yaml").write_text("name: Dir\nsteps: []\n")

        repo = LocalBundleRepository(root=str(tmp_path), include_archives=False)
        entries = repo.list_entries(str(tmp_path))

        assert len(entries) == 1
        assert entries[0].name == "dir-bundle"

    def test_list_entries_nonexistent_path(self):
        repo = LocalBundleRepository()
        entries = repo.list_entries("/nonexistent/path/that/does/not/exist")
        assert entries == []

    def test_get_bundle_info_yaml(self, tmp_path):
        bundle_dir = tmp_path / "test-bundle"
        bundle_dir.mkdir()
        (bundle_dir / "template.yaml").write_text(
            yaml.dump(
                {
                    "specificationVersion": "jobtemplate-2023-09",
                    "name": "Test Bundle",
                    "description": "A test",
                    "steps": [{"name": "Render"}],
                    "parameterDefinitions": [{"name": "Frames", "type": "STRING"}],
                }
            )
        )

        repo = LocalBundleRepository(root=str(tmp_path))
        info = repo.get_bundle_info(str(bundle_dir))

        assert info is not None
        assert info.name == "Test Bundle"
        assert info.description == "A test"
        assert info.step_names == ["Render"]
        assert len(info.parameters) == 1

    def test_get_bundle_info_with_parameter_values(self, tmp_path):
        bundle_dir = tmp_path / "pv-bundle"
        bundle_dir.mkdir()
        (bundle_dir / "template.yaml").write_text(
            yaml.dump(
                {
                    "name": "{{Param.JobName}}",
                    "steps": [{"name": "Run"}],
                    "parameterDefinitions": [
                        {"name": "JobName", "type": "STRING", "default": "Default"},
                        {"name": "Frames", "type": "STRING"},
                    ],
                }
            )
        )
        (bundle_dir / "parameter_values.yaml").write_text(
            yaml.dump(
                {
                    "parameterValues": [
                        {"name": "JobName", "value": "My Custom Job"},
                        {"name": "Frames", "value": "1-100"},
                    ]
                }
            )
        )

        repo = LocalBundleRepository(root=str(tmp_path))
        info = repo.get_bundle_info(str(bundle_dir))

        assert info is not None
        assert info.name == "{{Param.JobName}}"
        frames = next(p for p in info.parameters if p["name"] == "Frames")
        assert frames["_display_value"] == "1-100"

    def test_get_bundle_info_archive(self, tmp_path):
        ojd_path = tmp_path / "my-job.ojd"
        with zipfile.ZipFile(str(ojd_path), "w") as zf:
            zf.writestr(
                "template.yaml",
                yaml.dump(
                    {
                        "name": "Archive Job",
                        "description": "From an ojd",
                        "steps": [{"name": "Run"}],
                        "parameterDefinitions": [{"name": "Input", "type": "PATH"}],
                    }
                ),
            )

        repo = LocalBundleRepository(root=str(tmp_path))
        info = repo.get_bundle_info(str(ojd_path))

        assert info is not None
        assert info.name == "Archive Job"
        assert info.description == "From an ojd"
        assert info.step_names == ["Run"]
        assert len(info.parameters) == 1

    def test_get_bundle_info_not_a_bundle(self, tmp_path):
        regular_dir = tmp_path / "not-a-bundle"
        regular_dir.mkdir()

        repo = LocalBundleRepository(root=str(tmp_path))
        info = repo.get_bundle_info(str(regular_dir))
        assert info is None

    def test_extract_bundle_flat(self, tmp_path):
        ojd_path = tmp_path / "flat.ojd"
        with zipfile.ZipFile(str(ojd_path), "w") as zf:
            zf.writestr("template.yaml", "name: Flat\nsteps: []\n")
            zf.writestr("scripts/run.sh", "#!/bin/bash\necho hello\n")

        repo = LocalBundleRepository()
        result = repo.extract_bundle(str(ojd_path))

        assert os.path.isfile(os.path.join(result, "template.yaml"))
        assert os.path.isfile(os.path.join(result, "scripts", "run.sh"))

    def test_extract_bundle_wrapped(self, tmp_path):
        ojd_path = tmp_path / "wrapped.ojd"
        with zipfile.ZipFile(str(ojd_path), "w") as zf:
            zf.writestr("my-bundle/template.yaml", "name: Wrapped\nsteps: []\n")
            zf.writestr("my-bundle/scripts/run.sh", "#!/bin/bash\n")

        repo = LocalBundleRepository()
        result = repo.extract_bundle(str(ojd_path))

        assert os.path.isfile(os.path.join(result, "template.yaml"))

    def test_nested_bundles(self, tmp_path):
        parent = tmp_path / "projects"
        parent.mkdir()

        nested_bundle = parent / "my-job"
        nested_bundle.mkdir()
        (nested_bundle / "template.yaml").write_text("name: Nested\nsteps:\n- name: S1\n")

        repo = LocalBundleRepository(root=str(tmp_path))
        entries = repo.list_entries(str(parent))
        assert len(entries) == 1
        assert entries[0].is_bundle is True
        assert entries[0].name == "my-job"

    def testread_parameter_values_yaml(self, tmp_path):
        bundle_dir = tmp_path / "bundle"
        bundle_dir.mkdir()
        (bundle_dir / "parameter_values.yaml").write_text(
            yaml.dump({"parameterValues": [{"name": "X", "value": "1"}]})
        )
        result = LocalBundleRepository.read_parameter_values(str(bundle_dir))
        assert result is not None
        assert result["parameterValues"][0]["value"] == "1"

    def testread_parameter_values_json(self, tmp_path):
        bundle_dir = tmp_path / "bundle"
        bundle_dir.mkdir()
        (bundle_dir / "parameter_values.json").write_text(
            json.dumps({"parameterValues": [{"name": "Y", "value": "2"}]})
        )
        result = LocalBundleRepository.read_parameter_values(str(bundle_dir))
        assert result is not None
        assert result["parameterValues"][0]["value"] == "2"

    def testread_parameter_values_none(self, tmp_path):
        bundle_dir = tmp_path / "bundle"
        bundle_dir.mkdir()
        result = LocalBundleRepository.read_parameter_values(str(bundle_dir))
        assert result is None


class TestSafeZipExtract:
    def test_rejects_absolute_path(self, tmp_path):
        archive = tmp_path / "bad.zip"
        with zipfile.ZipFile(str(archive), "w") as zf:
            zf.writestr("/etc/passwd", "malicious")

        dest = tmp_path / "out"
        dest.mkdir()
        with zipfile.ZipFile(str(archive), "r") as zf:
            with pytest.raises(ValueError, match="absolute path"):
                _safe_zip_extract(zf, str(dest))

    def test_rejects_parent_directory_traversal(self, tmp_path):
        archive = tmp_path / "bad.zip"
        with zipfile.ZipFile(str(archive), "w") as zf:
            zf.writestr("../../etc/passwd", "malicious")

        dest = tmp_path / "out"
        dest.mkdir()
        with zipfile.ZipFile(str(archive), "r") as zf:
            with pytest.raises(ValueError, match="outside target directory"):
                _safe_zip_extract(zf, str(dest))

    def test_rejects_sibling_sharing_a_string_prefix(self, tmp_path):
        """A string prefix is not a directory prefix: 'out-evil' is outside 'out'."""
        archive = tmp_path / "bad.zip"
        with zipfile.ZipFile(str(archive), "w") as zf:
            zf.writestr("../out-evil/payload", "malicious")

        dest = tmp_path / "out"
        dest.mkdir()
        with zipfile.ZipFile(str(archive), "r") as zf:
            with pytest.raises(ValueError, match="outside target directory"):
                _safe_zip_extract(zf, str(dest))

    def test_allows_normal_archive(self, tmp_path):
        archive = tmp_path / "good.zip"
        with zipfile.ZipFile(str(archive), "w") as zf:
            zf.writestr("template.yaml", "name: Test\n")
            zf.writestr("subdir/file.txt", "hello")

        dest = tmp_path / "out"
        dest.mkdir()
        with zipfile.ZipFile(str(archive), "r") as zf:
            _safe_zip_extract(zf, str(dest))

        assert (dest / "template.yaml").exists()
        assert (dest / "subdir" / "file.txt").exists()


class TestSafeZipExtractWindowsPaths:
    """
    Windows path semantics for the extraction guard, exercised through a simulated
    ntpath filesystem so the cases run on every platform.

    A destination at a UNC share root is the pair os.path.commonpath rejected outright
    ('\\\\host\\share' vs '\\\\host\\share\\template.yaml' -> "Can't mix absolute and
    relative paths"), which the guard read as an escape -- so every entry of every
    archive was rejected there.
    """

    @contextmanager
    def _simulated_windows_extract(self, zf):
        """Run the guard against ntpath, with the extraction itself stubbed out.

        The destinations here do not exist on the host running the test, so the archive
        is never written; only the containment verdict is under test.
        """

        class _WindowsPath:
            def __getattr__(self, name):
                return getattr(ntpath, name)

            @staticmethod
            def realpath(path):
                return ntpath.normpath(path)

        with (
            patch.object(_repository.os, "path", _WindowsPath()),
            patch.object(
                _repository.shutil, "disk_usage", lambda path: SimpleNamespace(free=1 << 40)
            ),
            patch.object(zf, "extractall"),
        ):
            yield

    def test_allows_a_unc_share_root_destination(self, tmp_path):
        """The reported bug: entries of a bundle archive extracted onto a share root."""
        archive = tmp_path / "good.zip"
        with zipfile.ZipFile(str(archive), "w") as zf:
            zf.writestr("template.yaml", "name: Test\n")
            zf.writestr("subdir/file.txt", "hello")

        with zipfile.ZipFile(str(archive), "r") as zf:
            with self._simulated_windows_extract(zf):
                _safe_zip_extract(zf, r"\\host\share")

    def test_pardir_at_a_share_root_is_clamped_not_an_escape(self, tmp_path):
        """A share root is a path root, so '..' from it is clamped and stays inside.

        The entry lands on the share root rather than climbing to the host, so the guard
        has nothing to reject. Pinned because the opposite is the intuitive reading.
        """
        archive = tmp_path / "pardir.zip"
        with zipfile.ZipFile(str(archive), "w") as zf:
            zf.writestr("../escaped.txt", "nope")

        assert ntpath.normpath(ntpath.join(r"\\host\share", "../escaped.txt")) == (
            r"\\host\share\escaped.txt"
        )
        with zipfile.ZipFile(str(archive), "r") as zf:
            with self._simulated_windows_extract(zf):
                _safe_zip_extract(zf, r"\\host\share")

    def test_rejects_an_escape_from_a_directory_on_a_share(self, tmp_path):
        """A subdirectory of a share is the destination '..' can leave."""
        archive = tmp_path / "escape.zip"
        with zipfile.ZipFile(str(archive), "w") as zf:
            zf.writestr("../escaped.txt", "nope")

        with zipfile.ZipFile(str(archive), "r") as zf:
            with self._simulated_windows_extract(zf):
                with pytest.raises(ValueError, match="outside target directory"):
                    _safe_zip_extract(zf, r"\\host\share\bundle")

    def test_rejects_a_drive_relative_entry(self, tmp_path):
        """'D:evil' is neither absolute nor rooted, yet it discards the destination.

        ntpath.join('\\\\host\\share', 'D:evil') is 'D:evil', so the entry lands in a
        different path space entirely -- the containment check is the only thing between
        it and a write outside the destination.
        """
        archive = tmp_path / "drive.zip"
        with zipfile.ZipFile(str(archive), "w") as zf:
            zf.writestr("D:evil", "nope")

        assert not ntpath.isabs("D:evil"), "would be caught by the absolute-path check"
        with zipfile.ZipFile(str(archive), "r") as zf:
            with self._simulated_windows_extract(zf):
                with pytest.raises(ValueError, match="outside target directory"):
                    _safe_zip_extract(zf, r"\\host\share")


class TestSanitizeBundleName:
    def test_slashes_replaced(self):
        assert sanitize_bundle_name("path/to/bundle") == "path_to_bundle"

    @pytest.mark.parametrize(
        "platform, name, expected",
        [
            # Only what is illegal on the running OS is replaced, so each verdict holds on
            # one platform and not the other. The function reads sys.platform at call time,
            # so patching it pins both on every host -- written as a bare `if
            # sys.platform ...` these asserted nothing on two of the three CI platforms.
            ("win32", "path\\to\\bundle", "path_to_bundle"),
            ("linux", "path\\to\\bundle", "path\\to\\bundle"),
            ("win32", "file:name*with?bad<chars>", "file_name_with_bad_chars_"),
            ("linux", "file:name*with?bad<chars>", "file:name*with?bad<chars>"),
            ("win32", 'a"b|c', "a_b_c"),
            ("linux", "my:bundle", "my:bundle"),
            ("win32", "my:bundle", "my_bundle"),
            # Traversal: a separator that is illegal here is replaced, which flattens the
            # name to one component and leaves nothing to climb with.
            ("win32", "..\\..\\evil", ".._.._evil"),
            ("linux", "../../evil", ".._.._evil"),
        ],
    )
    def test_platform_specific_sanitization(self, platform, name, expected):
        with patch.object(_repository.sys, "platform", platform):
            assert sanitize_bundle_name(name) == expected

    def test_traversal_components_are_rejected_where_the_separator_is_legal(self):
        """A backslash is an ordinary filename character on POSIX, so it survives
        sanitization and the '..' components it separates have to be rejected outright."""
        with patch.object(_repository.sys, "platform", "linux"):
            with pytest.raises(ValueError, match="empty or unsafe"):
                sanitize_bundle_name("..\\..\\evil")

    def test_empty_after_sanitization_raises(self):
        with pytest.raises(ValueError, match="empty or unsafe"):
            sanitize_bundle_name("///")

    def test_long_name_preserved(self):
        long_name = "a" * 1000
        assert sanitize_bundle_name(long_name) == long_name

    def test_normal_name_unchanged(self):
        assert sanitize_bundle_name("blender-render_v2.1") == "blender-render_v2.1"


class TestS3BundleVisibility:
    """Bundle visibility is a local, per-user view stored in a file — hide/unhide
    changes only this user's listing and never touches S3."""

    def _make_repo(self, tmp_path, monkeypatch, bucket="test-bucket"):
        # Point the bundle cache at a temp location so tests don't touch ~/.deadline.
        monkeypatch.setattr(
            "deadline.client.job_bundle._repository.get_bundle_cache_dir",
            lambda: str(tmp_path / "cache"),
        )
        with patch("boto3.Session"):
            repo = S3BundleRepository(
                bucket_name=bucket,
                root_prefix="DeadlineCloud",
                session=MagicMock(),
            )
        repo._s3 = MagicMock()
        return repo

    def test_empty_when_nothing_hidden(self, tmp_path, monkeypatch):
        repo = self._make_repo(tmp_path, monkeypatch)
        assert repo.get_hidden_set() == set()
        # Reading visibility must not touch S3.
        repo._s3.head_object.assert_not_called()
        repo._s3.get_paginator.assert_not_called()

    def test_hide_then_read_roundtrip(self, tmp_path, monkeypatch):
        repo = self._make_repo(tmp_path, monkeypatch)
        repo.set_bundle_visibility("bundle-a", hidden=True)
        repo.set_bundle_visibility("rendering/bundle-c", hidden=True)
        assert repo.get_hidden_set() == {"bundle-a", "rendering/bundle-c"}
        # Hide is purely local — no S3 calls.
        repo._s3.head_object.assert_not_called()
        repo._s3.copy_object.assert_not_called()

    def test_unhide_removes_from_set(self, tmp_path, monkeypatch):
        repo = self._make_repo(tmp_path, monkeypatch)
        repo.set_bundle_visibility("bundle-a", hidden=True)
        repo.set_bundle_visibility("bundle-b", hidden=True)
        repo.set_bundle_visibility("bundle-a", hidden=False)
        assert repo.get_hidden_set() == {"bundle-b"}

    def test_view_persists_across_instances(self, tmp_path, monkeypatch):
        repo = self._make_repo(tmp_path, monkeypatch)
        repo.set_bundle_visibility("bundle-a", hidden=True)
        # A fresh repo for the same queue sees the persisted view.
        repo2 = self._make_repo(tmp_path, monkeypatch)
        assert repo2.get_hidden_set() == {"bundle-a"}

    def test_view_file_is_versioned_and_sorted(self, tmp_path, monkeypatch):
        repo = self._make_repo(tmp_path, monkeypatch)
        for name in ("z-bundle", "a-bundle", "m-bundle"):
            repo.set_bundle_visibility(name, hidden=True)
        view_files = list((tmp_path / "cache").glob("*/.visibility.json"))
        assert len(view_files) == 1
        data = json.loads(view_files[0].read_text())
        assert data["hidden"] == ["a-bundle", "m-bundle", "z-bundle"]
        assert data["version"] == VISIBILITY_VERSION

    def test_hide_noop_when_already_hidden(self, tmp_path, monkeypatch):
        repo = self._make_repo(tmp_path, monkeypatch)
        repo.set_bundle_visibility("bundle-a", hidden=True)
        repo.set_bundle_visibility("bundle-a", hidden=True)
        assert repo.get_hidden_set() == {"bundle-a"}

    def test_unhide_noop_when_not_hidden(self, tmp_path, monkeypatch):
        repo = self._make_repo(tmp_path, monkeypatch)
        repo.set_bundle_visibility("bundle-a", hidden=False)
        assert repo.get_hidden_set() == set()

    def test_view_is_per_queue(self, tmp_path, monkeypatch):
        repo_a = self._make_repo(tmp_path, monkeypatch, bucket="bucket-a")
        repo_b = self._make_repo(tmp_path, monkeypatch, bucket="bucket-b")
        repo_a.set_bundle_visibility("x", hidden=True)
        repo_b.set_bundle_visibility("y", hidden=True)
        assert repo_a.get_hidden_set() == {"x"}
        # A different queue (bucket) has an independent hidden set and its own file.
        assert repo_b.get_hidden_set() == {"y"}
        view_files = list((tmp_path / "cache").glob("*/.visibility.json"))
        assert len(view_files) == 2  # one per queue

    def test_malformed_view_file_is_ignored(self, tmp_path, monkeypatch):
        repo = self._make_repo(tmp_path, monkeypatch)
        repo.set_bundle_visibility("bundle-a", hidden=True)  # creates the file
        view_file = next((tmp_path / "cache").glob("*/.visibility.json"))
        view_file.write_text("{ not valid json")
        assert repo.get_hidden_set() == set()


class TestMakeS3Client:
    """The S3 client's connection pool is sized to cover the background preview
    prefetch's parallel HEADs (and managed transfers), so urllib3 doesn't warn."""

    def _pool(self, mock_default_config):
        return mock_default_config.call_args[1]["max_pool_connections"]

    @patch("deadline.client.api._session.get_default_client_config")
    @patch("deadline.client.job_bundle._repository.config_file")
    def test_pool_uses_larger_configured_setting(self, mock_config_file, mock_default_config):
        mock_config_file.get_setting.return_value = "50"
        session = MagicMock()
        _make_s3_client(session)
        assert self._pool(mock_default_config) == 50
        session.client.assert_called_once()

    @patch("deadline.client.api._session.get_default_client_config")
    @patch("deadline.client.job_bundle._repository.config_file")
    def test_pool_covers_prefetch_workers_when_setting_is_small(
        self, mock_config_file, mock_default_config
    ):
        mock_config_file.get_setting.return_value = "4"
        _make_s3_client(MagicMock())
        assert self._pool(mock_default_config) == PREVIEW_PREFETCH_MAX_WORKERS

    @patch("deadline.client.api._session.get_default_client_config")
    @patch("deadline.client.job_bundle._repository.config_file")
    def test_pool_falls_back_when_setting_unparseable(self, mock_config_file, mock_default_config):
        mock_config_file.get_setting.return_value = "not-a-number"
        _make_s3_client(MagicMock())
        assert self._pool(mock_default_config) == PREVIEW_PREFETCH_MAX_WORKERS


class TestPrefetchPreviews:
    """prefetch_previews warms _head_cache with parallel HEADs so preview/size/download
    reuse them; it's a background optimization decoupled from visibility."""

    def _make_repo(self):
        with patch("boto3.Session"):
            repo = S3BundleRepository("test-bucket", "DeadlineCloud", session=MagicMock())
        repo._s3 = MagicMock()
        return repo

    def _set_listing(self, repo, keys):
        paginator = MagicMock()
        paginator.paginate.return_value = [{"Contents": [{"Key": k} for k in keys]}]
        repo._s3.get_paginator.return_value = paginator

    def test_prefetch_all_warms_cache(self):
        repo = self._make_repo()
        prefix = repo._prefix
        self._set_listing(
            repo,
            [f"{prefix}a.ojd", f"{prefix}rendering/b.ojd", f"{prefix}notes.txt"],
        )
        repo._s3.head_object.side_effect = lambda Bucket, Key: {"ETag": f'"{Key}"', "Metadata": {}}

        repo.prefetch_previews()

        # Only .ojd objects are prefetched.
        assert set(repo._head_cache) == {f"{prefix}a.ojd", f"{prefix}rendering/b.ojd"}
        assert repo._s3.head_object.call_count == 2

    def test_prefetch_rebuilds_and_drops_stale(self):
        repo = self._make_repo()
        prefix = repo._prefix
        repo._head_cache[f"{prefix}deleted.ojd"] = {"ETag": '"old"'}
        self._set_listing(repo, [f"{prefix}still.ojd"])
        repo._s3.head_object.return_value = {"ETag": '"new"', "Metadata": {}}

        repo.prefetch_previews()

        assert f"{prefix}deleted.ojd" not in repo._head_cache
        assert f"{prefix}still.ojd" in repo._head_cache

    def test_prefetch_tolerates_head_failures(self):
        repo = self._make_repo()
        prefix = repo._prefix
        self._set_listing(repo, [f"{prefix}good.ojd", f"{prefix}bad.ojd"])

        def _head(Bucket, Key):
            if Key.endswith("bad.ojd"):
                raise ClientError({"Error": {"Code": "AccessDenied"}}, "HeadObject")
            return {"ETag": '"e"', "Metadata": {}}

        repo._s3.head_object.side_effect = _head
        repo.prefetch_previews()  # must not raise
        assert f"{prefix}good.ojd" in repo._head_cache
        assert f"{prefix}bad.ojd" not in repo._head_cache

    def test_preview_reuses_prefetched_head(self):
        repo = self._make_repo()
        key = f"{repo._prefix}blender.ojd"
        repo._head_cache[key] = {
            "ETag": '"e1"',
            "ContentLength": 4096,
            "Metadata": {"ojd-name": "Blender", "ojd-steps": "Render"},
        }

        info = repo.get_bundle_info(f"s3://test-bucket/{key}")

        repo._s3.head_object.assert_not_called()
        repo._s3.get_object.assert_not_called()
        assert info is not None
        assert info.name == "Blender"
        assert info.size_bytes == 4096

    def test_get_bundle_size_uses_live_head_not_cache(self):
        """Size must not trust the prefetch cache: a stale cached ContentLength
        would size the download against an object that may have been overwritten
        on the queue while the dialog is open."""
        repo = self._make_repo()
        key = f"{repo._prefix}blender.ojd"
        repo._head_cache[key] = {"ETag": '"e1"', "ContentLength": 9999, "Metadata": {}}
        repo._s3.head_object.return_value = {
            "ETag": '"live"',
            "ContentLength": 42,
            "Metadata": {},
        }
        assert repo.get_bundle_size(f"s3://test-bucket/{key}") == 42
        repo._s3.head_object.assert_called_once()


class TestArchiveBundleDir:
    """Tests for archive_bundle_dir — validates archiving produces correct zip content
    and progress callbacks report accurate byte counts."""

    def test_archives_all_files(self, tmp_path):
        """All files in the bundle directory end up in the archive."""
        bundle = tmp_path / "my-bundle"
        bundle.mkdir()
        (bundle / "template.yaml").write_text("name: Test\nsteps: []\n")
        (bundle / "script.sh").write_text("#!/bin/bash\necho hello\n")
        (bundle / "subdir").mkdir()
        (bundle / "subdir" / "data.json").write_text('{"key": "value"}')

        buf = archive_bundle_dir(str(bundle))

        with zipfile.ZipFile(buf, "r") as zf:
            names = sorted(zf.namelist())
            assert "template.yaml" in names
            assert "script.sh" in names
            assert "subdir/data.json" in names

    def test_progress_reports_total_bytes(self, tmp_path):
        """Progress callback receives total bytes equal to the source directory size."""
        bundle = tmp_path / "bundle"
        bundle.mkdir()
        (bundle / "template.yaml").write_text("name: T\nsteps: []\n")
        (bundle / "big.bin").write_bytes(b"x" * 1000)

        reported = []
        archive_bundle_dir(str(bundle), progress_callback=lambda n: reported.append(n))

        assert sum(reported) == get_bundle_dir_size(str(bundle))

    def test_skips_symlinks(self, tmp_path):
        """Symlinked files are not included in the archive."""
        bundle = tmp_path / "bundle"
        bundle.mkdir()
        (bundle / "template.yaml").write_text("name: T\nsteps: []\n")
        target = tmp_path / "outside.txt"
        target.write_text("secret")
        (bundle / "link.txt").symlink_to(target)

        buf = archive_bundle_dir(str(bundle))

        with zipfile.ZipFile(buf, "r") as zf:
            assert "link.txt" not in zf.namelist()


class TestGetBundleDirSize:
    def test_returns_total_file_size(self, tmp_path):
        bundle = tmp_path / "bundle"
        bundle.mkdir()
        (bundle / "a.txt").write_bytes(b"x" * 100)
        (bundle / "b.txt").write_bytes(b"y" * 200)

        assert get_bundle_dir_size(str(bundle)) == 300


class TestSafeZipExtractWithProgress:
    """Tests that _safe_zip_extract progress callback reports correct sizes."""

    def test_progress_reports_uncompressed_sizes(self, tmp_path):
        """Each callback receives the uncompressed file size."""
        archive = tmp_path / "test.zip"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("small.txt", "hello")  # 5 bytes
            zf.writestr("bigger.txt", "x" * 100)  # 100 bytes

        reported = []
        with zipfile.ZipFile(archive, "r") as zf:
            _safe_zip_extract(
                zf, str(tmp_path / "out"), progress_callback=lambda n: reported.append(n)
            )

        assert sorted(reported) == [5, 100]

    def test_size_callback_reports_total(self, tmp_path):
        """size_callback is called once with total uncompressed size before extraction."""
        archive = tmp_path / "test.zip"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("a.txt", "aaa")
            zf.writestr("b.txt", "bbbbb")

        size_reports = []
        with zipfile.ZipFile(archive, "r") as zf:
            _safe_zip_extract(
                zf,
                str(tmp_path / "out"),
                progress_callback=lambda n: None,
                size_callback=lambda t: size_reports.append(t),
            )

        assert size_reports == [8]  # 3 + 5


class TestFromConfig:
    """Tests for S3BundleRepository.from_config — validates the parallel initialization path."""

    @patch("deadline.client.api.get_queue_user_boto3_session")
    @patch("deadline.client.api.get_boto3_client")
    @patch("deadline.client.api.get_boto3_session")
    @patch("deadline.client.job_bundle._repository.config_file")
    def test_creates_repo_with_correct_bucket_and_prefix(
        self, mock_config_file, mock_get_session, mock_get_client, mock_get_queue_session
    ):
        """from_config returns a repo with bucket/prefix from GetQueue response."""
        mock_config_file.get_setting.side_effect = lambda key, config=None: {
            "defaults.farm_id": "farm-123",
            "defaults.queue_id": "queue-456",
        }.get(key, "")

        mock_deadline_client = MagicMock()
        mock_deadline_client.get_queue.return_value = {
            "jobAttachmentSettings": {
                "s3BucketName": "my-bucket",
                "rootPrefix": "DeadlineCloud",
            },
        }
        mock_get_client.return_value = mock_deadline_client

        mock_s3_session = MagicMock()
        mock_s3_client = MagicMock()
        mock_s3_session.client.return_value = mock_s3_client
        mock_get_queue_session.return_value = mock_s3_session

        repo = S3BundleRepository.from_config()

        assert repo._bucket == "my-bucket"
        assert "job-bundles" in repo._prefix
        assert repo._s3 is mock_s3_client
        mock_deadline_client.get_queue.assert_called_once_with(
            farmId="farm-123", queueId="queue-456"
        )

    @patch("deadline.client.job_bundle._repository.config_file")
    def test_raises_without_farm_or_queue(self, mock_config_file):
        """from_config raises when farm/queue IDs are not configured."""
        mock_config_file.get_setting.return_value = ""
        with pytest.raises(DeadlineOperationError, match="farm and queue"):
            S3BundleRepository.from_config()

    @patch("deadline.client.api.get_queue_user_boto3_session")
    @patch("deadline.client.api.get_boto3_client")
    @patch("deadline.client.api.get_boto3_session")
    @patch("deadline.client.job_bundle._repository.config_file")
    def test_raises_without_attachment_settings(
        self, mock_config_file, mock_get_session, mock_get_client, mock_get_queue_session
    ):
        """from_config raises when queue has no job attachment settings."""
        mock_config_file.get_setting.side_effect = lambda key, config=None: {
            "defaults.farm_id": "farm-123",
            "defaults.queue_id": "queue-456",
        }.get(key, "")

        mock_deadline_client = MagicMock()
        mock_deadline_client.get_queue.return_value = {}
        mock_get_client.return_value = mock_deadline_client
        mock_get_queue_session.return_value = MagicMock()

        with pytest.raises(DeadlineOperationError, match="attachment settings"):
            S3BundleRepository.from_config()


class TestS3ListEntries:
    """Tests for S3BundleRepository.list_entries — validates S3 listing logic."""

    def _make_repo(self):
        with patch("boto3.Session"):
            repo = S3BundleRepository(
                bucket_name="test-bucket", root_prefix="DC", session=MagicMock()
            )
        repo._s3 = MagicMock()
        return repo

    def test_lists_ojd_files_as_bundles(self):
        repo = self._make_repo()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "CommonPrefixes": [],
                "Contents": [
                    {"Key": "DC/job-bundles/render.ojd"},
                    {"Key": "DC/job-bundles/sim.ojd"},
                ],
            }
        ]
        repo._s3.get_paginator.return_value = paginator

        entries = repo.list_entries(repo.root_path())

        bundle_names = [e.name for e in entries if e.is_bundle]
        assert sorted(bundle_names) == ["render", "sim"]
        assert all(e.is_archive for e in entries if e.is_bundle)

    def test_lists_subfolders_as_non_bundles(self):
        repo = self._make_repo()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "CommonPrefixes": [{"Prefix": "DC/job-bundles/rendering/"}],
                "Contents": [],
            }
        ]
        repo._s3.get_paginator.return_value = paginator

        entries = repo.list_entries(repo.root_path())

        assert len(entries) == 1
        assert entries[0].name == "rendering"
        assert entries[0].is_bundle is False

    def test_ignores_non_ojd_files(self):
        repo = self._make_repo()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "CommonPrefixes": [],
                "Contents": [
                    {"Key": "DC/job-bundles/readme.txt"},
                    {"Key": "DC/job-bundles/valid.ojd"},
                ],
            }
        ]
        repo._s3.get_paginator.return_value = paginator

        entries = repo.list_entries(repo.root_path())

        assert len(entries) == 1
        assert entries[0].name == "valid"


class TestResolveArchiveBundle:
    """Tests for _resolve_archive_bundle — validates download, cache, and extract flow."""

    def _make_repo(self, fresh_deadline_config):
        with patch("boto3.Session"):
            repo = S3BundleRepository(
                bucket_name="test-bucket", root_prefix="DC", session=MagicMock()
            )
        repo._s3 = MagicMock()
        return repo

    def _make_ojd_bytes(self):
        """Create a minimal .ojd archive in memory."""
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("template.yaml", "name: TestBundle\nsteps:\n- name: Step1\n")
        return buf.getvalue()

    def test_downloads_and_extracts_to_cache(self, fresh_deadline_config, tmp_path):
        repo = self._make_repo(fresh_deadline_config)
        ojd_data = self._make_ojd_bytes()

        repo._s3.head_object.return_value = {"ETag": '"abc"', "ContentLength": len(ojd_data)}
        repo._s3.download_fileobj.side_effect = lambda Fileobj, **kw: Fileobj.write(ojd_data)

        path = "s3://test-bucket/DC/job-bundles/test.ojd"
        result = repo._resolve_archive_bundle(path)

        assert os.path.isfile(os.path.join(result, "template.yaml"))

    def test_uses_cache_on_matching_etag(self, fresh_deadline_config, tmp_path):
        repo = self._make_repo(fresh_deadline_config)
        ojd_data = self._make_ojd_bytes()

        repo._s3.head_object.return_value = {"ETag": '"abc"', "ContentLength": len(ojd_data)}
        repo._s3.download_fileobj.side_effect = lambda Fileobj, **kw: Fileobj.write(ojd_data)

        path = "s3://test-bucket/DC/job-bundles/cached.ojd"

        # First call downloads
        result1 = repo._resolve_archive_bundle(path)
        assert repo._s3.download_fileobj.call_count == 1

        # Second call uses cache (same ETag)
        result2 = repo._resolve_archive_bundle(path)
        assert repo._s3.download_fileobj.call_count == 1  # No additional download
        assert result1 == result2

    def test_calls_progress_callbacks(self, fresh_deadline_config):
        repo = self._make_repo(fresh_deadline_config)
        ojd_data = self._make_ojd_bytes()

        repo._s3.head_object.return_value = {"ETag": '"new"', "ContentLength": len(ojd_data)}
        repo._s3.download_fileobj.side_effect = lambda Fileobj, **kw: Fileobj.write(ojd_data)

        dl_progress = []
        ex_progress = []
        size_reports = []

        path = "s3://test-bucket/DC/job-bundles/progress.ojd"
        repo._resolve_archive_bundle(
            path,
            progress_callback=lambda n: dl_progress.append(n),
            extract_callback=lambda n: ex_progress.append(n),
            extract_size_callback=lambda t: size_reports.append(t),
        )

        # Download callback is called (by download_fileobj via Callback kwarg)
        # Extract callback receives file sizes
        assert len(ex_progress) > 0
        assert len(size_reports) == 1
        assert size_reports[0] > 0


class TestArchiveExtractionSafety:
    """_check_archive_extraction_safety guards against zip bombs, excessive entry
    counts, and extractions that wouldn't fit on disk — always enforced."""

    def _fake_zip(self, entries):
        """entries: list of (file_size, compress_size) tuples.

        Uses lightweight SimpleNamespace entries rather than MagicMock: the
        too-many-entries test builds MAX_ARCHIVE_ENTRIES + 1 (100_001) infos,
        and a MagicMock per entry exhausts worker memory (OOM-crashing the
        pytest-xdist worker). _check_archive_extraction_safety only reads
        .file_size / .compress_size, which SimpleNamespace provides.
        """
        zf = MagicMock()
        infos = [
            SimpleNamespace(file_size=file_size, compress_size=compress_size)
            for file_size, compress_size in entries
        ]
        zf.infolist.return_value = infos
        return zf

    def _ample_disk(self):
        return patch(
            "deadline.client.job_bundle._repository.shutil.disk_usage",
            return_value=MagicMock(free=100 * 1024**3),
        )

    def test_rejects_zip_bomb_high_ratio(self, tmp_path):
        # 1 GB uncompressed from 1 KB compressed — ratio far above the limit.
        zf = self._fake_zip([(1024 * 1024 * 1024, 1024)])
        with pytest.raises(ValueError, match="zip bomb"):
            _check_archive_extraction_safety(zf, str(tmp_path))

    def test_rejects_too_many_entries(self, tmp_path):
        zf = self._fake_zip([(10, 10)] * (MAX_ARCHIVE_ENTRIES + 1))
        with pytest.raises(ValueError, match="too many entries"):
            _check_archive_extraction_safety(zf, str(tmp_path))

    def test_allows_small_high_ratio_within_floor(self, tmp_path):
        # 100 MB (< 256 MB floor) from tiny compressed — allowed despite high ratio.
        zf = self._fake_zip([(100 * 1024 * 1024, 100)])
        with self._ample_disk():
            _check_archive_extraction_safety(zf, str(tmp_path))  # must not raise

    def test_allows_large_low_ratio_bundle(self, tmp_path):
        # 1 GB uncompressed from 900 MB compressed (real data) — passes the ratio.
        zf = self._fake_zip([(1024**3, 900 * 1024 * 1024)])
        with self._ample_disk():
            _check_archive_extraction_safety(zf, str(tmp_path))  # must not raise

    def test_blocks_when_insufficient_disk(self, tmp_path):
        # Low ratio (passes bomb check) but larger than the free space available.
        zf = self._fake_zip([(100 * 1024 * 1024, 60 * 1024 * 1024)])
        with patch(
            "deadline.client.job_bundle._repository.shutil.disk_usage",
            return_value=MagicMock(free=1024),  # only 1 KB free
        ):
            with pytest.raises(ValueError, match="disk space"):
                _check_archive_extraction_safety(zf, str(tmp_path))

    def test_safe_zip_extract_rejects_bomb_end_to_end(self, tmp_path):
        # A real, highly compressible archive; lower the floor so it trips the
        # ratio check, proving _safe_zip_extract enforces the guard on real zips.
        archive = tmp_path / "bomb.ojd"
        with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("payload.bin", b"\x00" * (2 * 1024 * 1024))  # 2 MB of zeros
        dest = tmp_path / "out"
        dest.mkdir()
        with patch("deadline.client.job_bundle._repository.MAX_ARCHIVE_UNCOMPRESSED_FLOOR", 1024):
            with zipfile.ZipFile(archive, "r") as zf:
                with pytest.raises(ValueError, match="zip bomb"):
                    _safe_zip_extract(zf, str(dest))


class TestTemplateReadCap:
    """_read_template_from_zip refuses to read an implausibly large template."""

    def test_rejects_oversized_template(self, tmp_path):
        archive = tmp_path / "b.ojd"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("template.yaml", "name: T\nsteps: []\n")
        with patch("deadline.client.job_bundle._repository.MAX_TEMPLATE_BYTES", 5):
            # read_template_from_archive swallows the error and returns None.
            assert read_template_from_archive(str(archive)) is None

    def test_reads_normal_template(self, tmp_path):
        archive = tmp_path / "b.ojd"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("template.yaml", "name: T\nsteps: []\n")
        result = read_template_from_archive(str(archive))
        assert result is not None
        assert result[1] == "template.yaml"


class TestDownloadSink:
    """_open_download_sink chooses memory vs. a temp file based on size, and spills
    large downloads to disk in the cache dir (not shared /tmp)."""

    def test_small_download_stays_in_memory(self, tmp_path):
        with patch(
            "deadline.client.job_bundle._repository.get_bundle_cache_dir",
            return_value=str(tmp_path),
        ):
            sink = _open_download_sink(1024)
        try:
            assert isinstance(sink, io.BytesIO)
        finally:
            sink.close()

    def test_large_download_spills_to_temp_file_in_cache_dir(self, tmp_path):
        cache = tmp_path / "cache"
        with patch(
            "deadline.client.job_bundle._repository.get_bundle_cache_dir",
            return_value=str(cache),
        ):
            sink = _open_download_sink(_DOWNLOAD_SPOOL_THRESHOLD + 1)
        try:
            # A real on-disk temp file (not an in-memory buffer), created under the
            # cache dir so it shares the extraction filesystem and stays off /tmp.
            assert not isinstance(sink, io.BytesIO)
            assert sink.fileno() >= 0  # backed by a real file descriptor
            assert cache.is_dir()  # cache dir was created for the spill
        finally:
            sink.close()

    def test_zero_size_hint_stays_in_memory(self, tmp_path):
        # Unknown size (no ContentLength) must not force a temp file.
        with patch(
            "deadline.client.job_bundle._repository.get_bundle_cache_dir",
            return_value=str(tmp_path),
        ):
            sink = _open_download_sink(0)
        try:
            assert isinstance(sink, io.BytesIO)
        finally:
            sink.close()


class TestS3GetBundleInfo:
    """Tests for S3BundleRepository.get_bundle_info — validates metadata preview path."""

    def _make_repo(self):
        with patch("boto3.Session"):
            repo = S3BundleRepository(
                bucket_name="test-bucket", root_prefix="DC", session=MagicMock()
            )
        repo._s3 = MagicMock()
        return repo

    def test_returns_info_from_s3_metadata(self):
        """When head_object returns bundle metadata, uses it without downloading."""
        repo = self._make_repo()
        repo._s3.head_object.return_value = {
            "ETag": '"abc"',
            "Metadata": {
                "ojd-name": "Preview Bundle",
                "ojd-desc": "A description",
                "ojd-steps": "Step1",
                "ojd-params": "Frames:STRING",
            },
        }

        info = repo.get_bundle_info("s3://test-bucket/DC/job-bundles/preview.ojd")

        assert info is not None
        assert info.name == "Preview Bundle"
        assert info.description == "A description"
        assert info.step_names == ["Step1"]
        # No download needed
        repo._s3.get_object.assert_not_called()

    def test_size_bytes_populated_from_head_content_length(self):
        """The archive size from head_object is stamped onto BundleInfo so the UI
        can show how much a Queue download will transfer, without an extra call."""
        repo = self._make_repo()
        repo._s3.head_object.return_value = {
            "ETag": '"abc"',
            "ContentLength": 12_345_678,
            "Metadata": {"ojd-name": "Sized Bundle"},
        }

        info = repo.get_bundle_info("s3://test-bucket/DC/job-bundles/sized.ojd")

        assert info is not None
        assert info.size_bytes == 12_345_678
        # Size came from the (already-performed) HEAD — no extra download.
        repo._s3.get_object.assert_not_called()

    def test_falls_back_to_download_when_no_metadata(self, fresh_deadline_config):
        """When head_object has no bundle metadata, downloads and parses the archive."""
        repo = self._make_repo()
        repo._s3.head_object.return_value = {"ETag": '"xyz"', "Metadata": {}}

        # Provide a real archive via get_object
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("template.yaml", "name: Downloaded\nsteps:\n- name: S1\n")

        repo._s3.get_object.return_value = {
            # A BytesIO is a faithful stand-in for a botocore StreamingBody:
            # read(n) returns chunks and b"" at EOF, so streaming (copyfileobj)
            # terminates. A MagicMock with a fixed read() return value would loop
            # forever.
            "Body": io.BytesIO(buf.getvalue()),
            "ETag": '"xyz"',
            "LastModified": "2026-01-01",
        }

        info = repo.get_bundle_info("s3://test-bucket/DC/job-bundles/fallback.ojd")

        assert info is not None
        assert info.name == "Downloaded"
        assert info.step_names == ["S1"]

    def test_large_archive_download_streams_via_temp_file(self, fresh_deadline_config):
        """A large ContentLength routes the download through a temp-file sink and
        still extracts/parses correctly (exercises the spill-to-disk branch)."""
        repo = self._make_repo()
        repo._s3.head_object.return_value = {
            "ETag": '"xyz"',
            "Metadata": {},
            "ContentLength": _DOWNLOAD_SPOOL_THRESHOLD + 1,
        }

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("template.yaml", "name: BigBundle\nsteps:\n- name: S1\n")

        repo._s3.get_object.return_value = {
            "Body": io.BytesIO(buf.getvalue()),
            "ETag": '"xyz"',
            "LastModified": "2026-01-01",
        }

        info = repo.get_bundle_info("s3://test-bucket/DC/job-bundles/big.ojd")

        assert info is not None
        assert info.name == "BigBundle"
        assert info.step_names == ["S1"]


class TestBuildBundleMetadata:
    """Tests for build_bundle_metadata — validates S3 metadata extraction from bundle dirs."""

    def test_extracts_name_and_description(self, tmp_path):

        bundle = tmp_path / "my-bundle"
        bundle.mkdir()
        (bundle / "template.yaml").write_text(
            yaml.dump(
                {
                    "specificationVersion": "jobtemplate-2023-09",
                    "name": "Test Bundle",
                    "description": "A test description",
                    "steps": [{"name": "Step1"}],
                }
            )
        )

        metadata = build_bundle_metadata(str(bundle))

        assert metadata["ojd-name"] == "Test Bundle"
        assert metadata["ojd-desc"] == "A test description"
        assert metadata["ojd-steps"] == "Step1"

    def test_overrides_name(self, tmp_path):

        bundle = tmp_path / "bundle"
        bundle.mkdir()
        (bundle / "template.yaml").write_text(yaml.dump({"name": "Original", "steps": []}))

        metadata = build_bundle_metadata(str(bundle), bundle_name="Override")

        assert metadata["ojd-name"] == "Override"

    def test_returns_empty_for_missing_template(self, tmp_path):

        empty = tmp_path / "empty"
        empty.mkdir()

        assert build_bundle_metadata(str(empty)) == {}

    def test_truncates_long_values(self, tmp_path):

        bundle = tmp_path / "bundle"
        bundle.mkdir()
        (bundle / "template.yaml").write_text(yaml.dump({"name": "A" * 300, "steps": []}))

        metadata = build_bundle_metadata(str(bundle))

        assert len(metadata["ojd-name"].encode("utf-8")) <= METADATA_LIMIT_NAME
        assert metadata["ojd-name"].endswith("...")

    def test_metadata_values_are_ascii_safe_for_non_ascii_template(self, tmp_path):
        """Non-ASCII template fields must round-trip through ASCII-safe metadata.

        S3 user metadata is sent as x-amz-meta-* HTTP headers, and botocore
        rejects any non-ASCII value with a ParamValidationError, so a bundle
        whose name/description/steps/params contain Japanese, accented Latin, or
        emoji characters would previously fail to upload entirely. Every stored
        metadata value must be ASCII-safe, and must decode back to the original
        Unicode so previews are accurate.
        """
        bundle = tmp_path / "bundle"
        bundle.mkdir()
        (bundle / "template.yaml").write_text(
            yaml.dump(
                {
                    "specificationVersion": "jobtemplate-2023-09",
                    "name": "レンダリング",  # Japanese
                    "description": "Café rendering job 🎬",  # accent + emoji
                    "steps": [{"name": "描画ステップ"}],
                    "parameterDefinitions": [
                        {"name": "出力先", "type": "PATH"},
                    ],
                },
                allow_unicode=True,
            ),
            encoding="utf-8",
        )

        metadata = build_bundle_metadata(str(bundle))

        # Every stored value must be ASCII (header-safe) so the upload succeeds.
        for key, value in metadata.items():
            value.encode("ascii")  # must not raise

        # ...and it must decode back to the original Unicode for previews.
        info = _bundle_info_from_s3_metadata(metadata, "s3://bucket/key.ojd")
        assert info is not None
        assert info.name == "レンダリング"
        assert info.description == "Café rendering job 🎬"
        assert info.step_names == ["描画ステップ"]
        assert info.parameters[0]["name"] == "出力先"
        assert info.parameters[0]["type"] == "PATH"

    def test_control_chars_are_collapsed_for_header_safety(self, tmp_path):
        """Control chars (CR/LF/TAB) in name/steps/params would otherwise land
        verbatim in an x-amz-meta-* header and make urllib3 reject the upload
        (or, on older urllib3, allow header injection). They must be collapsed."""
        bundle = tmp_path / "bundle"
        bundle.mkdir()
        (bundle / "template.yaml").write_text(
            yaml.dump(
                {
                    "name": "render\njob",
                    "description": "line1\nline2",
                    "steps": [{"name": "step\r\none"}],
                    "parameterDefinitions": [{"name": "pa\tram", "type": "STRING"}],
                }
            ),
            encoding="utf-8",
        )

        metadata = build_bundle_metadata(str(bundle))

        # No metadata value may contain a raw control whitespace char.
        for key, value in metadata.items():
            assert "\n" not in value, (key, value)
            assert "\r" not in value, (key, value)
            assert "\t" not in value, (key, value)
        # The newline in the name collapses to a single space.
        assert metadata[METADATA_KEY_NAME] == "render job"

    def test_large_non_ascii_bundle_stays_within_total_budget(self, tmp_path):
        """base64 encoding expands non-ASCII ~4/3 (+12B wrapper per field), so verify
        the per-field limits and 2 KB total budget still hold for a large all-Japanese
        bundle. The budget is enforced on the encoded (on-wire) values, so no numeric
        limit change was needed — this proves that.
        """
        bundle = tmp_path / "bundle"
        bundle.mkdir()
        (bundle / "template.yaml").write_text(
            yaml.dump(
                {
                    "specificationVersion": "jobtemplate-2023-09",
                    "name": "あ" * 300,
                    "description": "い" * 1000,
                    "steps": [{"name": f"ステップ{i:03d}描画"} for i in range(40)],
                    "parameterDefinitions": [
                        {"name": f"パラメータ{i:03d}", "type": "STRING"} for i in range(50)
                    ],
                },
                allow_unicode=True,
            ),
            encoding="utf-8",
        )

        metadata = build_bundle_metadata(str(bundle))

        # Every stored value is header-safe.
        for value in metadata.values():
            value.encode("ascii")  # must not raise
        # Per-field encoded limit respected.
        assert len(metadata["ojd-name"].encode("utf-8")) <= METADATA_LIMIT_NAME
        # Total encoded metadata (what actually goes on the wire) within S3 budget.
        total = sum(
            12 + len(k.encode("utf-8")) + len(v.encode("utf-8")) for k, v in metadata.items()
        )
        assert total <= S3_METADATA_TOTAL_BUDGET


class TestEncodeDecodeS3Value:
    """Tests for the S3-metadata encode/decode round-trip (_encode/_decode/_truncate)."""

    def test_ascii_value_unchanged(self):
        # ASCII values are stored verbatim (backward compatible with old uploads).
        assert _encode_s3_value("hello world") == "hello world"
        assert _decode_s3_value("hello world") == "hello world"

    def test_non_ascii_is_encoded_to_ascii(self):
        for value in ("日本", "café", "job🎬", "Café job 🎬", "出力先:PATH"):
            encoded = _encode_s3_value(value)
            encoded.encode("ascii")  # must not raise — header-safe
            assert encoded != value

    def test_round_trips_non_ascii(self):
        for value in ("レンダリング", "café", "job🎬", "Step1,描画ステップ", "出力先:PATH"):
            assert _decode_s3_value(_encode_s3_value(value)) == value

    def test_decode_of_plain_ascii_is_noop(self):
        # Older uploads / bundles stored via other means have plain ASCII values.
        assert _decode_s3_value("Blender Render") == "Blender Render"

    def test_decode_handles_foreign_encoded_word_variants(self):
        """Decoding must handle any valid RFC 2047 encoded-word, not just this
        module's exact output — bundles uploaded by other clients or earlier
        builds may use uppercase charset, folded multi-words, or Q-encoding.
        """
        # Uppercase charset + Base64 (as produced by email.header).
        assert _decode_s3_value("=?UTF-8?B?44Os44Oz44OA44Oq44Oz44Kw?=") == "レンダリング"
        # Folded into multiple space-separated encoded-words.
        folded = (
            "=?UTF-8?B?5pel5pys6Kqe44Gu44OG44K544OI55So44K444On?= "
            "=?UTF-8?B?44OW44OQ44Oz44OJ44Or44Gn44GZ44CC?="
        )
        assert _decode_s3_value(folded) == "日本語のテスト用ジョブバンドルです。"
        # Quoted-printable (Q) encoding.
        assert _decode_s3_value("=?UTF-8?Q?Caf=C3=A9?=") == "Café"

    def test_truncate_keeps_encoded_value_within_byte_limit(self):
        value = "あ" * 200  # long, all non-ASCII
        result = _truncate_s3_value(value, METADATA_LIMIT_NAME, field="ojd-name")
        result.encode("ascii")  # header-safe
        assert len(result.encode("utf-8")) <= METADATA_LIMIT_NAME
        assert result.endswith("...")

    def test_truncated_non_ascii_value_still_round_trips_prefix(self):
        value = "描画" * 100
        result = _truncate_s3_value(value, METADATA_LIMIT_NAME, field="ojd-steps")
        decoded = _decode_s3_value(result)
        # Decodes to a prefix of the original followed by the truncation marker.
        assert decoded.endswith("...")
        prefix = decoded[:-3]
        assert prefix and value.startswith(prefix)

    def test_encoded_length_matches_exact_formula(self):
        """Encoded length is exactly 12 + 4*ceil(N/3) — the guarantee that lets
        truncation size the cut in one shot without a retry loop."""
        for value in ("あ", "café", "レンダリング", "job🎬", "描画ステップ" * 5):
            encoded = _encode_s3_value(value)
            n = len(value.encode("utf-8"))
            assert len(encoded) == 12 + 4 * math.ceil(n / 3)

    def test_one_shot_truncation_never_exceeds_limit(self):
        """The computed truncation must fit the limit for every limit, with no
        retry — proves the exact-math approach is correct across the range."""
        non_ascii = "描画ステップ" * 50
        ascii_value = "A" * 400
        mixed = ("hello " + "日本") * 40
        for value in (non_ascii, ascii_value, mixed):
            for limit in range(16, 400):
                result = _truncate_s3_value(value, limit)
                assert len(result.encode("utf-8")) <= limit, (value[:5], limit)
                result.encode("ascii")  # header-safe
                _decode_s3_value(result)  # must not raise


class TestS3BundleExists:
    def _make_repo(self):
        with patch("boto3.Session"):
            repo = S3BundleRepository(
                bucket_name="test-bucket", root_prefix="DC", session=MagicMock()
            )
        repo._s3 = MagicMock()
        return repo

    def test_returns_true_when_exists(self):
        repo = self._make_repo()
        repo._s3.head_object.return_value = {"ETag": '"abc"'}

        assert repo.bundle_exists("my-bundle") is True
        repo._s3.head_object.assert_called_once_with(
            Bucket="test-bucket", Key="DC/job-bundles/my-bundle.ojd"
        )

    def test_returns_false_when_not_found(self):
        repo = self._make_repo()
        repo._s3.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")

        assert repo.bundle_exists("missing") is False


class TestS3UploadArchive:
    def _make_repo(self):
        with patch("boto3.Session"):
            repo = S3BundleRepository(
                bucket_name="test-bucket", root_prefix="DC", session=MagicMock()
            )
        repo._s3 = MagicMock()
        return repo

    def test_uploads_with_metadata(self):
        repo = self._make_repo()
        buf = io.BytesIO(b"fake archive data")

        result = repo.upload_archive(buf, "test-bundle", metadata={"ojd-name": "Test"})

        assert result == "s3://test-bucket/DC/job-bundles/test-bundle.ojd"
        repo._s3.upload_fileobj.assert_called_once()
        call_kwargs = repo._s3.upload_fileobj.call_args[1]
        assert call_kwargs["ExtraArgs"]["Metadata"]["ojd-name"] == "Test"
        # ContentType is advertised as a courtesy hint (not trusted on download).
        assert call_kwargs["ExtraArgs"]["ContentType"] == "application/zip"

    def test_uploads_without_metadata(self):
        repo = self._make_repo()
        buf = io.BytesIO(b"data")

        repo.upload_archive(buf, "simple")

        repo._s3.upload_fileobj.assert_called_once()
        # ContentType is set even when no user metadata is provided.
        call_kwargs = repo._s3.upload_fileobj.call_args[1]
        assert call_kwargs["ExtraArgs"]["ContentType"] == "application/zip"
        assert "Metadata" not in call_kwargs["ExtraArgs"]

    def test_calls_progress_callback(self):
        repo = self._make_repo()
        buf = io.BytesIO(b"data")
        cb = MagicMock()

        repo.upload_archive(buf, "prog", progress_callback=cb)

        call_kwargs = repo._s3.upload_fileobj.call_args[1]
        assert call_kwargs["Callback"] is cb


class TestS3ClearCacheFor:
    def _make_repo(self, fresh_deadline_config):
        with patch("boto3.Session"):
            repo = S3BundleRepository(
                bucket_name="test-bucket", root_prefix="DC", session=MagicMock()
            )
        repo._s3 = MagicMock()
        return repo

    def test_removes_cache_directory(self, fresh_deadline_config, tmp_path):

        repo = self._make_repo(fresh_deadline_config)

        # Create a fake cache entry
        cache_dir = get_bundle_cache_dir()
        os.makedirs(cache_dir, exist_ok=True)

        path = "s3://test-bucket/DC/job-bundles/cached.ojd"
        # Pre-populate cache so clear has something to remove
        repo.clear_cache_for(path)
        # No error raised — passes even if cache didn't exist


class TestGetBundleCacheDir:
    def test_returns_path_under_deadline_cache(self, fresh_deadline_config):

        result = get_bundle_cache_dir()

        assert ".deadline" in result
        assert "cache" in result
        assert "job-bundles" in result

    def test_truncates_multibyte_utf8_correctly(self, tmp_path):
        """Non-ASCII names are encoded to an ASCII-safe form, then byte-truncated.

        S3 user metadata must be US-ASCII, so multi-byte characters are RFC
        2047-encoded (see _encode_s3_value) before the byte limit is applied. The
        stored value stays within the byte limit and remains header-safe.
        """
        # CJK characters are 3 bytes each in UTF-8 and must be encoded for S3.
        bundle = tmp_path / "bundle"
        bundle.mkdir()
        (bundle / "template.yaml").write_text(
            yaml.dump({"name": "日本語テスト" * 50, "steps": []}, allow_unicode=True),
            encoding="utf-8",
        )

        metadata = build_bundle_metadata(str(bundle))

        # Value must be ASCII-safe (header-safe) and within the byte limit.
        metadata["ojd-name"].encode("ascii")  # must not raise
        encoded = metadata["ojd-name"].encode("utf-8")
        assert len(encoded) <= METADATA_LIMIT_NAME
        assert metadata["ojd-name"].endswith("...")
        # It decodes back to a (truncated) prefix of the original CJK name.
        info = _bundle_info_from_s3_metadata(metadata, "s3://bucket/key.ojd")
        assert info is not None
        assert info.name.endswith("...")
        assert ("日本語テスト" * 50).startswith(info.name[:-3])

    def test_metadata_limit_fixture_stays_within_budget(self):
        """Verify the static metadata-limit-test fixture produces valid truncated metadata."""

        # Fixture lives at test/fixtures/bundles/metadata-limit-test relative to repo root
        fixture_path = Path(__file__).parents[3] / "fixtures" / "bundles" / "metadata-limit-test"
        if not fixture_path.is_dir():
            pytest.skip("metadata-limit-test fixture not found")

        metadata = build_bundle_metadata(str(fixture_path))

        assert metadata  # Should produce metadata
        # Total must stay within S3 budget
        total = sum(
            12 + len(k.encode("utf-8")) + len(v.encode("utf-8")) for k, v in metadata.items()
        )
        assert total <= S3_METADATA_TOTAL_BUDGET
        # Name should be truncated (fixture has 300-char name)
        assert metadata["ojd-name"].endswith("...")


class TestCacheKeyTraversal:
    """A crafted bundle name must never let the per-bundle cache dir escape the
    cache root (which would let extraction/rmtree hit the whole cache)."""

    def _assert_under_root(self, key: str):
        root = os.path.realpath(get_bundle_cache_dir())
        resolved = os.path.realpath(os.path.join(get_bundle_cache_dir(), key))
        # Must be a *strict* descendant of the cache root, never the root itself
        # (which is what "<hash>/.." used to normalize to).
        assert resolved != root
        assert is_path_contained(resolved, root)
        assert ".." not in key.replace("\\", "/").split("/")

    @pytest.mark.parametrize(
        "s3_key",
        [
            "prefix/job-bundles/...ojd",  # strips to ".."
            "prefix/job-bundles/..ojd",  # strips to "."
            "prefix/job-bundles/../evil.ojd",
            "prefix/job-bundles/normal.ojd",
        ],
    )
    def test_cache_key_stays_under_root(self, s3_key):
        self._assert_under_root(_cache_key("bucket", s3_key))

    @pytest.mark.parametrize(
        "path",
        [
            "/tmp/bundles/...ojd",
            "/tmp/bundles/..ojd",
            "/tmp/bundles/normal.ojd",
        ],
    )
    def test_local_cache_key_stays_under_root(self, path):
        self._assert_under_root(_local_cache_key(path))


class TestBundleExists:
    """bundle_exists must fail closed: only a 404 means "absent"."""

    def _repo(self):
        repo = MagicMock(spec=S3BundleRepository)
        repo._s3 = MagicMock()
        repo._bucket = "bucket"
        repo._prefix = "prefix/job-bundles/"
        return repo

    def test_returns_true_when_present(self):
        repo = self._repo()
        assert S3BundleRepository.bundle_exists(repo, "render") is True

    def test_returns_false_on_404(self):
        repo = self._repo()
        repo._s3.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")
        assert S3BundleRepository.bundle_exists(repo, "render") is False

    def test_reraises_on_access_denied(self):
        repo = self._repo()
        repo._s3.head_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied"}}, "HeadObject"
        )
        with pytest.raises(ClientError):
            S3BundleRepository.bundle_exists(repo, "render")

    def test_reraises_on_throttle(self):
        repo = self._repo()
        repo._s3.head_object.side_effect = ClientError(
            {"Error": {"Code": "SlowDown"}}, "HeadObject"
        )
        with pytest.raises(ClientError):
            S3BundleRepository.bundle_exists(repo, "render")


class TestHeadObjectCache:
    """The prefetch head cache is preview-only; correctness paths must go live."""

    def _repo(self):
        repo = MagicMock(spec=S3BundleRepository)
        repo._s3 = MagicMock()
        repo._bucket = "bucket"
        repo._head_cache = {"k": {"ContentLength": 999, "ETag": "stale"}}
        repo._s3.head_object.return_value = {"ContentLength": 5, "ETag": "live"}
        return repo

    def test_preview_uses_cache(self):
        repo = self._repo()
        result = S3BundleRepository._head_object(repo, "k")
        assert result["ETag"] == "stale"
        repo._s3.head_object.assert_not_called()

    def test_live_bypasses_cache(self):
        repo = self._repo()
        result = S3BundleRepository._head_object(repo, "k", use_cache=False)
        assert result["ETag"] == "live"
        repo._s3.head_object.assert_called_once_with(Bucket="bucket", Key="k")


class TestVisibilityKeying:
    """Hidden state is keyed by the bundle's path relative to the queue prefix,
    so same-named bundles in different subfolders don't collide."""

    def _make_repo(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "deadline.client.job_bundle._repository.get_bundle_cache_dir",
            lambda: str(tmp_path / "cache"),
        )
        with patch("boto3.Session"):
            repo = S3BundleRepository("test-bucket", "DeadlineCloud", session=MagicMock())
        repo._s3 = MagicMock()
        return repo

    def test_visibility_key_is_prefix_relative(self, tmp_path, monkeypatch):
        repo = self._make_repo(tmp_path, monkeypatch)
        base = "s3://test-bucket/DeadlineCloud/job-bundles/"
        assert repo.visibility_key(base + "blender.ojd") == "blender"
        assert repo.visibility_key(base + "maya/render.ojd") == "maya/render"
        assert repo.visibility_key(base + "nuke/render.ojd") == "nuke/render"

    def test_same_name_different_folders_are_independent(self, tmp_path, monkeypatch):
        repo = self._make_repo(tmp_path, monkeypatch)
        base = "s3://test-bucket/DeadlineCloud/job-bundles/"
        repo.set_bundle_visibility(repo.visibility_key(base + "maya/render.ojd"), hidden=True)
        hidden = repo.get_hidden_set()
        assert "maya/render" in hidden
        assert "nuke/render" not in hidden
