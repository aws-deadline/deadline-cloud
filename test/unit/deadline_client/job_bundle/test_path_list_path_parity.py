# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""A PATH value and the same value as the only item of a LIST[PATH] are validated, resolved and
attached identically, at every step from loading a job bundle to formatting CreateJob."""

from __future__ import annotations

import json
import ntpath
import os
from typing import Any

import pytest
import yaml

from deadline.client.api._submit_job_bundle import _reject_relative_hook_path_values
from deadline.client.exceptions import DeadlineOperationError
from deadline.client.job_bundle import submission
from deadline.client.job_bundle.parameters import (
    apply_job_parameters,
    read_job_bundle_parameters,
    validate_job_parameter,
    validate_job_parameter_value,
)
from deadline.client.job_bundle.submission import AssetReferences

_LONG = "x" * 1025

# The PATH constraints of each case, which a LIST[PATH] sets on its "item".
CONSTRAINTS = {
    "none": {},
    "min-length": {"minLength": 5},
    "max-length": {"maxLength": 8},
    "max-length-over-cap": {"maxLength": 2000},
    "allowed-values": {"allowedValues": ["a.txt", "b.txt"]},
}
VALUES = {
    "relative": "a.txt",
    "empty": "",
    "dot-dot": "sub/../a.txt",
    "posix-absolute": "/abs/a.txt",
    "windows-absolute": r"C:\abs\a.txt",
    "uri": "s3://bucket/key",
    "too-long": _LONG,
    "short": "a.tx",
}
CASES = [
    pytest.param(constraints, value, id=f"{cname}-{vname}")
    for cname, constraints in CONSTRAINTS.items()
    for vname, value in VALUES.items()
]


def _path(constraints: dict, **fields: Any) -> Any:
    return {"name": "P", "type": "PATH", **constraints, **fields}


def _list_path(constraints: dict, **fields: Any) -> Any:
    definition = {"name": "P", "type": "LIST[PATH]", **fields}
    if constraints:
        definition["item"] = dict(constraints)
    if "default" in definition:
        definition["default"] = [definition["default"]]
    return definition


def _outcome(fn) -> tuple:
    """Whether fn succeeds, and its result: an error's message differs by type, so only
    whether it was raised is compared."""
    try:
        return ("ok", fn())
    except (ValueError, TypeError, Exception):
        return ("error",)


def _single(outcome: tuple) -> tuple:
    """The PATH-shaped form of a LIST[PATH] outcome with one item."""
    if outcome[0] == "ok" and isinstance(outcome[1], list) and len(outcome[1]) == 1:
        return ("ok", outcome[1][0])
    return outcome


@pytest.mark.parametrize(("constraints", "value"), CASES)
def test_definition_default(constraints: dict, value: str) -> None:
    path = _outcome(lambda: validate_job_parameter(_path(constraints, default=value))["default"])
    path_list = _outcome(
        lambda: validate_job_parameter(_list_path(constraints, default=value))["default"]
    )
    assert path == _single(path_list)


@pytest.mark.parametrize(("constraints", "value"), CASES)
def test_value(constraints: dict, value: str) -> None:
    path = _outcome(lambda: validate_job_parameter_value(_path(constraints), value))
    path_list = _outcome(lambda: validate_job_parameter_value(_list_path(constraints), [value]))
    assert path == _single(path_list)


@pytest.mark.parametrize("expr", [False, True], ids=["no-expr", "expr"])
@pytest.mark.parametrize(("constraints", "value"), CASES)
def test_bundle_default(fresh_deadline_config, tmp_path, constraints, value, expr) -> None:
    def load(definition: dict) -> tuple:
        bundle_dir = tmp_path / definition["type"].replace("[", "_").replace("]", "")
        bundle_dir.mkdir()
        template = {
            "specificationVersion": "jobtemplate-2023-09",
            **({"extensions": ["EXPR"]} if expr else {}),
            "name": "J",
            "parameterDefinitions": [definition],
            "steps": [{"name": "S", "script": {"actions": {"onRun": {"command": "echo"}}}}],
        }
        (bundle_dir / "template.yaml").write_text(yaml.safe_dump(template), encoding="utf8")

        def read() -> Any:
            (parameter,) = read_job_bundle_parameters(str(bundle_dir))
            resolved = parameter.get("value", parameter["default"])
            # Compare the paths relative to each bundle, which differ only in name.
            rel = lambda p: p.replace(str(bundle_dir), "<bundle>")  # noqa: E731
            return [rel(p) for p in resolved] if isinstance(resolved, list) else rel(resolved)

        return _outcome(read)

    path = load(_path(constraints, objectType="FILE", default=value))
    path_list = load(_list_path(constraints, objectType="FILE", default=value))
    assert path == _single(path_list)


@pytest.mark.parametrize("expr", [False, True], ids=["no-expr", "expr"])
@pytest.mark.parametrize("data_flow", ["NONE", "IN", "OUT", "INOUT"])
@pytest.mark.parametrize("object_type", ["FILE", "DIRECTORY"])
@pytest.mark.parametrize(("constraints", "value"), CASES)
def test_cli_value_and_attachments(
    temp_cwd, constraints, value, object_type, data_flow, expr
) -> None:
    def apply(definition: dict, cli_value: Any) -> tuple:
        def run() -> dict:
            parameters = [definition]
            references = AssetReferences()
            apply_job_parameters(
                [{"name": "P", "value": cli_value}],
                "bundle",
                parameters,  # type: ignore[arg-type]
                references,
                allow_uri_path_values=expr,
            )
            resolved = parameters[0].get("value")
            # An empty --parameter value names no path. For a PATH it keeps the bundle's value
            # (here none) unless allowedValues constrains it, and a LIST[PATH] holds it as an
            # empty item; both mean "no path".
            if isinstance(resolved, list):
                resolved = resolved[0]
            if resolved is None:
                resolved = ""
            return {
                "value": resolved,
                "inputs": sorted(references.input_filenames | references.input_directories),
                "outputs": sorted(references.output_directories),
                "referenced": sorted(references.referenced_paths),
            }

        return _outcome(run)

    fields = {"objectType": object_type, "dataFlow": data_flow}
    path = apply(_path(constraints, **fields), value)
    path_list = apply(_list_path(constraints, **fields), [value])
    assert path == path_list


@pytest.mark.parametrize("expr", [False, True], ids=["no-expr", "expr"])
@pytest.mark.parametrize("value", list(VALUES.values()), ids=list(VALUES))
def test_hook_value(value: str, expr: bool) -> None:
    def check(parameters: dict, definition: dict) -> tuple:
        return _outcome(
            lambda: _reject_relative_hook_path_values(
                parameters, [definition], path_module=ntpath, allow_uri_path_values=expr
            )
        )

    assert check({"P": value}, {"name": "P", "type": "PATH"}) == check(
        {"P": [value]}, {"name": "P", "type": "LIST[PATH]"}
    )


@pytest.mark.parametrize("value", list(VALUES.values()), ids=list(VALUES))
def test_create_job_formatting(value: str) -> None:
    def member(definition: Any, member_name: str) -> tuple:
        return _outcome(
            lambda: submission.split_parameter_args([definition], "bundle")[1]["P"][member_name]
        )

    path = member({"name": "P", "type": "PATH", "value": value}, "path")
    path_list = member(
        {"name": "P", "type": "LIST[PATH]", "value": json.dumps([value])}, "pathList"
    )
    assert path == _single(path_list)


@pytest.mark.parametrize(
    "definition",
    [
        pytest.param({"minLength": 5, "maxLength": 4}, id="min-over-max"),
        pytest.param({"minLength": 1025}, id="min-over-cap"),
        pytest.param({"allowedValues": ["abc"], "maxLength": 2}, id="allowed-too-long"),
        pytest.param({"allowedValues": []}, id="allowed-empty"),
        pytest.param({"allowedValues": [1]}, id="allowed-not-str"),
        pytest.param({"minLength": -1}, id="negative"),
    ],
)
def test_unsatisfiable_constraints(definition: dict) -> None:
    with pytest.raises((ValueError, TypeError)):
        validate_job_parameter(_path(definition))
    with pytest.raises((ValueError, TypeError)):
        validate_job_parameter(_list_path(definition))


def test_path_value_over_create_job_limit_is_rejected_before_submission() -> None:
    with pytest.raises(ValueError, match="longer than the maximum of 1024 characters"):
        validate_job_parameter_value({"name": "P", "type": "PATH"}, _LONG)  # type: ignore[typeddict-item]


def test_empty_path_with_data_flow_none_is_not_a_referenced_path() -> None:
    references = AssetReferences()
    apply_job_parameters(
        [{"name": "P", "value": ""}],
        "bundle",
        [{"name": "P", "type": "PATH", "allowedValues": [""], "dataFlow": "NONE"}],  # type: ignore[list-item]
        references,
    )
    assert references.referenced_paths == set()


def test_os_independent_bundle_paths(fresh_deadline_config, tmp_path) -> None:
    """Sanity check that the bundle comparison above resolves real defaults."""
    (tmp_path / "template.yaml").write_text(
        yaml.safe_dump(
            {
                "specificationVersion": "jobtemplate-2023-09",
                "name": "J",
                "parameterDefinitions": [{"name": "P", "type": "PATH", "default": "a.txt"}],
                "steps": [{"name": "S", "script": {"actions": {"onRun": {"command": "echo"}}}}],
            }
        ),
        encoding="utf8",
    )
    (parameter,) = read_job_bundle_parameters(str(tmp_path))
    assert parameter["value"] == os.path.join(str(tmp_path), "a.txt")


def _write_template(bundle_dir: Any, definition: dict) -> None:
    template = {
        "specificationVersion": "jobtemplate-2023-09",
        "extensions": ["EXPR"],
        "name": "J",
        "parameterDefinitions": [definition],
        "steps": [{"name": "S", "script": {"actions": {"onRun": {"command": "echo"}}}}],
    }
    (bundle_dir / "template.yaml").write_text(yaml.safe_dump(template), encoding="utf8")


@pytest.mark.parametrize("list_type", [False, True], ids=["PATH", "LIST[PATH]"])
def test_join_is_lexical(fresh_deadline_config, tmp_path, temp_cwd, list_type) -> None:
    """OpenJD specifies a lexical join. On Windows, os.path.abspath also strips trailing dots
    and spaces from each component, which would turn "..." into an empty component."""
    as_value = (lambda v: [v]) if list_type else (lambda v: v)
    definition = {"name": "P", "type": "LIST[PATH]" if list_type else "PATH"}
    _write_template(tmp_path, {**definition, "default": as_value("a/../b/...")})
    (parameter,) = read_job_bundle_parameters(str(tmp_path))
    assert parameter["value"] == as_value(os.path.join(str(tmp_path), "b", "..."))

    parameters = [dict(definition)]
    apply_job_parameters(
        [{"name": "P", "value": as_value("c/...")}],
        "bundle",
        parameters,  # type: ignore[arg-type]
        AssetReferences(),
    )
    assert parameters[0]["value"] == as_value(os.path.join(os.getcwd(), "c", "..."))


@pytest.mark.parametrize("list_type", [False, True], ids=["PATH", "LIST[PATH]"])
def test_default_allowed_values_checked_after_join(
    fresh_deadline_config, tmp_path, list_type
) -> None:
    """OpenJD checks a default against allowedValues as written at template validation, and
    after it is joined with the template directory at job creation, so both must be allowed."""
    joined = os.path.join(str(tmp_path), "config", "a")

    def definition(allowed: list) -> dict:
        if list_type:
            return {
                "name": "P",
                "type": "LIST[PATH]",
                "default": ["config/a"],
                "item": {"allowedValues": allowed},
            }
        return {"name": "P", "type": "PATH", "default": "config/a", "allowedValues": allowed}

    _write_template(tmp_path, definition(["config/a", "config/b"]))
    with pytest.raises(
        DeadlineOperationError,
        match="(?s)joined with the Job Bundle directory.*not an allowed value",
    ):
        read_job_bundle_parameters(str(tmp_path))

    _write_template(tmp_path, definition(["config/a", joined]))
    (parameter,) = read_job_bundle_parameters(str(tmp_path))
    assert parameter["value"] == ([joined] if list_type else joined)
