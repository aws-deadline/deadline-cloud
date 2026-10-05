# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Test cases for LIST[PATH] job parameters: definition and value validation, UI control
selection, bundle loading with bundle-relative defaults, job attachments asset references,
and CreateJob request formatting."""

from __future__ import annotations

import json
import os
from typing import Any

import pytest
import yaml

from deadline.client.exceptions import DeadlineOperationError
from deadline.client.job_bundle import submission
from deadline.client.job_bundle.parameters import (
    JobParameter,
    apply_job_parameters,
    get_ui_control_for_parameter_definition,
    merge_queue_job_parameters,
    read_job_bundle_parameters,
    validate_job_parameter,
    validate_job_parameter_value,
)
from deadline.client.job_bundle.submission import AssetReferences

# The valid parameter definitions from the OpenJD conformance test
# conformance-tests/2023-09/EXPR/job_templates/2.12--list-path-param.yaml and
# 2.12--list-path-item-allowed-values.yaml
CONFORMANCE_VALID_DEFINITIONS: list[dict[str, Any]] = [
    {"name": "Minimal", "type": "LIST[PATH]"},
    {"name": "WithDefault", "type": "LIST[PATH]", "default": ["/tmp/a.exr", "/tmp/b.exr"]},
    {"name": "EmptyDefault", "type": "LIST[PATH]", "default": []},
    {"name": "SingleElement", "type": "LIST[PATH]", "default": ["/tmp/only.exr"]},
    {
        "name": "FileIn",
        "type": "LIST[PATH]",
        "objectType": "FILE",
        "dataFlow": "IN",
        "default": ["/input/a.exr"],
    },
    {
        "name": "FileOut",
        "type": "LIST[PATH]",
        "objectType": "FILE",
        "dataFlow": "OUT",
        "default": ["/output/a.exr"],
    },
    {
        "name": "FileInOut",
        "type": "LIST[PATH]",
        "objectType": "FILE",
        "dataFlow": "INOUT",
        "default": ["/data/a.exr"],
    },
    {
        "name": "DirNone",
        "type": "LIST[PATH]",
        "objectType": "DIRECTORY",
        "dataFlow": "NONE",
        "default": ["/mnt/share"],
    },
    {
        "name": "WithMinMax",
        "type": "LIST[PATH]",
        "default": ["/a", "/b"],
        "minLength": 1,
        "maxLength": 50,
    },
    {
        "name": "ItemLength",
        "type": "LIST[PATH]",
        "default": ["/tmp/file"],
        "item": {"minLength": 1, "maxLength": 200},
    },
    {
        "name": "FullyConstrained",
        "type": "LIST[PATH]",
        "default": ["/opt/a", "/opt/b"],
        "minLength": 1,
        "maxLength": 10,
        "item": {"minLength": 1, "maxLength": 100},
    },
    {
        "name": "WithDescription",
        "type": "LIST[PATH]",
        "default": ["/tmp"],
        "description": "A list of paths",
    },
    {
        "name": "WithInputFileList",
        "type": "LIST[PATH]",
        "objectType": "FILE",
        "dataFlow": "IN",
        "default": ["/tmp/a"],
        "userInterface": {"control": "CHOOSE_INPUT_FILE_LIST", "label": "Input Files"},
    },
    {
        "name": "WithOutputFileList",
        "type": "LIST[PATH]",
        "objectType": "FILE",
        "dataFlow": "OUT",
        "default": ["/tmp/a"],
        "userInterface": {"control": "CHOOSE_OUTPUT_FILE_LIST"},
    },
    {
        "name": "WithDirList",
        "type": "LIST[PATH]",
        "objectType": "DIRECTORY",
        "default": ["/tmp"],
        "userInterface": {"control": "CHOOSE_DIRECTORY_LIST"},
    },
    {
        "name": "WithHidden",
        "type": "LIST[PATH]",
        "default": ["/tmp"],
        "userInterface": {"control": "HIDDEN"},
    },
    {
        "name": "Scenes",
        "type": "LIST[PATH]",
        "default": ["/assets/a.blend", "/assets/b.blend"],
        "item": {"allowedValues": ["/assets/a.blend", "/assets/b.blend", "/assets/c.blend"]},
    },
    {
        "name": "WithFileFilters",
        "type": "LIST[PATH]",
        "objectType": "FILE",
        "userInterface": {
            "fileFilters": [{"label": "Blender", "patterns": ["*.blend"]}],
            "fileFilterDefault": {"label": "Blender", "patterns": ["*.blend"]},
        },
    },
]


@pytest.mark.parametrize(
    "definition", [pytest.param(d, id=d["name"]) for d in CONFORMANCE_VALID_DEFINITIONS]
)
def test_validate_job_parameter_list_path_valid(definition: dict[str, Any]) -> None:
    # Does not raise.
    validate_job_parameter(definition)


# The invalid job templates from the OpenJD conformance tests
# conformance-tests/2023-09/EXPR/job_templates/2.12--list-path-*.invalid.yaml
@pytest.mark.parametrize(
    ("definition", "error_type", "match"),
    [
        pytest.param(
            {"type": "LIST[PATH]", "default": "/tmp/file"},
            TypeError,
            'got str for "default" but type "LIST\\[PATH\\]" expects list',
            id="scalar-not-list",
        ),
        pytest.param(
            {"type": "LIST[PATH]", "default": [123, 456]},
            TypeError,
            "item 0 is 123",
            id="wrong-item-type",
        ),
        pytest.param(
            {"type": "LIST[PATH]", "default": [], "minLength": 1},
            ValueError,
            "has 0 items but minLength is 1",
            id="too-short",
        ),
        pytest.param(
            {"type": "LIST[PATH]", "default": ["/a", "/b", "/c"], "maxLength": 2},
            ValueError,
            "has 3 items but at most 2 are allowed",
            id="too-long",
        ),
        pytest.param(
            {"type": "LIST[PATH]", "default": [""], "item": {"minLength": 1}},
            ValueError,
            "item 0 '' is shorter than item minLength 1",
            id="item-too-short",
        ),
        pytest.param(
            {
                "type": "LIST[PATH]",
                "default": ["/assets/a.blend", "/assets/z.blend"],
                "item": {"allowedValues": ["/assets/a.blend", "/assets/b.blend"]},
            },
            ValueError,
            "item 1 '/assets/z.blend' is not an allowed value",
            id="item-not-in-allowed",
        ),
    ],
)
def test_validate_job_parameter_list_path_invalid_default(
    definition: dict[str, Any], error_type: type, match: str
) -> None:
    with pytest.raises(error_type, match=match):
        validate_job_parameter({"name": "Files", **definition})


@pytest.mark.parametrize(
    ("field", "field_value"),
    [
        pytest.param("allowedValues", ["/a"], id="allowedValues"),
        pytest.param("minValue", 0, id="minValue"),
        pytest.param("maxValue", 1, id="maxValue"),
    ],
)
def test_validate_job_parameter_list_path_disallowed_fields(field: str, field_value: Any) -> None:
    with pytest.raises(ValueError) as ctx:
        validate_job_parameter({"name": "foo", "type": "LIST[PATH]", field: field_value})
    assert (
        str(ctx.value)
        == f'Job parameter "foo" has "{field}" but type "LIST[PATH]" does not support it'
    )


@pytest.mark.parametrize(
    ("field", "field_value", "match"),
    [
        pytest.param("objectType", "LINK", 'got LINK for "objectType"', id="objectType"),
        pytest.param("dataFlow", "SIDEWAYS", 'got "SIDEWAYS" for "dataFlow"', id="dataFlow"),
    ],
)
def test_validate_job_parameter_list_path_invalid_path_fields(
    field: str, field_value: str, match: str
) -> None:
    with pytest.raises(ValueError, match=match):
        validate_job_parameter({"name": "foo", "type": "LIST[PATH]", field: field_value})


@pytest.mark.parametrize(
    ("item", "error_type", "match"),
    [
        pytest.param(["a"], TypeError, 'got list for "item" but expected dict', id="not-dict"),
        pytest.param(
            {"minValue": 1},
            ValueError,
            '"item" -> "minValue" but type "LIST\\[PATH\\]" only supports',
            id="unknown-field",
        ),
        pytest.param(
            {"allowedValues": ["/a", 1]},
            TypeError,
            '"item" -> "allowedValues" \\[1\\] but expected str',
            id="allowed-non-str",
        ),
        pytest.param(
            {"minLength": 5, "maxLength": 4},
            ValueError,
            '"item" -> "minLength" 5 greater than the maximum length of 4',
            id="item-min-over-max",
        ),
        pytest.param(
            {"minLength": 1025},
            ValueError,
            "greater than the maximum length of 1024",
            id="item-min-over-cap",
        ),
    ],
)
def test_validate_job_parameter_list_path_invalid_item(
    item: Any, error_type: type, match: str
) -> None:
    with pytest.raises(error_type, match=match):
        validate_job_parameter({"name": "foo", "type": "LIST[PATH]", "item": item})


@pytest.mark.parametrize("list_type", ["LIST[STRING]", "LIST[INT]", "LIST[FLOAT]", "LIST[BOOL]"])
@pytest.mark.parametrize("field", ["objectType", "dataFlow"])
def test_path_fields_rejected_for_other_list_types(list_type: str, field: str) -> None:
    value = "FILE" if field == "objectType" else "IN"
    with pytest.raises(ValueError, match=f'has "{field}" but type'):
        validate_job_parameter({"name": "foo", "type": list_type, field: value})


LIST_PARAM: JobParameter = {"name": "Scenes", "type": "LIST[PATH]"}


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(["/a.blend", "rel/b.blend"], ["/a.blend", "rel/b.blend"], id="list"),
        pytest.param([], [], id="empty-list"),
        pytest.param(["", "s3://bucket/key"], ["", "s3://bucket/key"], id="empty-and-uri"),
        pytest.param('["/a", "b"]', ["/a", "b"], id="json-string"),
    ],
)
def test_validate_job_parameter_value_list_path(value: Any, expected: list[str]) -> None:
    # Validation does not make items absolute; that happens when parameters are applied.
    assert validate_job_parameter_value(LIST_PARAM, value) == expected


@pytest.mark.parametrize(
    ("value", "error_type", "match"),
    [
        pytest.param("/a.blend", ValueError, "is not a JSON array of paths", id="bare-path"),
        pytest.param("[1]", TypeError, "item 0 is 1", id="json-int"),
        pytest.param(["/a", None], TypeError, "item 1 is None", id="none-item"),
        pytest.param(5, TypeError, "has type LIST\\[PATH\\]", id="int"),
    ],
)
def test_validate_job_parameter_value_list_path_invalid(
    value: Any, error_type: type, match: str
) -> None:
    with pytest.raises(error_type, match=match):
        validate_job_parameter_value(LIST_PARAM, value)


def test_validate_job_parameter_value_list_path_service_caps() -> None:
    """CreateJob's pathList caps of 64 items of at most 1024 characters apply even when the
    template sets larger limits."""
    validate_job_parameter_value(LIST_PARAM, ["/x"] * 64)
    with pytest.raises(ValueError, match="has 65 items but at most 64 are allowed"):
        validate_job_parameter_value({**LIST_PARAM, "maxLength": 100}, ["/x"] * 65)
    validate_job_parameter_value(LIST_PARAM, ["x" * 1024])
    with pytest.raises(ValueError, match="longer than the maximum of 1024"):
        validate_job_parameter_value(LIST_PARAM, ["x" * 1025])


@pytest.mark.parametrize(
    ("definition", "expected"),
    [
        ({}, "CHOOSE_DIRECTORY_LIST"),
        ({"objectType": "DIRECTORY", "dataFlow": "OUT"}, "CHOOSE_DIRECTORY_LIST"),
        ({"objectType": "FILE"}, "CHOOSE_INPUT_FILE_LIST"),
        ({"objectType": "FILE", "dataFlow": "IN"}, "CHOOSE_INPUT_FILE_LIST"),
        ({"objectType": "FILE", "dataFlow": "INOUT"}, "CHOOSE_INPUT_FILE_LIST"),
        ({"objectType": "FILE", "dataFlow": "OUT"}, "CHOOSE_OUTPUT_FILE_LIST"),
        ({"userInterface": {"control": "CHOOSE_INPUT_FILE_LIST"}}, "CHOOSE_INPUT_FILE_LIST"),
        ({"userInterface": {"control": "CHOOSE_OUTPUT_FILE_LIST"}}, "CHOOSE_OUTPUT_FILE_LIST"),
        ({"userInterface": {"control": "HIDDEN"}}, "HIDDEN"),
    ],
)
def test_ui_control_for_list_path(definition: Any, expected: str) -> None:
    param: Any = {"name": "X", "type": "LIST[PATH]", **definition}
    assert get_ui_control_for_parameter_definition(param) == expected


@pytest.mark.parametrize(
    "control",
    ["CHOOSE_INPUT_FILE", "CHOOSE_DIRECTORY", "LINE_EDIT_LIST", "DROPDOWN_LIST", "LINE_EDIT"],
)
def test_ui_control_for_list_path_unsupported(control: str) -> None:
    definition: Any = {"name": "X", "type": "LIST[PATH]", "userInterface": {"control": control}}
    with pytest.raises(DeadlineOperationError, match="unsupported control"):
        get_ui_control_for_parameter_definition(definition)


@pytest.mark.parametrize(
    "control", ["CHOOSE_INPUT_FILE_LIST", "CHOOSE_OUTPUT_FILE_LIST", "CHOOSE_DIRECTORY_LIST"]
)
@pytest.mark.parametrize("other_type", ["PATH", "STRING", "LIST[STRING]"])
def test_path_list_controls_rejected_for_other_types(control: str, other_type: str) -> None:
    definition: Any = {"name": "X", "type": other_type, "userInterface": {"control": control}}
    with pytest.raises(DeadlineOperationError, match=f"unsupported control '{control}'"):
        get_ui_control_for_parameter_definition(definition)


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(["/a.blend", "/b.blend"], id="list"),
        pytest.param('["/a.blend", "/b.blend"]', id="json-string"),
    ],
)
def test_split_parameter_args_list_path(value: Any) -> None:
    params: list[JobParameter] = [
        {"name": "Scenes", "type": "LIST[PATH]", "value": value},
        {"name": "None", "type": "LIST[PATH]", "value": []},
    ]
    _, job_params = submission.split_parameter_args(params, "test_bundle")
    assert job_params == {
        "Scenes": {"pathList": ["/a.blend", "/b.blend"]},
        "None": {"pathList": []},
    }


def test_split_parameter_args_rejects_invalid_list_path() -> None:
    params: list[JobParameter] = [{"name": "Scenes", "type": "LIST[PATH]", "value": "/a"}]
    with pytest.raises(DeadlineOperationError, match="(?s)not a JSON array of paths.*test_bundle"):
        submission.split_parameter_args(params, "test_bundle")


LIST_PATH_TEMPLATE = """
specificationVersion: jobtemplate-2023-09
extensions:
- EXPR
name: ListPathJob
parameterDefinitions:
- name: Scenes
  type: list[path]
  objectType: FILE
  dataFlow: IN
  default: DEFAULT
steps:
- name: Render
  parameterSpace:
    taskParameterDefinitions:
    - name: Scene
      type: PATH
      range: "{{RawParam.Scenes}}"
  script:
    actions:
      onRun:
        command: echo
        args: ["{{Task.Param.Scene}}"]
"""


def _write_bundle(bundle_dir: str, default: Any, parameter_values: Any = None) -> None:
    with open(os.path.join(bundle_dir, "template.yaml"), "w", encoding="utf8") as f:
        f.write(LIST_PATH_TEMPLATE.replace("DEFAULT", json.dumps(default)))
    if parameter_values is not None:
        with open(os.path.join(bundle_dir, "parameter_values.json"), "w", encoding="utf8") as f:
            json.dump({"parameterValues": parameter_values}, f)


def test_read_job_bundle_parameters_list_path_resolves_default_items(
    fresh_deadline_config, temp_job_bundle_dir
) -> None:
    """Each item of a LIST[PATH] default is relative to the job bundle, like a PATH default.
    Empty items stay empty."""
    _write_bundle(temp_job_bundle_dir, ["scenes/a.blend", "", "b.blend"])

    (param,) = read_job_bundle_parameters(temp_job_bundle_dir)

    assert param["type"] == "LIST[PATH]"
    assert param["default"] == ["scenes/a.blend", "", "b.blend"]
    assert param["value"] == [
        os.path.normpath(os.path.join(os.path.abspath(temp_job_bundle_dir), "scenes", "a.blend")),
        "",
        os.path.normpath(os.path.join(os.path.abspath(temp_job_bundle_dir), "b.blend")),
    ]


def test_read_job_bundle_parameters_list_path_value_is_kept(
    fresh_deadline_config, temp_job_bundle_dir
) -> None:
    _write_bundle(
        temp_job_bundle_dir, ["a.blend"], [{"name": "Scenes", "value": ["/abs/x.blend", "y"]}]
    )
    (param,) = read_job_bundle_parameters(temp_job_bundle_dir)
    assert param["value"] == ["/abs/x.blend", "y"]


def test_read_job_bundle_parameters_list_path_allowed_values_checked_after_join(
    fresh_deadline_config, temp_job_bundle_dir
) -> None:
    """As OpenJD specifies, a default item constrained by item allowedValues is still joined
    with the job bundle directory, and the joined path must be an allowed value. A relative
    allowed value can't match it, so the bundle is rejected when it is loaded."""
    with open(os.path.join(temp_job_bundle_dir, "template.yaml"), "w", encoding="utf8") as f:
        f.write(
            LIST_PATH_TEMPLATE.replace(
                "default: DEFAULT", 'default: ["a"]\n  item:\n    allowedValues: ["a", "b"]'
            )
        )
    with pytest.raises(
        DeadlineOperationError, match="(?s)default for parameter 'Scenes'.*is not an allowed value"
    ):
        read_job_bundle_parameters(temp_job_bundle_dir)


@pytest.mark.parametrize(
    ("default_item", "match"),
    [
        pytest.param(os.path.abspath("/elsewhere/a.blend"), "is absolute", id="absolute"),
        pytest.param("../outside.blend", "outside of Job Bundle directory", id="outside"),
    ],
)
def test_read_job_bundle_parameters_list_path_default_must_be_in_bundle(
    fresh_deadline_config, temp_job_bundle_dir, default_item: str, match: str
) -> None:
    _write_bundle(temp_job_bundle_dir, ["ok.blend", default_item])
    with pytest.raises(
        DeadlineOperationError,
        match=rf"Default LIST\[PATH\] item 1 '.*' for parameter 'Scenes' .*{match}",
    ):
        read_job_bundle_parameters(temp_job_bundle_dir)


@pytest.mark.parametrize(
    ("item", "error_type", "match"),
    [
        pytest.param(None, TypeError, 'got NoneType for "item"', id="null"),
        pytest.param(5, TypeError, 'got int for "item"', id="int"),
    ],
)
def test_read_job_bundle_parameters_list_path_malformed_item(
    fresh_deadline_config, temp_job_bundle_dir, item: Any, error_type: type, match: str
) -> None:
    """A malformed item is reported by validation, not by the default resolution that
    runs before it."""
    _write_bundle(temp_job_bundle_dir, ["a.blend"])
    template_path = os.path.join(temp_job_bundle_dir, "template.yaml")
    with open(template_path, encoding="utf8") as f:
        template = yaml.safe_load(f)
    template["parameterDefinitions"][0]["item"] = item
    with open(template_path, "w", encoding="utf8") as f:
        yaml.safe_dump(template, f)
    with pytest.raises(error_type, match=match):
        read_job_bundle_parameters(temp_job_bundle_dir)


def test_read_job_bundle_parameters_list_path_invalid_default(
    fresh_deadline_config, temp_job_bundle_dir
) -> None:
    _write_bundle(temp_job_bundle_dir, ["a", 5])
    with pytest.raises(TypeError, match="item 1 is 5"):
        read_job_bundle_parameters(temp_job_bundle_dir)


@pytest.mark.parametrize(
    ("object_type", "data_flow", "expected"),
    [
        pytest.param(
            "FILE",
            "IN",
            AssetReferences(input_filenames={"/in/a.exr", "/in/b.exr"}),
            id="file-in",
        ),
        pytest.param(
            "DIRECTORY",
            "IN",
            AssetReferences(input_directories={"/in/a.exr", "/in/b.exr"}),
            id="dir-in",
        ),
        pytest.param(
            "FILE",
            "OUT",
            AssetReferences(output_directories={"/in"}),
            id="file-out",
        ),
        pytest.param(
            "DIRECTORY",
            "OUT",
            AssetReferences(output_directories={"/in/a.exr", "/in/b.exr"}),
            id="dir-out",
        ),
        pytest.param(
            "FILE",
            "INOUT",
            AssetReferences(input_filenames={"/in/a.exr", "/in/b.exr"}, output_directories={"/in"}),
            id="file-inout",
        ),
        pytest.param(
            "DIRECTORY",
            "INOUT",
            AssetReferences(
                input_directories={"/in/a.exr", "/in/b.exr"},
                output_directories={"/in/a.exr", "/in/b.exr"},
            ),
            id="dir-inout",
        ),
        pytest.param(
            "FILE",
            "NONE",
            AssetReferences(referenced_paths={"/in/a.exr", "/in/b.exr"}),
            id="file-none",
        ),
        pytest.param(
            None,
            None,
            AssetReferences(referenced_paths={"/in/a.exr", "/in/b.exr"}),
            id="defaults",
        ),
    ],
)
def test_apply_job_parameters_list_path_asset_references(
    object_type: Any, data_flow: Any, expected: AssetReferences
) -> None:
    """Each non-empty item of a LIST[PATH] becomes an asset reference, as if it were a PATH
    parameter with the same objectType and dataFlow."""
    param: Any = {"name": "Paths", "type": "LIST[PATH]", "value": ["/in/a.exr", "", "/in/b.exr"]}
    if object_type:
        param["objectType"] = object_type
    if data_flow:
        param["dataFlow"] = data_flow
    asset_references = AssetReferences()

    apply_job_parameters([], "bundle", [param], asset_references)

    assert asset_references == expected


def test_apply_job_parameters_list_path_from_default_and_json_value() -> None:
    asset_references = AssetReferences()
    params: Any = [
        {
            "name": "FromDefault",
            "type": "LIST[PATH]",
            "objectType": "FILE",
            "dataFlow": "IN",
            "default": ["/d/a"],
        },
        {
            "name": "FromJson",
            "type": "LIST[PATH]",
            "objectType": "FILE",
            "dataFlow": "IN",
            "value": '["/j/a"]',
        },
        # A value that is not a list of paths is left for split_parameter_args to report.
        {"name": "Bad", "type": "LIST[PATH]", "dataFlow": "IN", "value": "not json"},
    ]
    apply_job_parameters([], "bundle", params, asset_references)
    assert asset_references == AssetReferences(input_filenames={"/d/a", "/j/a"})


def test_apply_job_parameters_list_path_cli_items_made_absolute(temp_cwd) -> None:
    """Like a PATH, a LIST[PATH] value from job_parameters has relative items resolved against
    the current working directory. Empty items stay empty."""
    absolute = os.path.abspath(os.path.join(os.sep, "abs", "a.exr"))
    param: Any = {"name": "Paths", "type": "LIST[PATH]", "objectType": "FILE", "dataFlow": "IN"}
    asset_references = AssetReferences()

    apply_job_parameters(
        [{"name": "Paths", "value": json.dumps(["rel/b.exr", "", absolute])}],
        "bundle",
        [param],
        asset_references,
    )

    rel = os.path.abspath(os.path.join("rel", "b.exr"))
    assert param["value"] == [rel, "", absolute]
    assert asset_references == AssetReferences(input_filenames={rel, absolute})


def test_apply_job_parameters_list_path_allowed_values_checked_after_join(temp_cwd) -> None:
    """As OpenJD specifies, an item constrained by item allowedValues is still joined with the
    working directory, and the joined path is what allowedValues must contain."""
    allowed = os.path.join(os.getcwd(), "b")
    param: Any = {
        "name": "Paths",
        "type": "LIST[PATH]",
        "item": {"allowedValues": ["b", allowed]},
    }
    apply_job_parameters([{"name": "Paths", "value": ["b"]}], "bundle", [param], AssetReferences())
    assert param["value"] == [allowed]
    _, job_params = submission.split_parameter_args([param], "bundle")
    assert job_params["Paths"] == {"pathList": [allowed]}

    relative_only: Any = {"name": "Paths", "type": "LIST[PATH]", "item": {"allowedValues": ["b"]}}
    apply_job_parameters(
        [{"name": "Paths", "value": ["b"]}], "bundle", [relative_only], AssetReferences()
    )
    with pytest.raises(DeadlineOperationError, match="is not an allowed value"):
        submission.split_parameter_args([relative_only], "bundle")


def test_apply_job_parameters_list_path_invalid_cli_value_unchanged() -> None:
    param: Any = {"name": "Paths", "type": "LIST[PATH]"}
    apply_job_parameters([{"name": "Paths", "value": "a,b"}], "bundle", [param], AssetReferences())
    assert param["value"] == "a,b"


def test_merge_queue_job_parameters_list_path_mismatch() -> None:
    queue_param: JobParameter = {"name": "Paths", "type": "LIST[PATH]", "default": []}
    job_param: JobParameter = {"name": "Paths", "type": "LIST[PATH]", "dataFlow": "IN"}
    with pytest.raises(DeadlineOperationError, match="Paths: differences for fields"):
        merge_queue_job_parameters(job_parameters=[job_param], queue_parameters=[queue_param])
    with pytest.raises(DeadlineOperationError, match="differences for fields \"\\['type'\\]\""):
        merge_queue_job_parameters(
            job_parameters=[{"name": "Paths", "type": "LIST[STRING]", "default": []}],
            queue_parameters=[queue_param],
        )


URI_ITEMS = ["s3://bucket/scenes/a.blend", "https://example.com/x.exr"]


@pytest.mark.parametrize("data_flow", ["NONE", "IN", "OUT", "INOUT"])
@pytest.mark.parametrize("object_type", ["FILE", "DIRECTORY"])
def test_apply_job_parameters_uri_items_kept_and_not_attached(
    temp_cwd, data_flow: str, object_type: str, caplog
) -> None:
    """With the EXPR extension, a URI is neither made absolute nor given to job
    attachments, for a PATH and for each LIST[PATH] item. Other items are unchanged."""
    local = os.path.abspath("local.exr")
    path_param: Any = {
        "name": "One",
        "type": "PATH",
        "objectType": object_type,
        "dataFlow": data_flow,
    }
    list_param: Any = {
        "name": "Many",
        "type": "LIST[PATH]",
        "objectType": object_type,
        "dataFlow": data_flow,
    }
    asset_references = AssetReferences()

    with caplog.at_level("INFO"):
        apply_job_parameters(
            [
                {"name": "One", "value": URI_ITEMS[0]},
                {"name": "Many", "value": json.dumps([URI_ITEMS[1], "local.exr"])},
            ],
            "bundle",
            [path_param, list_param],
            asset_references,
            allow_uri_path_values=True,
        )

    assert path_param["value"] == URI_ITEMS[0]
    assert list_param["value"] == [URI_ITEMS[1], local]
    expected = AssetReferences()
    _local_only: Any = {**list_param, "value": [local]}
    apply_job_parameters([], "bundle", [_local_only], expected)
    assert asset_references == expected
    if data_flow != "NONE":
        assert "is a URI, so job attachments do not transfer it" in caplog.text


def test_apply_job_parameters_uri_without_expr_is_a_path(temp_cwd) -> None:
    """URI values are an EXPR feature; without it, PATH values keep their old handling."""
    param: Any = {"name": "One", "type": "PATH", "dataFlow": "IN"}
    asset_references = AssetReferences()
    apply_job_parameters(
        [{"name": "One", "value": URI_ITEMS[0]}], "bundle", [param], asset_references
    )
    assert param["value"] == os.path.abspath(URI_ITEMS[0])
    assert asset_references.input_directories == {os.path.abspath(URI_ITEMS[0])}


def test_apply_job_parameters_uri_from_bundle_value_not_attached() -> None:
    param: Any = {
        "name": "Many",
        "type": "LIST[PATH]",
        "objectType": "FILE",
        "dataFlow": "IN",
        "value": URI_ITEMS,
    }
    asset_references = AssetReferences()
    apply_job_parameters([], "bundle", [param], asset_references, allow_uri_path_values=True)
    assert not asset_references


URI_DEFAULT_TEMPLATE = """
specificationVersion: jobtemplate-2023-09
EXTENSIONS
name: UriJob
parameterDefinitions:
- name: One
  type: PATH
  objectType: FILE
  dataFlow: IN
  default: s3://bucket/one.blend
- name: Many
  type: LIST[PATH]
  objectType: FILE
  dataFlow: IN
  default: ["s3://bucket/a.blend", "local.blend", ""]
steps:
- name: S
  script:
    actions:
      onRun:
        command: echo
"""


def test_read_job_bundle_parameters_uri_defaults_kept(
    fresh_deadline_config, temp_job_bundle_dir
) -> None:
    """A URI default is not a path in the job bundle, so it is neither joined with the
    bundle directory nor rejected for lying outside it."""
    with open(os.path.join(temp_job_bundle_dir, "template.yaml"), "w", encoding="utf8") as f:
        f.write(URI_DEFAULT_TEMPLATE.replace("EXTENSIONS", "extensions: [EXPR]"))

    one, many = read_job_bundle_parameters(temp_job_bundle_dir)

    assert one.get("value", one["default"]) == "s3://bucket/one.blend"
    assert many["value"] == [
        "s3://bucket/a.blend",
        os.path.normpath(os.path.join(os.path.abspath(temp_job_bundle_dir), "local.blend")),
        "",
    ]
    asset_references = AssetReferences()
    apply_job_parameters(
        [], temp_job_bundle_dir, [one, many], asset_references, allow_uri_path_values=True
    )
    assert asset_references == AssetReferences(input_filenames={many["value"][1]})


def test_read_job_bundle_parameters_uri_default_without_expr_is_a_path(
    fresh_deadline_config, temp_job_bundle_dir
) -> None:
    """URI values are an EXPR feature; without it a PATH default keeps its old handling."""
    template = {
        "specificationVersion": "jobtemplate-2023-09",
        "name": "UriJob",
        "parameterDefinitions": [
            {"name": "One", "type": "PATH", "default": "s3://bucket/one.blend"}
        ],
        "steps": [{"name": "S", "script": {"actions": {"onRun": {"command": "echo"}}}}],
    }
    with open(os.path.join(temp_job_bundle_dir, "template.json"), "w", encoding="utf8") as f:
        json.dump(template, f)
    (one,) = read_job_bundle_parameters(temp_job_bundle_dir)
    assert one["value"] != "s3://bucket/one.blend"


@pytest.mark.parametrize("list_type", ["LIST[PATH]", "list[path]"])
def test_bundle_browser_labels_list_path(list_type: str) -> None:
    try:
        from deadline.client.ui.dialogs.job_bundle_browser_dialog import _friendly_param_type
    except ImportError:
        pytest.skip("GUI dependencies are not installed")
    assert _friendly_param_type(list_type) == "Path list"
