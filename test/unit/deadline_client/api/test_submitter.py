# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for deadline.client.api._submitter: the unified BaseSubmitter base class
and the get_queue_parameters helper (including its initial_values override).
"""

from __future__ import annotations

from typing import Any, Optional
from unittest.mock import patch

import pytest

from deadline.client.api import _submitter
from deadline.client.exceptions import DeadlineOperationError
from deadline.client.api._submitter import (
    SubmissionContext,
    BaseSubmitter,
    BaseSubmitterSettings,
    get_queue_parameters,
)
from deadline.client.job_bundle.submission import AssetReferences


class _StubSubmitter(BaseSubmitter):
    """Minimal concrete BaseSubmitter for exercising get_submission_context."""

    def __init__(self) -> None:
        self.calls: dict[str, Any] = {}

    def get_settings(self) -> BaseSubmitterSettings:
        self.calls["get_settings"] = True
        return BaseSubmitterSettings(job_name="from_scene")

    def get_job_template(
        self,
        settings: BaseSubmitterSettings,
        host_requirements: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        self.calls["host_requirements"] = host_requirements
        return {"name": settings.job_name, "steps": []}

    def get_parameter_values(
        self,
        settings: BaseSubmitterSettings,
        queue_parameters: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        self.calls["queue_parameters"] = queue_parameters
        return [{"name": "Frames", "value": "1-10"}]

    def get_asset_references(self, settings: BaseSubmitterSettings) -> AssetReferences:
        return AssetReferences(input_filenames={"/scene/hero.ma"})


# ---------------------------------------------------------------------------
# get_queue_parameters
# ---------------------------------------------------------------------------


def _defs():
    return [
        {"name": "Frames", "type": "STRING", "default": "1-1"},
        {"name": "CondaPackages", "type": "STRING", "default": ""},
        {"name": "HasValue", "type": "STRING", "default": "d", "value": "preset"},
    ]


def test_get_queue_parameters_uses_explicit_ids_and_resolves_values():
    with (
        patch.object(
            _submitter,
            "config_file",
        ),
        patch(
            "deadline.client.api._queue_parameters.get_queue_parameter_definitions",
            return_value=_defs(),
        ) as mock_defs,
    ):
        params = get_queue_parameters(farm_id="farm-x", queue_id="queue-y")

    mock_defs.assert_called_once_with(farmId="farm-x", queueId="queue-y")
    by_name = {p["name"]: p for p in params}
    # value defaulted from `default`
    assert by_name["Frames"]["value"] == "1-1"
    # empty default -> empty value (not missing)
    assert by_name["CondaPackages"]["value"] == ""
    # pre-existing value preserved
    assert by_name["HasValue"]["value"] == "preset"
    # full definition fields are retained (not reduced to name/value)
    assert by_name["Frames"]["type"] == "STRING"


def test_get_queue_parameters_initial_values_override():
    with (
        patch.object(_submitter, "config_file"),
        patch(
            "deadline.client.api._queue_parameters.get_queue_parameter_definitions",
            return_value=_defs(),
        ),
    ):
        params = get_queue_parameters(
            farm_id="farm-x",
            queue_id="queue-y",
            initial_values={"Frames": "1-10"},
        )
    assert {p["name"]: p["value"] for p in params}["Frames"] == "1-10"


def test_get_queue_parameters_falls_back_to_configured_defaults():
    def fake_get_setting(key: str) -> str:
        return {"defaults.farm_id": "farm-cfg", "defaults.queue_id": "queue-cfg"}[key]

    with (
        patch.object(_submitter.config_file, "get_setting", side_effect=fake_get_setting),
        patch(
            "deadline.client.api._queue_parameters.get_queue_parameter_definitions",
            return_value=_defs(),
        ) as mock_defs,
    ):
        get_queue_parameters()
    mock_defs.assert_called_once_with(farmId="farm-cfg", queueId="queue-cfg")


def test_get_queue_parameters_raises_when_unconfigured():
    with patch.object(_submitter.config_file, "get_setting", return_value=""):
        with pytest.raises(DeadlineOperationError):
            get_queue_parameters()


# ---------------------------------------------------------------------------
# BaseSubmitter.get_submission_context
# ---------------------------------------------------------------------------


def test_get_submission_context_builds_from_scene_when_no_settings():
    api = _StubSubmitter()
    with patch.object(
        _submitter,
        "get_queue_parameters",
        return_value=[{"name": "P", "value": "v"}],
    ) as mock_qp:
        ctx = api.get_submission_context()

    assert isinstance(ctx, SubmissionContext)
    assert api.calls.get("get_settings") is True
    assert ctx.settings.job_name == "from_scene"
    # asset_references is carried through as the typed AssetReferences object,
    # not reduced to a dict.
    assert isinstance(ctx.asset_references, AssetReferences)
    assert ctx.asset_references.input_filenames == {"/scene/hero.ma"}
    # defaults path: no explicit farm/queue/initial_values
    mock_qp.assert_called_once_with(farm_id=None, queue_id=None, initial_values=None)


def test_get_submission_context_threads_through_args():
    api = _StubSubmitter()
    host_req = {"attributes": [{"name": "attr.worker.os.family", "anyOf": ["linux"]}]}
    with patch.object(_submitter, "get_queue_parameters", return_value=[]) as mock_qp:
        api.get_submission_context(
            BaseSubmitterSettings(job_name="explicit"),
            farm_id="farm-a",
            queue_id="queue-b",
            initial_values={"Frames": "1-10"},
            host_requirements=host_req,
        )
    mock_qp.assert_called_once_with(
        farm_id="farm-a", queue_id="queue-b", initial_values={"Frames": "1-10"}
    )
    # host_requirements forwarded to get_job_template
    assert api.calls["host_requirements"] == host_req


def test_get_submission_context_uses_prefetched_queue_parameters():
    api = _StubSubmitter()
    prefetched = [{"name": "Pre", "value": "1"}]
    with patch.object(_submitter, "get_queue_parameters") as mock_qp:
        api.get_submission_context(BaseSubmitterSettings(job_name="x"), queue_parameters=prefetched)
    # When queue_parameters is supplied, the fetch helper is not called.
    mock_qp.assert_not_called()
    assert api.calls["queue_parameters"] == prefetched
