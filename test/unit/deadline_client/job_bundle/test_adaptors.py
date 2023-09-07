# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

import pytest

from deadline.client.job_bundle.adaptors import (
    parse_frame_range,
)


FAKE_ENVIRONMENT = {"fake": "environment"}
STEP = {
    "name": "MyStep",
    "parameterSpace": {
        "taskParameterDefinitions": [
            {"name": "Frame", "range": "{{ Param.Frames }}", "type": "INT"}
        ]
    },
    "jobEnvironments": [FAKE_ENVIRONMENT],
    "script": {
        "embeddedFiles": [
            {"name": "runData", "type": "TEXT", "data": "frame: {{ Task.Param.Frame }}"},
        ],
        "actions": {
            "onRun": {
                "command": "DCCAdaptor",
                "args": [
                    "background",
                    "run",
                    "--connection-file",
                    "{{ Session.WorkingDirectory }}/connection.json",
                    "--run-data",
                    "file://{{ Task.File.runData }}",
                ],
                "cancelation": {
                    "mode": "NOTIFY_THEN_TERMINATE",
                },
            },
        },
    },
}


def _get_inclusive_range(start, end, step):
    if end is None and step is None:
        return [start]
    elif (
        end is None
        or start is None
        or start < end
        and step is not None
        and step < 0
        or start > end
        and step is not None
        and step > 0
    ):
        return None
    if step is None:
        step = 1 if end > start else -1
    return list(range(start, end + (1 if step > 0 else -1), step))


@pytest.mark.parametrize(
    "frame_string,result",
    [
        (
            f"{start}{('-'+str(end)) if end is not None else ''}{(':'+str(step)) if step is not None else ''}",
            _get_inclusive_range(start, end, step),
        )
        for start in (-20, 0, 20)
        for end in (-20, 0, 20, None)
        for step in (-1, 1, -3, 3, None)
    ],
)
def test_parse_frame_range(frame_string: str, result: list[int]):
    if result is None:
        with pytest.raises(ValueError):
            parse_frame_range(frame_string)
    else:
        assert parse_frame_range(frame_string) == result
