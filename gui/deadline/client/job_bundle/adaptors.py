# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

import re


def parse_frame_range(frame_string: str) -> list[int]:
    framelist_re = re.compile(r"^(?P<start>-?\d+)(-(?P<stop>-?\d+)(:(?P<step>-?\d+))?)?$")
    match = framelist_re.match(frame_string)
    if not match:
        raise ValueError("Framelist not valid")

    start = int(match.group("start"))
    stop = int(match.group("stop")) if match.group("stop") is not None else start
    frame_step = (
        int(match.group("step")) if match.group("step") is not None else 1 if start < stop else -1
    )

    if frame_step == 0:
        raise ValueError("Frame step cannot be zero")
    if start > stop and frame_step > 0:
        raise ValueError("Start frame must be less than stop frame if step is positive")
    if start < stop and frame_step < 0:
        raise ValueError("Start frame must be greater than stop frame if step is negative")

    return list(range(start, stop + (1 if frame_step > 0 else -1), frame_step))
