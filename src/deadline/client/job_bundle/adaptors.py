# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

import re
from typing import Any

from deadline.job_attachments.utils import OJIOToken


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


REZ_ENTER_SCRIPT = """#!/bin/env bash

set -euo pipefail

if [ ! -z "{{Param.RezPackages}}" ]; then
    echo "Rez Package List:"
    echo "   {{Param.RezPackages}}"

    # Create the environment
    /usr/local/bin/deadline-rez init \\
        -d "{{Session.WorkingDirectory}}" \\
        {{Param.RezPackages}}

    # Capture the environment's vars
    {{Env.File.InitialVars}}
    . /usr/local/bin/deadline-rez activate \\
        -d "{{Session.WorkingDirectory}}"
    {{Env.File.CaptureVars}}
else
    echo "No Rez Packages, skipping environment creation."
fi
"""

REZ_EXIT_SCRIPT = """#!/bin/env bash

set -euo pipefail

if [ ! -z "{{Param.RezPackages}}" ]; then
    echo "Rez Package List:"
    echo "   {{Param.RezPackages}}"

    /usr/local/bin/deadline-rez destroy \\
        -d "{{ Session.WorkingDirectory }}"
else
    echo "No Rez Packages, skipping environment teardown."
fi
"""

ENV_INITIALVARS_SCRIPT = """#!/usr/bin/env python3
import os, json
envfile = "{{Session.WorkingDirectory}}/.envInitial"
with open(envfile, "w", encoding="utf8") as f:
    json.dump(dict(os.environ), f)
"""

ENV_CAPTUREVARS_SCRIPT = """#!/usr/bin/env python3
import os, json, sys
envfile = "{{Session.WorkingDirectory}}/.envInitial"
if os.path.isfile(envfile):
    with open(envfile, "r", encoding="utf8") as f:
        before = json.load(f)
else:
    print("No initial environment found, must run Env.File.CaptureVars script first")
    sys.exit(1)
after = dict(os.environ)

put = {k: v for k, v in after.items() if v != before.get(k)}
delete = {k for k in before if k not in after}

for k, v in put.items():
    print(f"updating {k}={v}")
    print(f"openjobio_env: {k}={v}")
for k in delete:
    print(f"openjobio_unset_env: {k}")
"""


def get_rez_environment() -> dict[str, Any]:
    """This is deprecated, and moving to Queue Environments."""
    return {
        "name": "Rez",
        "description": "Initializes and destroys the Rez environment for the run",
        "script": {
            "actions": {
                "onEnter": {
                    "command": str(OJIOToken("Env.File.Enter")),
                },
                "onExit": {
                    "command": str(OJIOToken("Env.File.Exit")),
                },
            },
            "embeddedFiles": [
                {
                    "name": "Enter",
                    "filename": "rez-enter.sh",
                    "type": "TEXT",
                    "runnable": True,
                    "data": REZ_ENTER_SCRIPT,
                },
                {
                    "name": "Exit",
                    "filename": "rez-exit.sh",
                    "type": "TEXT",
                    "runnable": True,
                    "data": REZ_EXIT_SCRIPT,
                },
                {
                    "name": "InitialVars",
                    "filename": "initial-vars.sh",
                    "type": "TEXT",
                    "runnable": True,
                    "data": ENV_INITIALVARS_SCRIPT,
                },
                {
                    "name": "CaptureVars",
                    "filename": "capture-vars.sh",
                    "type": "TEXT",
                    "runnable": True,
                    "data": ENV_CAPTUREVARS_SCRIPT,
                },
            ],
        },
    }
