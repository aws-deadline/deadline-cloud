# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import getpass
import json
import sys
import pytest
import ctypes


@pytest.fixture(scope="session")
def default_job_template() -> str:
    """
    A generic job template with 2 steps. First step has 2 tasks and the second step has 1 task.
    """
    return json.dumps(
        {
            "name": "custom-job",
            "specificationVersion": "jobtemplate-2023-09",
            "steps": [
                {
                    "name": "custom-step",
                    "parameterSpace": {
                        "taskParameterDefinitions": [
                            {"name": "frame", "type": "INT", "range": ["0", "1"]}
                        ]
                    },
                    "script": {
                        "actions": {"onRun": {"command": "{{ Task.File.run }}"}},
                        "embeddedFiles": [
                            {
                                "name": "run",
                                "data": "#!/bin/env bash\n" "set -ex\n" "echo 'First Step'",
                                "runnable": True,
                                "type": "TEXT",
                            }
                        ],
                    },
                },
                {
                    "name": "custom-step-2",
                    "parameterSpace": {
                        "taskParameterDefinitions": [
                            {"name": "frame", "type": "INT", "range": ["0"]}
                        ]
                    },
                    "script": {
                        "actions": {"onRun": {"command": "{{ Task.File.run }}"}},
                        "embeddedFiles": [
                            {
                                "name": "run",
                                "data": "#!/bin/env bash\n" "set -ex\n" "echo 'Second step'",
                                "runnable": True,
                                "type": "TEXT",
                            }
                        ],
                    },
                },
            ],
        }
    )


@pytest.fixture()
def default_job_template_one_task_one_step() -> str:
    """
    A generic job template with one step and one task.
    """
    return json.dumps(
        {
            "name": "custom-job",
            "specificationVersion": "jobtemplate-2023-09",
            "steps": [
                {
                    "name": "custom-step",
                    "parameterSpace": {
                        "taskParameterDefinitions": [
                            {"name": "frame", "type": "INT", "range": ["0"]}
                        ]
                    },
                    "script": {
                        "actions": {"onRun": {"command": "{{ Task.File.run }}"}},
                        "embeddedFiles": [
                            {
                                "name": "run",
                                "data": "#!/bin/env bash\n" "set -ex\n" "echo 'First Step'",
                                "runnable": True,
                                "type": "TEXT",
                            }
                        ],
                    },
                },
            ],
        }
    )


@pytest.fixture()
def external_bucket() -> str:
    """
    Return a bucket that all developers and test accounts have access to, but isn't in the testers account.
    """
    return "job-attachment-bucket-snipe-test"


def is_windows_non_admin():
    return sys.platform == "win32" and getpass.getuser() != "Administrator"


def is_Windows_file_path_limit():
    if sys.platform == "win32":
        ntdll = ctypes.WinDLL("ntdll")
        ntdll.RtlAreLongPathsEnabled.restype = ctypes.c_ubyte
        ntdll.RtlAreLongPathsEnabled.argtypes = ()

        return bool(ntdll.RtlAreLongPathsEnabled())
    return True
