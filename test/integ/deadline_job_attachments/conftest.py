# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import json

import pytest


@pytest.fixture(scope="session")
def default_job_template() -> str:
    """
    A generic job template with 2 steps. First step has 2 tasks and the second step has 1 task.
    """
    return json.dumps(
        {
            "name": "custom-job",
            "specificationVersion": "2022-09-01",
            "steps": [
                {
                    "name": "custom-step",
                    "parameterSpace": {
                        "parameters": [{"name": "frame", "type": "INT", "range": ["0", "1"]}]
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
                        "parameters": [{"name": "frame", "type": "INT", "range": ["0"]}]
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
            "specificationVersion": "2022-09-01",
            "steps": [
                {
                    "name": "custom-step",
                    "parameterSpace": {
                        "parameters": [{"name": "frame", "type": "INT", "range": ["0"]}]
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
