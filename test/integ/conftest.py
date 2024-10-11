# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import json
import os
import pytest


@pytest.fixture()
def external_bucket() -> str:
    """
    Return a bucket that all developers and test accounts have access to, but isn't in the testers account.
    """
    return os.environ.get("INTEG_TEST_JA_CROSS_ACCOUNT_BUCKET", "job-attachment-bucket-snipe-test")


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


@pytest.fixture(scope="session")
def default_job_template_step_step_dependency() -> str:
    """
    A generic job template with 2 steps. Second step depends on first step. Both steps have 1 task.
    """
    return json.dumps(
        {
            "name": "custom-step-step-job",
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
                {
                    "name": "custom-step-2",
                    "dependencies": [{"dependsOn": "custom-step"}],
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
