# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""CLI-level tests that a pre-submission hook's parameter changes reach CreateJob.

These drive the CLI entry point (via ``CliRunner``) against the moto-backed
``deadline_mock`` fixture and assert on the exact ``parameters`` sent to CreateJob — which
the subprocess-based ``test/cli_e2e`` mock backend does not record. The broader "hook runs
end to end" behaviors (a hook writes a file, post-submission fires, disabled-by-default)
are covered as true subprocess e2e tests in ``test/cli_e2e/test_bundle_hooks.py``.
"""

import os
import textwrap

from click.testing import CliRunner

from deadline.client import config
from deadline.client.cli import main

from ..api.test_job_bundle_submission import MOCK_FARM_ID, MOCK_QUEUE_ID
from ..testing_utilities import MOCK_CREATE_JOB_RESPONSE, MOCK_GET_JOB_RESPONSE

_TEMPLATE = """specificationVersion: 'jobtemplate-2023-09'
name: HookParamTest
parameterDefinitions:
- name: Message
  type: STRING
  default: original_message
steps:
- name: S
  script:
    actions:
      onRun:
        command: echo
"""

_PARAM_VALUES = "parameterValues:\n- name: Message\n  value: original_message\n"


def _write(path, contents):
    with open(path, "w", encoding="utf8") as f:
        f.write(contents)


def _write_hook_bundle(bundle_dir, hooks_yaml, *scripts):
    """Write template, parameter_values, hooks.yaml, and any (name, contents) scripts."""
    _write(os.path.join(bundle_dir, "template.yaml"), _TEMPLATE)
    _write(os.path.join(bundle_dir, "parameter_values.yaml"), _PARAM_VALUES)
    _write(os.path.join(bundle_dir, "hooks.yaml"), hooks_yaml)
    for name, contents in scripts:
        _write(os.path.join(bundle_dir, name), textwrap.dedent(contents))


def _enable_hooks():
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("settings.allow_bundle_hooks", "true")
    config.set_setting("settings.auto_accept", "true")


def test_pre_submission_hook_rewrites_parameter_reaches_create_job(
    fresh_deadline_config, deadline_mock, temp_job_bundle_dir
):
    """A pre-submission hook that rewrites parameter_values.yaml on disk changes the
    parameter value sent to CreateJob."""
    _enable_hooks()
    deadline_mock.create_job.return_value = MOCK_CREATE_JOB_RESPONSE
    deadline_mock.get_job.return_value = MOCK_GET_JOB_RESPONSE

    _write_hook_bundle(
        temp_job_bundle_dir,
        "version: '1.0'\npreSubmission:\n  - command: python3\n    args: [rewrite.py]\n",
        (
            "rewrite.py",
            """
            import os
            b = os.environ['DEADLINE_JOB_BUNDLE_DIR']
            open(os.path.join(b, 'parameter_values.yaml'), 'w').write(
                'parameterValues:\\n- name: Message\\n  value: changed_by_hook\\n')
            """,
        ),
    )

    result = CliRunner().invoke(main, ["bundle", "submit", temp_job_bundle_dir])

    assert result.exit_code == 0, result.output
    kwargs = deadline_mock.create_job.call_args.kwargs
    assert kwargs["parameters"]["Message"] == {"string": "changed_by_hook"}


def test_pre_submission_hook_stdout_parameter_reaches_create_job(
    fresh_deadline_config, deadline_mock, temp_job_bundle_dir
):
    """A pre-submission hook that emits a ``parameters`` map on stdout changes the
    parameter value sent to CreateJob."""
    _enable_hooks()
    deadline_mock.create_job.return_value = MOCK_CREATE_JOB_RESPONSE
    deadline_mock.get_job.return_value = MOCK_GET_JOB_RESPONSE

    _write_hook_bundle(
        temp_job_bundle_dir,
        "version: '1.0'\npreSubmission:\n  - command: python3\n    args: [emit.py]\n",
        (
            "emit.py",
            "import json; print(json.dumps({'parameters': {'Message': 'changed_via_stdout'}}))\n",
        ),
    )

    result = CliRunner().invoke(main, ["bundle", "submit", temp_job_bundle_dir])

    assert result.exit_code == 0, result.output
    kwargs = deadline_mock.create_job.call_args.kwargs
    assert kwargs["parameters"]["Message"] == {"string": "changed_via_stdout"}


def test_cli_parameter_takes_precedence_over_hook(
    fresh_deadline_config, deadline_mock, temp_job_bundle_dir
):
    """A CLI ``--parameter`` value wins over a hook-supplied value for the same parameter."""
    _enable_hooks()
    deadline_mock.create_job.return_value = MOCK_CREATE_JOB_RESPONSE
    deadline_mock.get_job.return_value = MOCK_GET_JOB_RESPONSE

    _write_hook_bundle(
        temp_job_bundle_dir,
        "version: '1.0'\npreSubmission:\n  - command: python3\n    args: [emit.py]\n",
        (
            "emit.py",
            "import json; print(json.dumps({'parameters': {'Message': 'from_hook'}}))\n",
        ),
    )

    result = CliRunner().invoke(
        main,
        ["bundle", "submit", temp_job_bundle_dir, "--parameter", "Message=from_cli"],
    )

    assert result.exit_code == 0, result.output
    kwargs = deadline_mock.create_job.call_args.kwargs
    assert kwargs["parameters"]["Message"] == {"string": "from_cli"}
