# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for `deadline --guide`."""

import json
import re
import textwrap
from unittest.mock import patch

import click
import pytest
import yaml
from click.testing import CliRunner
from openjd.model import TemplateSpecificationVersion, decode_job_template

from deadline.client import api, config
from deadline.client.cli import main
from deadline.client.cli._agent_guide import AGENT_GUIDE
from deadline.client.cli._groups import job_group
from deadline.client.exceptions import DeadlineOperationError, DeadlineOperationTimedOut
from deadline.job_attachments.progress_tracker import DownloadSummaryStatistics

# `deadline <group> <command>` followed by the rest of its (possibly continued) line.
_COMMAND_RE = re.compile(r"(?<![\w/-])deadline\s+([a-z-]+)\s+([a-z-]+)([^\n|;)]*)")


# The guide with all whitespace collapsed, so phrases can be matched across line wraps.
_GUIDE_TEXT = " ".join(AGENT_GUIDE.split())

# Flags in the guide that belong to other tools (`openjd run --step`).
_NON_DEADLINE_FLAGS = {"--step"}


def _all_commands(command=main):
    yield command
    if isinstance(command, click.Group):
        for sub in command.commands.values():
            yield from _all_commands(sub)


def _guide_commands():
    text = AGENT_GUIDE.replace("\\\n", " ")
    return [(m.group(1), m.group(2), m.group(3)) for m in _COMMAND_RE.finditer(text)]


def test_guide_flag_prints_guide_and_exits():
    result = CliRunner().invoke(main, ["--guide"])

    assert result.exit_code == 0, result.output
    assert result.output.strip() == AGENT_GUIDE.strip()


@pytest.mark.parametrize("args", [[], ["job"], ["job", "logs"], ["bundle", "submit"]])
def test_every_help_page_points_agents_at_guide(args):
    result = CliRunner().invoke(main, [*args, "--help"])

    assert result.exit_code == 0, result.output
    assert result.output.count("deadline --guide") == 1


def test_guide_commands_and_flags_exist():
    commands = _guide_commands()
    assert len(commands) > 10

    problems = []
    for group_name, command_name, rest in commands:
        group = main.commands.get(group_name)
        command = group.commands.get(command_name) if isinstance(group, click.Group) else None
        if command is None:
            problems.append(f"deadline {group_name} {command_name}")
            continue
        params = {
            opt: param for param in command.params for opt in param.opts + param.secondary_opts
        }
        tokens = rest.split()
        for i, token in enumerate(tokens):
            if not re.fullmatch(r"--?[a-z][a-z0-9-]*", token) or token == "--help":
                continue
            param = params.get(token)
            if param is None:
                problems.append(f"deadline {group_name} {command_name} {token}")
            elif isinstance(param.type, click.Choice) and i + 1 < len(tokens):
                value = tokens[i + 1].rstrip("`.,")
                choices = param.type.choices
                if not param.type.case_sensitive:
                    value, choices = value.lower(), tuple(c.lower() for c in choices)
                if value not in choices:
                    problems.append(f"deadline {group_name} {command_name} {token} {value}")

    assert not problems, f"guide references unknown commands, flags, or values: {problems}"


def test_every_guide_flag_exists_on_some_command():
    """Catches flags mentioned only in prose, which the per-command check can't see."""
    known = {
        opt
        for command in _all_commands()
        for param in command.params
        for opt in param.opts + param.secondary_opts
    }
    flags = set(re.findall(r"(?<![\w-])(--[a-z][a-z0-9-]*)", AGENT_GUIDE))

    unknown = flags - known - _NON_DEADLINE_FLAGS - {"--help"}

    assert len(flags) > 10
    assert not unknown, f"guide mentions flags no command has: {sorted(unknown)}"


@pytest.mark.parametrize(
    "status, expected_exit_code, guide_phrase",
    [
        ("SUCCEEDED", 0, "0 succeeded"),
        ("FAILED", 2, "2 failed"),
        ("CANCELED", 3, "3 canceled"),
        ("SUSPENDED", 4, "4 suspended"),
        ("ARCHIVED", 4, "4 suspended or archived"),
        ("NOT_COMPATIBLE", 5, "5 not compatible"),
    ],
)
def test_guide_job_wait_exit_codes_match_cli(
    fresh_deadline_config, status, expected_exit_code, guide_phrase
):
    assert guide_phrase in _GUIDE_TEXT

    result = _invoke_job_wait(
        api.JobCompletionResult(status=status, failed_tasks=[], elapsed_time=1.0)
    )

    assert result.exit_code == expected_exit_code, result.output


@pytest.mark.parametrize(
    "error, expected_exit_code, guide_phrase",
    [
        (DeadlineOperationTimedOut("timed out"), 1, "1 timed out"),
        (DeadlineOperationError("boom"), 2, "2 failed or the wait itself errored"),
    ],
)
def test_guide_job_wait_error_exit_codes_match_cli(
    fresh_deadline_config, error, expected_exit_code, guide_phrase
):
    assert guide_phrase in _GUIDE_TEXT
    assert "`error` key" in _GUIDE_TEXT

    result = _invoke_job_wait(error)

    assert result.exit_code == expected_exit_code, result.output
    assert "error" in json.loads(result.output)


def test_guide_failed_tasks_keys_match_job_wait_json(fresh_deadline_config):
    failed_task = api.FailedTask(
        step_id="step-1",
        task_id="task-1",
        step_name="Render",
        parameters={},
        session_id="session-1",
    )

    result = _invoke_job_wait(
        api.JobCompletionResult(status="FAILED", failed_tasks=[failed_task], elapsed_time=1.0)
    )

    output = json.loads(result.output)
    assert "`failedTasks` lists the stepName, stepId, taskId, and sessionId" in _GUIDE_TEXT
    assert {"stepName", "stepId", "taskId", "sessionId"} <= output["failedTasks"][0].keys()


def test_guide_download_summary_keys_match_download_output_json():
    line = job_group._get_download_summary_message(
        DownloadSummaryStatistics(downloaded_files=["/out/frame_1.txt"]), is_json_format=True
    )

    message = json.loads(line)
    assert 'm.get("messageType") == "summary"' in AGENT_GUIDE
    assert 'm.get("files", [])' in AGENT_GUIDE
    assert message["messageType"] == "summary"
    assert message["files"] == ["/out/frame_1.txt"]


def _invoke_job_wait(outcome):
    """Run `deadline job wait --output json` with `wait_for_job_completion` returning or raising `outcome`."""
    config.set_setting("defaults.farm_id", "farm-0123456789abcdefabcdefabcdefabcd")
    config.set_setting("defaults.queue_id", "queue-0123456789abcdefabcdefabcdefabcd")
    config.set_setting("defaults.job_id", "job-0123456789abcdefabcdefabcdefabcd")
    with (
        patch.object(api, "wait_for_job_completion") as mock_wait,
        patch.object(api, "get_boto3_client") as boto3_client_mock,
    ):
        if isinstance(outcome, Exception):
            mock_wait.side_effect = outcome
        else:
            mock_wait.return_value = outcome
        boto3_client_mock().get_job.return_value = {"name": "Test Job"}
        return CliRunner().invoke(main, ["job", "wait", "--output", "json"])


def test_guide_example_template_is_valid():
    match = re.search(r"    specificationVersion:.*?\n(?=\n- )", AGENT_GUIDE, re.DOTALL)
    assert match, "example job template not found in guide"

    template = yaml.safe_load(textwrap.dedent(match.group(0)))
    decode_job_template(
        template=template, supported_extensions=[]
    )  # raises DecodeValidationError if invalid
    assert template["specificationVersion"] == TemplateSpecificationVersion.JOBTEMPLATE_v2023_09
