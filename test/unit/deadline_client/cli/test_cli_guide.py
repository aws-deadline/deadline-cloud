# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for `deadline --guide`."""

import re
import textwrap

import click
import pytest
import yaml
from click.testing import CliRunner
from openjd.model import TemplateSpecificationVersion, decode_job_template

from deadline.client.cli import main
from deadline.client.cli._agent_guide import AGENT_GUIDE

# `deadline <group> <command>` followed by the rest of its (possibly continued) line.
_COMMAND_RE = re.compile(r"(?<![\w/-])deadline\s+([a-z-]+)\s+([a-z-]+)([^\n|;)]*)")


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
        known = {opt for param in command.params for opt in param.opts + param.secondary_opts}
        for flag in re.findall(r"(?<![\w-])(--[a-z][a-z0-9-]*)", rest):
            if flag not in known | {"--help"}:
                problems.append(f"deadline {group_name} {command_name} {flag}")

    assert not problems, f"guide references unknown commands or flags: {problems}"


def test_guide_example_template_is_valid():
    match = re.search(r"    specificationVersion:.*?\n(?=\n- )", AGENT_GUIDE, re.DOTALL)
    assert match, "example job template not found in guide"

    template = yaml.safe_load(textwrap.dedent(match.group(0)))
    decode_job_template(
        template=template, supported_extensions=[]
    )  # raises DecodeValidationError if invalid
    assert template["specificationVersion"] == TemplateSpecificationVersion.JOBTEMPLATE_v2023_09
