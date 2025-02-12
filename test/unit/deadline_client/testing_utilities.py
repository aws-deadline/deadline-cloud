# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
A set of utilities developed for this set of tests.
"""

import contextlib
import os
import tempfile
from typing import Dict, Optional, Tuple, Any

if os.name == "nt":

    def _batfile_quote(s: str) -> str:
        """Quotes the string so it can be echoed in a batfile script."""
        for replacement in [
            ("%", "%%"),
            ('"', '""'),
            ("^", "^^"),
            ("&", "^&"),
            ("<", "^<"),
            (">", "^>"),
            ("|", "^|"),
        ]:
            s = s.replace(*replacement)
        return f"{s}"

    _file_extension = ".bat"
    _header = "@echo off\n"

    def _format_sleep(seconds: float) -> str:
        return f'powershell -nop -c "{{sleep {seconds}}}" > nul\n'

    def _format_line(line: str) -> str:
        return f"echo.{line}\n"

    def _format_exit(exit_code: int) -> str:
        return f"exit {exit_code}\n"

    def _format_args_check(args: Tuple[str], args_index: int) -> str:
        result = ["set MATCHED=YES\n"]
        for i, arg in enumerate(args, start=1):
            result.append(f'if not "%{i}" == "{_batfile_quote(arg)}" set MATCHED=NO\n')
        result.append(f'if not "%{len(args) + 1}" == "" set MATCHED=NO\n')
        result.append(f"if %MATCHED% == NO goto no_match_{args_index}\n")
        return "".join(result)

    def _format_end_args_check(args_index: int) -> str:
        return f":no_match_{args_index}\n"

else:
    import shlex

    _file_extension = ".sh"
    _header = "#!/bin/sh\n"

    def _format_sleep(seconds: float) -> str:
        return f"sleep {seconds}\n"

    def _format_line(line: str) -> str:
        return f"echo {shlex.quote(line)}\n"

    def _format_exit(exit_code: int) -> str:
        return f"exit {exit_code}\n"

    def _format_args_check(args: Tuple[str], args_index: int) -> str:
        result = [f"if [ $# == {len(args)} ] "]
        for i, arg in enumerate(args, start=1):
            result.append(f'&& [ "${i}" == {shlex.quote(arg)} ] ')
        result.append("; then ")  # lack of \n here puts the next commend with the `then`
        return "".join(result)

    def _format_end_args_check(args_index: int) -> str:
        return "fi\n"


def _format_output_and_exit(program_output: str, exit_code: int) -> str:
    result = []
    for line in program_output.splitlines():
        result.append(_format_line(line))
    result.append(_format_exit(exit_code))
    return "".join(result)


@contextlib.contextmanager
def program_that_prints_output(
    program_output: str,
    exit_code: int,
    *,
    sleep_seconds=0.1,
    conditional_outputs: Optional[Dict[Tuple[str], Tuple[str, int]]] = None,
):
    """
    This context manager creates a program that prints the specified output, then returns
    the specified exit code.

    By default, the program sleeps for 0.1 seconds, so tests that check for a thread or process
    immediately after launching can do so.

    If conditional outputs are provided, they change the output and exit code for the specified args.
    """
    try:
        with tempfile.NamedTemporaryFile(
            mode="w+t", suffix=_file_extension, encoding="utf8", delete=False
        ) as temp:
            temp.write(_header)
            if sleep_seconds > 0:
                temp.write(_format_sleep(sleep_seconds))
            # Handle each conditional output
            if conditional_outputs:
                for args_index, (
                    args,
                    (conditional_program_output, conditional_exit_code),
                ) in enumerate(conditional_outputs.items()):
                    temp.write(_format_args_check(args, args_index))
                    temp.write(
                        _format_output_and_exit(conditional_program_output, conditional_exit_code)
                    )
                    temp.write(_format_end_args_check(args_index))
            # Handle the default output
            temp.write(_format_output_and_exit(program_output, exit_code))
            temp.flush()
            if os.name != "nt":
                os.chmod(temp.name, 0o500)

        yield temp.name
    finally:
        os.remove(temp.name)


def create_sample_job_template(
    num_of_job_env: int,
    num_of_step_env_per_step: int,
    num_of_steps: int,
    num_of_tasks_per_step: int,
    on_enter_timeout: Optional[int] = None,
    on_exit_timeout: Optional[int] = None,
    on_task_run_timeout: Optional[int] = None,
) -> dict[str, Any]:
    """
    This function creates simple job templates.
    Optionally adds the timeouts if explicitly added.
    """

    SAMPLE_ENVIRONMENT_SCRIPT = {
        "actions": {
            "onEnter": {"command": "/usr/bin/sleep", "args": ["1"]},
            "onExit": {"command": "/usr/bin/sleep", "args": ["1"]},
        }
    }

    job_template: dict[str, Any] = {
        "specificationVersion": "2022-09-01",
        "name": "DEFAULT_JOB_NAME",
        "description": "DEFAULT_DESC",
    }

    job_envs = []
    for job_env_id in range(num_of_job_env):
        job_env_dict: dict[str, Any] = {
            "name": f"DEFAULT_JOB_ENVIRONMENT_NAME-{job_env_id}",
            "script": SAMPLE_ENVIRONMENT_SCRIPT,
        }
        if on_enter_timeout:
            job_env_dict["script"]["actions"]["onEnter"]["timeout"] = on_enter_timeout
        if on_exit_timeout:
            job_env_dict["script"]["actions"]["onExit"]["timeout"] = on_exit_timeout
        job_envs.append(job_env_dict)

    job_template["jobEnvironments"] = job_envs

    steps = []
    for stepIndex in range(num_of_steps):
        step_dict: dict[str, Any]
        step_dict = {
            "name": f"Step{stepIndex}",
            "description": "DEFAULT_STEP_DESCRIPTION",
            "parameterSpace": {
                "taskParameterDefinitions": [
                    {
                        "name": "DEFAULT_PARAMETER_NAME",
                        "type": "INT",
                        "range": f"1-{num_of_tasks_per_step}",
                    },
                ],
            },
            "script": {
                "embeddedFiles": [{"name": "initData", "type": "TEXT", "data": "data1"}],
                "actions": {"onRun": {"command": "/usr/bin/sleep", "args": ["1"]}},
            },
        }
        if on_task_run_timeout:
            step_dict["script"]["actions"]["onRun"]["timeout"] = on_task_run_timeout
        step_envs = []
        for step_env_id in range(num_of_step_env_per_step):
            step_env_dict: dict[str, Any] = {
                "name": f"DEFAULT_STEP_ENVIRONMENT_NAME-{step_env_id}",
                "script": SAMPLE_ENVIRONMENT_SCRIPT,
            }
            if on_enter_timeout:
                step_env_dict["script"]["actions"]["onEnter"]["timeout"] = on_enter_timeout
            if on_exit_timeout:
                step_env_dict["script"]["actions"]["onExit"]["timeout"] = on_exit_timeout
            step_envs.append(step_env_dict)
        step_dict["stepEnvironments"] = step_envs
        steps.append(step_dict)

    job_template["steps"] = steps
    return job_template
