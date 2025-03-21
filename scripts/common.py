# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
import subprocess
import sys


class BadExitCodeError(Exception):
    pass


class EvaluationBuildError(Exception):
    pass


class UnsupportedOSError(Exception):
    pass


def run(cmd, cwd=None, env=None, echo=True):
    if echo:
        sys.stdout.write(f"Running cmd: {cmd}\n")
    kwargs = {
        "shell": True,
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
    }
    if isinstance(cmd, list):
        kwargs["shell"] = False
    if cwd is not None:
        kwargs["cwd"] = cwd
    if env is not None:
        kwargs["env"] = env
    p = subprocess.Popen(cmd, **kwargs)
    stdout, stderr = p.communicate()
    output = stdout.decode("utf-8") + stderr.decode("utf-8")
    if p.returncode != 0:
        raise BadExitCodeError(
            f"Process failed with exit code ({p.returncode}) for command '{cmd}': {output}"
        )
    return output
