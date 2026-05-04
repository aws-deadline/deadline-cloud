# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""Smoke tests for `deadline mcp-server`. We don't spin the server up fully,
since it blocks waiting for stdin; we just verify `--help` and that the
command is registered."""


def test_cli_mcp_server_help(deadline_env, run_cli):
    _, env = deadline_env
    r = run_cli(env, "mcp-server", "--help")
    assert r.returncode == 0
    assert "MCP" in r.stdout or "Model Context Protocol" in r.stdout
