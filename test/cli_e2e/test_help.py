# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""Top-level help / version / unknown-command behavior."""


def test_cli_deadline_no_args_prints_help(deadline_env, run_cli):
    _, env = deadline_env
    r = run_cli(env)
    # click groups with no args either error out or print help; either is fine.
    assert "Usage" in (r.stdout + r.stderr)


def test_cli_deadline_help_lists_all_groups(deadline_env, run_cli):
    _, env = deadline_env
    r = run_cli(env, "--help")
    assert r.returncode == 0
    for group in (
        "farm",
        "queue",
        "fleet",
        "worker",
        "job",
        "bundle",
        "manifest",
        "attachment",
        "auth",
        "config",
        "handle-web-url",
        "mcp-server",
    ):
        assert group in r.stdout, f"missing group: {group}"


def test_cli_job_help_lists_trace_schedule(deadline_env, run_cli):
    _, env = deadline_env
    r = run_cli(env, "job", "--help")
    assert r.returncode == 0
    assert "trace-schedule" in r.stdout


def test_cli_deadline_version(deadline_env, run_cli):
    _, env = deadline_env
    r = run_cli(env, "--version")
    assert r.returncode == 0
    out = r.stdout.strip()
    # Format is roughly "deadline, version X.Y.Z"
    assert "." in out


def test_cli_deadline_unknown_command(deadline_env, run_cli):
    _, env = deadline_env
    r = run_cli(env, "not-a-real-command")
    assert r.returncode != 0


def test_cli_deadline_unknown_option(deadline_env, run_cli):
    _, env = deadline_env
    r = run_cli(env, "--nonsense-flag")
    assert r.returncode != 0
