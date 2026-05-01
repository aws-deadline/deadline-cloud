# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Sanity tests proving the fixtures wire up the CLI correctly."""


def test_cli_help_exits_zero(deadline_env, run_cli):
    _, env = deadline_env
    r = run_cli(env, "--help")
    assert r.returncode == 0
    assert "deadline" in r.stdout.lower()


def test_cli_version_exits_zero(deadline_env, run_cli):
    _, env = deadline_env
    r = run_cli(env, "--version")
    assert r.returncode == 0
    assert r.stdout.strip()


def test_cli_unknown_command_exits_nonzero(deadline_env, run_cli):
    _, env = deadline_env
    r = run_cli(env, "definitely-not-a-command")
    assert r.returncode != 0


def test_cli_deadline_reaches_mock_backend(seeded_farm_queue, run_cli):
    """Minimal end-to-end path: CLI talks to mock backend via config + get."""
    _, farm_id, _, env = seeded_farm_queue
    r = run_cli(env, "farm", "get", "--farm-id", farm_id)
    assert r.returncode == 0, f"stderr={r.stderr}\nstdout={r.stdout}"
    assert farm_id in r.stdout
