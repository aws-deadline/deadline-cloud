# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""End-to-end tests for `deadline farm` subcommands."""


def test_cli_farm_list(seeded_farm_queue, run_cli):
    _, farm_id, _, env = seeded_farm_queue
    r = run_cli(env, "farm", "list")
    assert r.returncode == 0, r.stderr or r.stdout
    assert farm_id in r.stdout
    assert "Test Farm" in r.stdout


def test_cli_farm_list_empty(deadline_env, run_cli, configure_cli_defaults):
    _, env = deadline_env
    configure_cli_defaults(env)
    r = run_cli(env, "farm", "list")
    assert r.returncode == 0, r.stderr or r.stdout
    assert "[]" in r.stdout


def test_cli_farm_get_uses_config_default(seeded_farm_queue, run_cli):
    _, farm_id, _, env = seeded_farm_queue
    r = run_cli(env, "farm", "get")
    assert r.returncode == 0, r.stderr or r.stdout
    assert farm_id in r.stdout
    assert "Test Farm" in r.stdout


def test_cli_farm_get_with_id_flag(seeded_farm_queue, run_cli):
    _, farm_id, _, env = seeded_farm_queue
    r = run_cli(env, "farm", "get", "--farm-id", farm_id)
    assert r.returncode == 0, r.stderr or r.stdout
    assert farm_id in r.stdout


def test_cli_farm_get_unknown_id_fails(seeded_farm_queue, run_cli):
    _, _, _, env = seeded_farm_queue
    r = run_cli(env, "farm", "get", "--farm-id", "farm-00000000000000000000000000000999")
    assert r.returncode != 0
    assert "Failed to get Farm" in r.stdout or "Failed to get Farm" in r.stderr


def test_cli_farm_help(deadline_env, run_cli):
    _, env = deadline_env
    r = run_cli(env, "farm", "--help")
    assert r.returncode == 0
    assert "list" in r.stdout and "get" in r.stdout
