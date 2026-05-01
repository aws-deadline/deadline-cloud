# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""End-to-end tests for `deadline config` subcommands."""

import json
from pathlib import Path


def test_cli_config_show_verbose(deadline_env, run_cli):
    _, env = deadline_env
    r = run_cli(env, "config", "show")
    assert r.returncode == 0, r.stderr or r.stdout
    assert "defaults.farm_id" in r.stdout
    assert "defaults.queue_id" in r.stdout


def test_cli_config_show_json(deadline_env, run_cli):
    _, env = deadline_env
    r = run_cli(env, "config", "show", "--output", "json")
    assert r.returncode == 0, r.stderr or r.stdout
    payload = json.loads(r.stdout)
    assert "defaults.farm_id" in payload
    assert "settings.config_file_path" in payload


def test_cli_config_set_and_get(deadline_env, run_cli):
    _, env = deadline_env
    r = run_cli(env, "config", "set", "defaults.farm_id", "farm-1234")
    assert r.returncode == 0, r.stderr or r.stdout
    r = run_cli(env, "config", "get", "defaults.farm_id")
    assert r.returncode == 0, r.stderr or r.stdout
    assert r.stdout.strip() == "farm-1234"


def test_cli_config_set_persists_to_config_file_env_var(deadline_env, run_cli):
    _, env = deadline_env
    r = run_cli(env, "config", "set", "defaults.farm_id", "farm-5678")
    assert r.returncode == 0, r.stderr or r.stdout
    text = Path(env["DEADLINE_CONFIG_FILE_PATH"]).read_text()
    assert "farm-5678" in text


def test_cli_config_clear_restores_default(deadline_env, run_cli):
    _, env = deadline_env
    run_cli(env, "config", "set", "defaults.farm_id", "farm-to-clear")
    r = run_cli(env, "config", "clear", "defaults.farm_id")
    assert r.returncode == 0, r.stderr or r.stdout
    r = run_cli(env, "config", "get", "defaults.farm_id")
    assert r.returncode == 0, r.stderr or r.stdout
    assert "farm-to-clear" not in r.stdout


def test_cli_config_get_unknown_setting_fails(deadline_env, run_cli):
    _, env = deadline_env
    r = run_cli(env, "config", "get", "defaults.nonsense")
    assert r.returncode != 0


def test_cli_config_help(deadline_env, run_cli):
    _, env = deadline_env
    r = run_cli(env, "config", "--help")
    assert r.returncode == 0
    for sub in ("show", "get", "set", "clear"):
        assert sub in r.stdout
