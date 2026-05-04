# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""End-to-end tests for `deadline queue` subcommands."""

import json


def test_cli_queue_list(seeded_farm_queue, run_cli):
    _, _, queue_id, env = seeded_farm_queue
    r = run_cli(env, "queue", "list")
    assert r.returncode == 0, r.stderr or r.stdout
    assert queue_id in r.stdout
    assert "Test Queue" in r.stdout


def test_cli_queue_get_uses_config_defaults(seeded_farm_queue, run_cli):
    _, _, queue_id, env = seeded_farm_queue
    r = run_cli(env, "queue", "get")
    assert r.returncode == 0, r.stderr or r.stdout
    assert queue_id in r.stdout


def test_cli_queue_get_with_ids(seeded_farm_queue, run_cli):
    _, farm_id, queue_id, env = seeded_farm_queue
    r = run_cli(env, "queue", "get", "--farm-id", farm_id, "--queue-id", queue_id)
    assert r.returncode == 0, r.stderr or r.stdout
    assert queue_id in r.stdout


def test_cli_queue_get_unknown_id_fails(seeded_farm_queue, run_cli):
    _, farm_id, _, env = seeded_farm_queue
    r = run_cli(
        env,
        "queue",
        "get",
        "--farm-id",
        farm_id,
        "--queue-id",
        "queue-00000000000000000000000000000999",
    )
    assert r.returncode != 0


def test_cli_queue_paramdefs_empty_environments(seeded_farm_queue, run_cli):
    _, _, _, env = seeded_farm_queue
    r = run_cli(env, "queue", "paramdefs")
    assert r.returncode == 0, r.stderr or r.stdout


def test_cli_queue_paramdefs_unknown_queue_fails_with_suggestion(seeded_farm_queue, run_cli):
    """Unknown queue-id exercises ClientError -> _suggest_resources_on_client_error."""
    _, farm_id, real_queue_id, env = seeded_farm_queue
    r = run_cli(
        env,
        "queue",
        "paramdefs",
        "--farm-id",
        farm_id,
        "--queue-id",
        "queue-00000000000000000000000000000999",
    )
    assert r.returncode != 0
    combined = r.stdout + r.stderr
    assert "Failed to get Queue Parameter Definitions" in combined
    # Suggestion should include the real queue the user might have meant.
    assert real_queue_id in combined


def test_cli_queue_export_credentials_user_mode(seeded_farm_queue, run_cli):
    _, _, _, env = seeded_farm_queue
    r = run_cli(env, "queue", "export-credentials", "--mode", "USER")
    assert r.returncode == 0, r.stderr or r.stdout
    payload = json.loads(r.stdout)
    assert payload["Version"] == 1
    assert payload["AccessKeyId"]
    assert payload["SecretAccessKey"]
    assert payload["SessionToken"]
    assert payload["Expiration"]


def test_cli_queue_export_credentials_read_mode(seeded_farm_queue, run_cli):
    _, _, _, env = seeded_farm_queue
    r = run_cli(env, "queue", "export-credentials", "--mode", "READ")
    assert r.returncode == 0, r.stderr or r.stdout
    payload = json.loads(r.stdout)
    assert payload["Version"] == 1


def test_cli_queue_help(deadline_env, run_cli):
    _, env = deadline_env
    r = run_cli(env, "queue", "--help")
    assert r.returncode == 0
    for sub in ("list", "get", "export-credentials", "paramdefs", "sync-output"):
        assert sub in r.stdout
